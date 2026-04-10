# Ordo

An automated experimental workflow engine for VISA-controlled instruments.

Ordo loads instrument drivers from TOML, recipes from YAML, precompiles command templates and expressions, and executes scheduled tasks with optional preview, dry-run, CSV output, and TCP streaming.

## Requirements

- A VISA implementation to talk to instruments at runtime, such as NI-VISA or Keysight IO Libraries

`--preview` does not open VISA sessions, so it can be used without a VISA library installed.


## Installation
For Linux or macOS users, Run
```sh
curl -fsSL https://raw.githubusercontent.com/uwni/ordo/main/install.sh | sh
```

Or



```sh
wget -qO- https://raw.githubusercontent.com/uwni/ordo/main/install.sh | sh
```

For Microsoft Windows users, Run

```ps
irm https://raw.githubusercontent.com/uwni/ordo/main/install.ps1 | iex
```


## Requirements

To run an existing Ordo binary, only the VISA runtime is required. You can download it from [National Instrument](https://www.ni.com/en/support/downloads/drivers/download.ni-visa.html).

If your VISA library is not installed in a standard location, pass it explicitly with `--visa-lib <path>`.

## Quick Start

Preview the bundled example recipe without opening instruments:

```sh
ordo run -d test-data/drivers test-data/recipes/r1_set.yaml --preview
```

Open an interactive REPL and discover instruments:

```sh
ordo repl
```

Or connect directly to a known resource:

```sh
ordo repl -r USB0::0x0957::0x1798::MY12345678::INSTR
```

Execute a recipe:

```sh
ordo run -d ./drivers ./recipes/measure.yaml --duration-ms 5000
```

## CLI

```text
ordo run -d <driver_dir> <recipe> [--preview] [--dry-run] [--duration-ms <ms>] [--visa-lib <path>]
ordo repl [-r <resource>] [--visa-lib <path>]
```

Command behavior:

- `run` loads drivers, precompiles the recipe, and executes scheduled tasks.
- `run --preview` validates the recipe, drivers, commands, variables, and pipeline configuration without instrument I/O.
- `run --dry-run` renders commands and logs them instead of writing to the instrument.
- `run --duration-ms` adds a hard runtime limit from the CLI.
- `repl` opens a stateful interactive session. Use `-r` to connect on startup, or discover instruments with `list` and `open` inside the REPL.

## Core Concepts

Ordo has two configuration layers:

- Drivers define instrument metadata and command templates in TOML.
- Recipes define instruments, variables, task schedules, expressions, and sinks in YAML.

### Driver Files

Driver documents are TOML files with optional metadata, an optional `instrument` section for VISA session defaults, and a `commands` table.

```toml
[metadata]
version = "1.2.3"
description = "Bench power supply"

[instrument]
timeout_ms = 2500
write_termination = "\n"
read_termination = "\n"
query_delay_ms = 25
chunk_size = 4096
manufacturer = "Keysight Technologies"
models = ["E36312A"]
firmware = "1.10"

[commands.set_voltage]
write = "VOLT {voltage},(@{channels})"

[commands.measure_voltage]
write = "MEAS:VOLT?"
read = "float"
```

Notes:

- Command template placeholders use `{name}` syntax.
- Placeholder names must be valid identifiers.
- A command with `read = "raw" | "float" | "int" | "string"` reads and parses a response after the write.
- `write_termination` is appended automatically to every command sent through that driver.
- `read_termination`, `timeout_ms`, `query_delay_ms`, and `chunk_size` tune the VISA session used for instruments that reference the driver.

### Recipe Files

Recipe documents are YAML files with these top-level sections:

- `instruments`: named instrument instances with a driver file and VISA resource string
- `vars`: initial variable values used by expressions and `save_as` slots
- `tasks`: periodic work definitions
- `pipeline`: optional CSV and TCP sink configuration
- `stop_when`: optional runtime stop conditions

Example:

```yaml
instruments:
	psu:
		driver: psu.toml
		resource: USB0::1::INSTR

vars:
	target_voltage: 5
	measured_voltage: 0
	delta: 0

pipeline:
	mode: realtime
	record:
		- measured_voltage
		- delta
	file_path: samples.csv

tasks:
	- every: 100ms
		steps:
			- call: set_voltage
				instrument: psu
				args:
					voltage: "${target_voltage}"
					channels:
						- 1
						- 2
			- call: measure_voltage
				instrument: psu
				save_as: measured_voltage
			- compute: "${measured_voltage} - ${target_voltage}"
				save_as: delta
				when: "$ITER > 0"

stop_when:
	time_elapsed: 2s
	max_iterations: 20
```

Recipe notes:

- Task intervals can be written as `every_ms: 100` or `every: 100ms`, `2s`, `1m`.
- Step arguments can be scalars or lists.
- A string argument written exactly as `${name}` is treated as a runtime variable reference.
- `save_as` stores a response or compute result into a declared variable slot.
- Variables referenced by `${name}` or `save_as` must be declared in `vars`.
- `when` is optional on both `call` and `compute` steps.

## Expressions

Expressions are used by `compute` and `when`.

Supported syntax:

- Variable references: `${name}`
- Built-ins: `$ITER`, `$TASK_IDX`
- Arithmetic: `+`, `-`, `*`, `/`
- Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Logical: `&&`, `||`, `!`
- Functions: `min(x, y)`, `max(x, y)`
- Parentheses for grouping

All expression math is evaluated as `f64`. A non-zero result is treated as true.

Important distinction:

- Driver command templates use `{name}` placeholders.
- Recipe expressions use `${name}` variable references.

## Preview And Dry-Run

`--preview` stops after recipe precompilation and prints a summary of:

- Instruments and resource addresses
- Stop conditions
- Pipeline configuration
- Task intervals and step kinds

`--dry-run` performs full scheduling and expression evaluation, but instrument calls are logged instead of written:

```text
[dry-run] psu.toml -> VOLT 5,(@1,2)
```

This is useful for validating template rendering and variable flow before touching hardware.

## Sampling Pipeline

Execution uses a dedicated sampler thread plus an SPSC ring buffer so slow sinks do not block instrument sampling.

- The sampler thread performs instrument I/O and produces task-level result frames.
- A worker thread drains the ring buffer and performs slower output work.
- High buffer usage emits warnings.
- Buffer overflow stops scheduling new work and is reported in the final summary.

Pipeline fields:

```yaml
pipeline:
	mode: realtime
	buffer_size: 8192
	warn_usage_percent: 90
	file_path: samples.csv
	network_host: 127.0.0.1
	network_port: 9000
	record: all
```

Notes:

- `mode` currently selects default behavior such as conservative versus throughput-oriented buffer sizing.
- `buffer_size` is normalized internally to a power of two.
- `warn_usage_percent` must be between 1 and 100.
- `file_path` writes CSV rows using recorded `save_as` values.
- `network_host` and `network_port` stream the same task-level frames as JSON over TCP.
- `record` can be `all` or an explicit list of `save_as` variable names.
- Relative sink paths are resolved from the recipe file directory.

## REPL

The REPL is a stateful interactive session with two modes: **disconnected** and **connected**.

In disconnected mode:

```text
list              List available VISA instruments.
open [<resource>]  Connect by address, or scan and pick interactively.
help              Show available commands.
quit              Leave the REPL.
```

In connected mode, instrument I/O commands become available:

```text
write <command>   Send a command to the instrument.
read              Read a response from the instrument.
query <command>   Send a command and then read the response.
list              List available VISA instruments.
close             Disconnect from the current instrument.
help              Show available commands.
quit              Leave the REPL.
```

Examples:

```sh
# Start disconnected, discover and connect interactively
ordo repl

# Connect to a known resource on startup
ordo repl -r USB0::0x0957::0x1798::MY12345678::INSTR
```

## Examples

Bundled examples live in:

- `test-data/drivers/`
- `test-data/recipes/`

Useful starting points:

- `test-data/drivers/psu.toml`
- `test-data/drivers/dmm.toml`
- `test-data/recipes/r1_set.yaml`
- `test-data/recipes/r1_set_voltage.yaml`
- `test-data/recipes/r2_stop_when.yaml`