# Operor [ˈɔpɛrɔr]

Operor Performs Experimental Runs On Resources.

Operor loads instrument adapters and recipes from JSON, precompiles command templates and expressions, and executes scheduled tasks with optional preview, dry-run, CSV output, and TCP streaming.

## Requirements

- Instruments with VISA interface
- A VISA implementation to talk to instruments at runtime, such as NI-VISA or Keysight IO Libraries[^1]

[^1]: `--preview` does not open VISA sessions, so it can be used without a VISA library installed.


## Installation
- For **Linux** or **macOS** users, Run
  ```sh
  # download and install operor via a install script
  curl -fsSL https://raw.githubusercontent.com/uwni/operor/main/install.sh | sh
  # or use wget instead via
  wget -qO- https://raw.githubusercontent.com/uwni/operor/main/install.sh | sh
  ```

> [!NOTE]
> **macOS** users may get a Gatekeeper warning because the binary is not signed/notarized. If that happens, allow it manually in **System Settings -> Privacy & Security**.
  
- For **Microsoft Windows** users, Run
  ```ps
  irm https://raw.githubusercontent.com/uwni/operor/main/install.ps1 | iex
  ```

## Requirements

To run an existing Operor binary, only the VISA runtime is required. You can download it from [National Instrument](https://www.ni.com/en/support/downloads/drivers/download.ni-visa.html).

If your VISA library is not installed in a standard location, pass it explicitly with `--visa-lib <path>`.

## Quick Start

Preview the bundled example recipe without opening instruments:

```sh
operor run -d test-data/adapters test-data/recipes/r1_set.json --preview
```

Open an interactive REPL and discover instruments:

```sh
operor repl
```

Or connect directly to a known resource:

```sh
operor repl -r USB0::...::INSTR
```

Execute a recipe:

```sh
operor run -d ./adapters ./recipes/measure.json
```

## CLI

```text
operor run -d <adapter_dir> <recipe> [--preview] [--dry-run] [--visa-lib <path>]
operor repl [-r <resource>] [--visa-lib <path>]
```

Command behavior:

- `run` loads adapters, precompiles the recipe, and executes scheduled tasks.
- `run --preview` validates the recipe, adapters, commands, variables, and pipeline configuration without instrument I/O.
- `run --dry-run` renders commands and logs them instead of writing to the instrument.
- `repl` opens a stateful interactive session. Use `-r` to connect on startup, or discover instruments with `list` and `open` inside the REPL.

> [!WARNING]
> Prevent your device from sleeping during experiments while running a recipe. Unexpected sleep can interrupt a running workflow; for example, on macOS you can use `caffeinate` to keep the system awake.


## Core Concepts

Operor has two configuration layers:

- Adapters define instrument metadata and command templates in JSON.
- Recipes define instruments, variables, task schedules, expressions, and sinks in JSON.

### Adapter Files

Adapter documents are JSON files with optional metadata, an optional `instrument` section for VISA session defaults, and a `commands` object.

```json
{
  "metadata": {
    "version": "1.2.3",
    "description": "Bench power supply"
  },
  "instrument": {
    "timeout_ms": 2500,
    "write_termination": "\n",
    "read_termination": "\n",
    "query_delay_ms": 25,
    "chunk_size": 4096,
    "manufacturer": "Keysight Technologies",
    "models": ["E36312A"],
    "firmware": "1.10"
  },
  "commands": {
    "set_voltage": {
      "write": "VOLT {voltage},(@{channels})"
    },
    "measure_voltage": {
      "write": "MEAS:VOLT?",
      "read": "float"
    }
  }
}
```

Notes:

- Command template placeholders use `{name}` syntax.
- Placeholder names must be valid identifiers.
- A command with `read = "raw" | "float" | "int" | "string"` reads and parses a response after the write.
- `write_termination` is appended automatically to every command sent through that adapter.
- `read_termination`, `timeout_ms`, `query_delay_ms`, and `chunk_size` tune the VISA session used for instruments that reference the adapter.

### Recipe Files

Recipe documents are JSON files with these top-level sections:

- `instruments`: named instrument instances with an adapter file and VISA resource string
- `vars`: initial variable values used by expressions and `assign` slots
- `tasks`: periodic work definitions
- `pipeline`: optional CSV and TCP sink configuration
- `stop_when`: optional stop condition expression

Example:

```json
{
  "instruments": {
    "psu": {
      "adapter": "psu.json",
      "resource": "USB0::1::INSTR"
    }
  },
  "vars": {
    "target_voltage": 5,
    "measured_voltage": 0,
    "delta": 0
  },
  "pipeline": {
    "mode": "realtime",
    "record": ["measured_voltage", "delta"],
    "file_path": "samples.csv"
  },
  "tasks": [
    {
      "while": "true",
      "steps": [
        {
          "call": "psu.set_voltage",
          "args": {
            "voltage": "${target_voltage}",
            "channels": [1, 2]
          }
        },
        {
          "call": "measure_voltage",
          "instrument": "psu",
          "assign": "measured_voltage"
        },
        {
          "compute": "${measured_voltage} - ${target_voltage}",
          "assign": "delta",
          "if": "$ITER > 0"
        }
      ]
    }
  ],
  "stop_when": "$ELAPSED_MS >= 2000 || $ITER >= 20"
}
```

Recipe notes:

- Tasks can be sequential (just `steps`), conditional (`if` guard), or looping (`while` condition).
- `if` is optional on both `call` and `compute` steps — the step is skipped when the expression is falsy.
- Use `sleep_ms: 100` as a step to pause between iterations.
- Step arguments can be scalars or lists.
- A string argument written exactly as `${name}` is treated as a runtime variable reference.
- `assign` stores a response or compute result into a declared variable slot.
- Variables referenced by `${name}` or `assign` must be declared in `vars`.

## Iterations

An **iteration** is one complete execution of a task's step list. Every time a task finishes running all of its steps from top to bottom, that counts as one iteration. The global iteration counter `$ITER` starts at 0 and increments by 1 after each iteration, regardless of which task produced it.

Iterations drive two key behaviors:

1. **Pipeline output** — at the end of each iteration, all `assign` values recorded during that run are collected into a single frame and pushed to the pipeline (CSV file, TCP stream). One iteration = one row in the output.
2. **Stop conditions** — `stop_when` is evaluated between iterations. `$ITER` reflects the number of completed iterations so far.

For a **sequential** task (no `while` or `if`), the steps run exactly once — that is one iteration. For a **loop** task (`while: true`), each pass through the step list is one iteration. For a **conditional** task (`if: ...`), the steps run at most once, producing zero or one iteration depending on the guard.

Set `expected_iterations` at the recipe top level to tell the runtime how many iterations to expect. This enables progress reporting (e.g. progress bars).

```json
{
  "expected_iterations": 100
}
```

## Expressions

Expressions are used by `compute` and `if`.

Supported syntax:

### Variables

| Item | Meaning | Use | Example |
| --- | --- | --- | --- |
| `${name}` | Variable reference. Reads a value from `vars`. | Use recipe variables in `if` and `compute`. | `${target_voltage}` |
| `$ITER` | Global iteration counter, starting at `0`. | Stop conditions, warm-up logic, periodic logic. | `$ITER >= 20` |
| `$TASK_IDX` | Zero-based task index in the `tasks` list. | Task-specific branching in shared expressions. | `$TASK_IDX == 1` |
| `$ELAPSED_MS` | Elapsed runtime in milliseconds since execution start. | Time-based stop/guard conditions. | `$ELAPSED_MS >= 2000` |

### Operators

| Item | Meaning | Use | Example |
| --- | --- | --- | --- |
| `+` | Addition. | Numeric sum. | `${a} + ${b}` |
| `-` | Subtraction. | Numeric difference. | `${measured} - ${target}` |
| `*` | Multiplication. | Numeric product/scaling. | `${voltage} * 0.1` |
| `/` | Division. | Numeric ratio/average. | `${sum} / 2` |
| `>` | Greater-than comparison. | True when left side is greater than right side. | `${temp} > 40` |
| `<` | Less-than comparison. | True when left side is less than right side. | `${delta} < 0.1` |
| `>=` | Greater-than-or-equal comparison. | Inclusive upper/lower bound checks. | `$ITER >= 10` |
| `<=` | Less-than-or-equal comparison. | Inclusive threshold checks. | `$ELAPSED_MS <= 5000` |
| `==` | Equality comparison. | Exact numeric equality checks. | `$TASK_IDX == 0` |
| `!=` | Inequality comparison. | Not-equal checks. | `${state} != 0` |
| `&&` | Logical AND. | True only if both sides are true. | `$ITER > 0 && ${delta} < 0.1` |
| `\|\|` | Logical OR. | True if either side is true. | `$ELAPSED_MS > 2000 || $ITER > 100` |
| `!` | Logical NOT. | Invert a condition. | `!(${ok} == 1)` |
| `( ... )` | Parentheses/grouping. | Control evaluation order. | `(${a} + ${b}) / 2` |

### Functions

| Item | Meaning | Use | Example |
| --- | --- | --- | --- |
| `min(x, y)` | Smaller of two values. | Clamp to an upper limit. | `min(${v}, 10)` |
| `max(x, y)` | Larger of two values. | Clamp to a lower limit. | `max(${v}, 0)` |

All expression math is evaluated as `f64`. A non-zero result is treated as true.

Important distinction:

- Adapter command templates use `{name}` placeholders.
- Recipe expressions use `${name}` variable references.

## Preview And Dry-Run

`--preview` stops after recipe precompilation and prints a summary of:

- Instruments and resource addresses
- Stop conditions
- Pipeline configuration
- Task intervals and step kinds

`--dry-run` performs full scheduling and expression evaluation, but instrument calls are logged instead of written:

```text
[dry-run] psu.json -> VOLT 5,(@1,2)
```

This is useful for validating template rendering and variable flow before touching hardware.

## Sampling Pipeline

Execution uses a dedicated sampler thread plus an SPSC ring buffer so slow sinks do not block instrument sampling.

- The sampler thread performs instrument I/O and produces task-level result frames.
- A worker thread drains the ring buffer and performs slower output work.
- High buffer usage emits warnings.
- Buffer overflow stops scheduling new work and is reported in the final summary.

Pipeline fields:

```json
{
  "pipeline": {
    "mode": "realtime",
    "buffer_size": 8192,
    "warn_usage_percent": 90,
    "file_path": "samples.csv",
    "network_host": "127.0.0.1",
    "network_port": 9000,
    "record": "all"
  }
}
```

Notes:

- `mode` currently selects default behavior such as conservative versus throughput-oriented buffer sizing.
- `buffer_size` is normalized internally to a power of two.
- `warn_usage_percent` must be between 1 and 100.
- `file_path` writes CSV rows using recorded `assign` values.
- `network_host` and `network_port` stream the same task-level frames as JSON over TCP.
- `record` can be `all` or an explicit list of `assign` variable names.
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
operor repl

# Connect to a known resource on startup
operor repl -r USB0::0x0957::0x1798::MY12345678::INSTR
```

## Examples

Bundled examples live in:

- `test-data/adapters/`
- `test-data/recipes/`

Useful starting points:

- `test-data/adapters/psu.json`
- `test-data/adapters/dmm.json`
- `test-data/recipes/r1_set.json`
- `test-data/recipes/r1_set_voltage.json`
- `test-data/recipes/r2_stop_when.json`
