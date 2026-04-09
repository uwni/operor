# Ordo

A CI Engine for excute experiment plan on your instruments.

## Usage

Run the `run` subcommand with a driver directory and recipe path:

```sh
zig build run -- run <driver_dir> <recipe> [--preview] [--dry-run] [--duration-ms <ms>]
```

`--preview` validates referenced drivers, instruments, commands, and command arguments without opening VISA sessions.

## Sampling Pipeline

Execution now uses a dedicated sampler thread plus an SPSC ring buffer boundary so slow sinks never block device sampling.

- The sampler only performs instrument I/O, captures task-level result frames, and pushes them into the ring buffer.
- A worker thread drains the buffer and performs slow console, file, and network writes.
- When the buffer fills, the executor emits a warning, stops scheduling new work, and reports frame buffer overflow counts plus buffer high-water marks.

Recipe documents can optionally tune the pipeline:

```json
{
	"pipeline": {
		"mode": "realtime",
		"buffer_size": 8192,
		"warn_usage_percent": 90,
		"file_path": "samples.csv",
		"network_host": "127.0.0.1",
		"network_port": 9000
	}
}
```

Notes:

- `buffer_size` is optional and normalized to a power of two with sane defaults.
- `mode` currently mainly affects pipeline defaults such as buffer sizing; `realtime` favors throughput, while `safe` keeps more conservative defaults.
- Relative sink paths are resolved from the recipe file directory.
- `file_path` writes one CSV row per task iteration frame. The columns come from the recipe's `save_as` labels, so values captured in the same task iteration are persisted in the same row.
- `network_host` and `network_port` stream the same task-level result frames as self-contained JSON.
- Runtime warnings report high buffer usage and frame buffer overflows; execution ends with a summary of capacity, max usage, current usage ratio, and overflows.

## Command Lifecycle

Each driver command moves through three explicit representations:

| Stage | Input | Processing | Output |
| --- | --- | --- | --- |
| Driver parse | Driver document `commands.<name>.write` plus optional `read` | Parse the template string into literal/placeholder segments and resolve the optional response encoding. | `driver.Command` |
| Recipe precompile | `driver.Command` referenced by a recipe step | Clone the parsed template into recipe-owned memory, collect placeholder names, validate step arguments, bind each precompiled command to its owning instrument, and bind each step directly to the precompiled command it will execute. Only commands actually used by the recipe are kept. | `recipe.PrecompiledCommand` |
| Step execution | Bound `recipe.PrecompiledCommand` plus resolved step values | Resolve `${name}` references from runtime context, join list arguments, render the template into concrete bytes, and fall back to a heap buffer if the stack buffer is too small. | `recipe.RenderedCommand` |

In short, the command path is:

```text
driver.Command -> recipe.PrecompiledCommand -> recipe.RenderedCommand
```

What each stage means in practice:

1. Driver parse happens when a driver file is loaded. At this point the command is still a reusable driver definition: it knows the parsed template structure and optional response encoding, but it is not tied to any recipe step yet.
2. Recipe precompile happens before preview or execution starts. This is where a recipe step selects a driver command, the system copies it into recipe-owned memory, records the placeholders it expects, validates the step arguments against that placeholder list, stores the owning instrument pointer on that `PrecompiledCommand`, and binds each step directly to the resolved precompiled command pointer. `--preview` stops at this stage.
3. Step execution happens when the scheduler runs a step. The executor resolves runtime values, starts from `step.command`, recovers the owning instrument through `command.instrument`, calls `PrecompiledCommand.render`, and gets a `RenderedCommand` whose `bytes` can be sent directly to VISA. `--dry-run` stops after rendering and logs the rendered bytes instead of writing them to the instrument.
4. If the command declares a response encoding, the executor reads the instrument response after writing and parses it according to the encoding stored on the `PrecompiledCommand`.

Open an interactive VISA REPL against a single resource:

```sh
zig build run -- repl <resource>
```

## Driver I/O Options

Driver metadata can tune the VISA session opened for each recipe instrument:

```json
{
  "metadata": {
    "name": "scope",
    "write_termination": "\n",
    "read_termination": "\n",
    "timeout_ms": 2500,
    "query_delay_ms": 25,
    "chunk_size": 4096
  }
}
```

Notes:

- `write_termination` is the explicit write-side suffix appended before the VISA write.
- `read_termination` is removed from owned read buffers when present at the end of the response.
- `query_delay_ms` inserts a delay between a write and the following read for commands that expect a response.
- `chunk_size` controls the temporary buffer used while collecting multi-chunk responses.

Inside the REPL you can use:

```text
write <command>
read
query <command>
```

List all VISA instrument resources:

```sh
zig build run -- instrument list
```

# Expression
`{name[:format]}`

# Todo List
[] Exception Handle: Not throw the errors instead of give a comprehensive text
[] Github CI Config