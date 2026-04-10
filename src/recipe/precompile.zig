const std = @import("std");
const doc_parse = @import("../doc_parse.zig");
const Adapter = @import("../adapter/Adapter.zig");
const parse_mod = @import("../adapter/parse.zig");
const testing = @import("../testing.zig");
const config = @import("config.zig");
const diagnostic = @import("diagnostic.zig");
const types = @import("types.zig");
const expr = @import("../expr.zig");
const visa = @import("../visa/root.zig");

const max_recipe_size: usize = 512 * 1024;

const VarsMap = std.StringArrayHashMap(config.ArgScalarDoc);

pub fn precompilePath(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    adapter_dir: std.fs.Dir,
) !types.PrecompiledRecipe {
    return precompilePathInternal(allocator, recipe_path, adapter_dir, null);
}

pub fn precompilePathWithDiagnostic(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: *diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    precompile_diagnostic.reset();
    return precompilePathInternal(allocator, recipe_path, adapter_dir, precompile_diagnostic);
}

fn precompilePathInternal(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    var parse_arena = std.heap.ArenaAllocator.init(allocator);
    defer parse_arena.deinit();

    const recipe_cfg = try doc_parse.parseFilePath(config.RecipeConfig, parse_arena.allocator(), recipe_path, max_recipe_size);

    if (recipe_cfg.pipeline == null) {
        captureDiagnostic(precompile_diagnostic, .{});
        return error.MissingPipeline;
    }

    return try precompileInternal(allocator, &recipe_cfg, adapter_dir, precompile_diagnostic);
}

/// Converts a parsed recipe document into the arena-owned runtime form used by preview and execution.
///
/// Precompile walks the recipe in a strict fail-fast order:
/// 1. Create the arena that will own the returned recipe plus a temporary adapter cache used only during validation.
/// 2. Walk `recipe.instruments`, eagerly load every referenced adapter, assign each instrument a dense `instrument_idx`, and copy it into a `PrecompiledInstrument` with an empty per-instrument command cache.
/// 3. Walk `recipe.tasks`, normalize each interval from `every_ms` or parsed `every`, and allocate the arena-owned `Task` and `Step` arrays.
/// 4. For every step, resolve the referenced instrument and adapter command, compiling that command on first use so runtime only keeps commands this recipe actually calls while binding each command to its owning precompiled instrument.
/// 5. Clone step arguments into the runtime representation, preserving literal types while validating them against the compiled command placeholders, and bind each step directly to the precompiled command pointer it will execute.
/// 6. Parse `stop_when` and return a fully validated `PrecompiledRecipe` whose data is owned by the arena.
///
/// Precompile only validates and reshapes recipe data; it does not perform VISA I/O or talk to hardware.
fn precompileInternal(
    allocator: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime and a temporary adapter cache used only while validating the recipe.
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var adapter_arena = std.heap.ArenaAllocator.init(allocator);
    defer adapter_arena.deinit();

    const vars: VarsMap = recipe.vars orelse VarsMap.init(alloc);
    const initial_values = try buildInitialValues(alloc, &vars);

    // 2. Eagerly load every referenced adapter.
    var loaded_adapters = try loadAdapters(adapter_arena.allocator(), recipe, adapter_dir, precompile_diagnostic);
    defer {
        var it = loaded_adapters.valueIterator();
        while (it.next()) |adapter| adapter.deinit();
    }

    // 3. Compile instrument metadata from loaded adapters.
    var precompiled_instruments = try precompileInstruments(alloc, recipe, &loaded_adapters);

    // 3-5. Normalize tasks and steps, resolving commands and validating arguments.
    var save_as_set = std.StringArrayHashMap(void).init(alloc);
    const tasks = try precompileTasks(alloc, recipe, &vars, &loaded_adapters, &precompiled_instruments, &save_as_set, precompile_diagnostic);

    // 6. Validate and resolve pipeline record configuration.
    const pipeline = try resolvePipelineConfig(alloc, recipe, &vars, &save_as_set, precompile_diagnostic);

    // 7. Assign save_column indices to steps that contribute to recorded frames.
    assignSaveColumns(tasks, &vars, pipeline.record.?.explicit);

    // 8. Return the fully validated arena-owned recipe consumed by preview and execution.
    const stop_when = try parseStopWhen(recipe.stop_when);
    return .{
        .arena = arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = pipeline,
        .stop_when = stop_when,
        .expected_iterations = calculateExpectedIterations(tasks, stop_when),
        .initial_values = initial_values,
    };
}

fn loadAdapters(
    allocator: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !std.StringHashMap(Adapter) {
    var map = std.StringHashMap(Adapter).init(allocator);
    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const cfg = entry.value_ptr.*;
        _ = getOrParseAdapter(allocator, &map, adapter_dir, cfg.adapter) catch |err| {
            captureDiagnostic(precompile_diagnostic, .{
                .instrument_name = entry.key_ptr.*,
                .adapter_name = cfg.adapter,
            });
            return err;
        };
    }
    return map;
}

fn precompileInstruments(
    alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
) !std.StringArrayHashMap(types.PrecompiledInstrument) {
    var precompiled_instruments = std.StringArrayHashMap(types.PrecompiledInstrument).init(alloc);
    try precompiled_instruments.ensureTotalCapacity(recipe.instruments.count());

    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr.*;
        const adapter = loaded_adapters.getPtr(instrument_cfg.adapter).?;

        const name_copy = try alloc.dupe(u8, instrument_name);
        const adapter_copy = try alloc.dupe(u8, instrument_cfg.adapter);
        const resource_copy = try alloc.dupe(u8, instrument_cfg.resource);
        const write_termination = try cloneOptionalBytes(alloc, adapter.write_termination);
        try precompiled_instruments.put(name_copy, .{
            .adapter_name = adapter_copy,
            .resource = resource_copy,
            .commands = std.StringHashMap(*const types.PrecompiledCommand).init(alloc),
            .write_termination = write_termination,
            .options = .{
                .timeout_ms = adapter.options.timeout_ms,
                .read_termination = try cloneOptionalBytes(alloc, adapter.options.read_termination),
                .query_delay_ms = adapter.options.query_delay_ms,
                .chunk_size = adapter.options.chunk_size,
            },
        });
    }
    return precompiled_instruments;
}

fn precompileTasks(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    vars: *const VarsMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMap(types.PrecompiledInstrument),
    save_as_set: *std.StringArrayHashMap(void),
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) ![]types.Task {
    const tasks = try arena_alloc.alloc(types.Task, recipe.tasks.len);
    for (recipe.tasks, 0..) |*task_cfg, task_idx| {
        const every_ms = resolveEveryMs(task_cfg) catch |err| {
            captureDiagnostic(precompile_diagnostic, .{ .task_idx = task_idx });
            return err;
        };

        const steps = try arena_alloc.alloc(types.Step, task_cfg.steps.len);
        for (task_cfg.steps, 0..) |*step_cfg, step_idx| {
            steps[step_idx] = switch (step_cfg.*) {
                .compute => |*cfg| try precompileComputeStep(
                    arena_alloc,
                    vars,
                    save_as_set,
                    cfg,
                    task_idx,
                    step_idx,
                    precompile_diagnostic,
                ),
                .call => |*cfg| try precompileCallStep(
                    arena_alloc,
                    vars,
                    loaded_adapters,
                    precompiled_instruments,
                    save_as_set,
                    cfg,
                    task_idx,
                    step_idx,
                    precompile_diagnostic,
                ),
            };
        }
        tasks[task_idx] = .{ .every_ms = every_ms, .steps = steps };
    }
    return tasks;
}

fn precompileComputeStep(
    arena_alloc: std.mem.Allocator,
    vars: *const VarsMap,
    save_as_set: *std.StringArrayHashMap(void),
    cfg: *const config.ComputeStepConfig,
    task_idx: usize,
    step_idx: usize,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.Step {
    const diag_ctx = diagnostic.DiagnosticContext{
        .task_idx = task_idx,
        .step_idx = step_idx,
    };
    errdefer captureDiagnostic(precompile_diagnostic, diag_ctx);

    const when_expr = try precompileWhen(arena_alloc, vars, cfg.when);

    const save_as = cfg.save_as;
    const save_slot = vars.getIndex(save_as) orelse return error.UndeclaredVariable;
    const save_as_copy = try arena_alloc.dupe(u8, save_as);
    try save_as_set.put(save_as_copy, {});

    var compute_expr = try expr.parse(arena_alloc, cfg.compute);
    try compute_expr.bindVariables(vars);

    return .{
        .action = .{ .compute = .{
            .expression = compute_expr,
            .save_slot = save_slot,
        } },
        .when = when_expr,
    };
}

fn precompileCallStep(
    arena_alloc: std.mem.Allocator,
    vars: *const VarsMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMap(types.PrecompiledInstrument),
    save_as_set: *std.StringArrayHashMap(void),
    cfg: *const config.CallStepConfig,
    task_idx: usize,
    step_idx: usize,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.Step {
    const instrument_name = cfg.instrument;
    var diag_ctx = diagnostic.DiagnosticContext{
        .task_idx = task_idx,
        .step_idx = step_idx,
        .instrument_name = instrument_name,
        .command_name = cfg.call,
    };
    errdefer captureDiagnostic(precompile_diagnostic, diag_ctx);

    const when_expr = try precompileWhen(arena_alloc, vars, cfg.when);

    const precompiled_instrument = precompiled_instruments.getPtr(instrument_name) orelse return error.InstrumentNotFound;

    diag_ctx.adapter_name = precompiled_instrument.adapter_name;
    const loaded_adapter = loaded_adapters.getPtr(precompiled_instrument.adapter_name).?;
    const command = try getOrCompileCommand(arena_alloc, precompiled_instrument, loaded_adapter, cfg.call);

    const call_copy = try arena_alloc.dupe(u8, cfg.call);
    const instrument_copy = try arena_alloc.dupe(u8, instrument_name);
    const compiled_args = try compileStepArgs(arena_alloc, command, cfg.args, vars, &diag_ctx);

    var save_slot: ?usize = null;
    if (cfg.save_as) |label| {
        save_slot = vars.getIndex(label) orelse return error.UndeclaredVariable;
        const duped = try arena_alloc.dupe(u8, label);
        try save_as_set.put(duped, {});
    }

    return .{
        .action = .{ .instrument_call = .{
            .call = call_copy,
            .instrument = instrument_copy,
            .instrument_idx = precompiled_instruments.getIndex(instrument_name).?,
            .command = command,
            .args = compiled_args,
            .save_slot = save_slot,
        } },
        .when = when_expr,
    };
}

fn precompileWhen(
    arena_alloc: std.mem.Allocator,
    vars: *const VarsMap,
    when_src_opt: ?[]const u8,
) !?expr.Expression {
    if (when_src_opt) |when_src| {
        const e = try expr.parse(arena_alloc, when_src);
        var bound = e;
        try bound.bindVariables(vars);
        return bound;
    }
    return null;
}

fn resolvePipelineConfig(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    vars: *const VarsMap,
    save_as_set: *const std.StringArrayHashMap(void),
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PipelineConfig {
    const pipeline_cfg = recipe.pipeline orelse return error.MissingPipeline;
    if (pipeline_cfg.record == null) return error.MissingRecordConfig;
    var pipeline = try clonePipelineConfig(arena_alloc, pipeline_cfg);
    switch (pipeline.record.?) {
        .all => {
            pipeline.record = .{ .explicit = try arena_alloc.dupe([]const u8, save_as_set.keys()) };
        },
        .explicit => |columns| {
            for (columns) |name| {
                if (bindingForName(vars, name) == null) {
                    captureDiagnostic(precompile_diagnostic, .{});
                    return error.UndeclaredVariable;
                }
                if (!save_as_set.contains(name)) {
                    captureDiagnostic(precompile_diagnostic, .{});
                    return error.RecordVariableNotFound;
                }
            }
        },
    }
    return pipeline;
}

fn assignSaveColumns(tasks: []types.Task, vars: *const VarsMap, columns: []const []const u8) void {
    for (tasks) |*task| {
        for (task.steps) |*step| {
            switch (step.action) {
                .instrument_call => |*ic| {
                    ic.save_column = if (ic.save_slot) |slot| slotToColumn(vars, slot, columns) else null;
                },
                .compute => |*comp| {
                    comp.save_column = slotToColumn(vars, comp.save_slot, columns);
                },
            }
        }
    }
}

fn slotToColumn(vars: *const VarsMap, save_slot: usize, columns: []const []const u8) ?usize {
    const name = vars.keys()[save_slot];
    for (columns, 0..) |col_name, col_idx| {
        if (std.mem.eql(u8, col_name, name)) return col_idx;
    }
    return null;
}

/// Estimates the total number of task executions based on stop conditions.
fn calculateExpectedIterations(tasks: []const types.Task, stop: types.StopWhen) ?u64 {
    // If an explicit iteration limit is set, that's our total.
    if (stop.max_iterations) |limit| return limit;

    // If no stop conditions are set, each task runs once by default in the scheduler.
    if (stop.time_elapsed_ms == null) return tasks.len;

    // If only a time limit is set, we skip estimating because it's inaccurate
    // and depends on unknown instrument I/O latency.
    return null;
}

fn captureDiagnostic(
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
    context: diagnostic.DiagnosticContext,
) void {
    if (precompile_diagnostic) |diag| {
        diag.capture(context) catch {};
    }
}

fn cloneOptionalBytes(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    if (bytes.len == 0) return "";
    return allocator.dupe(u8, bytes);
}

fn getOrParseAdapter(
    allocator: std.mem.Allocator,
    loaded_adapters: *std.StringHashMap(Adapter),
    adapter_dir: std.fs.Dir,
    adapter_name: []const u8,
) !*const Adapter {
    if (loaded_adapters.getPtr(adapter_name)) |loaded| return loaded;

    const key = try allocator.dupe(u8, adapter_name);

    var loaded = try parse_mod.parseAdapterInDir(allocator, adapter_dir, adapter_name);
    errdefer loaded.deinit();

    try loaded_adapters.put(key, loaded);
    return loaded_adapters.getPtr(adapter_name).?;
}

fn getOrCompileCommand(
    allocator: std.mem.Allocator,
    instrument: *types.PrecompiledInstrument,
    loaded_adapter: *const Adapter,
    call: []const u8,
) !*const types.PrecompiledCommand {
    if (instrument.commands.get(call)) |command| return command;

    const source = loaded_adapter.commands.get(call) orelse return error.CommandNotFound;
    const key = try allocator.dupe(u8, call);
    const compiled_value = try compileCommand(allocator, source, instrument);

    const compiled = try allocator.create(types.PrecompiledCommand);
    compiled.* = compiled_value;

    try instrument.commands.put(key, compiled);
    return compiled;
}

fn buildInitialValues(
    arena_alloc: std.mem.Allocator,
    vars: *const VarsMap,
) ![]const ?types.Value {
    const initial_values = try arena_alloc.alloc(?types.Value, vars.count());
    for (vars.values(), 0..) |value, idx| {
        initial_values[idx] = try compileInitialValue(arena_alloc, value);
    }
    return initial_values;
}

fn compileCommand(
    allocator: std.mem.Allocator,
    source: Adapter.Command,
    instrument: *const types.PrecompiledInstrument,
) !types.PrecompiledCommand {
    var arg_names = std.ArrayList([]const u8).empty;
    const segments = try allocator.alloc(types.CompiledSegment, source.template.len);

    for (source.template, 0..) |segment, idx| {
        segments[idx] = switch (segment) {
            .literal => |literal| .{ .literal = try allocator.dupe(u8, literal) },
            .placeholder => |placeholder| .{ .arg = blk: {
                if (findArgIndex(arg_names.items, placeholder.name)) |arg_idx| break :blk arg_idx;
                const name_copy = try allocator.dupe(u8, placeholder.name);
                try arg_names.append(allocator, name_copy);
                break :blk arg_names.items.len - 1;
            } },
        };
    }

    return .{
        .instrument = instrument,
        .response = source.response,
        .segments = segments,
        .arg_names = try arg_names.toOwnedSlice(allocator),
    };
}

fn compileStepArgs(
    allocator: std.mem.Allocator,
    command: *const types.PrecompiledCommand,
    doc_args: ?std.StringHashMap(config.ArgValueDoc),
    vars: *const VarsMap,
    diag_ctx: *diagnostic.DiagnosticContext,
) ![]types.StepArg {
    const args = try allocator.alloc(types.StepArg, command.arg_names.len);

    for (command.arg_names, 0..) |arg_name, idx| {
        const doc_arg = if (doc_args) |map|
            map.get(arg_name) orelse {
                diag_ctx.argument_name = arg_name;
                return error.MissingCommandArgument;
            }
        else {
            diag_ctx.argument_name = arg_name;
            return error.MissingCommandArgument;
        };
        args[idx] = try compileArg(allocator, doc_arg, vars);
    }

    if (doc_args) |map| {
        var it = map.iterator();
        while (it.next()) |entry| {
            if (!command.hasPlaceholder(entry.key_ptr.*)) {
                diag_ctx.argument_name = entry.key_ptr.*;
                return error.UnexpectedCommandArgument;
            }
        }
    }

    return args;
}

fn compileInitialValue(allocator: std.mem.Allocator, value: config.ArgScalarDoc) !types.Value {
    return switch (value) {
        .string => |text| .{ .string = try allocator.dupe(u8, text) },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}

fn resolveEveryMs(task: *const config.TaskConfig) !u64 {
    if (task.every_ms) |ms| return ms;
    if (task.every) |text| return try parseDurationMs(text);
    return error.MissingTaskInterval;
}

fn clonePipelineConfig(allocator: std.mem.Allocator, cfg: config.PipelineConfig) !types.PipelineConfig {
    if (cfg.buffer_size) |size| {
        if (size == 0) return error.InvalidPipelineConfig;
    }
    if (cfg.warn_usage_percent) |percent| {
        if (percent == 0 or percent > 100) return error.InvalidPipelineConfig;
    }
    const has_network_host = cfg.network_host != null;
    const has_network_port = cfg.network_port != null;
    if (has_network_host != has_network_port) return error.InvalidPipelineConfig;
    if (cfg.network_port) |port| {
        if (port == 0) return error.InvalidPipelineConfig;
    }

    const record_copy: ?types.RecordConfig = if (cfg.record) |record| switch (record) {
        .all => |value| .{ .all = try allocator.dupe(u8, value) },
        .explicit => |columns| blk: {
            const items = try allocator.alloc([]const u8, columns.len);
            for (columns, 0..) |name, idx| {
                items[idx] = try allocator.dupe(u8, name);
            }
            break :blk .{ .explicit = items };
        },
    } else null;

    return .{
        .buffer_size = cfg.buffer_size,
        .warn_usage_percent = cfg.warn_usage_percent,
        .mode = cfg.mode,
        .file_path = if (cfg.file_path) |path| try allocator.dupe(u8, path) else null,
        .network_host = if (cfg.network_host) |host| try allocator.dupe(u8, host) else null,
        .network_port = cfg.network_port,
        .record = record_copy,
    };
}

fn parseStopWhen(stop: ?config.StopWhenConfig) !types.StopWhen {
    if (stop == null) return .{};

    const cfg = stop.?;
    return .{
        .time_elapsed_ms = if (cfg.time_elapsed) |value| try parseDurationMs(value) else null,
        .max_iterations = cfg.max_iterations,
    };
}

fn parseDurationMs(input: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, input, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidDuration;

    var suffix_start: usize = trimmed.len;
    while (suffix_start > 0 and std.ascii.isAlphabetic(trimmed[suffix_start - 1])) : (suffix_start -= 1) {}

    const number_part = trimmed[0..suffix_start];
    const suffix = trimmed[suffix_start..];
    if (number_part.len == 0) return error.InvalidDuration;

    const value = try std.fmt.parseInt(u64, number_part, 10);
    if (suffix.len == 0 or std.mem.eql(u8, suffix, "ms")) return value;
    if (std.mem.eql(u8, suffix, "s")) return value * 1000;
    if (std.mem.eql(u8, suffix, "m")) return value * 60 * 1000;
    return error.InvalidDuration;
}

fn bindingForName(vars: *const VarsMap, name: []const u8) ?expr.VariableBinding {
    if (std.mem.eql(u8, name, "$ITER")) return .{ .builtin = .iter };
    if (std.mem.eql(u8, name, "$TASK_IDX")) return .{ .builtin = .task_idx };
    const slot = vars.getIndex(name) orelse return null;
    return .{ .slot = slot };
}

fn compileArg(
    allocator: std.mem.Allocator,
    doc_arg: config.ArgValueDoc,
    vars: *const VarsMap,
) !types.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try compileArgScalar(allocator, scalar, vars) },
        .list => |items| blk: {
            const out = try allocator.alloc(types.CompiledArgValue, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileArgScalar(allocator, item, vars);
            }
            break :blk .{ .list = out };
        },
    };
}

fn compileArgScalar(
    allocator: std.mem.Allocator,
    value: config.ArgScalarDoc,
    vars: *const VarsMap,
) !types.CompiledArgValue {
    return switch (value) {
        .string => |text| blk: {
            if (referenceName(text)) |name| {
                const binding = bindingForName(vars, name) orelse return error.UndeclaredVariable;
                break :blk .{ .binding = binding };
            }
            break :blk .{ .const_value = .{ .string = try allocator.dupe(u8, text) } };
        },
        .int => |number| .{ .const_value = .{ .int = number } },
        .float => |number| .{ .const_value = .{ .float = number } },
        .bool => |flag| .{ .const_value = .{ .bool = flag } },
    };
}

fn findArgIndex(arg_names: []const []const u8, name: []const u8) ?usize {
    for (arg_names, 0..) |arg_name, idx| {
        if (std.mem.eql(u8, arg_name, name)) return idx;
    }
    return null;
}

fn referenceName(text: []const u8) ?[]const u8 {
    if (text.len >= 4 and std.mem.startsWith(u8, text, "${") and std.mem.endsWith(u8, text, "}")) {
        return text[2 .. text.len - 1];
    }
    return null;
}

test "load recipe and adapters" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r1_set.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\tasks:
        \\  - every: 100ms
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    const instrument = compiled.instruments.getPtr("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.eql(u8, instrument.resource, "USB0::1::INSTR"));
    try std.testing.expect(std.mem.eql(u8, instrument.adapter_name, "psu.toml"));
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());

    const command = instrument.commands.get("set") orelse return error.TestUnexpectedResult;
    try std.testing.expect(command.instrument == instrument);
    try std.testing.expect(command.response == null);
    try std.testing.expectEqual(@as(usize, 1), command.arg_names.len);
    try std.testing.expect(std.mem.eql(u8, command.arg_names[0], "voltage"));

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expectEqual(@as(u64, 100), compiled.tasks[0].every_ms);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps.len);
    const step0 = compiled.tasks[0].steps[0].action.instrument_call;
    try std.testing.expect(std.mem.eql(u8, step0.call, "set"));
    try std.testing.expect(step0.command == command);

    const voltage = step0.args[command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |value| switch (value) {
            .const_value => |text| switch (text) {
                .string => |s| try std.testing.expectEqualStrings("5", s),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "parse durations and stop conditions" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r2_stop_when.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every: 250ms
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args:
        \\          voltage: "5"
        \\stop_when:
        \\  time_elapsed: 2s
        \\  max_iterations: 3
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r2_stop_when.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(u64, 250), compiled.tasks[0].every_ms);

    const step_args = compiled.tasks[0].steps[0].action.instrument_call;
    const voltage = step_args.args[step_args.command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |value| switch (value) {
            .const_value => |text| switch (text) {
                .string => |s| try std.testing.expectEqualStrings("5", s),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    try std.testing.expectEqual(@as(?u64, 2000), compiled.stop_when.time_elapsed_ms);
    try std.testing.expectEqual(@as(?u64, 3), compiled.stop_when.max_iterations);
    try std.testing.expectEqual(@as(?u64, 3), compiled.expected_iterations);
}

test "precompile preserves initial variables" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/initial_vars.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v_set: 1.0
        \\  name: scan
        \\tasks: []
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/initial_vars.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.initial_values.len);
    var found_float = false;
    var found_string = false;
    for (compiled.initial_values) |val_opt| {
        const val = val_opt orelse continue;
        switch (val) {
            .float => |number| {
                try std.testing.expectEqual(@as(f64, 1.0), number);
                found_float = true;
            },
            .string => |text| {
                try std.testing.expectEqualStrings("scan", text);
                found_string = true;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(found_float);
    try std.testing.expect(found_string);
}

test "precompile estimates iterations for run-once recipes" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V"
    );
    try workspace.writeFile("recipes/run_once.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args: {}
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args: {}
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/run_once.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    // Default run-once behavior: each task runs once.
    try std.testing.expectEqual(@as(?u64, 2), compiled.expected_iterations);
}

test "precompile marks iterations unknown for time-limited infinite loops" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V"
    );
    try workspace.writeFile("recipes/time_limited.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - every_ms: 10
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args: {}
        \\stop_when:
        \\  time_elapsed: 1s
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/time_limited.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    // Iterations unknown because we skip time-based estimation.
    try std.testing.expectEqual(@as(?u64, null), compiled.expected_iterations);
}

test "precompile preserves typed literal step arguments" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/cfg.toml",
        \\[metadata]
        \\
        \\[commands.configure]
        \\write = "CONF {count} {voltage} {enabled} {channels} {mirror}"
    );
    try workspace.writeFile("recipes/typed_args.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: cfg.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  target: mir
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: configure
        \\        instrument: d1
        \\        args:
        \\          count: 5
        \\          voltage: 1.25
        \\          enabled: true
        \\          channels:
        \\            - 1
        \\            - 2
        \\          mirror: "${target}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/typed_args.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    const args = compiled.tasks[0].steps[0].action.instrument_call.args;

    const command = compiled.tasks[0].steps[0].action.instrument_call.command;

    const count = args[command.argIndex("count").?];
    switch (count) {
        .scalar => |value| switch (value) {
            .const_value => |number| switch (number) {
                .int => |n| try std.testing.expectEqual(@as(i64, 5), n),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const voltage = args[command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |value| switch (value) {
            .const_value => |number| switch (number) {
                .float => |n| try std.testing.expectApproxEqAbs(@as(f64, 1.25), n, 1e-9),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const enabled = args[command.argIndex("enabled").?];
    switch (enabled) {
        .scalar => |value| switch (value) {
            .const_value => |flag| switch (flag) {
                .bool => |b| try std.testing.expect(b),
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const channels = args[command.argIndex("channels").?];
    switch (channels) {
        .scalar => return error.TestUnexpectedResult,
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len);
            switch (items[0]) {
                .const_value => |number| switch (number) {
                    .int => |n| try std.testing.expectEqual(@as(i64, 1), n),
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
            switch (items[1]) {
                .const_value => |number| switch (number) {
                    .int => |n| try std.testing.expectEqual(@as(i64, 2), n),
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
    }

    const mirror = args[command.argIndex("mirror").?];
    switch (mirror) {
        .scalar => |value| switch (value) {
            .binding => |binding| switch (binding) {
                .slot => |slot| try std.testing.expect(slot < compiled.initial_values.len),
                .builtin => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

const vendor_psu_adapter =
    \\[metadata]
    \\
    \\[commands.set_voltage]
    \\write = "VOLT {voltage},(@{channels})"
;

test "precompile stores only referenced commands" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml",
        \\[metadata]
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage}"
        \\
        \\[commands.output_on]
        \\write = "OUTP ON"
    );
    try workspace.writeFile("recipes/r1_set_voltage.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    const instrument = compiled.instruments.get("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());
    try std.testing.expect(instrument.commands.contains("set_voltage"));
    try std.testing.expect(!instrument.commands.contains("output_on"));
}

test "precompile rejects missing instrument references" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_instrument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: missing
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_instrument.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.InstrumentNotFound, precompilePath(gpa, recipe_path, dir));
}

test "precompile validates command arguments" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
    );
    try workspace.writeFile("recipes/unexpected_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\          channel: 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const missing_argument_path = try workspace.realpathAlloc("recipes/missing_argument.yaml");
    defer gpa.free(missing_argument_path);
    const unexpected_argument_path = try workspace.realpathAlloc("recipes/unexpected_argument.yaml");
    defer gpa.free(unexpected_argument_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.MissingCommandArgument, precompilePath(gpa, missing_argument_path, dir));
    try std.testing.expectError(error.UnexpectedCommandArgument, precompilePath(gpa, unexpected_argument_path, dir));
}

test "precompiled command renders via helper" {
    const gpa = std.testing.allocator;

    const source = try Adapter.Command.parse(gpa, "VOLT {voltage}", null);
    defer source.deinit(gpa);

    var instrument = types.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(gpa),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    try std.testing.expect(compiled.instrument == &instrument);
    try std.testing.expectEqual(@as(usize, 1), compiled.arg_names.len);
    try std.testing.expectEqualStrings("voltage", compiled.arg_names[0]);

    const args = [_]types.RenderValue{
        .{ .scalar = .{ .float = 3.3 } },
    };

    var stack_buf: [32]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("VOLT 3.3\n", rendered.bytes);
    try std.testing.expect(rendered.owned == null);
}

test "precompiled command render falls back to heap when suffix leaves too little stack space" {
    const gpa = std.testing.allocator;

    const source = try Adapter.Command.parse(gpa, "VOLT {voltage}", null);
    defer source.deinit(gpa);

    var instrument = types.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(gpa),
        .write_termination = "\r\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    const args = [_]types.RenderValue{
        .{ .scalar = .{ .string = "1234567890" } },
    };

    var stack_buf: [8]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expect(rendered.owned != null);
    try std.testing.expectEqualStrings("VOLT 1234567890\r\n", rendered.bytes);
}

test "precompile diagnostic includes step context" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_command.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: missing
        \\        instrument: d1
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_command.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.CommandNotFound, err);

        var out = std.Io.Writer.Allocating.init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "task 0 step 0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "instrument=d1"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "adapter=psu0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "command=missing"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile compute step" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 0
        \\  doubled: 0
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "5"
        \\          channels: "1"
        \\        save_as: v
        \\      - compute: "${v} * 2"
        \\        save_as: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.tasks[0].steps.len);

    // First step: instrument call
    switch (compiled.tasks[0].steps[0].action) {
        .instrument_call => |ic| try std.testing.expectEqualStrings("set_voltage", ic.call),
        .compute => return error.TestUnexpectedResult,
    }

    // Second step: compute
    switch (compiled.tasks[0].steps[1].action) {
        .compute => |comp| {
            try std.testing.expect(comp.save_column != null);
        },
        .instrument_call => return error.TestUnexpectedResult,
    }
}

test "precompile compute step rejects missing save_as" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute_no_save.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - compute: 1 + 2
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute_no_save.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    // With the union-based StepConfig, missing required fields (save_as) results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, dir));
}

test "precompile step with when guard" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/when_guard.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  power: 0
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "5"
        \\          channels: "1"
        \\        when: "${power} > 100"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/when_guard.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    try std.testing.expect(compiled.tasks[0].steps[0].when != null);
}

test "precompile rejects invalid step (neither call nor compute)" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/invalid_step.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - save_as: orphan
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/invalid_step.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    // With the union-based StepConfig, an object matching neither variant results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, dir));
}

test "precompile rejects record with unknown save_as variable" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/bad_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record:
        \\    - voltage
        \\    - nonexistent
        \\vars:
        \\  voltage: 0
        \\  nonexistent: 0
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\            - 2
        \\        save_as: voltage
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/bad_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.RecordVariableNotFound, precompilePath(gpa, recipe_path, dir));
}

test "precompile accepts valid record subset" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_ok.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record:
        \\    - voltage
        \\vars:
        \\  voltage: 0
        \\  doubled: 0
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\            - 2
        \\        save_as: voltage
        \\      - compute: "${voltage} * 2"
        \\        save_as: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_ok.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    switch (compiled.pipeline.record.?) {
        .explicit => |columns| {
            try std.testing.expectEqual(@as(usize, 1), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
        },
        .all => return error.TestUnexpectedResult,
    }
}

test "precompile diagnostic for missing pipeline" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_pipeline.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: 1.0
        \\          channels:
        \\            - 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_pipeline.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.MissingPipeline, err);

        var out = std.Io.Writer.Allocating.init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'pipeline'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile diagnostic for missing record" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline: {}
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: 1.0
        \\          channels:
        \\            - 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.MissingRecordConfig, err);

        var out = std.Io.Writer.Allocating.init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'record'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile expands record all into explicit save_as list" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_all.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\  doubled: 0
        \\tasks:
        \\  - every_ms: 100
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: 5
        \\          channels:
        \\            - 1
        \\        save_as: voltage
        \\      - compute: "${voltage} * 2"
        \\        save_as: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_all.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir);
    defer compiled.deinit();

    const record = compiled.pipeline.record orelse return error.TestUnexpectedResult;
    switch (record) {
        .explicit => |columns| {
            try std.testing.expectEqual(@as(usize, 2), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
            try std.testing.expectEqualStrings("doubled", columns[1]);
        },
        .all => return error.TestUnexpectedResult,
    }
}

test "precompile rejects undeclared variable use" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V {voltage}"
    );
    try workspace.writeFile("recipes/undeclared.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 1
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set
        \\        instrument: d1
        \\        args:
        \\          voltage: "5"
        \\        save_as: undeclared_var
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, dir));
}

test "precompile rejects undeclared variable in expression" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/undeclared_expr.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 1
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - compute: "${v} + ${x}"
        \\        save_as: v
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared_expr.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, dir));
}
