const std = @import("std");
const doc_parse = @import("../doc_parse.zig");
const Driver = @import("../driver/Driver.zig");
const DriverRegistry = @import("../driver/DriverRegistry.zig");
const testing = @import("../testing.zig");
const config = @import("config.zig");
const diagnostic = @import("diagnostic.zig");
const types = @import("types.zig");
const expr = @import("../expr.zig");
const visa = @import("../visa/root.zig");

const max_recipe_size: usize = 512 * 1024;

pub fn precompilePath(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    driver_reg: *DriverRegistry,
) !types.PrecompiledRecipe {
    return precompilePathInternal(allocator, recipe_path, driver_reg, null);
}

pub fn precompilePathWithDiagnostic(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    driver_reg: *DriverRegistry,
    precompile_diagnostic: *diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    precompile_diagnostic.reset();
    return precompilePathInternal(allocator, recipe_path, driver_reg, precompile_diagnostic);
}

fn precompilePathInternal(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    driver_reg: *DriverRegistry,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    var parse_arena = std.heap.ArenaAllocator.init(allocator);
    defer parse_arena.deinit();

    const recipe_cfg = try doc_parse.parseFilePath(config.RecipeConfig, parse_arena.allocator(), recipe_path, max_recipe_size);

    if (recipe_cfg.pipeline == null) {
        captureDiagnostic(precompile_diagnostic, .{});
        return error.MissingPipeline;
    }

    return try precompileInternal(allocator, &recipe_cfg, driver_reg, precompile_diagnostic);
}

/// Converts a parsed recipe document into the arena-owned runtime form used by preview and execution.
///
/// Precompile walks the recipe in a strict fail-fast order:
/// 1. Create the arena that will own the returned recipe plus a temporary driver cache used only during validation.
/// 2. Walk `recipe.instruments`, eagerly load every referenced driver, assign each instrument a dense `instrument_idx`, and copy it into a `PrecompiledInstrument` with an empty per-instrument command cache.
/// 3. Walk `recipe.tasks`, normalize each interval from `every_ms` or parsed `every`, and allocate the arena-owned `Task` and `Step` arrays.
/// 4. For every step, resolve the referenced instrument and driver command, compiling that command on first use so runtime only keeps commands this recipe actually calls while binding each command to its owning precompiled instrument.
/// 5. Clone step arguments into the runtime representation, preserving literal types while validating them against the compiled command placeholders, and bind each step directly to the precompiled command pointer it will execute.
/// 6. Parse `stop_when` and return a fully validated `PrecompiledRecipe` whose data is owned by the arena.
///
/// Precompile only validates and reshapes recipe data; it does not perform VISA I/O or talk to hardware.
fn precompileInternal(
    allocator: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    driver_reg: *DriverRegistry,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime and a temporary driver cache used only while validating the recipe.
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var loaded_drivers = std.StringHashMap(Driver).init(allocator);
    defer deinitLoadedDrivers(allocator, &loaded_drivers);

    // 2. Pre-register every instrument and load drivers.
    var precompiled_instruments = try precompileInstruments(allocator, alloc, recipe, driver_reg, &loaded_drivers, precompile_diagnostic);

    // 3-5. Normalize tasks and steps, resolving commands and validating arguments.
    var save_as_set = std.StringArrayHashMap(void).init(alloc);
    const tasks = try precompileTasks(alloc, recipe, &loaded_drivers, &precompiled_instruments, &save_as_set, precompile_diagnostic);

    // 6. Validate and resolve pipeline record configuration.
    const pipeline = try resolvePipelineConfig(alloc, recipe, &save_as_set, precompile_diagnostic);

    // 7. Parse initial variables if any.
    const initial_vars = try parseInitialVars(alloc, recipe);

    // 8. Return the fully validated arena-owned recipe consumed by preview and execution.
    const stop_when = try parseStopWhen(recipe.stop_when);
    return .{
        .arena = arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = pipeline,
        .stop_when = stop_when,
        .expected_iterations = calculateExpectedIterations(tasks, stop_when),
        .initial_vars = initial_vars,
    };
}

fn precompileInstruments(
    allocator: std.mem.Allocator,
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    driver_reg: *DriverRegistry,
    loaded_drivers: *std.StringHashMap(Driver),
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !std.StringHashMap(types.PrecompiledInstrument) {
    var precompiled_instruments = std.StringHashMap(types.PrecompiledInstrument).init(arena_alloc);
    try precompiled_instruments.ensureTotalCapacity(recipe.instruments.count());

    var instrument_it = recipe.instruments.iterator();
    var next_instrument_idx: usize = 0;
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr.*;

        const diag_ctx = diagnostic.DiagnosticContext{
            .instrument_name = instrument_name,
            .driver_name = instrument_cfg.driver,
        };
        const driver = getOrParseDriver(allocator, loaded_drivers, driver_reg, instrument_cfg.driver) catch |err| {
            captureDiagnostic(precompile_diagnostic, diag_ctx);
            return err;
        };

        const name_copy = try arena_alloc.dupe(u8, instrument_name);
        const driver_copy = try arena_alloc.dupe(u8, instrument_cfg.driver);
        const resource_copy = try arena_alloc.dupe(u8, instrument_cfg.resource);
        const write_termination = try cloneOptionalBytes(arena_alloc, driver.write_termination);
        try precompiled_instruments.put(name_copy, .{
            .instrument_idx = next_instrument_idx,
            .driver_name = driver_copy,
            .resource = resource_copy,
            .commands = std.StringHashMap(*const types.PrecompiledCommand).init(arena_alloc),
            .write_termination = write_termination,
            .options = .{
                .timeout_ms = driver.options.timeout_ms,
                .read_termination = try cloneOptionalBytes(arena_alloc, driver.options.read_termination),
                .query_delay_ms = driver.options.query_delay_ms,
                .chunk_size = driver.options.chunk_size,
            },
        });
        next_instrument_idx += 1;
    }
    return precompiled_instruments;
}

fn precompileTasks(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_drivers: *const std.StringHashMap(Driver),
    precompiled_instruments: *std.StringHashMap(types.PrecompiledInstrument),
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
                    recipe,
                    save_as_set,
                    cfg,
                    task_idx,
                    step_idx,
                    precompile_diagnostic,
                ),
                .call => |*cfg| try precompileCallStep(
                    arena_alloc,
                    recipe,
                    loaded_drivers,
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
    recipe: *const config.RecipeConfig,
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

    const when_expr = try precompileWhen(arena_alloc, recipe, cfg.when);

    const save_as = cfg.save_as;
    if (!isDeclared(recipe.vars, save_as)) return error.UndeclaredVariable;
    const save_as_copy = try arena_alloc.dupe(u8, save_as);
    try save_as_set.put(save_as_copy, {});

    const compute_expr = try expr.parse(arena_alloc, cfg.compute);
    var it = compute_expr.variables();
    while (it.next()) |name| {
        if (!isDeclared(recipe.vars, name)) return error.UndeclaredVariable;
    }

    return .{
        .action = .{ .compute = .{
            .expression = compute_expr,
            .save_as = save_as_copy,
        } },
        .when = when_expr,
    };
}

fn precompileCallStep(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_drivers: *const std.StringHashMap(Driver),
    precompiled_instruments: *std.StringHashMap(types.PrecompiledInstrument),
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

    const when_expr = try precompileWhen(arena_alloc, recipe, cfg.when);

    const precompiled_instrument = precompiled_instruments.getPtr(instrument_name) orelse return error.InstrumentNotFound;

    diag_ctx.driver_name = precompiled_instrument.driver_name;
    const loaded_driver = loaded_drivers.getPtr(precompiled_instrument.driver_name).?;
    const command = try getOrCompileCommand(arena_alloc, precompiled_instrument, loaded_driver, cfg.call);

    const call_copy = try arena_alloc.dupe(u8, cfg.call);
    const instrument_copy = try arena_alloc.dupe(u8, instrument_name);
    var args_map = std.StringHashMap(types.StepArg).init(arena_alloc);
    if (cfg.args) |args| {
        var it = args.iterator();
        while (it.next()) |entry| {
            diag_ctx.argument_name = entry.key_ptr.*;
            const value_copy = try cloneAndValidateArg(arena_alloc, entry.value_ptr.*, recipe.vars);
            const key_copy = try arena_alloc.dupe(u8, entry.key_ptr.*);
            try args_map.put(key_copy, value_copy);
        }
    }
    diag_ctx.argument_name = null;
    try validateStepArgs(command, &args_map, &diag_ctx);

    const save_as_copy = if (cfg.save_as) |label| blk: {
        if (!isDeclared(recipe.vars, label)) return error.UndeclaredVariable;
        const duped = try arena_alloc.dupe(u8, label);
        try save_as_set.put(duped, {});
        break :blk duped;
    } else null;

    return .{
        .action = .{ .instrument_call = .{
            .call = call_copy,
            .instrument = instrument_copy,
            .command = command,
            .args = args_map,
            .save_as = save_as_copy,
        } },
        .when = when_expr,
    };
}

fn precompileWhen(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    when_src_opt: ?[]const u8,
) !?expr.Expression {
    if (when_src_opt) |when_src| {
        const e = try expr.parse(arena_alloc, when_src);
        var it = e.variables();
        while (it.next()) |name| {
            if (!isDeclared(recipe.vars, name)) return error.UndeclaredVariable;
        }
        return e;
    }
    return null;
}

fn resolvePipelineConfig(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
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
                if (!isDeclared(recipe.vars, name)) {
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

fn parseInitialVars(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
) !std.StringHashMap(types.StepScalar) {
    var initial_vars = std.StringHashMap(types.StepScalar).init(arena_alloc);
    if (recipe.vars) |vars| {
        try initial_vars.ensureTotalCapacity(vars.count());
        var var_it = vars.iterator();
        while (var_it.next()) |entry| {
            const key = try arena_alloc.dupe(u8, entry.key_ptr.*);
            const value = try cloneInitialVar(arena_alloc, entry.value_ptr.*);
            try initial_vars.put(key, value);
        }
    }
    return initial_vars;
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

fn deinitLoadedDrivers(allocator: std.mem.Allocator, loaded_drivers: *std.StringHashMap(Driver)) void {
    var it = loaded_drivers.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    loaded_drivers.deinit();
}

fn cloneOptionalBytes(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    if (bytes.len == 0) return "";
    return allocator.dupe(u8, bytes);
}

fn getOrParseDriver(
    allocator: std.mem.Allocator,
    loaded_drivers: *std.StringHashMap(Driver),
    driver_reg: *DriverRegistry,
    driver_name: []const u8,
) !*const Driver {
    if (loaded_drivers.getPtr(driver_name)) |loaded| return loaded;

    const key = try allocator.dupe(u8, driver_name);
    errdefer allocator.free(key);

    var loaded = driver_reg.parseDriverByName(allocator, driver_name) catch |err| switch (err) {
        error.DriverNotFound => blk: {
            try driver_reg.rebuild();
            break :blk try driver_reg.parseDriverByName(allocator, driver_name);
        },
        else => return err,
    };
    errdefer loaded.deinit();

    try loaded_drivers.put(key, loaded);
    return loaded_drivers.getPtr(driver_name).?;
}

fn getOrCompileCommand(
    allocator: std.mem.Allocator,
    instrument: *types.PrecompiledInstrument,
    loaded_driver: *const Driver,
    call: []const u8,
) !*const types.PrecompiledCommand {
    if (instrument.commands.get(call)) |command| return command;

    const source = loaded_driver.commands.get(call) orelse return error.CommandNotFound;
    const key = try allocator.dupe(u8, call);
    const compiled_value = try compileCommand(allocator, source, instrument);

    const compiled = try allocator.create(types.PrecompiledCommand);
    compiled.* = compiled_value;

    try instrument.commands.put(key, compiled);
    return compiled;
}

fn compileCommand(
    allocator: std.mem.Allocator,
    source: Driver.Command,
    instrument: *const types.PrecompiledInstrument,
) !types.PrecompiledCommand {
    const command = try source.clone(allocator);
    const placeholders = try command.placeholderNames(allocator);

    return .{
        .instrument = instrument,
        .response = command.response,
        .template = command.template,
        .placeholders = placeholders,
    };
}

fn validateStepArgs(
    command: *const types.PrecompiledCommand,
    args_map: *const std.StringHashMap(types.StepArg),
    diag_ctx: *diagnostic.DiagnosticContext,
) !void {
    for (command.placeholders) |placeholder| {
        if (!args_map.contains(placeholder)) {
            diag_ctx.argument_name = placeholder;
            return error.MissingCommandArgument;
        }
    }

    var it = args_map.iterator();
    while (it.next()) |entry| {
        if (!command.hasPlaceholder(entry.key_ptr.*)) {
            diag_ctx.argument_name = entry.key_ptr.*;
            return error.UnexpectedCommandArgument;
        }
    }
}

fn cloneInitialVar(allocator: std.mem.Allocator, value: config.ArgScalarDoc) !types.StepScalar {
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

fn isDeclared(vars: ?std.StringHashMap(config.ArgScalarDoc), name: []const u8) bool {
    // Built-in variables are always allowed.
    if (std.mem.eql(u8, name, "$ITER")) return true;
    if (std.mem.eql(u8, name, "$TASK_IDX")) return true;
    if (vars) |v| return v.contains(name);
    return false;
}

fn cloneAndValidateArg(
    allocator: std.mem.Allocator,
    doc_arg: config.ArgValueDoc,
    vars: ?std.StringHashMap(config.ArgScalarDoc),
) !types.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try cloneAndValidateArgScalar(allocator, scalar, vars) },
        .list => |items| blk: {
            const out = try allocator.alloc(types.StepScalar, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try cloneAndValidateArgScalar(allocator, item, vars);
            }
            break :blk .{ .list = out };
        },
    };
}

fn cloneAndValidateArgScalar(
    allocator: std.mem.Allocator,
    value: config.ArgScalarDoc,
    vars: ?std.StringHashMap(config.ArgScalarDoc),
) !types.StepScalar {
    return switch (value) {
        .string => |text| blk: {
            if (referenceName(text)) |name| {
                if (!isDeclared(vars, name)) return error.UndeclaredVariable;
                break :blk .{ .ref = try allocator.dupe(u8, name) };
            }
            break :blk .{ .string = try allocator.dupe(u8, text) };
        },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}

fn referenceName(text: []const u8) ?[]const u8 {
    if (text.len >= 4 and std.mem.startsWith(u8, text, "${") and std.mem.endsWith(u8, text, "}")) {
        return text[2 .. text.len - 1];
    }
    return null;
}

test "load recipe and drivers" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set": {
        \\      "write": "VOLT {voltage}",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );
    try workspace.writeFile("recipes/r1_set.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": { "voltage": null },
        \\  "tasks": [
        \\    {
        \\      "every": "100ms",
        \\      "steps": [
        \\        {
        \\          "call": "set",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "voltage": "5"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    const instrument = compiled.instruments.getPtr("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.eql(u8, instrument.resource, "USB0::1::INSTR"));
    try std.testing.expect(std.mem.eql(u8, instrument.driver_name, "psu"));
    try std.testing.expectEqual(@as(usize, 0), instrument.instrument_idx);
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());

    const command = instrument.commands.get("set") orelse return error.TestUnexpectedResult;
    try std.testing.expect(command.instrument == instrument);
    try std.testing.expect(command.response == null);
    try std.testing.expectEqual(@as(usize, 1), command.placeholders.len);
    try std.testing.expect(std.mem.eql(u8, command.placeholders[0], "voltage"));

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expectEqual(@as(u64, 100), compiled.tasks[0].every_ms);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps.len);
    const step0 = compiled.tasks[0].steps[0].action.instrument_call;
    try std.testing.expect(std.mem.eql(u8, step0.call, "set"));
    try std.testing.expect(step0.command == command);

    const voltage = step0.args.get("voltage") orelse return error.TestUnexpectedResult;
    switch (voltage) {
        .scalar => |value| switch (value) {
            .string => |text| try std.testing.expectEqualStrings("5", text),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "parse durations and stop conditions" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set": {
        \\      "write": "VOLT {voltage}",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );
    try workspace.writeFile("recipes/r2_stop_when.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "tasks": [
        \\    {
        \\      "every": "250ms",
        \\      "steps": [
        \\        {
        \\          "call": "set",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "voltage": "5"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ],
        \\  "stop_when": {
        \\    "time_elapsed": "2s",
        \\    "max_iterations": 3
        \\  }
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r2_stop_when.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(u64, 250), compiled.tasks[0].every_ms);

    const voltage = compiled.tasks[0].steps[0].action.instrument_call.args.get("voltage") orelse return error.TestUnexpectedResult;
    switch (voltage) {
        .scalar => |value| switch (value) {
            .string => |text| try std.testing.expectEqualStrings("5", text),
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

    try workspace.makePath("drivers");
    try workspace.writeFile("recipes/initial_vars.json",
        \\{
        \\  "instruments": {},
        \\  "pipeline": { "record": "all" },
        \\  "vars": {
        \\    "v_set": 1.0,
        \\    "name": "scan"
        \\  },
        \\  "tasks": []
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/initial_vars.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.initial_vars.count());
    const v_set = compiled.initial_vars.get("v_set").?;
    try std.testing.expectEqual(@as(f64, 1.0), v_set.float);
    const name = compiled.initial_vars.get("name").?;
    try std.testing.expectEqualStrings("scan", name.string);
}

test "precompile estimates iterations for run-once recipes" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/psu.json",
        \\{
        \\  "metadata": { "name": "psu" },
        \\  "commands": { "set": { "write": "V", "read": null } }
        \\}
    );
    try workspace.writeFile("recipes/run_once.json",
        \\{
        \\  "instruments": { "d1": { "driver": "psu", "resource": "R" } },
        \\  "pipeline": { "record": "all" },
        \\  "vars": {},
        \\  "tasks": [
        \\    { "every_ms": 0, "steps": [ { "call": "set", "instrument": "d1", "args": {} } ] },
        \\    { "every_ms": 0, "steps": [ { "call": "set", "instrument": "d1", "args": {} } ] }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/run_once.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    // Default run-once behavior: each task runs once.
    try std.testing.expectEqual(@as(?u64, 2), compiled.expected_iterations);
}

test "precompile marks iterations unknown for time-limited infinite loops" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/psu.json",
        \\{
        \\  "metadata": { "name": "psu" },
        \\  "commands": { "set": { "write": "V", "read": null } }
        \\}
    );
    try workspace.writeFile("recipes/time_limited.json",
        \\{
        \\  "instruments": { "d1": { "driver": "psu", "resource": "R" } },
        \\  "pipeline": { "record": "all" },
        \\  "vars": {},
        \\  "tasks": [ { "every_ms": 10, "steps": [ { "call": "set", "instrument": "d1", "args": {} } ] } ],
        \\  "stop_when": { "time_elapsed": "1s" }
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/time_limited.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    // Iterations unknown because we skip time-based estimation.
    try std.testing.expectEqual(@as(?u64, null), compiled.expected_iterations);
}

test "precompile preserves typed literal step arguments" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_configure.json",
        \\{
        \\  "metadata": {
        \\    "name": "cfg",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "configure": {
        \\      "write": "CONF {count} {voltage} {enabled} {channels} {mirror}",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );
    try workspace.writeFile("recipes/typed_args.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "cfg",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": { "target": "mir" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "configure",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "count": 5,
        \\            "voltage": 1.25,
        \\            "enabled": true,
        \\            "channels": [1, 2],
        \\            "mirror": "${target}"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/typed_args.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    const args = compiled.tasks[0].steps[0].action.instrument_call.args;

    const count = args.get("count") orelse return error.TestUnexpectedResult;
    switch (count) {
        .scalar => |value| switch (value) {
            .int => |number| try std.testing.expectEqual(@as(i64, 5), number),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const voltage = args.get("voltage") orelse return error.TestUnexpectedResult;
    switch (voltage) {
        .scalar => |value| switch (value) {
            .float => |number| try std.testing.expectApproxEqAbs(@as(f64, 1.25), number, 1e-9),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const enabled = args.get("enabled") orelse return error.TestUnexpectedResult;
    switch (enabled) {
        .scalar => |value| switch (value) {
            .bool => |flag| try std.testing.expect(flag),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const channels = args.get("channels") orelse return error.TestUnexpectedResult;
    switch (channels) {
        .scalar => return error.TestUnexpectedResult,
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len);
            switch (items[0]) {
                .int => |number| try std.testing.expectEqual(@as(i64, 1), number),
                else => return error.TestUnexpectedResult,
            }
            switch (items[1]) {
                .int => |number| try std.testing.expectEqual(@as(i64, 2), number),
                else => return error.TestUnexpectedResult,
            }
        },
    }

    const mirror = args.get("mirror") orelse return error.TestUnexpectedResult;
    switch (mirror) {
        .scalar => |value| switch (value) {
            .ref => |name| try std.testing.expectEqualStrings("target", name),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

const vendor_psu_driver =
    \\{
    \\  "metadata": {
    \\    "name": "psu0",
    \\    "version": null,
    \\    "description": null
    \\  },
    \\  "commands": {
    \\    "set_voltage": {
    \\      "write": "VOLT {voltage},(@{channels})",
    \\      "read": null
    \\    }
    \\  }
    \\}
;

test "precompile stores only referenced commands" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/bench_supply.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu0",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage}",
        \\      "read": null
        \\    },
        \\    "output_on": {
        \\      "write": "OUTP ON",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );
    try workspace.writeFile("recipes/r1_set_voltage.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": {},
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "voltage": "1.0"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
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

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/missing_instrument.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "vars": {},
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "missing",
        \\          "args": {
        \\            "voltage": "1.0"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_instrument.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectError(error.InstrumentNotFound, precompilePath(gpa, recipe_path, &registry));
}

test "precompile validates command arguments" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/missing_argument.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );
    try workspace.writeFile("recipes/unexpected_argument.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "voltage": "1.0",
        \\            "channels": [1],
        \\            "channel": 1
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const missing_argument_path = try workspace.realpathAlloc("recipes/missing_argument.json");
    defer gpa.free(missing_argument_path);
    const unexpected_argument_path = try workspace.realpathAlloc("recipes/unexpected_argument.json");
    defer gpa.free(unexpected_argument_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectError(error.MissingCommandArgument, precompilePath(gpa, missing_argument_path, &registry));
    try std.testing.expectError(error.UnexpectedCommandArgument, precompilePath(gpa, unexpected_argument_path, &registry));
}

test "precompiled command renders via helper" {
    const gpa = std.testing.allocator;

    const source = try Driver.Command.parse(gpa, "VOLT {voltage}", null);
    defer source.deinit(gpa);

    var instrument = types.PrecompiledInstrument{
        .instrument_idx = 0,
        .driver_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(gpa),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    try std.testing.expect(compiled.instrument == &instrument);

    var values = std.StringHashMap([]const u8).init(gpa);
    defer values.deinit();
    try values.put("voltage", "3.3");

    var stack_buf: [32]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], &values, instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("VOLT 3.3\n", rendered.bytes);
    try std.testing.expect(rendered.owned == null);
}

test "precompiled command render falls back to heap when suffix leaves too little stack space" {
    const gpa = std.testing.allocator;

    const source = try Driver.Command.parse(gpa, "VOLT {voltage}", null);
    defer source.deinit(gpa);

    var instrument = types.PrecompiledInstrument{
        .instrument_idx = 0,
        .driver_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(gpa),
        .write_termination = "\r\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    var values = std.StringHashMap([]const u8).init(gpa);
    defer values.deinit();
    try values.put("voltage", "1234567890");

    var stack_buf: [8]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], &values, instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expect(rendered.owned != null);
    try std.testing.expectEqualStrings("VOLT 1234567890\r\n", rendered.bytes);
}

test "precompile diagnostic includes step context" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/missing_command.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "missing",
        \\          "instrument": "d1",
        \\          "args": {
        \\            "voltage": "1.0"
        \\          }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_command.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, &registry, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.CommandNotFound, err);

        var out = std.Io.Writer.Allocating.init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "task 0 step 0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "instrument=d1"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "driver=psu0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "command=missing"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile compute step" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/compute.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": { "v": 0, "doubled": 0 },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": "5", "channels": "1" },
        \\          "save_as": "v"
        \\        },
        \\        {
        \\          "compute": "${v} * 2",
        \\          "save_as": "doubled"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.tasks[0].steps.len);

    // First step: instrument call
    switch (compiled.tasks[0].steps[0].action) {
        .instrument_call => |ic| try std.testing.expectEqualStrings("set_voltage", ic.call),
        .compute => return error.TestUnexpectedResult,
    }

    // Second step: compute
    switch (compiled.tasks[0].steps[1].action) {
        .compute => |comp| try std.testing.expectEqualStrings("doubled", comp.save_as),
        .instrument_call => return error.TestUnexpectedResult,
    }
}

test "precompile compute step rejects missing save_as" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/compute_no_save.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "compute": "1 + 2"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute_no_save.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    // With the union-based StepConfig, missing required fields (save_as) results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, &registry));
}

test "precompile step with when guard" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/when_guard.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": { "power": 0 },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": "5", "channels": "1" },
        \\          "when": "${power} > 100"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/when_guard.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
    defer compiled.deinit();

    try std.testing.expect(compiled.tasks[0].steps[0].when != null);
}

test "precompile rejects invalid step (neither call nor compute)" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/invalid_step.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": { "record": "all" },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "save_as": "orphan"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/invalid_step.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    // With the union-based StepConfig, an object matching neither variant results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, &registry));
}

test "precompile rejects record with unknown save_as variable" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/bad_record.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": ["voltage", "nonexistent"]
        \\  },
        \\  "vars": { "voltage": 0, "nonexistent": 0 },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": "1.0", "channels": [1, 2] },
        \\          "save_as": "voltage"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/bad_record.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectError(error.RecordVariableNotFound, precompilePath(gpa, recipe_path, &registry));
}

test "precompile accepts valid record subset" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/record_ok.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": ["voltage"]
        \\  },
        \\  "vars": { "voltage": 0, "doubled": 0 },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": "1.0", "channels": [1, 2] },
        \\          "save_as": "voltage"
        \\        },
        \\        {
        \\          "compute": "${voltage} * 2",
        \\          "save_as": "doubled"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_ok.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
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

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/no_pipeline.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": 1.0, "channels": [1] }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_pipeline.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, &registry, &precompile_diagnostic) catch |err| {
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

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/no_record.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {},
        \\  "tasks": [
        \\    {
        \\      "every_ms": 0,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": 1.0, "channels": [1] }
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_record.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var precompile_diagnostic = diagnostic.PrecompileDiagnostic.init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePathWithDiagnostic(gpa, recipe_path, &registry, &precompile_diagnostic) catch |err| {
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

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json", vendor_psu_driver);
    try workspace.writeFile("recipes/record_all.json",
        \\{
        \\  "instruments": {
        \\    "d1": {
        \\      "driver": "psu0",
        \\      "resource": "USB0::1::INSTR"
        \\    }
        \\  },
        \\  "pipeline": {
        \\    "record": "all"
        \\  },
        \\  "vars": { "voltage": 0, "doubled": 0 },
        \\  "tasks": [
        \\    {
        \\      "every_ms": 100,
        \\      "steps": [
        \\        {
        \\          "call": "set_voltage",
        \\          "instrument": "d1",
        \\          "args": { "voltage": 5, "channels": [1] },
        \\          "save_as": "voltage"
        \\        },
        \\        {
        \\          "compute": "${voltage} * 2",
        \\          "save_as": "doubled"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_all.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    var compiled = try precompilePath(gpa, recipe_path, &registry);
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

    try workspace.writeFile("drivers/psu.json",
        \\{
        \\  "metadata": { "name": "psu" },
        \\  "commands": { "set": { "write": "V {voltage}", "read": null } }
        \\}
    );
    try workspace.writeFile("recipes/undeclared.json",
        \\{
        \\  "instruments": { "d1": { "driver": "psu", "resource": "R" } },
        \\  "pipeline": { "record": "all" },
        \\  "vars": { "voltage": 1 },
        \\  "tasks": [
        \\    { "every_ms": 0, "steps": [ { "call": "set", "instrument": "d1", "args": { "voltage": "5" }, "save_as": "undeclared_var" } ] }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, &registry));
}

test "precompile rejects undeclared variable in expression" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.makePath("drivers");
    try workspace.writeFile("recipes/undeclared_expr.json",
        \\{
        \\  "instruments": {},
        \\  "pipeline": { "record": "all" },
        \\  "vars": { "v": 1 },
        \\  "tasks": [
        \\    { "every_ms": 0, "steps": [ { "compute": "${v} + ${x}", "save_as": "v" } ] }
        \\  ]
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared_expr.json");
    defer gpa.free(recipe_path);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, &registry));
}
