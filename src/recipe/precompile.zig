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
    driver_reg: *const DriverRegistry,
) !types.PrecompiledRecipe {
    return precompilePathInternal(allocator, recipe_path, driver_reg, null);
}

pub fn precompilePathWithDiagnostic(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    driver_reg: *const DriverRegistry,
    precompile_diagnostic: *diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    precompile_diagnostic.reset();
    return precompilePathInternal(allocator, recipe_path, driver_reg, precompile_diagnostic);
}

fn precompilePathInternal(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    driver_reg: *const DriverRegistry,
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
    driver_reg: *const DriverRegistry,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime and a temporary driver cache used only while validating the recipe.
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var loaded_drivers = std.StringHashMap(Driver).init(allocator);
    defer deinitLoadedDrivers(allocator, &loaded_drivers);

    var diag_ctx: diagnostic.DiagnosticContext = .{};
    errdefer captureDiagnostic(precompile_diagnostic, diag_ctx);

    var precompiled_instruments = std.StringHashMap(types.PrecompiledInstrument).init(alloc);
    try precompiled_instruments.ensureTotalCapacity(recipe.instruments.count());

    // 2. Pre-register every instrument, fail fast on missing drivers, assign dense instrument indices, and prepare empty command caches.
    var instrument_it = recipe.instruments.iterator();
    var next_instrument_idx: usize = 0;
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr.*;

        diag_ctx = .{
            .instrument_name = instrument_name,
            .driver_name = instrument_cfg.driver,
        };
        const driver = try getOrLoadDriver(allocator, &loaded_drivers, driver_reg, instrument_cfg.driver);

        const name_copy = try alloc.dupe(u8, instrument_name);
        const driver_copy = try alloc.dupe(u8, instrument_cfg.driver);
        const resource_copy = try alloc.dupe(u8, instrument_cfg.resource);
        const write_termination = try cloneOptionalBytes(alloc, driver.write_termination);
        try precompiled_instruments.put(name_copy, .{
            .instrument_idx = next_instrument_idx,
            .driver_name = driver_copy,
            .resource = resource_copy,
            .commands = std.StringHashMap(*const types.PrecompiledCommand).init(alloc),
            .write_termination = write_termination,
            .options = .{
                .timeout_ms = driver.options.timeout_ms,
                .read_termination = try cloneOptionalBytes(alloc, driver.options.read_termination),
                .query_delay_ms = driver.options.query_delay_ms,
                .chunk_size = driver.options.chunk_size,
            },
        });
        next_instrument_idx += 1;
    }

    // 3. Normalize task intervals and allocate the arena-owned runtime task and step arrays.
    const tasks = try alloc.alloc(types.Task, recipe.tasks.len);
    for (recipe.tasks, 0..) |*task_cfg, task_idx| {
        diag_ctx = .{ .task_idx = task_idx };
        const every_ms = try resolveEveryMs(task_cfg);

        const steps = try alloc.alloc(types.Step, task_cfg.steps.len);
        for (task_cfg.steps, 0..) |*step_cfg, step_idx| {
            diag_ctx = .{
                .task_idx = task_idx,
                .step_idx = step_idx,
                .instrument_name = step_cfg.instrument,
                .command_name = step_cfg.call,
            };

            // Parse optional `when` guard expression.
            const when_expr: ?expr.Expression = if (step_cfg.when) |when_src|
                expr.parse(alloc, when_src) catch return error.InvalidExpression
            else
                null;

            if (step_cfg.isCompute()) {
                // ── Compute step ────────────────────────────────
                const compute_src = step_cfg.compute.?;
                const save_as = step_cfg.save_as orelse return error.ComputeStepMissingSaveAs;

                const compute_expr = expr.parse(alloc, compute_src) catch return error.InvalidExpression;

                const save_as_copy = try alloc.dupe(u8, save_as);
                steps[step_idx] = .{
                    .action = .{ .compute = .{
                        .expression = compute_expr,
                        .save_as = save_as_copy,
                    } },
                    .when = when_expr,
                };
            } else if (step_cfg.isCall()) {
                // ── Instrument call step (existing logic) ──────
                const call = step_cfg.call.?;
                const instrument_name = step_cfg.instrument orelse return error.InstrumentNotFound;

                // 4. Resolve the referenced instrument and command, compiling the command on first use for this instrument.
                const precompiled_instrument = precompiled_instruments.getPtr(instrument_name) orelse return error.InstrumentNotFound;

                diag_ctx.driver_name = precompiled_instrument.driver_name;
                const loaded_driver = try getOrLoadDriver(allocator, &loaded_drivers, driver_reg, precompiled_instrument.driver_name);
                const command = try getOrCompileCommand(alloc, precompiled_instrument, loaded_driver, call);

                // 5. Clone step arguments into runtime form, validate them, and store the command index used by the executor.
                const call_copy = try alloc.dupe(u8, call);
                const instrument_copy = try alloc.dupe(u8, instrument_name);
                var args_map = std.StringHashMap(types.StepArg).init(alloc);
                if (step_cfg.args) |args| {
                    try cloneStepArgs(alloc, &args_map, args);
                }
                try validateStepArgs(command, &args_map, &diag_ctx);
                const save_as_copy = if (step_cfg.save_as) |value| try alloc.dupe(u8, value) else null;
                steps[step_idx] = .{
                    .action = .{ .instrument_call = .{
                        .call = call_copy,
                        .instrument = instrument_copy,
                        .command = command,
                        .args = args_map,
                        .save_as = save_as_copy,
                    } },
                    .when = when_expr,
                };
            } else {
                // Neither call nor compute – invalid step.
                return error.InvalidStepConfig;
            }
        }
        tasks[task_idx] = .{ .every_ms = every_ms, .steps = steps };
    }

    // 6. Validate and resolve pipeline record configuration.
    //    When record is "all", expand it into the explicit list of all save_as labels
    //    so that downstream code never needs to handle the "all" case.
    diag_ctx = .{};
    const pipeline_cfg = recipe.pipeline orelse return error.MissingPipeline;
    if (pipeline_cfg.record == null) return error.MissingRecordConfig;
    var pipeline = try clonePipelineConfig(alloc, pipeline_cfg);
    switch (pipeline.record.?) {
        .all => |value| {
            if (!std.mem.eql(u8, value, "all")) return error.InvalidRecordConfig;
            // Expand "all" into an explicit list of every save_as label, preserving first-occurrence order.
            pipeline.record = .{ .explicit = try collectAllSaveAs(alloc, tasks) };
        },
        .explicit => |columns| {
            for (columns) |name| {
                if (!hasSaveAsLabel(tasks, name)) return error.RecordVariableNotFound;
            }
        },
    }

    // 7. Return the fully validated arena-owned recipe consumed by preview and execution.
    return .{
        .arena = arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = pipeline,
        .stop_when = try parseStopWhen(recipe.stop_when),
    };
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

fn getOrLoadDriver(
    allocator: std.mem.Allocator,
    loaded_drivers: *std.StringHashMap(Driver),
    driver_reg: *const DriverRegistry,
    driver_name: []const u8,
) !*const Driver {
    if (loaded_drivers.getPtr(driver_name)) |loaded| return loaded;

    const key = try allocator.dupe(u8, driver_name);
    errdefer allocator.free(key);

    var loaded = try driver_reg.loadDriver(allocator, driver_name);
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
    errdefer allocator.free(key);

    const compiled_value = try compileCommand(allocator, source, instrument);
    errdefer compiled_value.deinit(allocator);

    const compiled = try allocator.create(types.PrecompiledCommand);
    errdefer allocator.destroy(compiled);
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
    errdefer command.deinit(allocator);

    const placeholders = try command.placeholderNames(allocator);
    errdefer allocator.free(placeholders);

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
        if (!containsString(command.placeholders, entry.key_ptr.*)) {
            diag_ctx.argument_name = entry.key_ptr.*;
            return error.UnexpectedCommandArgument;
        }
    }
}

fn containsString(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn hasSaveAsLabel(tasks: []const types.Task, name: []const u8) bool {
    for (tasks) |task| {
        for (task.steps) |*step| {
            const label = switch (step.action) {
                .instrument_call => |ic| ic.save_as orelse continue,
                .compute => |comp| comp.save_as,
            };
            if (std.mem.eql(u8, label, name)) return true;
        }
    }
    return false;
}

/// Collects every unique `save_as` label across all tasks, preserving first-occurrence order.
fn collectAllSaveAs(allocator: std.mem.Allocator, tasks: []const types.Task) ![]const []const u8 {
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    var columns = std.ArrayList([]const u8).empty;
    defer columns.deinit(allocator);

    for (tasks) |task| {
        for (task.steps) |*step| {
            const label = switch (step.action) {
                .instrument_call => |ic| ic.save_as orelse continue,
                .compute => |comp| comp.save_as,
            };
            const entry = try seen.getOrPut(label);
            if (!entry.found_existing) {
                try columns.append(allocator, label);
            }
        }
    }

    return try columns.toOwnedSlice(allocator);
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

fn cloneStepArgs(
    allocator: std.mem.Allocator,
    args_map: *std.StringHashMap(types.StepArg),
    doc_args: std.StringHashMap(config.ArgValueDoc),
) !void {
    var it = doc_args.iterator();
    while (it.next()) |entry| {
        const key_copy = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(key_copy);

        const value_copy = try cloneStepArg(allocator, entry.value_ptr.*);
        try args_map.put(key_copy, value_copy);
    }
}

fn cloneStepArg(allocator: std.mem.Allocator, doc_arg: config.ArgValueDoc) !types.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try cloneArgScalar(allocator, scalar) },
        .list => |items| blk: {
            const out = try allocator.alloc(types.StepScalar, items.len);
            errdefer allocator.free(out);

            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |item| freeClonedStepScalar(allocator, item);
            }

            for (items, 0..) |item, idx| {
                out[idx] = try cloneArgScalar(allocator, item);
                initialized += 1;
            }
            break :blk .{ .list = out };
        },
    };
}

fn cloneArgScalar(allocator: std.mem.Allocator, value: config.ArgScalarDoc) !types.StepScalar {
    return switch (value) {
        .string => |text| blk: {
            if (referenceName(text)) |name| {
                break :blk .{ .ref = try allocator.dupe(u8, name) };
            }
            break :blk .{ .string = try allocator.dupe(u8, text) };
        },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}

fn freeClonedStepScalar(allocator: std.mem.Allocator, value: types.StepScalar) void {
    switch (value) {
        .string => |text| allocator.free(text),
        .ref => |name| allocator.free(name),
        .int, .float, .bool => {},
    }
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

    try std.testing.expectError(error.ComputeStepMissingSaveAs, precompilePath(gpa, recipe_path, &registry));
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

    try std.testing.expectError(error.InvalidStepConfig, precompilePath(gpa, recipe_path, &registry));
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
    try registry.rebuild();

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
