const std = @import("std");
const Driver = @import("../driver/Driver.zig");
const recipe_mod = @import("../recipe/root.zig");
const testing = @import("../testing.zig");
const visa = @import("../visa/root.zig");
const common = @import("common.zig");
const pipeline_mod = @import("pipeline/root.zig");
const scheduler = @import("scheduler.zig");

/// Precompiles a recipe, opens instrument sessions, and runs tasks until completion.
pub fn execute(allocator: std.mem.Allocator, opts: common.ExecOptions) !void {
    const log = opts.log;

    scheduler.initializeStopHandling();

    var vtable: visa.loader.Vtable = undefined;
    var rm: ?visa.ResourceManager = null;
    defer if (rm) |*mgr| mgr.deinit();
    if (!opts.dry_run) {
        vtable = try visa.loader.load(opts.visa_lib);
        rm = try visa.ResourceManager.init(&vtable);
    }

    var precompile_diagnostic = recipe_mod.PrecompileDiagnostic.init(allocator);
    defer precompile_diagnostic.deinit();

    var compiled = blk: {
        var dir = if (std.fs.path.isAbsolute(opts.driver_dir))
            try std.fs.openDirAbsolute(opts.driver_dir, .{})
        else
            try std.fs.cwd().openDir(opts.driver_dir, .{});
        defer dir.close();
        break :blk recipe_mod.PrecompiledRecipe.precompilePathWithDiagnostic(allocator, opts.recipe_path, dir, &precompile_diagnostic) catch |err| {
            try precompile_diagnostic.write(log, err);
            return err;
        };
    };
    defer compiled.deinit();

    if (compiled.expected_iterations) |total| {
        try log.print("[PLAN] Expected iterations: {d}\n", .{total});
    } else {
        try log.print("[PLAN] Expected iterations: Unknown (running until manual stop or time limit)\n", .{});
    }

    const instruments = try allocator.alloc(common.InstrumentRuntime, compiled.instruments.count());
    var ctx = try common.Context.init(allocator, compiled.initial_values.len);

    // Initialize context variables from the recipe.
    for (compiled.initial_values, 0..) |value_opt, slot_idx| {
        const value = value_opt orelse continue;
        try ctx.setSlot(slot_idx, value);
    }

    defer {
        for (instruments) |*runtime| {
            if (runtime.handle) |*instr| instr.deinit();
        }
        allocator.free(instruments);
        ctx.deinit();
    }

    for (instruments) |*runtime| {
        runtime.* = .{ .handle = null };
    }

    try prepareRuntime(allocator, &compiled.instruments, instruments, rm);

    var pipeline_config = pipeline_mod.resolveConfig(&compiled.pipeline, &opts);
    var resolved_file_path: ?[]u8 = null;
    defer if (resolved_file_path) |path| allocator.free(path);
    if (pipeline_config.file_path) |path| {
        resolved_file_path = try resolveConfiguredPath(allocator, opts.recipe_path, path);
        pipeline_config.file_path = resolved_file_path.?;
    }

    const frame_columns = compiled.pipeline.record.?.explicit;

    var pipeline_runtime = try pipeline_mod.Runtime.init(allocator, pipeline_config, frame_columns, log);
    defer pipeline_runtime.deinit();
    try pipeline_runtime.start();

    var sampler_state = scheduler.SamplerState{
        .allocator = allocator,
        .compiled_recipe = &compiled,
        .instruments = instruments,
        .ctx = &ctx,
        .pipeline_runtime = &pipeline_runtime,
        .dry_run = opts.dry_run,
        .max_duration_ms = opts.max_duration_ms,
    };

    var monitor_state = pipeline_mod.MonitorState{};
    const sampler_thread = try std.Thread.spawn(.{}, scheduler.runTasksThread, .{&sampler_state});
    var finalized = false;
    defer if (!finalized) {
        finalizeExecution(&pipeline_runtime, &monitor_state, sampler_thread);
    };
    while (!sampler_state.done.load(.seq_cst)) {
        pipeline_runtime.emitWarnings(&monitor_state);
        std.Thread.sleep(pipeline_mod.monitor_interval_ns);
    }

    finalizeExecution(&pipeline_runtime, &monitor_state, sampler_thread);
    finalized = true;

    if (sampler_state.result) |err| return err;
    if (pipeline_runtime.workerResult()) |err| return err;
}

fn finalizeExecution(
    pipeline_runtime: *pipeline_mod.Runtime,
    monitor_state: *pipeline_mod.MonitorState,
    sampler_thread: std.Thread,
) void {
    sampler_thread.join();
    pipeline_runtime.markProducerDone();
    pipeline_runtime.emitWarnings(monitor_state);
    pipeline_runtime.join();
    pipeline_runtime.writeSummary();
    pipeline_runtime.finishLogs();
}

fn resolveConfiguredPath(allocator: std.mem.Allocator, recipe_path: []const u8, configured_path: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(configured_path)) {
        return allocator.dupe(u8, configured_path);
    }

    const recipe_dir = std.fs.path.dirname(recipe_path) orelse ".";
    return std.fs.path.join(allocator, &.{ recipe_dir, configured_path });
}

/// Opens VISA sessions for every precompiled instrument when not running in dry-run mode.
fn prepareRuntime(
    allocator: std.mem.Allocator,
    precompiled_instruments: *const std.StringArrayHashMap(recipe_mod.PrecompiledInstrument),
    instruments: []common.InstrumentRuntime,
    rm: ?visa.ResourceManager,
) !void {
    const mgr = rm orelse return;
    for (precompiled_instruments.values(), 0..) |*compiled_instrument, idx| {
        const runtime = &instruments[idx];
        var instr = visa.Instrument.init(mgr.session, mgr.vtable);
        try instr.open(allocator, compiled_instrument.resource, compiled_instrument.options);
        runtime.handle = instr;
    }
}

const vendor_psu_driver =
    \\[metadata]
    \\
    \\[commands.set_voltage]
    \\write = "VOLT {voltage},(@{channels})"
;

test "executor execute dry run" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/psu0.toml", vendor_psu_driver);
    try workspace.writeFile("recipes/r1_set_voltage.yaml",
        \\instruments:
        \\  d1:
        \\    driver: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: 5
        \\          channels:
        \\            - 1
        \\            - 2
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.yaml");
    defer gpa.free(recipe_path);

    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();

    const opts = common.ExecOptions{
        .driver_dir = driver_dir,
        .recipe_path = recipe_path,
        .dry_run = true,
        .log = &out.writer,
        .max_duration_ms = null,
    };

    try execute(gpa, opts);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "dry-run"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "[SUMMARY] frame buffer overflows: 0"));
}

test "executor pipeline creates csv frame sink during dry run" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/psu0.toml", vendor_psu_driver);
    try workspace.writeFile("recipes/pipeline.yaml",
        \\instruments:
        \\  d1:
        \\    driver: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  buffer_size: 64
        \\  warn_usage_percent: 80
        \\  mode: safe
        \\  file_path: samples.csv
        \\  record: all
        \\tasks:
        \\  - every_ms: 0
        \\    steps:
        \\      - call: set_voltage
        \\        instrument: d1
        \\        args:
        \\          voltage: 5
        \\          channels:
        \\            - 1
        \\            - 2
        \\stop_when:
        \\  max_iterations: 2
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/pipeline.yaml");
    defer gpa.free(recipe_path);

    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();

    try execute(gpa, .{
        .driver_dir = driver_dir,
        .recipe_path = recipe_path,
        .dry_run = true,
        .log = &out.writer,
    });

    const file_data = try workspace.readFileAlloc(gpa, "recipes/samples.csv", 8 * 1024);
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings("timestamp_ns,task_idx\n", file_data);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "[SUMMARY] buffer capacity: 64"));
}
