const std = @import("std");
const tty = @import("../tty.zig");
const recipe_mod = @import("../recipe/root.zig");
const testing = @import("../testing.zig");
const visa = @import("../visa/root.zig");
const session = @import("session.zig");
const pipeline_mod = @import("pipeline/root.zig");
const scheduler = @import("scheduler.zig");

const plan_tag = tty.styledText("[PLAN]", .{.aqua});

/// Precompiles a recipe, opens instrument sessions, and runs tasks until completion.
pub fn execute(allocator: std.mem.Allocator, opts: session.ExecOptions) !void {
    const log = opts.log;

    scheduler.initializeStopHandling();

    var vtable: visa.loader.Vtable = undefined;
    var rm: ?visa.ResourceManager = null;
    defer if (rm) |*mgr| mgr.deinit();
    if (!opts.dry_run) {
        var visa_diag: visa.loader.LoadDiagnostic = undefined;
        vtable = visa.loader.load(opts.visa_lib, &visa_diag) catch |err| {
            try visa_diag.write(log, err);
            return error.Diagnosed;
        };
        rm = try visa.ResourceManager.init(&vtable);
    }

    var precompile_diagnostic: recipe_mod.PrecompileDiagnostic = .init(allocator);
    defer precompile_diagnostic.deinit();

    var compiled = blk: {
        const dir = if (std.fs.path.isAbsolute(opts.adapter_dir))
            std.Io.Dir.openDirAbsolute(opts.io, opts.adapter_dir, .{})
        else
            std.Io.Dir.cwd().openDir(opts.io, opts.adapter_dir, .{});
        const opened = dir catch |err| {
            try log.writeAll(tty.error_prefix);
            try log.print("cannot open adapter directory '{s}': {s}\n", .{ opts.adapter_dir, @errorName(err) });
            return error.Diagnosed;
        };
        defer opened.close(opts.io);
        break :blk recipe_mod.PrecompiledRecipe.precompilePath(allocator, opts.io, opts.recipe_path, opened, &precompile_diagnostic) catch |err| {
            try precompile_diagnostic.write(log, err);
            return error.Diagnosed;
        };
    };
    defer compiled.deinit();

    if (compiled.expected_iterations) |total| {
        try log.print(plan_tag ++ " Expected iterations: {d}\n", .{total});
    } else {
        try log.print(plan_tag ++ " Expected iterations: Unknown (running until manual stop or time limit)\n", .{});
    }

    const instruments = try allocator.alloc(session.InstrumentRuntime, compiled.instruments.count());
    var ctx: session.Context = try .init(allocator, opts.io, compiled.initial_values);

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

    var pipeline_runtime: pipeline_mod.Runtime = try .init(allocator, opts.io, pipeline_config, frame_columns, log);
    defer pipeline_runtime.deinit();
    try pipeline_runtime.start();

    var sampler_state = scheduler.SamplerState{
        .allocator = allocator,
        .compiled_recipe = &compiled,
        .instruments = instruments,
        .ctx = &ctx,
        .pipeline_runtime = &pipeline_runtime,
        .dry_run = opts.dry_run,
    };

    var monitor_state = pipeline_mod.MonitorState{};
    const sampler_thread = try std.Thread.spawn(.{}, scheduler.runTasksThread, .{&sampler_state});
    var finalized = false;
    defer if (!finalized) {
        sampler_thread.join();
        finalizePipeline(&pipeline_runtime, &monitor_state);
    };
    while (!sampler_state.done.load(.seq_cst)) {
        pipeline_runtime.emitWarnings(&monitor_state);
        opts.io.sleep(.fromNanoseconds(pipeline_mod.monitor_interval_ns), .awake) catch break;
    }

    sampler_thread.join();
    finalizePipeline(&pipeline_runtime, &monitor_state);
    finalized = true;

    if (sampler_state.result) |err| return err;
    if (pipeline_runtime.workerResult()) |err| return err;
}

fn finalizePipeline(
    pipeline_runtime: *pipeline_mod.Runtime,
    monitor_state: *pipeline_mod.MonitorState,
) void {
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
    precompiled_instruments: *const std.StringArrayHashMapUnmanaged(recipe_mod.PrecompiledInstrument),
    instruments: []session.InstrumentRuntime,
    rm: ?visa.ResourceManager,
) !void {
    const mgr = rm orelse return;
    for (precompiled_instruments.values(), 0..) |*compiled_instrument, idx| {
        const runtime = &instruments[idx];
        var instr: visa.Instrument = .init(mgr.session, mgr.vtable);
        try instr.open(allocator, compiled_instrument.resource, compiled_instrument.options);
        runtime.handle = instr;
    }
}

const vendor_psu_adapter =
    \\{"metadata": {}, "commands": {"set_voltage": {"write": "VOLT {voltage},(@{channels})"}}}
;

test "executor execute dry run" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.json", vendor_psu_adapter);
    try workspace.writeFile("recipes/r1_set_voltage.json",
        \\{"instruments": {"d1": {"adapter": "psu0.json", "resource": "USB0::1::INSTR"}}, "pipeline": {"record": {"all": "all"}}, "tasks": [{"steps": [{"call": {"call": "d1.set_voltage", "args": {"voltage": {"scalar": {"int": 5}}, "channels": {"list": [{"int": 1}, {"int": 2}]}}}}]}]}
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.json");
    defer gpa.free(recipe_path);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    const opts = session.ExecOptions{
        .adapter_dir = adapter_dir,
        .recipe_path = recipe_path,
        .io = std.testing.io,
        .dry_run = true,
        .log = &out.writer,
    };

    try execute(gpa, opts);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "dry-run"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "[SUMMARY]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "frame buffer overflows: 0"));
}

test "executor pipeline creates csv frame sink during dry run" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.json", vendor_psu_adapter);
    try workspace.writeFile("recipes/pipeline.json",
        \\{"instruments": {"d1": {"adapter": "psu0.json", "resource": "USB0::1::INSTR"}}, "pipeline": {"buffer_size": 64, "warn_usage_percent": 80, "mode": "safe", "file_path": "samples.csv", "record": {"all": "all"}}, "stop_when": {"string": "$ITER >= 2"}, "tasks": [{"steps": [{"call": {"call": "d1.set_voltage", "args": {"voltage": {"scalar": {"int": 5}}, "channels": {"list": [{"int": 1}, {"int": 2}]}}}}]}]}
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/pipeline.json");
    defer gpa.free(recipe_path);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try execute(gpa, .{
        .adapter_dir = adapter_dir,
        .recipe_path = recipe_path,
        .dry_run = true,
        .io = std.testing.io,
        .log = &out.writer,
    });

    const file_data = try workspace.readFileAlloc(gpa, "recipes/samples.csv", 8 * 1024);
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings("\n", file_data);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "[SUMMARY]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "buffer capacity: 64"));
}
