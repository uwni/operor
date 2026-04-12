const std = @import("std");

/// Document parsing helpers for adapter and recipe files.
pub const doc_parse = @import("doc_parse.zig");
/// Expression parser and evaluator for compute steps and `if` guards.
pub const expr = @import("expr.zig");
/// Adapter parsing APIs.
pub const Adapter = @import("adapter/Adapter.zig");
/// Recipe execution engine.
pub const executor = @import("executor/root.zig");
/// Recipe parsing and precompilation APIs.
pub const recipe = @import("recipe/root.zig");
/// Low-level VISA integration layer.
pub const visa = @import("visa/root.zig");

/// Execution options re-exported from the executor module.
pub const ExecOptions = executor.ExecOptions;
/// Resource listing result re-exported from the VISA layer.
pub const ResourceList = visa.ResourceList;
const repl_api = @import("repl.zig");

/// Recommended stdin buffer size for the interactive REPL.
pub const repl_stdin_buffer_bytes = repl_api.stdin_buffer_bytes;

/// Executes a recipe against its referenced instruments.
pub fn execute(allocator: std.mem.Allocator, opts: ExecOptions) !void {
    try executor.execute(allocator, opts);
}

/// Enumerates VISA resources visible to the default resource manager.
/// `visa_lib` optionally overrides the VISA shared library path.
pub fn listResources(allocator: std.mem.Allocator, visa_lib: ?[]const u8) !ResourceList {
    const vtable = try visa.loader.load(visa_lib);
    var rm = try visa.ResourceManager.init(&vtable);
    defer rm.deinit();

    return try rm.listResources(allocator);
}

/// Opens the interactive VISA REPL against a single resource address.
/// `visa_lib` optionally overrides the VISA shared library path.
pub fn repl(
    allocator: std.mem.Allocator,
    resource_addr: ?[]const u8,
    visa_lib: ?[]const u8,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !void {
    try repl_api.run(allocator, resource_addr, visa_lib, reader, out);
}

/// Precompiles and prints a human-readable preview of a recipe without opening VISA sessions.
pub fn preview(
    allocator: std.mem.Allocator,
    adapter_dir: []const u8,
    recipe_path: []const u8,
    log: *std.Io.Writer,
) !void {
    var precompile_diagnostic: recipe.PrecompileDiagnostic = .init(allocator);
    defer precompile_diagnostic.deinit();

    var compiled = blk: {
        var dir = if (std.fs.path.isAbsolute(adapter_dir))
            try std.fs.openDirAbsolute(adapter_dir, .{})
        else
            try std.fs.cwd().openDir(adapter_dir, .{});
        defer dir.close();
        break :blk recipe.PrecompiledRecipe.precompilePath(allocator, recipe_path, dir, &precompile_diagnostic) catch |err| {
            try precompile_diagnostic.write(log, err);
            return err;
        };
    };
    defer compiled.deinit();

    try log.print("Instruments: {d}\n", .{compiled.instruments.count()});
    var instrument_it = compiled.instruments.iterator();
    while (instrument_it.next()) |entry| {
        try log.print("- {s} -> {s}\n", .{ entry.key_ptr.*, entry.value_ptr.resource });
    }
    if (compiled.stop_when != null) {
        try log.print("Stop: expression-based\n", .{});
    }
    if (compiled.pipeline.mode != null or
        compiled.pipeline.buffer_size != null or
        compiled.pipeline.warn_usage_percent != null or
        compiled.pipeline.file_path != null or
        compiled.pipeline.network_host != null)
    {
        try log.print(
            "Pipeline: mode={s} buffer_size={d} warn_usage_percent={d}\n",
            .{
                @tagName(compiled.pipeline.mode orelse .safe),
                compiled.pipeline.buffer_size orelse 0,
                compiled.pipeline.warn_usage_percent orelse 85,
            },
        );
        if (compiled.pipeline.file_path) |path| {
            try log.print("  file={s}\n", .{path});
        }
        if (compiled.pipeline.network_host) |host| {
            try log.print("  network={s}:{d}\n", .{ host, compiled.pipeline.network_port orelse 0 });
        }
    }
    try log.print("Tasks: {d}\n", .{compiled.tasks.len});
    for (compiled.tasks, 0..) |*task, task_idx| {
        const task_kind: []const u8 = switch (task.*) {
            .loop => "loop",
            .sequential => "sequential",
            .conditional => "conditional",
        };
        const task_steps = task.steps();
        try log.print("  Task {d}: {s}, {d} steps\n", .{ task_idx, task_kind, task_steps.len });
        for (task_steps, 0..) |*step, step_idx| {
            switch (step.action) {
                .instrument_call => |ic| {
                    try log.print("    [{d}] call={s} instrument={s}", .{ step_idx, ic.call, ic.instrument });
                    if (ic.save_slot) |slot| try log.print(" slot={d}", .{slot});
                    if (ic.save_column) |col| try log.print(" col={d}", .{col});
                    try log.print("\n", .{});
                },
                .compute => |comp| {
                    try log.print("    [{d}] compute -> slot={d}", .{ step_idx, comp.save_slot });
                    if (comp.save_column) |col| try log.print(" col={d}", .{col});
                    try log.print("\n", .{});
                },
                .sleep => |s| {
                    try log.print("    [{d}] sleep {d}ms\n", .{ step_idx, s.duration_ms });
                },
            }
            if (step.@"if" != null) {
                try log.print("         if: (guard expression)\n", .{});
            }
        }
    }
}

test "preview output" {
    const gpa = std.testing.allocator;
    const testing = @import("testing.zig");

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml",
        \\[metadata]
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage},(@{channels})"
    );
    try workspace.writeFile("recipes/r1_set_voltage.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 5
        \\          channels:
        \\            - 1
        \\            - 2
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.yaml");
    defer gpa.free(recipe_path);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try preview(gpa, adapter_dir, recipe_path, &out.writer);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "Instruments: 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "Tasks: 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "call=set_voltage"));
}

test {
    std.testing.refAllDecls(@This());
}
