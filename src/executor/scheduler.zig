const std = @import("std");
const recipe_mod = @import("../recipe/root.zig");
const common = @import("common.zig");
const pipeline_mod = @import("pipeline/root.zig");
const step_mod = @import("step.zig");

var stop_requested = std.atomic.Value(bool).init(false);

/// Resets the stop flag and installs SIGINT handling before an execution run.
pub fn initializeStopHandling() void {
    stop_requested.store(false, .seq_cst);
    installSigintHandler();
}

/// Signal handler that asks the scheduler loop to stop at the next safe point.
fn sigintHandler(_: c_int) callconv(.c) void {
    stop_requested.store(true, .seq_cst);
}

/// Installs SIGINT handling on supported platforms.
fn installSigintHandler() void {
    if (@import("builtin").os.tag != .windows) {
        const act = std.posix.Sigaction{
            .handler = .{ .handler = sigintHandler },
            .mask = std.mem.zeroes(std.posix.sigset_t),
            .flags = 0,
        };
        _ = std.posix.sigaction(std.posix.SIG.INT, &act, null);
    }
}

/// Schedules tasks according to their period until a stop condition is reached.
pub fn runTasks(
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    opts: common.ExecOptions,
) !void {
    if (compiled_recipe.tasks.len == 0) return;

    const next_due = try allocator.alloc(i128, compiled_recipe.tasks.len);
    defer allocator.free(next_due);
    ctx.start_ns = std.time.nanoTimestamp();
    ctx.task_idx = 0;
    ctx.iteration = 0;
    for (next_due, compiled_recipe.tasks) |*due, t| {
        due.* = ctx.start_ns + @as(i128, t.every_ms) * 1_000_000;
    }

    var end_ns: ?i128 = if (opts.max_duration_ms) |ms| ctx.start_ns + @as(i128, ms) * 1_000_000 else null;
    if (compiled_recipe.stop_when.time_elapsed_ms) |ms| {
        const limit = ctx.start_ns + @as(i128, ms) * 1_000_000;
        if (end_ns) |current| {
            end_ns = if (limit < current) limit else current;
        } else {
            end_ns = limit;
        }
    }

    const ran_once = try allocator.alloc(bool, compiled_recipe.tasks.len);
    defer allocator.free(ran_once);
    @memset(ran_once, false);
    const task_runs = try allocator.alloc(u64, compiled_recipe.tasks.len);
    defer allocator.free(task_runs);
    @memset(task_runs, 0);

    var remaining_once = compiled_recipe.tasks.len;
    var total_runs: u64 = 0;
    const max_iterations = compiled_recipe.stop_when.max_iterations;

    var scratch = step_mod.StepScratch.init(allocator);
    defer scratch.deinit();

    const column_count = if (compiled_recipe.pipeline.record) |rec| switch (rec) {
        .explicit => |cols| cols.len,
        .all => 0,
    } else 0;

    while (true) {
        if (stop_requested.load(.seq_cst)) break;
        const now = std.time.nanoTimestamp();
        if (end_ns) |limit| if (now >= limit) break;
        if (max_iterations) |limit| if (total_runs >= limit) break;

        var best_idx: usize = 0;
        var best_time = next_due[0];
        for (next_due[1..], 1..) |due, idx| {
            if (due < best_time) {
                best_time = due;
                best_idx = idx;
            }
        }

        const now_after = std.time.nanoTimestamp();
        if (best_time > now_after) {
            const delta_ns: u64 = @intCast(best_time - now_after);
            std.Thread.sleep(delta_ns);
        }

        try runTask(allocator, compiled_recipe, best_idx, task_runs[best_idx], instruments, ctx, pipeline_runtime, opts.dry_run, &scratch, column_count);
        next_due[best_idx] += @as(i128, compiled_recipe.tasks[best_idx].every_ms) * 1_000_000;

        task_runs[best_idx] += 1;
        total_runs += 1;
        if (!ran_once[best_idx]) {
            ran_once[best_idx] = true;
            remaining_once -= 1;
        }
        if (end_ns == null and max_iterations == null and remaining_once == 0) break;
    }
}

/// Executes all steps for one task iteration.
fn runTask(
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    task_idx: usize,
    iteration: u64,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
    scratch: *step_mod.StepScratch,
    column_count: usize,
) !void {
    const task = compiled_recipe.tasks[task_idx];
    var frame_builder = try TaskFrameBuilder.init(allocator, task_idx, column_count);
    defer frame_builder.deinit();

    ctx.task_idx = task_idx;
    ctx.iteration = iteration;

    for (task.steps) |*step| {
        const instrument: ?*common.InstrumentRuntime = switch (step.action) {
            .instrument_call => |ic| &instruments[ic.instrument_idx],
            .compute => null,
        };
        const saved_value = try step_mod.executeStep(
            allocator,
            instrument,
            step,
            ctx,
            dry_run,
            pipeline_runtime.asyncLog(),
            scratch,
        );
        if (saved_value) |captured| {
            frame_builder.captureOwned(captured.column, captured.value_owned);
        }
    }

    if (frame_builder.finish(std.time.nanoTimestamp())) |frame| {
        var owned_frame = frame;
        if (!pipeline_runtime.publish(&owned_frame)) {
            stop_requested.store(true, .seq_cst);
            return;
        }
        std.debug.assert(owned_frame.values_owned == null);
    }
}

const TaskFrameBuilder = struct {
    allocator: std.mem.Allocator,
    task_idx: usize,
    values: []?[]u8,
    has_values: bool = false,

    fn init(allocator: std.mem.Allocator, task_idx: usize, column_count: usize) !TaskFrameBuilder {
        const values = try allocator.alloc(?[]u8, column_count);
        @memset(values, null);
        return .{
            .allocator = allocator,
            .task_idx = task_idx,
            .values = values,
        };
    }

    fn deinit(self: *TaskFrameBuilder) void {
        for (self.values) |v| if (v) |owned| self.allocator.free(owned);
        self.allocator.free(self.values);
    }

    fn captureOwned(self: *TaskFrameBuilder, column: usize, value_owned: []u8) void {
        if (self.values[column]) |old| self.allocator.free(old);
        self.values[column] = value_owned;
        self.has_values = true;
    }

    fn finish(self: *TaskFrameBuilder, timestamp_ns: i128) ?pipeline_mod.Frame {
        if (!self.has_values) return null;

        const frame_values = self.values;
        // Transfer ownership: allocate a fresh slate for builder reuse isn't needed
        // since the builder is per-iteration. Just null out our pointer.
        self.values = &.{};
        self.has_values = false;

        return .{
            .timestamp_ns = timestamp_ns,
            .task_idx = self.task_idx,
            .values_owned = frame_values,
        };
    }
};

test "task frame builder groups multiple saved values into one frame" {
    var builder = try TaskFrameBuilder.init(std.testing.allocator, 2, 2);
    defer builder.deinit();

    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.23"));
    builder.captureOwned(1, try std.testing.allocator.dupe(u8, "0.45"));

    var frame = builder.finish(123).?;
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(i128, 123), frame.timestamp_ns);
    try std.testing.expectEqual(@as(usize, 2), frame.task_idx);
    try std.testing.expectEqual(@as(usize, 2), frame.fieldCount());
    try std.testing.expectEqualStrings("1.23", frame.getColumn(0).?);
    try std.testing.expectEqualStrings("0.45", frame.getColumn(1).?);
}

test "task frame builder keeps the latest value for duplicate columns" {
    var builder = try TaskFrameBuilder.init(std.testing.allocator, 0, 1);
    defer builder.deinit();

    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.23"));
    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.24"));

    var frame = builder.finish(1).?;
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), frame.fieldCount());
    try std.testing.expectEqualStrings("1.24", frame.getColumn(0).?);
}

test "task frame builder captures saved value by ownership transfer" {
    const gpa = std.testing.allocator;

    var builder = try TaskFrameBuilder.init(gpa, 0, 1);
    defer builder.deinit();

    const saved = step_mod.SavedValue{
        .column = 0,
        .value_owned = try gpa.dupe(u8, "1.23"),
    };
    builder.captureOwned(saved.column, saved.value_owned);

    var frame = builder.finish(1).?;
    defer frame.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), frame.fieldCount());
    try std.testing.expectEqualStrings("1.23", frame.getColumn(0).?);
}

pub const SamplerState = struct {
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
    max_duration_ms: ?u64,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    result: ?anyerror = null,
};

pub fn runTasksThread(state: *SamplerState) void {
    state.result = null;
    runTasks(
        state.allocator,
        state.compiled_recipe,
        state.instruments,
        state.ctx,
        state.pipeline_runtime,
        .{
            .driver_dir = "",
            .recipe_path = "",
            .dry_run = state.dry_run,
            .max_duration_ms = state.max_duration_ms,
            .log = state.pipeline_runtime.log_writer,
        },
    ) catch |err| {
        state.result = err;
    };
    state.done.store(true, .seq_cst);
}
