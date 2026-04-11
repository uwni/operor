const std = @import("std");
const recipe_mod = @import("../recipe/root.zig");
const common = @import("common.zig");
const pipeline_mod = @import("pipeline/root.zig");
const step_mod = @import("step.zig");

var stop_requested: std.atomic.Value(bool) = .init(false);

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

/// Executes tasks sequentially in declaration order.
pub fn runTasks(
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
) !void {
    if (compiled_recipe.tasks.len == 0) return;

    ctx.start_ns = std.time.nanoTimestamp();
    ctx.task_idx = 0;
    ctx.iteration = 0;

    var scratch: step_mod.StepScratch = .init(allocator);
    defer scratch.deinit();

    const column_count = if (compiled_recipe.pipeline.record) |rec| switch (rec) {
        .explicit => |cols| cols.len,
        .all => 0,
    } else 0;

    for (compiled_recipe.tasks, 0..) |*task, task_idx| {
        if (stop_requested.load(.seq_cst)) break;
        if (shouldStop(compiled_recipe, ctx)) break;

        switch (task.*) {
            .sequential => {
                try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch, column_count);
                ctx.iteration += 1;
            },
            .conditional => |cond| {
                const is_true = cond.@"if".isTruthy(ctx.varResolver()) catch false;
                if (is_true) {
                    try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch, column_count);
                    ctx.iteration += 1;
                }
            },
            .loop => |loop_task| {
                while (true) {
                    if (stop_requested.load(.seq_cst)) break;
                    if (shouldStop(compiled_recipe, ctx)) break;

                    const is_true = loop_task.condition.isTruthy(ctx.varResolver()) catch false;
                    if (!is_true) break;

                    try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch, column_count);
                    ctx.iteration += 1;
                }
            },
        }
    }
}

/// Executes all steps for one task iteration.
fn runTask(
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    task_idx: usize,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
    scratch: *step_mod.StepScratch,
    column_count: usize,
) !void {
    const task = compiled_recipe.tasks[task_idx];
    var frame_builder: TaskFrameBuilder = try .init(allocator, column_count);
    defer frame_builder.deinit();

    ctx.task_idx = task_idx;

    for (task.steps()) |*step| {
        const instrument: ?*common.InstrumentRuntime = switch (step.action) {
            .instrument_call => |ic| &instruments[ic.instrument_idx],
            .compute, .sleep => null,
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

    if (frame_builder.finish()) |frame| {
        var owned_frame = frame;
        if (!pipeline_runtime.publish(&owned_frame)) {
            stop_requested.store(true, .seq_cst);
            return;
        }
        // After push, ownership has moved to the ring buffer slot.
    }
}

const TaskFrameBuilder = struct {
    allocator: std.mem.Allocator,
    values: []?[]u8,
    has_values: bool = false,

    fn init(allocator: std.mem.Allocator, column_count: usize) !TaskFrameBuilder {
        const values = try allocator.alloc(?[]u8, column_count);
        @memset(values, null);
        return .{
            .allocator = allocator,
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

    fn finish(self: *TaskFrameBuilder) ?pipeline_mod.Frame {
        if (!self.has_values) return null;

        const frame_values = self.values;
        // Transfer ownership: allocate a fresh slate for builder reuse isn't needed
        // since the builder is per-iteration. Just null out our pointer.
        self.values = &.{};
        self.has_values = false;

        return .{
            .values = frame_values,
        };
    }
};

test "task frame builder groups multiple saved values into one frame" {
    var builder: TaskFrameBuilder = try .init(std.testing.allocator, 2);
    defer builder.deinit();

    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.23"));
    builder.captureOwned(1, try std.testing.allocator.dupe(u8, "0.45"));

    var frame = builder.finish().?;
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), frame.fieldCount());
    try std.testing.expectEqualStrings("1.23", frame.getColumn(0).?);
    try std.testing.expectEqualStrings("0.45", frame.getColumn(1).?);
}

test "task frame builder keeps the latest value for duplicate columns" {
    var builder: TaskFrameBuilder = try .init(std.testing.allocator, 1);
    defer builder.deinit();

    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.23"));
    builder.captureOwned(0, try std.testing.allocator.dupe(u8, "1.24"));

    var frame = builder.finish().?;
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), frame.fieldCount());
    try std.testing.expectEqualStrings("1.24", frame.getColumn(0).?);
}

test "task frame builder captures saved value by ownership transfer" {
    const gpa = std.testing.allocator;

    var builder: TaskFrameBuilder = try .init(gpa, 1);
    defer builder.deinit();

    const saved = step_mod.SavedValue{
        .column = 0,
        .value_owned = try gpa.dupe(u8, "1.23"),
    };
    builder.captureOwned(saved.column, saved.value_owned);

    var frame = builder.finish().?;
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
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    result: ?anyerror = null,
};

fn shouldStop(compiled_recipe: *const recipe_mod.PrecompiledRecipe, ctx: *common.Context) bool {
    const stop_expr = compiled_recipe.stop_when orelse return false;
    return stop_expr.isTruthy(ctx.varResolver()) catch false;
}

pub fn runTasksThread(state: *SamplerState) void {
    state.result = null;
    runTasks(
        state.allocator,
        state.compiled_recipe,
        state.instruments,
        state.ctx,
        state.pipeline_runtime,
        state.dry_run,
    ) catch |err| {
        state.result = err;
    };
    state.done.store(true, .seq_cst);
}
