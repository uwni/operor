const std = @import("std");
const recipe_mod = @import("../recipe/root.zig");
const session = @import("session.zig");
const pipeline_mod = @import("pipeline/root.zig");
const step_mod = @import("step.zig");
const expr = @import("../expr.zig");

var stop_requested: std.atomic.Value(bool) = .init(false);

/// Resets the stop flag and installs SIGINT handling before an execution run.
pub fn initializeStopHandling() void {
    stop_requested.store(false, .seq_cst);
    installSigintHandler();
}

/// Signal handler that asks the scheduler loop to stop at the next safe point.
fn sigintHandler(_: std.posix.SIG) callconv(.c) void {
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
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
) !void {
    if (compiled_recipe.tasks.len == 0) return;

    ctx.start_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
    ctx.task_idx = 0;
    ctx.iteration = 0;

    var scratch: step_mod.StepScratch = .init(allocator);
    defer scratch.deinit();

    for (compiled_recipe.tasks, 0..) |*task, task_idx| {
        if (stop_requested.load(.seq_cst)) break;
        if (try shouldStop(compiled_recipe, ctx, allocator)) break;

        switch (task.*) {
            .sequential => {
                try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch);
                ctx.iteration += 1;
            },
            .conditional => |cond| {
                const is_true = try cond.@"if".isTruthy(ctx.varResolver(), allocator);
                if (is_true) {
                    try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch);
                    ctx.iteration += 1;
                }
            },
            .loop => |loop_task| {
                while (true) {
                    if (stop_requested.load(.seq_cst)) break;
                    if (try shouldStop(compiled_recipe, ctx, allocator)) break;

                    const is_true = try loop_task.condition.isTruthy(ctx.varResolver(), allocator);
                    if (!is_true) break;

                    try runTask(allocator, compiled_recipe, task_idx, instruments, ctx, pipeline_runtime, dry_run, &scratch);
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
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
    scratch: *step_mod.StepScratch,
) !void {
    const task = compiled_recipe.tasks[task_idx];

    ctx.task_idx = task_idx;

    for (task.steps()) |*step| {
        const instrument: ?*session.InstrumentRuntime = switch (step.action) {
            .instrument_call => |ic| &instruments[ic.instrument_idx],
            .compute, .sleep, .parallel => null,
        };
        var async_log = pipeline_runtime.asyncLog();
        try step_mod.executeStep(
            allocator,
            instrument,
            step,
            ctx,
            dry_run,
            async_log.logSink(),
            scratch,
            instruments,
            compiled_recipe.float_precision,
        );
    }

    if (try captureRecordFrame(allocator, compiled_recipe.record_bindings, ctx)) |frame| {
        var owned_frame = frame;
        if (!pipeline_runtime.publish(&owned_frame)) {
            stop_requested.store(true, .seq_cst);
            return;
        }
        // After push, ownership has moved to the ring buffer slot.
    }
}

fn captureRecordFrame(
    allocator: std.mem.Allocator,
    bindings: []const expr.VariableBinding,
    ctx: *const session.Context,
) !?pipeline_mod.Frame {
    if (bindings.len == 0) return null;

    const values = try allocator.alloc(?[]u8, bindings.len);
    @memset(values, null);
    errdefer {
        for (values) |value| if (value) |owned| allocator.free(owned);
        allocator.free(values);
    }

    for (bindings, 0..) |binding, idx| {
        values[idx] = try formatValueOwned(allocator, ctx.resolveBinding(binding));
    }

    return .{ .values = values };
}

fn formatValueOwned(allocator: std.mem.Allocator, value: session.Value) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    value.format(&out.writer) catch return error.OutOfMemory;
    return out.toOwnedSlice() catch error.OutOfMemory;
}

test "record frame samples bindings in column order" {
    const gpa = std.testing.allocator;
    const initial_values = [_]session.Value{
        .{ .int = 7 },
        .{ .bool = true },
    };
    var ctx: session.Context = try .init(gpa, std.testing.io, &initial_values, &.{});
    defer ctx.deinit();
    ctx.iteration = 3;
    ctx.task_idx = 2;

    const bindings = [_]expr.VariableBinding{
        .{ .slot = 0 },
        .{ .builtin = .iter },
        .{ .slot = 1 },
        .{ .builtin = .task_idx },
    };
    var frame = (try captureRecordFrame(gpa, &bindings, &ctx)).?;
    defer frame.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 4), frame.fieldCount());
    try std.testing.expectEqualStrings("7", frame.getColumn(0).?);
    try std.testing.expectEqualStrings("3", frame.getColumn(1).?);
    try std.testing.expectEqualStrings("true", frame.getColumn(2).?);
    try std.testing.expectEqualStrings("2", frame.getColumn(3).?);
}

test "record frame is omitted when no columns are configured" {
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{}, &.{});
    defer ctx.deinit();

    try std.testing.expect(try captureRecordFrame(std.testing.allocator, &.{}, &ctx) == null);
}

pub const SamplerState = struct {
    allocator: std.mem.Allocator,
    compiled_recipe: *const recipe_mod.PrecompiledRecipe,
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
    pipeline_runtime: *pipeline_mod.Runtime,
    dry_run: bool,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    result: ?anyerror = null,
};

fn shouldStop(compiled_recipe: *const recipe_mod.PrecompiledRecipe, ctx: *session.Context, allocator: std.mem.Allocator) !bool {
    const stop_expr = compiled_recipe.stop_when orelse return false;
    return try stop_expr.isTruthy(ctx.varResolver(), allocator);
}

pub fn runTasksThread(state: *SamplerState) void {
    defer state.done.store(true, .seq_cst);
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
}
