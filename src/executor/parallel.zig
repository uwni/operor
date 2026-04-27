const std = @import("std");
const recipe_mod = @import("../recipe/root.zig");
const session = @import("session.zig");
const step_mod = @import("step.zig");
const visa = @import("../visa/root.zig");

const SavedValue = step_mod.SavedValue;
const ns_per_ms: i96 = 1_000_000;

const Completion = struct {
    ret_count: visa.ViUInt32,
    io_status: visa.ViStatus,
};

/// Shared completion facts written by the VISA callback and consumed by the main thread.
const CompletionSlot = struct {
    ret_count: std.atomic.Value(visa.ViUInt32) = .init(0),
    io_status: std.atomic.Value(visa.ViStatus) = .init(visa.c.VI_SUCCESS),
    ready: std.atomic.Value(bool) = .init(false),

    fn prepare(self: *CompletionSlot) void {
        self.ret_count.store(0, .monotonic);
        self.io_status.store(visa.c.VI_SUCCESS, .monotonic);
        self.ready.store(false, .monotonic);
    }

    fn take(self: *CompletionSlot) ?Completion {
        if (!self.ready.load(.acquire)) return null;
        const completion: Completion = .{
            .ret_count = self.ret_count.load(.monotonic),
            .io_status = self.io_status.load(.monotonic),
        };
        self.ready.store(false, .monotonic);
        return completion;
    }
};

const HandlerContext = struct {
    event: *std.Io.Event,
    io: std.Io,
    slot: *CompletionSlot,
    session_handle: visa.ViSession,
    vtable: *const visa.loader.Vtable,
};

/// VISA I/O completion handler — called from a VISA-internal thread.
/// Captures completion facts, then wakes the main thread to advance the state machine.
fn ioCompletionHandler(
    vi: visa.c.ViSession,
    event_type: visa.c.ViEventType,
    event: visa.c.ViEvent,
    user_handle: visa.c.ViAddr,
) callconv(.c) visa.c.ViStatus {
    const handler: *HandlerContext = @ptrCast(@alignCast(user_handle));
    if (event_type != visa.c.VI_EVENT_IO_COMPLETION or vi != handler.session_handle) {
        return visa.c.VI_SUCCESS;
    }

    var ret_count: visa.ViUInt32 = undefined;
    var io_status: visa.ViStatus = undefined;
    const ret_status = handler.vtable.viGetAttribute(event, visa.c.VI_ATTR_RET_COUNT_32, @ptrCast(&ret_count));
    const io_attr_status = handler.vtable.viGetAttribute(event, visa.c.VI_ATTR_STATUS, @ptrCast(&io_status));

    handler.slot.ret_count.store(ret_count, .monotonic);
    handler.slot.io_status.store(if (ret_status < visa.c.VI_SUCCESS) ret_status else if (io_attr_status < visa.c.VI_SUCCESS) io_attr_status else io_status, .monotonic);
    handler.slot.ready.store(true, .release);
    handler.event.set(handler.io);
    return visa.c.VI_SUCCESS;
}

/// State machine phase for one inner step within a parallel block.
const Phase = enum {
    write_waiting,
    query_delay_waiting,
    read_waiting,
    sleep_waiting,
    done,
};

/// Tracks the runtime state of one inner step being executed asynchronously.
const SessionState = struct {
    step: *const recipe_mod.Step,
    phase: Phase,
    guard_passed: bool = true,
    handler_installed: bool = false,
    handler_events_enabled: bool = false,
    completion: ?*CompletionSlot = null,
    /// VISA async job identifier for the current in-flight write/read, if any.
    job_id: ?visa.ViJobId = null,
    /// Chunk buffer for async reads (allocated once, reused for multi-chunk).
    read_chunk_buf: ?[]u8 = null,
    /// Accumulates multi-chunk async read responses.
    read_accum: std.ArrayList(u8) = .empty,
    /// Timestamp (ns) when a time-based wait started (sleep or query delay).
    wait_start_ns: i96 = 0,
    /// Saved value produced by this step (if any).
    result: ?SavedValue = null,
    /// Rendered command bytes kept alive until write completes.
    rendered_command: ?[]u8 = null,
};

const WaitPlan = struct {
    has_io_waiting: bool = false,
    min_remaining_ns: i96 = std.math.maxInt(i96),

    fn observeRemaining(self: *WaitPlan, remaining_ns: i96) void {
        if (remaining_ns <= 0) {
            self.min_remaining_ns = 0;
        } else if (remaining_ns < self.min_remaining_ns) {
            self.min_remaining_ns = remaining_ns;
        }
    }

    fn hasTimer(self: WaitPlan) bool {
        return self.min_remaining_ns < std.math.maxInt(i96);
    }

    fn timeout(self: WaitPlan) std.Io.Timeout {
        if (!self.hasTimer()) return .none;
        return .{ .duration = .{ .raw = .fromNanoseconds(self.min_remaining_ns), .clock = .awake } };
    }
};

/// Executes all inner steps of a parallel block concurrently using VISA async I/O.
/// In dry-run mode, steps are executed sequentially instead.
pub fn executeParallel(
    allocator: std.mem.Allocator,
    parallel: *const recipe_mod.Step.Parallel,
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
    dry_run: bool,
    log_sink: session.LogSink,
    scratch: *step_mod.StepScratch,
    float_precision: ?u8,
) !step_mod.SavedValues {
    const steps = parallel.steps;
    if (steps.len == 0) return .{};

    // --- Duplicate instrument check ---
    for (steps, 0..) |*s, i| {
        const idx_i = instrumentIdx(s) orelse continue;
        for (steps[i + 1 ..]) |*t| {
            const idx_j = instrumentIdx(t) orelse continue;
            if (idx_i == idx_j) {
                return error.DuplicateInstrumentInParallel;
            }
        }
    }

    if (dry_run) {
        return executeSequential(allocator, parallel, instruments, ctx, dry_run, log_sink, scratch, float_precision);
    }

    // --- Initialize session states ---
    const completion_slots = try allocator.alloc(CompletionSlot, steps.len);
    defer allocator.free(completion_slots);
    const handler_contexts = try allocator.alloc(HandlerContext, steps.len);
    defer allocator.free(handler_contexts);
    const states = try allocator.alloc(SessionState, steps.len);

    for (steps, 0..) |*s, i| {
        completion_slots[i] = .{};
        states[i] = .{
            .step = s,
            .phase = .done,
            .guard_passed = true,
            .handler_installed = false,
            .handler_events_enabled = false,
            .completion = if (instrumentIdx(s) != null) &completion_slots[i] else null,
        };
    }

    defer {
        for (states) |*st| {
            if (st.read_chunk_buf) |buf| allocator.free(buf);
            st.read_accum.deinit(allocator);
            if (st.rendered_command) |cmd| allocator.free(cmd);
            if (st.result) |saved| allocator.free(saved.value_owned);
        }
        allocator.free(states);
    }
    errdefer cancelActiveJobs(states, instruments);

    for (steps, 0..) |*s, i| {
        if (s.@"if") |*if_expr| {
            const is_true = try if_expr.isTruthy(ctx.varResolver(), allocator);
            if (!is_true) {
                states[i].guard_passed = false;
                continue;
            }
        }

        switch (s.action) {
            .compute => {
                const comp = &s.action.compute;
                states[i].result = try step_mod.executeCompute(allocator, comp, ctx);
            },
            .sleep => {
                states[i].phase = .sleep_waiting;
                states[i].wait_start_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
            },
            else => {},
        }
    }

    // --- Install wakeup handlers ---
    // The handler records completion facts in a per-session slot, then wakes the main thread.
    var wake_event: std.Io.Event = .unset;
    defer for (steps, 0..) |*s, i| {
        if (instrumentIdx(s)) |idx| {
            if (instruments[idx].handle) |*instr| {
                if (states[i].handler_installed) {
                    instr.uninstallHandler(ioCompletionHandler, @ptrCast(&handler_contexts[i])) catch {};
                }
                if (states[i].handler_events_enabled) {
                    instr.disableHandlerEvents() catch {};
                }
            }
        }
    };
    for (steps, 0..) |*s, i| {
        if (!states[i].guard_passed) continue;
        if (instrumentIdx(s)) |idx| {
            if (instruments[idx].handle) |*instr| {
                handler_contexts[i] = .{
                    .event = &wake_event,
                    .io = ctx.io,
                    .slot = states[i].completion orelse unreachable,
                    .session_handle = instr.instrument,
                    .vtable = instr.vtable,
                };
                try instr.enableHandlerEvents();
                states[i].handler_events_enabled = true;
                try instr.installHandler(ioCompletionHandler, @ptrCast(&handler_contexts[i]));
                states[i].handler_installed = true;
            }
        }
    }

    // --- Kick off initial writes ---
    // All handlers are installed before any write starts, so every completion will be delivered.
    for (steps, 0..) |*s, i| {
        if (!states[i].guard_passed) continue;
        switch (s.action) {
            .instrument_call => |*ic| {
                const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
                var render_stack_buf: [step_mod.command_stack_bytes]u8 = undefined;
                var rendered = try step_mod.renderInstrumentCall(allocator, ic, ctx, scratch, render_stack_buf[0..], float_precision);
                defer rendered.deinit(allocator);
                states[i].rendered_command = if (rendered.owned) |owned| blk: {
                    rendered.owned = null;
                    break :blk owned;
                } else try allocator.dupe(u8, rendered.bytes);
                (states[i].completion orelse unreachable).prepare();
                states[i].job_id = try instr.writeAsync(states[i].rendered_command.?);
                states[i].phase = .write_waiting;
            },
            else => {},
        }
    }

    // --- Poll loop ---
    while (true) {
        var all_done = true;
        for (states) |*st| {
            if (st.phase == .done) continue;
            all_done = false;
            try advanceState(st, allocator, instruments, ctx);
        }
        if (all_done) break;

        try waitForProgress(ctx, &wake_event, computeWaitPlan(states, instruments, ctx));
    }

    // --- Collect saved values from all inner steps ---
    var saved_values: step_mod.SavedValues = .{};
    errdefer saved_values.deinit(allocator);
    for (states) |*st| {
        if (st.result) |res| {
            try saved_values.items.append(allocator, res);
            st.result = null;
        }
    }
    return saved_values;
}

fn computeWaitPlan(
    states: []SessionState,
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
) WaitPlan {
    var plan: WaitPlan = .{};
    const now_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();

    for (states) |*st| {
        switch (st.phase) {
            .write_waiting, .read_waiting => plan.has_io_waiting = true,
            .sleep_waiting => plan.observeRemaining(remainingNs(st.wait_start_ns, now_ns, st.step.action.sleep.duration_ms)),
            .query_delay_waiting => {
                const ic = &st.step.action.instrument_call;
                const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
                plan.observeRemaining(remainingNs(st.wait_start_ns, now_ns, instr.options.query_delay_ms));
            },
            else => {},
        }
    }

    return plan;
}

fn remainingNs(start_ns: i96, now_ns: i96, duration_ms: anytype) i96 {
    const target_ns: i96 = @as(i96, @intCast(duration_ms)) * ns_per_ms;
    return target_ns - (now_ns - start_ns);
}

fn waitForProgress(ctx: *session.Context, wake_event: *std.Io.Event, plan: WaitPlan) !void {
    if (plan.has_io_waiting) {
        wake_event.waitTimeout(ctx.io, plan.timeout()) catch |err| switch (err) {
            error.Timeout => {},
            error.Canceled => return error.Canceled,
        };
        wake_event.reset();
    } else if (plan.hasTimer() and plan.min_remaining_ns > 0) {
        // Timer-only: no one will set the event; just sleep for the remaining duration.
        try ctx.io.sleep(.fromNanoseconds(plan.min_remaining_ns), .awake);
    }
}

/// Advances one session's state machine by one tick.
fn advanceState(
    st: *SessionState,
    allocator: std.mem.Allocator,
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
) !void {
    switch (st.phase) {
        .write_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
            const completion = (st.completion orelse return error.UnexpectedAsyncCompletion).take() orelse return;
            st.job_id = null;
            try visa.checkStatus(completion.io_status);

            if (st.rendered_command) |cmd| {
                allocator.free(cmd);
                st.rendered_command = null;
            }

            if (ic.command.response != null) {
                if (instr.options.query_delay_ms > 0) {
                    st.wait_start_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
                    st.phase = .query_delay_waiting;
                } else {
                    try startRead(st, allocator, instr);
                }
            } else {
                st.phase = .done;
            }
        },
        .query_delay_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
            const now_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
            if (remainingNs(st.wait_start_ns, now_ns, instr.options.query_delay_ms) <= 0) {
                try startRead(st, allocator, instr);
            }
        },
        .read_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
            const completion = (st.completion orelse return error.UnexpectedAsyncCompletion).take() orelse return;
            st.job_id = null;
            const has_more = switch (completion.io_status) {
                visa.c.VI_SUCCESS_MAX_CNT => true,
                else => blk: {
                    try visa.checkStatus(completion.io_status);
                    break :blk false;
                },
            };

            if (completion.ret_count > 0) {
                try st.read_accum.appendSlice(allocator, st.read_chunk_buf.?[0..completion.ret_count]);
            }

            if (has_more) {
                // More data — issue another async read, stay in read_waiting.
                try startRead(st, allocator, instr);
                return;
            }

            // Read complete (VI_SUCCESS, VI_SUCCESS_TERM_CHAR, or error).
            try processReadResult(st, allocator, ic, instr, ctx);
            st.phase = .done;
        },
        .sleep_waiting => {
            const now_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
            if (remainingNs(st.wait_start_ns, now_ns, st.step.action.sleep.duration_ms) <= 0) st.phase = .done;
        },
        .done => {},
    }
}

fn startRead(st: *SessionState, allocator: std.mem.Allocator, instr: *const visa.Instrument) !void {
    if (st.read_chunk_buf == null) {
        st.read_chunk_buf = try allocator.alloc(u8, instr.options.normalizedChunkSize());
    }
    (st.completion orelse return error.UnexpectedAsyncCompletion).prepare();
    st.job_id = try instr.readAsync(st.read_chunk_buf.?);
    st.phase = .read_waiting;
}

fn cancelActiveJobs(states: []SessionState, instruments: []session.InstrumentRuntime) void {
    for (states) |*st| {
        const job_id = st.job_id orelse continue;
        switch (st.step.action) {
            .instrument_call => |ic| {
                if (instruments[ic.instrument_idx].handle) |*instr| {
                    instr.terminate(job_id) catch {};
                }
            },
            else => {},
        }
        st.job_id = null;
    }
}

/// Parses the accumulated read data and stores the result in the session state.
fn processReadResult(
    st: *SessionState,
    allocator: std.mem.Allocator,
    ic: *const recipe_mod.Step.InstrumentCall,
    instr: *const visa.Instrument,
    ctx: *session.Context,
) !void {
    // Trim read termination (same logic as Instrument.readToOwnedWithChunk).
    const term = instr.options.read_termination.constSlice();
    if (term.len > 0 and std.mem.endsWith(u8, st.read_accum.items, term)) {
        st.read_accum.items.len -= term.len;
    }

    st.result = try step_mod.storeInstrumentResponse(allocator, ic, ctx, st.read_accum.items);
}

/// Executes inner steps sequentially; used for dry-run.
fn executeSequential(
    allocator: std.mem.Allocator,
    parallel: *const recipe_mod.Step.Parallel,
    instruments: []session.InstrumentRuntime,
    ctx: *session.Context,
    dry_run: bool,
    log_sink: session.LogSink,
    scratch: *step_mod.StepScratch,
    float_precision: ?u8,
) anyerror!step_mod.SavedValues {
    var saved_values: step_mod.SavedValues = .{};
    errdefer saved_values.deinit(allocator);
    for (parallel.steps) |*s| {
        const instrument: ?*session.InstrumentRuntime = switch (s.action) {
            .instrument_call => |ic| &instruments[ic.instrument_idx],
            .compute, .sleep, .parallel => null,
        };
        var result = try step_mod.executeStep(allocator, instrument, s, ctx, dry_run, log_sink, scratch, instruments, float_precision);
        errdefer result.deinit(allocator);
        try saved_values.items.appendSlice(allocator, result.items.items);
        result.items.clearRetainingCapacity();
        result.deinit(allocator);
    }
    return saved_values;
}

/// Extracts the instrument index from a step, if it's an instrument call.
fn instrumentIdx(step: *const recipe_mod.Step) ?usize {
    return switch (step.action) {
        .instrument_call => |ic| ic.instrument_idx,
        else => null,
    };
}
