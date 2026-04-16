const std = @import("std");
const recipe_mod = @import("../recipe/root.zig");
const common = @import("common.zig");
const step_mod = @import("step.zig");
const visa = @import("../visa/root.zig");

const SavedValue = step_mod.SavedValue;

/// State machine phase for one inner step within a parallel block.
const Phase = enum {
    write_pending,
    write_waiting,
    query_delay_waiting,
    read_pending,
    read_waiting,
    sleep_waiting,
    done,
};

/// Tracks the runtime state of one inner step being executed asynchronously.
const SessionState = struct {
    step: *const recipe_mod.Step,
    phase: Phase,
    job_id: visa.ViJobId = 0,
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

/// Executes all inner steps of a parallel block concurrently using VISA async I/O.
///
/// Falls back to sequential execution when the VISA library does not expose
/// async primitives.
pub fn executeParallel(
    allocator: std.mem.Allocator,
    parallel: *const recipe_mod.Step.Parallel,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    dry_run: bool,
    log_sink: common.LogSink,
    scratch: *step_mod.StepScratch,
) !?SavedValue {
    const steps = parallel.steps;
    if (steps.len == 0) return null;

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

    // --- Check async support (use first instrument_call step as probe) ---
    if (!dry_run) {
        for (steps) |*s| {
            if (instrumentIdx(s)) |idx| {
                if (instruments[idx].handle) |*instr| {
                    if (!instr.hasAsyncSupport()) {
                        return executeSequentialFallback(allocator, parallel, instruments, ctx, dry_run, log_sink, scratch);
                    }
                }
                break;
            }
        }
    }

    if (dry_run) {
        return executeSequentialFallback(allocator, parallel, instruments, ctx, dry_run, log_sink, scratch);
    }

    // --- Cleanup registered before enabling so partial failures are covered ---
    defer {
        for (steps) |*s| {
            if (instrumentIdx(s)) |idx| {
                if (instruments[idx].handle) |*instr| {
                    instr.disableAsyncEvents() catch {};
                }
            }
        }
    }

    // --- Enable async events on all participating instruments ---
    for (steps) |*s| {
        if (instrumentIdx(s)) |idx| {
            if (instruments[idx].handle) |*instr| {
                instr.enableAsyncEvents() catch {
                    return executeSequentialFallback(allocator, parallel, instruments, ctx, dry_run, log_sink, scratch);
                };
            }
        }
    }

    // --- Initialize session states ---
    const states = try allocator.alloc(SessionState, steps.len);
    defer {
        for (states) |*st| {
            if (st.read_chunk_buf) |buf| allocator.free(buf);
            st.read_accum.deinit(allocator);
            if (st.rendered_command) |cmd| allocator.free(cmd);
        }
        allocator.free(states);
    }

    for (steps, 0..) |*s, i| {
        states[i] = .{
            .step = s,
            .phase = initialPhase(&s.action),
        };
        // Execute compute steps synchronously upfront (no I/O involved).
        if (s.action == .compute) {
            const comp = &s.action.compute;
            const result = step_mod.executeCompute(allocator, comp, ctx) catch null;
            states[i].result = result;
        }
    }

    // --- Poll loop ---
    var all_done = false;
    while (!all_done) {
        all_done = true;
        for (states) |*st| {
            if (st.phase == .done) continue;
            all_done = false;
            try advanceState(st, allocator, instruments, ctx, scratch);
        }
        if (!all_done) {
            // Yield to avoid busy-spinning.
            ctx.io.sleep(.fromNanoseconds(1_000_000), .awake) catch {}; // 1ms
        }
    }

    // --- Collect first saved value; free any extras ---
    var first_result: ?SavedValue = null;
    for (states) |*st| {
        if (st.result) |res| {
            if (first_result == null) {
                first_result = res;
            } else {
                allocator.free(res.value_owned);
            }
        }
    }
    return first_result;
}

/// Returns the initial phase for a step based on its action type.
fn initialPhase(action: *const recipe_mod.Step.Action) Phase {
    return switch (action.*) {
        .instrument_call => .write_pending,
        .sleep => .sleep_waiting,
        .compute, .parallel => .done, // compute is synchronous; nested parallel rejected at precompile
    };
}

/// Advances one session's state machine by one tick.
fn advanceState(
    st: *SessionState,
    allocator: std.mem.Allocator,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    scratch: *step_mod.StepScratch,
) !void {
    switch (st.phase) {
        .write_pending => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);

            scratch.reset();
            const alloc = scratch.tempAllocator();
            const resolved_args = try alloc.alloc(common.RenderValue, ic.args.len);
            for (ic.args, 0..) |arg, idx| {
                resolved_args[idx] = try step_mod.resolveStepArg(ctx, arg, alloc);
            }

            var render_stack_buf: [512]u8 = undefined;
            const rendered = try ic.command.render(allocator, render_stack_buf[0..], resolved_args, ic.command.instrument.write_termination);

            st.rendered_command = rendered.owned orelse try allocator.dupe(u8, rendered.bytes);

            st.job_id = try instr.writeAsync(st.rendered_command.?);
            st.phase = .write_waiting;
        },
        .write_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);

            const event = (try instr.waitEvent(0)) orelse return;
            defer instr.close(event) catch {};

            if (st.rendered_command) |cmd| {
                allocator.free(cmd);
                st.rendered_command = null;
            }

            if (ic.command.response != null) {
                if (instr.options.query_delay_ms > 0) {
                    st.wait_start_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
                    st.phase = .query_delay_waiting;
                } else {
                    st.phase = .read_pending;
                }
            } else {
                st.phase = .done;
            }
        },
        .query_delay_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);
            const now_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
            const target_ns: i96 = @as(i96, @intCast(instr.options.query_delay_ms)) * 1_000_000;
            if (now_ns - st.wait_start_ns >= target_ns) st.phase = .read_pending;
        },
        .read_pending => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);

            if (st.read_chunk_buf == null) {
                st.read_chunk_buf = try allocator.alloc(u8, instr.options.normalizedChunkSize());
            }

            st.job_id = try instr.readAsync(st.read_chunk_buf.?);
            st.phase = .read_waiting;
        },
        .read_waiting => {
            const ic = &st.step.action.instrument_call;
            const instr = &(instruments[ic.instrument_idx].handle orelse unreachable);

            const event = (try instr.waitEvent(0)) orelse return;
            const ret_count = instr.eventRetCount(event) catch |e| {
                instr.close(event) catch {};
                return e;
            };
            const has_more = instr.eventIoHasMore(event) catch |e| {
                instr.close(event) catch {};
                return e;
            };
            instr.close(event) catch {};

            if (ret_count > 0) {
                try st.read_accum.appendSlice(allocator, st.read_chunk_buf.?[0..ret_count]);
            }

            if (has_more) {
                // More data — issue another async read, stay in read_waiting.
                st.job_id = try instr.readAsync(st.read_chunk_buf.?);
                return;
            }

            // Read complete (VI_SUCCESS, VI_SUCCESS_TERM_CHAR, or error).
            try processReadResult(st, allocator, ic, instr, ctx);
            st.phase = .done;
        },
        .sleep_waiting => {
            const now_ns = std.Io.Timestamp.now(ctx.io, .awake).toNanoseconds();
            if (st.wait_start_ns == 0) st.wait_start_ns = now_ns;
            const target_ns: i96 = @as(i96, @intCast(st.step.action.sleep.duration_ms)) * 1_000_000;
            if (now_ns - st.wait_start_ns >= target_ns) st.phase = .done;
        },
        .done => {},
    }
}

/// Parses the accumulated read data and stores the result in the session state.
fn processReadResult(
    st: *SessionState,
    allocator: std.mem.Allocator,
    ic: *const recipe_mod.Step.InstrumentCall,
    instr: *const visa.Instrument,
    ctx: *common.Context,
) !void {
    // Trim read termination (same logic as Instrument.readToOwnedWithChunk).
    const term = instr.options.read_termination.constSlice();
    if (term.len > 0 and std.mem.endsWith(u8, st.read_accum.items, term)) {
        st.read_accum.items.len -= term.len;
    }

    const read_data = st.read_accum.items;
    const slot = ic.save_slot orelse return;
    const encoding = ic.command.response orelse return;
    const stored = try step_mod.parseResponse(encoding, read_data);
    const value: common.Value = switch (stored) {
        .raw => |v| .{ .string = v },
        .string => |v| .{ .string = v },
        .int => |v| .{ .int = v },
        .float => |v| .{ .float = v },
    };
    try ctx.setSlot(slot, value);

    const col = ic.save_column orelse return;
    const value_owned = try std.fmt.allocPrint(allocator, "{f}", .{value});
    st.result = .{ .column = col, .value_owned = value_owned };
}

/// Fallback: execute inner steps sequentially using the existing step executor.
fn executeSequentialFallback(
    allocator: std.mem.Allocator,
    parallel: *const recipe_mod.Step.Parallel,
    instruments: []common.InstrumentRuntime,
    ctx: *common.Context,
    dry_run: bool,
    log_sink: common.LogSink,
    scratch: *step_mod.StepScratch,
) anyerror!?SavedValue {
    var first_result: ?SavedValue = null;
    for (parallel.steps) |*s| {
        const instrument: ?*common.InstrumentRuntime = switch (s.action) {
            .instrument_call => |ic| &instruments[ic.instrument_idx],
            .compute, .sleep, .parallel => null,
        };
        const result = try step_mod.executeStep(allocator, instrument, s, ctx, dry_run, log_sink, scratch, instruments);
        if (result != null and first_result == null) {
            first_result = result;
        }
    }
    return first_result;
}

/// Extracts the instrument index from a step, if it's an instrument call.
fn instrumentIdx(step: *const recipe_mod.Step) ?usize {
    return switch (step.action) {
        .instrument_call => |ic| ic.instrument_idx,
        else => null,
    };
}
