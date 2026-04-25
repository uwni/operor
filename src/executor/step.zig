const std = @import("std");
const tty = @import("../tty.zig");
const instrument_types = @import("../instrument.zig");
const recipe_mod = @import("../recipe/root.zig");
const session = @import("session.zig");
const expr = @import("../expr.zig");
const parallel_mod = @import("parallel.zig");

const warn_tag = tty.styledText("[WARN]", .{.yellow});
const dry_run_tag = tty.styledText("[dry-run]", .{.fuchsia});

const command_stack_bytes: usize = 512;
/// Parsed response value in the native Zig type indicated by the command encoding.
pub const ParsedValue = union(instrument_types.Encoding) {
    raw: []const u8,
    float: f64,
    int: i64,
    string: []const u8,
    bool: bool,
};

pub const SavedValue = struct {
    column: usize,
    value_owned: []u8,
};

/// Reusable scratch space for step argument resolution, avoiding per-step HashMap allocation.
pub const StepScratch = struct {
    temp_arena: std.heap.ArenaAllocator,

    pub fn init(allocator: std.mem.Allocator) StepScratch {
        return .{
            .temp_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *StepScratch) void {
        self.temp_arena.deinit();
    }

    pub fn tempAllocator(self: *StepScratch) std.mem.Allocator {
        return self.temp_arena.allocator();
    }

    /// Clears resolved values and reuses retained arena capacity for temporary buffers.
    pub fn reset(self: *StepScratch) void {
        _ = self.temp_arena.reset(.retain_capacity);
    }
};

/// Renders, sends, and optionally parses the response for a single step.
/// Supports both instrument call steps and local compute steps.
pub fn executeStep(
    allocator: std.mem.Allocator,
    instrument: ?*session.InstrumentRuntime,
    step: *const recipe_mod.Step,
    ctx: *session.Context,
    dry_run: bool,
    log_sink: session.LogSink,
    scratch: *StepScratch,
    instruments: []session.InstrumentRuntime,
    float_precision: ?u8,
) !?SavedValue {
    // Evaluate optional `if` guard.
    if (step.@"if") |*if_expr| {
        const is_true = try if_expr.isTruthy(ctx.varResolver(), allocator);
        if (!is_true) return null;
    }

    return switch (step.action) {
        .instrument_call => |ic| executeInstrumentCall(allocator, instrument.?, &ic, ctx, dry_run, log_sink, scratch, float_precision),
        .compute => |comp| executeCompute(allocator, &comp, ctx),
        .sleep => |s| {
            ctx.io.sleep(.fromNanoseconds(@as(i96, s.duration_ms) * 1_000_000), .awake) catch {};
            return null;
        },
        .parallel => |p| parallel_mod.executeParallel(allocator, &p, instruments, ctx, dry_run, log_sink, scratch, float_precision),
    };
}

/// Evaluates a local compute expression and stores the result in the context.
pub fn executeCompute(
    allocator: std.mem.Allocator,
    comp: *const recipe_mod.Step.Compute,
    ctx: *session.Context,
) !?SavedValue {
    var eval_res = try comp.expression.eval(ctx.varResolver(), allocator);
    defer eval_res.deinit();
    const result = eval_res.value;

    try ctx.setSlot(comp.save_slot, switch (result) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s },
    });

    const save_column = comp.save_column orelse return null;

    // String for pipeline Frame.
    const value_owned = switch (result) {
        .int => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
        .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .string => |s| try allocator.dupe(u8, s),
    };

    return .{
        .column = save_column,
        .value_owned = value_owned,
    };
}

/// Sends an instrument command and optionally saves the parsed response.
fn executeInstrumentCall(
    allocator: std.mem.Allocator,
    instrument: *session.InstrumentRuntime,
    step: *const recipe_mod.Step.InstrumentCall,
    ctx: *session.Context,
    dry_run: bool,
    log_sink: session.LogSink,
    scratch: *StepScratch,
    float_precision: ?u8,
) !?SavedValue {
    const cmd = step.command;
    const adapter_name = cmd.instrument.adapter_name;

    scratch.reset();
    const alloc = scratch.tempAllocator();
    const resolved_args = try alloc.alloc(session.RenderValue, step.args.len);
    for (step.args, 0..) |arg, idx| {
        resolved_args[idx] = try resolveStepArg(ctx, arg, alloc);
    }

    var render_stack_buf: [command_stack_bytes]u8 = undefined;
    const write_termination = step.command.instrument.write_termination;
    var rendered = cmd.render(allocator, render_stack_buf[0..], resolved_args, write_termination, float_precision) catch |err| switch (err) {
        error.MissingVariable => {
            var warning_buf: [160]u8 = undefined;
            const warning = try std.fmt.bufPrint(warning_buf[0..], "missing template variable for call {s}", .{step.call});
            logWarning(log_sink, warning);
            return null;
        },
        else => return err,
    };
    defer rendered.deinit(allocator);

    if (dry_run) {
        logDryRun(log_sink, adapter_name, rendered.bytes);
        return null;
    }

    const instr = &(instrument.handle orelse unreachable);
    instr.write(rendered.bytes) catch |err| {
        var warning_buf: [192]u8 = undefined;
        const warning = try std.fmt.bufPrint(warning_buf[0..], "write failed {s}: {any}", .{ adapter_name, err });
        logWarning(log_sink, warning);
        return null;
    };

    if (cmd.response) |encoding| {
        instr.waitQueryDelay();
        const resp = instr.readToOwned(allocator) catch |err| {
            var warning_buf: [192]u8 = undefined;
            const warning = try std.fmt.bufPrint(warning_buf[0..], "read failed {s}: {any}", .{ adapter_name, err });
            logWarning(log_sink, warning);
            return null;
        };
        errdefer allocator.free(resp);
        if (step.save_slot) |slot| {
            defer allocator.free(resp);
            const stored = try parseResponse(encoding, resp, cmd.instrument.bool_map);
            const value = switch (stored) {
                .raw => |v| session.Value{ .string = v },
                .string => |v| session.Value{ .string = v },
                .int => |v| session.Value{ .int = v },
                .float => |v| session.Value{ .float = v },
                .bool => |v| session.Value{ .bool = v },
            };
            try ctx.setSlot(slot, value);

            const save_column = step.save_column orelse return null;
            const stored_value_owned = try std.fmt.allocPrint(allocator, "{f}", .{value});

            return .{
                .column = save_column,
                .value_owned = stored_value_owned,
            };
        } else {
            defer allocator.free(resp);
            return null;
        }
    }

    return null;
}

/// Convert the raw response byte string into a ParsedValue according to the specified encoding.
pub fn parseResponse(
    encoding: instrument_types.Encoding,
    resp: []const u8,
    bool_map: ?recipe_mod.BoolTextMap,
) !ParsedValue {
    const trimmed = std.mem.trim(u8, resp, &std.ascii.whitespace);
    return switch (encoding) {
        .raw => .{ .raw = resp },
        .float => .{ .float = try std.fmt.parseFloat(f64, trimmed) },
        .int => .{ .int = try std.fmt.parseInt(i64, trimmed, 10) },
        .string => .{ .string = trimmed },
        .bool => .{ .bool = try parseBoolResponse(trimmed, bool_map) },
    };
}

fn parseBoolResponse(trimmed: []const u8, bool_map: ?recipe_mod.BoolTextMap) !bool {
    if (bool_map) |map| {
        if (std.ascii.eqlIgnoreCase(trimmed, map.true_text)) return true;
        if (std.ascii.eqlIgnoreCase(trimmed, map.false_text)) return false;
        return error.InvalidBoolResponse;
    }

    // No mapping configured: preserve legacy behavior.
    return trimmed.len > 0 and trimmed[0] == '1';
}

fn evalToValue(
    e: *const expr.Expression,
    ctx: *const session.Context,
    allocator: std.mem.Allocator,
) !session.Value {
    const result = try e.eval(ctx.varResolver(), allocator);
    // Owned strings (if any) live in `allocator` — caller manages lifetime.
    return switch (result.value) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s },
    };
}

pub fn resolveStepArg(
    ctx: *const session.Context,
    value: recipe_mod.StepArg,
    allocator: std.mem.Allocator,
) !session.RenderValue {
    return switch (value) {
        .scalar => |e| .{ .scalar = try evalToValue(&e, ctx, allocator) },
        .list => |items| blk: {
            const resolved = try allocator.alloc(session.Value, items.len);
            for (items, 0..) |*item, idx| {
                resolved[idx] = try evalToValue(item, ctx, allocator);
            }
            break :blk .{ .list = resolved };
        },
    };
}

fn logWarning(log_sink: session.LogSink, message: []const u8) void {
    var buf: [512]u8 = undefined;
    const text = std.fmt.bufPrint(&buf, warn_tag ++ " {s}\n", .{message}) catch return;
    log_sink.writeAll(text);
}

fn logDryRun(log_sink: session.LogSink, adapter_name: []const u8, rendered: []const u8) void {
    var buf: [1024]u8 = undefined;
    const text = std.fmt.bufPrint(&buf, dry_run_tag ++ " {s} -> {s}\n", .{ adapter_name, rendered }) catch return;
    log_sink.writeAll(text);
}

test "executor parse response" {
    const raw = "  2.5 \n";
    const parsed = try parseResponse(.float, raw, null);
    switch (parsed) {
        .float => |value| try std.testing.expectApproxEqAbs(@as(f64, 2.5), value, 1e-9),
        else => return error.TestUnexpectedResult,
    }

    const parsed_int = try parseResponse(.int, "7", null);
    switch (parsed_int) {
        .int => |value| try std.testing.expectEqual(@as(i64, 7), value),
        else => return error.TestUnexpectedResult,
    }

    const parsed_bool_legacy_true = try parseResponse(.bool, "1", null);
    switch (parsed_bool_legacy_true) {
        .bool => |value| try std.testing.expect(value),
        else => return error.TestUnexpectedResult,
    }

    const parsed_bool_legacy_false = try parseResponse(.bool, "ON", null);
    switch (parsed_bool_legacy_false) {
        .bool => |value| try std.testing.expect(!value),
        else => return error.TestUnexpectedResult,
    }

    const parsed_bool_custom = try parseResponse(.bool, "ENABLE", .{ .true_text = "ENABLE", .false_text = "DISABLE" });
    switch (parsed_bool_custom) {
        .bool => |value| try std.testing.expect(value),
        else => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(error.InvalidBoolResponse, parseResponse(.bool, "ON", .{ .true_text = "ENABLE", .false_text = "DISABLE" }));
}
