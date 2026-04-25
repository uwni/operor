const std = @import("std");
const tty = @import("../tty.zig");
const instrument_types = @import("../instrument.zig");
const recipe_mod = @import("../recipe/root.zig");
const session = @import("session.zig");
const expr = @import("../expr.zig");
const parallel_mod = @import("parallel.zig");

const dry_run_tag = tty.styledText("[dry-run]", .{.fuchsia});

pub const command_stack_bytes: usize = 512;
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
            try ctx.io.sleep(.fromNanoseconds(@as(i96, s.duration_ms) * 1_000_000), .awake);
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

    const value: session.Value = switch (result) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s },
    };
    try ctx.setSlot(comp.save_slot, value);
    return try saveColumnValue(allocator, comp.save_column, value);
}

pub fn renderInstrumentCall(
    allocator: std.mem.Allocator,
    step: *const recipe_mod.Step.InstrumentCall,
    ctx: *const session.Context,
    scratch: *StepScratch,
    stack_buffer: []u8,
    float_precision: ?u8,
) !recipe_mod.RenderedCommand {
    scratch.reset();
    const alloc = scratch.tempAllocator();
    const resolved_args = try alloc.alloc(session.RenderValue, step.args.len);
    for (step.args, 0..) |arg, idx| {
        resolved_args[idx] = try resolveStepArg(ctx, arg, alloc);
    }

    return try step.command.render(
        allocator,
        stack_buffer,
        resolved_args,
        step.command.instrument.write_termination,
        float_precision,
    );
}

pub fn storeInstrumentResponse(
    allocator: std.mem.Allocator,
    step: *const recipe_mod.Step.InstrumentCall,
    ctx: *session.Context,
    resp: []const u8,
) !?SavedValue {
    const slot = step.save_slot orelse return null;
    const encoding = step.command.response orelse return null;

    const stored = try parseResponse(encoding, resp, step.command.instrument.bool_map);
    const value = parsedValueToSessionValue(stored);
    try ctx.setSlot(slot, value);
    return try saveColumnValue(allocator, step.save_column, value);
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
    var render_stack_buf: [command_stack_bytes]u8 = undefined;
    var rendered = try renderInstrumentCall(allocator, step, ctx, scratch, render_stack_buf[0..], float_precision);
    defer rendered.deinit(allocator);

    if (dry_run) {
        logDryRun(log_sink, step.command.instrument.adapter_name, rendered.bytes);
        return null;
    }

    const instr = &(instrument.handle orelse unreachable);
    try instr.write(rendered.bytes);

    if (step.command.response != null) {
        if (instr.options.query_delay_ms > 0) {
            try ctx.io.sleep(.fromMilliseconds(@as(i64, instr.options.query_delay_ms)), .awake);
        }
        const resp = try instr.readToOwned(allocator);
        defer allocator.free(resp);
        return try storeInstrumentResponse(allocator, step, ctx, resp);
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

fn parsedValueToSessionValue(stored: ParsedValue) session.Value {
    return switch (stored) {
        .raw => |v| .{ .string = v },
        .string => |v| .{ .string = v },
        .int => |v| .{ .int = v },
        .float => |v| .{ .float = v },
        .bool => |v| .{ .bool = v },
    };
}

fn saveColumnValue(
    allocator: std.mem.Allocator,
    save_column: ?usize,
    value: session.Value,
) !?SavedValue {
    const column = save_column orelse return null;
    return .{
        .column = column,
        .value_owned = try std.fmt.allocPrint(allocator, "{f}", .{value}),
    };
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
