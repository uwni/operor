const std = @import("std");
const tty = @import("../tty.zig");
const instrument_types = @import("../instrument.zig");
const recipe_mod = @import("../recipe/root.zig");
const session = @import("session.zig");
const expr = @import("../expr.zig");
const parallel_mod = @import("parallel.zig");

const dry_run_tag = tty.styledText("[dry-run]", .{.fuchsia});

pub const command_stack_bytes: usize = 512;

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
) !void {
    // Evaluate optional `if` guard.
    if (step.@"if") |*if_expr| {
        const is_true = try if_expr.isTruthy(ctx.varResolver(), allocator);
        if (!is_true) return;
    }

    switch (step.action) {
        .instrument_call => |ic| try executeInstrumentCall(allocator, instrument.?, &ic, ctx, dry_run, log_sink, scratch, float_precision),
        .compute => |comp| try executeCompute(allocator, &comp, ctx),
        .sleep => |s| {
            try ctx.io.sleep(.fromNanoseconds(@as(i96, s.duration_ms) * 1_000_000), .awake);
        },
        .parallel => |p| try parallel_mod.executeParallel(allocator, &p, instruments, ctx, dry_run, log_sink, scratch, float_precision),
    }
}

/// Evaluates a local compute expression and stores the result in the context.
pub fn executeCompute(
    allocator: std.mem.Allocator,
    comp: *const recipe_mod.Step.Compute,
    ctx: *session.Context,
) !void {
    var eval_res = try comp.expression.eval(ctx.varResolver(), allocator);
    defer eval_res.deinit();
    const result = eval_res.value;

    const value: session.Value = switch (result) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = session.Value.String.borrow(s) },
    };
    try ctx.setSlot(comp.save_slot, value);
}

/// Resolves step arguments against the current context and renders the precompiled command.
/// Returned bytes may borrow `stack_buffer`, so callers must keep it alive until `deinit`.
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
    const resolved_args = try alloc.alloc(session.Value, step.args.len);
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

/// Parses and stores a response only when both the recipe assignment and adapter response exist.
pub fn storeInstrumentResponse(
    step: *const recipe_mod.Step.InstrumentCall,
    ctx: *session.Context,
    response_bytes: []const u8,
) !void {
    const save = step.save_result orelse return;
    const response_spec = step.command.response orelse return;
    const bool_map = step.command.instrument.bool_map;

    switch (save) {
        .scalar => |slot| try parseResponseIntoSlot(response_spec, response_bytes, bool_map, ctx, slot),
        .object => |object_field_slots| {
            const object_spec = switch (response_spec) {
                .object => |t| t,
                else => return error.ResponseSpecMismatch,
            };
            try parseObjectResponseIntoSlots(ctx, object_field_slots, object_spec, response_bytes, bool_map);
        },
    }
}

fn parseObjectResponseIntoSlots(
    ctx: *session.Context,
    object_field_slots: []const usize,
    spec: instrument_types.ObjectResponseSpec,
    response_bytes: []const u8,
    bool_map: ?recipe_mod.BoolTextMap,
) !void {
    var remaining = std.mem.trim(u8, response_bytes, &std.ascii.whitespace);
    var field_idx: usize = 0;
    // Pending field waiting to learn its right boundary from the next literal.
    var pending: ?instrument_types.ObjectField = null;

    for (spec.segments) |seg| switch (seg) {
        .literal => |lit| {
            if (pending) |f| {
                // This literal terminates the pending field — find, capture, consume both.
                const sep_idx = findSeparatorOutsideQuotes(remaining, lit) orelse return error.ObjectResponseMismatch;
                const parsed = try parseScalarResponseValue(f.encoding, std.mem.trim(u8, remaining[0..sep_idx], &std.ascii.whitespace), bool_map);
                try ctx.setSlot(object_field_slots[field_idx], parsed);
                field_idx += 1;
                remaining = remaining[sep_idx + lit.len ..];
                pending = null;
            } else {
                // Prefix literal — match and consume.
                if (!std.mem.startsWith(u8, remaining, lit)) return error.ObjectResponseMismatch;
                remaining = remaining[lit.len..];
            }
        },
        .field => |f| pending = f,
    };

    // Last field has no following literal: rest of remaining is its value.
    if (pending) |f| {
        const parsed = try parseScalarResponseValue(f.encoding, std.mem.trim(u8, remaining, &std.ascii.whitespace), bool_map);
        try ctx.setSlot(object_field_slots[field_idx], parsed);
    }
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
) !void {
    var render_stack_buf: [command_stack_bytes]u8 = undefined;
    var rendered = try renderInstrumentCall(allocator, step, ctx, scratch, render_stack_buf[0..], float_precision);
    defer rendered.deinit(allocator);

    if (dry_run) {
        logDryRun(log_sink, step.command.instrument.adapter_name, rendered.bytes);
        return;
    }

    const instr = &(instrument.handle orelse unreachable);
    if (step.command.response != null) {
        const response_bytes = try instr.queryToOwned(allocator, rendered.bytes);
        defer allocator.free(response_bytes);
        return try storeInstrumentResponse(step, ctx, response_bytes);
    }

    try instr.write(rendered.bytes);
}

/// Parses the raw response byte string and stores it into the target runtime slot.
pub fn parseResponseIntoSlot(
    response_spec: instrument_types.ResponseSpec,
    response_bytes: []const u8,
    bool_map: ?recipe_mod.BoolTextMap,
    ctx: *session.Context,
    slot: usize,
) !void {
    switch (response_spec) {
        .scalar => |encoding| try ctx.setSlot(slot, try parseScalarResponseValue(encoding, response_bytes, bool_map)),
        .list => |list_spec| try parseListResponseIntoSlot(ctx, slot, list_spec, response_bytes, bool_map),
        .object => return error.ResponseSpecMismatch,
    }
}

fn parseScalarResponseValue(
    encoding: instrument_types.Encoding,
    response_bytes: []const u8,
    bool_map: ?recipe_mod.BoolTextMap,
) !session.Value {
    const trimmed = std.mem.trim(u8, response_bytes, &std.ascii.whitespace);
    return switch (encoding) {
        // Raw responses intentionally preserve leading/trailing bytes exactly as received.
        .raw => .{ .string = session.Value.String.borrow(response_bytes) },
        .float => .{ .float = try std.fmt.parseFloat(f64, trimmed) },
        .int => .{ .int = try std.fmt.parseInt(i64, trimmed, 10) },
        .string => .{ .string = session.Value.String.borrow(unwrapQuotedString(trimmed)) },
        .bool => .{ .bool = try parseBoolResponse(trimmed, bool_map) },
    };
}

fn parseListResponseIntoSlot(
    ctx: *session.Context,
    slot: usize,
    response_spec: instrument_types.ListResponseSpec,
    response_bytes: []const u8,
    bool_map: ?recipe_mod.BoolTextMap,
) !void {
    // Prepare the destination before parsing so repeated list responses can reuse capacity.
    const items = try ctx.prepareListSlot(slot, response_spec.items.len);

    var remaining = std.mem.trim(u8, response_bytes, &std.ascii.whitespace);
    for (response_spec.items, 0..) |encoding, idx| {
        const field = if (idx + 1 == response_spec.items.len) blk: {
            // Extra separators after the final expected field indicate a response shape mismatch.
            if (findSeparatorOutsideQuotes(remaining, response_spec.separator) != null) return error.ResponseFieldCountMismatch;
            break :blk remaining;
        } else blk: {
            const sep_idx = findSeparatorOutsideQuotes(remaining, response_spec.separator) orelse return error.ResponseFieldCountMismatch;
            const field = remaining[0..sep_idx];
            remaining = remaining[sep_idx + response_spec.separator.len ..];
            break :blk field;
        };

        const parsed = try parseScalarResponseValue(
            encoding,
            std.mem.trim(u8, field, &std.ascii.whitespace),
            bool_map,
        );
        try ctx.setPreparedListItem(items, idx, parsed);
    }
}

/// Finds a response separator while ignoring separators inside double-quoted fields.
/// Doubled quotes inside quoted strings are treated as escaped quotes.
fn findSeparatorOutsideQuotes(source: []const u8, separator: []const u8) ?usize {
    if (separator.len == 0) return null;

    var in_quotes = false;
    var idx: usize = 0;
    while (idx < source.len) {
        if (source[idx] == '"') {
            if (in_quotes and idx + 1 < source.len and source[idx + 1] == '"') {
                idx += 2;
                continue;
            }
            in_quotes = !in_quotes;
            idx += 1;
            continue;
        }
        if (!in_quotes and std.mem.startsWith(u8, source[idx..], separator)) return idx;
        idx += 1;
    }
    return null;
}

/// Removes one surrounding double-quote pair; escaping is handled only for separator scanning.
fn unwrapQuotedString(value: []const u8) []const u8 {
    if (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') {
        return value[1 .. value.len - 1];
    }
    return value;
}

/// Parses adapter-specific boolean text when configured, otherwise accepts legacy `1` truthiness.
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
        .string => |s| .{ .string = session.Value.String.borrow(s) },
    };
}

pub fn resolveStepArg(
    ctx: *const session.Context,
    value: recipe_mod.StepArg,
    allocator: std.mem.Allocator,
) !session.Value {
    return switch (value) {
        .scalar => |e| try evalToValue(&e, ctx, allocator),
        .list => |items| blk: {
            // The returned list borrows scratch-owned items for the duration of command rendering.
            const resolved = try allocator.alloc(session.Value, items.len);
            for (items, 0..) |*item, idx| {
                resolved[idx] = try evalToValue(item, ctx, allocator);
            }
            break :blk .{ .list = session.Value.List.borrow(resolved) };
        },
    };
}

fn logDryRun(log_sink: session.LogSink, adapter_name: []const u8, rendered: []const u8) void {
    var buf: [1024]u8 = undefined;
    const text = std.fmt.bufPrint(&buf, dry_run_tag ++ " {s} -> {s}\n", .{ adapter_name, rendered }) catch return;
    log_sink.writeAll(text);
}

test "executor parses scalar responses" {
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{
        .{ .float = 0 },
        .{ .int = 0 },
        .{ .string = session.Value.String.borrow("") },
        .{ .string = session.Value.String.borrow("") },
    }, &.{});
    defer ctx.deinit();

    try parseResponseIntoSlot(.{ .scalar = .float }, "  2.5 \n", null, &ctx, 0);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), ctx.getSlot(0).float, 1e-9);

    try parseResponseIntoSlot(.{ .scalar = .int }, "7", null, &ctx, 1);
    try std.testing.expectEqual(@as(i64, 7), ctx.getSlot(1).int);

    try parseResponseIntoSlot(.{ .scalar = .string }, "  ready \n", null, &ctx, 2);
    try std.testing.expectEqualStrings("ready", ctx.getSlot(2).string.items());

    try parseResponseIntoSlot(.{ .scalar = .raw }, "  raw \n", null, &ctx, 3);
    try std.testing.expectEqualStrings("  raw \n", ctx.getSlot(3).string.items());
}

test "executor parses bool responses with optional adapter mapping" {
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{
        .{ .bool = false },
        .{ .bool = true },
        .{ .bool = false },
    }, &.{});
    defer ctx.deinit();

    try parseResponseIntoSlot(.{ .scalar = .bool }, "1", null, &ctx, 0);
    try std.testing.expect(ctx.getSlot(0).bool);

    try parseResponseIntoSlot(.{ .scalar = .bool }, "ON", null, &ctx, 1);
    try std.testing.expect(!ctx.getSlot(1).bool);

    const map: recipe_mod.BoolTextMap = .{ .true_text = "ENABLE", .false_text = "DISABLE" };
    try parseResponseIntoSlot(.{ .scalar = .bool }, "ENABLE", map, &ctx, 2);
    try std.testing.expect(ctx.getSlot(2).bool);

    try parseResponseIntoSlot(.{ .scalar = .bool }, "disable", map, &ctx, 2);
    try std.testing.expect(!ctx.getSlot(2).bool);

    try std.testing.expectError(error.InvalidBoolResponse, parseResponseIntoSlot(.{ .scalar = .bool }, "ON", map, &ctx, 2));
}

test "executor parses list responses and reuses list slot capacity" {
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{
        .{ .list = session.Value.List.borrow(&.{}) },
    }, &.{2});
    defer ctx.deinit();

    switch (ctx.getSlot(0)) {
        .list => |items| try std.testing.expectEqual(@as(usize, 0), items.len()),
        else => return error.TestUnexpectedResult,
    }

    try parseResponseIntoSlot(.{ .list = .{
        .separator = ",",
        .items = &.{ .float, .float },
    } }, " 1.25, 2.5\n", null, &ctx, 0);

    switch (ctx.getSlot(0)) {
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len());
            try std.testing.expectApproxEqAbs(@as(f64, 1.25), items.items()[0].float, 1e-9);
            try std.testing.expectApproxEqAbs(@as(f64, 2.5), items.items()[1].float, 1e-9);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "executor parses quoted list fields without splitting embedded separators" {
    const initial_items = [_]session.Value{ .{ .int = 0 }, .{ .string = session.Value.String.borrow("") } };
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{
        .{ .list = session.Value.List.borrow(initial_items[0..]) },
    }, &.{});
    defer ctx.deinit();

    try parseResponseIntoSlot(.{ .list = .{
        .separator = ",",
        .items = &.{ .int, .string },
    } }, "-221,\"Settings conflict, channel 1\"", null, &ctx, 0);

    switch (ctx.getSlot(0)) {
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len());
            try std.testing.expectEqual(@as(i64, -221), items.items()[0].int);
            try std.testing.expectEqualStrings("Settings conflict, channel 1", items.items()[1].string.items());
        },
        else => return error.TestUnexpectedResult,
    }
}

test "executor rejects list response field count mismatches" {
    const initial_items = [_]session.Value{ .{ .int = 0 }, .{ .string = session.Value.String.borrow("") } };
    var ctx: session.Context = try .init(std.testing.allocator, std.testing.io, &.{
        .{ .list = session.Value.List.borrow(initial_items[0..]) },
    }, &.{});
    defer ctx.deinit();

    try std.testing.expectError(error.ResponseFieldCountMismatch, parseResponseIntoSlot(.{ .list = .{
        .separator = ",",
        .items = &.{ .int, .string },
    } }, "1,a,b", null, &ctx, 0));
}
