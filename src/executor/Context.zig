const Context = @This();
const std = @import("std");
const expr = @import("../expr.zig");
const recipe_types = @import("../recipe/types.zig");

pub const Value = recipe_types.Value;
pub const RenderValue = recipe_types.RenderValue;

/// Execution-time value store used for `${name}` substitutions and `save_as` outputs.
allocator: std.mem.Allocator,
start_ns: i128 = 0,
iteration: u64 = 0,
task_idx: usize = 0,
values: []ContextValue,

const ContextValue = union(enum) {
    unset,
    float: f64,
    int: i64,
    bool: bool,
    string: struct {
        buffer: []u8,
        len: usize,
    },
};

/// Creates an empty execution context.
pub fn init(allocator: std.mem.Allocator, slot_count: usize) !Context {
    const values = try allocator.alloc(ContextValue, slot_count);
    @memset(values, .unset);

    return .{
        .allocator = allocator,
        .values = values,
    };
}

/// Releases all context-owned keys and values.
pub fn deinit(self: *Context) void {
    for (self.values) |*slot| {
        switch (slot.*) {
            .string => |s| self.allocator.free(s.buffer),
            else => {},
        }
    }
    self.allocator.free(self.values);
}

/// Stores or replaces a runtime value by compiled slot index.
pub fn setSlot(self: *Context, slot_idx: usize, value: Value) !void {
    const stored = &self.values[slot_idx];
    switch (value) {
        .string => |s| {
            if (stored.* == .string) {
                if (stored.string.buffer.len < s.len) {
                    const replacement = try self.allocator.alloc(u8, s.len);
                    self.allocator.free(stored.string.buffer);
                    stored.string.buffer = replacement;
                }
                @memcpy(stored.string.buffer[0..s.len], s);
                stored.string.len = s.len;
            } else {
                const buffer = try self.allocator.dupe(u8, s);
                stored.* = .{ .string = .{ .buffer = buffer, .len = s.len } };
            }
        },
        .float => |f| {
            if (stored.* == .string) self.allocator.free(stored.string.buffer);
            stored.* = .{ .float = f };
        },
        .int => |i| {
            if (stored.* == .string) self.allocator.free(stored.string.buffer);
            stored.* = .{ .int = i };
        },
        .bool => |b| {
            if (stored.* == .string) self.allocator.free(stored.string.buffer);
            stored.* = .{ .bool = b };
        },
    }
}

/// Returns a previously stored runtime value by compiled slot index.
pub fn getSlot(self: *const Context, slot_idx: usize) ?Value {
    const stored = self.values[slot_idx];
    return switch (stored) {
        .unset => null,
        .float => |f| .{ .float = f },
        .int => |i| .{ .int = i },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s.buffer[0..s.len] },
    };
}

pub fn resolveBinding(self: *const Context, binding: expr.VariableBinding) ?Value {
    return switch (binding) {
        .slot => |slot_idx| self.getSlot(slot_idx),
        .builtin => |builtin| switch (builtin) {
            .iter => .{ .int = @intCast(self.iteration) },
            .task_idx => .{ .int = @intCast(self.task_idx) },
            .elapsed_ms => .{ .int = if (self.start_ns == 0)
                @as(i64, 0)
            else
                @intCast(@divTrunc(std.time.nanoTimestamp() - self.start_ns, 1_000_000)) },
        },
    };
}

fn resolveBindingValue(ctx_ptr: *const anyopaque, binding: expr.VariableBinding) ?expr.ResolvedValue {
    const self: *const Context = @ptrCast(@alignCast(ctx_ptr));
    const val = self.resolveBinding(binding) orelse return null;
    return val.toResolvedValue();
}

/// Returns an expression resolver over slot-based values plus built-in execution state.
pub fn varResolver(self: *const Context) expr.VarResolver {
    return .{
        .ctx = @ptrCast(self),
        .resolve_fn = resolveBindingValue,
    };
}

test "Value and RenderValue format support formatter specifier" {
    const testing = std.testing;
    const list = [_]Value{
        .{ .int = 1 },
        .{ .float = 2.5 },
        .{ .string = "ch3" },
    };

    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();

    try out.writer.print("{f}|{f}|{f}|{f}|{f}", .{
        Value{ .float = 1.25 },
        Value{ .int = 42 },
        Value{ .bool = true },
        Value{ .string = "ok" },
        RenderValue{ .list = list[0..] },
    });

    try testing.expectEqualStrings("1.25|42|true|ok|1,2.5,ch3", out.written());
}

test "Context exposes built-ins alongside stored values" {
    const testing = std.testing;
    const expr_mod = @import("../expr.zig");

    var ctx = try Context.init(testing.allocator, 1);
    defer ctx.deinit();

    try ctx.setSlot(0, .{ .float = 3.3 });
    ctx.task_idx = 2;
    ctx.iteration = 7;

    try testing.expectEqualDeep(Value{ .int = 7 }, ctx.resolveBinding(.{ .builtin = .iter }).?);
    try testing.expectEqualDeep(Value{ .int = 2 }, ctx.resolveBinding(.{ .builtin = .task_idx }).?);
    try testing.expectEqualDeep(Value{ .float = 3.3 }, ctx.resolveBinding(.{ .slot = 0 }).?);
    try testing.expect(ctx.getSlot(0) != null);

    var expr_obj = try expr_mod.parse(testing.allocator, "$ITER + $TASK_IDX");
    defer expr_obj.deinit(testing.allocator);
    var empty_slots = std.StringArrayHashMap(void).init(testing.allocator);
    defer empty_slots.deinit();
    try expr_obj.bindVariables(&empty_slots);
    try testing.expectApproxEqAbs(@as(f64, 9.0), try expr_obj.eval(ctx.varResolver()), 1e-9);
}

test "Context stores run start state separately from iteration state" {
    const testing = std.testing;

    var ctx = try Context.init(testing.allocator, 0);
    defer ctx.deinit();

    ctx.start_ns = 1234;
    ctx.task_idx = 5;
    ctx.iteration = 9;

    try testing.expectEqual(@as(i128, 1234), ctx.start_ns);
    try testing.expectEqual(@as(usize, 5), ctx.task_idx);
    try testing.expectEqual(@as(u64, 9), ctx.iteration);
}

test "Context resolves $ELAPSED_MS builtin as elapsed milliseconds" {
    const testing = std.testing;

    var ctx = try Context.init(testing.allocator, 0);
    defer ctx.deinit();

    // Before start_ns is set, elapsed_ms is 0.
    const before = ctx.resolveBinding(.{ .builtin = .elapsed_ms }).?;
    try testing.expectEqual(@as(i64, 0), before.int);

    // Set start_ns to a recent past so elapsed > 0.
    ctx.start_ns = std.time.nanoTimestamp() - 50_000_000; // 50ms ago
    const after = ctx.resolveBinding(.{ .builtin = .elapsed_ms }).?;
    try testing.expect(after.int >= 40 and after.int <= 200);
}
