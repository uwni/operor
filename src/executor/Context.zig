const Context = @This();
const std = @import("std");
const expr = @import("../expr.zig");
const recipe_types = @import("../recipe/types.zig");

pub const Value = recipe_types.Value;
pub const RenderValue = recipe_types.RenderValue;

/// Execution-time value store used for `${name}` substitutions and `assign` outputs.
allocator: std.mem.Allocator,
start_ns: i128 = 0,
iteration: u64 = 0,
task_idx: usize = 0,
values: []Slot,

const Slot = union(enum) {
    unset,
    float: f64,
    int: i64,
    bool: bool,
    string: struct {
        buffer: []u8,
        len: usize,
    },
    list: []Value,
};

/// Creates an execution context from initial values, deep-copying owned data.
pub fn init(allocator: std.mem.Allocator, initial_values: []const Value) !Context {
    const values = try allocator.alloc(Slot, initial_values.len);
    @memset(values, .unset);
    var self: Context = .{ .allocator = allocator, .values = values };
    errdefer self.deinit();
    for (initial_values, 0..) |value, idx| {
        self.values[idx] = try self.dupeToSlot(value);
    }
    return self;
}

/// Releases all context-owned keys and values.
pub fn deinit(self: *Context) void {
    for (self.values) |*slot| self.freeStored(slot);
    self.allocator.free(self.values);
}

/// Deep-copies a recipe Value into a context-owned Slot.
fn dupeToSlot(self: *Context, value: Value) !Slot {
    return switch (value) {
        .float => |f| .{ .float = f },
        .int => |i| .{ .int = i },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = .{
            .buffer = try self.allocator.dupe(u8, s),
            .len = s.len,
        } },
        .list => |items| .{ .list = try self.dupeList(items) },
    };
}

/// Stores a runtime value by compiled slot index.
/// Auto-coerces between int and float; all other cross-type writes are errors.
pub fn setSlot(self: *Context, slot_idx: usize, value: Value) !void {
    const stored = &self.values[slot_idx];
    switch (stored.*) {
        .float => stored.float = switch (value) {
            .float => |f| f,
            .int => |i| @floatFromInt(i),
            else => return error.TypeMismatch,
        },
        .int => stored.int = switch (value) {
            .int => |i| i,
            .float => |f| @intFromFloat(f),
            else => return error.TypeMismatch,
        },
        .bool => switch (value) {
            .bool => |b| stored.bool = b,
            else => return error.TypeMismatch,
        },
        .string => {
            const s = switch (value) {
                .string => |v| v,
                else => return error.TypeMismatch,
            };
            if (stored.string.buffer.len < s.len) {
                const replacement = try self.allocator.alloc(u8, s.len);
                self.allocator.free(stored.string.buffer);
                stored.string.buffer = replacement;
            }
            @memcpy(stored.string.buffer[0..s.len], s);
            stored.string.len = s.len;
        },
        .list => {
            const items = switch (value) {
                .list => |v| v,
                else => return error.TypeMismatch,
            };
            const duped = try self.dupeList(items);
            self.freeStored(stored);
            stored.* = .{ .list = duped };
        },
        .unset => unreachable,
    }
}

/// Returns a previously stored runtime value by compiled slot index.
pub fn getSlot(self: *const Context, slot_idx: usize) Value {
    const stored = self.values[slot_idx];
    return switch (stored) {
        .float => |f| .{ .float = f },
        .int => |i| .{ .int = i },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s.buffer[0..s.len] },
        .list => |items| .{ .list = items },
        .unset => unreachable,
    };
}

pub fn resolveBinding(self: *const Context, binding: expr.VariableBinding) Value {
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
    const val = self.resolveBinding(binding);
    return switch (val) {
        .list => |items| .{ .list = .{
            .len = items.len,
            .ctx = @ptrCast(items.ptr),
            .at_fn = listAtFn,
        } },
        else => val.toResolvedValue(),
    };
}

fn listAtFn(ctx: *const anyopaque, index: usize) ?expr.ResolvedValue {
    const ptr: [*]const Value = @ptrCast(@alignCast(ctx));
    // Caller already bounds-checked via integer subscript; this is a safety net.
    const item = ptr[index];
    return item.toResolvedValue();
}

/// Returns an expression resolver over slot-based values plus built-in execution state.
pub fn varResolver(self: *const Context) expr.VarResolver {
    return .{
        .ctx = @ptrCast(self),
        .resolve_fn = resolveBindingValue,
    };
}

fn freeStored(self: *Context, stored: *Slot) void {
    switch (stored.*) {
        .string => |s| self.allocator.free(s.buffer),
        .list => |items| {
            for (items) |item| self.freeValue(item);
            self.allocator.free(items);
        },
        else => {},
    }
}

fn freeValue(self: *Context, val: Value) void {
    switch (val) {
        .string => |s| self.allocator.free(@constCast(s)),
        else => {},
    }
}

fn dupeList(self: *Context, items: []const Value) ![]Value {
    const duped = try self.allocator.alloc(Value, items.len);
    errdefer self.allocator.free(duped);
    var initialized: usize = 0;
    errdefer for (duped[0..initialized]) |item| self.freeValue(item);
    for (items, 0..) |item, idx| {
        duped[idx] = switch (item) {
            .string => |s| .{ .string = try self.allocator.dupe(u8, s) },
            .list => unreachable, // nested lists structurally impossible from YAML
            else => item,
        };
        initialized += 1;
    }
    return duped;
}

test "Value and RenderValue format support formatter specifier" {
    const testing = std.testing;
    const list = [_]Value{
        .{ .int = 1 },
        .{ .float = 2.5 },
        .{ .string = "ch3" },
    };

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
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

    var ctx: Context = try .init(testing.allocator, &.{Value{ .float = 3.3 }});
    defer ctx.deinit();

    ctx.task_idx = 2;
    ctx.iteration = 7;

    try testing.expectEqualDeep(Value{ .int = 7 }, ctx.resolveBinding(.{ .builtin = .iter }));
    try testing.expectEqualDeep(Value{ .int = 2 }, ctx.resolveBinding(.{ .builtin = .task_idx }));
    try testing.expectEqualDeep(Value{ .float = 3.3 }, ctx.resolveBinding(.{ .slot = 0 }));

    var expr_obj = try expr_mod.parse(testing.allocator, "$ITER + $TASK_IDX");
    defer expr_obj.deinit(testing.allocator);
    var empty_slots: std.StringArrayHashMap(void) = .init(testing.allocator);
    defer empty_slots.deinit();
    try expr_obj.bindVariables(&empty_slots);
    const eval_result = try expr_obj.eval(ctx.varResolver(), testing.allocator);
    try testing.expectEqual(@as(i64, 9), eval_result.value.int);
}

test "Context stores run start state separately from iteration state" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, &.{});
    defer ctx.deinit();

    ctx.start_ns = 1234;
    ctx.task_idx = 5;
    ctx.iteration = 9;

    try testing.expectEqual(@as(i128, 1234), ctx.start_ns);
    try testing.expectEqual(@as(usize, 5), ctx.task_idx);
    try testing.expectEqual(@as(u64, 9), ctx.iteration);
}

test "Context list round-trip through setSlot and varResolver" {
    const testing = std.testing;
    const expr_mod = @import("../expr.zig");

    const items = [_]Value{ .{ .float = 10.0 }, .{ .float = 20.0 }, .{ .float = 30.0 } };
    var ctx: Context = try .init(testing.allocator, &.{
        Value{ .list = items[0..] },
        Value{ .int = 0 },
    });
    defer ctx.deinit();

    // Verify getSlot returns the list.
    const got = ctx.getSlot(0);
    switch (got) {
        .list => |l| try testing.expectEqual(@as(usize, 3), l.len),
        else => return error.TestUnexpectedResult,
    }

    // Evaluate len(${arr}) via varResolver.
    var slots: std.StringArrayHashMap(void) = .init(testing.allocator);
    defer slots.deinit();
    try slots.put("arr", {});
    try slots.put("idx", {});

    var e = try expr_mod.parse(testing.allocator, "${arr}[${idx}]");
    defer e.deinit(testing.allocator);
    try e.bindVariables(&slots);
    const eval_result = try e.eval(ctx.varResolver(), testing.allocator);
    try testing.expectApproxEqAbs(@as(f64, 10.0), eval_result.value.float, 1e-9);

    // Overwrite list slot to verify freeStored works.
    const items2 = [_]Value{.{ .int = 99 }};
    try ctx.setSlot(0, .{ .list = items2[0..] });
    const got2 = ctx.getSlot(0);
    switch (got2) {
        .list => |l| try testing.expectEqual(@as(usize, 1), l.len),
        else => return error.TestUnexpectedResult,
    }
}

test "Context resolves $ELAPSED_MS builtin as elapsed milliseconds" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, &.{});
    defer ctx.deinit();

    // Before start_ns is set, elapsed_ms is 0.
    const before = ctx.resolveBinding(.{ .builtin = .elapsed_ms });
    try testing.expectEqual(@as(i64, 0), before.int);

    // Set start_ns to a recent past so elapsed > 0.
    ctx.start_ns = std.time.nanoTimestamp() - 50_000_000; // 50ms ago
    const after = ctx.resolveBinding(.{ .builtin = .elapsed_ms });
    try testing.expect(after.int >= 40 and after.int <= 200);
}

test "setSlot auto-coerces between int and float" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, &.{
        Value{ .int = 5 },
        Value{ .float = 1.0 },
    });
    defer ctx.deinit();

    // Slot 0: declared as int, receives float → truncated to int.
    try ctx.setSlot(0, .{ .float = 7.9 });
    try testing.expectEqualDeep(Value{ .int = 7 }, ctx.getSlot(0));

    // Slot 1: declared as float, receives int → widened to float.
    try ctx.setSlot(1, .{ .int = 42 });
    try testing.expectEqualDeep(Value{ .float = 42.0 }, ctx.getSlot(1));
}

test "setSlot rejects incompatible types" {
    const testing = std.testing;

    const list_items = [_]Value{.{ .int = 1 }};
    var ctx: Context = try .init(testing.allocator, &.{
        Value{ .string = "hello" },
        Value{ .float = 1.0 },
        Value{ .list = list_items[0..] },
    });
    defer ctx.deinit();

    // String slot rejects float.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(0, .{ .float = 1.0 }));

    // Float slot rejects string.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(1, .{ .string = "oops" }));

    // List slot rejects scalar.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(2, .{ .int = 1 }));
}

test "init cleans up on partial allocation failure" {
    const testing = std.testing;

    // alloc 0: Slot slice, alloc 1: first string dupe, alloc 2: fail
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 2 });
    const result = Context.init(failing.allocator(), &.{
        Value{ .string = "aaa" },
        Value{ .string = "bbb" },
    });
    try testing.expectError(error.OutOfMemory, result);
}
