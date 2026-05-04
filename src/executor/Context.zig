const Context = @This();
const std = @import("std");
const expr = @import("../expr.zig");
const diagnostic = @import("../diagnostic.zig");
const recipe_compiled = @import("../recipe/compiled.zig");

pub const Value = recipe_compiled.Value;
const String = Value.String;
const List = Value.List;

/// Execution-time value store used for `${name}` substitutions and `assign` outputs.
/// Slot positions are assigned during precompile; runtime never looks values up by name.
allocator: std.mem.Allocator,
io: std.Io,
/// Monotonic start timestamp used by the `$ELAPSED_MS` built-in.
start_ns: i96 = 0,
/// Count of completed task iterations exposed through `$ITER`.
iteration: u64 = 0,
/// Index of the task currently being executed, exposed through `$TASK_IDX`.
task_idx: usize = 0,
/// Context-owned runtime slots, aligned to `PrecompiledRecipe.initial_values`.
values: []Slot,

const Slot = union(enum) {
    unset,
    float: f64,
    int: i64,
    bool: bool,
    string: String,
    list: List,
};

/// Creates an execution context from initial values, deep-copying owned data.
pub fn init(
    allocator: std.mem.Allocator,
    io: std.Io,
    initial_values: []const Value,
    list_slot_capacities: []const usize,
) !Context {
    const values = try allocator.alloc(Slot, initial_values.len);
    @memset(values, .unset);
    var self: Context = .{ .allocator = allocator, .io = io, .values = values };
    errdefer self.deinit();
    for (initial_values, 0..) |value, idx| {
        const capacity_hint = if (idx < list_slot_capacities.len) list_slot_capacities[idx] else 0;
        self.values[idx] = try self.dupeToSlot(value, capacity_hint);
    }
    return self;
}

/// Releases all context-owned keys and values.
pub fn deinit(self: *Context) void {
    for (self.values) |*slot| self.freeStored(slot);
    self.allocator.free(self.values);
}

/// Deep-copies a recipe Value into a context-owned Slot.
fn dupeToSlot(self: *Context, value: Value, capacity_hint: usize) !Slot {
    return switch (value) {
        .float => |f| .{ .float = f },
        .int => |i| .{ .int = i },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = try self.dupeString(s.items()) },
        .list => |items| .{ .list = try self.dupeList(items.items(), capacity_hint) },
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
            .float => |f| try floatToIntFloor(f),
            else => return error.TypeMismatch,
        },
        .bool => switch (value) {
            .bool => |b| stored.bool = b,
            else => return error.TypeMismatch,
        },
        .string => {
            const s = switch (value) {
                .string => |v| v.items(),
                else => return error.TypeMismatch,
            };
            try self.setString(&stored.string, s);
        },
        .list => {
            const items = switch (value) {
                .list => |v| v.items(),
                else => return error.TypeMismatch,
            };
            const duped = try self.dupeList(items, items.len);
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
        .string => |s| .{ .string = String.borrow(s.items()) },
        .list => |list| .{ .list = List.borrow(list.items()) },
        .unset => unreachable,
    };
}

/// Resizes a list slot for in-place population and returns its mutable item slice.
/// Existing elements beyond the new length are freed before the slot is reused.
pub fn prepareListSlot(self: *Context, slot_idx: usize, len: usize) ![]Value {
    const stored = &self.values[slot_idx];
    switch (stored.*) {
        .list => |*list| {
            const old_len = list.len();
            if (old_len > len) {
                const items = list.mutItems();
                for (items[len..old_len]) |item| self.freeValue(item);
            }
            try list.ensureOwnedCapacity(self.allocator, len);
            if (old_len < len) {
                list.setLen(len);
                const items = list.mutItems();
                @memset(items[old_len..len], .{ .int = 0 });
            } else {
                list.setLen(len);
            }
            return list.mutItems();
        },
        else => return error.TypeMismatch,
    }
}

/// Stores one value into a slice returned by `prepareListSlot`, preserving ownership rules.
pub fn setPreparedListItem(self: *Context, items: []Value, index: usize, value: Value) !void {
    try self.setValue(&items[index], value);
}

/// Resolves either a compiled recipe slot or one of the executor built-ins.
pub fn resolveBinding(self: *const Context, binding: expr.VariableBinding) Value {
    return switch (binding) {
        .slot => |slot_idx| self.getSlot(slot_idx),
        .builtin => |builtin| switch (builtin) {
            .iter => .{ .int = @intCast(self.iteration) },
            .task_idx => .{ .int = @intCast(self.task_idx) },
            .elapsed_ms => .{ .int = if (self.start_ns == 0)
                0
            else blk: {
                const now = std.Io.Timestamp.now(self.io, .awake).toNanoseconds();
                const delta_ns = now - self.start_ns;
                const ms = @divTrunc(delta_ns, 1_000_000);
                break :blk @intCast(@min(ms, std.math.maxInt(i64)));
            } },
        },
    };
}

/// Converts finite floats to integers using truncation toward zero and explicit range checks.
fn floatToIntFloor(f: f64) !i64 {
    if (!std.math.isFinite(f)) return error.InvalidNumericConversion;

    const rounded: f64 = @trunc(f);
    const min_bound: f64 = @floatFromInt(std.math.minInt(i64));
    const max_bound: f64 = @floatFromInt(std.math.maxInt(i64));
    if (rounded < min_bound or rounded >= max_bound) return error.InvalidNumericConversion;
    return @trunc(f);
}

fn listAtFn(ctx: *const anyopaque, index: usize) ?expr.ResolvedValue {
    const ptr: [*]const Value = @ptrCast(@alignCast(ctx));
    // Caller already bounds-checked via integer subscript; this is a safety net.
    const item = ptr[index];
    return item.toResolvedValue();
}

/// Zero-cost resolver: holds a typed pointer so the compiler can inline
/// `resolve` directly into `Expression.eval` / `Expression.isTruthy`.
pub const Resolver = struct {
    ctx: *const Context,

    pub fn resolve(self: Resolver, binding: expr.VariableBinding) ?expr.ResolvedValue {
        const val = self.ctx.resolveBinding(binding);
        return switch (val) {
            .list => |items| blk: {
                const slice = items.items();
                break :blk .{ .list = .{
                    .len = slice.len,
                    .ctx = @ptrCast(slice.ptr),
                    .at_fn = listAtFn,
                } };
            },
            else => val.toResolvedValue(),
        };
    }
};

/// Returns a resolver that the compiler can monomorphize and inline.
pub fn resolver(self: *const Context) Resolver {
    return .{ .ctx = self };
}

fn freeStored(self: *Context, stored: *Slot) void {
    switch (stored.*) {
        .string => |*s| s.deinit(self.allocator),
        .list => |*list| {
            for (list.items()) |item| self.freeValue(item);
            list.deinit(self.allocator);
        },
        else => {},
    }
}

fn freeValue(self: *Context, val: Value) void {
    switch (val) {
        .string => |s| {
            var string = s;
            string.deinit(self.allocator);
        },
        .list => |list| {
            for (list.items()) |item| self.freeValue(item);
            var value_list = list;
            value_list.deinit(self.allocator);
        },
        else => {},
    }
}

fn dupeString(self: *Context, bytes: []const u8) !String {
    return .{ .owned = .{ .items = try self.allocator.dupe(u8, bytes), .len = bytes.len } };
}

fn setString(self: *Context, stored: *String, bytes: []const u8) !void {
    if (stored.* != .owned or stored.owned.items.len < bytes.len) {
        const replacement = try self.allocator.dupe(u8, bytes);
        stored.deinit(self.allocator);
        stored.* = .{ .owned = .{ .items = replacement, .len = bytes.len } };
        return;
    }
    stored.owned.len = bytes.len;
    @memcpy(stored.owned.items[0..bytes.len], bytes);
}

fn setValue(self: *Context, stored: *Value, value: Value) !void {
    // String slots keep and reuse their buffer when capacity is sufficient.
    if (stored.* == .string and value == .string) {
        return self.setString(&stored.string, value.string.items());
    }

    const replacement = try self.dupeValue(value);
    self.freeValue(stored.*);
    stored.* = replacement;
}

fn dupeValue(self: *Context, item: Value) !Value {
    return switch (item) {
        .string => |s| .{ .string = try self.dupeString(s.items()) },
        .list => unreachable, // nested lists are not supported in runtime slots
        else => item,
    };
}

fn dupeList(self: *Context, items: []const Value, capacity_hint: usize) !List {
    const capacity = @max(items.len, capacity_hint);
    var duped = List{ .owned = .{ .items = try self.allocator.alloc(Value, capacity), .len = items.len } };
    errdefer duped.deinit(self.allocator);
    var initialized: usize = 0;
    const dest = duped.mutItems();
    errdefer for (dest[0..initialized]) |item| self.freeValue(item);
    // Only the logical prefix is initialized; spare capacity is reserved for future list responses.
    for (items, 0..) |item, idx| {
        dest[idx] = try self.dupeValue(item);
        initialized += 1;
    }
    return duped;
}

test "Value format supports formatter specifier" {
    const testing = std.testing;
    const list = [_]Value{
        .{ .int = 1 },
        .{ .float = 2.5 },
        .{ .string = String.borrow("ch3") },
    };

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();

    try out.writer.print("{f}|{f}|{f}|{f}|{f}", .{
        Value{ .float = 1.25 },
        Value{ .int = 42 },
        Value{ .bool = true },
        Value{ .string = String.borrow("ok") },
        Value{ .list = List.borrow(list[0..]) },
    });

    try testing.expectEqualStrings("1.25|42|true|ok|1, 2.5, ch3", out.written());
}

test "Context exposes built-ins alongside stored values" {
    const testing = std.testing;
    const expr_mod = @import("../expr.zig");

    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{Value{ .float = 3.3 }}, &.{});
    defer ctx.deinit();

    ctx.task_idx = 2;
    ctx.iteration = 7;

    try testing.expectEqualDeep(Value{ .int = 7 }, ctx.resolveBinding(.{ .builtin = .iter }));
    try testing.expectEqualDeep(Value{ .int = 2 }, ctx.resolveBinding(.{ .builtin = .task_idx }));
    try testing.expectEqualDeep(Value{ .float = 3.3 }, ctx.resolveBinding(.{ .slot = 0 }));

    var empty_slots: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer empty_slots.deinit(testing.allocator);
    var temp_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer temp_arena.deinit();
    const source = "$ITER + $TASK_IDX";
    var common_diagnostics = diagnostic.Diagnostics.init(null, "<test>");
    defer common_diagnostics.deinit();
    const diagnostics = common_diagnostics.reporter().withSource(.expression, source);
    var ast = try expr_mod.parseAst(temp_arena.allocator(), source, diagnostics);
    try ast.bindVariables(&empty_slots, diagnostics);
    var expr_obj = try ast.lower(testing.allocator, diagnostics);
    defer expr_obj.deinit(testing.allocator);
    const eval_result = try expr_obj.eval(ctx.resolver(), testing.allocator);
    try testing.expectEqual(@as(i64, 9), eval_result.value.int);
}

test "Context stores run start state separately from iteration state" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{}, &.{});
    defer ctx.deinit();

    ctx.start_ns = 1234;
    ctx.task_idx = 5;
    ctx.iteration = 9;

    try testing.expectEqual(@as(i96, 1234), ctx.start_ns);
    try testing.expectEqual(@as(usize, 5), ctx.task_idx);
    try testing.expectEqual(@as(u64, 9), ctx.iteration);
}

test "Context list round-trip through setSlot and varResolver" {
    const testing = std.testing;
    const expr_mod = @import("../expr.zig");

    const items = [_]Value{ .{ .float = 10.0 }, .{ .float = 20.0 }, .{ .float = 30.0 } };
    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{
        Value{ .list = List.borrow(items[0..]) },
        Value{ .int = 0 },
    }, &.{});
    defer ctx.deinit();

    // Verify getSlot returns the list.
    const got = ctx.getSlot(0);
    switch (got) {
        .list => |l| try testing.expectEqual(@as(usize, 3), l.len()),
        else => return error.TestUnexpectedResult,
    }

    // Evaluate len(${arr}) via resolver.
    var slots: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer slots.deinit(testing.allocator);
    try slots.put(testing.allocator, "arr", {});
    try slots.put(testing.allocator, "idx", {});

    var temp_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer temp_arena.deinit();
    const source = "${arr}[${idx}]";
    var common_diagnostics = diagnostic.Diagnostics.init(null, "<test>");
    defer common_diagnostics.deinit();
    const diagnostics = common_diagnostics.reporter().withSource(.expression, source);
    var ast = try expr_mod.parseAst(temp_arena.allocator(), source, diagnostics);
    try ast.bindVariables(&slots, diagnostics);
    var e = try ast.lower(testing.allocator, diagnostics);
    defer e.deinit(testing.allocator);
    const eval_result = try e.eval(ctx.resolver(), testing.allocator);
    try testing.expectApproxEqAbs(@as(f64, 10.0), eval_result.value.float, 1e-9);

    // Overwrite list slot to verify freeStored works.
    const items2 = [_]Value{.{ .int = 99 }};
    try ctx.setSlot(0, .{ .list = List.borrow(items2[0..]) });
    const got2 = ctx.getSlot(0);
    switch (got2) {
        .list => |l| try testing.expectEqual(@as(usize, 1), l.len()),
        else => return error.TestUnexpectedResult,
    }
}

test "Context resolves $ELAPSED_MS builtin as elapsed milliseconds" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{}, &.{});
    defer ctx.deinit();

    // Before start_ns is set, elapsed_ms is 0.
    const before = ctx.resolveBinding(.{ .builtin = .elapsed_ms });
    try testing.expectEqual(@as(i64, 0), before.int);

    // Set start_ns to a recent past so elapsed > 0.
    ctx.start_ns = std.Io.Timestamp.now(std.testing.io, .awake).nanoseconds - 50_000_000; // 50ms ago
    const after = ctx.resolveBinding(.{ .builtin = .elapsed_ms });
    try testing.expect(after.int >= 40 and after.int <= 200);
}

test "setSlot auto-coerces between int and float" {
    const testing = std.testing;

    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{
        Value{ .int = 5 },
        Value{ .float = 1.0 },
    }, &.{});
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
    var ctx: Context = try .init(testing.allocator, std.testing.io, &.{
        Value{ .string = String.borrow("hello") },
        Value{ .float = 1.0 },
        Value{ .list = List.borrow(list_items[0..]) },
    }, &.{});
    defer ctx.deinit();

    // String slot rejects float.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(0, .{ .float = 1.0 }));

    // Float slot rejects string.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(1, .{ .string = String.borrow("oops") }));

    // List slot rejects scalar.
    try testing.expectError(error.TypeMismatch, ctx.setSlot(2, .{ .int = 1 }));
}

test "init cleans up on partial allocation failure" {
    const testing = std.testing;

    // alloc 0: Slot slice, alloc 1: first string dupe, alloc 2: fail
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 2 });
    const result = Context.init(failing.allocator(), std.testing.io, &.{
        Value{ .string = String.borrow("aaa") },
        Value{ .string = String.borrow("bbb") },
    }, &.{});
    try testing.expectError(error.OutOfMemory, result);
}
