const std = @import("std");
const types = @import("types.zig");

const Value = types.Value;
const EvalError = types.EvalError;
const VariableBinding = types.VariableBinding;
const ResolvedValue = types.ResolvedValue;
const ResolvedList = types.ResolvedList;
const VarResolver = types.VarResolver;

const CmpOp = enum { gt, lt, ge, le, eq, ne };

pub const Op = union(enum) {
    push_int: i64,
    push_float: f64,
    push_string: []const u8,
    push_bool: bool,
    /// Push the scalar value of a bound variable.
    load_var: VariableBinding,
    /// Push the length (as int) of a list variable.
    load_list_len: VariableBinding,
    /// Pop an integer index, push the element from a list variable.
    load_list_elem: VariableBinding,
    add,
    sub,
    mul,
    div,
    cmp: CmpOp,
    negate,
    not,
    call_min,
    call_max,
    /// Pop delimiter string, resolve list variable, join elements, push result string.
    call_join: VariableBinding,
    /// Skip `offset` following ops when the top of stack is falsy.
    jump_if_false: u16,
    /// Skip `offset` following ops when the top of stack is truthy.
    jump_if_true: u16,
    /// Discard the top of stack.
    pop,
    /// Replace top of stack with int 0 or 1.
    to_bool,
};

pub const Expression = struct {
    ops: []Op,

    pub fn deinit(self: *Expression, allocator: std.mem.Allocator) void {
        freeOwnedOps(allocator, self.ops);
        allocator.free(self.ops);
        self.* = undefined;
    }

    pub const max_stack = 64;
    const max_owned = 4;

    pub const EvalResult = struct {
        value: Value,
        owned: ?[]u8 = null,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *EvalResult) void {
            if (self.owned) |buf| self.allocator.free(buf);
            self.* = undefined;
        }
    };

    pub fn eval(self: *const Expression, resolver: VarResolver, allocator: std.mem.Allocator) EvalError!EvalResult {
        if (self.ops.len == 1) switch (self.ops[0]) {
            .push_int => |i| return .{ .value = .{ .int = i }, .allocator = undefined },
            .push_float => |f| return .{ .value = .{ .float = f }, .allocator = undefined },
            .push_bool => |b| return .{ .value = .{ .bool = b }, .allocator = undefined },
            .push_string => |s| {
                const owned = try allocator.dupe(u8, s);
                return .{
                    .value = .{ .string = owned },
                    .owned = owned,
                    .allocator = allocator,
                };
            },
            else => {},
        };

        var stack: [max_stack]Value = [1]Value{.{ .int = 0 }} ** max_stack;
        var sp: usize = 0;
        var ip: usize = 0;
        var owned_strings: [max_owned]?[]u8 = .{null} ** max_owned;
        var owned_count: usize = 0;
        errdefer for (owned_strings[0..owned_count]) |maybe_str| {
            if (maybe_str) |s| allocator.free(s);
        };

        while (ip < self.ops.len) : (ip += 1) {
            switch (self.ops[ip]) {
                .push_int => |n| {
                    stack[sp] = .{ .int = n };
                    sp += 1;
                },
                .push_float => |n| {
                    stack[sp] = .{ .float = n };
                    sp += 1;
                },
                .push_string => |s| {
                    stack[sp] = .{ .string = s };
                    sp += 1;
                },
                .push_bool => |b| {
                    stack[sp] = .{ .bool = b };
                    sp += 1;
                },
                .load_var => |binding| {
                    const resolved = resolver.resolve(binding) orelse return error.VariableNotFound;
                    stack[sp] = try resolveScalar(resolved);
                    sp += 1;
                },
                .load_list_len => |binding| {
                    const resolved = resolver.resolve(binding) orelse return error.VariableNotFound;
                    stack[sp] = switch (resolved) {
                        .list => |l| .{ .int = @intCast(l.len) },
                        else => return error.InvalidExpression,
                    };
                    sp += 1;
                },
                .load_list_elem => |binding| {
                    sp -= 1;
                    const idx_val = stack[sp];
                    const iv = switch (idx_val) {
                        .int => |v| v,
                        .float, .string, .bool => return error.InvalidExpression,
                    };
                    if (iv < 0) return error.InvalidExpression;
                    const i: usize = @intCast(iv);
                    const resolved = resolver.resolve(binding) orelse return error.VariableNotFound;
                    const list = switch (resolved) {
                        .list => |l| l,
                        else => return error.InvalidExpression,
                    };
                    if (i >= list.len) return error.InvalidExpression;
                    const elem = list.at(i) orelse return error.VariableNotFound;
                    stack[sp] = try resolveScalar(elem);
                    sp += 1;
                },
                .add => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = try promoteArith(a, b, .add);
                },
                .sub => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = try promoteArith(a, b, .sub);
                },
                .mul => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = try promoteArith(a, b, .mul);
                },
                .div => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    if (a == .string or a == .bool or b == .string or b == .bool) return error.InvalidExpression;
                    sp -= 1;
                    const rf = b.toFloat();
                    if (rf == 0.0) return error.DivisionByZero;
                    stack[sp - 1] = .{ .float = a.toFloat() / rf };
                },
                .cmp => |op| {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = .{ .bool = try cmpValues(a, b, op) };
                },
                .negate => {
                    const v = stack[sp - 1];
                    stack[sp - 1] = switch (v) {
                        .int => |i| .{ .int = -i },
                        .float => |f| .{ .float = -f },
                        .string, .bool => return error.InvalidExpression,
                    };
                },
                .not => {
                    const v = stack[sp - 1];
                    stack[sp - 1] = .{ .bool = !v.isTruthy() };
                },
                .call_min => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = try promoteMinMax(a, b, true);
                },
                .call_max => {
                    const b = stack[sp - 1];
                    const a = stack[sp - 2];
                    sp -= 1;
                    stack[sp - 1] = try promoteMinMax(a, b, false);
                },
                .jump_if_false => |skip| {
                    const v = stack[sp - 1];
                    if (!v.isTruthy()) ip += skip;
                },
                .jump_if_true => |skip| {
                    const v = stack[sp - 1];
                    if (v.isTruthy()) ip += skip;
                },
                .pop => sp -= 1,
                .to_bool => {
                    const v = stack[sp - 1];
                    stack[sp - 1] = .{ .bool = v.isTruthy() };
                },
                .call_join => |binding| {
                    sp -= 1;
                    const delim = switch (stack[sp]) {
                        .string => |s| s,
                        else => return error.InvalidExpression,
                    };
                    const resolved = resolver.resolve(binding) orelse return error.VariableNotFound;
                    const list = switch (resolved) {
                        .list => |l| l,
                        else => return error.InvalidExpression,
                    };
                    const joined = try joinList(allocator, list, delim);
                    if (owned_count >= max_owned) {
                        allocator.free(joined);
                        return error.OutOfMemory;
                    }
                    owned_strings[owned_count] = joined;
                    owned_count += 1;
                    stack[sp] = .{ .string = joined };
                    sp += 1;
                },
            }
        }

        const result = stack[0];
        var result_owned: ?[]u8 = null;
        for (owned_strings[0..owned_count]) |maybe_str| {
            if (maybe_str) |s| {
                if (result == .string and result.string.ptr == s.ptr) {
                    result_owned = s;
                } else {
                    allocator.free(s);
                }
            }
        }
        return .{ .value = result, .owned = result_owned, .allocator = allocator };
    }

    pub fn isTruthy(self: *const Expression, resolver: VarResolver, allocator: std.mem.Allocator) EvalError!bool {
        var result = try self.eval(resolver, allocator);
        defer result.deinit();
        return result.value.isTruthy();
    }
};

pub fn freeOwnedOps(allocator: std.mem.Allocator, ops: []const Op) void {
    for (ops) |op| switch (op) {
        .push_string => |s| allocator.free(s),
        else => {},
    };
}

pub fn validateStackShape(ops: []const Op) EvalError!void {
    var depth: usize = 0;
    var max_depth: usize = 0;
    for (ops) |op| {
        switch (op) {
            .push_int, .push_float, .push_string, .push_bool, .load_var, .load_list_len => depth += 1,
            .load_list_elem, .call_join => {},
            .add, .sub, .mul, .div, .cmp, .call_min, .call_max => {
                if (depth < 2) return error.InvalidExpression;
                depth -= 1;
            },
            .negate, .not, .to_bool, .jump_if_false, .jump_if_true => {
                if (depth < 1) return error.InvalidExpression;
            },
            .pop => {
                if (depth < 1) return error.InvalidExpression;
                depth -= 1;
            },
        }
        if (depth > max_depth) max_depth = depth;
    }
    if (depth != 1) return error.InvalidExpression;
    if (max_depth > Expression.max_stack) return error.StackOverflow;
}

fn promoteArith(a: Value, b: Value, comptime op: enum { add, sub, mul }) EvalError!Value {
    return switch (a) {
        .string, .bool => error.InvalidExpression,
        .int => |ai| switch (b) {
            .string, .bool => error.InvalidExpression,
            .int => |bi| .{ .int = switch (op) {
                .add => ai + bi,
                .sub => ai - bi,
                .mul => ai * bi,
            } },
            .float => |bf| blk: {
                const af: f64 = @floatFromInt(ai);
                break :blk .{ .float = switch (op) {
                    .add => af + bf,
                    .sub => af - bf,
                    .mul => af * bf,
                } };
            },
        },
        .float => |af| switch (b) {
            .string, .bool => error.InvalidExpression,
            .int, .float => blk: {
                const bf = b.toFloat();
                break :blk .{ .float = switch (op) {
                    .add => af + bf,
                    .sub => af - bf,
                    .mul => af * bf,
                } };
            },
        },
    };
}

fn cmpValues(a: Value, b: Value, op: CmpOp) EvalError!bool {
    return switch (a) {
        .string => |sa| switch (b) {
            .string => |sb| {
                const order = std.mem.order(u8, sa, sb);
                return switch (op) {
                    .eq => order == .eq,
                    .ne => order != .eq,
                    .lt => order == .lt,
                    .le => order != .gt,
                    .gt => order == .gt,
                    .ge => order != .lt,
                };
            },
            else => error.InvalidExpression,
        },
        else => switch (b) {
            .string => error.InvalidExpression,
            else => {
                const l = a.toFloat();
                const r = b.toFloat();
                return switch (op) {
                    .gt => l > r,
                    .lt => l < r,
                    .ge => l >= r,
                    .le => l <= r,
                    .eq => l == r,
                    .ne => l != r,
                };
            },
        },
    };
}

fn promoteMinMax(a: Value, b: Value, comptime pick_min: bool) EvalError!Value {
    return switch (a) {
        .string, .bool => error.InvalidExpression,
        .int => |ai| switch (b) {
            .string, .bool => error.InvalidExpression,
            .int => |bi| .{ .int = if (pick_min) @min(ai, bi) else @max(ai, bi) },
            .float => |bf| blk: {
                const af: f64 = @floatFromInt(ai);
                break :blk .{ .float = if (pick_min) @min(af, bf) else @max(af, bf) };
            },
        },
        .float => |af| switch (b) {
            .string, .bool => error.InvalidExpression,
            .int, .float => blk: {
                const bf = b.toFloat();
                break :blk .{ .float = if (pick_min) @min(af, bf) else @max(af, bf) };
            },
        },
    };
}

fn resolveScalar(val: ResolvedValue) EvalError!Value {
    return switch (val) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s },
        .list => error.InvalidExpression,
    };
}

fn joinList(allocator: std.mem.Allocator, list: ResolvedList, delimiter: []const u8) EvalError![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    for (0..list.len) |i| {
        if (i > 0) out.appendSlice(allocator, delimiter) catch return error.OutOfMemory;
        const elem = list.at(i) orelse return error.VariableNotFound;
        switch (elem) {
            .int => |v| {
                var buf: [64]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return error.OutOfMemory;
                out.appendSlice(allocator, s) catch return error.OutOfMemory;
            },
            .float => |v| {
                var buf: [64]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return error.OutOfMemory;
                out.appendSlice(allocator, s) catch return error.OutOfMemory;
            },
            .bool => |b| out.appendSlice(allocator, if (b) "true" else "false") catch return error.OutOfMemory,
            .string => |s| out.appendSlice(allocator, s) catch return error.OutOfMemory,
            .list => return error.InvalidExpression,
        }
    }

    return out.toOwnedSlice(allocator) catch error.OutOfMemory;
}
