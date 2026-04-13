/// Lightweight expression evaluator for compute steps and `if` guards.
///
/// Supports:
///   - Arithmetic: `+`, `-`, `*`, `/`
///   - Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
///   - Logical:    `&&`, `||`, `!`
///   - Parentheses for grouping
///   - Number literals: integer (`42`) and float (`3.14`, `1e3`)
///   - String literals: `"hello"`
///   - Variable references: `${name}`
///
/// Type rules:
///   - Integer literals produce `int`; literals with `.` or exponent produce `float`.
///   - `int` is implicitly promoted to `float` when the other operand is `float`.
///   - `float` can **never** be demoted to `int`; float subscripts are rejected.
///   - Division (`/`) always produces `float`.
///   - Comparison and logical operators always produce `int` (0 or 1).
///   - `len()` returns `int`; `min()`/`max()` follow promotion rules.
const std = @import("std");

/// Expression result: can be a 64-bit integer, a 64-bit float, or a string.
///
/// Promotion rule: `int` → `float` is implicit (lossless widening);
/// `float` → `int` is forbidden (precision loss).
pub const Value = union(enum) {
    int: i64,
    float: f64,
    bool: bool,
    string: []const u8,

    /// Promote to `f64`. Integer values are widened losslessly.
    /// Caller must ensure this is not called on `.string`.
    pub fn toFloat(self: Value) f64 {
        return switch (self) {
            .int => |i| @floatFromInt(i),
            .float => |f| f,
            .bool => |b| if (b) 1.0 else 0.0,
            .string => unreachable,
        };
    }

    /// True when the value is non-zero (numeric) or non-empty (string).
    pub fn isTruthy(self: Value) bool {
        return switch (self) {
            .int => |i| i != 0,
            .float => |f| f != 0.0,
            .bool => |b| b,
            .string => |s| s.len != 0,
        };
    }
};

pub const EvalError = error{
    InvalidExpression,
    UnexpectedToken,
    UnmatchedParen,
    DivisionByZero,
    VariableNotFound,
    InvalidNumber,
    OutOfMemory,
    StackOverflow,
};

pub const BuiltinVar = enum {
    iter,
    task_idx,
    elapsed_ms,
};

pub const VariableBinding = union(enum) {
    slot: usize,
    builtin: BuiltinVar,
};

pub const VariableRef = union(enum) {
    name: []const u8,
    binding: VariableBinding,

    pub fn slotIndex(self: VariableRef) ?usize {
        return switch (self) {
            .binding => |b| switch (b) {
                .slot => |s| s,
                .builtin => null,
            },
            .name => null,
        };
    }
};

// ── Bytecode ────────────────────────────────────────────────────────────
//
// The parser emits a linear array of `Op` instructions in reverse-Polish
// (postfix) order.  Evaluation uses a small value stack and a flat `for`
// loop

const CmpOp = enum { gt, lt, ge, le, eq, ne };

pub const Op = union(enum) {
    push_int: i64,
    push_float: f64,
    push_string: []const u8,
    push_bool: bool,
    /// Push the scalar value of a bound variable.
    load_var: VariableRef,
    /// Push the length (as int) of a list variable.
    load_list_len: VariableRef,
    /// Pop an integer index, push the element from a list variable.
    load_list_elem: VariableRef,
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
    call_join: VariableRef,
    /// Short-circuit AND: if top is falsy, replace with int(0) and skip
    /// `skip` ops (jumping past the RHS and its trailing `to_bool`).
    and_sc: u16,
    /// Short-circuit OR: if top is truthy, replace with int(1) and skip.
    or_sc: u16,
    /// Replace top of stack with int 0 or 1.
    to_bool,
};

/// Pre-parsed, compiled expression that can be evaluated many times.
pub const Expression = struct {
    ops: []Op,

    pub fn deinit(self: *Expression, allocator: std.mem.Allocator) void {
        allocator.free(self.ops);
    }

    pub const max_stack = 64;
    const max_owned = 4;

    /// Result of expression evaluation, bundling the value with ownership info.
    pub const EvalResult = struct {
        value: Value,
        /// Allocator-owned string produced by join(); null when no allocation was made.
        owned: ?[]u8 = null,
        allocator: std.mem.Allocator,

        pub fn deinit(self: EvalResult) void {
            if (self.owned) |buf| self.allocator.free(buf);
        }
    };

    pub fn eval(self: *const Expression, resolver: VarResolver, allocator: std.mem.Allocator) EvalError!EvalResult {
        // Fast path: single push op needs no stack machinery.
        if (self.ops.len == 1) switch (self.ops[0]) {
            .push_int => |i| return .{ .value = .{ .int = i }, .allocator = undefined },
            .push_float => |f| return .{ .value = .{ .float = f }, .allocator = undefined },
            .push_bool => |b| return .{ .value = .{ .bool = b }, .allocator = undefined },
            .push_string => |s| return .{ .value = .{ .string = s }, .allocator = undefined },
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
                .load_var => |ref| {
                    const resolved = resolver.resolve(ref.binding) orelse return error.VariableNotFound;
                    stack[sp] = try resolveScalar(resolved);
                    sp += 1;
                },
                .load_list_len => |ref| {
                    const resolved = resolver.resolve(ref.binding) orelse return error.VariableNotFound;
                    stack[sp] = switch (resolved) {
                        .list => |l| .{ .int = @intCast(l.len) },
                        else => return error.InvalidExpression,
                    };
                    sp += 1;
                },
                .load_list_elem => |ref| {
                    sp -= 1;
                    const idx_val = stack[sp];
                    const iv = switch (idx_val) {
                        .int => |v| v,
                        .float, .string, .bool => return error.InvalidExpression,
                    };
                    if (iv < 0) return error.InvalidExpression;
                    const i: usize = @intCast(iv);
                    const resolved = resolver.resolve(ref.binding) orelse return error.VariableNotFound;
                    const list = switch (resolved) {
                        .list => |l| l,
                        else => return error.InvalidExpression,
                    };
                    if (i >= list.len) return error.InvalidExpression;
                    const elem = list.at(i) orelse return error.VariableNotFound;
                    stack[sp] = try resolveScalar(elem);
                    sp += 1;
                },
                // NOTE: all handlers below read `stack[sp - 1]` (or `stack[sp]`)
                // into locals BEFORE writing back.  This works around a Zig ≤ 0.15
                // miscompilation where constructing a tagged-union literal on the
                // LHS (`stack[runtime_idx] = .{ .float = stack[runtime_idx]… }`)
                // writes the tag byte before the RHS finishes reading the payload,
                // corrupting the value.  See repro.zig in this repo.
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
                .and_sc => |skip| {
                    const v = stack[sp - 1];
                    if (!v.isTruthy()) {
                        stack[sp - 1] = .{ .bool = false };
                        ip += skip;
                    } else {
                        sp -= 1;
                    }
                },
                .or_sc => |skip| {
                    const v = stack[sp - 1];
                    if (v.isTruthy()) {
                        stack[sp - 1] = .{ .bool = true };
                        ip += skip;
                    } else {
                        sp -= 1;
                    }
                },
                .to_bool => {
                    const v = stack[sp - 1];
                    stack[sp - 1] = .{ .bool = v.isTruthy() };
                },
                .call_join => |ref| {
                    sp -= 1;
                    const delim = switch (stack[sp]) {
                        .string => |s| s,
                        else => return error.InvalidExpression,
                    };
                    const resolved = resolver.resolve(ref.binding) orelse return error.VariableNotFound;
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

    /// Evaluates and returns true when the result is non-zero.
    pub fn isTruthy(self: *const Expression, resolver: VarResolver, allocator: std.mem.Allocator) EvalError!bool {
        const result = try self.eval(resolver, allocator);
        defer result.deinit();
        return result.value.isTruthy();
    }

    /// Iterates through all variable names referenced in the expression.
    pub fn variables(self: *const Expression) VariableIterator {
        return .{ .ops = self.ops };
    }

    /// Rewrites variable references from source names to compiled bindings.
    pub fn bindVariables(self: *Expression, slots: anytype) !void {
        for (self.ops) |*op| {
            switch (op.*) {
                .load_var => |ref| switch (ref) {
                    .name => |name| {
                        op.* = .{ .load_var = .{ .binding = resolveBuiltin(name) orelse .{
                            .slot = slots.getIndex(name) orelse return error.UndeclaredVariable,
                        } } };
                    },
                    .binding => return error.AlreadyBound,
                },
                .load_list_len => |ref| switch (ref) {
                    .name => |name| {
                        op.* = .{ .load_list_len = .{ .binding = resolveBuiltin(name) orelse .{
                            .slot = slots.getIndex(name) orelse return error.UndeclaredVariable,
                        } } };
                    },
                    .binding => return error.AlreadyBound,
                },
                .load_list_elem => |ref| switch (ref) {
                    .name => |name| {
                        op.* = .{ .load_list_elem = .{ .binding = resolveBuiltin(name) orelse .{
                            .slot = slots.getIndex(name) orelse return error.UndeclaredVariable,
                        } } };
                    },
                    .binding => return error.AlreadyBound,
                },
                .call_join => |ref| switch (ref) {
                    .name => |name| {
                        op.* = .{ .call_join = .{ .binding = resolveBuiltin(name) orelse .{
                            .slot = slots.getIndex(name) orelse return error.UndeclaredVariable,
                        } } };
                    },
                    .binding => return error.AlreadyBound,
                },
                else => {},
            }
        }
    }
};

/// Iterator over all variable names in an expression.
pub const VariableIterator = struct {
    ops: []const Op,
    idx: usize = 0,

    pub fn next(self: *VariableIterator) ?[]const u8 {
        while (self.idx < self.ops.len) : (self.idx += 1) {
            const ref: VariableRef = switch (self.ops[self.idx]) {
                .load_var => |r| r,
                .load_list_len => |r| r,
                .load_list_elem => |r| r,
                .call_join => |r| r,
                else => continue,
            };
            switch (ref) {
                .name => |n| {
                    self.idx += 1;
                    return n;
                },
                .binding => {},
            }
        }
        return null;
    }
};

pub const ResolvedValue = union(enum) {
    int: i64,
    float: f64,
    bool: bool,
    string: []const u8,
    /// Opaque list: length and a callback to resolve individual elements.
    list: ResolvedList,
};

pub const ResolvedList = struct {
    len: usize,
    ctx: *const anyopaque,
    at_fn: *const fn (ctx: *const anyopaque, index: usize) ?ResolvedValue,

    fn at(self: ResolvedList, index: usize) ?ResolvedValue {
        return self.at_fn(self.ctx, index);
    }
};

/// Opaque variable resolver: calls the provided function to map bindings to values.
pub const VarResolver = struct {
    ctx: *const anyopaque,
    resolve_fn: *const fn (ctx: *const anyopaque, binding: VariableBinding) ?ResolvedValue,

    pub fn resolve(self: VarResolver, binding: VariableBinding) ?ResolvedValue {
        return self.resolve_fn(self.ctx, binding);
    }

    /// Returns a resolver that always yields null (for variable-free expressions).
    pub fn none() VarResolver {
        return .{
            .ctx = undefined,
            .resolve_fn = struct {
                fn noResolve(_: *const anyopaque, _: VariableBinding) ?ResolvedValue {
                    return null;
                }
            }.noResolve,
        };
    }
};

/// Parse an expression string into a compiled bytecode program.
pub fn parse(allocator: std.mem.Allocator, source: []const u8) EvalError!Expression {
    var parser = Parser{
        .allocator = allocator,
        .source = source,
        .pos = 0,
        .ops = .empty,
    };
    errdefer parser.ops.deinit(allocator);

    try parser.parseOr();
    if (parser.pos < parser.source.len) {
        parser.skipWhitespace();
        if (parser.pos < parser.source.len) return error.UnexpectedToken;
    }

    return .{
        .ops = try parser.ops.toOwnedSlice(allocator),
    };
}

// ── Eval helpers ────────────────────────────────────────────────────────

/// Arithmetic with integer promotion: int ⊗ int → int; otherwise float.
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

/// Compare two values. Mixed int/float promotes to float.
/// String-string comparison uses lexicographic order; mixed string/number is an error.
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

/// min/max with integer promotion: int ⊗ int → int; otherwise float.
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

/// Convert a resolved external value to an expression `Value`.
fn resolveScalar(val: ResolvedValue) EvalError!Value {
    return switch (val) {
        .int => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .bool => |b| .{ .bool = b },
        .string => |s| .{ .string = s },
        .list => error.InvalidExpression,
    };
}

/// Join list elements into a single string with the given delimiter.
fn joinList(allocator: std.mem.Allocator, list: ResolvedList, delimiter: []const u8) EvalError![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    for (0..list.len) |i| {
        if (i > 0) out.appendSlice(allocator, delimiter) catch return error.OutOfMemory;
        const elem = list.at(i) orelse return error.VariableNotFound;
        switch (elem) {
            .int => |v| out.writer(allocator).print("{d}", .{v}) catch return error.OutOfMemory,
            .float => |v| out.writer(allocator).print("{d}", .{v}) catch return error.OutOfMemory,
            .bool => |b| out.appendSlice(allocator, if (b) "true" else "false") catch return error.OutOfMemory,
            .string => |s| out.appendSlice(allocator, s) catch return error.OutOfMemory,
            .list => return error.InvalidExpression,
        }
    }

    return out.toOwnedSlice(allocator) catch error.OutOfMemory;
}

pub fn resolveBuiltin(name: []const u8) ?VariableBinding {
    if (std.mem.eql(u8, name, "$ITER")) return .{ .builtin = .iter };
    if (std.mem.eql(u8, name, "$TASK_IDX")) return .{ .builtin = .task_idx };
    if (std.mem.eql(u8, name, "$ELAPSED_MS")) return .{ .builtin = .elapsed_ms };
    return null;
}

// ── Recursive-descent parser (emits bytecode directly) ──────────────────
//
// Precedence (lowest to highest):
//   ||
//   &&
//   == !=
//   > < >= <=
//   + -
//   * /
//   unary (! -)
//   atom (number, variable, parenthesized)

const Parser = struct {
    allocator: std.mem.Allocator,
    source: []const u8,
    pos: usize,
    ops: std.ArrayList(Op),
    depth: usize = 0,
    max_depth: usize = 0,

    fn push(self: *Parser) EvalError!void {
        self.depth += 1;
        if (self.depth > Expression.max_stack) return error.StackOverflow;
        if (self.depth > self.max_depth) self.max_depth = self.depth;
    }

    fn pop(self: *Parser, n: usize) void {
        self.depth -= n;
    }

    fn emit(self: *Parser, op: Op) EvalError!void {
        self.ops.append(self.allocator, op) catch return error.OutOfMemory;
    }

    fn skipWhitespace(self: *Parser) void {
        while (self.pos < self.source.len and (self.source[self.pos] == ' ' or self.source[self.pos] == '\t')) {
            self.pos += 1;
        }
    }

    fn peek(self: *Parser) ?u8 {
        self.skipWhitespace();
        if (self.pos >= self.source.len) return null;
        return self.source[self.pos];
    }

    fn matchChar(self: *Parser, expected: u8) bool {
        self.skipWhitespace();
        if (self.pos < self.source.len and self.source[self.pos] == expected) {
            self.pos += 1;
            return true;
        }
        return false;
    }

    fn matchStr(self: *Parser, prefix: []const u8) bool {
        self.skipWhitespace();
        if (std.mem.startsWith(u8, self.source[self.pos..], prefix)) {
            self.pos += prefix.len;
            return true;
        }
        return false;
    }

    // ── Precedence levels ───────────────────────────────────────────

    fn parseOr(self: *Parser) EvalError!void {
        try self.parseAnd();
        while (true) {
            if (self.matchStr("||")) {
                const sc_pos = self.ops.items.len;
                try self.emit(.{ .or_sc = 0 }); // placeholder
                self.pop(1); // LHS consumed by short-circuit
                try self.parseAnd();
                try self.emit(.to_bool);
                self.ops.items[sc_pos] = .{ .or_sc = @intCast(self.ops.items.len - sc_pos - 1) };
            } else break;
        }
    }

    fn parseAnd(self: *Parser) EvalError!void {
        try self.parseEquality();
        while (true) {
            if (self.matchStr("&&")) {
                const sc_pos = self.ops.items.len;
                try self.emit(.{ .and_sc = 0 }); // placeholder
                self.pop(1); // LHS consumed by short-circuit
                try self.parseEquality();
                try self.emit(.to_bool);
                self.ops.items[sc_pos] = .{ .and_sc = @intCast(self.ops.items.len - sc_pos - 1) };
            } else break;
        }
    }

    fn parseEquality(self: *Parser) EvalError!void {
        try self.parseComparison();
        while (true) {
            if (self.matchStr("==")) {
                try self.parseComparison();
                try self.emit(.{ .cmp = .eq });
                self.pop(1); // binary: 2 in, 1 out
            } else if (self.matchStr("!=")) {
                try self.parseComparison();
                try self.emit(.{ .cmp = .ne });
                self.pop(1);
            } else break;
        }
    }

    fn parseComparison(self: *Parser) EvalError!void {
        try self.parseAddSub();
        while (true) {
            if (self.matchStr(">=")) {
                try self.parseAddSub();
                try self.emit(.{ .cmp = .ge });
                self.pop(1);
            } else if (self.matchStr("<=")) {
                try self.parseAddSub();
                try self.emit(.{ .cmp = .le });
                self.pop(1);
            } else if (self.matchStr(">")) {
                try self.parseAddSub();
                try self.emit(.{ .cmp = .gt });
                self.pop(1);
            } else if (self.matchStr("<")) {
                try self.parseAddSub();
                try self.emit(.{ .cmp = .lt });
                self.pop(1);
            } else break;
        }
    }

    fn parseAddSub(self: *Parser) EvalError!void {
        try self.parseMulDiv();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('+')) {
                try self.parseMulDiv();
                try self.emit(.add);
                self.pop(1);
            } else if (self.pos < self.source.len and self.source[self.pos] == '-') {
                // Distinguish binary minus from unary minus by consuming here.
                self.pos += 1;
                try self.parseMulDiv();
                try self.emit(.sub);
                self.pop(1);
            } else break;
        }
    }

    fn parseMulDiv(self: *Parser) EvalError!void {
        try self.parseUnary();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('*')) {
                try self.parseUnary();
                try self.emit(.mul);
                self.pop(1);
            } else if (self.matchChar('/')) {
                try self.parseUnary();
                try self.emit(.div);
                self.pop(1);
            } else break;
        }
    }

    fn parseUnary(self: *Parser) EvalError!void {
        self.skipWhitespace();
        if (self.matchChar('!')) {
            try self.parseUnary();
            try self.emit(.not);
            return;
        }
        if (self.pos < self.source.len and self.source[self.pos] == '-') {
            // Only treat as unary if the next char is not a digit (avoid conflicting with negative number literals handled by atom).
            if (self.pos + 1 < self.source.len and !std.ascii.isDigit(self.source[self.pos + 1]) and self.source[self.pos + 1] != '.') {
                self.pos += 1;
                try self.parseUnary();
                try self.emit(.negate);
                return;
            }
        }
        try self.parseAtom();
    }

    fn parseAtom(self: *Parser) EvalError!void {
        self.skipWhitespace();
        if (self.pos >= self.source.len) return error.InvalidExpression;

        // Parenthesized sub-expression.
        if (self.source[self.pos] == '(') {
            self.pos += 1;
            try self.parseOr();
            if (!self.matchChar(')')) return error.UnmatchedParen;
            return;
        }

        // Variable reference: ${name} or ${name}[subscript]
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            self.pos += 2;
            const name_start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '}') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return error.InvalidExpression;
            const name = self.source[name_start..self.pos];
            self.pos += 1; // skip '}'

            // Check for list subscript [expr]
            if (self.pos < self.source.len and self.source[self.pos] == '[') {
                self.pos += 1;
                try self.parseOr(); // subscript → pushes index onto stack
                if (!self.matchChar(']')) return error.UnmatchedParen;
                try self.emit(.{ .load_list_elem = .{ .name = name } });
                // subscript pushed +1, load_list_elem pops index & pushes elem → net 0
                // from the subscript's perspective; overall atom contributes +1
            } else {
                try self.emit(.{ .load_var = .{ .name = name } });
                try self.push();
            }
            return;
        }

        // Built-in variable: $ITER, $TASK_IDX
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            const start = self.pos;
            self.pos += 1;
            while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
            const name = self.source[start..self.pos];
            try self.emit(.{ .load_var = .{ .name = name } });
            try self.push();
            return;
        }

        // Function call: min(x, y), max(x, y), len(x)
        if (std.ascii.isAlphabetic(self.source[self.pos])) {
            const name_start = self.pos;
            while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
            const name = self.source[name_start..self.pos];
            self.skipWhitespace();
            if (self.matchChar('(')) {
                if (std.mem.eql(u8, name, "len")) {
                    // len() requires a single variable argument.
                    const var_ref = try self.parseVarArg();
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    try self.emit(.{ .load_list_len = var_ref });
                    try self.push();
                } else if (std.mem.eql(u8, name, "min") or std.mem.eql(u8, name, "max")) {
                    try self.parseOr(); // first arg
                    if (!self.matchChar(',')) return error.UnexpectedToken;
                    try self.parseOr(); // second arg
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    try self.emit(if (std.mem.eql(u8, name, "min")) .call_min else .call_max);
                    self.pop(1); // binary: 2 in, 1 out
                } else if (std.mem.eql(u8, name, "join")) {
                    // join(${list_var}, "delimiter")
                    const var_ref = try self.parseVarArg();
                    if (!self.matchChar(',')) return error.UnexpectedToken;
                    try self.parseOr(); // delimiter expression (usually a string literal)
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    try self.emit(.{ .call_join = var_ref });
                    // delimiter pushed by parseOr (+1), call_join pops it and pushes result: net 0
                } else {
                    return error.InvalidExpression;
                }
                return;
            } else {
                // Not a function, backtrack to start of name.
                self.pos = name_start;
            }
        }

        // String literal: "..."
        if (self.pos < self.source.len and self.source[self.pos] == '"') {
            self.pos += 1;
            const start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '"') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return error.InvalidExpression;
            const str = self.source[start..self.pos];
            self.pos += 1; // skip closing quote
            try self.push();
            try self.emit(.{ .push_string = str });
            return;
        }

        // Number literal (including negative).
        if (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '-' or self.source[self.pos] == '.') {
            try self.parseNumber();
            return;
        }

        return error.UnexpectedToken;
    }

    /// Parse a bare variable reference (${name} or $BUILTIN) without
    /// emitting an op.  Used for function args that must be variables.
    fn parseVarArg(self: *Parser) EvalError!VariableRef {
        self.skipWhitespace();
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            self.pos += 2;
            const name_start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '}') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return error.InvalidExpression;
            const name = self.source[name_start..self.pos];
            self.pos += 1;
            return .{ .name = name };
        }
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            const start = self.pos;
            self.pos += 1;
            while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
            return .{ .name = self.source[start..self.pos] };
        }
        return error.InvalidExpression;
    }

    fn parseNumber(self: *Parser) EvalError!void {
        const start = self.pos;
        if (self.pos < self.source.len and self.source[self.pos] == '-') self.pos += 1;
        var is_float = false;
        while (self.pos < self.source.len and (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '.')) {
            if (self.source[self.pos] == '.') is_float = true;
            self.pos += 1;
        }
        // Scientific notation (e.g., 1e3, 2.5E-4) implies float.
        if (self.pos < self.source.len and (self.source[self.pos] == 'e' or self.source[self.pos] == 'E')) {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.source.len and (self.source[self.pos] == '+' or self.source[self.pos] == '-')) self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) : (self.pos += 1) {}
        }
        if (self.pos == start) return error.InvalidNumber;
        const text = self.source[start..self.pos];
        if (!is_float) {
            const int_val = std.fmt.parseInt(i64, text, 10) catch return error.InvalidNumber;
            try self.emit(.{ .push_int = int_val });
        } else {
            const float_val = std.fmt.parseFloat(f64, text) catch return error.InvalidNumber;
            try self.emit(.{ .push_float = float_val });
        }
        try self.push();
    }
};

// ── Tests ───────────────────────────────────────────────────────────────

/// Test helper: parse + eval in one shot (no variable binding).
fn testEval(allocator: std.mem.Allocator, source: []const u8, resolver: VarResolver) EvalError!Value {
    var expr_obj = try parse(allocator, source);
    defer expr_obj.deinit(allocator);
    const result = try expr_obj.eval(resolver, allocator);
    std.debug.assert(result.owned == null);
    return result.value;
}

/// Test helper: resolves slots back to string values via a name-indexed HashMap.
const TestContext = struct {
    vars: *const std.StringHashMap([]const u8),
    slot_names: [32][]const u8 = undefined,
    count: usize = 0,

    fn addVar(self: *TestContext, name: []const u8) usize {
        for (self.slot_names[0..self.count], 0..) |n, i| {
            if (std.mem.eql(u8, n, name)) return i;
        }
        const idx = self.count;
        self.slot_names[self.count] = name;
        self.count += 1;
        return idx;
    }

    fn resolve(ctx_ptr: *const anyopaque, binding: VariableBinding) ?ResolvedValue {
        const self: *const TestContext = @ptrCast(@alignCast(ctx_ptr));
        return switch (binding) {
            .slot => |slot| {
                if (slot >= self.count) return null;
                const s = self.vars.get(self.slot_names[slot]) orelse return null;
                if (std.fmt.parseInt(i64, s, 10)) |i| return .{ .int = i } else |_| {}
                if (std.fmt.parseFloat(f64, s)) |f| return .{ .float = f } else |_| {}
                return .{ .string = s };
            },
            .builtin => null,
        };
    }

    fn resolver(self: *const TestContext) VarResolver {
        return .{ .ctx = @ptrCast(self), .resolve_fn = resolve };
    }
};

fn testBoundEval(source: []const u8, vars: *const std.StringHashMap([]const u8)) !Value {
    var expr_obj = try parse(std.testing.allocator, source);
    defer expr_obj.deinit(std.testing.allocator);
    var tc = TestContext{ .vars = vars };
    var slots: std.StringArrayHashMap(void) = .init(std.testing.allocator);
    defer slots.deinit();
    var it = expr_obj.variables();
    while (it.next()) |name| {
        if (slots.getIndex(name) == null) {
            _ = tc.addVar(name);
            try slots.put(name, {});
        }
    }
    try expr_obj.bindVariables(&slots);
    const result = try expr_obj.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    return result.value;
}

fn expectInt(expected: i64, actual: Value) !void {
    switch (actual) {
        .int => |i| try std.testing.expectEqual(expected, i),
        .float, .bool, .string => return error.TestUnexpectedResult,
    }
}

fn expectFloat(expected: f64, actual: Value) !void {
    switch (actual) {
        .float => |f| try std.testing.expectApproxEqAbs(expected, f, 1e-9),
        .int, .bool, .string => return error.TestUnexpectedResult,
    }
}

fn expectString(expected: []const u8, actual: Value) !void {
    switch (actual) {
        .string => |s| try std.testing.expectEqualStrings(expected, s),
        .int, .float, .bool => return error.TestUnexpectedResult,
    }
}

fn expectBool(expected: bool, actual: Value) !void {
    switch (actual) {
        .bool => |b| try std.testing.expectEqual(expected, b),
        .int, .float, .string => return error.TestUnexpectedResult,
    }
}

test "expr arithmetic" {
    const r = VarResolver.none();

    try expectInt(7, try testEval(std.testing.allocator, "3 + 4", r));
    try expectInt(6, try testEval(std.testing.allocator, "2 * 3", r));
    try expectFloat(2.5, try testEval(std.testing.allocator, "5 / 2", r)); // division always float
    try expectFloat(0.0, try testEval(std.testing.allocator, "0 + 0.0", r));
    try expectInt(0, try testEval(std.testing.allocator, "0 + 0", r));

    try expectInt(14, try testEval(std.testing.allocator, "2 + 3 * 4", r));
    try expectInt(20, try testEval(std.testing.allocator, "(2 + 3) * 4", r));
}

test "expr comparison" {
    const r = VarResolver.none();

    try expectBool(true, try testEval(std.testing.allocator, "5 > 3", r));
    try expectBool(false, try testEval(std.testing.allocator, "2 > 3", r));
    try expectBool(true, try testEval(std.testing.allocator, "3 >= 3", r));
    try expectBool(true, try testEval(std.testing.allocator, "3 == 3", r));
    try expectBool(true, try testEval(std.testing.allocator, "3 != 4", r));
}

test "expr logical" {
    const r = VarResolver.none();

    try expectBool(true, try testEval(std.testing.allocator, "1 && 1", r));
    try expectBool(false, try testEval(std.testing.allocator, "1 && 0", r));
    try expectBool(true, try testEval(std.testing.allocator, "0 || 1", r));
    try expectBool(true, try testEval(std.testing.allocator, "!0", r));
    try expectBool(false, try testEval(std.testing.allocator, "!1", r));
}

test "expr variables" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("voltage", "4.5");
    try vars.put("current", "2.0");

    try expectFloat(9.0, try testBoundEval("${voltage} * ${current}", &vars));
    try expectBool(true, try testBoundEval("${voltage} > 3", &vars));
}

test "expr built-in variables" {
    const resolver_v = VarResolver{
        .ctx = undefined,
        .resolve_fn = struct {
            fn resolve(_: *const anyopaque, binding: VariableBinding) ?ResolvedValue {
                return switch (binding) {
                    .builtin => |b| switch (b) {
                        .iter => .{ .int = 42 },
                        .task_idx => null,
                        .elapsed_ms => null,
                    },
                    .slot => null,
                };
            }
        }.resolve,
    };

    var empty_slots: std.StringArrayHashMap(void) = .init(std.testing.allocator);
    defer empty_slots.deinit();

    var e1 = try parse(std.testing.allocator, "$ITER");
    defer e1.deinit(std.testing.allocator);
    try e1.bindVariables(&empty_slots);
    try expectInt(42, (try e1.eval(resolver_v, std.testing.allocator)).value);

    var e2 = try parse(std.testing.allocator, "$ITER + 1");
    defer e2.deinit(std.testing.allocator);
    try e2.bindVariables(&empty_slots);
    try expectInt(43, (try e2.eval(resolver_v, std.testing.allocator)).value);
}

test "expr functions" {
    const r = VarResolver.none();

    try expectInt(3, try testEval(std.testing.allocator, "min(3, 5)", r));
    try expectInt(5, try testEval(std.testing.allocator, "max(3, 5)", r));
    try expectInt(7, try testEval(std.testing.allocator, "max(min(10, 2), 7)", r));
}

test "expr complex power check" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("voltage", "12.0");
    try vars.put("current", "9.0");

    // power = 108.0 (float * float), check > 100 (int) → promoted comparison
    try expectBool(true, try testBoundEval("${voltage} * ${current} > 100", &vars));
}

test "expr division by zero" {
    try std.testing.expectError(error.DivisionByZero, testEval(std.testing.allocator, "1 / 0", VarResolver.none()));
}

test "expr missing variable" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();

    try std.testing.expectError(error.VariableNotFound, testBoundEval("${missing}", &vars));
}

test "expr unmatched paren" {
    try std.testing.expectError(error.UnmatchedParen, testEval(std.testing.allocator, "(1 + 2", VarResolver.none()));
}

test "expr negative literal" {
    const r = VarResolver.none();

    try expectInt(-3, try testEval(std.testing.allocator, "-3", r));
    try expectInt(-1, try testEval(std.testing.allocator, "2 + -3", r));
}

test "expr unary negation of variable" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("x", "5");

    try expectInt(-5, try testBoundEval("-${x}", &vars));
}

test "expr parse reuse" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();

    var expr_obj = try parse(std.testing.allocator, "${x} * 2 + 1");
    defer expr_obj.deinit(std.testing.allocator);

    var tc = TestContext{ .vars = &vars };
    var slots: std.StringArrayHashMap(void) = .init(std.testing.allocator);
    defer slots.deinit();
    var it = expr_obj.variables();
    while (it.next()) |name| {
        if (slots.getIndex(name) == null) {
            _ = tc.addVar(name);
            try slots.put(name, {});
        }
    }
    try expr_obj.bindVariables(&slots);

    try vars.put("x", "3");
    try expectInt(7, (try expr_obj.eval(tc.resolver(), std.testing.allocator)).value);

    try vars.put("x", "10");
    try expectInt(21, (try expr_obj.eval(tc.resolver(), std.testing.allocator)).value);
}

test "resolveBuiltin recognizes $ELAPSED_MS" {
    const binding = resolveBuiltin("$ELAPSED_MS") orelse return error.TestUnexpectedResult;
    switch (binding) {
        .builtin => |b| try std.testing.expect(b == .elapsed_ms),
        .slot => return error.TestUnexpectedResult,
    }
}

// ── List tests ──────────────────────────────────────────────────────────

/// Test helper that stores both scalar and list values for slot-based resolution.
const ListTestContext = struct {
    /// Per-slot values: scalar (string) or list (slice of f64).
    slot_values: [32]SlotValue = undefined,
    slot_names: [32][]const u8 = undefined,
    count: usize = 0,

    const SlotValue = union(enum) {
        scalar: []const u8,
        list: []const f64,
    };

    fn addScalar(self: *ListTestContext, name: []const u8, value: []const u8) void {
        self.slot_names[self.count] = name;
        self.slot_values[self.count] = .{ .scalar = value };
        self.count += 1;
    }

    fn addList(self: *ListTestContext, name: []const u8, items: []const f64) void {
        self.slot_names[self.count] = name;
        self.slot_values[self.count] = .{ .list = items };
        self.count += 1;
    }

    fn resolve(ctx_ptr: *const anyopaque, binding: VariableBinding) ?ResolvedValue {
        const self: *const ListTestContext = @ptrCast(@alignCast(ctx_ptr));
        return switch (binding) {
            .slot => |slot| {
                if (slot >= self.count) return null;
                return switch (self.slot_values[slot]) {
                    .scalar => |s| {
                        if (std.fmt.parseInt(i64, s, 10)) |i| return .{ .int = i } else |_| {}
                        if (std.fmt.parseFloat(f64, s)) |f| return .{ .float = f } else |_| {}
                        return .{ .string = s };
                    },
                    .list => |items| .{ .list = .{
                        .len = items.len,
                        .ctx = @ptrCast(items.ptr),
                        .at_fn = listAtFn,
                    } },
                };
            },
            .builtin => null,
        };
    }

    fn listAtFn(ctx: *const anyopaque, index: usize) ?ResolvedValue {
        const ptr: [*]const f64 = @ptrCast(@alignCast(ctx));
        return .{ .float = ptr[index] };
    }

    fn resolver(self: *const ListTestContext) VarResolver {
        return .{ .ctx = @ptrCast(self), .resolve_fn = resolve };
    }

    fn slots(self: *const ListTestContext) std.StringArrayHashMap(void) {
        var map: std.StringArrayHashMap(void) = .init(std.testing.allocator);
        for (self.slot_names[0..self.count]) |name| {
            map.put(name, {}) catch unreachable;
        }
        return map;
    }
};

test "expr len() on list variable" {
    var tc: ListTestContext = .{};
    tc.addList("voltages", &.{ 1.0, 2.0, 3.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "len(${voltages})");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try expectInt(3, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list indexing" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });
    tc.addScalar("idx", "1");

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "${arr}[${idx}]");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try expectFloat(20.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list index with literal" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "${arr}[2]");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try expectFloat(30.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list index out of bounds" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "${arr}[5]");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}

test "expr list in arithmetic" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "${arr}[0] + ${arr}[2]");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try expectFloat(40.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr bare list variable is error" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{1.0});

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "${arr} + 1");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}

test "expr len() in arithmetic" {
    var tc: ListTestContext = .{};
    tc.addList("items", &.{ 5.0, 10.0, 15.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "len(${items}) - 1");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try expectInt(3, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr stack overflow rejected at parse time" {
    // Build "1+(1+(1+(...)))" with >64 nesting levels.
    // Each pending "1+" keeps its LHS on the stack while recursing into the RHS,
    // so 65 levels requires 65 simultaneous stack slots → exceeds max_stack.
    var buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();
    for (0..Expression.max_stack + 1) |_| {
        writer.writeAll("1+(") catch unreachable;
    }
    writer.writeAll("1") catch unreachable;
    for (0..Expression.max_stack + 1) |_| {
        writer.writeByte(')') catch unreachable;
    }
    const src = stream.getWritten();
    try std.testing.expectError(error.StackOverflow, parse(std.testing.allocator, src));
}

test "expr string literal" {
    const r = VarResolver.none();
    try expectString("hello", try testEval(std.testing.allocator, "\"hello\"", r));
    try expectString("", try testEval(std.testing.allocator, "\"\"", r));
}

test "expr string comparison" {
    const r = VarResolver.none();

    try expectBool(true, try testEval(std.testing.allocator, "\"abc\" == \"abc\"", r));
    try expectBool(false, try testEval(std.testing.allocator, "\"abc\" == \"def\"", r));
    try expectBool(true, try testEval(std.testing.allocator, "\"abc\" != \"def\"", r));
    try expectBool(true, try testEval(std.testing.allocator, "\"abc\" < \"def\"", r));
    try expectBool(false, try testEval(std.testing.allocator, "\"def\" < \"abc\"", r));
    try expectBool(true, try testEval(std.testing.allocator, "\"z\" >= \"a\"", r));
}

test "expr string variable comparison" {
    var vars: std.StringHashMap([]const u8) = .init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("status", "ready");

    try expectBool(true, try testBoundEval("${status} == \"ready\"", &vars));
    try expectBool(false, try testBoundEval("${status} == \"stopped\"", &vars));
    try expectBool(true, try testBoundEval("${status} != \"stopped\"", &vars));
}

test "expr string truthiness" {
    const r = VarResolver.none();

    // Non-empty string is truthy
    try expectBool(true, try testEval(std.testing.allocator, "\"hello\" && 1", r));
    // Empty string is falsy
    try expectBool(false, try testEval(std.testing.allocator, "\"\" && 1", r));
    try expectBool(false, try testEval(std.testing.allocator, "!\"hello\"", r));
    try expectBool(true, try testEval(std.testing.allocator, "!\"\"", r));
}

test "expr string arithmetic is error" {
    const r = VarResolver.none();

    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "\"a\" + 1", r));
    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "\"a\" * 2", r));
    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "\"a\" / 1", r));
    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "-\"a\"", r));
}

test "expr mixed string-number comparison is error" {
    const r = VarResolver.none();

    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "\"a\" > 1", r));
    try std.testing.expectError(error.InvalidExpression, testEval(std.testing.allocator, "1 == \"a\"", r));
}

test "expr join() on list variable" {
    var tc: ListTestContext = .{};
    tc.addList("channels", &.{ 1.0, 2.0, 3.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "join(${channels}, \",\")");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    const result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("1,2,3", result.value);
}

test "expr join() with custom delimiter" {
    var tc: ListTestContext = .{};
    tc.addList("items", &.{ 10.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "join(${items}, \" | \")");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    const result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("10 | 20", result.value);
}

test "expr join() on empty list" {
    var tc: ListTestContext = .{};
    tc.addList("empty", &.{});

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "join(${empty}, \",\")");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    const result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("", result.value);
}

test "expr join() on non-list is error" {
    var tc: ListTestContext = .{};
    tc.addScalar("x", "42");

    var slot_map = tc.slots();
    defer slot_map.deinit();

    var e = try parse(std.testing.allocator, "join(${x}, \",\")");
    defer e.deinit(std.testing.allocator);
    try e.bindVariables(&slot_map);

    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}
