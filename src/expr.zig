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
const diagnostic_mod = @import("diagnostic.zig");

const types = @import("expr/types.zig");
const ast_mod = @import("expr/ast.zig");
const bytecode = @import("expr/bytecode.zig");
const parse_ast_mod = @import("expr/parse_ast.zig");

pub const Value = types.Value;
pub const ArithOp = types.ArithOp;
pub const CmpOp = types.CmpOp;
pub const EvalError = types.EvalError;
pub const CompileError = types.CompileError;
pub const Diagnostics = types.Diagnostics;
pub const Diagnostic = types.Diagnostic;
pub const Message = types.Message;
pub const Span = types.Span;
pub const BuiltinVar = types.BuiltinVar;
pub const VariableBinding = types.VariableBinding;
pub const VariableRef = types.VariableRef;
pub const ResolvedValue = types.ResolvedValue;
pub const ResolvedList = types.ResolvedList;
pub const VarResolver = types.VarResolver;
pub const resolveBuiltin = types.resolveBuiltin;
pub const promoteArith = types.promoteArith;
pub const divValues = types.divValues;
pub const cmpValues = types.cmpValues;
pub const promoteMinMax = types.promoteMinMax;

pub const Ast = ast_mod.Ast;
pub const Op = bytecode.Op;
pub const Expression = bytecode.Expression;

pub const parseAst = parse_ast_mod.parseAst;

// ── Tests ───────────────────────────────────────────────────────────────

fn lowerTestExpr(allocator: std.mem.Allocator, source: []const u8) !Expression {
    var temp_arena = std.heap.ArenaAllocator.init(allocator);
    defer temp_arena.deinit();

    var common_diagnostics = diagnostic_mod.Diagnostics.init(temp_arena.allocator(), "<expr-test>");
    defer common_diagnostics.deinit();
    var diagnostics = Diagnostics.init(&common_diagnostics, .{}, .expression, source);

    var ast = try parseAst(temp_arena.allocator(), source, &diagnostics);
    return try ast.lower(allocator, &diagnostics);
}

fn lowerBoundTestExpr(allocator: std.mem.Allocator, source: []const u8, slots: anytype) !Expression {
    var temp_arena = std.heap.ArenaAllocator.init(allocator);
    defer temp_arena.deinit();

    var common_diagnostics = diagnostic_mod.Diagnostics.init(temp_arena.allocator(), "<expr-test>");
    defer common_diagnostics.deinit();
    var diagnostics = Diagnostics.init(&common_diagnostics, .{}, .expression, source);

    var ast = try parseAst(temp_arena.allocator(), source, &diagnostics);
    try ast.bindVariables(slots, &diagnostics);
    return try ast.lower(allocator, &diagnostics);
}

/// Test helper: parse + eval in one shot (no variable binding).
fn testEval(allocator: std.mem.Allocator, source: []const u8, resolver: VarResolver) !Value {
    var expr_obj = try lowerTestExpr(allocator, source);
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
        self.slot_names[self.count] = std.testing.allocator.dupe(u8, name) catch unreachable;
        self.count += 1;
        return idx;
    }

    fn deinit(self: *TestContext) void {
        for (self.slot_names[0..self.count]) |name| {
            std.testing.allocator.free(name);
        }
        self.* = undefined;
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
    var tc = TestContext{ .vars = vars };
    defer tc.deinit();
    var slots: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer slots.deinit(std.testing.allocator);
    var temp_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer temp_arena.deinit();
    var common_diagnostics = diagnostic_mod.Diagnostics.init(temp_arena.allocator(), "<expr-test>");
    defer common_diagnostics.deinit();
    var diagnostics = Diagnostics.init(&common_diagnostics, .{}, .expression, source);
    const ast = try parseAst(temp_arena.allocator(), source, &diagnostics);
    try collectAstNames(ast.root, &tc, &slots);
    var expr_obj = try lowerBoundTestExpr(std.testing.allocator, source, &slots);
    defer expr_obj.deinit(std.testing.allocator);
    var result = try expr_obj.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    return result.value;
}

fn collectAstNames(
    node: *const Ast.Node,
    tc: *TestContext,
    slots: *std.StringArrayHashMapUnmanaged(void),
) !void {
    switch (node.data) {
        .load_var => |ref| switch (ref) {
            .name => |name| try addAstName(tc, slots, name),
            .binding => {},
        },
        .load_list_len => |ref| switch (ref) {
            .name => |name| try addAstName(tc, slots, name),
            .binding => {},
        },
        .load_list_elem => |data| {
            switch (data.ref) {
                .name => |name| try addAstName(tc, slots, name),
                .binding => {},
            }
            try collectAstNames(data.index, tc, slots);
        },
        .call_join => |data| {
            switch (data.ref) {
                .name => |name| try addAstName(tc, slots, name),
                .binding => {},
            }
            try collectAstNames(data.delim, tc, slots);
        },
        .unary => |data| try collectAstNames(data.child, tc, slots),
        .binary => |data| {
            try collectAstNames(data.lhs, tc, slots);
            try collectAstNames(data.rhs, tc, slots);
        },
        .logical_and => |data| {
            try collectAstNames(data.lhs, tc, slots);
            try collectAstNames(data.rhs, tc, slots);
        },
        .logical_or => |data| {
            try collectAstNames(data.lhs, tc, slots);
            try collectAstNames(data.rhs, tc, slots);
        },
        else => {},
    }
}

fn addAstName(
    tc: *TestContext,
    slots: *std.StringArrayHashMapUnmanaged(void),
    name: []const u8,
) !void {
    if (slots.getIndex(name) != null) return;
    _ = tc.addVar(name);
    try slots.put(std.testing.allocator, name, {});
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
    try expectFloat(2.5, try testEval(std.testing.allocator, "5 / 2", r));
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

    var empty_slots: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer empty_slots.deinit(std.testing.allocator);

    var e1 = try lowerBoundTestExpr(std.testing.allocator, "$ITER", &empty_slots);
    defer e1.deinit(std.testing.allocator);
    try expectInt(42, (try e1.eval(resolver_v, std.testing.allocator)).value);

    var e2 = try lowerBoundTestExpr(std.testing.allocator, "$ITER + 1", &empty_slots);
    defer e2.deinit(std.testing.allocator);
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
    try std.testing.expectError(error.AnalysisFail, testEval(std.testing.allocator, "(1 + 2", VarResolver.none()));
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

    var tc = TestContext{ .vars = &vars };
    defer tc.deinit();
    var slots: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer slots.deinit(std.testing.allocator);
    _ = tc.addVar("x");
    try slots.put(std.testing.allocator, "x", {});

    var expr_obj = try lowerBoundTestExpr(std.testing.allocator, "${x} * 2 + 1", &slots);
    defer expr_obj.deinit(std.testing.allocator);

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

const ListTestContext = struct {
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

    fn slots(self: *const ListTestContext) std.StringArrayHashMapUnmanaged(void) {
        var map: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (self.slot_names[0..self.count]) |name| {
            map.put(std.testing.allocator, name, {}) catch unreachable;
        }
        return map;
    }
};

test "expr len() on list variable" {
    var tc: ListTestContext = .{};
    tc.addList("voltages", &.{ 1.0, 2.0, 3.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "len(${voltages})", &slot_map);
    defer e.deinit(std.testing.allocator);
    try expectInt(3, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list indexing" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });
    tc.addScalar("idx", "1");

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "${arr}[${idx}]", &slot_map);
    defer e.deinit(std.testing.allocator);
    try expectFloat(20.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list index with literal" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "${arr}[2]", &slot_map);
    defer e.deinit(std.testing.allocator);
    try expectFloat(30.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr list index out of bounds" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "${arr}[5]", &slot_map);
    defer e.deinit(std.testing.allocator);
    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}

test "expr list in arithmetic" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{ 10.0, 20.0, 30.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "${arr}[0] + ${arr}[2]", &slot_map);
    defer e.deinit(std.testing.allocator);
    try expectFloat(40.0, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr bare list variable is error" {
    var tc: ListTestContext = .{};
    tc.addList("arr", &.{1.0});

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "${arr} + 1", &slot_map);
    defer e.deinit(std.testing.allocator);
    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}

test "expr len() in arithmetic" {
    var tc: ListTestContext = .{};
    tc.addList("items", &.{ 5.0, 10.0, 15.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "len(${items}) - 1", &slot_map);
    defer e.deinit(std.testing.allocator);
    try expectInt(3, (try e.eval(tc.resolver(), std.testing.allocator)).value);
}

test "expr stack overflow records diagnostic" {
    var buf: [1024]u8 = undefined;
    var pos: usize = 0;
    for (0..Expression.max_stack + 1) |_| {
        @memcpy(buf[pos..][0..3], "1+(");
        pos += 3;
    }
    buf[pos] = '1';
    pos += 1;
    for (0..Expression.max_stack + 1) |_| {
        buf[pos] = ')';
        pos += 1;
    }
    const src = buf[0..pos];
    try std.testing.expectError(error.AnalysisFail, lowerTestExpr(std.testing.allocator, src));
}

test "expr string literal" {
    const r = VarResolver.none();

    var hello = try lowerTestExpr(std.testing.allocator, "\"hello\"");
    defer hello.deinit(std.testing.allocator);
    var hello_result = try hello.eval(r, std.testing.allocator);
    defer hello_result.deinit();
    try expectString("hello", hello_result.value);

    var empty = try lowerTestExpr(std.testing.allocator, "\"\"");
    defer empty.deinit(std.testing.allocator);
    var empty_result = try empty.eval(r, std.testing.allocator);
    defer empty_result.deinit();
    try expectString("", empty_result.value);
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

    try expectBool(true, try testEval(std.testing.allocator, "\"hello\" && 1", r));
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
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "join(${channels}, \",\")", &slot_map);
    defer e.deinit(std.testing.allocator);
    var result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("1,2,3", result.value);
}

test "expr join() with custom delimiter" {
    var tc: ListTestContext = .{};
    tc.addList("items", &.{ 10.0, 20.0 });

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "join(${items}, \" | \")", &slot_map);
    defer e.deinit(std.testing.allocator);
    var result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("10 | 20", result.value);
}

test "expr join() on empty list" {
    var tc: ListTestContext = .{};
    tc.addList("empty", &.{});

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "join(${empty}, \",\")", &slot_map);
    defer e.deinit(std.testing.allocator);
    var result = try e.eval(tc.resolver(), std.testing.allocator);
    defer result.deinit();
    try expectString("", result.value);
}

test "expr join() on non-list is error" {
    var tc: ListTestContext = .{};
    tc.addScalar("x", "42");

    var slot_map = tc.slots();
    defer slot_map.deinit(std.testing.allocator);

    var e = try lowerBoundTestExpr(std.testing.allocator, "join(${x}, \",\")", &slot_map);
    defer e.deinit(std.testing.allocator);
    try std.testing.expectError(error.InvalidExpression, e.eval(tc.resolver(), std.testing.allocator));
}
