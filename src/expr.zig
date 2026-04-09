/// Lightweight expression evaluator for compute steps and `when` guards.
///
/// Supports:
///   - Arithmetic: `+`, `-`, `*`, `/`
///   - Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
///   - Logical:    `&&`, `||`, `!`
///   - Parentheses for grouping
///   - Number literals (integer and float)
///   - Variable references: `${name}`
///
/// All arithmetic is performed in f64.  Comparisons return 1.0 (true) or 0.0 (false).
/// A value is "truthy" when it is not exactly 0.0.
const std = @import("std");

pub const Value = f64;

pub const EvalError = error{
    InvalidExpression,
    UnexpectedToken,
    UnmatchedParen,
    DivisionByZero,
    VariableNotFound,
    InvalidNumber,
    OutOfMemory,
};

/// Pre-parsed expression tree that can be evaluated many times with different variable bindings.
pub const Expression = struct {
    allocator: std.mem.Allocator,
    root: *const Node,
    /// All nodes are arena-owned; this slice lets us free them in bulk.
    nodes: []*Node,

    pub fn deinit(self: *Expression) void {
        for (self.nodes) |node| self.allocator.destroy(node);
        self.allocator.free(self.nodes);
    }

    pub fn eval(self: *const Expression, resolver: VarResolver) EvalError!Value {
        return evalNode(self.root, resolver);
    }

    /// Evaluates and returns true when the result is non-zero.
    pub fn isTruthy(self: *const Expression, resolver: VarResolver) EvalError!bool {
        return (try self.eval(resolver)) != 0.0;
    }
};

/// Opaque variable resolver: calls the provided function to map names to string values.
pub const VarResolver = struct {
    ctx: *const anyopaque,
    resolveFn: *const fn (ctx: *const anyopaque, name: []const u8) ?[]const u8,

    pub fn resolve(self: VarResolver, name: []const u8) ?[]const u8 {
        return self.resolveFn(self.ctx, name);
    }

    /// Convenience constructor from a `*const StringHashMap([]const u8)`.
    pub fn fromStringHashMap(map: *const std.StringHashMap([]const u8)) VarResolver {
        return .{
            .ctx = @ptrCast(map),
            .resolveFn = struct {
                fn resolve(ctx_ptr: *const anyopaque, name: []const u8) ?[]const u8 {
                    const m: *const std.StringHashMap([]const u8) = @ptrCast(@alignCast(ctx_ptr));
                    return m.get(name);
                }
            }.resolve,
        };
    }
};

/// Parse an expression string into a reusable Expression tree.
pub fn parse(allocator: std.mem.Allocator, source: []const u8) EvalError!Expression {
    var parser = Parser{
        .allocator = allocator,
        .source = source,
        .pos = 0,
        .nodes = .empty,
    };
    errdefer {
        for (parser.nodes.items) |node| allocator.destroy(node);
        parser.nodes.deinit(allocator);
    }

    const root = try parser.parseOr();
    if (parser.pos < parser.source.len) {
        // Trailing characters after a valid expression.
        parser.skipWhitespace();
        if (parser.pos < parser.source.len) return error.UnexpectedToken;
    }

    return .{
        .allocator = allocator,
        .root = root,
        .nodes = try parser.nodes.toOwnedSlice(allocator),
    };
}

/// Evaluate an expression string directly against a variable map.
pub fn eval(allocator: std.mem.Allocator, source: []const u8, resolver: VarResolver) EvalError!Value {
    var expr_obj = try parse(allocator, source);
    defer expr_obj.deinit();
    return expr_obj.eval(resolver);
}

// ── AST ─────────────────────────────────────────────────────────────────

const Node = union(enum) {
    number: f64,
    variable: []const u8,
    unary_not: *const Node,
    unary_neg: *const Node,
    binary: BinaryNode,
};

const BinaryNode = struct {
    op: BinaryOp,
    lhs: *const Node,
    rhs: *const Node,
};

const BinaryOp = enum {
    add,
    sub,
    mul,
    div,
    gt,
    lt,
    ge,
    le,
    eq,
    ne,
    @"and",
    @"or",
};

fn evalNode(node: *const Node, resolver: VarResolver) EvalError!Value {
    return switch (node.*) {
        .number => |n| n,
        .variable => |name| {
            const text = resolver.resolve(name) orelse return error.VariableNotFound;
            return std.fmt.parseFloat(f64, std.mem.trim(u8, text, &std.ascii.whitespace)) catch return error.InvalidNumber;
        },
        .unary_not => |inner| {
            const val = try evalNode(inner, resolver);
            return if (val == 0.0) @as(f64, 1.0) else @as(f64, 0.0);
        },
        .unary_neg => |inner| {
            return -(try evalNode(inner, resolver));
        },
        .binary => |bin| {
            const lhs = try evalNode(bin.lhs, resolver);
            // Short-circuit for logical operators.
            switch (bin.op) {
                .@"and" => return if (lhs == 0.0) @as(f64, 0.0) else if ((try evalNode(bin.rhs, resolver)) != 0.0) @as(f64, 1.0) else @as(f64, 0.0),
                .@"or" => return if (lhs != 0.0) @as(f64, 1.0) else if ((try evalNode(bin.rhs, resolver)) != 0.0) @as(f64, 1.0) else @as(f64, 0.0),
                else => {},
            }
            const rhs = try evalNode(bin.rhs, resolver);
            return switch (bin.op) {
                .add => lhs + rhs,
                .sub => lhs - rhs,
                .mul => lhs * rhs,
                .div => if (rhs == 0.0) return error.DivisionByZero else lhs / rhs,
                .gt => boolToValue(lhs > rhs),
                .lt => boolToValue(lhs < rhs),
                .ge => boolToValue(lhs >= rhs),
                .le => boolToValue(lhs <= rhs),
                .eq => boolToValue(lhs == rhs),
                .ne => boolToValue(lhs != rhs),
                .@"and", .@"or" => unreachable,
            };
        },
    };
}

fn boolToValue(b: bool) f64 {
    return if (b) 1.0 else 0.0;
}

// ── Recursive-descent parser ────────────────────────────────────────────
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
    nodes: std.ArrayList(*Node),

    fn createNode(self: *Parser, value: Node) EvalError!*Node {
        const node = self.allocator.create(Node) catch return error.OutOfMemory;
        node.* = value;
        self.nodes.append(self.allocator, node) catch {
            self.allocator.destroy(node);
            return error.OutOfMemory;
        };
        return node;
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

    // ── Precedence levels ───────────────────────────────────────────

    fn parseOr(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseAnd();
        while (true) {
            self.skipWhitespace();
            if (self.matchTwo('|', '|')) {
                const rhs = try self.parseAnd();
                lhs = try self.createNode(.{ .binary = .{ .op = .@"or", .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseAnd(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseEquality();
        while (true) {
            self.skipWhitespace();
            if (self.matchTwo('&', '&')) {
                const rhs = try self.parseEquality();
                lhs = try self.createNode(.{ .binary = .{ .op = .@"and", .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseEquality(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseComparison();
        while (true) {
            self.skipWhitespace();
            if (self.matchTwo('=', '=')) {
                const rhs = try self.parseComparison();
                lhs = try self.createNode(.{ .binary = .{ .op = .eq, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchTwo('!', '=')) {
                const rhs = try self.parseComparison();
                lhs = try self.createNode(.{ .binary = .{ .op = .ne, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseComparison(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseAddSub();
        while (true) {
            self.skipWhitespace();
            if (self.pos + 1 < self.source.len and self.source[self.pos] == '>' and self.source[self.pos + 1] == '=') {
                self.pos += 2;
                const rhs = try self.parseAddSub();
                lhs = try self.createNode(.{ .binary = .{ .op = .ge, .lhs = lhs, .rhs = rhs } });
            } else if (self.pos + 1 < self.source.len and self.source[self.pos] == '<' and self.source[self.pos + 1] == '=') {
                self.pos += 2;
                const rhs = try self.parseAddSub();
                lhs = try self.createNode(.{ .binary = .{ .op = .le, .lhs = lhs, .rhs = rhs } });
            } else if (self.pos < self.source.len and self.source[self.pos] == '>') {
                self.pos += 1;
                const rhs = try self.parseAddSub();
                lhs = try self.createNode(.{ .binary = .{ .op = .gt, .lhs = lhs, .rhs = rhs } });
            } else if (self.pos < self.source.len and self.source[self.pos] == '<') {
                self.pos += 1;
                const rhs = try self.parseAddSub();
                lhs = try self.createNode(.{ .binary = .{ .op = .lt, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseAddSub(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseMulDiv();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('+')) {
                const rhs = try self.parseMulDiv();
                lhs = try self.createNode(.{ .binary = .{ .op = .add, .lhs = lhs, .rhs = rhs } });
            } else if (self.pos < self.source.len and self.source[self.pos] == '-') {
                // Distinguish binary minus from unary minus by consuming here.
                self.pos += 1;
                const rhs = try self.parseMulDiv();
                lhs = try self.createNode(.{ .binary = .{ .op = .sub, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseMulDiv(self: *Parser) EvalError!*const Node {
        var lhs = try self.parseUnary();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('*')) {
                const rhs = try self.parseUnary();
                lhs = try self.createNode(.{ .binary = .{ .op = .mul, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchChar('/')) {
                const rhs = try self.parseUnary();
                lhs = try self.createNode(.{ .binary = .{ .op = .div, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseUnary(self: *Parser) EvalError!*const Node {
        self.skipWhitespace();
        if (self.matchChar('!')) {
            const inner = try self.parseUnary();
            return self.createNode(.{ .unary_not = inner });
        }
        if (self.pos < self.source.len and self.source[self.pos] == '-') {
            // Only treat as unary if the next char is not a digit (avoid conflicting with negative number literals handled by atom).
            if (self.pos + 1 < self.source.len and !std.ascii.isDigit(self.source[self.pos + 1]) and self.source[self.pos + 1] != '.') {
                self.pos += 1;
                const inner = try self.parseUnary();
                return self.createNode(.{ .unary_neg = inner });
            }
        }
        return self.parseAtom();
    }

    fn parseAtom(self: *Parser) EvalError!*const Node {
        self.skipWhitespace();
        if (self.pos >= self.source.len) return error.InvalidExpression;

        // Parenthesized sub-expression.
        if (self.source[self.pos] == '(') {
            self.pos += 1;
            const inner = try self.parseOr();
            if (!self.matchChar(')')) return error.UnmatchedParen;
            return inner;
        }

        // Variable reference: ${name}
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            self.pos += 2;
            const name_start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '}') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return error.InvalidExpression;
            const name = self.source[name_start..self.pos];
            self.pos += 1; // skip '}'
            return self.createNode(.{ .variable = name });
        }

        // Number literal (including negative).
        if (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '-' or self.source[self.pos] == '.') {
            return self.parseNumber();
        }

        return error.UnexpectedToken;
    }

    fn parseNumber(self: *Parser) EvalError!*const Node {
        const start = self.pos;
        if (self.pos < self.source.len and self.source[self.pos] == '-') self.pos += 1;
        while (self.pos < self.source.len and (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '.')) {
            self.pos += 1;
        }
        // Accept scientific notation (e.g., 1e3, 2.5E-4).
        if (self.pos < self.source.len and (self.source[self.pos] == 'e' or self.source[self.pos] == 'E')) {
            self.pos += 1;
            if (self.pos < self.source.len and (self.source[self.pos] == '+' or self.source[self.pos] == '-')) self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) : (self.pos += 1) {}
        }
        if (self.pos == start) return error.InvalidNumber;
        const text = self.source[start..self.pos];
        const value = std.fmt.parseFloat(f64, text) catch return error.InvalidNumber;
        return self.createNode(.{ .number = value });
    }

    fn matchTwo(self: *Parser, first: u8, second: u8) bool {
        if (self.pos + 1 < self.source.len and self.source[self.pos] == first and self.source[self.pos + 1] == second) {
            self.pos += 2;
            return true;
        }
        return false;
    }
};

// ── Tests ───────────────────────────────────────────────────────────────

test "expr arithmetic" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, 7.0), try eval(std.testing.allocator, "3 + 4", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), try eval(std.testing.allocator, "2 * 3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), try eval(std.testing.allocator, "5 / 2", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 14.0), try eval(std.testing.allocator, "2 + 3 * 4", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), try eval(std.testing.allocator, "(2 + 3) * 4", r), 1e-9);
}

test "expr comparison" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "5 > 3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), try eval(std.testing.allocator, "2 > 3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "3 >= 3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "3 == 3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "3 != 4", r), 1e-9);
}

test "expr logical" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "1 && 1", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), try eval(std.testing.allocator, "1 && 0", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "0 || 1", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "!0", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), try eval(std.testing.allocator, "!1", r), 1e-9);
}

test "expr variables" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("voltage", "4.5");
    try vars.put("current", "2.0");
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, 9.0), try eval(std.testing.allocator, "${voltage} * ${current}", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "${voltage} > 3", r), 1e-9);
}

test "expr complex power check" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("voltage", "12.0");
    try vars.put("current", "9.0");
    const r = VarResolver.fromStringHashMap(&vars);

    // power = 108, check > 100
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), try eval(std.testing.allocator, "${voltage} * ${current} > 100", r), 1e-9);
}

test "expr division by zero" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectError(error.DivisionByZero, eval(std.testing.allocator, "1 / 0", r));
}

test "expr missing variable" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectError(error.VariableNotFound, eval(std.testing.allocator, "${missing}", r));
}

test "expr unmatched paren" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectError(error.UnmatchedParen, eval(std.testing.allocator, "(1 + 2", r));
}

test "expr negative literal" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, -3.0), try eval(std.testing.allocator, "-3", r), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, -1.0), try eval(std.testing.allocator, "2 + -3", r), 1e-9);
}

test "expr unary negation of variable" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();
    try vars.put("x", "5");
    const r = VarResolver.fromStringHashMap(&vars);

    try std.testing.expectApproxEqAbs(@as(f64, -5.0), try eval(std.testing.allocator, "-${x}", r), 1e-9);
}

test "expr parse reuse" {
    var vars = std.StringHashMap([]const u8).init(std.testing.allocator);
    defer vars.deinit();

    var expr_obj = try parse(std.testing.allocator, "${x} * 2 + 1");
    defer expr_obj.deinit();

    try vars.put("x", "3");
    try std.testing.expectApproxEqAbs(@as(f64, 7.0), try expr_obj.eval(VarResolver.fromStringHashMap(&vars)), 1e-9);

    try vars.put("x", "10");
    try std.testing.expectApproxEqAbs(@as(f64, 21.0), try expr_obj.eval(VarResolver.fromStringHashMap(&vars)), 1e-9);
}
