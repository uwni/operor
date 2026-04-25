const std = @import("std");
const types = @import("types.zig");
const ast_mod = @import("ast.zig");

const EvalError = types.EvalError;
const VariableRef = types.VariableRef;
const Ast = ast_mod.Ast;

const AstParser = struct {
    allocator: std.mem.Allocator,
    source: []const u8,
    pos: usize,

    fn newNode(self: *AstParser, value: Ast.Node) !*Ast.Node {
        const node = try self.allocator.create(Ast.Node);
        node.* = value;
        return node;
    }

    fn skipWhitespace(self: *AstParser) void {
        while (self.pos < self.source.len and (self.source[self.pos] == ' ' or self.source[self.pos] == '\t')) {
            self.pos += 1;
        }
    }

    fn matchChar(self: *AstParser, expected: u8) bool {
        self.skipWhitespace();
        if (self.pos < self.source.len and self.source[self.pos] == expected) {
            self.pos += 1;
            return true;
        }
        return false;
    }

    fn matchStr(self: *AstParser, prefix: []const u8) bool {
        self.skipWhitespace();
        if (std.mem.startsWith(u8, self.source[self.pos..], prefix)) {
            self.pos += prefix.len;
            return true;
        }
        return false;
    }

    fn parseOr(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseAnd();
        while (true) {
            if (!self.matchStr("||")) break;
            const rhs = try self.parseAnd();
            lhs = try self.newNode(.{ .logical_or = .{ .lhs = lhs, .rhs = rhs } });
        }
        return lhs;
    }

    fn parseAnd(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseEquality();
        while (true) {
            if (!self.matchStr("&&")) break;
            const rhs = try self.parseEquality();
            lhs = try self.newNode(.{ .logical_and = .{ .lhs = lhs, .rhs = rhs } });
        }
        return lhs;
    }

    fn parseEquality(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseComparison();
        while (true) {
            if (self.matchStr("==")) {
                const rhs = try self.parseComparison();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_eq, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchStr("!=")) {
                const rhs = try self.parseComparison();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_ne, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseComparison(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseAddSub();
        while (true) {
            if (self.matchStr(">=")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_ge, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchStr("<=")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_le, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchStr(">")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_gt, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchStr("<")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_lt, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseAddSub(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseMulDiv();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('+')) {
                const rhs = try self.parseMulDiv();
                lhs = try self.newNode(.{ .binary = .{ .op = .add, .lhs = lhs, .rhs = rhs } });
            } else if (self.pos < self.source.len and self.source[self.pos] == '-') {
                self.pos += 1;
                const rhs = try self.parseMulDiv();
                lhs = try self.newNode(.{ .binary = .{ .op = .sub, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseMulDiv(self: *AstParser) EvalError!*Ast.Node {
        var lhs = try self.parseUnary();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('*')) {
                const rhs = try self.parseUnary();
                lhs = try self.newNode(.{ .binary = .{ .op = .mul, .lhs = lhs, .rhs = rhs } });
            } else if (self.matchChar('/')) {
                const rhs = try self.parseUnary();
                lhs = try self.newNode(.{ .binary = .{ .op = .div, .lhs = lhs, .rhs = rhs } });
            } else break;
        }
        return lhs;
    }

    fn parseUnary(self: *AstParser) EvalError!*Ast.Node {
        self.skipWhitespace();
        if (self.matchChar('!')) {
            const child = try self.parseUnary();
            return try self.newNode(.{ .unary = .{ .op = .not, .child = child } });
        }
        if (self.pos < self.source.len and self.source[self.pos] == '-') {
            if (self.pos + 1 < self.source.len and !std.ascii.isDigit(self.source[self.pos + 1]) and self.source[self.pos + 1] != '.') {
                self.pos += 1;
                const child = try self.parseUnary();
                return try self.newNode(.{ .unary = .{ .op = .negate, .child = child } });
            }
        }
        return self.parseAtom();
    }

    fn parseAtom(self: *AstParser) EvalError!*Ast.Node {
        self.skipWhitespace();
        if (self.pos >= self.source.len) return error.InvalidExpression;

        if (self.source[self.pos] == '(') {
            self.pos += 1;
            const node = try self.parseOr();
            if (!self.matchChar(')')) return error.UnmatchedParen;
            return node;
        }

        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            const ref = try self.parseBracedRef();
            if (self.pos < self.source.len and self.source[self.pos] == '[') {
                self.pos += 1;
                const index = try self.parseOr();
                if (!self.matchChar(']')) return error.UnmatchedParen;
                return try self.newNode(.{ .load_list_elem = .{ .ref = ref, .index = index } });
            }
            return try self.newNode(.{ .load_var = ref });
        }

        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            return try self.newNode(.{ .load_var = try self.parseBuiltinRef() });
        }

        if (std.ascii.isAlphabetic(self.source[self.pos])) {
            const name_start = self.pos;
            while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
            const name = self.source[name_start..self.pos];
            self.skipWhitespace();
            if (self.matchChar('(')) {
                if (std.mem.eql(u8, name, "len")) {
                    const ref = try self.parseVarArg();
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    return try self.newNode(.{ .load_list_len = ref });
                } else if (std.mem.eql(u8, name, "min") or std.mem.eql(u8, name, "max")) {
                    const lhs = try self.parseOr();
                    if (!self.matchChar(',')) return error.UnexpectedToken;
                    const rhs = try self.parseOr();
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    return try self.newNode(.{ .binary = .{
                        .op = if (std.mem.eql(u8, name, "min")) .call_min else .call_max,
                        .lhs = lhs,
                        .rhs = rhs,
                    } });
                } else if (std.mem.eql(u8, name, "join")) {
                    const ref = try self.parseVarArg();
                    if (!self.matchChar(',')) return error.UnexpectedToken;
                    const delim = try self.parseOr();
                    if (!self.matchChar(')')) return error.UnexpectedToken;
                    return try self.newNode(.{ .call_join = .{ .ref = ref, .delim = delim } });
                } else {
                    return error.InvalidExpression;
                }
            }
            self.pos = name_start;
        }

        if (self.pos < self.source.len and self.source[self.pos] == '"') {
            self.pos += 1;
            const start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '"') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return error.InvalidExpression;
            const text = self.source[start..self.pos];
            self.pos += 1;
            return try self.newNode(.{ .string = text });
        }

        if (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '-' or self.source[self.pos] == '.') {
            return self.parseNumber();
        }

        return error.UnexpectedToken;
    }

    fn parseVarArg(self: *AstParser) EvalError!VariableRef {
        self.skipWhitespace();
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            return self.parseBracedRef();
        }
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            return self.parseBuiltinRef();
        }
        return error.InvalidExpression;
    }

    fn parseBracedRef(self: *AstParser) EvalError!VariableRef {
        self.pos += 2;
        const start = self.pos;
        while (self.pos < self.source.len and self.source[self.pos] != '}') : (self.pos += 1) {}
        if (self.pos >= self.source.len) return error.InvalidExpression;
        const text = self.source[start..self.pos];
        self.pos += 1;
        return .{ .name = text };
    }

    fn parseBuiltinRef(self: *AstParser) EvalError!VariableRef {
        const start = self.pos;
        self.pos += 1;
        while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
        return .{ .name = self.source[start..self.pos] };
    }

    fn parseNumber(self: *AstParser) EvalError!*Ast.Node {
        const start = self.pos;
        if (self.pos < self.source.len and self.source[self.pos] == '-') self.pos += 1;
        var is_float = false;
        while (self.pos < self.source.len and (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '.')) {
            if (self.source[self.pos] == '.') is_float = true;
            self.pos += 1;
        }
        if (self.pos < self.source.len and (self.source[self.pos] == 'e' or self.source[self.pos] == 'E')) {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.source.len and (self.source[self.pos] == '+' or self.source[self.pos] == '-')) self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) : (self.pos += 1) {}
        }
        if (self.pos == start) return error.InvalidNumber;
        const text = self.source[start..self.pos];
        if (!is_float) {
            return try self.newNode(.{ .int = std.fmt.parseInt(i64, text, 10) catch return error.InvalidNumber });
        }
        return try self.newNode(.{ .float = std.fmt.parseFloat(f64, text) catch return error.InvalidNumber });
    }
};

pub fn parseAst(allocator: std.mem.Allocator, source: []const u8) EvalError!Ast {
    var parser = AstParser{
        .allocator = allocator,
        .source = source,
        .pos = 0,
    };
    const root = try parser.parseOr();
    errdefer ast_mod.destroyNode(allocator, root);
    if (parser.pos < parser.source.len) {
        parser.skipWhitespace();
        if (parser.pos < parser.source.len) return error.UnexpectedToken;
    }
    return .{ .root = root };
}
