const std = @import("std");
const types = @import("types.zig");
const ast_mod = @import("ast.zig");

const CompileError = types.CompileError;
const Span = types.Span;
const VariableRef = types.VariableRef;
const Ast = ast_mod.Ast;

const AstParser = struct {
    allocator: std.mem.Allocator,
    diagnostics: *types.Diagnostics,
    source: []const u8,
    pos: usize,

    fn newNode(self: *AstParser, data: Ast.Node.Data, span: Span) CompileError!*Ast.Node {
        const node = try self.allocator.create(Ast.Node);
        node.* = .{ .span = span, .data = data };
        return node;
    }

    fn fail(self: *AstParser, span: Span, kind: types.Message) CompileError {
        return self.diagnostics.fail(span, kind);
    }

    fn failAt(self: *AstParser, pos: usize, kind: types.Message) CompileError {
        return self.fail(.at(pos), kind);
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

    fn expectChar(self: *AstParser, expected: u8, expected_text: []const u8) CompileError!void {
        if (!self.matchChar(expected)) return self.failAt(self.pos, .{ .expected_token = expected_text });
    }

    fn matchStr(self: *AstParser, prefix: []const u8) bool {
        self.skipWhitespace();
        if (std.mem.startsWith(u8, self.source[self.pos..], prefix)) {
            self.pos += prefix.len;
            return true;
        }
        return false;
    }

    fn parseOr(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseAnd();
        while (true) {
            if (!self.matchStr("||")) break;
            const rhs = try self.parseAnd();
            lhs = try self.newNode(.{ .logical_or = .{ .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
        }
        return lhs;
    }

    fn parseAnd(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseEquality();
        while (true) {
            if (!self.matchStr("&&")) break;
            const rhs = try self.parseEquality();
            lhs = try self.newNode(.{ .logical_and = .{ .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
        }
        return lhs;
    }

    fn parseEquality(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseComparison();
        while (true) {
            if (self.matchStr("==")) {
                const rhs = try self.parseComparison();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_eq, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.matchStr("!=")) {
                const rhs = try self.parseComparison();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_ne, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else break;
        }
        return lhs;
    }

    fn parseComparison(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseAddSub();
        while (true) {
            if (self.matchStr(">=")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_ge, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.matchStr("<=")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_le, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.matchStr(">")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_gt, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.matchStr("<")) {
                const rhs = try self.parseAddSub();
                lhs = try self.newNode(.{ .binary = .{ .op = .cmp_lt, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else break;
        }
        return lhs;
    }

    fn parseAddSub(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseMulDiv();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('+')) {
                const rhs = try self.parseMulDiv();
                lhs = try self.newNode(.{ .binary = .{ .op = .add, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.pos < self.source.len and self.source[self.pos] == '-') {
                self.pos += 1;
                const rhs = try self.parseMulDiv();
                lhs = try self.newNode(.{ .binary = .{ .op = .sub, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else break;
        }
        return lhs;
    }

    fn parseMulDiv(self: *AstParser) CompileError!*Ast.Node {
        var lhs = try self.parseUnary();
        while (true) {
            self.skipWhitespace();
            if (self.matchChar('*')) {
                const rhs = try self.parseUnary();
                lhs = try self.newNode(.{ .binary = .{ .op = .mul, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else if (self.matchChar('/')) {
                const rhs = try self.parseUnary();
                lhs = try self.newNode(.{ .binary = .{ .op = .div, .lhs = lhs, .rhs = rhs } }, .cover(lhs.span, rhs.span));
            } else break;
        }
        return lhs;
    }

    fn parseUnary(self: *AstParser) CompileError!*Ast.Node {
        self.skipWhitespace();
        const start = self.pos;
        if (self.matchChar('!')) {
            const child = try self.parseUnary();
            return try self.newNode(.{ .unary = .{ .op = .not, .child = child } }, .{ .start = start, .end = child.span.end });
        }
        if (self.pos < self.source.len and self.source[self.pos] == '-') {
            if (self.pos + 1 < self.source.len and !std.ascii.isDigit(self.source[self.pos + 1]) and self.source[self.pos + 1] != '.') {
                self.pos += 1;
                const child = try self.parseUnary();
                return try self.newNode(.{ .unary = .{ .op = .negate, .child = child } }, .{ .start = start, .end = child.span.end });
            }
        }
        return self.parseAtom();
    }

    fn parseAtom(self: *AstParser) CompileError!*Ast.Node {
        self.skipWhitespace();
        if (self.pos >= self.source.len) return self.failAt(self.pos, .expected_expression);

        if (self.source[self.pos] == '(') {
            self.pos += 1;
            const node = try self.parseOr();
            try self.expectChar(')', ")");
            return node;
        }

        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            const start = self.pos;
            const ref = try self.parseBracedRef();
            if (self.pos < self.source.len and self.source[self.pos] == '[') {
                self.pos += 1;
                const index = try self.parseOr();
                try self.expectChar(']', "]");
                return try self.newNode(.{ .load_list_elem = .{ .ref = ref, .index = index } }, .{ .start = start, .end = self.pos });
            }
            return try self.newNode(.{ .load_var = ref }, .{ .start = start, .end = self.pos });
        }

        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            const start = self.pos;
            return try self.newNode(.{ .load_var = try self.parseBuiltinRef() }, .{ .start = start, .end = self.pos });
        }

        if (std.ascii.isAlphabetic(self.source[self.pos])) {
            const name_start = self.pos;
            while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
            const name = self.source[name_start..self.pos];
            self.skipWhitespace();
            if (self.matchChar('(')) {
                if (std.mem.eql(u8, name, "len")) {
                    const ref = try self.parseVarArg();
                    try self.expectChar(')', ")");
                    return try self.newNode(.{ .load_list_len = ref }, .{ .start = name_start, .end = self.pos });
                } else if (std.mem.eql(u8, name, "min") or std.mem.eql(u8, name, "max")) {
                    const lhs = try self.parseOr();
                    try self.expectChar(',', ",");
                    const rhs = try self.parseOr();
                    try self.expectChar(')', ")");
                    return try self.newNode(.{ .binary = .{
                        .op = if (std.mem.eql(u8, name, "min")) .call_min else .call_max,
                        .lhs = lhs,
                        .rhs = rhs,
                    } }, .{ .start = name_start, .end = self.pos });
                } else if (std.mem.eql(u8, name, "join")) {
                    const ref = try self.parseVarArg();
                    try self.expectChar(',', ",");
                    const delim = try self.parseOr();
                    try self.expectChar(')', ")");
                    return try self.newNode(.{ .call_join = .{ .ref = ref, .delim = delim } }, .{ .start = name_start, .end = self.pos });
                } else {
                    return self.fail(.{ .start = name_start, .end = self.pos }, .{ .unknown_function = name });
                }
            }
            self.pos = name_start;
        }

        if (self.pos < self.source.len and self.source[self.pos] == '"') {
            const start = self.pos;
            self.pos += 1;
            const text_start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != '"') : (self.pos += 1) {}
            if (self.pos >= self.source.len) return self.fail(.{ .start = start, .end = self.pos }, .unterminated_string);
            const text = self.source[text_start..self.pos];
            self.pos += 1;
            return try self.newNode(.{ .string = text }, .{ .start = start, .end = self.pos });
        }

        if (std.ascii.isDigit(self.source[self.pos]) or self.source[self.pos] == '-' or self.source[self.pos] == '.') {
            return self.parseNumber();
        }

        return self.failAt(self.pos, .unexpected_token);
    }

    fn parseVarArg(self: *AstParser) CompileError!VariableRef {
        self.skipWhitespace();
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and self.source[self.pos + 1] == '{') {
            return self.parseBracedRef();
        }
        if (self.pos + 1 < self.source.len and self.source[self.pos] == '$' and std.ascii.isAlphabetic(self.source[self.pos + 1])) {
            return self.parseBuiltinRef();
        }
        return self.failAt(self.pos, .expected_variable);
    }

    fn parseBracedRef(self: *AstParser) CompileError!VariableRef {
        self.pos += 2;
        const start = self.pos;
        while (self.pos < self.source.len and self.source[self.pos] != '}') : (self.pos += 1) {}
        if (self.pos >= self.source.len) return self.failAt(self.pos, .{ .expected_token = "}" });
        const text = self.source[start..self.pos];
        self.pos += 1;
        return .{ .name = text };
    }

    fn parseBuiltinRef(self: *AstParser) CompileError!VariableRef {
        const start = self.pos;
        self.pos += 1;
        while (self.pos < self.source.len and (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_')) : (self.pos += 1) {}
        return .{ .name = self.source[start..self.pos] };
    }

    fn parseNumber(self: *AstParser) CompileError!*Ast.Node {
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
        if (self.pos == start) return self.failAt(start, .expected_expression);
        const span: Span = .{ .start = start, .end = self.pos };
        const text = self.source[start..self.pos];
        if (!is_float) {
            return try self.newNode(.{ .int = std.fmt.parseInt(i64, text, 10) catch return self.fail(span, .{ .invalid_number = text }) }, span);
        }
        return try self.newNode(.{ .float = std.fmt.parseFloat(f64, text) catch return self.fail(span, .{ .invalid_number = text }) }, span);
    }
};

pub fn parseAst(allocator: std.mem.Allocator, source: []const u8, diagnostics: *types.Diagnostics) CompileError!Ast {
    var parser = AstParser{
        .allocator = allocator,
        .diagnostics = diagnostics,
        .source = source,
        .pos = 0,
    };
    const root = try parser.parseOr();
    errdefer ast_mod.destroyNode(allocator, root);
    parser.skipWhitespace();
    if (parser.pos < parser.source.len) return parser.failAt(parser.pos, .unexpected_token);
    return .{ .root = root };
}
