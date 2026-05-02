const std = @import("std");
const diagnostic = @import("../diagnostic.zig");
const types = @import("types.zig");
const ast_mod = @import("ast.zig");

const CompileError = types.CompileError;
const Span = types.Span;
const VariableRef = types.VariableRef;
const Ast = ast_mod.Ast;

// ── Tokens ────────────────────────────────────────────────────────────────────

const Tag = enum {
    int,
    float,
    string,
    ident,
    dollar_brace, // ${name}  — text is the name without ${ }
    dollar_ident, // $NAME    — text includes the leading $
    plus,
    minus,
    star,
    slash,
    bang,
    amp_amp,
    pipe_pipe,
    eq_eq,
    bang_eq,
    lt,
    gt,
    lt_eq,
    gt_eq,
    lparen,
    rparen,
    lbracket,
    rbracket,
    comma,
    eof,
};

const Token = struct {
    tag: Tag,
    span: Span,
    int_val: i64 = 0,
    float_val: f64 = 0,
    text: []const u8 = "",
};

// ── Lexer ─────────────────────────────────────────────────────────────────

const Lexer = struct {
    source: []const u8,
    pos: usize,
    diagnostics: diagnostic.Reporter,

    fn skipWs(self: *Lexer) void {
        while (self.pos < self.source.len) {
            switch (self.source[self.pos]) {
                ' ', '\t' => self.pos += 1,
                else => break,
            }
        }
    }

    fn cur(self: *const Lexer) u8 {
        return if (self.pos < self.source.len) self.source[self.pos] else 0;
    }

    fn peek1(self: *const Lexer) u8 {
        return if (self.pos + 1 < self.source.len) self.source[self.pos + 1] else 0;
    }

    fn simple(self: *Lexer, tag: Tag) Token {
        const start = self.pos;
        self.pos += 1;
        return .{ .tag = tag, .span = .{ .start = start, .end = self.pos } };
    }

    fn two(self: *Lexer, tag: Tag) Token {
        const start = self.pos;
        self.pos += 2;
        return .{ .tag = tag, .span = .{ .start = start, .end = self.pos } };
    }

    fn scanNumber(self: *Lexer) CompileError!Token {
        const start = self.pos;
        var is_float = false;
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (std.ascii.isDigit(c)) {
                self.pos += 1;
            } else if (c == '.') {
                is_float = true;
                self.pos += 1;
            } else break;
        }
        if (self.pos < self.source.len and
            (self.source[self.pos] == 'e' or self.source[self.pos] == 'E'))
        {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.source.len and
                (self.source[self.pos] == '+' or self.source[self.pos] == '-'))
                self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos]))
                self.pos += 1;
        }
        const span: Span = .{ .start = start, .end = self.pos };
        const text = self.source[start..self.pos];
        if (!is_float) {
            const val = std.fmt.parseInt(i64, text, 10) catch
                return self.diagnostics.fail(span, .{ .invalid_number = .{ .number = text } });
            return .{ .tag = .int, .span = span, .int_val = val };
        }
        const val = std.fmt.parseFloat(f64, text) catch
            return self.diagnostics.fail(span, .{ .invalid_number = .{ .number = text } });
        return .{ .tag = .float, .span = span, .float_val = val };
    }

    fn scanString(self: *Lexer) CompileError!Token {
        const start = self.pos;
        self.pos += 1; // skip "
        const text_start = self.pos;
        while (self.pos < self.source.len and self.source[self.pos] != '"')
            self.pos += 1;
        if (self.pos >= self.source.len)
            return self.diagnostics.fail(
                .{ .start = start, .end = self.pos },
                .unterminated_string,
            );
        const text = self.source[text_start..self.pos];
        self.pos += 1; // skip "
        return .{ .tag = .string, .span = .{ .start = start, .end = self.pos }, .text = text };
    }

    fn scanDollarBrace(self: *Lexer) CompileError!Token {
        const start = self.pos;
        self.pos += 2; // skip ${
        const name_start = self.pos;
        while (self.pos < self.source.len and self.source[self.pos] != '}')
            self.pos += 1;
        if (self.pos >= self.source.len)
            return self.diagnostics.fail(
                .{ .start = start, .end = self.pos },
                .{ .expected_token = .{ .token = "}" } },
            );
        const text = self.source[name_start..self.pos];
        self.pos += 1; // skip }
        return .{ .tag = .dollar_brace, .span = .{ .start = start, .end = self.pos }, .text = text };
    }

    fn scanDollarIdent(self: *Lexer) Token {
        const start = self.pos;
        self.pos += 1; // skip $
        while (self.pos < self.source.len and
            (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_'))
            self.pos += 1;
        return .{
            .tag = .dollar_ident,
            .span = .{ .start = start, .end = self.pos },
            .text = self.source[start..self.pos],
        };
    }

    fn scanIdent(self: *Lexer) Token {
        const start = self.pos;
        while (self.pos < self.source.len and
            (std.ascii.isAlphanumeric(self.source[self.pos]) or self.source[self.pos] == '_'))
            self.pos += 1;
        return .{
            .tag = .ident,
            .span = .{ .start = start, .end = self.pos },
            .text = self.source[start..self.pos],
        };
    }

    fn next(self: *Lexer) CompileError!Token {
        self.skipWs();
        if (self.pos >= self.source.len)
            return .{ .tag = .eof, .span = .at(self.pos) };

        return switch (self.cur()) {
            '"' => self.scanString(),
            '0'...'9', '.' => self.scanNumber(),
            '$' => if (self.peek1() == '{')
                self.scanDollarBrace()
            else if (std.ascii.isAlphabetic(self.peek1()))
                self.scanDollarIdent()
            else
                self.diagnostics.fail(.at(self.pos), .unexpected_token),
            'a'...'z', 'A'...'Z', '_' => self.scanIdent(),
            '+' => self.simple(.plus),
            '-' => self.simple(.minus),
            '*' => self.simple(.star),
            '/' => self.simple(.slash),
            '(' => self.simple(.lparen),
            ')' => self.simple(.rparen),
            '[' => self.simple(.lbracket),
            ']' => self.simple(.rbracket),
            ',' => self.simple(.comma),
            '!' => if (self.peek1() == '=') self.two(.bang_eq) else self.simple(.bang),
            '=' => if (self.peek1() == '=')
                self.two(.eq_eq)
            else
                self.diagnostics.fail(.at(self.pos), .unexpected_token),
            '<' => if (self.peek1() == '=') self.two(.lt_eq) else self.simple(.lt),
            '>' => if (self.peek1() == '=') self.two(.gt_eq) else self.simple(.gt),
            '&' => if (self.peek1() == '&')
                self.two(.amp_amp)
            else
                self.diagnostics.fail(.at(self.pos), .unexpected_token),
            '|' => if (self.peek1() == '|')
                self.two(.pipe_pipe)
            else
                self.diagnostics.fail(.at(self.pos), .unexpected_token),
            else => self.diagnostics.fail(.at(self.pos), .unexpected_token),
        };
    }
};

// ── Binding powers (C-style precedence, low → high) ───────────────────────────
//
// Infix (left_bp, right_bp): left_bp must be exceeded to consume the operator;
// right_bp is the min_bp passed to the right-operand parse — right_bp = left_bp + 1
// gives left-associativity.
//
// Precedence table (matches C / most languages):
//   ||   →  2 / 3
//   &&   →  4 / 5
//   == != →  6 / 7
//   < > <= >= → 8 / 9
//   + -  → 10 / 11
//   * /  → 12 / 13
//   prefix ! - → right_bp 14

const InfixBp = packed struct { left: u8, right: u8 };

fn infixBp(tag: Tag) ?InfixBp {
    return switch (tag) {
        .pipe_pipe => .{ .left = 2, .right = 3 },
        .amp_amp => .{ .left = 4, .right = 5 },
        .eq_eq, .bang_eq => .{ .left = 6, .right = 7 },
        .lt, .gt, .lt_eq, .gt_eq => .{ .left = 8, .right = 9 },
        .plus, .minus => .{ .left = 10, .right = 11 },
        .star, .slash => .{ .left = 12, .right = 13 },
        else => null,
    };
}

const prefix_bp: u8 = 14;

// ── Pratt parser ──────────────────────────────────────────────────────────────

const AstParser = struct {
    allocator: std.mem.Allocator,
    diagnostics: diagnostic.Reporter,
    lexer: Lexer,
    current: Token,

    fn init(
        allocator: std.mem.Allocator,
        source: []const u8,
        diagnostics: diagnostic.Reporter,
    ) CompileError!AstParser {
        var lexer = Lexer{ .source = source, .pos = 0, .diagnostics = diagnostics };
        const first = try lexer.next();
        return .{
            .allocator = allocator,
            .diagnostics = diagnostics,
            .lexer = lexer,
            .current = first,
        };
    }

    fn peek(self: *AstParser) Token {
        return self.current;
    }

    fn advance(self: *AstParser) CompileError!Token {
        const tok = self.current;
        self.current = try self.lexer.next();
        return tok;
    }

    fn expect(self: *AstParser, tag: Tag, text: []const u8) CompileError!Token {
        if (self.current.tag != tag)
            return self.diagnostics.fail(
                .at(self.current.span.start),
                .{ .expected_token = .{ .token = text } },
            );
        return self.advance();
    }

    fn newNode(self: *AstParser, data: Ast.Node.Data, span: Span) CompileError!*Ast.Node {
        const node = try self.allocator.create(Ast.Node);
        node.* = .{ .span = span, .data = data };
        return node;
    }

    // Parse a variable reference for use as the first argument of len() / join().
    fn parseVarArg(self: *AstParser) CompileError!VariableRef {
        const tok = self.peek();
        return switch (tok.tag) {
            .dollar_brace, .dollar_ident => {
                _ = try self.advance();
                return .{ .name = tok.text };
            },
            else => self.diagnostics.fail(tok.span, .expected_variable),
        };
    }

    // Pratt nud: prefix position — literals, prefix operators, parentheses,
    // variable references, and named function calls.
    fn nud(self: *AstParser) CompileError!*Ast.Node {
        const tok = try self.advance();
        switch (tok.tag) {
            .int => return self.newNode(.{ .int = tok.int_val }, tok.span),
            .float => return self.newNode(.{ .float = tok.float_val }, tok.span),
            .string => return self.newNode(.{ .string = tok.text }, tok.span),

            .minus => {
                const child = try self.parseExpr(prefix_bp);
                return self.newNode(
                    .{ .unary = .{ .op = .negate, .child = child } },
                    .{ .start = tok.span.start, .end = child.span.end },
                );
            },
            .bang => {
                const child = try self.parseExpr(prefix_bp);
                return self.newNode(
                    .{ .unary = .{ .op = .not, .child = child } },
                    .{ .start = tok.span.start, .end = child.span.end },
                );
            },

            .lparen => {
                const inner = try self.parseExpr(0);
                _ = try self.expect(.rparen, ")");
                return inner;
            },

            .dollar_brace => {
                const ref = VariableRef{ .name = tok.text };
                if (self.peek().tag == .lbracket) {
                    _ = try self.advance(); // consume [
                    const index = try self.parseExpr(0);
                    const rb = try self.expect(.rbracket, "]");
                    return self.newNode(
                        .{ .load_list_elem = .{ .ref = ref, .index = index } },
                        .{ .start = tok.span.start, .end = rb.span.end },
                    );
                }
                return self.newNode(.{ .load_var = ref }, tok.span);
            },

            .dollar_ident => return self.newNode(.{ .load_var = .{ .name = tok.text } }, tok.span),

            .ident => {
                const name = tok.text;
                _ = try self.expect(.lparen, "(");
                if (std.mem.eql(u8, name, "len")) {
                    const ref = try self.parseVarArg();
                    const rp = try self.expect(.rparen, ")");
                    return self.newNode(
                        .{ .load_list_len = ref },
                        .{ .start = tok.span.start, .end = rp.span.end },
                    );
                } else if (std.mem.eql(u8, name, "min") or std.mem.eql(u8, name, "max")) {
                    const lhs = try self.parseExpr(0);
                    _ = try self.expect(.comma, ",");
                    const rhs = try self.parseExpr(0);
                    const rp = try self.expect(.rparen, ")");
                    const op: Ast.BinaryOp = if (std.mem.eql(u8, name, "min")) .call_min else .call_max;
                    return self.newNode(
                        .{ .binary = .{ .op = op, .lhs = lhs, .rhs = rhs } },
                        .{ .start = tok.span.start, .end = rp.span.end },
                    );
                } else if (std.mem.eql(u8, name, "join")) {
                    const ref = try self.parseVarArg();
                    _ = try self.expect(.comma, ",");
                    const delim = try self.parseExpr(0);
                    const rp = try self.expect(.rparen, ")");
                    return self.newNode(
                        .{ .call_join = .{ .ref = ref, .delim = delim } },
                        .{ .start = tok.span.start, .end = rp.span.end },
                    );
                } else {
                    return self.diagnostics.fail(tok.span, .{ .unknown_function = .{ .name = name } });
                }
            },

            .eof => return self.diagnostics.fail(tok.span, .expected_expression),
            else => return self.diagnostics.fail(tok.span, .unexpected_token),
        }
    }

    // Pratt main loop: parse an expression whose operators have left_bp > min_bp.
    fn parseExpr(self: *AstParser, min_bp: u8) CompileError!*Ast.Node {
        var lhs = try self.nud();
        while (true) {
            const tok = self.peek();
            const bps = infixBp(tok.tag) orelse break;
            if (bps.left <= min_bp) break;
            _ = try self.advance();
            const rhs = try self.parseExpr(bps.right);
            const span: Span = .cover(lhs.span, rhs.span);
            lhs = switch (tok.tag) {
                .pipe_pipe => try self.newNode(.{ .logical_or = .{ .lhs = lhs, .rhs = rhs } }, span),
                .amp_amp => try self.newNode(.{ .logical_and = .{ .lhs = lhs, .rhs = rhs } }, span),
                .eq_eq => try self.newNode(.{ .binary = .{ .op = .cmp_eq, .lhs = lhs, .rhs = rhs } }, span),
                .bang_eq => try self.newNode(.{ .binary = .{ .op = .cmp_ne, .lhs = lhs, .rhs = rhs } }, span),
                .lt => try self.newNode(.{ .binary = .{ .op = .cmp_lt, .lhs = lhs, .rhs = rhs } }, span),
                .gt => try self.newNode(.{ .binary = .{ .op = .cmp_gt, .lhs = lhs, .rhs = rhs } }, span),
                .lt_eq => try self.newNode(.{ .binary = .{ .op = .cmp_le, .lhs = lhs, .rhs = rhs } }, span),
                .gt_eq => try self.newNode(.{ .binary = .{ .op = .cmp_ge, .lhs = lhs, .rhs = rhs } }, span),
                .plus => try self.newNode(.{ .binary = .{ .op = .add, .lhs = lhs, .rhs = rhs } }, span),
                .minus => try self.newNode(.{ .binary = .{ .op = .sub, .lhs = lhs, .rhs = rhs } }, span),
                .star => try self.newNode(.{ .binary = .{ .op = .mul, .lhs = lhs, .rhs = rhs } }, span),
                .slash => try self.newNode(.{ .binary = .{ .op = .div, .lhs = lhs, .rhs = rhs } }, span),
                else => unreachable,
            };
        }
        return lhs;
    }
};

pub fn parseAst(
    allocator: std.mem.Allocator,
    source: []const u8,
    diagnostics: diagnostic.Reporter,
) CompileError!Ast {
    var parser = try AstParser.init(allocator, source, diagnostics);
    const root = try parser.parseExpr(0);
    errdefer ast_mod.destroyNode(allocator, root);
    if (parser.peek().tag != .eof)
        return parser.diagnostics.fail(parser.peek().span, .unexpected_token);
    return .{ .root = root };
}
