const std = @import("std");
const diagnostic = @import("../diagnostic.zig");

pub const Span = diagnostic.Span;
pub const Message = diagnostic.Message;
pub const Diagnostic = diagnostic.Diagnostic;

pub const CompileError = error{
    AnalysisFail,
    OutOfMemory,
};

pub const Diagnostics = struct {
    inner: *diagnostic.Diagnostics,
    context: diagnostic.Context = .{},
    source_kind: diagnostic.SourceKind,
    source: []const u8,

    pub fn init(
        inner: *diagnostic.Diagnostics,
        context: diagnostic.Context,
        source_kind: diagnostic.SourceKind,
        source: []const u8,
    ) Diagnostics {
        return .{
            .inner = inner,
            .context = context,
            .source_kind = source_kind,
            .source = source,
        };
    }

    pub fn fail(self: *Diagnostics, span: Span, message: Message) CompileError {
        return self.inner.failDiagnostic(.{
            .severity = .fatal,
            .context = self.context,
            .source_kind = self.source_kind,
            .source = self.source,
            .span = span,
            .message = message,
        });
    }
};

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

pub const ArithOp = enum {
    add,
    sub,
    mul,
};

pub const CmpOp = enum {
    gt,
    lt,
    ge,
    le,
    eq,
    ne,
};

pub const EvalError = error{
    InvalidExpression,
    DivisionByZero,
    VariableNotFound,
    OutOfMemory,
};

pub fn promoteArith(a: Value, b: Value, comptime op: ArithOp) EvalError!Value {
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

pub fn divValues(a: Value, b: Value) EvalError!Value {
    if (a == .string or a == .bool or b == .string or b == .bool) return error.InvalidExpression;
    const rf = b.toFloat();
    if (rf == 0.0) return error.DivisionByZero;
    return .{ .float = a.toFloat() / rf };
}

pub fn cmpValues(a: Value, b: Value, op: CmpOp) EvalError!bool {
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

pub fn promoteMinMax(a: Value, b: Value, comptime pick_min: bool) EvalError!Value {
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

    pub fn at(self: ResolvedList, index: usize) ?ResolvedValue {
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

pub fn resolveBuiltin(name: []const u8) ?VariableBinding {
    if (std.mem.eql(u8, name, "$ITER")) return .{ .builtin = .iter };
    if (std.mem.eql(u8, name, "$TASK_IDX")) return .{ .builtin = .task_idx };
    if (std.mem.eql(u8, name, "$ELAPSED_MS")) return .{ .builtin = .elapsed_ms };
    return null;
}
