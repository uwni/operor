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

pub fn bindBorrowedVariableRef(ref: *VariableRef, slots: anytype) !void {
    switch (ref.*) {
        .name => |name| {
            const binding: VariableBinding = resolveBuiltin(name) orelse .{
                .slot = slots.getIndex(name) orelse return error.UndeclaredVariable,
            };
            ref.* = .{ .binding = binding };
        },
        .binding => return error.AlreadyBound,
    }
}

pub fn remapBoundVariableRef(ref: *VariableRef, mapper: anytype) !void {
    switch (ref.*) {
        .binding => |binding| {
            ref.* = .{ .binding = try mapper.remap(binding) };
        },
        .name => return error.UnboundVariable,
    }
}
