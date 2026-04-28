const std = @import("std");
const diagnostic = @import("../diagnostic.zig");
const types = @import("types.zig");
const bytecode = @import("bytecode.zig");

const CompileError = types.CompileError;
const Span = types.Span;
const VariableBinding = types.VariableBinding;
const VariableRef = types.VariableRef;

pub const Ast = struct {
    root: *Node,

    pub const UnaryOp = enum {
        negate,
        not,
        to_bool,
    };

    pub const BinaryOp = enum {
        add,
        sub,
        mul,
        div,
        cmp_gt,
        cmp_lt,
        cmp_ge,
        cmp_le,
        cmp_eq,
        cmp_ne,
        call_min,
        call_max,
    };

    pub const Node = struct {
        span: Span,
        data: Data,

        pub const Data = union(enum) {
            int: i64,
            float: f64,
            bool: bool,
            string: []const u8,
            load_var: VariableRef,
            load_list_len: VariableRef,
            load_list_elem: struct {
                ref: VariableRef,
                index: *Node,
            },
            call_join: struct {
                ref: VariableRef,
                delim: *Node,
            },
            unary: struct {
                op: UnaryOp,
                child: *Node,
            },
            binary: struct {
                op: BinaryOp,
                lhs: *Node,
                rhs: *Node,
            },
            logical_and: struct {
                lhs: *Node,
                rhs: *Node,
            },
            logical_or: struct {
                lhs: *Node,
                rhs: *Node,
            },
        };

        pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
            switch (self.data) {
                .int, .float, .bool, .string, .load_var, .load_list_len => {},
                .load_list_elem => |data| {
                    destroyNode(allocator, data.index);
                },
                .call_join => |data| {
                    destroyNode(allocator, data.delim);
                },
                .unary => |data| destroyNode(allocator, data.child),
                .binary => |data| {
                    destroyNode(allocator, data.lhs);
                    destroyNode(allocator, data.rhs);
                },
                .logical_and => |data| {
                    destroyNode(allocator, data.lhs);
                    destroyNode(allocator, data.rhs);
                },
                .logical_or => |data| {
                    destroyNode(allocator, data.lhs);
                    destroyNode(allocator, data.rhs);
                },
            }
        }

        pub fn bindVariables(self: *Node, slots: anytype, diagnostics: diagnostic.Reporter) CompileError!void {
            switch (self.data) {
                .int, .float, .bool, .string => {},
                .load_var => |*ref| try bindVariableRef(ref, slots, diagnostics, self.span),
                .load_list_len => |*ref| try bindVariableRef(ref, slots, diagnostics, self.span),
                .load_list_elem => |*data| {
                    try bindVariableRef(&data.ref, slots, diagnostics, self.span);
                    try data.index.bindVariables(slots, diagnostics);
                },
                .call_join => |*data| {
                    try bindVariableRef(&data.ref, slots, diagnostics, self.span);
                    try data.delim.bindVariables(slots, diagnostics);
                },
                .unary => |*data| try data.child.bindVariables(slots, diagnostics),
                .binary => |*data| {
                    try data.lhs.bindVariables(slots, diagnostics);
                    try data.rhs.bindVariables(slots, diagnostics);
                },
                .logical_and => |*data| {
                    try data.lhs.bindVariables(slots, diagnostics);
                    try data.rhs.bindVariables(slots, diagnostics);
                },
                .logical_or => |*data| {
                    try data.lhs.bindVariables(slots, diagnostics);
                    try data.rhs.bindVariables(slots, diagnostics);
                },
            }
        }

        pub fn remapBindings(self: *Node, mapper: anytype, diagnostics: diagnostic.Reporter) CompileError!void {
            switch (self.data) {
                .int, .float, .bool, .string => {},
                .load_var => |*ref| try remapVariableRef(ref, mapper, diagnostics, self.span),
                .load_list_len => |*ref| try remapVariableRef(ref, mapper, diagnostics, self.span),
                .load_list_elem => |*data| {
                    try remapVariableRef(&data.ref, mapper, diagnostics, self.span);
                    try data.index.remapBindings(mapper, diagnostics);
                },
                .call_join => |*data| {
                    try remapVariableRef(&data.ref, mapper, diagnostics, self.span);
                    try data.delim.remapBindings(mapper, diagnostics);
                },
                .unary => |*data| try data.child.remapBindings(mapper, diagnostics),
                .binary => |*data| {
                    try data.lhs.remapBindings(mapper, diagnostics);
                    try data.rhs.remapBindings(mapper, diagnostics);
                },
                .logical_and => |*data| {
                    try data.lhs.remapBindings(mapper, diagnostics);
                    try data.rhs.remapBindings(mapper, diagnostics);
                },
                .logical_or => |*data| {
                    try data.lhs.remapBindings(mapper, diagnostics);
                    try data.rhs.remapBindings(mapper, diagnostics);
                },
            }
        }

        pub fn lower(
            self: *const Node,
            allocator: std.mem.Allocator,
            out: *std.ArrayList(bytecode.Op),
            diagnostics: diagnostic.Reporter,
        ) CompileError!void {
            switch (self.data) {
                .int => |value| try out.append(allocator, .{ .push_int = value }),
                .float => |value| try out.append(allocator, .{ .push_float = value }),
                .bool => |value| try out.append(allocator, .{ .push_bool = value }),
                .string => |value| try out.append(allocator, .{ .push_string = try allocator.dupe(u8, value) }),
                .load_var => |ref| try out.append(allocator, .{ .load_var = try expectBinding(ref, diagnostics, self.span) }),
                .load_list_len => |ref| try out.append(allocator, .{ .load_list_len = try expectBinding(ref, diagnostics, self.span) }),
                .load_list_elem => |data| {
                    try data.index.lower(allocator, out, diagnostics);
                    try out.append(allocator, .{ .load_list_elem = try expectBinding(data.ref, diagnostics, self.span) });
                },
                .call_join => |data| {
                    try data.delim.lower(allocator, out, diagnostics);
                    try out.append(allocator, .{ .call_join = try expectBinding(data.ref, diagnostics, self.span) });
                },
                .unary => |data| {
                    try data.child.lower(allocator, out, diagnostics);
                    try out.append(allocator, switch (data.op) {
                        .negate => .negate,
                        .not => .not,
                        .to_bool => .to_bool,
                    });
                },
                .binary => |data| {
                    try data.lhs.lower(allocator, out, diagnostics);
                    try data.rhs.lower(allocator, out, diagnostics);
                    try out.append(allocator, switch (data.op) {
                        .add => .add,
                        .sub => .sub,
                        .mul => .mul,
                        .div => .div,
                        .cmp_gt => .{ .cmp = .gt },
                        .cmp_lt => .{ .cmp = .lt },
                        .cmp_ge => .{ .cmp = .ge },
                        .cmp_le => .{ .cmp = .le },
                        .cmp_eq => .{ .cmp = .eq },
                        .cmp_ne => .{ .cmp = .ne },
                        .call_min => .call_min,
                        .call_max => .call_max,
                    });
                },
                .logical_and => |data| {
                    try data.lhs.lowerAsBool(allocator, out, diagnostics);
                    const jump_pos = out.items.len;
                    try out.append(allocator, .{ .jump_if_false = 0 });
                    try out.append(allocator, .pop);
                    try data.rhs.lowerAsBool(allocator, out, diagnostics);
                    out.items[jump_pos] = .{ .jump_if_false = @intCast(out.items.len - jump_pos - 1) };
                },
                .logical_or => |data| {
                    try data.lhs.lowerAsBool(allocator, out, diagnostics);
                    const jump_pos = out.items.len;
                    try out.append(allocator, .{ .jump_if_true = 0 });
                    try out.append(allocator, .pop);
                    try data.rhs.lowerAsBool(allocator, out, diagnostics);
                    out.items[jump_pos] = .{ .jump_if_true = @intCast(out.items.len - jump_pos - 1) };
                },
            }
        }

        pub fn lowerAsBool(
            self: *const Node,
            allocator: std.mem.Allocator,
            out: *std.ArrayList(bytecode.Op),
            diagnostics: diagnostic.Reporter,
        ) CompileError!void {
            try self.lower(allocator, out, diagnostics);
            if (!self.producesBool()) try out.append(allocator, .to_bool);
        }

        pub fn producesBool(self: *const Node) bool {
            return switch (self.data) {
                .bool => true,
                .unary => |data| switch (data.op) {
                    .not, .to_bool => true,
                    .negate => false,
                },
                .binary => |data| switch (data.op) {
                    .cmp_gt,
                    .cmp_lt,
                    .cmp_ge,
                    .cmp_le,
                    .cmp_eq,
                    .cmp_ne,
                    => true,
                    else => false,
                },
                .logical_and, .logical_or => true,
                else => false,
            };
        }
    };

    pub fn deinit(self: *Ast, allocator: std.mem.Allocator) void {
        destroyNode(allocator, self.root);
        self.* = undefined;
    }

    pub fn bindVariables(self: *Ast, slots: anytype, diagnostics: diagnostic.Reporter) CompileError!void {
        try self.root.bindVariables(slots, diagnostics);
    }

    pub fn remapBindings(self: *Ast, mapper: anytype, diagnostics: diagnostic.Reporter) CompileError!void {
        try self.root.remapBindings(mapper, diagnostics);
    }

    pub fn lower(
        self: *const Ast,
        allocator: std.mem.Allocator,
        diagnostics: diagnostic.Reporter,
    ) CompileError!bytecode.Expression {
        var out: std.ArrayList(bytecode.Op) = .empty;
        errdefer {
            bytecode.freeOwnedOps(allocator, out.items);
            out.deinit(allocator);
        }
        try self.root.lower(allocator, &out, diagnostics);
        try bytecode.validateStackShape(out.items, self.root.span, diagnostics);
        return .{ .ops = try out.toOwnedSlice(allocator) };
    }
};

fn bindVariableRef(ref: *VariableRef, slots: anytype, diagnostics: diagnostic.Reporter, span: Span) CompileError!void {
    switch (ref.*) {
        .name => |name| {
            const binding: VariableBinding = types.resolveBuiltin(name) orelse .{
                .slot = slots.getIndex(name) orelse return diagnostics.fail(span, .{ .unknown_variable = .{ .variable = name } }),
            };
            ref.* = .{ .binding = binding };
        },
        .binding => {},
    }
}

fn remapVariableRef(ref: *VariableRef, mapper: anytype, diagnostics: diagnostic.Reporter, span: Span) CompileError!void {
    switch (ref.*) {
        .binding => |binding| {
            ref.* = .{ .binding = try mapper.remap(binding, span, diagnostics) };
        },
        .name => return diagnostics.fail(span, .unbound_variable),
    }
}

fn expectBinding(ref: VariableRef, diagnostics: diagnostic.Reporter, span: Span) CompileError!VariableBinding {
    return switch (ref) {
        .binding => |binding| binding,
        .name => diagnostics.fail(span, .unbound_variable),
    };
}

pub fn destroyNode(allocator: std.mem.Allocator, node: *Ast.Node) void {
    node.deinit(allocator);
    allocator.destroy(node);
}
