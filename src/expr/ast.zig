const std = @import("std");
const types = @import("types.zig");
const bytecode = @import("bytecode.zig");

const EvalError = types.EvalError;
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

    pub const Node = union(enum) {
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

        pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
            switch (self.*) {
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

        pub fn bindVariables(self: *Node, slots: anytype) !void {
            switch (self.*) {
                .int, .float, .bool, .string => {},
                .load_var => |*ref| try types.bindBorrowedVariableRef(ref, slots),
                .load_list_len => |*ref| try types.bindBorrowedVariableRef(ref, slots),
                .load_list_elem => |*data| {
                    try types.bindBorrowedVariableRef(&data.ref, slots);
                    try data.index.bindVariables(slots);
                },
                .call_join => |*data| {
                    try types.bindBorrowedVariableRef(&data.ref, slots);
                    try data.delim.bindVariables(slots);
                },
                .unary => |*data| try data.child.bindVariables(slots),
                .binary => |*data| {
                    try data.lhs.bindVariables(slots);
                    try data.rhs.bindVariables(slots);
                },
                .logical_and => |*data| {
                    try data.lhs.bindVariables(slots);
                    try data.rhs.bindVariables(slots);
                },
                .logical_or => |*data| {
                    try data.lhs.bindVariables(slots);
                    try data.rhs.bindVariables(slots);
                },
            }
        }

        pub fn remapBindings(self: *Node, mapper: anytype) !void {
            switch (self.*) {
                .int, .float, .bool, .string => {},
                .load_var => |*ref| try types.remapBoundVariableRef(ref, mapper),
                .load_list_len => |*ref| try types.remapBoundVariableRef(ref, mapper),
                .load_list_elem => |*data| {
                    try types.remapBoundVariableRef(&data.ref, mapper);
                    try data.index.remapBindings(mapper);
                },
                .call_join => |*data| {
                    try types.remapBoundVariableRef(&data.ref, mapper);
                    try data.delim.remapBindings(mapper);
                },
                .unary => |*data| try data.child.remapBindings(mapper),
                .binary => |*data| {
                    try data.lhs.remapBindings(mapper);
                    try data.rhs.remapBindings(mapper);
                },
                .logical_and => |*data| {
                    try data.lhs.remapBindings(mapper);
                    try data.rhs.remapBindings(mapper);
                },
                .logical_or => |*data| {
                    try data.lhs.remapBindings(mapper);
                    try data.rhs.remapBindings(mapper);
                },
            }
        }

        pub fn lower(self: *const Node, allocator: std.mem.Allocator, out: *std.ArrayList(bytecode.Op)) EvalError!void {
            switch (self.*) {
                .int => |value| try out.append(allocator, .{ .push_int = value }),
                .float => |value| try out.append(allocator, .{ .push_float = value }),
                .bool => |value| try out.append(allocator, .{ .push_bool = value }),
                .string => |value| try out.append(allocator, .{ .push_string = try allocator.dupe(u8, value) }),
                .load_var => |ref| try out.append(allocator, .{ .load_var = ref.binding }),
                .load_list_len => |ref| try out.append(allocator, .{ .load_list_len = ref.binding }),
                .load_list_elem => |data| {
                    try data.index.lower(allocator, out);
                    try out.append(allocator, .{ .load_list_elem = data.ref.binding });
                },
                .call_join => |data| {
                    try data.delim.lower(allocator, out);
                    try out.append(allocator, .{ .call_join = data.ref.binding });
                },
                .unary => |data| {
                    try data.child.lower(allocator, out);
                    try out.append(allocator, switch (data.op) {
                        .negate => .negate,
                        .not => .not,
                        .to_bool => .to_bool,
                    });
                },
                .binary => |data| {
                    try data.lhs.lower(allocator, out);
                    try data.rhs.lower(allocator, out);
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
                    try data.lhs.lowerAsBool(allocator, out);
                    const jump_pos = out.items.len;
                    try out.append(allocator, .{ .jump_if_false = 0 });
                    try out.append(allocator, .pop);
                    try data.rhs.lowerAsBool(allocator, out);
                    out.items[jump_pos] = .{ .jump_if_false = @intCast(out.items.len - jump_pos - 1) };
                },
                .logical_or => |data| {
                    try data.lhs.lowerAsBool(allocator, out);
                    const jump_pos = out.items.len;
                    try out.append(allocator, .{ .jump_if_true = 0 });
                    try out.append(allocator, .pop);
                    try data.rhs.lowerAsBool(allocator, out);
                    out.items[jump_pos] = .{ .jump_if_true = @intCast(out.items.len - jump_pos - 1) };
                },
            }
        }

        pub fn lowerAsBool(self: *const Node, allocator: std.mem.Allocator, out: *std.ArrayList(bytecode.Op)) EvalError!void {
            try self.lower(allocator, out);
            if (!self.producesBool()) try out.append(allocator, .to_bool);
        }

        pub fn producesBool(self: *const Node) bool {
            return switch (self.*) {
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

    pub fn bindVariables(self: *Ast, slots: anytype) !void {
        try self.root.bindVariables(slots);
    }

    pub fn remapBindings(self: *Ast, mapper: anytype) !void {
        try self.root.remapBindings(mapper);
    }

    pub fn lower(self: *const Ast, allocator: std.mem.Allocator) EvalError!bytecode.Expression {
        var out: std.ArrayList(bytecode.Op) = .empty;
        errdefer {
            bytecode.freeOwnedOps(allocator, out.items);
            out.deinit(allocator);
        }
        try self.root.lower(allocator, &out);
        try bytecode.validateStackShape(out.items);
        return .{ .ops = try out.toOwnedSlice(allocator) };
    }
};

pub fn destroyNode(allocator: std.mem.Allocator, node: *Ast.Node) void {
    node.deinit(allocator);
    allocator.destroy(node);
}
