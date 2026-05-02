const std = @import("std");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");
const slot_map_mod = @import("slot_map.zig");

const SlotMap = slot_map_mod.SlotMap;
const SlotBindingRemapper = slot_map_mod.SlotBindingRemapper;
pub const ExprSourceKind = slot_map_mod.ExprSourceKind;

/// Parses, optimizes, binds, and lowers a source expression into bytecode.
pub fn compileExpr(
    slot_map: *const SlotMap,
    arena: std.mem.Allocator,
    diag: diagnostic.Reporter,
    context: slot_map_mod.DiagnosticContext,
    source: []const u8,
    source_kind: ExprSourceKind,
) !expr.Expression {
    var temp_arena: std.heap.ArenaAllocator = .init(slot_map.scratch_alloc);
    defer temp_arena.deinit();

    const expr_diags = diag.withContext(context).withSource(source_kind.sourceKind(), source);

    var ast = try expr.parseAst(temp_arena.allocator(), source, expr_diags);
    try ast.bindVariables(&slot_map.slots, expr_diags);

    var optimizer = ExprOptimizer.init(slot_map, temp_arena.allocator(), expr_diags);
    try optimizer.optimize(&ast);
    try ast.remapBindings(SlotBindingRemapper{ .slot_map = slot_map }, expr_diags);
    return try ast.lower(arena, expr_diags);
}

const ExprOptimizer = struct {
    slot_map: *const SlotMap,
    scratch_alloc: std.mem.Allocator,
    diagnostics: diagnostic.Reporter,

    const ScalarClass = enum {
        int,
        float,
        other,
    };

    pub fn init(slot_map: *const SlotMap, scratch_alloc: std.mem.Allocator, diagnostics: diagnostic.Reporter) ExprOptimizer {
        return .{
            .slot_map = slot_map,
            .scratch_alloc = scratch_alloc,
            .diagnostics = diagnostics,
        };
    }

    pub fn optimize(self: *ExprOptimizer, ast: *expr.Ast) !void {
        ast.root = try self.simplify(ast.root);
    }

    fn newNode(self: *ExprOptimizer, data: expr.Ast.Node.Data, span: expr.Span) !*expr.Ast.Node {
        const node = try self.scratch_alloc.create(expr.Ast.Node);
        node.* = .{ .span = span, .data = data };
        return node;
    }

    fn simplify(self: *ExprOptimizer, node: *expr.Ast.Node) !*expr.Ast.Node {
        return switch (node.data) {
            .int, .float, .bool, .string => node,
            .load_var => |ref| blk: {
                if (try self.constScalarNode(ref, node.span)) |const_node| break :blk const_node;
                break :blk node;
            },
            .load_list_len => |ref| blk: {
                if (self.constListItems(ref)) |items| {
                    break :blk try self.newNode(.{ .int = @intCast(items.len) }, node.span);
                }
                break :blk node;
            },
            .load_list_elem => |data| blk: {
                const index = try self.simplify(data.index);
                if (self.constListItems(data.ref)) |items| {
                    if (constInt(index)) |idx| {
                        break :blk try self.foldConstListElem(items, idx, node.span, index.span);
                    }
                }
                if (index == data.index) break :blk node;
                break :blk try self.newNode(.{ .load_list_elem = .{ .ref = data.ref, .index = index } }, node.span);
            },
            .call_join => |data| blk: {
                const delim = try self.simplify(data.delim);
                if (try self.foldConstJoin(data.ref, delim, node.span)) |folded| break :blk folded;
                if (delim == data.delim) break :blk node;
                break :blk try self.newNode(.{ .call_join = .{ .ref = data.ref, .delim = delim } }, node.span);
            },
            .unary => |data| blk: {
                const child = try self.simplify(data.child);
                if (data.op == .to_bool and child.producesBool()) break :blk child;
                if (constValue(child)) |value| {
                    const folded = try self.foldConstValue(foldUnaryConst(data.op, value), node.span);
                    break :blk try self.newConstNode(folded, node.span);
                }
                if (child == data.child) break :blk node;
                break :blk try self.newNode(.{ .unary = .{ .op = data.op, .child = child } }, node.span);
            },
            .binary => |data| blk: {
                const lhs = try self.simplify(data.lhs);
                const rhs = try self.simplify(data.rhs);
                if (constValue(lhs)) |left_value| {
                    if (constValue(rhs)) |right_value| {
                        const folded = try self.foldConstValue(foldBinaryConst(data.op, left_value, right_value), node.span);
                        break :blk try self.newConstNode(folded, node.span);
                    }
                }
                if (data.op == .add or data.op == .mul) {
                    if (try self.reassociateBinary(data.op, lhs, rhs, node.span)) |rewritten| break :blk rewritten;
                }
                if (lhs == data.lhs and rhs == data.rhs) break :blk node;
                break :blk try self.newNode(.{ .binary = .{ .op = data.op, .lhs = lhs, .rhs = rhs } }, node.span);
            },
            .logical_and => |data| blk: {
                const lhs = try self.simplify(data.lhs);
                const rhs = try self.simplify(data.rhs);
                if (constValue(lhs)) |left_value| {
                    if (!left_value.isTruthy()) break :blk try self.newNode(.{ .bool = false }, node.span);
                    break :blk try self.makeToBool(rhs);
                }
                if (constValue(rhs)) |right_value| {
                    if (right_value.isTruthy()) break :blk try self.makeToBool(lhs);
                }
                if (lhs == data.lhs and rhs == data.rhs) break :blk node;
                break :blk try self.newNode(.{ .logical_and = .{ .lhs = lhs, .rhs = rhs } }, node.span);
            },
            .logical_or => |data| blk: {
                const lhs = try self.simplify(data.lhs);
                const rhs = try self.simplify(data.rhs);
                if (constValue(lhs)) |left_value| {
                    if (left_value.isTruthy()) break :blk try self.newNode(.{ .bool = true }, node.span);
                    break :blk try self.makeToBool(rhs);
                }
                if (constValue(rhs)) |right_value| {
                    if (!right_value.isTruthy()) break :blk try self.makeToBool(lhs);
                }
                if (lhs == data.lhs and rhs == data.rhs) break :blk node;
                break :blk try self.newNode(.{ .logical_or = .{ .lhs = lhs, .rhs = rhs } }, node.span);
            },
        };
    }

    fn newConstNode(self: *ExprOptimizer, value: expr.Value, span: expr.Span) !*expr.Ast.Node {
        return switch (value) {
            .int => |v| try self.newNode(.{ .int = v }, span),
            .float => |v| try self.newNode(.{ .float = v }, span),
            .bool => |v| try self.newNode(.{ .bool = v }, span),
            .string => |v| try self.newNode(.{ .string = try self.scratch_alloc.dupe(u8, v) }, span),
        };
    }

    fn constValue(node: *expr.Ast.Node) ?expr.Value {
        return switch (node.data) {
            .int => |value| .{ .int = value },
            .float => |value| .{ .float = value },
            .bool => |value| .{ .bool = value },
            .string => |value| .{ .string = value },
            else => null,
        };
    }

    fn constInt(node: *expr.Ast.Node) ?i64 {
        return switch (node.data) {
            .int => |value| value,
            else => null,
        };
    }

    fn constString(node: *expr.Ast.Node) ?[]const u8 {
        return switch (node.data) {
            .string => |value| value,
            else => null,
        };
    }

    fn constScalarNode(self: *ExprOptimizer, ref: expr.VariableRef, span: expr.Span) !?*expr.Ast.Node {
        const binding = switch (ref) {
            .binding => |binding| binding,
            .name => return null,
        };
        return switch (binding) {
            .builtin => null,
            .slot => |slot| if (slot >= self.slot_map.const_count)
                null
            else switch (self.slot_map.initial_values[slot]) {
                .int => |value| try self.newNode(.{ .int = value }, span),
                .float => |value| try self.newNode(.{ .float = value }, span),
                .bool => |value| try self.newNode(.{ .bool = value }, span),
                .string => |value| try self.newNode(.{ .string = try self.scratch_alloc.dupe(u8, value.items()) }, span),
                .list => null,
            },
        };
    }

    fn constListItems(self: *ExprOptimizer, ref: expr.VariableRef) ?[]const recipe_ir.Value {
        const binding = switch (ref) {
            .binding => |binding| binding,
            .name => return null,
        };
        return switch (binding) {
            .builtin => null,
            .slot => |slot| if (slot >= self.slot_map.const_count)
                null
            else switch (self.slot_map.initial_values[slot]) {
                .list => |items| items.items(),
                else => null,
            },
        };
    }

    fn makeToBool(self: *ExprOptimizer, node: *expr.Ast.Node) !*expr.Ast.Node {
        if (node.producesBool()) return node;
        if (constValue(node)) |value| return try self.newNode(.{ .bool = value.isTruthy() }, node.span);
        return try self.newNode(.{ .unary = .{ .op = .to_bool, .child = node } }, node.span);
    }

    fn scalarClass(self: *ExprOptimizer, node: *expr.Ast.Node) ScalarClass {
        return switch (node.data) {
            .int => .int,
            .float => .float,
            .bool, .string => .other,
            .load_var => |ref| self.bindingScalarClass(ref),
            .load_list_len => .int,
            .load_list_elem, .call_join, .logical_and, .logical_or => .other,
            .unary => |data| switch (data.op) {
                .negate => self.scalarClass(data.child),
                .not, .to_bool => .other,
            },
            .binary => |data| switch (data.op) {
                .add, .sub, .mul, .call_min, .call_max => blk: {
                    const lhs = self.scalarClass(data.lhs);
                    const rhs = self.scalarClass(data.rhs);
                    if (lhs == .int and rhs == .int) break :blk .int;
                    if ((lhs == .int or lhs == .float) and (rhs == .int or rhs == .float)) break :blk .float;
                    break :blk .other;
                },
                .div => blk: {
                    const lhs = self.scalarClass(data.lhs);
                    const rhs = self.scalarClass(data.rhs);
                    if ((lhs == .int or lhs == .float) and (rhs == .int or rhs == .float)) break :blk .float;
                    break :blk .other;
                },
                .cmp_gt, .cmp_lt, .cmp_ge, .cmp_le, .cmp_eq, .cmp_ne => .other,
            },
        };
    }

    fn bindingScalarClass(self: *ExprOptimizer, ref: expr.VariableRef) ScalarClass {
        const binding = switch (ref) {
            .binding => |binding| binding,
            .name => return .other,
        };
        return switch (binding) {
            .builtin => .int,
            .slot => |slot| switch (self.slot_map.initial_values[slot]) {
                .int => .int,
                .float => .float,
                else => .other,
            },
        };
    }

    fn reassociateBinary(self: *ExprOptimizer, op: expr.Ast.BinaryOp, lhs: *expr.Ast.Node, rhs: *expr.Ast.Node, span: expr.Span) !?*expr.Ast.Node {
        if (op != .add and op != .mul) return null;

        var operands: std.ArrayList(*expr.Ast.Node) = .empty;
        defer operands.deinit(self.scratch_alloc);
        try self.collectAssocOperands(&operands, op, lhs);
        try self.collectAssocOperands(&operands, op, rhs);

        for (operands.items) |item| {
            if (self.scalarClass(item) != .int) return null;
        }

        var const_count: usize = 0;
        var first_const_pos: ?usize = null;
        var aggregate: i64 = if (op == .add) 0 else 1;
        var nonconst: std.ArrayList(*expr.Ast.Node) = .empty;
        defer nonconst.deinit(self.scratch_alloc);

        for (operands.items, 0..) |item, idx| {
            if (constInt(item)) |value| {
                if (first_const_pos == null) first_const_pos = idx;
                const_count += 1;
                aggregate = switch (op) {
                    .add => aggregate + value,
                    .mul => aggregate * value,
                    else => unreachable,
                };
            } else {
                try nonconst.append(self.scratch_alloc, item);
            }
        }

        if (const_count == 0) return null;

        const drop_identity = switch (op) {
            .add => aggregate == 0 and nonconst.items.len > 0,
            .mul => aggregate == 1 and nonconst.items.len > 0,
            else => unreachable,
        };
        if (const_count == 1 and !drop_identity) return null;

        var ordered: std.ArrayList(*expr.Ast.Node) = .empty;
        defer ordered.deinit(self.scratch_alloc);

        const insert_pos = blk: {
            if (first_const_pos == null) break :blk nonconst.items.len;
            var count_before: usize = 0;
            for (operands.items[0..first_const_pos.?]) |item| {
                if (constInt(item) == null) count_before += 1;
            }
            break :blk count_before;
        };

        for (nonconst.items, 0..) |item, idx| {
            if (!drop_identity and idx == insert_pos) {
                try ordered.append(self.scratch_alloc, try self.newNode(.{ .int = aggregate }, span));
            }
            try ordered.append(self.scratch_alloc, item);
        }
        if (!drop_identity and insert_pos >= nonconst.items.len) {
            try ordered.append(self.scratch_alloc, try self.newNode(.{ .int = aggregate }, span));
        }

        if (ordered.items.len == 0) return try self.newNode(.{ .int = aggregate }, span);
        if (ordered.items.len == 1) return ordered.items[0];

        var current = ordered.items[0];
        for (ordered.items[1..]) |item| {
            current = try self.newNode(.{ .binary = .{ .op = op, .lhs = current, .rhs = item } }, expr.Span.cover(current.span, item.span));
        }
        return current;
    }

    fn collectAssocOperands(self: *ExprOptimizer, out: *std.ArrayList(*expr.Ast.Node), op: expr.Ast.BinaryOp, node: *expr.Ast.Node) !void {
        switch (node.data) {
            .binary => |data| if (data.op == op) {
                try self.collectAssocOperands(out, op, data.lhs);
                try self.collectAssocOperands(out, op, data.rhs);
                return;
            },
            else => {},
        }
        try out.append(self.scratch_alloc, node);
    }

    fn foldConstListElem(self: *ExprOptimizer, items: []const recipe_ir.Value, index: i64, span: expr.Span, index_span: expr.Span) !*expr.Ast.Node {
        if (index < 0) return self.diagnostics.fail(index_span, .{ .negative_list_index = .{ .index = index } });
        const idx: usize = @intCast(index);
        if (idx >= items.len) return self.diagnostics.fail(index_span, .{ .list_index_out_of_bounds = .{ .index = index, .len = items.len } });
        return switch (items[idx]) {
            .int => |value| try self.newNode(.{ .int = value }, span),
            .float => |value| try self.newNode(.{ .float = value }, span),
            .bool => |value| try self.newNode(.{ .bool = value }, span),
            .string => |value| try self.newNode(.{ .string = try self.scratch_alloc.dupe(u8, value.items()) }, span),
            .list => self.diagnostics.fail(span, .nested_list_value),
        };
    }

    fn foldConstJoin(self: *ExprOptimizer, ref: expr.VariableRef, delim: *expr.Ast.Node, span: expr.Span) !?*expr.Ast.Node {
        const items = self.constListItems(ref) orelse return null;
        const delimiter = constString(delim) orelse return null;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(self.scratch_alloc);
        for (items, 0..) |item, idx| {
            if (idx > 0) try out.appendSlice(self.scratch_alloc, delimiter);
            expr.appendResolvedValueText(&out, self.scratch_alloc, SlotMap.resolveConstValue(item)) catch |err| switch (err) {
                error.InvalidExpression => return self.diagnostics.fail(span, .nested_list_value),
                error.OutOfMemory => return error.OutOfMemory,
            };
        }
        return try self.newNode(.{ .string = try out.toOwnedSlice(self.scratch_alloc) }, span);
    }

    fn foldConstValue(self: *ExprOptimizer, value: expr.EvalError!expr.Value, span: expr.Span) expr.CompileError!expr.Value {
        return value catch |err| switch (err) {
            error.InvalidExpression => self.diagnostics.fail(span, .invalid_expression),
            error.DivisionByZero => self.diagnostics.fail(span, .division_by_zero),
            error.VariableNotFound => self.diagnostics.fail(span, .unbound_variable),
            error.OutOfMemory => error.OutOfMemory,
        };
    }

    fn foldUnaryConst(op: expr.Ast.UnaryOp, value: expr.Value) expr.EvalError!expr.Value {
        return switch (op) {
            .negate => switch (value) {
                .int => |v| .{ .int = -v },
                .float => |v| .{ .float = -v },
                else => error.InvalidExpression,
            },
            .not => .{ .bool = !value.isTruthy() },
            .to_bool => .{ .bool = value.isTruthy() },
        };
    }

    fn foldBinaryConst(op: expr.Ast.BinaryOp, lhs: expr.Value, rhs: expr.Value) expr.EvalError!expr.Value {
        return switch (op) {
            .add => try expr.promoteArith(lhs, rhs, .add),
            .sub => try expr.promoteArith(lhs, rhs, .sub),
            .mul => try expr.promoteArith(lhs, rhs, .mul),
            .div => try expr.divValues(lhs, rhs),
            .cmp_gt => .{ .bool = try expr.cmpValues(lhs, rhs, .gt) },
            .cmp_lt => .{ .bool = try expr.cmpValues(lhs, rhs, .lt) },
            .cmp_ge => .{ .bool = try expr.cmpValues(lhs, rhs, .ge) },
            .cmp_le => .{ .bool = try expr.cmpValues(lhs, rhs, .le) },
            .cmp_eq => .{ .bool = try expr.cmpValues(lhs, rhs, .eq) },
            .cmp_ne => .{ .bool = try expr.cmpValues(lhs, rhs, .ne) },
            .call_min => try expr.promoteMinMax(lhs, rhs, true),
            .call_max => try expr.promoteMinMax(lhs, rhs, false),
        };
    }
};
