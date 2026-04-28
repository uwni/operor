const std = @import("std");
const doc_parse = @import("../doc_parse.zig");
const Adapter = @import("../adapter/Adapter.zig");
const template = @import("../adapter/template.zig");
const adapter_schema = @import("../adapter/schema.zig");
const parse_mod = @import("../adapter/parse.zig");
const testing = @import("../testing.zig");
const config = @import("config.zig");
const diagnostic = @import("../diagnostic.zig");
const recipe_ir = @import("compiled.zig");
const expr = @import("../expr.zig");

const max_recipe_size: usize = 512 * 1024;

const SlotTable = std.StringArrayHashMapUnmanaged(void);
const DiagnosticContext = diagnostic.Context;

const ExprSourceKind = enum {
    expression,
    argument,

    fn sourceKind(self: ExprSourceKind) diagnostic.SourceKind {
        return switch (self) {
            .expression => .expression,
            .argument => .argument_expression,
        };
    }
};

pub fn precompilePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe_path: []const u8,
    adapter_dir: std.Io.Dir,
    log: ?*std.Io.Writer,
) !recipe_ir.PrecompiledRecipe {
    var diagnostics: diagnostic.Diagnostics = .init(allocator, recipe_path);
    defer diagnostics.deinit();
    var empty_diagnostics: diagnostic.EmptyDiagnostics = .init();
    defer empty_diagnostics.deinit();
    const reporter = if (log != null) diagnostics.reporter() else empty_diagnostics.reporter();

    var precompile_arena: std.heap.ArenaAllocator = .init(allocator);
    defer precompile_arena.deinit();
    const precompile_allocator = precompile_arena.allocator();
    defer if (log) |writer| {
        diagnostics.writeAll(writer) catch {};
    };

    var recipe_parse_arena: std.heap.ArenaAllocator = .init(precompile_allocator);

    const recipe_cfg = doc_parse.parseFilePath(config.RecipeConfig, recipe_parse_arena.allocator(), io, recipe_path, max_recipe_size) catch |err| {
        try addDocumentError(reporter, err, .{});
        return error.AnalysisFail;
    };

    if (recipe_cfg.pipeline == null) {
        return @as(diagnostic.Error!recipe_ir.PrecompiledRecipe, reporter.fail(null, .{ .missing_pipeline = {} }));
    }

    var adapter_cache_arena: std.heap.ArenaAllocator = .init(precompile_allocator);

    var loaded_adapters = try loadAdapters(adapter_cache_arena.allocator(), io, &recipe_cfg, adapter_dir, reporter);

    const compiled = try precompileInternal(allocator, &recipe_cfg, &loaded_adapters, reporter);

    return compiled;
}

/// Converts a parsed recipe document into the arena-owned runtime form used by preview and execution.
///
/// Precompile walks the recipe in dependency order while accumulating recoverable diagnostics:
/// 1. Create the arena that will own the returned recipe plus a temporary adapter cache used only during validation.
/// 2. Walk `recipe.instruments`, eagerly load every referenced adapter, assign each instrument a dense `instrument_idx`, and copy it into a `PrecompiledInstrument` with an empty per-instrument command cache.
/// 3. Walk `recipe.tasks`, classify each task (sequential, loop, or conditional), and allocate the arena-owned `Task` and `Step` arrays.
/// 4. For every step, resolve the referenced instrument and adapter command, compiling that command on first use so runtime only keeps commands this recipe actually calls while binding each command to its owning precompiled instrument.
/// 5. Clone step arguments into the runtime representation, preserving literal types while validating them against the compiled command placeholders, and bind each step directly to the precompiled command pointer it will execute.
/// 6. Parse `stop_when` and return a fully validated `PrecompiledRecipe` whose data is owned by the arena.
///
/// Precompile only validates and reshapes recipe data; it does not perform VISA I/O or talk to hardware.
fn precompileInternal(
    allocator: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
    reporter: diagnostic.Reporter,
) !recipe_ir.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime.
    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var slot_map = try buildSlotMap(alloc, recipe, reporter);

    // 3. Compile instrument metadata from loaded adapters.
    var precompiled_instruments = try precompileInstruments(alloc, recipe, loaded_adapters);

    // 3-5. Normalize tasks and steps, resolving commands and validating arguments.
    var assign_set: std.StringArrayHashMapUnmanaged(void) = .empty;
    const tasks = try precompileTasks(alloc, recipe, &slot_map, loaded_adapters, &precompiled_instruments, &assign_set, reporter);

    // 6. Validate and resolve pipeline record configuration.
    const pipeline = try resolvePipelineConfig(alloc, recipe, &slot_map, &assign_set, reporter);

    // 7. Parse optional stop_when expression.
    var stop_when: ?expr.Expression = null;
    var stop_when_failed = false;
    if (recipe.stop_when) |src|
        stop_when = slot_map.compileExpr(reporter, .{}, src.source(), .expression) catch |err| switch (err) {
            error.AnalysisFail => blk: {
                stop_when_failed = true;
                break :blk null;
            },
            else => return err,
        };

    if (stop_when_failed) return error.AnalysisFail;

    // 8. Assign save_column indices to steps that contribute to recorded frames.
    assignSaveColumns(tasks, &slot_map, pipeline.record.?.explicit);

    return .{
        .arena = arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = pipeline,
        .stop_when = stop_when,
        .expected_iterations = recipe.expected_iterations,
        .float_precision = recipe.float_precision,
        .initial_values = slot_map.varInitialValues(),
    };
}

fn addDocumentError(diag: diagnostic.Reporter, err: anyerror, context: DiagnosticContext) !void {
    const message: diagnostic.Message = switch (err) {
        error.FileNotFound => .{ .file_not_found = {} },
        error.SyntaxError => .{ .syntax_error = {} },
        error.UnsupportedFormat => .{ .unsupported_format = {} },
        error.WrongType => .{ .wrong_type = {} },
        else => return err,
    };
    try diag.withContext(context).withSourceKind(.recipe_document).add(.fatal, null, message);
}

const SlotMap = struct {
    slots: SlotTable,
    initial_values: []const recipe_ir.Value,
    const_count: usize,
    alloc: std.mem.Allocator,

    /// Compile an expression source string, bind variables, and rewrite it
    /// into runtime-ready bytecode with partial constant folding applied.
    fn compileExpr(
        self: *const SlotMap,
        diag: diagnostic.Reporter,
        context: DiagnosticContext,
        source: []const u8,
        source_kind: ExprSourceKind,
    ) !expr.Expression {
        var temp_arena: std.heap.ArenaAllocator = .init(self.alloc);
        defer temp_arena.deinit();

        const expr_diags = diag.withContext(context).withSource(source_kind.sourceKind(), source);

        var ast = try expr.parseAst(temp_arena.allocator(), source, expr_diags);
        try ast.bindVariables(&self.slots, expr_diags);

        var optimizer = ExprOptimizer.init(self, temp_arena.allocator(), expr_diags);
        try optimizer.optimize(&ast);
        try ast.remapBindings(SlotBindingRemapper{ .slot_map = self }, expr_diags);
        return try ast.lower(self.alloc, expr_diags);
    }

    /// Look up a name and return the runtime binding (var slot remapped)
    /// or the const value if the name refers to a const.
    const ResolvedName = union(enum) {
        binding: expr.VariableBinding,
        const_value: recipe_ir.Value,
    };

    fn resolveName(self: *const SlotMap, name: []const u8) ?ResolvedName {
        if (expr.resolveBuiltin(name)) |b| return .{ .binding = b };
        const slot = self.slots.getIndex(name) orelse return null;
        if (slot < self.const_count) return .{ .const_value = self.initial_values[slot] };
        return .{ .binding = .{ .slot = slot - self.const_count } };
    }

    /// Returns only the var portion of initial_values (excluding consts).
    fn varInitialValues(self: *const SlotMap) []const recipe_ir.Value {
        return self.initial_values[self.const_count..];
    }

    /// Validate that `name` refers to a mutable var and return its remapped slot index.
    fn varSlotIndex(self: *const SlotMap, diag: diagnostic.Reporter, context: DiagnosticContext, name: []const u8) !usize {
        var variable_context = context;
        variable_context.variable_name = name;
        const variable_reporter = diag.withContext(variable_context);

        if (expr.resolveBuiltin(name) != null) {
            return variable_reporter.fail(null, .{ .builtin_variable_conflict = .{ .variable = name } });
        }

        const slot = self.slots.getIndex(name) orelse {
            return variable_reporter.fail(null, .{ .unknown_variable = .{ .variable = name } });
        };

        if (slot < self.const_count) {
            return variable_reporter.fail(null, .{ .assign_to_const = .{ .variable = name } });
        }

        return slot - self.const_count;
    }

    fn resolveConstValue(value: recipe_ir.Value) expr.ResolvedValue {
        return switch (value) {
            .float => |f| .{ .float = f },
            .int => |i| .{ .int = i },
            .bool => |b| .{ .bool = b },
            .string => |s| .{ .string = s },
            .list => |items| .{ .list = .{
                .len = items.len,
                .ctx = @ptrCast(items.ptr),
                .at_fn = constListAt,
            } },
        };
    }

    fn constListAt(ctx: *const anyopaque, index: usize) ?expr.ResolvedValue {
        const items: [*]const recipe_ir.Value = @ptrCast(@alignCast(ctx));
        return resolveConstValue(items[index]);
    }
};

const SlotBindingRemapper = struct {
    slot_map: *const SlotMap,

    pub fn remap(
        self: @This(),
        binding: expr.VariableBinding,
        span: expr.Span,
        diagnostics: diagnostic.Reporter,
    ) expr.CompileError!expr.VariableBinding {
        return switch (binding) {
            .builtin => binding,
            .slot => |slot| if (slot < self.slot_map.const_count)
                diagnostics.fail(span, .const_runtime_value)
            else
                .{ .slot = slot - self.slot_map.const_count },
        };
    }
};

const ExprOptimizer = struct {
    slot_map: *const SlotMap,
    allocator: std.mem.Allocator,
    diagnostics: diagnostic.Reporter,

    const ScalarClass = enum {
        int,
        float,
        other,
    };

    fn init(slot_map: *const SlotMap, allocator: std.mem.Allocator, diagnostics: diagnostic.Reporter) ExprOptimizer {
        return .{
            .slot_map = slot_map,
            .allocator = allocator,
            .diagnostics = diagnostics,
        };
    }

    fn optimize(self: *ExprOptimizer, ast: *expr.Ast) !void {
        ast.root = try self.simplify(ast.root);
    }

    fn newNode(self: *ExprOptimizer, data: expr.Ast.Node.Data, span: expr.Span) !*expr.Ast.Node {
        const node = try self.allocator.create(expr.Ast.Node);
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
            .string => |v| try self.newNode(.{ .string = try self.allocator.dupe(u8, v) }, span),
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
                .string => |value| try self.newNode(.{ .string = try self.allocator.dupe(u8, value) }, span),
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
                .list => |items| items,
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
        defer operands.deinit(self.allocator);
        try self.collectAssocOperands(&operands, op, lhs);
        try self.collectAssocOperands(&operands, op, rhs);

        for (operands.items) |item| {
            if (self.scalarClass(item) != .int) return null;
        }

        var const_count: usize = 0;
        var first_const_pos: ?usize = null;
        var aggregate: i64 = if (op == .add) 0 else 1;
        var nonconst: std.ArrayList(*expr.Ast.Node) = .empty;
        defer nonconst.deinit(self.allocator);

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
                try nonconst.append(self.allocator, item);
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
        defer ordered.deinit(self.allocator);

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
                try ordered.append(self.allocator, try self.newNode(.{ .int = aggregate }, span));
            }
            try ordered.append(self.allocator, item);
        }
        if (!drop_identity and insert_pos >= nonconst.items.len) {
            try ordered.append(self.allocator, try self.newNode(.{ .int = aggregate }, span));
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
        try out.append(self.allocator, node);
    }

    fn foldConstListElem(self: *ExprOptimizer, items: []const recipe_ir.Value, index: i64, span: expr.Span, index_span: expr.Span) !*expr.Ast.Node {
        if (index < 0) return self.diagnostics.fail(index_span, .{ .negative_list_index = index });
        const idx: usize = @intCast(index);
        if (idx >= items.len) return self.diagnostics.fail(index_span, .{ .list_index_out_of_bounds = .{ .index = index, .len = items.len } });
        return switch (items[idx]) {
            .int => |value| try self.newNode(.{ .int = value }, span),
            .float => |value| try self.newNode(.{ .float = value }, span),
            .bool => |value| try self.newNode(.{ .bool = value }, span),
            .string => |value| try self.newNode(.{ .string = try self.allocator.dupe(u8, value) }, span),
            .list => self.diagnostics.fail(span, .nested_list_value),
        };
    }

    fn foldConstJoin(self: *ExprOptimizer, ref: expr.VariableRef, delim: *expr.Ast.Node, span: expr.Span) !?*expr.Ast.Node {
        const items = self.constListItems(ref) orelse return null;
        const delimiter = constString(delim) orelse return null;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(self.allocator);
        for (items, 0..) |item, idx| {
            if (idx > 0) try out.appendSlice(self.allocator, delimiter);
            expr.appendResolvedValueText(&out, self.allocator, SlotMap.resolveConstValue(item)) catch |err| switch (err) {
                error.InvalidExpression => return self.diagnostics.fail(span, .nested_list_value),
                error.OutOfMemory => return error.OutOfMemory,
            };
        }
        return try self.newNode(.{ .string = try out.toOwnedSlice(self.allocator) }, span);
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

/// Validate consts/vars, build the merged slot map (consts first, then vars),
/// compile initial values, and create the compile-time const resolver.
fn buildSlotMap(
    alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    diag: diagnostic.Reporter,
) !SlotMap {
    const const_keys = if (recipe.consts) |c| c.keys() else &.{};
    const const_vals = if (recipe.consts) |c| c.values() else &.{};
    const var_keys = if (recipe.vars) |v| v.keys() else &.{};
    const var_vals = if (recipe.vars) |v| v.values() else &.{};

    // Validate: no name conflicts between consts, vars, and builtins.
    var has_error = false;
    for (const_keys) |name| {
        if (expr.resolveBuiltin(name) != null) {
            try diag.withContext(.{ .variable_name = name }).add(.fatal, null, .{ .builtin_variable_conflict = .{ .variable = name } });
            has_error = true;
        }
    }
    for (var_keys) |name| {
        if (expr.resolveBuiltin(name) != null) {
            try diag.withContext(.{ .variable_name = name }).add(.fatal, null, .{ .builtin_variable_conflict = .{ .variable = name } });
            has_error = true;
        }
        if (recipe.consts != null and recipe.consts.?.contains(name)) {
            try diag.withContext(.{ .variable_name = name }).add(.fatal, null, .{ .duplicate_variable = .{ .variable = name } });
            has_error = true;
        }
    }
    if (has_error) return error.AnalysisFail;

    // Build initial values array: consts first, then vars.
    const initial_values = try alloc.alloc(recipe_ir.Value, const_keys.len + var_keys.len);
    for (const_vals, 0..) |value, idx| {
        initial_values[idx] = try compileInitialValue(alloc, value);
    }
    for (var_vals, 0..) |value, idx| {
        initial_values[const_keys.len + idx] = try compileInitialValue(alloc, value);
    }

    // Build the key-only slot map: consts first, then vars.
    var all_slots: SlotTable = .empty;
    for (const_keys) |name| try all_slots.put(alloc, name, {});
    for (var_keys) |name| try all_slots.put(alloc, name, {});

    return .{
        .slots = all_slots,
        .initial_values = initial_values,
        .const_count = const_keys.len,
        .alloc = alloc,
    };
}

fn loadAdapters(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: *const config.RecipeConfig,
    adapter_dir: std.Io.Dir,
    diag: diagnostic.Reporter,
) !std.StringHashMap(Adapter) {
    var map: std.StringHashMap(Adapter) = .init(allocator);
    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const cfg = entry.value_ptr.*;
        _ = try getOrParseAdapter(allocator, io, &map, adapter_dir, entry.key_ptr.*, cfg.adapter, diag);
    }
    return map;
}

fn precompileInstruments(
    alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
) !std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument) {
    var precompiled_instruments: std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument) = .empty;
    try precompiled_instruments.ensureTotalCapacity(alloc, recipe.instruments.count());

    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr;
        const adapter = loaded_adapters.getPtr(instrument_cfg.adapter).?;

        const name_copy = try alloc.dupe(u8, instrument_name);
        const precompiled_instrument = try precompileOwnedInstrument(alloc, instrument_cfg, adapter);
        try precompiled_instruments.put(alloc, name_copy, precompiled_instrument);
    }
    return precompiled_instruments;
}

fn precompileOwnedInstrument(alloc: std.mem.Allocator, instrument_cfg: *const config.InstrumentConfig, adapter: *const Adapter) !recipe_ir.PrecompiledInstrument {
    const adapter_copy = try alloc.dupe(u8, instrument_cfg.adapter);
    const resource_copy = try alloc.dupe(u8, instrument_cfg.resource);
    const write_termination = try cloneOptionalBytes(alloc, adapter.write_termination);
    const bool_map = try cloneBoolTextMap(alloc, adapter.instrument.bool_format);
    return .{
        .adapter_name = adapter_copy,
        .resource = resource_copy,
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = write_termination,
        .bool_map = bool_map,
        .options = .{
            .timeout_ms = adapter.options.timeout_ms,
            .read_termination = adapter.options.read_termination,
            .query_delay_ms = adapter.options.query_delay_ms,
            .chunk_size = adapter.options.chunk_size,
        },
    };
}

fn precompileTasks(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMapUnmanaged(void),
    diag: diagnostic.Reporter,
) ![]recipe_ir.Task {
    var tasks: std.ArrayList(recipe_ir.Task) = .empty;
    errdefer tasks.deinit(arena_alloc);
    var has_error = false;

    for (recipe.tasks, 0..) |*task_cfg, task_idx| {
        const steps = precompileSteps(arena_alloc, task_cfg.steps, slot_map, loaded_adapters, precompiled_instruments, assign_set, task_idx, diag) catch |err| switch (err) {
            error.AnalysisFail => {
                has_error = true;
                continue;
            },
            else => return err,
        };

        if (task_cfg.@"while") |while_src| {
            const task_context: DiagnosticContext = .{ .task_idx = task_idx };
            const condition = slot_map.compileExpr(diag, task_context, while_src.source(), .expression) catch |err| {
                switch (err) {
                    error.AnalysisFail => {
                        has_error = true;
                        continue;
                    },
                    else => return err,
                }
            };
            // Loop task
            try tasks.append(arena_alloc, .{ .loop = .{
                .condition = condition,
                .steps = steps,
            } });
        } else if (task_cfg.@"if") |guard_src| {
            const task_context: DiagnosticContext = .{ .task_idx = task_idx };
            const condition = slot_map.compileExpr(diag, task_context, guard_src.source(), .expression) catch |err| {
                switch (err) {
                    error.AnalysisFail => {
                        has_error = true;
                        continue;
                    },
                    else => return err,
                }
            };
            // Conditional task
            try tasks.append(arena_alloc, .{ .conditional = .{
                .@"if" = condition,
                .steps = steps,
            } });
        } else {
            // Sequential task
            try tasks.append(arena_alloc, .{ .sequential = .{
                .steps = steps,
            } });
        }
    }
    if (has_error) return error.AnalysisFail;
    return try tasks.toOwnedSlice(arena_alloc);
}

fn precompileSteps(
    arena_alloc: std.mem.Allocator,
    step_cfgs: []config.StepConfig,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMapUnmanaged(void),
    task_idx: usize,
    diag: diagnostic.Reporter,
) ![]recipe_ir.Step {
    var steps: std.ArrayList(recipe_ir.Step) = .empty;
    errdefer steps.deinit(arena_alloc);
    var has_error = false;

    for (step_cfgs, 0..) |*step_cfg, step_idx| {
        const step = switch (step_cfg.*) {
            .compute => |*cfg| precompileComputeStep(
                arena_alloc,
                slot_map,
                assign_set,
                cfg,
                task_idx,
                step_idx,
                diag,
            ),
            .call => |*cfg| precompileCallStep(
                arena_alloc,
                slot_map,
                loaded_adapters,
                precompiled_instruments,
                assign_set,
                cfg,
                task_idx,
                step_idx,
                diag,
            ),
            .sleep_ms => |*cfg| precompileSleepStep(slot_map, cfg, task_idx, step_idx, diag),
            .parallel => |*cfg| precompileParallelStep(
                arena_alloc,
                slot_map,
                loaded_adapters,
                precompiled_instruments,
                assign_set,
                cfg,
                task_idx,
                step_idx,
                diag,
            ),
        } catch |err| switch (err) {
            error.AnalysisFail => {
                has_error = true;
                continue;
            },
            else => return err,
        };
        try steps.append(arena_alloc, step);
    }
    if (has_error) return error.AnalysisFail;
    return try steps.toOwnedSlice(arena_alloc);
}

fn precompileComputeStep(
    arena_alloc: std.mem.Allocator,
    slot_map: *const SlotMap,
    assign_set: *std.StringArrayHashMapUnmanaged(void),
    cfg: *const config.ComputeStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) !recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };

    const if_expr = try precompileIf(slot_map, diag, step_context, cfg.@"if");

    const assign_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx, .variable_name = cfg.assign };
    const save_slot = try slot_map.varSlotIndex(diag, assign_context, cfg.assign);
    const assign_copy = try arena_alloc.dupe(u8, cfg.assign);
    try assign_set.put(arena_alloc, assign_copy, {});

    const compute_expr = try slot_map.compileExpr(diag, assign_context, cfg.compute, .expression);

    return .{
        .action = .{ .compute = .{
            .expression = compute_expr,
            .save_slot = save_slot,
        } },
        .@"if" = if_expr,
    };
}

fn precompileCallStep(
    arena_alloc: std.mem.Allocator,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMapUnmanaged(void),
    cfg: *const config.CallStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) !recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };

    const dot_pos = std.mem.findScalar(u8, cfg.call, '.') orelse {
        return diag.withContext(step_context).fail(null, .{ .invalid_call_format = .{ .call = cfg.call } });
    };
    const instrument_name = cfg.call[0..dot_pos];
    const command_name = cfg.call[dot_pos + 1 ..];
    if (instrument_name.len == 0 or command_name.len == 0) {
        return diag.withContext(step_context).fail(null, .{ .invalid_call_format = .{ .call = cfg.call } });
    }

    var call_context: DiagnosticContext = .{
        .task_idx = task_idx,
        .step_idx = step_idx,
        .instrument_name = instrument_name,
        .command_name = command_name,
    };

    const if_expr = try precompileIf(slot_map, diag, call_context, cfg.@"if");

    const precompiled_instrument = precompiled_instruments.getPtr(instrument_name) orelse {
        return diag.withContext(call_context).fail(null, .{ .instrument_not_found = .{ .instrument = instrument_name } });
    };

    call_context.adapter_name = loaded_adapters.getKey(precompiled_instrument.adapter_name).?;
    const loaded_adapter = loaded_adapters.getPtr(precompiled_instrument.adapter_name).?;
    const command_source = loaded_adapter.commands.get(command_name) orelse {
        return diag.withContext(call_context).fail(null, .{ .command_not_found = .{
            .instrument = instrument_name,
            .command = command_name,
        } });
    };
    const command = try getOrCompileCommand(arena_alloc, precompiled_instrument, command_source, command_name, loaded_adapter.instrument.bool_format, diag, call_context);

    const call_copy = try arena_alloc.dupe(u8, command_name);
    const instrument_copy = try arena_alloc.dupe(u8, instrument_name);
    const compiled_args = try compileStepArgs(arena_alloc, command, cfg.args, slot_map, diag, call_context);

    var save_slot: ?usize = null;
    if (cfg.assign) |label| {
        var assign_context = call_context;
        assign_context.variable_name = label;
        save_slot = try slot_map.varSlotIndex(diag, assign_context, label);
        const duped = try arena_alloc.dupe(u8, label);
        try assign_set.put(arena_alloc, duped, {});
    }

    return .{
        .action = .{ .instrument_call = .{
            .call = call_copy,
            .instrument = instrument_copy,
            .instrument_idx = precompiled_instruments.getIndex(instrument_name).?,
            .command = command,
            .args = compiled_args,
            .save_slot = save_slot,
        } },
        .@"if" = if_expr,
    };
}

fn precompileIf(
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
    if_src_opt: ?config.BooleanExpr,
) !?expr.Expression {
    if (if_src_opt) |if_src| {
        return try slot_map.compileExpr(diag, context, if_src.source(), .expression);
    }
    return null;
}

fn precompileSleepStep(
    slot_map: *const SlotMap,
    cfg: *const config.SleepStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) !recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };
    const if_expr = try precompileIf(slot_map, diag, step_context, cfg.@"if");
    return .{
        .action = .{ .sleep = .{ .duration_ms = cfg.sleep_ms } },
        .@"if" = if_expr,
    };
}

fn precompileParallelStep(
    arena_alloc: std.mem.Allocator,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMapUnmanaged(void),
    cfg: *const config.ParallelStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) anyerror!recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };

    // Reject nested parallel steps.
    for (cfg.parallel) |*inner| {
        if (inner.* == .parallel) {
            return diag.withContext(step_context).fail(null, .{ .nested_parallel_step = {} });
        }
    }

    const inner_steps = try precompileSteps(
        arena_alloc,
        cfg.parallel,
        slot_map,
        loaded_adapters,
        precompiled_instruments,
        assign_set,
        task_idx,
        diag,
    );
    try validateParallelUniqueInstruments(
        arena_alloc,
        inner_steps,
        precompiled_instruments.count(),
        step_context,
        diag,
    );

    const if_expr = try precompileIf(slot_map, diag, step_context, cfg.@"if");
    return .{
        .action = .{ .parallel = .{ .steps = inner_steps } },
        .@"if" = if_expr,
    };
}

fn validateParallelUniqueInstruments(
    allocator: std.mem.Allocator,
    steps: []const recipe_ir.Step,
    instrument_count: usize,
    context: DiagnosticContext,
    diag: diagnostic.Reporter,
) !void {
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(allocator, instrument_count);
    defer seen.deinit(allocator);

    for (steps) |*step| {
        switch (step.action) {
            .instrument_call => |ic| {
                if (seen.isSet(ic.instrument_idx)) {
                    var duplicate_context = context;
                    duplicate_context.instrument_name = ic.instrument;
                    return diag.withContext(duplicate_context).fail(null, .{ .duplicate_parallel_instrument = .{ .instrument = ic.instrument } });
                }
                seen.set(ic.instrument_idx);
            },
            else => {},
        }
    }
}

fn resolvePipelineConfig(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *const SlotMap,
    assign_set: *const std.StringArrayHashMapUnmanaged(void),
    diag: diagnostic.Reporter,
) !recipe_ir.PipelineConfig {
    const empty_columns: []const []const u8 = &.{};
    const pipeline_cfg = recipe.pipeline orelse {
        try diag.add(.fatal, null, .{ .missing_pipeline = {} });
        return error.AnalysisFail;
    };
    var has_error = false;
    if (pipeline_cfg.record == null) {
        try diag.add(.fatal, null, .{ .missing_record_config = {} });
        has_error = true;
    }

    try validatePipelineConfig(&pipeline_cfg, diag);
    var pipeline = try pipeline_cfg.clone(arena_alloc);
    if (pipeline.record == null) {
        pipeline.record = .{ .explicit = empty_columns };
    }

    switch (pipeline.record.?) {
        .all => {
            pipeline.record = .{ .explicit = try arena_alloc.dupe([]const u8, assign_set.keys()) };
        },
        .explicit => |columns| {
            var seen: std.StringArrayHashMapUnmanaged(void) = .empty;
            var unique_columns: std.ArrayList([]const u8) = .empty;

            for (columns) |name| {
                const column_context: DiagnosticContext = .{ .variable_name = name };
                const column_reporter = diag.withContext(column_context);
                if (seen.getIndex(name) != null) {
                    try column_reporter.warn(null, .{ .duplicate_record_column = .{ .column = name } });
                    continue;
                }
                try seen.put(arena_alloc, name, {});

                var valid = true;
                if (slot_map.resolveName(name) == null) {
                    try column_reporter.add(.fatal, null, .{ .unknown_variable = .{ .variable = name } });
                    valid = false;
                    has_error = true;
                }
                if (!assign_set.contains(name)) {
                    try column_reporter.add(.fatal, null, .{ .record_variable_not_found = .{ .variable = name } });
                    valid = false;
                    has_error = true;
                }
                if (valid) try unique_columns.append(arena_alloc, name);
            }
            pipeline.record = .{ .explicit = try unique_columns.toOwnedSlice(arena_alloc) };
        },
    }
    if (has_error) return error.AnalysisFail;
    return pipeline;
}

fn validatePipelineConfig(cfg: *const recipe_ir.PipelineConfig, diag: diagnostic.Reporter) !void {
    var has_error = false;
    if (cfg.buffer_size) |size| {
        if (size == 0) has_error = true;
    }
    if (cfg.warn_usage_percent) |percent| {
        if (percent == 0 or percent > 100) has_error = true;
    }
    const has_network_host = cfg.network_host != null;
    const has_network_port = cfg.network_port != null;
    if (has_network_host != has_network_port) has_error = true;
    if (cfg.network_port) |port| {
        if (port == 0) has_error = true;
    }
    if (!has_error) return;

    try diag.add(.fatal, null, .{ .invalid_pipeline_config = {} });
    return error.AnalysisFail;
}

fn assignSaveColumns(tasks: []recipe_ir.Task, slot_map: *const SlotMap, columns: []const []const u8) void {
    const var_keys = slot_map.slots.keys()[slot_map.const_count..];
    for (tasks) |*task| {
        for (task.steps()) |*step| {
            assignStepSaveColumn(step, var_keys, columns);
        }
    }
}

fn assignStepSaveColumn(step: *recipe_ir.Step, var_keys: []const []const u8, columns: []const []const u8) void {
    switch (step.action) {
        .instrument_call => |*ic| {
            ic.save_column = if (ic.save_slot) |slot| slotToColumn(var_keys, slot, columns) else null;
        },
        .compute => |*comp| {
            comp.save_column = slotToColumn(var_keys, comp.save_slot, columns);
        },
        .sleep => {},
        .parallel => |par| {
            for (par.steps) |*inner| {
                assignStepSaveColumn(inner, var_keys, columns);
            }
        },
    }
}

fn slotToColumn(var_keys: []const []const u8, save_slot: usize, columns: []const []const u8) ?usize {
    const name = var_keys[save_slot];
    for (columns, 0..) |col_name, col_idx| {
        if (std.mem.eql(u8, col_name, name)) return col_idx;
    }
    return null;
}

fn cloneOptionalBytes(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    if (bytes.len == 0) return "";
    return allocator.dupe(u8, bytes);
}

fn cloneBoolTextMap(
    allocator: std.mem.Allocator,
    source: ?adapter_schema.BoolFormat,
) !?recipe_ir.BoolTextMap {
    const src = source orelse return null;
    return .{
        .true_text = try allocator.dupe(u8, src.true),
        .false_text = try allocator.dupe(u8, src.false),
    };
}

fn getOrParseAdapter(
    allocator: std.mem.Allocator,
    io: std.Io,
    loaded_adapters: *std.StringHashMap(Adapter),
    adapter_dir: std.Io.Dir,
    instrument_name: []const u8,
    adapter_name: []const u8,
    diag: diagnostic.Reporter,
) !*const Adapter {
    if (loaded_adapters.getPtr(adapter_name)) |loaded| return loaded;

    const key = try allocator.dupe(u8, adapter_name);

    var loaded = try parse_mod.parseAdapterInDir(allocator, io, adapter_dir, adapter_name, diag.withContext(.{
        .instrument_name = instrument_name,
        .adapter_name = adapter_name,
    }));
    errdefer loaded.deinit();

    try loaded_adapters.put(key, loaded);
    return loaded_adapters.getPtr(adapter_name).?;
}

fn getOrCompileCommand(
    allocator: std.mem.Allocator,
    instrument: *recipe_ir.PrecompiledInstrument,
    source: Adapter.Command,
    call: []const u8,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !*const recipe_ir.PrecompiledCommand {
    if (instrument.commands.get(call)) |command| return command;

    const key = try allocator.dupe(u8, call);
    const compiled_value = try compileCommand(allocator, source, instrument, adapter_bool_format, diag, context);

    const compiled = try allocator.create(recipe_ir.PrecompiledCommand);
    compiled.* = compiled_value;

    try instrument.commands.put(key, compiled);
    return compiled;
}

fn compileCommand(
    allocator: std.mem.Allocator,
    source: Adapter.Command,
    instrument: *const recipe_ir.PrecompiledInstrument,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) !recipe_ir.PrecompiledCommand {
    var arg_entries: std.ArrayList(ArgBuildEntry) = .empty;
    const segments = try compileSegments(allocator, source.template, &arg_entries, false);

    const args = try allocator.alloc(recipe_ir.CommandArg, arg_entries.items.len);
    for (arg_entries.items, 0..) |entry, idx| {
        var arg_context = base_context;
        arg_context.argument_name = entry.name;
        args[idx] = .{
            .name = entry.name,
            .is_optional = !entry.required,
            .default = try compileArgDefault(allocator, source.args, entry.name),
            .format = try compileArgFormat(allocator, source.args, entry.name, adapter_bool_format, diag, arg_context),
        };
    }
    arg_entries.deinit(allocator);

    return .{
        .instrument = instrument,
        .response = source.response,
        .segments = segments,
        .args = args,
    };
}

const ArgBuildEntry = struct {
    name: []const u8,
    required: bool,
};

fn compileArgFormat(
    allocator: std.mem.Allocator,
    source_args: ?std.StringHashMap(adapter_schema.ArgSpec),
    arg_name: []const u8,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !recipe_ir.ArgFormat {
    var format: recipe_ir.ArgFormat = .{};

    if (adapter_bool_format) |bf| {
        format.bool_map = .{
            .true_text = try allocator.dupe(u8, bf.true),
            .false_text = try allocator.dupe(u8, bf.false),
        };
    }

    if (source_args) |args_map| {
        if (args_map.get(arg_name)) |spec| {
            switch (spec) {
                .string => {},
                .object => |obj| {
                    if (std.mem.eql(u8, obj.type, "bool")) {
                        if ((obj.true == null) != (obj.false == null)) {
                            return diag.withContext(context).fail(null, .{ .partial_bool_map = {} });
                        }
                        if (obj.true) |t| {
                            if (format.bool_map) |old| {
                                allocator.free(old.true_text);
                                allocator.free(old.false_text);
                            }
                            format.bool_map = .{
                                .true_text = try allocator.dupe(u8, t),
                                .false_text = try allocator.dupe(u8, obj.false.?),
                            };
                        }
                    }
                    if (obj.separator) |sep| {
                        format.list_separator = try allocator.dupe(u8, sep);
                    }
                },
            }
        }
    }

    return format;
}

fn compileSegments(
    allocator: std.mem.Allocator,
    template_segments: []const template.Segment,
    arg_entries: *std.ArrayList(ArgBuildEntry),
    in_optional: bool,
) ![]recipe_ir.CompiledSegment {
    const segments = try allocator.alloc(recipe_ir.CompiledSegment, template_segments.len);

    for (template_segments, 0..) |segment, idx| {
        segments[idx] = switch (segment) {
            .literal => |literal| .{ .literal = try allocator.dupe(u8, literal) },
            .placeholder => |name| .{ .arg = blk: {
                if (findArgEntryIndex(arg_entries.items, name)) |arg_idx| {
                    if (!in_optional) arg_entries.items[arg_idx].required = true;
                    break :blk arg_idx;
                }
                const name_copy = try allocator.dupe(u8, name);
                try arg_entries.append(allocator, .{
                    .name = name_copy,
                    .required = !in_optional,
                });
                break :blk arg_entries.items.len - 1;
            } },
            .optional => |inner| .{ .optional = try compileSegments(allocator, inner, arg_entries, true) },
        };
    }

    return segments;
}

fn compileStepArgs(
    allocator: std.mem.Allocator,
    command: *const recipe_ir.PrecompiledCommand,
    doc_args: ?std.StringHashMap(config.ArgValueDoc),
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) ![]recipe_ir.StepArg {
    const args = try allocator.alloc(recipe_ir.StepArg, command.args.len);
    var has_error = false;

    for (command.args, 0..) |arg, idx| {
        if (doc_args) |wrapper| {
            if (wrapper.get(arg.name)) |doc_arg| {
                var arg_context = base_context;
                arg_context.argument_name = arg.name;
                args[idx] = compileArg(allocator, doc_arg, slot_map, diag, arg_context) catch |err| blk: {
                    switch (err) {
                        error.AnalysisFail => {
                            has_error = true;
                            break :blk try makeEmptyStringArg(allocator);
                        },
                        else => return err,
                    }
                };
                continue;
            }
        }

        if (arg.default) |default_arg| {
            args[idx] = default_arg;
            continue;
        }
        if (arg.is_optional) {
            args[idx] = try makeEmptyStringArg(allocator);
            continue;
        }

        var arg_context = base_context;
        arg_context.argument_name = arg.name;
        try diag.withContext(arg_context).add(.fatal, null, .{ .missing_command_argument = .{ .argument = arg.name } });
        has_error = true;
        args[idx] = try makeEmptyStringArg(allocator);
    }

    if (doc_args) |wrapper| {
        var it = wrapper.iterator();
        while (it.next()) |entry| {
            if (!command.hasPlaceholder(entry.key_ptr.*)) {
                var arg_context = base_context;
                arg_context.argument_name = entry.key_ptr.*;
                try diag.withContext(arg_context).add(.fatal, null, .{ .unexpected_command_argument = .{ .argument = entry.key_ptr.* } });
                has_error = true;
            }
        }
    }

    if (has_error) return error.AnalysisFail;
    return args;
}

fn compileInitialValue(allocator: std.mem.Allocator, value: config.ArgValueDoc) !recipe_ir.Value {
    return switch (value) {
        .scalar => |scalar| compileScalarValue(allocator, scalar),
        .list => |items| blk: {
            const compiled = try allocator.alloc(recipe_ir.Value, items.len);
            for (items, 0..) |item, idx| {
                compiled[idx] = try compileScalarValue(allocator, item);
            }
            break :blk .{ .list = compiled };
        },
    };
}

fn compileScalarValue(allocator: std.mem.Allocator, value: config.ArgScalarDoc) !recipe_ir.Value {
    return switch (value) {
        .string => |text| .{ .string = try allocator.dupe(u8, text) },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}

fn compileArg(
    allocator: std.mem.Allocator,
    doc_arg: config.ArgValueDoc,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !recipe_ir.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try compileArgScalar(allocator, scalar, slot_map, diag, context) },
        .list => |items| blk: {
            const out = try allocator.alloc(expr.Expression, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileArgScalar(allocator, item, slot_map, diag, context);
            }
            break :blk .{ .list = out };
        },
    };
}

fn compileArgDefault(
    allocator: std.mem.Allocator,
    source_args: ?std.StringHashMap(adapter_schema.ArgSpec),
    arg_name: []const u8,
) !?recipe_ir.StepArg {
    const args_map = source_args orelse return null;
    const spec = args_map.get(arg_name) orelse return null;
    return switch (spec) {
        .string => null,
        .object => |obj| if (obj.default) |default_value|
            try compileAdapterDefaultArg(allocator, default_value)
        else
            null,
    };
}

fn compileAdapterDefaultArg(
    allocator: std.mem.Allocator,
    value: adapter_schema.ArgDefault,
) !recipe_ir.StepArg {
    return switch (value) {
        .scalar => |scalar| .{ .scalar = try compileAdapterDefaultScalar(allocator, scalar) },
        .list => |items| blk: {
            const out = try allocator.alloc(expr.Expression, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileAdapterDefaultScalar(allocator, item);
            }
            break :blk .{ .list = out };
        },
    };
}

fn compileAdapterDefaultScalar(
    allocator: std.mem.Allocator,
    value: adapter_schema.ArgDefaultScalar,
) !expr.Expression {
    return switch (value) {
        .string => |text| makeLiteralExpr(allocator, .{ .push_string = try allocator.dupe(u8, text) }),
        .int => |n| makeLiteralExpr(allocator, .{ .push_int = n }),
        .float => |n| makeLiteralExpr(allocator, .{ .push_float = n }),
        .bool => |b| makeLiteralExpr(allocator, .{ .push_bool = b }),
    };
}

fn makeEmptyStringArg(allocator: std.mem.Allocator) !recipe_ir.StepArg {
    return .{ .scalar = try makeLiteralExpr(allocator, .{ .push_string = try allocator.dupe(u8, "") }) };
}

fn compileArgScalar(
    allocator: std.mem.Allocator,
    value: config.ArgScalarDoc,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !expr.Expression {
    return switch (value) {
        .string => |text| {
            if (std.mem.indexOf(u8, text, "${") != null) {
                return slot_map.compileExpr(diag, context, text, .argument);
            }
            return makeLiteralExpr(allocator, .{ .push_string = try allocator.dupe(u8, text) });
        },
        .int => |n| makeLiteralExpr(allocator, .{ .push_int = n }),
        .float => |n| makeLiteralExpr(allocator, .{ .push_float = n }),
        .bool => |b| makeLiteralExpr(allocator, .{ .push_bool = b }),
    };
}

fn makeLiteralExpr(allocator: std.mem.Allocator, op: expr.Op) !expr.Expression {
    const ops = try allocator.alloc(expr.Op, 1);
    ops[0] = op;
    return .{ .ops = ops };
}

fn findArgEntryIndex(arg_entries: []const ArgBuildEntry, name: []const u8) ?usize {
    for (arg_entries, 0..) |arg_entry, idx| {
        if (std.mem.eql(u8, arg_entry.name, name)) return idx;
    }
    return null;
}

test "load recipe and adapters" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r1_set.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const instrument = compiled.instruments.getPtr("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.eql(u8, instrument.resource, "USB0::1::INSTR"));
    try std.testing.expect(std.mem.eql(u8, instrument.adapter_name, "psu.yaml"));
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());

    const command = instrument.commands.get("set") orelse return error.TestUnexpectedResult;
    try std.testing.expect(command.instrument == instrument);
    try std.testing.expect(command.response == null);
    try std.testing.expectEqual(@as(usize, 1), command.args.len);
    try std.testing.expect(std.mem.eql(u8, command.args[0].name, "voltage"));

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    const task0_steps = compiled.tasks[0].steps();
    try std.testing.expectEqual(@as(usize, 1), task0_steps.len);
    const step0 = task0_steps[0].action.instrument_call;
    try std.testing.expect(std.mem.eql(u8, step0.call, "set"));
    try std.testing.expect(step0.command == command);

    const voltage = step0.args[command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_string => |s| try std.testing.expectEqualStrings("5", s),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "parse durations and stop conditions" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r2_stop_when.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\stop_when: "$ELAPSED_MS >= 2000 || $ITER >= 3"
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r2_stop_when.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const step_args = compiled.tasks[0].steps()[0].action.instrument_call;
    const voltage = step_args.args[step_args.command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_string => |s| try std.testing.expectEqualStrings("5", s),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }

    try std.testing.expect(compiled.stop_when != null);
}

test "precompile preserves initial variables" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/initial_vars.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v_set: 1.0
        \\  name: scan
        \\tasks: []
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/initial_vars.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.initial_values.len);
    var found_float = false;
    var found_string = false;
    for (compiled.initial_values) |val| {
        switch (val) {
            .float => |number| {
                try std.testing.expectEqual(@as(f64, 1.0), number);
                found_float = true;
            },
            .string => |text| {
                try std.testing.expectEqualStrings("scan", text);
                found_string = true;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(found_float);
    try std.testing.expect(found_string);
}

test "precompile estimates iterations for run-once recipes" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: V
    );
    try workspace.writeFile("recipes/run_once.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args: {}
        \\  - steps:
        \\      - call: d1.set
        \\        args: {}
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/run_once.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    // No expected_iterations in recipe, so null.
    try std.testing.expectEqual(@as(?u64, null), compiled.expected_iterations);
}

test "precompile preserves typed literal step arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/cfg.yaml",
        \\metadata: {}
        \\commands:
        \\  configure:
        \\    write: "CONF {count} {voltage} {enabled} {channels} {mirror}"
    );
    try workspace.writeFile("recipes/typed_args.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: cfg.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  target: mir
        \\tasks:
        \\  - steps:
        \\      - call: d1.configure
        \\        args:
        \\          count: 5
        \\          voltage: 1.25
        \\          enabled: true
        \\          channels: [1, 2]
        \\          mirror: "${target}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/typed_args.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const args = compiled.tasks[0].steps()[0].action.instrument_call.args;

    const command = compiled.tasks[0].steps()[0].action.instrument_call.command;

    const count = args[command.argIndex("count").?];
    switch (count) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_int => |n| try std.testing.expectEqual(@as(i64, 5), n),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }

    const voltage = args[command.argIndex("voltage").?];
    switch (voltage) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_float => |n| try std.testing.expectApproxEqAbs(@as(f64, 1.25), n, 1e-9),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }

    const enabled = args[command.argIndex("enabled").?];
    switch (enabled) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_bool => |b| try std.testing.expect(b),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }

    const channels = args[command.argIndex("channels").?];
    switch (channels) {
        .scalar => return error.TestUnexpectedResult,
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len);
            try std.testing.expectEqual(@as(usize, 1), items[0].ops.len);
            switch (items[0].ops[0]) {
                .push_int => |n| try std.testing.expectEqual(@as(i64, 1), n),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expectEqual(@as(usize, 1), items[1].ops.len);
            switch (items[1].ops[0]) {
                .push_int => |n| try std.testing.expectEqual(@as(i64, 2), n),
                else => return error.TestUnexpectedResult,
            }
        },
    }

    const mirror = args[command.argIndex("mirror").?];
    switch (mirror) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .load_var => |binding| switch (binding) {
                    .slot => |slot| try std.testing.expect(slot < compiled.initial_values.len),
                    .builtin => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }
}

const vendor_psu_adapter =
    \\metadata: {}
    \\commands:
    \\  set_voltage:
    \\    write: "VOLT {voltage},(@{channels})"
;

test "precompile rejects duplicate instrument in parallel block" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage}"
        \\  output_on:
        \\    write: "OUTP ON"
    );
    try workspace.writeFile("recipes/duplicate_parallel_instrument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - steps:
        \\      - parallel:
        \\          - call: d1.set_voltage
        \\            args:
        \\              voltage: 5
        \\          - call: d1.output_on
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/duplicate_parallel_instrument.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    _ = precompilePath(gpa, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "parallel steps cannot use instrument 'd1' more than once"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile stores only referenced commands" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage}"
        \\  output_on:
        \\    write: "OUTP ON"
    );
    try workspace.writeFile("recipes/r1_set_voltage.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r1_set_voltage.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const instrument = compiled.instruments.get("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());
    try std.testing.expect(instrument.commands.contains("set_voltage"));
    try std.testing.expect(!instrument.commands.contains("output_on"));
}

test "precompile rejects missing instrument references" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_instrument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars: {}
        \\tasks:
        \\  - steps:
        \\      - call: missing.set_voltage
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_instrument.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile validates command arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
    );
    try workspace.writeFile("recipes/unexpected_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels: [1]
        \\          channel: 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const missing_argument_path = try workspace.realpathAlloc("recipes/missing_argument.yaml");
    defer gpa.free(missing_argument_path);
    const unexpected_argument_path = try workspace.realpathAlloc("recipes/unexpected_argument.yaml");
    defer gpa.free(unexpected_argument_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, missing_argument_path, dir, null));
    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, unexpected_argument_path, dir, null));
}

test "precompile allows omitted optional group arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage}[,(@{channels})]"
    );
    try workspace.writeFile("recipes/r.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 1.0
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const call = compiled.tasks[0].steps()[0].action.instrument_call;
    const command = call.command;
    const channels_idx = command.argIndex("channels") orelse return error.TestUnexpectedResult;
    try std.testing.expect(command.args[channels_idx].is_optional);

    switch (call.args[channels_idx]) {
        .scalar => |e| switch (e.ops[0]) {
            .push_string => |s| try std.testing.expectEqualStrings("", s),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompile uses adapter argument defaults for omitted arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/switch.yaml",
        \\metadata: {}
        \\commands:
        \\  select_channel:
        \\    write: "INST {channel}"
        \\    args:
        \\      channel:
        \\        type: string
        \\        default: "1"
    );
    try workspace.writeFile("recipes/r.yaml",
        \\instruments:
        \\  sw:
        \\    adapter: switch.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: sw.select_channel
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const call = compiled.tasks[0].steps()[0].action.instrument_call;
    const channel_idx = call.command.argIndex("channel") orelse return error.TestUnexpectedResult;
    try std.testing.expect(!call.command.args[channel_idx].is_optional);

    switch (call.args[channel_idx]) {
        .scalar => |e| switch (e.ops[0]) {
            .push_string => |s| try std.testing.expectEqualStrings("1", s),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompiled command renders via helper" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();
    var diags: diagnostic.Diagnostics = .init(gpa, "<test>");
    defer diags.deinit();

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    try std.testing.expect(compiled.instrument == &instrument);
    try std.testing.expectEqual(@as(usize, 1), compiled.args.len);
    try std.testing.expectEqualStrings("voltage", compiled.args[0].name);

    const args = [_]recipe_ir.RenderValue{
        .{ .scalar = .{ .float = 3.3 } },
    };

    var stack_buf: [32]u8 = undefined;
    var rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, null);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("VOLT 3.3\n", rendered.bytes);
    try std.testing.expect(rendered.owned == null);
}

test "precompiled command render falls back to heap when suffix leaves too little stack space" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();
    var diags: diagnostic.Diagnostics = .init(gpa, "<test>");
    defer diags.deinit();

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\r\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.RenderValue{
        .{ .scalar = .{ .string = "1234567890" } },
    };

    var stack_buf: [8]u8 = undefined;
    var rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, null);
    defer rendered.deinit(gpa);

    try std.testing.expect(rendered.owned != null);
    try std.testing.expectEqualStrings("VOLT 1234567890\r\n", rendered.bytes);
}

test "float_precision controls decimal places in rendered command" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();
    var diags: diagnostic.Diagnostics = .init(gpa, "<test>");
    defer diags.deinit();

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.RenderValue{
        .{ .scalar = .{ .float = 3.14159265 } },
    };

    var stack_buf: [64]u8 = undefined;

    // With precision 2: "VOLT 3.14\n"
    var r2 = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, 2);
    defer r2.deinit(gpa);
    try std.testing.expectEqualStrings("VOLT 3.14\n", r2.bytes);

    // With precision 0: "VOLT 3\n"
    var r0 = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, 0);
    defer r0.deinit(gpa);
    try std.testing.expectEqualStrings("VOLT 3\n", r0.bytes);

    // Without precision (null): shortest representation
    var rn = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, null);
    defer rn.deinit(gpa);
    try std.testing.expectEqualStrings("VOLT 3.14159265\n", rn.bytes);
}

test "precompiled command applies bool format from adapter defaults" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();
    var diags: diagnostic.Diagnostics = .init(gpa, "<test>");
    defer diags.deinit();

    const source = try Adapter.Command.parse(alloc, "OUTP {state}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, source, &instrument, .{ .true = "ON", .false = "OFF" }, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.RenderValue{
        .{ .scalar = .{ .bool = true } },
    };

    var stack_buf: [32]u8 = undefined;
    var rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, null);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("OUTP ON\n", rendered.bytes);
}

test "precompile rejects partial bool arg map" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\instrument: {}
        \\commands:
        \\  output:
        \\    write: "OUTP {state}"
        \\    args:
        \\      state:
        \\        type: bool
        \\        true: "ON"
    );

    try workspace.writeFile("recipes/r.yaml",
        \\instruments:
        \\  psu:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: psu.output
        \\        args:
        \\          state: true
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile diagnostic includes step context" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_command.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.missing
        \\        args:
        \\          voltage: "1.0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/missing_command.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    _ = precompilePath(gpa, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);

        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "task 0 step 0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "instrument=d1"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "adapter=psu0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "command=missing"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "command not found"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile compute step" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 0
        \\  doubled: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "5"
        \\          channels: "1"
        \\        assign: v
        \\      - compute: "${v} * 2"
        \\        assign: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 2), compiled.tasks[0].steps().len);

    // First step: instrument call
    switch (compiled.tasks[0].steps()[0].action) {
        .instrument_call => |ic| try std.testing.expectEqualStrings("set_voltage", ic.call),
        else => return error.TestUnexpectedResult,
    }

    // Second step: compute
    switch (compiled.tasks[0].steps()[1].action) {
        .compute => |comp| {
            try std.testing.expect(comp.save_column != null);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "precompile compute step rejects missing assign" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute_no_save.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - compute: "1 + 2"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute_no_save.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile step with if guard" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/if_guard.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  power: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "5"
        \\          channels: "1"
        \\        if: "${power} > 100"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/if_guard.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expect(compiled.tasks[0].steps()[0].@"if" != null);
}

test "precompile rejects invalid step (neither call nor compute)" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/invalid_step.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - assign: orphan
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/invalid_step.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile rejects record with unknown assign variable" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/bad_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: [voltage, nonexistent]
        \\vars:
        \\  voltage: 0
        \\  nonexistent: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels: [1, 2]
        \\        assign: voltage
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/bad_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile accepts valid record subset" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_ok.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: [voltage]
        \\vars:
        \\  voltage: 0
        \\  doubled: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels: [1, 2]
        \\        assign: voltage
        \\      - compute: "${voltage} * 2"
        \\        assign: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_ok.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    switch (compiled.pipeline.record.?) {
        .explicit => |columns| {
            try std.testing.expectEqual(@as(usize, 1), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
        },
        .all => return error.TestUnexpectedResult,
    }
}

test "precompile diagnostic for missing pipeline" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_pipeline.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 1.0
        \\          channels: [1]
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_pipeline.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    _ = precompilePath(gpa, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);

        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'pipeline'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile diagnostic for missing record" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline: {}
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 1.0
        \\          channels: [1]
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    _ = precompilePath(gpa, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);

        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'record'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile expands record all into explicit assign list" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_all.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\  doubled: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 5
        \\          channels: [1]
        \\        assign: voltage
        \\      - compute: "${voltage} * 2"
        \\        assign: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_all.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const record = compiled.pipeline.record orelse return error.TestUnexpectedResult;
    switch (record) {
        .explicit => |columns| {
            try std.testing.expectEqual(@as(usize, 2), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
            try std.testing.expectEqualStrings("doubled", columns[1]);
        },
        .all => return error.TestUnexpectedResult,
    }
}

test "precompile rejects undeclared variable use" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage}"
    );
    try workspace.writeFile("recipes/undeclared.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 1
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
        \\        assign: undeclared_var
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile rejects undeclared variable in expression" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/undeclared_expr.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 1
        \\tasks:
        \\  - steps:
        \\      - compute: "${v} + ${x}"
        \\        assign: v
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/undeclared_expr.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile rejects variable shadowing builtin" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/shadow_builtin.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  $ITER: 0
        \\tasks:
        \\  - steps:
        \\      - compute: "1 + 1"
        \\        assign: $ITER
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/shadow_builtin.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile sequential task" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/sequential.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/sequential.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expect(compiled.tasks[0] == .sequential);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps().len);
}

test "precompile loop task with while" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/loop_task.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
        \\    while: "$ITER < 10"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/loop_task.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expect(compiled.tasks[0] == .loop);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps().len);
}

test "precompile conditional task with if" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/conditional_task.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 5
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
        \\    if: "${voltage} > 0"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/conditional_task.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expect(compiled.tasks[0] == .conditional);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps().len);
}

test "precompile sleep step" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/sleep_step.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 0
        \\tasks:
        \\  - steps:
        \\      - compute: "1 + 2"
        \\        assign: v
        \\      - sleep_ms: 100
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/sleep_step.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const task_steps = compiled.tasks[0].steps();
    try std.testing.expectEqual(@as(usize, 2), task_steps.len);
    switch (task_steps[1].action) {
        .sleep => |s| try std.testing.expectEqual(@as(u64, 100), s.duration_ms),
        else => return error.TestUnexpectedResult,
    }
}

test "precompile recipe with list variable" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/list_vars.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  idx: 0
        \\  voltages:
        \\    - 1.5
        \\    - 3.0
        \\    - 4.5
        \\tasks:
        \\  - steps:
        \\      - compute: "${voltages}[${idx}]"
        \\        assign: idx
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/list_vars.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    // Verify the list variable was parsed as initial values.
    // Slot 0 = idx (scalar), Slot 1 = voltages (list).
    const initial = compiled.initial_values;
    try std.testing.expectEqual(@as(usize, 2), initial.len);

    // idx = 0 (int or float)
    const idx_val = initial[0];
    switch (idx_val) {
        .int => |v| try std.testing.expectEqual(@as(i64, 0), v),
        .float => |v| try std.testing.expectApproxEqAbs(@as(f64, 0.0), v, 1e-9),
        else => return error.TestUnexpectedResult,
    }

    // voltages = [1.5, 3.0, 4.5]
    const list_val = initial[1];
    switch (list_val) {
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 3), items.len);
            switch (items[0]) {
                .float => |v| try std.testing.expectApproxEqAbs(@as(f64, 1.5), v, 1e-9),
                else => return error.TestUnexpectedResult,
            }
            switch (items[2]) {
                .float => |v| try std.testing.expectApproxEqAbs(@as(f64, 4.5), v, 1e-9),
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "precompile const-folds join() in step args" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage},(@{channels})"
    );
    try workspace.writeFile("recipes/const_join.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\consts:
        \\  channels:
        \\    - 1
        \\    - 2
        \\    - 3
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "5.0"
        \\          channels: 'join(${channels}, ",")'
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/const_join.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const step = compiled.tasks[0].steps()[0].action.instrument_call;
    const channels_arg = step.args[step.command.argIndex("channels").?];
    // The join expression should be const-folded to a literal string "1,2,3".
    switch (channels_arg) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_string => |s| try std.testing.expectEqualStrings("1,2,3", s),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompile const scalar expression folding" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage}"
    );
    try workspace.writeFile("recipes/const_arith.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\consts:
        \\  base_v: 3.0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "${base_v} * 2"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/const_arith.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const step = compiled.tasks[0].steps()[0].action.instrument_call;
    const voltage_arg = step.args[step.command.argIndex("voltage").?];
    switch (voltage_arg) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 1), e.ops.len);
            switch (e.ops[0]) {
                .push_float => |f| try std.testing.expectApproxEqAbs(@as(f64, 6.0), f, 1e-9),
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompile rejects assign to const" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage}"
        \\    response: float
    );
    try workspace.writeFile("recipes/assign_const.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\consts:
        \\  fixed: 5.0
        \\vars:
        \\  result: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "1.0"
        \\        assign: fixed
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/assign_const.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile rejects duplicate const and var names" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/dup.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\consts:
        \\  x: 1
        \\vars:
        \\  x: 0
        \\tasks: []
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/dup.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, recipe_path, dir, null));
}

test "precompile does not fold expressions referencing runtime vars" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage}"
    );
    try workspace.writeFile("recipes/no_fold.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\vars:
        \\  v: 1.0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "${v} * 2"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_fold.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const step = compiled.tasks[0].steps()[0].action.instrument_call;
    const voltage_arg = step.args[step.command.argIndex("voltage").?];
    // Expression references a runtime var, so it should NOT be const-folded;
    // it is compiled as a proper expression with load_var + arithmetic ops.
    switch (voltage_arg) {
        .scalar => |e| {
            try std.testing.expect(e.ops.len > 1);
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompile partially folds const prefix with runtime var" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage}"
    );
    try workspace.writeFile("recipes/partial_fold.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\consts:
        \\  base: 1
        \\vars:
        \\  v: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "${base} + 2 + ${v}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/partial_fold.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const step = compiled.tasks[0].steps()[0].action.instrument_call;
    const voltage_arg = step.args[step.command.argIndex("voltage").?];
    switch (voltage_arg) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 3), e.ops.len);
            switch (e.ops[0]) {
                .push_int => |value| try std.testing.expectEqual(@as(i64, 3), value),
                else => return error.TestUnexpectedResult,
            }
            switch (e.ops[1]) {
                .load_var => |binding| switch (binding) {
                    .slot => |slot| try std.testing.expectEqual(@as(usize, 0), slot),
                    .builtin => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(e.ops[2] == .add);
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "precompile reassociates builtin plus trailing constants" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/reassoc.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  out: 0
        \\tasks:
        \\  - steps:
        \\      - compute: "$ITER + 1 + 2"
        \\        assign: out
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/reassoc.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    switch (compiled.tasks[0].steps()[0].action) {
        .compute => |comp| {
            try std.testing.expectEqual(@as(usize, 3), comp.expression.ops.len);
            switch (comp.expression.ops[0]) {
                .load_var => |binding| switch (binding) {
                    .builtin => |builtin| try std.testing.expect(builtin == .iter),
                    .slot => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
            switch (comp.expression.ops[1]) {
                .push_int => |value| try std.testing.expectEqual(@as(i64, 3), value),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(comp.expression.ops[2] == .add);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "precompile simplifies logical rhs constant" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/logical_simplify.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: all
        \\vars:
        \\  a: 0
        \\stop_when: "${a} && (1 + 2)"
        \\tasks: []
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/logical_simplify.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const stop_when = compiled.stop_when orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), stop_when.ops.len);
    switch (stop_when.ops[0]) {
        .load_var => |binding| switch (binding) {
            .slot => |slot| try std.testing.expectEqual(@as(usize, 0), slot),
            .builtin => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(stop_when.ops[1] == .to_bool);
}
