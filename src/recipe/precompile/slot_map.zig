const std = @import("std");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");

pub const SlotTable = std.StringArrayHashMapUnmanaged(void);
pub const DiagnosticContext = diagnostic.Context;

pub const ExprSourceKind = enum {
    expression,
    argument,

    pub fn sourceKind(self: ExprSourceKind) diagnostic.SourceKind {
        return switch (self) {
            .expression => .expression,
            .argument => .argument_expression,
        };
    }
};

pub const SlotMap = struct {
    slots: SlotTable,
    initial_values: []const recipe_ir.Value,
    list_slot_capacities: []usize,
    const_count: usize,
    /// Allocator that owns only compile-time slot-table backing and expression
    /// scratch arenas. Returned recipe data must not reference it.
    scratch_alloc: std.mem.Allocator,

    pub fn deinit(self: *SlotMap) void {
        self.slots.deinit(self.scratch_alloc);
        self.* = undefined;
    }

    /// Look up a name and return the runtime binding (var slot remapped)
    /// or the const value if the name refers to a const.
    pub const ResolvedName = union(enum) {
        binding: expr.VariableBinding,
        const_value: recipe_ir.Value,
    };

    pub fn resolveName(self: *const SlotMap, name: []const u8) ?ResolvedName {
        if (expr.resolveBuiltin(name)) |b| return .{ .binding = b };
        const slot = self.slots.getIndex(name) orelse return null;
        if (slot < self.const_count) return .{ .const_value = self.initial_values[slot] };
        return .{ .binding = .{ .slot = slot - self.const_count } };
    }

    /// Returns only the var portion of initial_values (excluding consts).
    pub fn varInitialValues(self: *const SlotMap) []const recipe_ir.Value {
        return self.initial_values[self.const_count..];
    }

    pub fn recordListResponseCapacity(self: *SlotMap, slot_idx: usize, response: recipe_ir.ResponseSpec) void {
        const list = switch (response) {
            .scalar, .object => return,
            .list => |list| list,
        };
        switch (self.initial_values[self.const_count + slot_idx]) {
            .list => {
                self.list_slot_capacities[slot_idx] = @max(self.list_slot_capacities[slot_idx], list.items.len);
            },
            else => {},
        }
    }

    /// Returns recipe var names in runtime slot order.
    pub fn varNames(self: *const SlotMap) []const []const u8 {
        return self.slots.keys()[self.const_count..];
    }

    /// Validate that `name` refers to a mutable var and return its remapped slot index.
    pub fn varSlotIndex(self: *const SlotMap, diag: diagnostic.Reporter, context: DiagnosticContext, name: []const u8) !usize {
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

    pub fn resolveConstValue(value: recipe_ir.Value) expr.ResolvedValue {
        return switch (value) {
            .float => |f| .{ .float = f },
            .int => |i| .{ .int = i },
            .bool => |b| .{ .bool = b },
            .string => |s| .{ .string = s.items() },
            .list => |items| blk: {
                const slice = items.items();
                break :blk .{ .list = .{
                    .len = slice.len,
                    .ctx = @ptrCast(slice.ptr),
                    .at_fn = constListAt,
                } };
            },
        };
    }

    fn constListAt(ctx: *const anyopaque, index: usize) ?expr.ResolvedValue {
        const items: [*]const recipe_ir.Value = @ptrCast(@alignCast(ctx));
        return resolveConstValue(items[index]);
    }
};

pub const SlotBindingRemapper = struct {
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

/// Validate consts/vars, build the merged slot map (consts first, then vars),
/// compile initial values, and create the compile-time const resolver.
/// `scratch_alloc` owns the compile-time slot table backing; `arena` owns
/// initial values that are returned as part of `PrecompiledRecipe`.
pub fn buildSlotMap(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
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
    const initial_values = try arena.alloc(recipe_ir.Value, const_keys.len + var_keys.len);
    for (const_vals, 0..) |value, idx| {
        initial_values[idx] = try compileInitialValue(arena, value);
    }
    for (var_vals, 0..) |value, idx| {
        initial_values[const_keys.len + idx] = try compileInitialValue(arena, value);
    }

    const list_slot_capacities = try arena.alloc(usize, var_keys.len);
    for (initial_values[const_keys.len..], 0..) |value, idx| {
        list_slot_capacities[idx] = switch (value) {
            .list => |items| items.len(),
            else => 0,
        };
    }

    // Build the key-only slot map: consts first, then vars.
    var all_slots: SlotTable = .empty;
    errdefer all_slots.deinit(scratch_alloc);
    for (const_keys) |name| try all_slots.put(scratch_alloc, name, {});
    for (var_keys) |name| try all_slots.put(scratch_alloc, name, {});

    return .{
        .slots = all_slots,
        .initial_values = initial_values,
        .list_slot_capacities = list_slot_capacities,
        .const_count = const_keys.len,
        .scratch_alloc = scratch_alloc,
    };
}

pub fn compileInitialValue(arena: std.mem.Allocator, value: config.ArgValueDoc) !recipe_ir.Value {
    return switch (value) {
        .scalar => |scalar| compileScalarValue(arena, scalar),
        .list => |items| blk: {
            const compiled = try arena.alloc(recipe_ir.Value, items.len);
            for (items, 0..) |item, idx| {
                compiled[idx] = try compileScalarValue(arena, item);
            }
            break :blk .{ .list = recipe_ir.Value.List.borrow(compiled) };
        },
    };
}

pub fn compileScalarValue(arena: std.mem.Allocator, value: config.ArgScalarDoc) !recipe_ir.Value {
    return switch (value) {
        .string => |text| .{ .string = recipe_ir.Value.String.borrow(try arena.dupe(u8, text)) },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}
