const std = @import("std");
const adapter_schema = @import("../../adapter/schema.zig");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");
const slot_map_mod = @import("slot_map.zig");
const adapter_mod = @import("adapter.zig");
const expr_compile = @import("expr_compile.zig");

const SlotMap = slot_map_mod.SlotMap;
const DiagnosticContext = slot_map_mod.DiagnosticContext;
pub const ArgBuildEntry = adapter_mod.ArgBuildEntry;

pub fn compileStepArgs(
    arena: std.mem.Allocator,
    command: *const recipe_ir.PrecompiledCommand,
    doc_args: ?std.StringHashMap(config.ArgValueDoc),
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) ![]recipe_ir.StepArg {
    const args = try arena.alloc(recipe_ir.StepArg, command.args.len);
    var has_error = false;

    for (command.args, 0..) |arg, idx| {
        if (doc_args) |wrapper| {
            if (wrapper.get(arg.name)) |doc_arg| {
                var arg_context = base_context;
                arg_context.argument_name = arg.name;
                validateOptionDocArg(arg.format, doc_arg, diag, arg_context) catch |err| switch (err) {
                    error.AnalysisFail => has_error = true,
                    else => return err,
                };
                args[idx] = compileArg(arena, doc_arg, slot_map, diag, arg_context) catch |err| blk: {
                    switch (err) {
                        error.AnalysisFail => {
                            has_error = true;
                            break :blk try makeEmptyStringArg(arena);
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
            args[idx] = try makeEmptyStringArg(arena);
            continue;
        }

        var arg_context = base_context;
        arg_context.argument_name = arg.name;
        try diag.withContext(arg_context).add(.fatal, null, .{ .missing_command_argument = .{ .argument = arg.name } });
        has_error = true;
        args[idx] = try makeEmptyStringArg(arena);
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

pub fn compileArg(
    arena: std.mem.Allocator,
    doc_arg: config.ArgValueDoc,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !recipe_ir.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try compileArgScalar(arena, scalar, slot_map, diag, context) },
        .list => |items| blk: {
            const out = try arena.alloc(expr.Expression, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileArgScalar(arena, item, slot_map, diag, context);
            }
            break :blk .{ .list = out };
        },
    };
}

pub fn compileArgDefault(
    arena: std.mem.Allocator,
    source_args: ?std.StringHashMap(adapter_schema.ArgSpec),
    arg_name: []const u8,
) !?recipe_ir.StepArg {
    const args_map = source_args orelse return null;
    const spec = args_map.get(arg_name) orelse return null;
    return if (spec.default) |default_value|
        try compileAdapterDefaultArg(arena, default_value)
    else
        null;
}

pub fn compileAdapterDefaultArg(
    arena: std.mem.Allocator,
    value: adapter_schema.ArgDefault,
) !recipe_ir.StepArg {
    return switch (value) {
        .scalar => |scalar| .{ .scalar = try compileAdapterDefaultScalar(arena, scalar) },
        .list => |items| blk: {
            const out = try arena.alloc(expr.Expression, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileAdapterDefaultScalar(arena, item);
            }
            break :blk .{ .list = out };
        },
    };
}

pub fn compileAdapterDefaultScalar(
    arena: std.mem.Allocator,
    value: adapter_schema.ArgDefaultScalar,
) !expr.Expression {
    return switch (value) {
        .string => |text| makeLiteralExpr(arena, .{ .push_string = try arena.dupe(u8, text) }),
        .int => |n| makeLiteralExpr(arena, .{ .push_int = n }),
        .float => |n| makeLiteralExpr(arena, .{ .push_float = n }),
        .bool => |b| makeLiteralExpr(arena, .{ .push_bool = b }),
    };
}

pub fn compileArgScalar(
    arena: std.mem.Allocator,
    value: config.ArgScalarDoc,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !expr.Expression {
    return switch (value) {
        .string => |text| {
            if (std.mem.indexOf(u8, text, "${") != null) {
                return expr_compile.compileExpr(slot_map, arena, diag, context, text, .argument);
            }
            return makeLiteralExpr(arena, .{ .push_string = try arena.dupe(u8, text) });
        },
        .int => |n| makeLiteralExpr(arena, .{ .push_int = n }),
        .float => |n| makeLiteralExpr(arena, .{ .push_float = n }),
        .bool => |b| makeLiteralExpr(arena, .{ .push_bool = b }),
    };
}

pub fn makeEmptyStringArg(arena: std.mem.Allocator) !recipe_ir.StepArg {
    return .{ .scalar = try makeLiteralExpr(arena, .{ .push_string = try arena.dupe(u8, "") }) };
}

pub fn makeLiteralExpr(arena: std.mem.Allocator, op: expr.Op) !expr.Expression {
    const ops = try arena.alloc(expr.Op, 1);
    ops[0] = op;
    return .{ .ops = ops };
}

pub fn findArgEntryIndex(arg_entries: []const ArgBuildEntry, name: []const u8) ?usize {
    return adapter_mod.findArgEntryIndex(arg_entries, name);
}

pub fn validateOptionDocArg(
    format: recipe_ir.ArgFormat,
    doc_arg: config.ArgValueDoc,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) diagnostic.Error!void {
    const options = format.option_values orelse return;
    switch (doc_arg) {
        .scalar => |scalar| try validateOptionDocScalar(options, scalar, diag, context),
        .list => |items| for (items) |item| {
            try validateOptionDocScalar(options, item, diag, context);
        },
    }
}

pub fn validateOptionDocScalar(
    options: []const []const u8,
    scalar: config.ArgScalarDoc,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) diagnostic.Error!void {
    const text = switch (scalar) {
        .string => |s| s,
        else => return diag.withContext(context).fail(null, .{ .invalid_option_value = {} }),
    };
    if (std.mem.indexOf(u8, text, "${") != null) return;
    if (!adapter_mod.containsOptionValue(options, text)) {
        return diag.withContext(context).fail(null, .{ .invalid_option_value = {} });
    }
}

pub fn compileInitialValue(arena: std.mem.Allocator, value: config.ArgValueDoc) !recipe_ir.Value {
    return slot_map_mod.compileInitialValue(arena, value);
}

pub fn compileScalarValue(arena: std.mem.Allocator, value: config.ArgScalarDoc) !recipe_ir.Value {
    return slot_map_mod.compileScalarValue(arena, value);
}
