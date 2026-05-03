const std = @import("std");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");
const slot_map_mod = @import("slot_map.zig");

const SlotMap = slot_map_mod.SlotMap;
const DiagnosticContext = slot_map_mod.DiagnosticContext;

pub const PipelineResolution = struct {
    pipeline: recipe_ir.PipelineConfig,
    record_bindings: []const expr.VariableBinding,
    echo_bindings: []const expr.VariableBinding,
};

pub const ColumnResolution = struct {
    columns: []const []const u8,
    bindings: []const expr.VariableBinding,
};

pub fn resolvePipelineConfig(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
) !PipelineResolution {
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

    var pipeline = blk: {
        var _pipeline_cfg = pipeline_cfg;
        _pipeline_cfg.record = null;
        _pipeline_cfg.echo = null;
        break :blk try _pipeline_cfg.clone(arena);
    };

    const record_cfg = pipeline_cfg.record orelse recipe_ir.ColumnConfig{ .explicit = empty_columns };
    const record = try resolveColumnConfig(scratch_alloc, arena, record_cfg, slot_map, diag);
    pipeline.record = .{ .explicit = record.columns };

    const echo_bindings: []const expr.VariableBinding = if (pipeline_cfg.echo) |echo_cfg| blk: {
        const echo = try resolveColumnConfig(scratch_alloc, arena, echo_cfg, slot_map, diag);
        pipeline.echo = .{ .explicit = echo.columns };
        break :blk echo.bindings;
    } else &.{};

    if (has_error) return error.AnalysisFail;
    return .{
        .pipeline = pipeline,
        .record_bindings = record.bindings,
        .echo_bindings = echo_bindings,
    };
}

fn resolveAllColumnSources(arena: std.mem.Allocator, slot_map: *const SlotMap) !ColumnResolution {
    const var_names = slot_map.varNames();
    const total = var_names.len + expr.BuiltinVar.vars.len;
    const columns = try arena.alloc([]const u8, total);
    const bindings = try arena.alloc(expr.VariableBinding, total);

    var out_idx: usize = 0;
    for (var_names, 0..) |name, slot| {
        columns[out_idx] = try arena.dupe(u8, name);
        bindings[out_idx] = .{ .slot = slot };
        out_idx += 1;
    }
    inline for (expr.BuiltinVar.vars) |builtin| {
        columns[out_idx] = try arena.dupe(u8, builtin.name());
        bindings[out_idx] = .{ .builtin = builtin };
        out_idx += 1;
    }

    return .{
        .columns = columns,
        .bindings = bindings,
    };
}

pub fn resolveColumnSource(
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    name: []const u8,
) diagnostic.Error!expr.VariableBinding {
    const resolved = slot_map.resolveName(name) orelse {
        try diag.add(.fatal, null, .{ .unknown_variable = .{ .variable = name } });
        return error.AnalysisFail;
    };
    return switch (resolved) {
        .binding => |binding| binding,
        .const_value => {
            try diag.add(.fatal, null, .{ .record_const_not_recordable = .{ .variable = name } });
            return error.AnalysisFail;
        },
    };
}

pub fn validatePipelineConfig(cfg: *const recipe_ir.PipelineConfig, diag: diagnostic.Reporter) !void {
    var has_error = false;
    if (cfg.buffer_size) |size| {
        if (size == 0) has_error = true;
    }
    if (cfg.warn_usage_percent) |percent| {
        if (percent == 0 or percent > 100) has_error = true;
    }
    if (cfg.api_port) |port| {
        if (port == 0) has_error = true;
    }
    if (!has_error) return;

    try diag.add(.fatal, null, .{ .invalid_pipeline_config = {} });
    return error.AnalysisFail;
}

fn resolveColumnConfig(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    cfg: recipe_ir.ColumnConfig,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
) !ColumnResolution {
    switch (cfg) {
        .all => return resolveAllColumnSources(arena, slot_map),
        .explicit => |columns| {
            var seen: std.StringArrayHashMapUnmanaged(void) = .empty;
            defer seen.deinit(scratch_alloc);
            var unique_columns: std.ArrayList([]const u8) = .empty;
            var unique_bindings: std.ArrayList(expr.VariableBinding) = .empty;
            var has_error = false;

            for (columns) |name| {
                const column_context: DiagnosticContext = .{ .variable_name = name };
                const column_reporter = diag.withContext(column_context);
                if (seen.getIndex(name) != null) {
                    try column_reporter.warn(null, .{ .duplicate_record_column = .{ .column = name } });
                    continue;
                }
                try seen.put(scratch_alloc, name, {});

                const binding = resolveColumnSource(slot_map, column_reporter, name) catch |err| switch (err) {
                    error.AnalysisFail => {
                        has_error = true;
                        continue;
                    },
                    else => return err,
                };
                try unique_columns.append(arena, try arena.dupe(u8, name));
                try unique_bindings.append(arena, binding);
            }
            if (has_error) return error.AnalysisFail;
            return .{
                .columns = try unique_columns.toOwnedSlice(arena),
                .bindings = try unique_bindings.toOwnedSlice(arena),
            };
        },
    }
}
