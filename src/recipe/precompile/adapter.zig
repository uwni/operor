const std = @import("std");
const Adapter = @import("../../adapter/Adapter.zig");
const template = @import("../../adapter/template.zig");
const adapter_schema = @import("../../adapter/schema.zig");
const parse_mod = @import("../../adapter/parse.zig");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const instrument_mod = @import("../../instrument.zig");
const slot_map_mod = @import("slot_map.zig");

const DiagnosticContext = slot_map_mod.DiagnosticContext;

pub const ArgBuildEntry = struct {
    name: []const u8,
    arg_type: []const u8,
    required: bool,
};

/// `cache_alloc` owns loaded adapter documents for the duration of precompile.
pub fn loadAdapters(
    cache_alloc: std.mem.Allocator,
    io: std.Io,
    recipe: *const config.RecipeConfig,
    adapter_dir: std.Io.Dir,
    diag: diagnostic.Reporter,
) !std.StringHashMap(Adapter) {
    var map: std.StringHashMap(Adapter) = .init(cache_alloc);
    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const cfg = entry.value_ptr.*;
        _ = try getOrParseAdapter(cache_alloc, io, &map, adapter_dir, entry.key_ptr.*, cfg.adapter, diag);
    }
    return map;
}

pub fn getOrParseAdapter(
    cache_alloc: std.mem.Allocator,
    io: std.Io,
    loaded_adapters: *std.StringHashMap(Adapter),
    adapter_dir: std.Io.Dir,
    instrument_name: []const u8,
    adapter_name: []const u8,
    diag: diagnostic.Reporter,
) !*const Adapter {
    if (loaded_adapters.getPtr(adapter_name)) |loaded| return loaded;

    const key = try cache_alloc.dupe(u8, adapter_name);

    var loaded = try parse_mod.parseAdapterInDir(cache_alloc, io, adapter_dir, adapter_name, diag.withContext(.{
        .instrument_name = instrument_name,
        .adapter_name = adapter_name,
    }));
    errdefer loaded.deinit();

    try loaded_adapters.put(key, loaded);
    return loaded_adapters.getPtr(adapter_name).?;
}

/// `scratch_alloc` owns temporary command compilation backing; `arena` owns the
/// cached command key, command object, and all data reachable from it.
pub fn getOrCompileCommand(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    instrument: *recipe_ir.PrecompiledInstrument,
    source: Adapter.Command,
    call: []const u8,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !*const recipe_ir.PrecompiledCommand {
    if (instrument.commands.get(call)) |command| return command;

    const key = try arena.dupe(u8, call);
    const compiled_value = try compileCommand(scratch_alloc, arena, source, instrument, adapter_bool_format, diag, context);

    const compiled = try arena.create(recipe_ir.PrecompiledCommand);
    compiled.* = compiled_value;

    try instrument.commands.put(key, compiled);
    return compiled;
}

/// `scratch_alloc` owns temporary command-builder backing such as `arg_entries`.
/// `arena` owns returned command data referenced by `PrecompiledRecipe`.
pub fn compileCommand(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    source: Adapter.Command,
    instrument: *const recipe_ir.PrecompiledInstrument,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) !recipe_ir.PrecompiledCommand {
    var arg_entries: std.ArrayList(ArgBuildEntry) = .empty;
    defer arg_entries.deinit(scratch_alloc);
    const segments = try compileSegments(scratch_alloc, arena, source.template, &arg_entries, false, diag, base_context);

    const args = try arena.alloc(recipe_ir.CommandArg, arg_entries.items.len);
    for (arg_entries.items, 0..) |entry, idx| {
        var arg_context = base_context;
        arg_context.argument_name = entry.name;
        args[idx] = .{
            .name = entry.name,
            .is_optional = !entry.required,
            .default = try compileArgDefault(arena, source.args, entry.name),
            .format = try compileArgFormat(arena, source.args, entry.name, entry.arg_type, adapter_bool_format, diag, arg_context),
        };
    }

    return .{
        .instrument = instrument,
        .response = try cloneResponseSpec(arena, source.response),
        .segments = segments,
        .args = args,
    };
}

pub fn compileArgFormat(
    arena: std.mem.Allocator,
    source_args: ?std.StringHashMap(adapter_schema.ArgSpec),
    arg_name: []const u8,
    arg_type: []const u8,
    adapter_bool_format: ?adapter_schema.BoolFormat,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) !recipe_ir.ArgFormat {
    var format: recipe_ir.ArgFormat = .{};
    var bool_format = adapter_bool_format;
    const obj_spec: ?adapter_schema.ArgSpec = if (source_args) |args_map| args_map.get(arg_name) else null;

    if (obj_spec) |obj| {
        const has_bool_map = obj.true_text != null or obj.false_text != null;
        if (isArgType(arg_type, "bool") or has_bool_map) {
            if ((obj.true_text == null) != (obj.false_text == null)) {
                return diag.withContext(context).fail(null, .{ .partial_bool_map = {} });
            }
            if (obj.true_text) |t| {
                bool_format = .{ .true_text = t, .false_text = obj.false_text.? };
            }
        }
        if (obj.precision) |precision| {
            format.float_precision = precision;
        }
        if (obj.separator) |sep| {
            format.list_separator = try arena.dupe(u8, sep);
        }
        if (isArgType(arg_type, "option") or obj.options != null) {
            const options = obj.options orelse return diag.withContext(context).fail(null, .{ .missing_option_values = {} });
            if (options.len == 0) return diag.withContext(context).fail(null, .{ .missing_option_values = {} });
            if (obj.default) |default_value| {
                try validateAdapterDefaultOption(default_value, options, diag, context);
            }
            format.option_values = try cloneStringList(arena, options);
        }
    } else if (isArgType(arg_type, "option")) {
        return diag.withContext(context).fail(null, .{ .missing_option_values = {} });
    }

    if (bool_format) |bf| {
        format.bool_map = .{
            .true_text = try arena.dupe(u8, bf.true_text),
            .false_text = try arena.dupe(u8, bf.false_text),
        };
    }

    return format;
}

pub fn cloneResponseSpec(
    arena: std.mem.Allocator,
    source: ?adapter_schema.ResponseSpec,
) !?recipe_ir.ResponseSpec {
    const src = source orelse return null;
    return switch (src) {
        .scalar => |encoding| .{ .scalar = encoding },
        .list => |list| .{ .list = .{
            .separator = try arena.dupe(u8, list.separator),
            .items = try arena.dupe(adapter_schema.Encoding, list.items),
        } },
        .object => |obj| blk: {
            const segs = try arena.alloc(instrument_mod.ObjectSegment, obj.segments.len);
            for (obj.segments, 0..) |seg, i| {
                segs[i] = switch (seg) {
                    .literal => |lit| .{ .literal = try arena.dupe(u8, lit) },
                    .field => |f| .{ .field = .{
                        .name = try arena.dupe(u8, f.name),
                        .encoding = f.encoding,
                    } },
                };
            }
            break :blk .{ .object = .{ .segments = segs } };
        },
    };
}

pub fn cloneBoolTextMap(
    arena: std.mem.Allocator,
    source: ?adapter_schema.BoolFormat,
) !?recipe_ir.BoolTextMap {
    const src = source orelse return null;
    return .{
        .true_text = try arena.dupe(u8, src.true_text),
        .false_text = try arena.dupe(u8, src.false_text),
    };
}

pub fn cloneOptionalBytes(arena: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    if (bytes.len == 0) return "";
    return arena.dupe(u8, bytes);
}

pub fn cloneStringList(arena: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try arena.alloc([]const u8, values.len);
    for (values, 0..) |value, idx| {
        out[idx] = try arena.dupe(u8, value);
    }
    return out;
}

pub fn isArgType(arg_type: []const u8, expected: []const u8) bool {
    return std.mem.eql(u8, arg_type, expected);
}

pub fn isKnownArgType(arg_type: []const u8) bool {
    const known_types = [_][]const u8{ "string", "int", "float", "bool", "list", "option" };
    for (known_types) |known| {
        if (std.mem.eql(u8, arg_type, known)) return true;
    }
    return false;
}

pub fn validateAdapterDefaultOption(
    value: adapter_schema.ArgDefault,
    options: []const []const u8,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) diagnostic.Error!void {
    switch (value) {
        .scalar => |scalar| try validateAdapterDefaultOptionScalar(scalar, options, diag, context),
        .list => |items| for (items) |item| {
            try validateAdapterDefaultOptionScalar(item, options, diag, context);
        },
    }
}

pub fn validateAdapterDefaultOptionScalar(
    value: adapter_schema.ArgDefaultScalar,
    options: []const []const u8,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
) diagnostic.Error!void {
    const text = switch (value) {
        .string => |s| s,
        else => return diag.withContext(context).fail(null, .{ .invalid_option_value = {} }),
    };
    if (!containsOptionValue(options, text)) {
        return diag.withContext(context).fail(null, .{ .invalid_option_value = {} });
    }
}

pub fn containsOptionValue(options: []const []const u8, text: []const u8) bool {
    for (options) |option| {
        if (std.mem.eql(u8, option, text)) return true;
    }
    return false;
}

/// `scratch_alloc` owns command argument builder backing; `arena` owns returned
/// compiled template segments and placeholder names.
pub fn compileSegments(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    template_segments: []const template.Segment,
    arg_entries: *std.ArrayList(ArgBuildEntry),
    in_optional: bool,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) ![]recipe_ir.CompiledSegment {
    const segments = try arena.alloc(recipe_ir.CompiledSegment, template_segments.len);

    for (template_segments, 0..) |segment, idx| {
        segments[idx] = switch (segment) {
            .literal => |literal| .{ .literal = try arena.dupe(u8, literal) },
            .placeholder => |placeholder| .{ .arg = blk: {
                if (findArgEntryIndex(arg_entries.items, placeholder.name)) |arg_idx| {
                    if (!in_optional) arg_entries.items[arg_idx].required = true;
                    try mergePlaceholderType(&arg_entries.items[arg_idx], placeholder, diag, base_context);
                    break :blk arg_idx;
                }
                const name_copy = try arena.dupe(u8, placeholder.name);
                try arg_entries.append(scratch_alloc, .{
                    .name = name_copy,
                    .arg_type = try validatePlaceholderType(placeholder, diag, base_context),
                    .required = !in_optional,
                });
                break :blk arg_entries.items.len - 1;
            } },
            .optional => |inner| .{ .optional = try compileSegments(scratch_alloc, arena, inner, arg_entries, true, diag, base_context) },
        };
    }

    return segments;
}

pub fn validatePlaceholderType(
    placeholder: template.Placeholder,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) ![]const u8 {
    const arg_type = placeholder.arg_type;
    var context = base_context;
    context.argument_name = placeholder.name;
    if (!isKnownArgType(arg_type)) {
        return diag.withContext(context).fail(null, .{ .invalid_argument_type = .{ .arg_type = arg_type } });
    }
    return arg_type;
}

pub fn mergePlaceholderType(
    entry: *ArgBuildEntry,
    placeholder: template.Placeholder,
    diag: diagnostic.Reporter,
    base_context: DiagnosticContext,
) !void {
    const arg_type = placeholder.arg_type;
    var context = base_context;
    context.argument_name = placeholder.name;
    if (!isKnownArgType(arg_type)) {
        return diag.withContext(context).fail(null, .{ .invalid_argument_type = .{ .arg_type = arg_type } });
    }
    if (!std.mem.eql(u8, entry.arg_type, arg_type)) {
        return diag.withContext(context).fail(null, .{ .conflicting_argument_type = {} });
    }
}

fn compileArgDefault(
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

fn compileAdapterDefaultArg(
    arena: std.mem.Allocator,
    value: adapter_schema.ArgDefault,
) !recipe_ir.StepArg {
    const expr = @import("../../expr.zig");
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

fn compileAdapterDefaultScalar(
    arena: std.mem.Allocator,
    value: adapter_schema.ArgDefaultScalar,
) !@import("../../expr.zig").Expression {
    return switch (value) {
        .string => |text| makeLiteralExpr(arena, .{ .push_string = try arena.dupe(u8, text) }),
        .int => |n| makeLiteralExpr(arena, .{ .push_int = n }),
        .float => |n| makeLiteralExpr(arena, .{ .push_float = n }),
        .bool => |b| makeLiteralExpr(arena, .{ .push_bool = b }),
    };
}

pub fn makeLiteralExpr(arena: std.mem.Allocator, op: @import("../../expr.zig").Op) !@import("../../expr.zig").Expression {
    const expr = @import("../../expr.zig");
    const ops = try arena.alloc(expr.Op, 1);
    ops[0] = op;
    return .{ .ops = ops };
}

pub fn findArgEntryIndex(arg_entries: []const ArgBuildEntry, name: []const u8) ?usize {
    for (arg_entries, 0..) |arg_entry, idx| {
        if (std.mem.eql(u8, arg_entry.name, name)) return idx;
    }
    return null;
}
