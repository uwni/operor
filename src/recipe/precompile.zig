const std = @import("std");
const doc_parse = @import("../doc_parse.zig");
const Adapter = @import("../adapter/Adapter.zig");
const parse_mod = @import("../adapter/parse.zig");
const testing = @import("../testing.zig");
const config = @import("config.zig");
const diagnostic = @import("diagnostic.zig");
const types = @import("types.zig");
const expr = @import("../expr.zig");
const visa = @import("../visa/root.zig");

const max_recipe_size: usize = 512 * 1024;

const SlotTable = std.StringArrayHashMap(void);

pub fn precompilePath(
    allocator: std.mem.Allocator,
    recipe_path: []const u8,
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    if (precompile_diagnostic) |d| d.reset();

    var parse_arena: std.heap.ArenaAllocator = .init(allocator);
    defer parse_arena.deinit();

    const recipe_cfg = try doc_parse.parseFilePath(config.RecipeConfig, parse_arena.allocator(), recipe_path, max_recipe_size);

    if (recipe_cfg.pipeline == null) {
        if (precompile_diagnostic) |d| d.capture(.{}) catch {};
        return error.MissingPipeline;
    }

    return try precompileInternal(allocator, &recipe_cfg, adapter_dir, precompile_diagnostic);
}

/// Converts a parsed recipe document into the arena-owned runtime form used by preview and execution.
///
/// Precompile walks the recipe in a strict fail-fast order:
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
    adapter_dir: std.fs.Dir,
    precompile_diagnostic: ?*diagnostic.PrecompileDiagnostic,
) !types.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime and a temporary adapter cache used only while validating the recipe.
    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    defer adapter_arena.deinit();

    var diag_ctx: diagnostic.DiagnosticContext = .{};
    errdefer if (precompile_diagnostic) |d| d.capture(diag_ctx) catch {};

    var slot_map = try buildSlotMap(alloc, recipe);

    // 2. Eagerly load every referenced adapter.
    var loaded_adapters = try loadAdapters(adapter_arena.allocator(), recipe, adapter_dir, &diag_ctx);
    defer {
        var it = loaded_adapters.valueIterator();
        while (it.next()) |adapter| adapter.deinit();
    }

    // 3. Compile instrument metadata from loaded adapters.
    var precompiled_instruments = try precompileInstruments(alloc, recipe, &loaded_adapters);

    // 3-5. Normalize tasks and steps, resolving commands and validating arguments.
    var assign_set: std.StringArrayHashMap(void) = .init(alloc);
    const tasks = try precompileTasks(alloc, recipe, &slot_map, &loaded_adapters, &precompiled_instruments, &assign_set, &diag_ctx);

    // 6. Validate and resolve pipeline record configuration.
    diag_ctx = .{};
    const pipeline = try resolvePipelineConfig(alloc, recipe, &slot_map, &assign_set);

    // 7. Assign save_column indices to steps that contribute to recorded frames.
    assignSaveColumns(tasks, &slot_map, pipeline.record.?.explicit);

    // 8. Parse optional stop_when expression.
    const stop_when: ?expr.Expression = if (recipe.stop_when) |src|
        try slot_map.compileExpr(src.source())
    else
        null;

    return .{
        .arena = arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = pipeline,
        .stop_when = stop_when,
        .expected_iterations = recipe.expected_iterations,
        .initial_values = slot_map.varInitialValues(),
    };
}

const SlotMap = struct {
    slots: SlotTable,
    initial_values: []const types.Value,
    const_count: usize,
    alloc: std.mem.Allocator,

    /// Compile an expression source string: parse, bind variables, attempt
    /// full const evaluation, and fall back to inline + remap.
    fn compileExpr(self: *const SlotMap, source: []const u8) !expr.Expression {
        var e = try expr.parse(self.alloc, source);
        try e.bindVariables(&self.slots);
        if (self.tryConstFold(&e)) |result_op| {
            const ops = try self.alloc.alloc(expr.Op, 1);
            ops[0] = result_op;
            e.ops = ops;
        } else {
            self.inlineAndRemap(e.ops);
        }
        return e;
    }

    /// Try to evaluate a bound expression fully at compile time.
    /// Returns a single push Op when all variable references are consts.
    fn tryConstFold(self: *const SlotMap, e: *const expr.Expression) ?expr.Op {
        for (e.ops) |op| {
            switch (op) {
                .load_var, .load_list_len, .load_list_elem, .call_join => |ref| {
                    const slot = ref.slotIndex() orelse return null;
                    if (slot >= self.const_count) return null;
                },
                else => {},
            }
        }
        const result = e.eval(self.constResolver(), self.alloc) catch return null;
        defer result.deinit();
        return switch (result.value) {
            .int => |i| .{ .push_int = i },
            .float => |f| .{ .push_float = f },
            .bool => |b| .{ .push_bool = b },
            .string => |s| .{ .push_string = self.alloc.dupe(u8, s) catch return null },
        };
    }

    /// Inline const scalar values into ops and remap var slots.
    fn inlineAndRemap(self: *const SlotMap, ops: []expr.Op) void {
        for (ops) |*op| {
            switch (op.*) {
                .load_var => |ref| if (ref.slotIndex()) |slot| {
                    if (slot < self.const_count) {
                        // when try to load a const var, inline its value directly into the op and avoid any runtime lookup;
                        op.* = switch (self.initial_values[slot]) {
                            .int => |i| .{ .push_int = i },
                            .float => |f| .{ .push_float = f },
                            .bool => |b| .{ .push_bool = b },
                            .string => |s| .{ .push_string = s },
                            .list => .{ .push_int = 0 }, // list cannot inline to scalar; eval will error
                        };
                    } else {
                        op.* = .{ .load_var = .{ .binding = .{ .slot = slot - self.const_count } } };
                    }
                },
                .load_list_len => |ref| if (ref.slotIndex()) |slot| {
                    if (slot < self.const_count) {
                        switch (self.initial_values[slot]) {
                            .list => |items| op.* = .{ .push_int = @intCast(items.len) },
                            else => {}, // type error caught at eval time
                        }
                    } else {
                        op.* = .{ .load_list_len = .{ .binding = .{ .slot = slot - self.const_count } } };
                    }
                },
                .load_list_elem => |ref| if (ref.slotIndex()) |slot| {
                    if (slot >= self.const_count) {
                        op.* = .{ .load_list_elem = .{ .binding = .{ .slot = slot - self.const_count } } };
                    }
                    // const list: keep original slot for constResolver during full eval
                },
                .call_join => |ref| if (ref.slotIndex()) |slot| {
                    if (slot >= self.const_count) {
                        op.* = .{ .call_join = .{ .binding = .{ .slot = slot - self.const_count } } };
                    }
                    // const list: keep original slot for constResolver during full eval
                },
                else => {},
            }
        }
    }

    /// Look up a name and return the runtime binding (var slot remapped)
    /// or the const value if the name refers to a const.
    const ResolvedName = union(enum) {
        binding: expr.VariableBinding,
        const_value: types.Value,
    };

    fn resolveName(self: *const SlotMap, name: []const u8) ?ResolvedName {
        if (expr.resolveBuiltin(name)) |b| return .{ .binding = b };
        const slot = self.slots.getIndex(name) orelse return null;
        if (slot < self.const_count) return .{ .const_value = self.initial_values[slot] };
        return .{ .binding = .{ .slot = slot - self.const_count } };
    }

    /// Returns only the var portion of initial_values (excluding consts).
    fn varInitialValues(self: *const SlotMap) []const types.Value {
        return self.initial_values[self.const_count..];
    }

    /// Validate that `name` refers to a mutable var and return its remapped slot index.
    fn varSlotIndex(self: *const SlotMap, name: []const u8) !usize {
        const slot = self.slots.getIndex(name) orelse return error.UndeclaredVariable;
        if (slot < self.const_count) return error.AssignToConst;
        return slot - self.const_count;
    }

    /// Returns a compile-time resolver for const slots (using original indices).
    /// Used by tryConstFold for full expression evaluation.
    fn resolve(ctx_ptr: *const anyopaque, binding: expr.VariableBinding) ?expr.ResolvedValue {
        const self: *const SlotMap = @ptrCast(@alignCast(ctx_ptr));
        return switch (binding) {
            .slot => |slot| {
                // After inlineAndRemap, load_list_elem/call_join still use original slot indices
                if (slot >= self.const_count) return null;
                return resolveConstValue(self.initial_values[slot]);
            },
            .builtin => null,
        };
    }

    fn constResolver(self: *const SlotMap) expr.VarResolver {
        return .{ .ctx = @ptrCast(self), .resolve_fn = resolve };
    }

    fn resolveConstValue(value: types.Value) expr.ResolvedValue {
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
        const items: [*]const types.Value = @ptrCast(@alignCast(ctx));
        return resolveConstValue(items[index]);
    }
};

/// Validate consts/vars, build the merged slot map (consts first, then vars),
/// compile initial values, and create the compile-time const resolver.
fn buildSlotMap(alloc: std.mem.Allocator, recipe: *const config.RecipeConfig) !SlotMap {
    const const_keys = if (recipe.consts) |c| c.keys() else &.{};
    const const_vals = if (recipe.consts) |c| c.values() else &.{};
    const var_keys = if (recipe.vars) |v| v.keys() else &.{};
    const var_vals = if (recipe.vars) |v| v.values() else &.{};

    // Validate: no name conflicts between consts, vars, and builtins.
    for (const_keys) |name| {
        if (expr.resolveBuiltin(name) != null) return error.BuiltinVariableConflict;
    }
    for (var_keys) |name| {
        if (expr.resolveBuiltin(name) != null) return error.BuiltinVariableConflict;
        if (recipe.consts != null and recipe.consts.?.contains(name)) return error.DuplicateVariable;
    }

    // Build initial values array: consts first, then vars.
    const initial_values = try alloc.alloc(types.Value, const_keys.len + var_keys.len);
    for (const_vals, 0..) |value, idx| {
        initial_values[idx] = try compileInitialValue(alloc, value);
    }
    for (var_vals, 0..) |value, idx| {
        initial_values[const_keys.len + idx] = try compileInitialValue(alloc, value);
    }

    // Build the key-only slot map: consts first, then vars.
    var all_slots: SlotTable = .init(alloc);
    for (const_keys) |name| try all_slots.put(name, {});
    for (var_keys) |name| try all_slots.put(name, {});

    return .{
        .slots = all_slots,
        .initial_values = initial_values,
        .const_count = const_keys.len,
        .alloc = alloc,
    };
}

fn loadAdapters(
    allocator: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    adapter_dir: std.fs.Dir,
    diag_ctx: *diagnostic.DiagnosticContext,
) !std.StringHashMap(Adapter) {
    var map: std.StringHashMap(Adapter) = .init(allocator);
    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const cfg = entry.value_ptr.*;
        _ = getOrParseAdapter(allocator, &map, adapter_dir, cfg.adapter) catch |err| {
            diag_ctx.* = .{
                .instrument_name = entry.key_ptr.*,
                .adapter_name = cfg.adapter,
            };
            return err;
        };
    }
    return map;
}

fn precompileInstruments(
    alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
) !std.StringArrayHashMap(types.PrecompiledInstrument) {
    var precompiled_instruments: std.StringArrayHashMap(types.PrecompiledInstrument) = .init(alloc);
    try precompiled_instruments.ensureTotalCapacity(recipe.instruments.count());

    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr.*;
        const adapter = loaded_adapters.getPtr(instrument_cfg.adapter).?;

        const name_copy = try alloc.dupe(u8, instrument_name);
        const adapter_copy = try alloc.dupe(u8, instrument_cfg.adapter);
        const resource_copy = try alloc.dupe(u8, instrument_cfg.resource);
        const write_termination = try cloneOptionalBytes(alloc, adapter.write_termination);
        try precompiled_instruments.put(name_copy, .{
            .adapter_name = adapter_copy,
            .resource = resource_copy,
            .commands = std.StringHashMap(*const types.PrecompiledCommand).init(alloc),
            .write_termination = write_termination,
            .options = .{
                .timeout_ms = adapter.options.timeout_ms,
                .read_termination = adapter.options.read_termination,
                .query_delay_ms = adapter.options.query_delay_ms,
                .chunk_size = adapter.options.chunk_size,
            },
        });
    }
    return precompiled_instruments;
}

fn precompileTasks(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMap(types.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMap(void),
    diag_ctx: *diagnostic.DiagnosticContext,
) ![]types.Task {
    const tasks = try arena_alloc.alloc(types.Task, recipe.tasks.len);
    for (recipe.tasks, 0..) |*task_cfg, task_idx| {
        const steps = try precompileSteps(arena_alloc, task_cfg.steps, slot_map, loaded_adapters, precompiled_instruments, assign_set, task_idx, diag_ctx);

        if (task_cfg.@"while") |while_src| {
            // Loop task
            tasks[task_idx] = .{ .loop = .{
                .condition = try slot_map.compileExpr(while_src.source()),
                .steps = steps,
            } };
        } else if (task_cfg.@"if") |guard_src| {
            // Conditional task
            tasks[task_idx] = .{ .conditional = .{
                .@"if" = try slot_map.compileExpr(guard_src.source()),
                .steps = steps,
            } };
        } else {
            // Sequential task
            tasks[task_idx] = .{ .sequential = .{
                .steps = steps,
            } };
        }
    }
    return tasks;
}

fn precompileSteps(
    arena_alloc: std.mem.Allocator,
    step_cfgs: []config.StepConfig,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMap(types.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMap(void),
    task_idx: usize,
    diag_ctx: *diagnostic.DiagnosticContext,
) ![]types.Step {
    const steps = try arena_alloc.alloc(types.Step, step_cfgs.len);
    for (step_cfgs, 0..) |*step_cfg, step_idx| {
        steps[step_idx] = switch (step_cfg.*) {
            .compute => |*cfg| try precompileComputeStep(
                arena_alloc,
                slot_map,
                assign_set,
                cfg,
                task_idx,
                step_idx,
                diag_ctx,
            ),
            .call => |*cfg| try precompileCallStep(
                arena_alloc,
                slot_map,
                loaded_adapters,
                precompiled_instruments,
                assign_set,
                cfg,
                task_idx,
                step_idx,
                diag_ctx,
            ),
            .sleep_ms => |*cfg| try precompileSleepStep(slot_map, cfg),
        };
    }
    return steps;
}

fn precompileComputeStep(
    arena_alloc: std.mem.Allocator,
    slot_map: *const SlotMap,
    assign_set: *std.StringArrayHashMap(void),
    cfg: *const config.ComputeStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag_ctx: *diagnostic.DiagnosticContext,
) !types.Step {
    diag_ctx.* = .{
        .task_idx = task_idx,
        .step_idx = step_idx,
    };

    const if_expr = try precompileIf(slot_map, cfg.@"if");

    const save_slot = try slot_map.varSlotIndex(cfg.assign);
    const assign_copy = try arena_alloc.dupe(u8, cfg.assign);
    try assign_set.put(assign_copy, {});

    return .{
        .action = .{ .compute = .{
            .expression = try slot_map.compileExpr(cfg.compute),
            .save_slot = save_slot,
        } },
        .@"if" = if_expr,
    };
}

fn precompileCallStep(
    arena_alloc: std.mem.Allocator,
    slot_map: *const SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMap(types.PrecompiledInstrument),
    assign_set: *std.StringArrayHashMap(void),
    cfg: *const config.CallStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag_ctx: *diagnostic.DiagnosticContext,
) !types.Step {
    const dot_pos = std.mem.indexOfScalar(u8, cfg.call, '.') orelse return error.InvalidCallFormat;
    const instrument_name = cfg.call[0..dot_pos];
    const command_name = cfg.call[dot_pos + 1 ..];
    if (instrument_name.len == 0 or command_name.len == 0) return error.InvalidCallFormat;

    diag_ctx.* = .{
        .task_idx = task_idx,
        .step_idx = step_idx,
        .instrument_name = instrument_name,
        .command_name = command_name,
    };

    const if_expr = try precompileIf(slot_map, cfg.@"if");

    const precompiled_instrument = precompiled_instruments.getPtr(instrument_name) orelse return error.InstrumentNotFound;

    diag_ctx.adapter_name = precompiled_instrument.adapter_name;
    const loaded_adapter = loaded_adapters.getPtr(precompiled_instrument.adapter_name).?;
    const command = try getOrCompileCommand(arena_alloc, precompiled_instrument, loaded_adapter, command_name);

    const call_copy = try arena_alloc.dupe(u8, command_name);
    const instrument_copy = try arena_alloc.dupe(u8, instrument_name);
    const compiled_args = try compileStepArgs(arena_alloc, command, cfg.args, slot_map, diag_ctx);

    var save_slot: ?usize = null;
    if (cfg.assign) |label| {
        save_slot = try slot_map.varSlotIndex(label);
        const duped = try arena_alloc.dupe(u8, label);
        try assign_set.put(duped, {});
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
    if_src_opt: ?config.BooleanExpr,
) !?expr.Expression {
    if (if_src_opt) |if_src| {
        return try slot_map.compileExpr(if_src.source());
    }
    return null;
}

fn precompileSleepStep(
    slot_map: *const SlotMap,
    cfg: *const config.SleepStepConfig,
) !types.Step {
    return .{
        .action = .{ .sleep = .{ .duration_ms = cfg.sleep_ms } },
        .@"if" = try precompileIf(slot_map, cfg.@"if"),
    };
}

fn resolvePipelineConfig(
    arena_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *const SlotMap,
    assign_set: *const std.StringArrayHashMap(void),
) !types.PipelineConfig {
    const pipeline_cfg = recipe.pipeline orelse return error.MissingPipeline;
    if (pipeline_cfg.record == null) return error.MissingRecordConfig;
    var pipeline = try clonePipelineConfig(arena_alloc, pipeline_cfg);
    switch (pipeline.record.?) {
        .all => {
            pipeline.record = .{ .explicit = try arena_alloc.dupe([]const u8, assign_set.keys()) };
        },
        .explicit => |columns| {
            for (columns) |name| {
                if (slot_map.resolveName(name) == null) {
                    return error.UndeclaredVariable;
                }
                if (!assign_set.contains(name)) {
                    return error.RecordVariableNotFound;
                }
            }
        },
    }
    return pipeline;
}

fn assignSaveColumns(tasks: []types.Task, slot_map: *const SlotMap, columns: []const []const u8) void {
    const var_keys = slot_map.slots.keys()[slot_map.const_count..];
    for (tasks) |*task| {
        for (task.steps()) |*step| {
            switch (step.action) {
                .instrument_call => |*ic| {
                    ic.save_column = if (ic.save_slot) |slot| slotToColumn(var_keys, slot, columns) else null;
                },
                .compute => |*comp| {
                    comp.save_column = slotToColumn(var_keys, comp.save_slot, columns);
                },
                .sleep => {},
            }
        }
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

fn getOrParseAdapter(
    allocator: std.mem.Allocator,
    loaded_adapters: *std.StringHashMap(Adapter),
    adapter_dir: std.fs.Dir,
    adapter_name: []const u8,
) !*const Adapter {
    if (loaded_adapters.getPtr(adapter_name)) |loaded| return loaded;

    const key = try allocator.dupe(u8, adapter_name);

    var loaded = try parse_mod.parseAdapterInDir(allocator, adapter_dir, adapter_name);
    errdefer loaded.deinit();

    try loaded_adapters.put(key, loaded);
    return loaded_adapters.getPtr(adapter_name).?;
}

fn getOrCompileCommand(
    allocator: std.mem.Allocator,
    instrument: *types.PrecompiledInstrument,
    loaded_adapter: *const Adapter,
    call: []const u8,
) !*const types.PrecompiledCommand {
    if (instrument.commands.get(call)) |command| return command;

    const source = loaded_adapter.commands.get(call) orelse return error.CommandNotFound;
    const key = try allocator.dupe(u8, call);
    const compiled_value = try compileCommand(allocator, source, instrument);

    const compiled = try allocator.create(types.PrecompiledCommand);
    compiled.* = compiled_value;

    try instrument.commands.put(key, compiled);
    return compiled;
}

fn compileCommand(
    allocator: std.mem.Allocator,
    source: Adapter.Command,
    instrument: *const types.PrecompiledInstrument,
) !types.PrecompiledCommand {
    var arg_names = std.ArrayList([]const u8).empty;
    const segments = try allocator.alloc(types.CompiledSegment, source.template.len);

    for (source.template, 0..) |segment, idx| {
        segments[idx] = switch (segment) {
            .literal => |literal| .{ .literal = try allocator.dupe(u8, literal) },
            .placeholder => |name| .{ .arg = blk: {
                if (findArgIndex(arg_names.items, name)) |arg_idx| break :blk arg_idx;
                const name_copy = try allocator.dupe(u8, name);
                try arg_names.append(allocator, name_copy);
                break :blk arg_names.items.len - 1;
            } },
        };
    }

    return .{
        .instrument = instrument,
        .response = source.response,
        .segments = segments,
        .arg_names = try arg_names.toOwnedSlice(allocator),
    };
}

fn compileStepArgs(
    allocator: std.mem.Allocator,
    command: *const types.PrecompiledCommand,
    doc_args: ?std.StringHashMap(config.ArgValueDoc),
    slot_map: *const SlotMap,
    diag_ctx: *diagnostic.DiagnosticContext,
) ![]types.StepArg {
    const args = try allocator.alloc(types.StepArg, command.arg_names.len);

    for (command.arg_names, 0..) |arg_name, idx| {
        const doc_arg = if (doc_args) |map|
            map.get(arg_name) orelse {
                diag_ctx.argument_name = arg_name;
                return error.MissingCommandArgument;
            }
        else {
            diag_ctx.argument_name = arg_name;
            return error.MissingCommandArgument;
        };
        args[idx] = try compileArg(allocator, doc_arg, slot_map);
    }

    if (doc_args) |map| {
        var it = map.iterator();
        while (it.next()) |entry| {
            if (!command.hasPlaceholder(entry.key_ptr.*)) {
                diag_ctx.argument_name = entry.key_ptr.*;
                return error.UnexpectedCommandArgument;
            }
        }
    }

    return args;
}

fn compileInitialValue(allocator: std.mem.Allocator, value: config.ArgValueDoc) !types.Value {
    return switch (value) {
        .scalar => |scalar| compileScalarValue(allocator, scalar),
        .list => |items| blk: {
            const compiled = try allocator.alloc(types.Value, items.len);
            for (items, 0..) |item, idx| {
                compiled[idx] = try compileScalarValue(allocator, item);
            }
            break :blk .{ .list = compiled };
        },
    };
}

fn compileScalarValue(allocator: std.mem.Allocator, value: config.ArgScalarDoc) !types.Value {
    return switch (value) {
        .string => |text| .{ .string = try allocator.dupe(u8, text) },
        .int => |number| .{ .int = number },
        .float => |number| .{ .float = number },
        .bool => |flag| .{ .bool = flag },
    };
}

fn clonePipelineConfig(allocator: std.mem.Allocator, cfg: config.PipelineConfig) !types.PipelineConfig {
    if (cfg.buffer_size) |size| {
        if (size == 0) return error.InvalidPipelineConfig;
    }
    if (cfg.warn_usage_percent) |percent| {
        if (percent == 0 or percent > 100) return error.InvalidPipelineConfig;
    }
    const has_network_host = cfg.network_host != null;
    const has_network_port = cfg.network_port != null;
    if (has_network_host != has_network_port) return error.InvalidPipelineConfig;
    if (cfg.network_port) |port| {
        if (port == 0) return error.InvalidPipelineConfig;
    }

    const record_copy: ?types.RecordConfig = if (cfg.record) |record| switch (record) {
        .all => |value| .{ .all = try allocator.dupe(u8, value) },
        .explicit => |columns| blk: {
            const items = try allocator.alloc([]const u8, columns.len);
            for (columns, 0..) |name, idx| {
                items[idx] = try allocator.dupe(u8, name);
            }
            break :blk .{ .explicit = items };
        },
    } else null;

    return .{
        .buffer_size = cfg.buffer_size,
        .warn_usage_percent = cfg.warn_usage_percent,
        .mode = cfg.mode,
        .file_path = if (cfg.file_path) |path| try allocator.dupe(u8, path) else null,
        .network_host = if (cfg.network_host) |host| try allocator.dupe(u8, host) else null,
        .network_port = cfg.network_port,
        .record = record_copy,
    };
}

fn compileArg(
    allocator: std.mem.Allocator,
    doc_arg: config.ArgValueDoc,
    slot_map: *const SlotMap,
) !types.StepArg {
    return switch (doc_arg) {
        .scalar => |scalar| .{ .scalar = try compileArgScalar(allocator, scalar, slot_map) },
        .list => |items| blk: {
            const out = try allocator.alloc(expr.Expression, items.len);
            for (items, 0..) |item, idx| {
                out[idx] = try compileArgScalar(allocator, item, slot_map);
            }
            break :blk .{ .list = out };
        },
    };
}

fn compileArgScalar(
    allocator: std.mem.Allocator,
    value: config.ArgScalarDoc,
    slot_map: *const SlotMap,
) !expr.Expression {
    return switch (value) {
        .string => |text| {
            if (std.mem.indexOf(u8, text, "${") != null) {
                return slot_map.compileExpr(text);
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

fn findArgIndex(arg_names: []const []const u8, name: []const u8) ?usize {
    for (arg_names, 0..) |arg_name, idx| {
        if (std.mem.eql(u8, arg_name, name)) return idx;
    }
    return null;
}

test "load recipe and adapters" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r1_set.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
    defer compiled.deinit();

    const instrument = compiled.instruments.getPtr("d1") orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.eql(u8, instrument.resource, "USB0::1::INSTR"));
    try std.testing.expect(std.mem.eql(u8, instrument.adapter_name, "psu.toml"));
    try std.testing.expectEqual(@as(usize, 1), instrument.commands.count());

    const command = instrument.commands.get("set") orelse return error.TestUnexpectedResult;
    try std.testing.expect(command.instrument == instrument);
    try std.testing.expect(command.response == null);
    try std.testing.expectEqual(@as(usize, 1), command.arg_names.len);
    try std.testing.expect(std.mem.eql(u8, command.arg_names[0], "voltage"));

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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/r2_stop_when.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
        \\stop_when: "$ELAPSED_MS >= 2000 || $ITER >= 3"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/r2_stop_when.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V"
    );
    try workspace.writeFile("recipes/run_once.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
    defer compiled.deinit();

    // No expected_iterations in recipe, so null.
    try std.testing.expectEqual(@as(?u64, null), compiled.expected_iterations);
}

test "precompile preserves typed literal step arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/cfg.toml",
        \\[metadata]
        \\
        \\[commands.configure]
        \\write = "CONF {count} {voltage} {enabled} {channels} {mirror}"
    );
    try workspace.writeFile("recipes/typed_args.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: cfg.toml
        \\    resource: USB0::1::INSTR
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
        \\          channels:
        \\            - 1
        \\            - 2
        \\          mirror: "${target}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/typed_args.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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
                .load_var => |ref| switch (ref) {
                    .binding => |b| switch (b) {
                        .slot => |slot| try std.testing.expect(slot < compiled.initial_values.len),
                        .builtin => return error.TestUnexpectedResult,
                    },
                    .name => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .list => return error.TestUnexpectedResult,
    }
}

const vendor_psu_adapter =
    \\[metadata]
    \\
    \\[commands.set_voltage]
    \\write = "VOLT {voltage},(@{channels})"
;

test "precompile stores only referenced commands" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml",
        \\[metadata]
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage}"
        \\
        \\[commands.output_on]
        \\write = "OUTP ON"
    );
    try workspace.writeFile("recipes/r1_set_voltage.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_instrument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.InstrumentNotFound, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile validates command arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
    );
    try workspace.writeFile("recipes/unexpected_argument.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\          channel: 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const missing_argument_path = try workspace.realpathAlloc("recipes/missing_argument.yaml");
    defer gpa.free(missing_argument_path);
    const unexpected_argument_path = try workspace.realpathAlloc("recipes/unexpected_argument.yaml");
    defer gpa.free(unexpected_argument_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.MissingCommandArgument, precompilePath(gpa, missing_argument_path, dir, null));
    try std.testing.expectError(error.UnexpectedCommandArgument, precompilePath(gpa, unexpected_argument_path, dir, null));
}

test "precompiled command renders via helper" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage}", null, null);

    var instrument = types.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    try std.testing.expect(compiled.instrument == &instrument);
    try std.testing.expectEqual(@as(usize, 1), compiled.arg_names.len);
    try std.testing.expectEqualStrings("voltage", compiled.arg_names[0]);

    const args = [_]types.RenderValue{
        .{ .scalar = .{ .float = 3.3 } },
    };

    var stack_buf: [32]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("VOLT 3.3\n", rendered.bytes);
    try std.testing.expect(rendered.owned == null);
}

test "precompiled command render falls back to heap when suffix leaves too little stack space" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage}", null, null);

    var instrument = types.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const types.PrecompiledCommand).init(alloc),
        .write_termination = "\r\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    const compiled = try compileCommand(gpa, source, &instrument);
    defer compiled.deinit(gpa);

    const args = [_]types.RenderValue{
        .{ .scalar = .{ .string = "1234567890" } },
    };

    var stack_buf: [8]u8 = undefined;
    const rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination);
    defer rendered.deinit(gpa);

    try std.testing.expect(rendered.owned != null);
    try std.testing.expectEqualStrings("VOLT 1234567890\r\n", rendered.bytes);
}

test "precompile diagnostic includes step context" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/missing_command.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic: diagnostic.PrecompileDiagnostic = .init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePath(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.CommandNotFound, err);

        var out: std.Io.Writer.Allocating = .init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "task 0 step 0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "instrument=d1"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "adapter=psu0"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "command=missing"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile compute step" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/compute_no_save.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - compute: 1 + 2
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/compute_no_save.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    // With the union-based StepConfig, missing required fields (assign) results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile step with if guard" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/if_guard.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expect(compiled.tasks[0].steps()[0].@"if" != null);
}

test "precompile rejects invalid step (neither call nor compute)" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/invalid_step.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    // With the union-based StepConfig, an object matching neither variant results in a parse error.
    try std.testing.expectError(error.WrongType, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile rejects record with unknown assign variable" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/bad_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record:
        \\    - voltage
        \\    - nonexistent
        \\vars:
        \\  voltage: 0
        \\  nonexistent: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\            - 2
        \\        assign: voltage
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/bad_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.RecordVariableNotFound, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile accepts valid record subset" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_ok.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record:
        \\    - voltage
        \\vars:
        \\  voltage: 0
        \\  doubled: 0
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: "1.0"
        \\          channels:
        \\            - 1
        \\            - 2
        \\        assign: voltage
        \\      - compute: "${voltage} * 2"
        \\        assign: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_ok.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_pipeline.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 1.0
        \\          channels:
        \\            - 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_pipeline.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic: diagnostic.PrecompileDiagnostic = .init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePath(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.MissingPipeline, err);

        var out: std.Io.Writer.Allocating = .init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'pipeline'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile diagnostic for missing record" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/no_record.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
        \\pipeline: {}
        \\tasks:
        \\  - steps:
        \\      - call: d1.set_voltage
        \\        args:
        \\          voltage: 1.0
        \\          channels:
        \\            - 1
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/no_record.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var precompile_diagnostic: diagnostic.PrecompileDiagnostic = .init(gpa);
    defer precompile_diagnostic.deinit();

    _ = precompilePath(gpa, recipe_path, dir, &precompile_diagnostic) catch |err| {
        try std.testing.expectEqual(error.MissingRecordConfig, err);

        var out: std.Io.Writer.Allocating = .init(gpa);
        defer out.deinit();

        try precompile_diagnostic.write(&out.writer, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "missing required 'record'"));
        return;
    };

    return error.TestUnexpectedResult;
}

test "precompile expands record all into explicit assign list" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.toml", vendor_psu_adapter);
    try workspace.writeFile("recipes/record_all.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu0.toml
        \\    resource: USB0::1::INSTR
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
        \\          channels:
        \\            - 1
        \\        assign: voltage
        \\      - compute: "${voltage} * 2"
        \\        assign: doubled
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_all.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V {voltage}"
    );
    try workspace.writeFile("recipes/undeclared.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, dir, null));
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.UndeclaredVariable, precompilePath(gpa, recipe_path, dir, null));
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.BuiltinVariableConflict, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile sequential task" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/sequential.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expect(compiled.tasks[0] == .sequential);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps().len);
}

test "precompile loop task with while" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/loop_task.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 0
        \\tasks:
        \\  - while: "$ITER < 10"
        \\    steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/loop_task.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(usize, 1), compiled.tasks.len);
    try std.testing.expect(compiled.tasks[0] == .loop);
    try std.testing.expectEqual(@as(usize, 1), compiled.tasks[0].steps().len);
}

test "precompile conditional task with if" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/conditional_task.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
        \\pipeline:
        \\  record: all
        \\vars:
        \\  voltage: 5
        \\tasks:
        \\  - if: "${voltage} > 0"
        \\    steps:
        \\      - call: d1.set
        \\        args:
        \\          voltage: "5"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/conditional_task.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "VOLT {voltage}"
    );
    try workspace.writeFile("recipes/list_vars.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage},(@{channels})"
    );
    try workspace.writeFile("recipes/const_join.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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
        \\          channels: "join(${channels}, \",\")"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/const_join.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V {voltage}"
    );
    try workspace.writeFile("recipes/const_arith.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V {voltage}"
        \\response = "float"
    );
    try workspace.writeFile("recipes/assign_const.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.AssignToConst, precompilePath(gpa, recipe_path, dir, null));
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    try std.testing.expectError(error.DuplicateVariable, precompilePath(gpa, recipe_path, dir, null));
}

test "precompile does not fold expressions referencing runtime vars" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.toml",
        \\[metadata]
        \\
        \\[commands.set]
        \\write = "V {voltage}"
    );
    try workspace.writeFile("recipes/no_fold.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.toml
        \\    resource: USB0::1::INSTR
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

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var compiled = try precompilePath(gpa, recipe_path, dir, null);
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
