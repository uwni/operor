const std = @import("std");
const Adapter = @import("../../adapter/Adapter.zig");
const adapter_schema = @import("../../adapter/schema.zig");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");
const slot_map_mod = @import("slot_map.zig");
const adapter_mod = @import("adapter.zig");
const args_mod = @import("args.zig");
const expr_compile = @import("expr_compile.zig");

const SlotMap = slot_map_mod.SlotMap;
const DiagnosticContext = slot_map_mod.DiagnosticContext;

pub fn precompileTasks(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    slot_map: *SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    diag: diagnostic.Reporter,
) ![]recipe_ir.Task {
    var tasks: std.ArrayList(recipe_ir.Task) = .empty;
    errdefer tasks.deinit(arena);
    var has_error = false;

    for (recipe.tasks, 0..) |*task_cfg, task_idx| {
        const steps = precompileSteps(scratch_alloc, arena, task_cfg.steps, slot_map, loaded_adapters, precompiled_instruments, task_idx, diag) catch |err| switch (err) {
            error.AnalysisFail => {
                has_error = true;
                continue;
            },
            else => return err,
        };

        const task_name = try arena.dupe(u8, task_cfg.name);
        const task_iter = task_cfg.iter orelse true;

        if (task_cfg.@"while") |while_src| {
            const task_context: DiagnosticContext = .{ .task_idx = task_idx };
            const condition = expr_compile.compileExpr(slot_map, arena, diag, task_context, while_src.source(), .expression) catch |err| {
                switch (err) {
                    error.AnalysisFail => {
                        has_error = true;
                        continue;
                    },
                    else => return err,
                }
            };
            try tasks.append(arena, .{ .name = task_name, .iter = task_iter, .kind = .{ .loop = .{
                .condition = condition,
                .steps = steps,
            } } });
        } else if (task_cfg.@"if") |guard_src| {
            const task_context: DiagnosticContext = .{ .task_idx = task_idx };
            const condition = expr_compile.compileExpr(slot_map, arena, diag, task_context, guard_src.source(), .expression) catch |err| {
                switch (err) {
                    error.AnalysisFail => {
                        has_error = true;
                        continue;
                    },
                    else => return err,
                }
            };
            try tasks.append(arena, .{ .name = task_name, .iter = task_iter, .kind = .{ .conditional = .{
                .@"if" = condition,
                .steps = steps,
            } } });
        } else {
            try tasks.append(arena, .{ .name = task_name, .iter = task_iter, .kind = .{ .sequential = .{
                .steps = steps,
            } } });
        }
    }
    if (has_error) return error.AnalysisFail;
    return try tasks.toOwnedSlice(arena);
}

pub fn precompileSteps(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    step_cfgs: []config.StepConfig,
    slot_map: *SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
    task_idx: usize,
    diag: diagnostic.Reporter,
) ![]recipe_ir.Step {
    var steps: std.ArrayList(recipe_ir.Step) = .empty;
    errdefer steps.deinit(arena);
    var has_error = false;

    for (step_cfgs, 0..) |*step_cfg, step_idx| {
        const step = switch (step_cfg.*) {
            .compute => |*cfg| precompileComputeStep(
                arena,
                slot_map,
                cfg,
                task_idx,
                step_idx,
                diag,
            ),
            .call => |*cfg| precompileCallStep(
                scratch_alloc,
                arena,
                slot_map,
                loaded_adapters,
                precompiled_instruments,
                cfg,
                task_idx,
                step_idx,
                diag,
            ),
            .sleep_ms => |*cfg| precompileSleepStep(arena, slot_map, cfg, task_idx, step_idx, diag),
            .parallel => |*cfg| precompileParallelStep(
                scratch_alloc,
                arena,
                slot_map,
                loaded_adapters,
                precompiled_instruments,
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
        try steps.append(arena, step);
    }
    if (has_error) return error.AnalysisFail;
    return try steps.toOwnedSlice(arena);
}

pub fn precompileCallStep(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    slot_map: *SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
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

    const if_expr = try precompileIf(arena, slot_map, diag, call_context, cfg.@"if");

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
    const command = try adapter_mod.getOrCompileCommand(scratch_alloc, arena, precompiled_instrument, command_source, command_name, loaded_adapter.instrument.bool_format, loaded_adapter.instrument.float_precision, diag, call_context);

    const call_copy = try arena.dupe(u8, command_name);
    const instrument_copy = try arena.dupe(u8, instrument_name);
    const compiled_args = try args_mod.compileStepArgs(arena, command, cfg.args, slot_map, diag, call_context);

    var save_result: ?recipe_ir.Step.SaveResult = null;
    if (cfg.assign) |label| {
        var assign_context = call_context;
        assign_context.variable_name = label;

        const is_object = if (command.response) |r| (r == .object) else false;
        if (is_object) {
            if (slot_map.slots.contains(label)) {
                return diag.withContext(assign_context).fail(null, .{ .object_assign_to_var = .{ .variable = label } });
            }
            const object_spec = command.response.?.object;
            var field_count: usize = 0;
            for (object_spec.segments) |seg| if (seg == .field) { field_count += 1; };
            const object_field_slots = try arena.alloc(usize, field_count);
            var idx: usize = 0;
            for (object_spec.segments) |seg| switch (seg) {
                .literal => {},
                .field => |f| {
                    const field_slot_name = try std.fmt.allocPrint(arena, "{s}.{s}", .{ label, f.name });
                    object_field_slots[idx] = try slot_map.varSlotIndex(diag, assign_context, field_slot_name);
                    idx += 1;
                },
            };
            save_result = .{ .object = object_field_slots };
        } else {
            const save_slot = try slot_map.varSlotIndex(diag, assign_context, label);
            if (command.response) |response| {
                slot_map.recordListResponseCapacity(save_slot, response);
            }
            save_result = .{ .scalar = save_slot };
        }
    }

    return .{
        .action = .{ .instrument_call = .{
            .call = call_copy,
            .instrument = instrument_copy,
            .instrument_idx = precompiled_instruments.getIndex(instrument_name).?,
            .command = command,
            .args = compiled_args,
            .save_result = save_result,
        } },
        .@"if" = if_expr,
    };
}

pub fn precompileComputeStep(
    arena: std.mem.Allocator,
    slot_map: *const SlotMap,
    cfg: *const config.ComputeStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) !recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };

    const if_expr = try precompileIf(arena, slot_map, diag, step_context, cfg.@"if");

    const assign_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx, .variable_name = cfg.assign };
    const save_slot = try slot_map.varSlotIndex(diag, assign_context, cfg.assign);

    const compute_expr = try expr_compile.compileExpr(slot_map, arena, diag, assign_context, cfg.compute, .expression);

    return .{
        .action = .{ .compute = .{
            .expression = compute_expr,
            .save_slot = save_slot,
        } },
        .@"if" = if_expr,
    };
}

pub fn precompileIf(
    arena: std.mem.Allocator,
    slot_map: *const SlotMap,
    diag: diagnostic.Reporter,
    context: DiagnosticContext,
    if_src_opt: ?config.BooleanExpr,
) !?expr.Expression {
    if (if_src_opt) |if_src| {
        return try expr_compile.compileExpr(slot_map, arena, diag, context, if_src.source(), .expression);
    }
    return null;
}

pub fn precompileSleepStep(
    arena: std.mem.Allocator,
    slot_map: *const SlotMap,
    cfg: *const config.SleepStepConfig,
    task_idx: usize,
    step_idx: usize,
    diag: diagnostic.Reporter,
) !recipe_ir.Step {
    const step_context: DiagnosticContext = .{ .task_idx = task_idx, .step_idx = step_idx };
    const if_expr = try precompileIf(arena, slot_map, diag, step_context, cfg.@"if");
    return .{
        .action = .{ .sleep = .{ .duration_ms = cfg.sleep_ms } },
        .@"if" = if_expr,
    };
}

pub fn precompileParallelStep(
    scratch_alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    slot_map: *SlotMap,
    loaded_adapters: *const std.StringHashMap(Adapter),
    precompiled_instruments: *std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument),
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
        scratch_alloc,
        arena,
        cfg.parallel,
        slot_map,
        loaded_adapters,
        precompiled_instruments,
        task_idx,
        diag,
    );
    try validateParallelUniqueInstruments(
        scratch_alloc,
        inner_steps,
        precompiled_instruments.count(),
        step_context,
        diag,
    );

    const if_expr = try precompileIf(arena, slot_map, diag, step_context, cfg.@"if");
    return .{
        .action = .{ .parallel = .{ .steps = inner_steps } },
        .@"if" = if_expr,
    };
}

pub fn validateParallelUniqueInstruments(
    scratch_alloc: std.mem.Allocator,
    steps: []const recipe_ir.Step,
    instrument_count: usize,
    context: DiagnosticContext,
    diag: diagnostic.Reporter,
) !void {
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(scratch_alloc, instrument_count);
    defer seen.deinit(scratch_alloc);

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
