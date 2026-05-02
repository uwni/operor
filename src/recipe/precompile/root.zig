const std = @import("std");
const doc_parse = @import("../../doc_parse.zig");
const Adapter = @import("../../adapter/Adapter.zig");
const adapter_schema = @import("../../adapter/schema.zig");
const testing = @import("../../testing.zig");
const config = @import("../config.zig");
const diagnostic = @import("../../diagnostic.zig");
const recipe_ir = @import("../compiled.zig");
const expr = @import("../../expr.zig");

const slot_map_mod = @import("slot_map.zig");
const adapter_mod = @import("adapter.zig");
const steps_mod = @import("steps.zig");
const pipeline_mod = @import("pipeline.zig");
const expr_compile = @import("expr_compile.zig");

// Re-export public symbols that callers may need.
pub const SlotMap = slot_map_mod.SlotMap;
pub const buildSlotMap = slot_map_mod.buildSlotMap;
pub const loadAdapters = adapter_mod.loadAdapters;
pub const compileCommand = adapter_mod.compileCommand;

const max_recipe_size: usize = 512 * 1024;

pub fn precompilePath(
    gpa: std.mem.Allocator,
    io: std.Io,
    recipe_path: []const u8,
    adapter_dir: std.Io.Dir,
    log: ?*std.Io.Writer,
) !recipe_ir.PrecompiledRecipe {
    var diagnostics: diagnostic.Diagnostics = .init(gpa, recipe_path);
    defer diagnostics.deinit();
    var empty_diagnostics: diagnostic.EmptyDiagnostics = .init();
    defer empty_diagnostics.deinit();
    const reporter = if (log != null) diagnostics.reporter() else empty_diagnostics.reporter();

    var precompile_arena: std.heap.ArenaAllocator = .init(gpa);
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

    const compiled = try precompileInternal(gpa, precompile_allocator, &recipe_cfg, &loaded_adapters, reporter);

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
/// `gpa` is the parent allocator for the returned recipe arena.
/// `scratch_alloc` owns temporary compiler data; returned recipe data must not
/// reference allocations owned only by it. Data referenced by the returned
/// `PrecompiledRecipe` is allocated from the local result `arena`.
fn precompileInternal(
    gpa: std.mem.Allocator,
    scratch_alloc: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
    reporter: diagnostic.Reporter,
) !recipe_ir.PrecompiledRecipe {
    // 1. Create the arena-owned result lifetime.
    var result_arena: std.heap.ArenaAllocator = .init(gpa);
    errdefer result_arena.deinit();
    const arena = result_arena.allocator();

    var slot_map = try buildSlotMap(scratch_alloc, arena, recipe, reporter);
    defer slot_map.deinit();

    // 3. Compile instrument metadata from loaded adapters.
    var precompiled_instruments = try precompileInstruments(arena, recipe, loaded_adapters);

    // 3-5. Normalize tasks and steps, resolving commands and validating arguments.
    const tasks = try steps_mod.precompileTasks(scratch_alloc, arena, recipe, &slot_map, loaded_adapters, &precompiled_instruments, reporter);

    // 6. Validate and resolve pipeline record configuration.
    const record_resolution = try pipeline_mod.resolvePipelineConfig(scratch_alloc, arena, recipe, &slot_map, reporter);

    // 7. Parse optional stop_when expression.
    var stop_when: ?expr.Expression = null;
    var stop_when_failed = false;
    if (recipe.stop_when) |src|
        stop_when = expr_compile.compileExpr(&slot_map, arena, reporter, .{}, src.source(), .expression) catch |err| switch (err) {
            error.AnalysisFail => blk: {
                stop_when_failed = true;
                break :blk null;
            },
            else => return err,
        };

    if (stop_when_failed) return error.AnalysisFail;

    return .{
        .arena = result_arena,
        .instruments = precompiled_instruments,
        .tasks = tasks,
        .pipeline = record_resolution.pipeline,
        .record_bindings = record_resolution.bindings,
        .stop_when = stop_when,
        .expected_iterations = recipe.expected_iterations,
        .float_precision = recipe.float_precision,
        .initial_values = slot_map.varInitialValues(),
        .list_slot_capacities = slot_map.list_slot_capacities,
    };
}

fn addDocumentError(diag: diagnostic.Reporter, err: anyerror, context: diagnostic.Context) !void {
    const message: diagnostic.Message = switch (err) {
        error.FileNotFound => .{ .file_not_found = {} },
        error.SyntaxError => .{ .syntax_error = {} },
        error.UnsupportedFormat => .{ .unsupported_format = {} },
        error.WrongType => .{ .wrong_type = {} },
        else => return err,
    };
    try diag.withContext(context).withSourceKind(.recipe_document).add(.fatal, null, message);
}

fn precompileInstruments(
    arena: std.mem.Allocator,
    recipe: *const config.RecipeConfig,
    loaded_adapters: *const std.StringHashMap(Adapter),
) !std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument) {
    var precompiled_instruments: std.StringArrayHashMapUnmanaged(recipe_ir.PrecompiledInstrument) = .empty;
    try precompiled_instruments.ensureTotalCapacity(arena, recipe.instruments.count());

    var instrument_it = recipe.instruments.iterator();
    while (instrument_it.next()) |entry| {
        const instrument_name = entry.key_ptr.*;
        const instrument_cfg = entry.value_ptr;
        const adapter = loaded_adapters.getPtr(instrument_cfg.adapter).?;

        const name_copy = try arena.dupe(u8, instrument_name);
        const precompiled_instrument = try precompileOwnedInstrument(arena, instrument_cfg, adapter);
        try precompiled_instruments.put(arena, name_copy, precompiled_instrument);
    }
    return precompiled_instruments;
}

fn precompileOwnedInstrument(arena: std.mem.Allocator, instrument_cfg: *const config.InstrumentConfig, adapter: *const Adapter) !recipe_ir.PrecompiledInstrument {
    const adapter_copy = try arena.dupe(u8, instrument_cfg.adapter);
    const resource_copy = try arena.dupe(u8, instrument_cfg.resource);
    const write_termination = try adapter_mod.cloneOptionalBytes(arena, adapter.write_termination);
    const bool_map = try adapter_mod.cloneBoolTextMap(arena, adapter.instrument.bool_format);
    return .{
        .adapter_name = adapter_copy,
        .resource = resource_copy,
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(arena),
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

test "load recipe and adapters" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage:float}"
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

test "precompile parses stop_when expression" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage:float}"
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
                try std.testing.expectEqualStrings("scan", text.items());
                found_string = true;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(found_float);
    try std.testing.expect(found_string);
}

test "precompile preserves explicit expected_iterations and leaves omitted value null" {
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
    try workspace.writeFile("recipes/with_expected_iterations.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\expected_iterations: 12
        \\vars: {}
        \\tasks:
        \\  - steps:
        \\      - call: d1.set
        \\        args: {}
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/run_once.yaml");
    defer gpa.free(recipe_path);
    const explicit_recipe_path = try workspace.realpathAlloc("recipes/with_expected_iterations.yaml");
    defer gpa.free(explicit_recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    try std.testing.expectEqual(@as(?u64, null), compiled.expected_iterations);

    var explicit_compiled = try precompilePath(gpa, std.testing.io, explicit_recipe_path, dir, null);
    defer explicit_compiled.deinit();

    try std.testing.expectEqual(@as(?u64, 12), explicit_compiled.expected_iterations);
}

test "precompile preserves typed literal step arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/cfg.yaml",
        \\metadata: {}
        \\commands:
        \\  configure:
        \\    write: "CONF {count:int} {voltage:float} {enabled:bool} {channels:list} {mirror:string}"
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
    \\    write: "VOLT {voltage:float},(@{channels:list})"
;

test "precompile rejects duplicate instrument in parallel block" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage:float}"
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
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "parallel steps cannot use instrument '\x1b[4md1\x1b[0m' more than once"));
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
        \\    write: "VOLT {voltage:float}"
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "instrument '\x1b[4mmissing\x1b[0m' is not declared in recipe",
    });
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

    var missing_dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer missing_dir.close(std.testing.io);
    try std.testing.expectError(error.AnalysisFail, precompilePath(gpa, std.testing.io, missing_argument_path, missing_dir, null));

    try expectPrecompileAnalysisFail(gpa, adapter_dir, unexpected_argument_path, &.{
        "unexpected command argument '\x1b[4mchannel\x1b[0m'",
    });
}

test "precompile allows omitted optional group arguments" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage:float}[,(@{channels:list})]"
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
        \\    write: "INST {channel:string}"
        \\    args:
        \\      channel:
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

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage:float}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    try std.testing.expect(compiled.instrument == &instrument);
    try std.testing.expectEqual(@as(usize, 1), compiled.args.len);
    try std.testing.expectEqualStrings("voltage", compiled.args[0].name);

    const args = [_]recipe_ir.Value{
        .{ .float = 3.3 },
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

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage:float}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\r\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.Value{
        .{ .string = recipe_ir.Value.String.borrow("1234567890") },
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

    const source = try Adapter.Command.parse(alloc, "VOLT {voltage:float}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, gpa, source, &instrument, null, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.Value{
        .{ .float = 3.14159265 },
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

test "adapter arg precision overrides recipe global float precision" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/laser.yaml",
        \\metadata: {}
        \\commands:
        \\  tune:
        \\    write: "WA{wavelength:float};CU{current:float}"
        \\    args:
        \\      wavelength:
        \\        precision: 2
    );
    try workspace.writeFile("recipes/precision.yaml",
        \\instruments:
        \\  laser:
        \\    adapter: laser.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\float_precision: 0
        \\tasks:
        \\  - steps:
        \\      - call: laser.tune
        \\        args:
        \\          wavelength: 1550.126
        \\          current: 12.7
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/precision.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const call = compiled.tasks[0].steps()[0].action.instrument_call;
    var args: [2]recipe_ir.Value = undefined;
    args[call.command.argIndex("wavelength").?] = .{ .float = 1550.126 };
    args[call.command.argIndex("current").?] = .{ .float = 12.7 };

    var stack_buf: [64]u8 = undefined;
    var rendered = try call.command.render(gpa, stack_buf[0..], args[0..], call.command.instrument.write_termination, compiled.float_precision);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("WA1550.13;CU13", rendered.bytes);
}

test "precompiled command applies bool format from adapter defaults" {
    const gpa = std.testing.allocator;
    var cmd_arena: std.heap.ArenaAllocator = .init(gpa);
    defer cmd_arena.deinit();
    const alloc = cmd_arena.allocator();
    var diags: diagnostic.Diagnostics = .init(gpa, "<test>");
    defer diags.deinit();

    const source = try Adapter.Command.parse(alloc, "OUTP {state:bool}", null, null, diags.reporter());

    var instrument = recipe_ir.PrecompiledInstrument{
        .adapter_name = "psu",
        .resource = "USB0::1::INSTR",
        .commands = std.StringHashMap(*const recipe_ir.PrecompiledCommand).init(alloc),
        .write_termination = "\n",
        .options = .{},
    };
    defer instrument.commands.deinit();

    var compiled = try compileCommand(gpa, gpa, source, &instrument, .{ .true_text = "ON", .false_text = "OFF" }, diags.reporter(), .{});
    defer compiled.deinit(gpa);

    const args = [_]recipe_ir.Value{
        .{ .bool = true },
    };

    var stack_buf: [32]u8 = undefined;
    var rendered = try compiled.render(gpa, stack_buf[0..], args[0..], instrument.write_termination, null);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("OUTP ON\n", rendered.bytes);
}

test "precompiled command applies list separators and element formats" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  configure:
        \\    write: "OUTP {states:list};VOLT {voltages:list}"
        \\    args:
        \\      states:
        \\        true: "ON"
        \\        false: "OFF"
        \\        separator: "|"
        \\      voltages:
        \\        separator: ";"
    );
    try workspace.writeFile("recipes/list_format.yaml",
        \\instruments:
        \\  d1:
        \\    adapter: psu.yaml
        \\    resource: "USB0::1::INSTR"
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: d1.configure
        \\        args:
        \\          states: [true, false]
        \\          voltages: [1.0, 2.5]
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/list_format.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    const call = compiled.tasks[0].steps()[0].action.instrument_call;
    const state_items = [_]recipe_ir.Value{ .{ .bool = true }, .{ .bool = false } };
    const voltage_items = [_]recipe_ir.Value{ .{ .float = 1.0 }, .{ .float = 2.5 } };
    var args: [2]recipe_ir.Value = undefined;
    args[call.command.argIndex("states").?] = .{ .list = recipe_ir.Value.List.borrow(state_items[0..]) };
    args[call.command.argIndex("voltages").?] = .{ .list = recipe_ir.Value.List.borrow(voltage_items[0..]) };

    var stack_buf: [64]u8 = undefined;
    var rendered = try call.command.render(gpa, stack_buf[0..], args[0..], call.command.instrument.write_termination, 1);
    defer rendered.deinit(gpa);

    try std.testing.expectEqualStrings("OUTP ON|OFF;VOLT 1.0;2.5", rendered.bytes);
}

test "option args validate static values and dynamic renders" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/trigger.yaml",
        \\metadata: {}
        \\commands:
        \\  source:
        \\    write: "TRIG:SOUR {source:option}"
        \\    args:
        \\      source:
        \\        options: [IMM, BUS]
    );
    try workspace.writeFile("recipes/invalid_option.yaml",
        \\instruments:
        \\  trig:
        \\    adapter: trigger.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\tasks:
        \\  - steps:
        \\      - call: trig.source
        \\        args:
        \\          source: EXT
    );
    try workspace.writeFile("recipes/dynamic_option.yaml",
        \\instruments:
        \\  trig:
        \\    adapter: trigger.yaml
        \\    resource: R
        \\pipeline:
        \\  record: all
        \\vars:
        \\  source: EXT
        \\tasks:
        \\  - steps:
        \\      - call: trig.source
        \\        args:
        \\          source: "${source}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const invalid_recipe_path = try workspace.realpathAlloc("recipes/invalid_option.yaml");
    defer gpa.free(invalid_recipe_path);
    const dynamic_recipe_path = try workspace.realpathAlloc("recipes/dynamic_option.yaml");
    defer gpa.free(dynamic_recipe_path);

    try expectPrecompileAnalysisFail(gpa, adapter_dir, invalid_recipe_path, &.{
        "argument value is not one of the adapter option values",
    });

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var compiled = try precompilePath(gpa, std.testing.io, dynamic_recipe_path, dir, null);
    defer compiled.deinit();

    const call = compiled.tasks[0].steps()[0].action.instrument_call;
    var args = [_]recipe_ir.Value{
        .{ .string = recipe_ir.Value.String.borrow("IMM") },
    };

    var stack_buf: [64]u8 = undefined;
    var rendered = try call.command.render(gpa, stack_buf[0..], args[0..], call.command.instrument.write_termination, null);
    defer rendered.deinit(gpa);
    try std.testing.expectEqualStrings("TRIG:SOUR IMM", rendered.bytes);

    args[0] = .{ .string = recipe_ir.Value.String.borrow("EXT") };
    try std.testing.expectError(error.InvalidOptionValue, call.command.render(gpa, stack_buf[0..], args[0..], call.command.instrument.write_termination, null));
}

test "precompile rejects partial bool arg map" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu0.yaml",
        \\instrument: {}
        \\commands:
        \\  output:
        \\    write: "OUTP {state:bool}"
        \\    args:
        \\      state:
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
            try std.testing.expectEqual(@as(usize, 1), comp.save_slot);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 5), compiled.record_bindings.len);
    try std.testing.expectEqual(expr.VariableBinding{ .slot = 1 }, compiled.record_bindings[1]);
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "invalid configuration value type",
    });
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "invalid configuration value type",
    });
}

test "precompile accepts record with declared unassigned variable" {
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

    var compiled = try precompilePath(gpa, std.testing.io, recipe_path, dir, null);
    defer compiled.deinit();

    switch (compiled.pipeline.record.?) {
        .explicit => |columns| {
            try std.testing.expectEqual(@as(usize, 2), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
            try std.testing.expectEqualStrings("nonexistent", columns[1]);
        },
        .all => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 2), compiled.record_bindings.len);
    try std.testing.expectEqual(expr.VariableBinding{ .slot = 0 }, compiled.record_bindings[0]);
    try std.testing.expectEqual(expr.VariableBinding{ .slot = 1 }, compiled.record_bindings[1]);
}

test "precompile rejects record column referencing const" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    try workspace.writeFile("recipes/record_const.yaml",
        \\instruments: {}
        \\pipeline:
        \\  record: [limit]
        \\consts:
        \\  limit: 10
        \\tasks:
        \\  - steps: []
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    const recipe_path = try workspace.realpathAlloc("recipes/record_const.yaml");
    defer gpa.free(recipe_path);

    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    _ = precompilePath(gpa, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "pipeline record references const '\x1b[4mlimit\x1b[0m'"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "consts are compile-time values and cannot be recorded"));
        try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "Declare it in 'vars'"));
        return;
    };

    return error.TestUnexpectedResult;
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
            try std.testing.expectEqual(@as(usize, 5), columns.len);
            try std.testing.expectEqualStrings("voltage", columns[0]);
            try std.testing.expectEqualStrings("doubled", columns[1]);
            try std.testing.expectEqualStrings("$ITER", columns[2]);
            try std.testing.expectEqualStrings("$TASK_IDX", columns[3]);
            try std.testing.expectEqualStrings("$ELAPSED_MS", columns[4]);
        },
        .all => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 5), compiled.record_bindings.len);
    try std.testing.expectEqual(expr.VariableBinding{ .slot = 0 }, compiled.record_bindings[0]);
    try std.testing.expectEqual(expr.VariableBinding{ .slot = 1 }, compiled.record_bindings[1]);
    try std.testing.expectEqual(expr.VariableBinding{ .builtin = .iter }, compiled.record_bindings[2]);
    try std.testing.expectEqual(expr.VariableBinding{ .builtin = .task_idx }, compiled.record_bindings[3]);
    try std.testing.expectEqual(expr.VariableBinding{ .builtin = .elapsed_ms }, compiled.record_bindings[4]);
}

test "precompile rejects undeclared variable use" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage:float}"
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "variable '\x1b[4mundeclared_var\x1b[0m' is not declared in recipe 'vars' section",
    });
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "variable '\x1b[4mx\x1b[0m' is not declared in recipe 'vars' section",
    });
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "variable name '\x1b[4m$ITER\x1b[0m' conflicts with a built-in variable",
    });
}

test "precompile sequential task" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "VOLT {voltage:float}"
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
        \\    write: "VOLT {voltage:float}"
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
        \\    write: "VOLT {voltage:float}"
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
        \\    write: "VOLT {voltage:float}"
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
            try std.testing.expectEqual(@as(usize, 3), items.len());
            switch (items.items()[0]) {
                .float => |v| try std.testing.expectApproxEqAbs(@as(f64, 1.5), v, 1e-9),
                else => return error.TestUnexpectedResult,
            }
            switch (items.items()[2]) {
                .float => |v| try std.testing.expectApproxEqAbs(@as(f64, 4.5), v, 1e-9),
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    try std.testing.expectEqual(@as(usize, 2), compiled.list_slot_capacities.len);
    try std.testing.expectEqual(@as(usize, 0), compiled.list_slot_capacities[0]);
    try std.testing.expectEqual(@as(usize, 3), compiled.list_slot_capacities[1]);
}

test "precompile const-folds join() in step args" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage:float},(@{channels:list})"
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
        \\    write: "V {voltage:float}"
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
        \\    write: "V {voltage:float}"
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "cannot assign to const variable '\x1b[4mfixed\x1b[0m'",
    });
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

    try expectPrecompileAnalysisFail(gpa, adapter_dir, recipe_path, &.{
        "const and var sections both define variable '\x1b[4mx\x1b[0m'",
    });
}

test "precompile does not fold expressions referencing runtime vars" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata: {}
        \\commands:
        \\  set:
        \\    write: "V {voltage:float}"
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
    switch (voltage_arg) {
        .scalar => |e| {
            try std.testing.expectEqual(@as(usize, 3), e.ops.len);
            switch (e.ops[0]) {
                .load_var => |binding| switch (binding) {
                    .slot => |slot| try std.testing.expectEqual(@as(usize, 0), slot),
                    .builtin => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
            switch (e.ops[1]) {
                .push_int => |value| try std.testing.expectEqual(@as(i64, 2), value),
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(e.ops[2] == .mul);
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
        \\    write: "V {voltage:float}"
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

fn expectPrecompileAnalysisFail(
    allocator: std.mem.Allocator,
    adapter_dir_path: []const u8,
    recipe_path: []const u8,
    expected_messages: []const []const u8,
) !void {
    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir_path, .{});
    defer dir.close(std.testing.io);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    _ = precompilePath(allocator, std.testing.io, recipe_path, dir, &out.writer) catch |err| {
        try std.testing.expectEqual(error.AnalysisFail, err);
        for (expected_messages) |message| {
            try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, message));
        }
        return;
    };

    return error.TestUnexpectedResult;
}
