const std = @import("std");
const Driver = @import("../driver/Driver.zig");
const recipe_mod = @import("../recipe/root.zig");
const common = @import("common.zig");
const pipeline_mod = @import("pipeline/root.zig");
const expr = @import("../expr.zig");

const command_stack_bytes: usize = 512;
/// Parsed response value in the native Zig type indicated by the command encoding.
pub const ParsedValue = union(Driver.Encoding) {
    raw: []const u8,
    float: f64,
    int: i64,
    string: []const u8,
};

pub const SavedValue = struct {
    label: []const u8,
    value_owned: []u8,

    pub fn deinit(self: *SavedValue, allocator: std.mem.Allocator) void {
        allocator.free(self.value_owned);
    }
};

/// Reusable scratch space for step argument resolution, avoiding per-step HashMap allocation.
pub const StepScratch = struct {
    values: std.StringHashMap([]const u8),
    temp_arena: std.heap.ArenaAllocator,

    pub fn init(allocator: std.mem.Allocator) StepScratch {
        return .{
            .values = std.StringHashMap([]const u8).init(allocator),
            .temp_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *StepScratch) void {
        self.temp_arena.deinit();
        self.values.deinit();
    }

    fn tempAllocator(self: *StepScratch) std.mem.Allocator {
        return self.temp_arena.allocator();
    }

    fn formatTemp(self: *StepScratch, comptime fmt: []const u8, args: anytype) ![]const u8 {
        return try std.fmt.allocPrint(self.tempAllocator(), fmt, args);
    }

    /// Clears resolved values and reuses retained arena capacity for temporary buffers.
    pub fn reset(self: *StepScratch) void {
        _ = self.temp_arena.reset(.retain_capacity);
        self.values.clearRetainingCapacity();
    }
};

/// Renders, sends, and optionally parses the response for a single step.
/// Supports both instrument call steps and local compute steps.
pub fn executeStep(
    allocator: std.mem.Allocator,
    instrument: ?*common.InstrumentRuntime,
    step: *const recipe_mod.Step,
    ctx: *common.Context,
    dry_run: bool,
    log_sink: pipeline_mod.AsyncLog,
    scratch: *StepScratch,
) !?SavedValue {
    // Evaluate optional `when` guard.
    if (step.when) |*when_expr| {
        const is_true = when_expr.isTruthy(ctx.varResolver()) catch |err| switch (err) {
            error.VariableNotFound => {
                logWarning(log_sink, "when guard: variable not found, skipping step");
                return null;
            },
            error.InvalidNumber => {
                logWarning(log_sink, "when guard: invalid number in variable, skipping step");
                return null;
            },
            else => return err,
        };
        if (!is_true) return null;
    }

    return switch (step.action) {
        .instrument_call => |ic| executeInstrumentCall(allocator, instrument.?, &ic, ctx, dry_run, log_sink, scratch),
        .compute => |comp| executeCompute(allocator, &comp, ctx, log_sink),
    };
}

/// Evaluates a local compute expression and stores the result in the context.
fn executeCompute(
    allocator: std.mem.Allocator,
    comp: *const recipe_mod.Step.Compute,
    ctx: *common.Context,
    log_sink: pipeline_mod.AsyncLog,
) !?SavedValue {
    const result = comp.expression.eval(ctx.varResolver()) catch |err| switch (err) {
        error.VariableNotFound => {
            logWarning(log_sink, "compute: variable not found");
            return null;
        },
        error.InvalidNumber => {
            logWarning(log_sink, "compute: invalid number in variable");
            return null;
        },
        error.DivisionByZero => {
            logWarning(log_sink, "compute: division by zero");
            return null;
        },
        else => return err,
    };

    const value_owned = try std.fmt.allocPrint(allocator, "{d}", .{result});
    errdefer allocator.free(value_owned);
    try ctx.set(comp.save_as, value_owned);

    return .{
        .label = comp.save_as,
        .value_owned = value_owned,
    };
}

/// Sends an instrument command and optionally saves the parsed response.
fn executeInstrumentCall(
    allocator: std.mem.Allocator,
    instrument: *common.InstrumentRuntime,
    step: *const recipe_mod.Step.InstrumentCall,
    ctx: *common.Context,
    dry_run: bool,
    log_sink: pipeline_mod.AsyncLog,
    scratch: *StepScratch,
) !?SavedValue {
    const cmd = step.command;
    const driver_name = cmd.instrument.driver_name;

    scratch.reset();

    var arg_it = step.args.iterator();
    while (arg_it.next()) |entry| {
        switch (entry.value_ptr.*) {
            .scalar => |value| {
                const resolved = try resolveStepScalar(ctx, scratch, value);
                try scratch.values.put(entry.key_ptr.*, resolved);
            },
            .list => |items| {
                const alloc = scratch.tempAllocator();
                var joined = std.ArrayList(u8).empty;
                defer joined.deinit(alloc);
                for (items, 0..) |item, idx| {
                    const resolved = try resolveStepScalar(ctx, scratch, item);
                    if (idx > 0) try joined.append(alloc, ',');
                    try joined.appendSlice(alloc, resolved);
                }
                const joined_value = try joined.toOwnedSlice(alloc);
                try scratch.values.put(entry.key_ptr.*, joined_value);
            },
        }
    }

    var render_stack_buf: [command_stack_bytes]u8 = undefined;
    const write_termination = step.command.instrument.write_termination;
    const rendered = cmd.render(allocator, render_stack_buf[0..], &scratch.values, write_termination) catch |err| switch (err) {
        error.MissingVariable => {
            var warning_buf: [160]u8 = undefined;
            const warning = try std.fmt.bufPrint(warning_buf[0..], "missing template variable for call {s}", .{step.call});
            logWarning(log_sink, warning);
            return null;
        },
        else => return err,
    };
    defer rendered.deinit(allocator);

    if (dry_run) {
        logDryRun(log_sink, driver_name, rendered.bytes);
        return null;
    }

    const instr = &(instrument.handle orelse unreachable);
    instr.write(rendered.bytes) catch |err| {
        var warning_buf: [192]u8 = undefined;
        const warning = try std.fmt.bufPrint(warning_buf[0..], "write failed {s}: {any}", .{ driver_name, err });
        logWarning(log_sink, warning);
        return null;
    };

    if (cmd.response) |encoding| {
        instr.waitQueryDelay();
        const resp = instr.readToOwned(allocator) catch |err| {
            var warning_buf: [192]u8 = undefined;
            const warning = try std.fmt.bufPrint(warning_buf[0..], "read failed {s}: {any}", .{ driver_name, err });
            logWarning(log_sink, warning);
            return null;
        };
        errdefer allocator.free(resp);
        if (step.save_as) |label| {
            defer allocator.free(resp);
            const stored = try parseResponse(encoding, resp);
            const stored_value_owned = switch (stored) {
                .raw => |value| try allocator.dupe(u8, value),
                .string => |value| try allocator.dupe(u8, value),
                .int => |value| try std.fmt.allocPrint(allocator, "{d}", .{value}),
                .float => |value| try std.fmt.allocPrint(allocator, "{d}", .{value}),
            };
            errdefer allocator.free(stored_value_owned);
            try ctx.set(label, stored_value_owned);
            return .{
                .label = label,
                .value_owned = stored_value_owned,
            };
        } else {
            defer allocator.free(resp);
            return null;
        }
    }

    return null;
}

/// Convert the raw response byte string into a ParsedValue according to the specified encoding.
pub fn parseResponse(encoding: Driver.Encoding, resp: []const u8) !ParsedValue {
    const trimmed = std.mem.trim(u8, resp, &std.ascii.whitespace);
    return switch (encoding) {
        .raw => .{ .raw = resp },
        .float => .{ .float = try std.fmt.parseFloat(f64, trimmed) },
        .int => .{ .int = try std.fmt.parseInt(i64, trimmed, 10) },
        .string => .{ .string = trimmed },
    };
}

/// Resolves `${name}` references from the execution context or returns the literal argument unchanged.
pub fn resolveArg(ctx: *const common.Context, arg: []const u8) ![]const u8 {
    if (arg.len >= 4 and std.mem.startsWith(u8, arg, "${") and std.mem.endsWith(u8, arg, "}")) {
        return resolveReference(ctx, arg[2 .. arg.len - 1]);
    }
    return arg;
}

fn resolveReference(ctx: *const common.Context, key: []const u8) ![]const u8 {
    if (ctx.get(key)) |val| return val;
    return error.MissingArgument;
}

fn resolveStepScalar(
    ctx: *const common.Context,
    scratch: *StepScratch,
    value: recipe_mod.StepScalar,
) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        .int => |number| try scratch.formatTemp("{d}", .{number}),
        .float => |number| try scratch.formatTemp("{d}", .{number}),
        .bool => |flag| if (flag) "true" else "false",
        .ref => |key| try resolveReference(ctx, key),
    };
}

fn logWarning(log_sink: pipeline_mod.AsyncLog, message: []const u8) void {
    log_sink.print("[WARN] {s}\n", .{message});
}

fn logDryRun(log_sink: pipeline_mod.AsyncLog, driver_name: []const u8, rendered: []const u8) void {
    log_sink.print("[dry-run] {s} -> {s}\n", .{ driver_name, rendered });
}

test "executor resolve arg" {
    var ctx = common.Context.init(std.testing.allocator);
    defer ctx.deinit();
    var scratch = StepScratch.init(std.testing.allocator);
    defer scratch.deinit();

    try ctx.set("v", "5.0");
    const resolved = try resolveArg(&ctx, "${v}");
    try std.testing.expectEqualStrings("5.0", resolved);
    const literal = try resolveArg(&ctx, "3.3");
    try std.testing.expectEqualStrings("3.3", literal);

    const typed_ref = try resolveStepScalar(&ctx, &scratch, .{ .ref = "v" });
    try std.testing.expectEqualStrings("5.0", typed_ref);
    const typed_int = try resolveStepScalar(&ctx, &scratch, .{ .int = 7 });
    try std.testing.expectEqualStrings("7", typed_int);
    const typed_float = try resolveStepScalar(&ctx, &scratch, .{ .float = 3.25 });
    try std.testing.expectEqualStrings("3.25", typed_float);
    const typed_bool = try resolveStepScalar(&ctx, &scratch, .{ .bool = true });
    try std.testing.expectEqualStrings("true", typed_bool);
    const typed_string = try resolveStepScalar(&ctx, &scratch, .{ .string = "literal" });
    try std.testing.expectEqualStrings("literal", typed_string);
}

test "executor parse response" {
    const raw = "  2.5 \n";
    const parsed = try parseResponse(.float, raw);
    switch (parsed) {
        .float => |value| try std.testing.expectApproxEqAbs(@as(f64, 2.5), value, 1e-9),
        else => return error.TestUnexpectedResult,
    }

    const parsed_int = try parseResponse(.int, "7");
    switch (parsed_int) {
        .int => |value| try std.testing.expectEqual(@as(i64, 7), value),
        else => return error.TestUnexpectedResult,
    }
}
