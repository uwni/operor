const std = @import("std");
const instrument = @import("../instrument.zig");
const diagnostic = @import("diagnostic.zig");
const expr = @import("../expr.zig");

/// Writes a float with exactly `decimal_places` digits after the decimal point,
/// using the standard library's Ryu-based decimal formatter.
fn writeFloatFixed(writer: anytype, value: f64, decimal_places: u8) !void {
    var buf: [std.fmt.float.bufferSize(.decimal, f64)]u8 = undefined;
    const formatted = std.fmt.float.render(&buf, value, .{
        .mode = .decimal,
        .precision = decimal_places,
    }) catch unreachable; // buffer is statically sized to fit any f64
    try writer.writeAll(formatted);
}

pub const Value = union(enum) {
    float: f64,
    int: i64,
    bool: bool,
    string: []const u8,
    list: []const Value,

    pub fn toResolvedValue(self: Value) expr.ResolvedValue {
        return switch (self) {
            .float => |f| .{ .float = f },
            .int => |i| .{ .int = i },
            .bool => |b| .{ .bool = b },
            .string => |s| .{ .string = s },
            .list => unreachable, // lists are resolved by Context directly
        };
    }

    pub fn format(self: Value, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .float => |f| try writer.print("{d}", .{f}),
            .int => |i| try writer.print("{d}", .{i}),
            .bool => |b| try writer.writeAll(if (b) "true" else "false"),
            .string => |s| try writer.writeAll(s),
            .list => |items| {
                for (items, 0..) |item, idx| {
                    if (idx > 0) try writer.writeAll(", ");
                    try item.format(writer);
                }
            },
        }
    }
};

/// Render-time value used by command templates.
pub const RenderValue = union(enum) {
    scalar: Value,
    list: []const Value,

    /// Returns true when this value would produce no output when rendered.
    pub fn isEmpty(self: RenderValue) bool {
        return switch (self) {
            .scalar => |v| switch (v) {
                .string => |s| s.len == 0,
                else => false,
            },
            .list => |items| items.len == 0,
        };
    }

    pub fn format(self: RenderValue, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .scalar => |value| try value.format(writer),
            .list => |items| {
                for (items, 0..) |item, idx| {
                    if (idx > 0) try writer.writeByte(',');
                    try item.format(writer);
                }
            },
        }
    }
};

/// Borrowed-or-owned bytes produced by rendering a precompiled command.
pub const RenderedCommand = struct {
    /// Rendered bytes that can be sent directly to the instrument.
    bytes: []const u8,
    /// Heap allocation to free when the stack buffer was too small.
    owned: ?[]u8 = null,

    /// Releases the owned render buffer when one was allocated.
    pub fn deinit(self: RenderedCommand, allocator: std.mem.Allocator) void {
        if (self.owned) |buffer| allocator.free(buffer);
    }
};

pub const CompiledSegment = union(enum) {
    literal: []const u8,
    arg: usize,
    optional: []const CompiledSegment,
};

pub const StepArg = union(enum) {
    scalar: expr.Expression,
    list: []const expr.Expression,
};

/// Executable command prepared during recipe precompilation.
pub const PrecompiledCommand = struct {
    /// Precompiled instrument that owns this command.
    instrument: *const PrecompiledInstrument,
    /// Response encoding declared by the source adapter command.
    response: ?instrument.Encoding,
    /// Allocator-owned compiled render segments used at execution time.
    segments: []const CompiledSegment,
    /// Unique placeholder names in render order.
    arg_names: []const []const u8,
    /// Errors that can occur while rendering the precompiled template.
    pub const RenderError = error{
        MissingVariable,
        BufferTooSmall,
        OutOfMemory,
    };

    /// Releases heap-owned template and placeholder data.
    pub fn deinit(self: PrecompiledCommand, allocator: std.mem.Allocator) void {
        for (self.arg_names) |name| allocator.free(name);
        allocator.free(self.arg_names);
        freeCompiledSegments(allocator, self.segments);
    }

    /// Returns whether the compiled template expects a given placeholder.
    pub fn hasPlaceholder(self: *const PrecompiledCommand, name: []const u8) bool {
        return self.argIndex(name) != null;
    }

    pub fn argIndex(self: *const PrecompiledCommand, name: []const u8) ?usize {
        for (self.arg_names, 0..) |arg_name, idx| {
            if (std.mem.eql(u8, arg_name, name)) return idx;
        }
        return null;
    }

    /// Renders the command plus optional suffix using a stack buffer with automatic heap fallback.
    pub fn render(
        self: *const PrecompiledCommand,
        allocator: std.mem.Allocator,
        stack_buffer: []u8,
        args: []const RenderValue,
        suffix: []const u8,
        float_precision: ?u8,
    ) RenderError!RenderedCommand {
        if (stack_buffer.len >= suffix.len) {
            const render_buffer = stack_buffer[0 .. stack_buffer.len - suffix.len];
            var w: std.Io.Writer = .fixed(render_buffer);
            renderInternal(&w, self.segments, args, float_precision) catch |err| switch (err) {
                error.WriteFailed => {
                    const owned = try renderAllocWithSuffix(allocator, self.segments, args, suffix, float_precision);
                    return .{ .bytes = owned, .owned = owned };
                },
                else => unreachable,
            };

            const rendered = w.buffered();
            const combined_len = rendered.len + suffix.len;
            @memcpy(stack_buffer[rendered.len..combined_len], suffix);
            return .{ .bytes = stack_buffer[0..combined_len] };
        }

        const owned = try renderAllocWithSuffix(allocator, self.segments, args, suffix, float_precision);
        return .{ .bytes = owned, .owned = owned };
    }
};

/// Recipe instrument bound to a adapter and the subset of commands it actually uses.
pub const PrecompiledInstrument = struct {
    /// Adapter name resolved during precompile.
    adapter_name: []const u8,
    /// VISA resource address for opening the instrument.
    resource: []const u8,
    /// Stable store of precompiled command pointers referenced by this recipe instrument.
    commands: std.StringHashMap(*const PrecompiledCommand),
    /// Suffix appended to every write command (e.g. "\n", "\r\n").
    /// Empty string means no write termination. Owned by the recipe arena.
    write_termination: []const u8,
    /// Session options applied when the runtime opens the instrument.
    options: instrument.InstrumentOptions,
};

/// Parsed and validated recipe step ready for execution.
pub const Step = struct {
    /// Step payload: either an instrument call or a local compute expression.
    action: Action,
    /// Optional guard expression; when present, the step is skipped if the expression evaluates to 0.0.
    @"if": ?expr.Expression = null,

    pub const Action = union(enum) {
        /// Send a command to an instrument and optionally save the response.
        instrument_call: InstrumentCall,
        /// Evaluate an arithmetic expression and store the result.
        compute: Compute,
        /// Pause execution for a fixed duration.
        sleep: Sleep,
        /// A group of independent steps that may be executed in parallel.
        parallel: Parallel,
    };

    pub const InstrumentCall = struct {
        /// Adapter command name preserved for preview and diagnostics.
        call: []const u8,
        /// Recipe instrument name preserved for preview and diagnostics.
        instrument: []const u8,
        /// Zero-based position into the executor runtime array.
        instrument_idx: usize,
        /// Precompiled command resolved during recipe precompile.
        command: *const PrecompiledCommand,
        /// Compiled arguments aligned to `command.arg_names`.
        args: []const StepArg,
        /// Optional slot that receives the parsed response value.
        save_slot: ?usize = null,
        /// Optional column index into the pipeline record for frame persistence.
        save_column: ?usize = null,
    };

    pub const Compute = struct {
        /// Expression to evaluate (pre-parsed AST).
        expression: expr.Expression,
        /// Slot that receives the computed result.
        save_slot: usize,
        /// Optional column index into the pipeline record for frame persistence.
        save_column: ?usize = null,
    };

    pub const Sleep = struct {
        /// Duration to sleep in milliseconds.
        duration_ms: u64,
    };

    pub const Parallel = struct {
        /// Inner steps declared as independent by the user.
        steps: []Step,
    };
};

/// Task variant describing when and how steps are executed.
pub const Task = union(enum) {
    /// Runs steps in a loop while the condition is truthy.
    loop: LoopTask,
    /// Runs steps exactly once in order.
    sequential: SequentialTask,
    /// Runs steps once if the condition is truthy.
    conditional: ConditionalTask,

    /// Returns the step slice for any task variant.
    pub fn steps(self: *const Task) []Step {
        return switch (self.*) {
            .loop => |t| t.steps,
            .sequential => |t| t.steps,
            .conditional => |t| t.steps,
        };
    }
};

/// Loop task: re-executes steps while the condition remains truthy.
pub const LoopTask = struct {
    /// Condition expression; loop continues while this evaluates to truthy.
    condition: expr.Expression,
    /// Steps executed each iteration of the loop.
    steps: []Step,
};

/// Sequential task: runs steps exactly once in declaration order.
pub const SequentialTask = struct {
    /// Steps executed once.
    steps: []Step,
};

/// Conditional task: runs steps once when the guard expression is truthy.
pub const ConditionalTask = struct {
    /// Guard expression; steps execute only when this evaluates to truthy.
    @"if": expr.Expression,
    /// Steps executed if the condition is true.
    steps: []Step,
};

/// Runtime mode presets for the sampling pipeline.
pub const PipelineMode = enum {
    safe,
    realtime,
};

/// Optional pipeline configuration attached to a recipe.
pub const PipelineConfig = struct {
    /// Requested ring buffer size; executor normalizes it to a power of two.
    buffer_size: ?usize = null,
    /// Warning threshold for current buffer usage.
    warn_usage_percent: ?u8 = null,
    /// Optional preset that changes defaults such as buffer sizing and console logging.
    mode: ?PipelineMode = null,
    /// Optional CSV file sink written by the consumer thread.
    file_path: ?[]const u8 = null,
    /// Optional TCP sink host written by the consumer thread.
    network_host: ?[]const u8 = null,
    /// Optional TCP sink port written by the consumer thread.
    network_port: ?u16 = null,
    /// Declares which `assign` variables to record as frame columns.
    /// Use `"all"` to record every `assign` variable, or list names explicitly.
    record: ?RecordConfig = null,
};

/// Controls which `assign` variables are persisted by pipeline sinks.
pub const RecordConfig = union(enum) {
    /// Record every `assign` variable.
    all: []const u8,
    /// Record only the listed variable names.
    explicit: []const []const u8,
};

/// Fully validated recipe ready for preview or execution.
/// Owns arena-backed data and should have a single logical owner until `deinit`.
pub const PrecompiledRecipe = struct {
    arena: std.heap.ArenaAllocator,
    instruments: std.StringArrayHashMapUnmanaged(PrecompiledInstrument),
    tasks: []Task,
    pipeline: PipelineConfig,
    /// Optional stop condition expression; scheduler stops when this evaluates to truthy.
    stop_when: ?expr.Expression,
    /// Estimated total number of task iterations across all tasks.
    /// Null when the recipe runs indefinitely or stop conditions are dynamic.
    expected_iterations: ?u64,
    /// Maximum decimal places for float-to-string conversion in command templates.
    /// Null preserves the full f64 shortest representation.
    float_precision: ?u8,
    /// Default values for slot-based context variables at execution startup.
    initial_values: []const Value,

    /// Releases all arena-owned precompiled recipe data.
    pub fn deinit(self: *PrecompiledRecipe) void {
        self.arena.deinit();
    }

    /// Loads and precompiles a recipe document from disk.
    pub fn precompilePath(
        allocator: std.mem.Allocator,
        io: std.Io,
        recipe_path: []const u8,
        adapter_dir: std.Io.Dir,
        precompile_diagnostic: *diagnostic.PrecompileDiagnostic,
    ) !PrecompiledRecipe {
        return @import("precompile.zig").precompilePath(allocator, io, recipe_path, adapter_dir, precompile_diagnostic);
    }
};

fn renderInternal(writer: anytype, segments: []const CompiledSegment, args: []const RenderValue, float_precision: ?u8) !void {
    for (segments) |segment| {
        switch (segment) {
            .literal => |literal| try writer.writeAll(literal),
            .arg => |arg_idx| {
                const rv = args[arg_idx];
                if (float_precision) |precision| {
                    if (rv == .scalar and rv.scalar == .float) {
                        try writeFloatFixed(writer, rv.scalar.float, precision);
                        continue;
                    }
                }
                try writer.print("{f}", .{rv});
            },
            .optional => |inner| {
                const has_value = for (inner) |s| {
                    if (s == .arg and args[s.arg].isEmpty()) continue;
                    if (s == .arg) break true;
                } else false;
                if (has_value) try renderInternal(writer, inner, args, float_precision);
            },
        }
    }
}

fn freeCompiledSegments(allocator: std.mem.Allocator, segments: []const CompiledSegment) void {
    for (segments) |segment| switch (segment) {
        .literal => |literal| allocator.free(literal),
        .arg => {},
        .optional => |inner| freeCompiledSegments(allocator, inner),
    };
    allocator.free(segments);
}

fn renderAllocWithSuffix(
    allocator: std.mem.Allocator,
    segments: []const CompiledSegment,
    args: []const RenderValue,
    suffix: []const u8,
    float_precision: ?u8,
) PrecompiledCommand.RenderError![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    renderInternal(&out.writer, segments, args, float_precision) catch |err| switch (err) {
        error.WriteFailed => return error.OutOfMemory,
    };

    out.writer.writeAll(suffix) catch return error.OutOfMemory;
    return out.toOwnedSlice() catch error.OutOfMemory;
}
