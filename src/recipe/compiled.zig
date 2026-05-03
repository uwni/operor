const std = @import("std");
const instrument = @import("../instrument.zig");
const expr = @import("../expr.zig");

pub const ResponseSpec = instrument.ResponseSpec;

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
    /// Runtime string value that either borrows arena/static bytes or owns a mutable buffer.
    pub const String = union(enum) {
        /// Caller-owned bytes; lifetime must outlive the consumer of this `Value`.
        borrowed: []const u8,
        /// Allocator-owned buffer with a logical length that may be shorter than capacity.
        owned: struct { items: []u8, len: usize },

        pub fn borrow(items_: []const u8) String {
            return .{ .borrowed = items_ };
        }

        pub fn deinit(self: *String, allocator: std.mem.Allocator) void {
            switch (self.*) {
                .borrowed => {},
                .owned => |buffer| allocator.free(buffer.items),
            }
            self.* = undefined;
        }

        pub fn items(self: String) []const u8 {
            return switch (self) {
                .borrowed => |v| v,
                .owned => |b| b.items[0..b.len],
            };
        }
    };

    /// Runtime list value that either borrows immutable items or owns a mutable backing buffer.
    pub const List = union(enum) {
        /// Caller-owned items; lifetime must outlive the consumer of this `Value`.
        borrowed: []const Value,
        /// Allocator-owned item buffer with a logical length that may be shorter than capacity.
        owned: struct { items: []Value, len: usize },

        pub fn borrow(items_: []const Value) List {
            return .{ .borrowed = items_ };
        }

        pub fn deinit(self: *List, allocator: std.mem.Allocator) void {
            switch (self.*) {
                .borrowed => {},
                .owned => |buffer| allocator.free(buffer.items),
            }
            self.* = undefined;
        }

        pub fn len(self: List) usize {
            return self.items().len;
        }

        pub fn items(self: List) []const Value {
            return switch (self) {
                .borrowed => |v| v,
                .owned => |b| b.items[0..b.len],
            };
        }

        pub fn mutItems(self: *List) []Value {
            return switch (self.*) {
                .borrowed => unreachable,
                .owned => |*b| b.items[0..b.len],
            };
        }

        pub fn ensureOwnedCapacity(self: *List, allocator: std.mem.Allocator, capacity: usize) !void {
            if (self.* == .owned and self.owned.items.len >= capacity) return;
            const old_items = self.items();
            const replacement = try allocator.alloc(Value, @max(capacity, old_items.len));
            @memcpy(replacement[0..old_items.len], old_items);
            switch (self.*) {
                .borrowed => {},
                .owned => |buffer| allocator.free(buffer.items),
            }
            self.* = .{ .owned = .{ .items = replacement, .len = old_items.len } };
        }

        pub fn setLen(self: *List, length: usize) void {
            switch (self.*) {
                .borrowed => unreachable,
                .owned => |*buffer| {
                    std.debug.assert(length <= buffer.items.len);
                    buffer.len = length;
                },
            }
        }
    };

    /// Floating-point scalar used by expressions and command rendering.
    float: f64,
    /// Integer scalar used by expressions and command rendering.
    int: i64,
    /// Boolean scalar, optionally mapped to adapter-specific text at render/parse boundaries.
    bool: bool,
    /// UTF-8/text payload; ownership is carried by the nested `String` tag.
    string: String,
    /// Ordered scalar values; nested lists are intentionally not supported by runtime slots.
    list: List,

    pub fn toResolvedValue(self: Value) expr.ResolvedValue {
        return switch (self) {
            .float => |f| .{ .float = f },
            .int => |i| .{ .int = i },
            .bool => |b| .{ .bool = b },
            .string => |s| .{ .string = s.items() },
            .list => unreachable, // lists are resolved by Context directly
        };
    }

    pub fn isEmpty(self: Value) bool {
        return switch (self) {
            .string => |s| s.items().len == 0,
            .list => |items| items.len() == 0,
            else => false,
        };
    }

    pub fn format(self: Value, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .float => |f| try writer.print("{d}", .{f}),
            .int => |i| try writer.print("{d}", .{i}),
            .bool => |b| try writer.writeAll(if (b) "true" else "false"),
            .string => |s| try writer.writeAll(s.items()),
            .list => |list| {
                for (list.items(), 0..) |item, idx| {
                    if (idx > 0) try writer.writeAll(", ");
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
    pub fn deinit(self: *RenderedCommand, allocator: std.mem.Allocator) void {
        if (self.owned) |buffer| allocator.free(buffer);
        self.* = undefined;
    }
};

/// Arena-owned command template segment after placeholder names have been resolved to indices.
pub const CompiledSegment = union(enum) {
    /// Literal bytes copied directly into the rendered command.
    literal: []const u8,
    /// Index into `PrecompiledCommand.args` and the per-step resolved argument array.
    arg: usize,
    /// Nested segment list rendered only when at least one contained argument is non-empty.
    optional: []CompiledSegment,
};

/// Precompiled step argument expression aligned to one command placeholder.
pub const StepArg = union(enum) {
    /// Single expression evaluated to one runtime value immediately before rendering.
    scalar: expr.Expression,
    /// Expressions evaluated independently, then rendered with the placeholder's list separator.
    list: []expr.Expression,

    fn deinit(self: *StepArg, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .scalar => |*expr_value| expr_value.deinit(allocator),
            .list => |items| {
                for (items) |*expr_value| expr_value.deinit(allocator);
                allocator.free(items);
            },
        }
        self.* = undefined;
    }
};

/// Optional text mapping for boolean values.
pub const BoolTextMap = struct {
    true_text: []const u8,
    false_text: []const u8,
};

/// Key-value pair for an option arg: `key` is what the recipe specifies, `value` is the rendered SCPI string.
pub const OptionEntry = struct {
    key: []const u8,
    value: []const u8,
};

/// Per-placeholder formatting rules resolved from adapter defaults and command arg specs.
pub const ArgFormat = struct {
    bool_map: ?BoolTextMap = null,
    list_separator: ?[]const u8 = null,
    float_precision: ?u8 = null,
    option_entries: ?[]const OptionEntry = null,

    pub fn deinit(self: *ArgFormat, allocator: std.mem.Allocator) void {
        if (self.bool_map) |map| {
            allocator.free(map.true_text);
            allocator.free(map.false_text);
        }
        if (self.list_separator) |text| allocator.free(text);
        if (self.option_entries) |entries| {
            for (entries) |entry| {
                allocator.free(entry.key);
                allocator.free(entry.value);
            }
            allocator.free(entries);
        }
        self.* = undefined;
    }
};

/// Metadata for one unique placeholder in a precompiled command.
pub const CommandArg = struct {
    name: []const u8,
    is_optional: bool,
    default: ?StepArg = null,
    format: ArgFormat = .{},
};

/// Executable command prepared during recipe precompilation.
pub const PrecompiledCommand = struct {
    /// Precompiled instrument that owns this command.
    instrument: *const PrecompiledInstrument,
    /// Response parsing spec declared by the source adapter command.
    response: ?ResponseSpec,
    /// Allocator-owned compiled render segments used at execution time.
    segments: []CompiledSegment,
    /// Placeholder metadata used for validation, defaults, and rendering.
    args: []CommandArg,
    /// Errors that can occur while rendering the precompiled template.
    pub const RenderError = error{
        BufferTooSmall,
        OutOfMemory,
        InvalidOptionValue,
        MissingListSeparator,
    };

    /// Releases heap-owned template and placeholder data.
    pub fn deinit(self: *PrecompiledCommand, allocator: std.mem.Allocator) void {
        for (self.args) |*arg| {
            allocator.free(arg.name);
            if (arg.default) |*default_arg| default_arg.deinit(allocator);
            arg.format.deinit(allocator);
        }
        allocator.free(self.args);
        if (self.response) |response| response.deinit(allocator);
        freeCompiledSegments(allocator, self.segments);
        self.* = undefined;
    }

    /// Returns whether the compiled template expects a given placeholder.
    pub fn hasPlaceholder(self: *const PrecompiledCommand, name: []const u8) bool {
        return self.argIndex(name) != null;
    }

    /// Returns the stable placeholder index assigned during template compilation.
    pub fn argIndex(self: *const PrecompiledCommand, name: []const u8) ?usize {
        for (self.args, 0..) |arg, idx| {
            if (std.mem.eql(u8, arg.name, name)) return idx;
        }
        return null;
    }

    /// Renders the command plus optional suffix using a stack buffer with automatic heap fallback.
    pub fn render(
        self: *const PrecompiledCommand,
        allocator: std.mem.Allocator,
        stack_buffer: []u8,
        args: []const Value,
        suffix: []const u8,
        float_precision: ?u8,
    ) RenderError!RenderedCommand {
        if (stack_buffer.len >= suffix.len) {
            const render_buffer = stack_buffer[0 .. stack_buffer.len - suffix.len];
            var w: std.Io.Writer = .fixed(render_buffer);
            renderInternal(&w, self.segments, args, self.args, float_precision) catch |err| switch (err) {
                error.WriteFailed => {
                    const owned = try renderAllocWithSuffix(allocator, self.segments, args, self.args, suffix, float_precision);
                    return .{ .bytes = owned, .owned = owned };
                },
                error.InvalidOptionValue => return error.InvalidOptionValue,
                error.MissingListSeparator => return error.MissingListSeparator,
                else => unreachable,
            };

            const rendered = w.buffered();
            const combined_len = rendered.len + suffix.len;
            @memcpy(stack_buffer[rendered.len..combined_len], suffix);
            return .{ .bytes = stack_buffer[0..combined_len] };
        }

        const owned = try renderAllocWithSuffix(allocator, self.segments, args, self.args, suffix, float_precision);
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
    /// Optional adapter-level bool read/write mappings.
    bool_map: ?BoolTextMap = null,
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

    /// Where a parsed instrument response is stored after execution.
    pub const SaveResult = union(enum) {
        /// Single scalar or list slot index.
        scalar: usize,
        /// One slot index per object field, aligned to `ResponseSpec.object.fields`.
        object: []const usize,
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
        /// Compiled argument values aligned to `command.args`.
        args: []const StepArg,
        /// Where to store the parsed response, if the recipe requests it.
        save_result: ?SaveResult = null,
    };

    pub const Compute = struct {
        /// Expression to evaluate (pre-parsed AST).
        expression: expr.Expression,
        /// Slot that receives the computed result.
        save_slot: usize,
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
pub const Task = struct {
    /// Display name shown in plan and log output.
    name: []const u8,
    /// When false, this task does not increment $ITER and produces no pipeline frame.
    /// Defaults to true.
    iter: bool = true,
    kind: Kind,

    pub const Kind = union(enum) {
        /// Runs steps in a loop while the condition is truthy.
        loop: LoopTask,
        /// Runs steps exactly once in order.
        sequential: SequentialTask,
        /// Runs steps once if the condition is truthy.
        conditional: ConditionalTask,
    };

    pub fn steps(self: *const Task) []Step {
        return switch (self.kind) {
            inline else => |t| t.steps,
        };
    }
};

/// Loop task: re-executes steps while the condition remains truthy.
pub const LoopTask = struct {
    condition: expr.Expression,
    steps: []Step,
};

/// Sequential task: runs steps exactly once in declaration order.
pub const SequentialTask = struct {
    steps: []Step,
};

/// Conditional task: runs steps once when the guard expression is truthy.
pub const ConditionalTask = struct {
    @"if": expr.Expression,
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
    /// Optional port for the HTTP/WebSocket API server.
    api_port: ?u16 = null,
    /// Declares which runtime variables to record as frame columns.
    /// Use `"all"` to record every recipe var plus built-ins, or list names explicitly.
    record: ?ColumnConfig = null,
    /// Optional live echo: displays these variables on a single overwriting CLI line.
    echo: ?ColumnConfig = null,

    pub fn clone(
        cfg: *const PipelineConfig,
        allocator: std.mem.Allocator,
    ) !PipelineConfig {
        return .{
            .buffer_size = cfg.buffer_size,
            .warn_usage_percent = cfg.warn_usage_percent,
            .mode = cfg.mode,
            .file_path = if (cfg.file_path) |path| try allocator.dupe(u8, path) else null,
            .api_port = cfg.api_port,
            .record = if (cfg.record) |r| try r.clone(allocator) else null,
            .echo = if (cfg.echo) |r| try r.clone(allocator) else null,
        };
    }
};

const serde_lib = @import("serde");

/// Controls which runtime variables are persisted by pipeline sinks.
pub const ColumnConfig = union(enum) {
    /// Record every recipe var and built-in variable.
    all: []const u8,
    /// Record only the listed recipe var or built-in names.
    explicit: []const []const u8,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };

    pub fn clone(self: *const ColumnConfig, allocator: std.mem.Allocator) !ColumnConfig {
        return switch (self.*) {
            .all => |value| .{ .all = try allocator.dupe(u8, value) },
            .explicit => |columns| blk: {
                const items = try allocator.alloc([]const u8, columns.len);
                for (columns, 0..) |name, idx| {
                    items[idx] = try allocator.dupe(u8, name);
                }
                break :blk .{ .explicit = items };
            },
        };
    }
};

/// Fully validated recipe ready for preview or execution.
/// Owns arena-backed data and should have a single logical owner until `deinit`.
pub const PrecompiledRecipe = struct {
    arena: std.heap.ArenaAllocator,
    instruments: std.StringArrayHashMapUnmanaged(PrecompiledInstrument),
    tasks: []Task,
    pipeline: PipelineConfig,
    /// Runtime variable sources for pipeline columns, aligned with `pipeline.record.explicit`.
    /// `.slot` values are Context slot indices, not record column indices.
    record_bindings: []const expr.VariableBinding,
    /// Variable bindings for the CLI echo display, aligned with `pipeline.echo.explicit`.
    echo_bindings: []const expr.VariableBinding,
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
    /// Runtime var slot aligned list backing capacities used by `Context`.
    list_slot_capacities: []const usize,

    /// Releases all arena-owned precompiled recipe data.
    pub fn deinit(self: *PrecompiledRecipe) void {
        self.arena.deinit();
    }
};

fn writeValueWithFormat(writer: anytype, value: Value, fmt: ArgFormat, float_precision: ?u8) !void {
    switch (value) {
        .bool => |b| {
            if (fmt.option_entries != null) return error.InvalidOptionValue;
            if (fmt.bool_map) |map| {
                try writer.writeAll(if (b) map.true_text else map.false_text);
                return;
            }
            try writer.writeAll(if (b) "true" else "false");
        },
        .float => |f| {
            if (fmt.option_entries != null) return error.InvalidOptionValue;
            if (fmt.float_precision orelse float_precision) |precision| {
                try writeFloatFixed(writer, f, precision);
            } else {
                try writer.print("{d}", .{f});
            }
        },
        .int => |i| {
            if (fmt.option_entries != null) return error.InvalidOptionValue;
            try writer.print("{d}", .{i});
        },
        .string => |s| {
            const text = s.items();
            if (fmt.option_entries) |entries| {
                for (entries) |entry| {
                    if (std.mem.eql(u8, entry.key, text)) {
                        try writer.writeAll(entry.value);
                        return;
                    }
                }
                return error.InvalidOptionValue;
            }
            try writer.writeAll(text);
        },
        .list => |items| {
            const sep = fmt.list_separator orelse return error.MissingListSeparator;
            for (items.items(), 0..) |item, idx| {
                if (idx > 0) try writer.writeAll(sep);
                // List elements inherit the same placeholder formatting rules.
                try writeValueWithFormat(writer, item, fmt, float_precision);
            }
        },
    }
}

fn containsOptionKey(entries: []const OptionEntry, key: []const u8) bool {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return true;
    }
    return false;
}

fn renderInternal(
    writer: anytype,
    segments: []const CompiledSegment,
    args: []const Value,
    arg_meta: []const CommandArg,
    float_precision: ?u8,
) !void {
    for (segments) |segment| {
        switch (segment) {
            .literal => |literal| try writer.writeAll(literal),
            .arg => |arg_idx| {
                const fmt = arg_meta[arg_idx].format;
                try writeValueWithFormat(writer, args[arg_idx], fmt, float_precision);
            },
            .optional => |inner| {
                // Optional groups are present only when at least one inner argument has content.
                const has_value = for (inner) |s| {
                    if (s == .arg and args[s.arg].isEmpty()) continue;
                    if (s == .arg) break true;
                } else false;
                if (has_value) try renderInternal(writer, inner, args, arg_meta, float_precision);
            },
        }
    }
}

fn freeCompiledSegments(allocator: std.mem.Allocator, segments: []CompiledSegment) void {
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
    args: []const Value,
    arg_meta: []const CommandArg,
    suffix: []const u8,
    float_precision: ?u8,
) PrecompiledCommand.RenderError![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    renderInternal(&out.writer, segments, args, arg_meta, float_precision) catch |err| switch (err) {
        error.WriteFailed => return error.OutOfMemory,
        error.InvalidOptionValue => return error.InvalidOptionValue,
        error.MissingListSeparator => return error.MissingListSeparator,
    };

    out.writer.writeAll(suffix) catch return error.OutOfMemory;
    return out.toOwnedSlice() catch error.OutOfMemory;
}
