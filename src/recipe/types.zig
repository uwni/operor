const std = @import("std");
const serde_lib = @import("serde");
const Driver = @import("../driver/Driver.zig");
const template = @import("../driver/template.zig");
const DriverRegistry = @import("../driver/DriverRegistry.zig");
const diagnostic = @import("diagnostic.zig");
const expr = @import("../expr.zig");
const visa = @import("../visa/root.zig");

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

/// Executable command prepared during recipe precompilation.
pub const PrecompiledCommand = struct {
    /// Precompiled instrument that owns this command.
    instrument: *const PrecompiledInstrument,
    /// Response encoding declared by the source driver command.
    response: ?Driver.Encoding,
    /// Allocator-owned cloned template segments used at execution time.
    template: []const template.Segment,
    /// Unique placeholder names referenced by the command template.
    placeholders: []const []const u8,
    /// Errors that can occur while rendering the precompiled template.
    pub const RenderError = template.RenderError;

    /// Releases heap-owned template and placeholder data.
    pub fn deinit(self: PrecompiledCommand, allocator: std.mem.Allocator) void {
        allocator.free(self.placeholders);
        template.freeSegments(allocator, self.template);
    }

    /// Returns whether the compiled template expects a given placeholder.
    pub fn hasPlaceholder(self: *const PrecompiledCommand, name: []const u8) bool {
        for (self.placeholders) |placeholder| {
            if (std.mem.eql(u8, placeholder, name)) return true;
        }
        return false;
    }

    /// Renders the command plus optional suffix using a stack buffer with automatic heap fallback.
    pub fn render(
        self: *const PrecompiledCommand,
        allocator: std.mem.Allocator,
        stack_buffer: []u8,
        values: anytype,
        suffix: []const u8,
    ) RenderError!RenderedCommand {
        if (template.renderIntoWithSuffix(stack_buffer, self.template, values, suffix)) |rendered| {
            return .{ .bytes = rendered };
        } else |err| switch (err) {
            error.BufferTooSmall => {},
            else => return err,
        }

        const owned = try template.renderAllocWithSuffix(allocator, self.template, values, suffix);
        return .{ .bytes = owned, .owned = owned };
    }
};

/// Parsed representation of a single step argument item.
pub const StepScalar = union(enum) {
    /// String literal preserved as-is.
    string: []const u8,
    /// Integer literal preserved until execution-time rendering.
    int: i64,
    /// Floating-point literal preserved until execution-time rendering.
    float: f64,
    /// Boolean literal preserved until execution-time rendering.
    bool: bool,
    /// Execution-context reference declared as `${name}`. Stores the bare `name` key.
    ref: []const u8,
};

/// Parsed representation of a step argument value.
pub const StepArg = union(enum) {
    /// Single typed literal or `${name}` reference.
    scalar: StepScalar,
    /// Ordered list argument preserved item-by-item.
    list: []const StepScalar,
};

/// Recipe instrument bound to a driver and the subset of commands it actually uses.
pub const PrecompiledInstrument = struct {
    /// Zero-based position into the executor runtime array.
    instrument_idx: usize,
    /// Driver name resolved during precompile.
    driver_name: []const u8,
    /// VISA resource address for opening the instrument.
    resource: []const u8,
    /// Stable store of precompiled command pointers referenced by this recipe instrument.
    commands: std.StringHashMap(*const PrecompiledCommand),
    /// Suffix appended to every write command (e.g. "\n", "\r\n").
    /// Empty string means no write termination. Owned by the recipe arena.
    write_termination: []const u8,
    /// Session options applied when the runtime opens the instrument.
    options: visa.InstrumentOptions,
};

/// Parsed and validated recipe step ready for execution.
pub const Step = struct {
    /// Step payload: either an instrument call or a local compute expression.
    action: Action,
    /// Optional guard expression; when present, the step is skipped if the expression evaluates to 0.0.
    when: ?expr.Expression = null,

    pub const Action = union(enum) {
        /// Send a command to an instrument and optionally save the response.
        instrument_call: InstrumentCall,
        /// Evaluate an arithmetic expression and store the result.
        compute: Compute,
    };

    pub const InstrumentCall = struct {
        /// Driver command name preserved for preview and diagnostics.
        call: []const u8,
        /// Recipe instrument name preserved for preview and diagnostics.
        instrument: []const u8,
        /// Precompiled command resolved during recipe precompile.
        command: *const PrecompiledCommand,
        /// Parsed argument map keyed by placeholder name.
        args: std.StringHashMap(StepArg),
        /// Optional context key that receives the parsed response value.
        save_as: ?[]const u8 = null,
    };

    pub const Compute = struct {
        /// Expression to evaluate (pre-parsed AST).
        expression: expr.Expression,
        /// Context key that receives the formatted f64 result.
        save_as: []const u8,
    };
};

/// Task schedule and the steps that should run at that interval.
pub const Task = struct {
    /// Execution period in milliseconds.
    every_ms: u64,
    /// Steps executed each time the task becomes due.
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
    /// Declares which `save_as` variables to record as frame columns.
    /// Use `"all"` to record every `save_as` variable, or list names explicitly.
    record: ?RecordConfig = null,
};

/// Controls which `save_as` variables are persisted by pipeline sinks.
pub const RecordConfig = union(enum) {
    /// Record every `save_as` variable.
    all: []const u8,
    /// Record only the listed variable names.
    explicit: []const []const u8,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

/// Optional stop conditions applied to the scheduler loop.
pub const StopWhen = struct {
    /// Maximum wall-clock runtime in milliseconds.
    time_elapsed_ms: ?u64 = null,
    /// Maximum number of task executions across all tasks.
    max_iterations: ?u64 = null,
};

/// Fully validated recipe ready for preview or execution.
/// Owns arena-backed data and should have a single logical owner until `deinit`.
pub const PrecompiledRecipe = struct {
    arena: std.heap.ArenaAllocator,
    instruments: std.StringHashMap(PrecompiledInstrument),
    tasks: []Task,
    pipeline: PipelineConfig,
    stop_when: StopWhen,
    /// Estimated total number of task iterations across all tasks.
    /// Null when the recipe runs indefinitely or stop conditions are dynamic.
    expected_iterations: ?u64,
    /// Default values for context variables at execution startup.
    initial_vars: std.StringHashMap(StepScalar),

    /// Releases all arena-owned precompiled recipe data.
    pub fn deinit(self: *PrecompiledRecipe) void {
        self.arena.deinit();
    }

    /// Loads and precompiles a recipe document from disk.
    pub fn precompilePath(
        allocator: std.mem.Allocator,
        recipe_path: []const u8,
        driver_reg: *DriverRegistry,
    ) !PrecompiledRecipe {
        return @import("precompile.zig").precompilePath(allocator, recipe_path, driver_reg);
    }

    /// Loads and precompiles a recipe document while capturing failure context.
    pub fn precompilePathWithDiagnostic(
        allocator: std.mem.Allocator,
        recipe_path: []const u8,
        driver_reg: *DriverRegistry,
        diagnostic_ctx: *diagnostic.PrecompileDiagnostic,
    ) !PrecompiledRecipe {
        return @import("precompile.zig").precompilePathWithDiagnostic(allocator, recipe_path, driver_reg, diagnostic_ctx);
    }
};
