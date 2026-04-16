const std = @import("std");
const recipe_types = @import("../recipe/types.zig");
const visa = @import("../visa/root.zig");
pub const Context = @import("Context.zig");

/// Runtime options for recipe execution.
pub const ExecOptions = struct {
    /// Directory containing adapter documents and the registry cache.
    adapter_dir: []const u8,
    /// Path to the recipe document to execute.
    recipe_path: []const u8,
    /// I/O interface for filesystem and other operations.
    io: std.Io,
    /// If true, rendered commands are logged instead of being sent to instruments.
    dry_run: bool = true,
    /// Optional runtime override for the ring buffer size.
    pipeline_buffer_size: ?usize = null,
    /// Optional runtime override for the pipeline mode preset.
    pipeline_mode: ?recipe_types.PipelineMode = null,
    /// Optional runtime override for the buffer usage warning threshold.
    pipeline_warn_usage_percent: ?u8 = null,
    /// Writer for logs.
    log: *std.Io.Writer,
    /// Optional path to the VISA shared library. When null the platform default
    /// locations are searched (e.g. /Library/Frameworks/VISA.framework/VISA on macOS).
    visa_lib: ?[]const u8 = null,
};

/// Runtime state associated with one precompiled instrument.
pub const InstrumentRuntime = struct {
    handle: ?visa.Instrument,
};

pub const Value = Context.Value;
pub const RenderValue = Context.RenderValue;

/// Type-erased fire-and-forget log sink for executor diagnostics.
pub const LogSink = struct {
    context: *anyopaque,
    writeFn: *const fn (*anyopaque, []const u8) void,

    pub fn writeAll(self: LogSink, bytes: []const u8) void {
        self.writeFn(self.context, bytes);
    }
};
