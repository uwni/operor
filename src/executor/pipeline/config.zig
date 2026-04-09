const std = @import("std");
const common = @import("../common.zig");
const recipe_types = @import("../../recipe/types.zig");

/// Fully resolved runtime configuration for the sampling pipeline.
pub const ResolvedConfig = struct {
    mode: recipe_types.PipelineMode,
    buffer_size: usize,
    warn_usage_percent: u8,
    file_path: ?[]const u8 = null,
    network_host: ?[]const u8 = null,
    network_port: ?u16 = null,
};

pub fn resolveConfig(recipe_pipeline: *const recipe_types.PipelineConfig, opts: *const common.ExecOptions) ResolvedConfig {
    const mode = opts.pipeline_mode orelse recipe_pipeline.mode orelse .safe;
    const requested_buffer_size = opts.pipeline_buffer_size orelse recipe_pipeline.buffer_size orelse defaultBufferSize(mode);

    return .{
        .mode = mode,
        .buffer_size = normalizeBufferSize(requested_buffer_size),
        .warn_usage_percent = normalizeWarnUsagePercent(opts.pipeline_warn_usage_percent orelse recipe_pipeline.warn_usage_percent orelse 85),
        .file_path = recipe_pipeline.file_path,
        .network_host = recipe_pipeline.network_host,
        .network_port = recipe_pipeline.network_port,
    };
}

fn defaultBufferSize(mode: recipe_types.PipelineMode) usize {
    return switch (mode) {
        .safe => 4096,
        .realtime => 8192,
    };
}

fn normalizeBufferSize(requested: usize) usize {
    const clamped = @max(requested, 64);
    return std.math.ceilPowerOfTwo(usize, clamped) catch clamped;
}

fn normalizeWarnUsagePercent(requested: u8) u8 {
    if (requested == 0) return 85;
    if (requested > 100) return 100;
    return requested;
}
