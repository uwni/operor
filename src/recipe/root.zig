const std = @import("std");
const diagnostic = @import("../diagnostic.zig");
const precompile = @import("precompile.zig");
const recipe_ir = @import("compiled.zig");
const tty = @import("../tty.zig");

/// Executable command prepared during recipe precompilation.
pub const PrecompiledCommand = recipe_ir.PrecompiledCommand;
/// Borrowed-or-owned bytes produced by rendering a precompiled command.
pub const RenderedCommand = recipe_ir.RenderedCommand;
/// Parsed and validated recipe step ready for execution.
pub const Step = recipe_ir.Step;
/// Parsed representation of a step argument value.
pub const StepArg = recipe_ir.StepArg;
/// Optional adapter text mapping for boolean true/false values.
pub const BoolTextMap = recipe_ir.BoolTextMap;
/// Task variant describing when and how steps are executed.
pub const Task = recipe_ir.Task;
/// Runtime mode presets for the sampling pipeline.
pub const PipelineMode = recipe_ir.PipelineMode;
/// Optional pipeline configuration attached to a recipe.
pub const PipelineConfig = recipe_ir.PipelineConfig;
/// Recipe instrument bound to a adapter and the subset of commands it actually uses.
pub const PrecompiledInstrument = recipe_ir.PrecompiledInstrument;
/// Accumulates user-facing diagnostics.
pub const Diagnostics = diagnostic.Diagnostics;
/// Fully validated recipe ready for preview or execution.
pub const PrecompiledRecipe = recipe_ir.PrecompiledRecipe;

/// Opens `adapter_dir_path`, precompiles `recipe_path`, and writes any user-facing diagnostics to `log`.
pub fn precompilePathFromAdapterDir(
    allocator: std.mem.Allocator,
    io: std.Io,
    adapter_dir_path: []const u8,
    recipe_path: []const u8,
    log: *std.Io.Writer,
) !PrecompiledRecipe {
    const dir = if (std.fs.path.isAbsolute(adapter_dir_path))
        std.Io.Dir.openDirAbsolute(io, adapter_dir_path, .{})
    else
        std.Io.Dir.cwd().openDir(io, adapter_dir_path, .{});
    const opened = dir catch |err| {
        try log.writeAll(tty.error_prefix);
        try log.print("cannot open adapter directory '{s}': {s}\n", .{ adapter_dir_path, @errorName(err) });
        return error.Diagnosed;
    };
    defer opened.close(io);

    return precompile.precompilePath(allocator, io, recipe_path, opened, log) catch |err| switch (err) {
        error.AnalysisFail => return error.Diagnosed,
        else => return err,
    };
}

test {
    std.testing.refAllDecls(@This());
}
