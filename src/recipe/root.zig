const std = @import("std");
const diagnostic = @import("diagnostic.zig");
const precompile = @import("precompile.zig");
const types = @import("types.zig");

/// Executable command prepared during recipe precompilation.
pub const PrecompiledCommand = types.PrecompiledCommand;
/// Borrowed-or-owned bytes produced by rendering a precompiled command.
pub const RenderedCommand = types.RenderedCommand;
/// Parsed and validated recipe step ready for execution.
pub const Step = types.Step;
/// Compiled representation of one step argument item.
pub const CompiledArgValue = types.CompiledArgValue;
/// Parsed representation of a step argument value.
pub const StepArg = types.StepArg;
/// Task variant describing when and how steps are executed.
pub const Task = types.Task;
/// Runtime mode presets for the sampling pipeline.
pub const PipelineMode = types.PipelineMode;
/// Optional pipeline configuration attached to a recipe.
pub const PipelineConfig = types.PipelineConfig;
/// Recipe instrument bound to a adapter and the subset of commands it actually uses.
pub const PrecompiledInstrument = types.PrecompiledInstrument;
/// Context captured for the most recent precompile failure.
pub const PrecompileDiagnostic = diagnostic.PrecompileDiagnostic;
/// Fully validated recipe ready for preview or execution.
pub const PrecompiledRecipe = types.PrecompiledRecipe;

test {
    std.testing.refAllDecls(@This());
}
