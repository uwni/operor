const std = @import("std");
const diagnostic = @import("diagnostic.zig");
const precompile = @import("precompile.zig");
const recipe_ir = @import("compiled.zig");

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
/// Context captured for the most recent precompile failure.
pub const PrecompileDiagnostic = diagnostic.PrecompileDiagnostic;
/// Fully validated recipe ready for preview or execution.
pub const PrecompiledRecipe = recipe_ir.PrecompiledRecipe;

test {
    std.testing.refAllDecls(@This());
}
