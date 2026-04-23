const std = @import("std");
const session = @import("session.zig");
const execute_mod = @import("execute.zig");
const parallel = @import("parallel.zig");
const pipeline = @import("pipeline/root.zig");
const scheduler = @import("scheduler.zig");
const step = @import("step.zig");

/// Runtime options for recipe execution.
pub const ExecOptions = session.ExecOptions;

/// Executes a recipe against its referenced instruments.
pub fn execute(allocator: std.mem.Allocator, opts: ExecOptions) !void {
    try execute_mod.execute(allocator, opts);
}

test {
    std.testing.refAllDecls(@This());
}
