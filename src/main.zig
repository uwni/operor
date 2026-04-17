const std = @import("std");
const cli = @import("cli/root.zig");

/// Executable entrypoint that delegates to the CLI module.
/// User-facing errors (bad commands, missing args, etc.) are printed by the
/// CLI layer before being returned; we catch them here to suppress the Zig
/// runtime error trace and exit with code 1 instead.
pub fn main(init: std.process.Init) void {
    cli.main(init) catch |err| {
        std.debug.print("Unexpected error: {s}\n", .{@errorName(err)});
        std.process.exit(1);
    };
}

test {
    std.testing.refAllDecls(@This());
}
