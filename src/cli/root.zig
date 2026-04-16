const std = @import("std");
const clap = @import("clap");
const common = @import("common.zig");
const repl = @import("repl.zig");
const run = @import("run.zig");

/// Parses top-level CLI arguments and dispatches to the selected subcommand.
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer iter.deinit();
    _ = iter.next();

    var diag = clap.Diagnostic{};
    var res = clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
        .terminating_positional = 0,
    }) catch |err| {
        try diag.reportToFile(io, .stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(io, .stdout(), common.exe_name, clap.Help, &common.root_params);
        return;
    }

    // The positional is a raw string; validate it manually so we can show the
    // user what they actually typed instead of a cryptic error code.
    const raw = res.positionals[0] orelse {
        try common.reportUnknownPositional(
            io,
            clap.Help,
            &common.root_params,
            common.exe_name,
            "",
            "run, repl",
        );
        return error.MissingCommand;
    };

    const command = std.meta.stringToEnum(common.Command, raw) orelse {
        try common.reportUnknownPositional(
            io,
            clap.Help,
            &common.root_params,
            common.exe_name,
            raw,
            "run, repl",
        );
        return error.UnknownCommand;
    };

    switch (command) {
        .run => try run.handle(allocator, io, &iter),
        .repl => try repl.handle(allocator, io, &iter),
    }
}

test {
    std.testing.refAllDecls(@This());
}
