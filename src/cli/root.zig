const std = @import("std");
const clap = @import("clap");
const common = @import("common.zig");
const instrument = @import("instrument.zig");
const repl = @import("repl.zig");
const run = @import("run.zig");

/// Parses top-level CLI arguments and dispatches to the selected subcommand.
pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa_state.deinit();
        std.debug.assert(check == .ok);
    }
    const allocator = gpa_state.allocator();

    var iter = try std.process.ArgIterator.initWithAllocator(allocator);
    defer iter.deinit();
    _ = iter.next();

    var diag = clap.Diagnostic{};
    var res = clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
        .terminating_positional = 0,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(.stdout(), common.exe_name, clap.Help, &common.root_params);
        return;
    }

    // The positional is a raw string; validate it manually so we can show the
    // user what they actually typed instead of a cryptic error code.
    const raw = res.positionals[0] orelse {
        try common.reportUnknownPositional(
            clap.Help,
            &common.root_params,
            common.exe_name,
            "",
            "run, instrument, repl",
        );
        return error.MissingCommand;
    };

    const command = std.meta.stringToEnum(common.Command, raw) orelse {
        try common.reportUnknownPositional(
            clap.Help,
            &common.root_params,
            common.exe_name,
            raw,
            "run, instrument, repl",
        );
        return error.UnknownCommand;
    };

    switch (command) {
        .run => try run.handle(allocator, &iter),
        .instrument => try instrument.handle(allocator, &iter),
        .repl => try repl.handle(allocator, &iter),
    }
}

test {
    std.testing.refAllDecls(@This());
}
