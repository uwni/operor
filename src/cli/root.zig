const std = @import("std");
const clap = @import("clap");
const common = @import("common.zig");
const repl = @import("repl.zig");
const run = @import("run.zig");

/// Parses top-level CLI arguments and dispatches to the selected subcommand.
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var cli_diag: common.CliDiagnostic = .{};
    return dispatch(allocator, io, init.minimal.args, &cli_diag) catch |err| {
        switch (err) {
            error.MissingCommand,
            error.UnknownCommand,
            error.MissingAdapterDirectory,
            error.MissingRecipePath,
            error.MissingResourceAddress,
            => |cli_err| {
                var buf: [256]u8 = undefined;
                var w = std.Io.File.stderr().writer(io, &buf);
                cli_diag.write(&w.interface, cli_err) catch {};
                w.interface.flush() catch {};
                std.process.exit(1);
            },
            else => {},
        }
        return err;
    };
}

fn dispatch(allocator: std.mem.Allocator, io: std.Io, args: anytype, cli_diag: *common.CliDiagnostic) !void {
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
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

    const raw = res.positionals[0] orelse return error.MissingCommand;
    const command = std.meta.stringToEnum(common.Command, raw) orelse {
        cli_diag.command = raw;
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
