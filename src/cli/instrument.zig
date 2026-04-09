const std = @import("std");
const clap = @import("clap");
const ordo = @import("ordo");
const common = @import("common.zig");

/// Supported `instrument` subcommands.
const Subcommand = enum {
    list,
};

/// Parser table for the `instrument` command: positional as raw string.
const parsers = .{
    .command = clap.parsers.string,
};

/// Parameter definitions for the `instrument` command.
const params = clap.parseParamsComptime(
    \\-h, --help  Display this help and exit.
    \\<command>
    \\        Instrument command to run. Currently supported: list.
    \\
);

/// Parsed result type for the `instrument` command.
const Args = clap.ResultEx(clap.Help, &params, parsers);

const list_params = clap.parseParamsComptime(
    \\-h, --help              Display this help and exit.
    \\    --visa-lib <str>    Path to VISA shared library (overrides platform default).
    \\
);

const ListArgs = clap.ResultEx(clap.Help, &list_params, clap.parsers.default);

/// Parses and dispatches the `instrument` command group.
pub fn handle(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {
    var diag = clap.Diagnostic{};
    var res: Args = clap.parseEx(clap.Help, &params, parsers, iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
        .terminating_positional = 0,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(.stdout(), common.exe_name ++ " instrument", clap.Help, &params);
        return;
    }

    const raw = res.positionals[0] orelse {
        try common.reportUnknownPositional(
            clap.Help,
            &params,
            common.exe_name ++ " instrument",
            "",
            "list",
        );
        return error.MissingCommand;
    };

    const command = std.meta.stringToEnum(Subcommand, raw) orelse {
        try common.reportUnknownPositional(
            clap.Help,
            &params,
            common.exe_name ++ " instrument",
            raw,
            "list",
        );
        return error.UnknownCommand;
    };

    switch (command) {
        .list => try handleList(allocator, iter),
    }
}

/// Parses and executes `instrument list`.
fn handleList(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {
    var diag = clap.Diagnostic{};
    var res: ListArgs = clap.parseEx(clap.Help, &list_params, clap.parsers.default, iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(.stdout(), common.exe_name ++ " instrument list", clap.Help, &list_params);
        return;
    }

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out = &stdout_writer.interface;
    var resources = try ordo.listResources(allocator, res.args.@"visa-lib");
    defer resources.deinit();

    for (resources.items) |resource| {
        try out.print("{s}\n", .{resource});
    }
    try out.flush();
}

test "main parses instrument list command" {
    const gpa = std.testing.allocator;

    var iter = common.SliceArgIter{ .items = &.{
        "instrument",
        "list",
    } };

    var root = try clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer root.deinit();

    try std.testing.expectEqual(@as(usize, 0), root.args.help);
    try std.testing.expectEqualStrings("instrument", root.positionals[0].?);

    var instr = try clap.parseEx(clap.Help, &params, parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer instr.deinit();

    try std.testing.expectEqual(@as(usize, 0), instr.args.help);
    try std.testing.expectEqualStrings("list", instr.positionals[0].?);

    var list = try clap.parseEx(clap.Help, &list_params, clap.parsers.default, &iter, .{
        .allocator = gpa,
    });
    defer list.deinit();

    try std.testing.expectEqual(@as(usize, 0), list.args.help);
}
