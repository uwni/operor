const std = @import("std");
const clap = @import("clap");
const operor = @import("operor");
const common = @import("common.zig");

/// Parser table for the `repl` command.
const parsers = .{
    .str = clap.parsers.string,
};

/// Parameter definitions for the `repl` command.
const params = clap.parseParamsComptime(
    \\-h, --help              Display this help and exit.
    \\    --visa-lib <str>    Path to VISA shared library (overrides platform default).
    \\-r, --resource <str>    VISA resource address to connect to.
    \\
);

/// Parsed result type for the `repl` command.
const Args = clap.ResultEx(clap.Help, &params, parsers);

/// Parses and executes the `repl` command.
pub fn handle(allocator: std.mem.Allocator, io: std.Io, iter: *std.process.Args.Iterator) !void {
    var diag = clap.Diagnostic{};
    var res: Args = clap.parseEx(clap.Help, &params, parsers, iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
    }) catch |err| {
        try diag.reportToFile(io, .stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(io, .stdout(), common.exe_name ++ " repl", clap.Help, &params);
        return;
    }

    const resource_addr = res.args.resource;

    var stdin_buffer: [operor.repl_stdin_buffer_bytes]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buffer);

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    try operor.repl(allocator, io, resource_addr, res.args.@"visa-lib", &stdin_reader.interface, &stdout_writer.interface);
}

test "main parses repl command with resource flag" {
    const gpa = std.testing.allocator;

    var iter = common.SliceArgIter{ .items = &.{
        "repl",
        "--resource",
        "USB0::0x0957::0x1798::MY12345678::INSTR",
    } };

    var root = try clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer root.deinit();

    try std.testing.expectEqual(@as(usize, 0), root.args.help);
    try std.testing.expectEqualStrings("repl", root.positionals[0].?);

    var repl = try clap.parseEx(clap.Help, &params, parsers, &iter, .{
        .allocator = gpa,
    });
    defer repl.deinit();

    try std.testing.expectEqual(@as(usize, 0), repl.args.help);
    try std.testing.expectEqualStrings(
        "USB0::0x0957::0x1798::MY12345678::INSTR",
        repl.args.resource.?,
    );
}

test "main parses repl command without resource" {
    const gpa = std.testing.allocator;

    var iter = common.SliceArgIter{ .items = &.{
        "repl",
    } };

    var root = try clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer root.deinit();

    try std.testing.expectEqualStrings("repl", root.positionals[0].?);

    var repl = try clap.parseEx(clap.Help, &params, parsers, &iter, .{
        .allocator = gpa,
    });
    defer repl.deinit();

    try std.testing.expectEqual(@as(?[]const u8, null), repl.args.resource);
}
