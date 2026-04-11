const std = @import("std");
const clap = @import("clap");
const ordo = @import("ordo");
const common = @import("common.zig");

/// Parser table for the `run` command.
const parsers = .{
    .adapter_dir = clap.parsers.string,
    .recipe = clap.parsers.string,
    .u64 = clap.parsers.int(u64, 10),
    .str = clap.parsers.string,
};

/// Parameter definitions for the `run` command.
const params = clap.parseParamsComptime(
    \\-h, --help                          Display this help and exit.
    \\-d, --adapter-dir <adapter_dir>       Path to adapter directory.
    \\    --preview                       Preview the recipe without instrument I/O.
    \\    --dry-run                       Force dry-run even in run mode.
    \\    --visa-lib <str>                Path to VISA shared library (overrides platform default).
    \\<recipe>
    \\
);

/// Parsed result type for the `run` command.
const Args = clap.ResultEx(clap.Help, &params, parsers);

/// Parses and executes the `run` command, including preview mode.
pub fn handle(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {
    var diag = clap.Diagnostic{};
    var res: Args = clap.parseEx(clap.Help, &params, parsers, iter, .{
        .allocator = allocator,
        .diagnostic = &diag,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try common.usageAndHelpToFile(.stdout(), common.exe_name ++ " run", clap.Help, &params);
        return;
    }

    const adapter_dir = res.args.@"adapter-dir" orelse {
        try common.usageAndHelpToFile(.stderr(), common.exe_name ++ " run", clap.Help, &params);
        return error.MissingAdapterDirectory;
    };
    const recipe_path = res.positionals[0] orelse {
        try common.usageAndHelpToFile(.stderr(), common.exe_name ++ " run", clap.Help, &params);
        return error.MissingRecipePath;
    };
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const out = &stdout_writer.interface;
    defer out.flush() catch {};

    if (res.args.preview != 0) {
        try ordo.preview(allocator, adapter_dir, recipe_path, out);
        return;
    }

    const opts = ordo.ExecOptions{
        .adapter_dir = adapter_dir,
        .recipe_path = recipe_path,
        .dry_run = res.args.@"dry-run" != 0,
        .visa_lib = res.args.@"visa-lib",
        .log = out,
    };

    try ordo.execute(allocator, opts);
}

test "main parses run command" {
    const gpa = std.testing.allocator;

    var iter = common.SliceArgIter{ .items = &.{
        "run",
        "--adapter-dir",
        "adapters",
        "recipes/r1.yaml",
        "--preview",
    } };

    var root = try clap.parseEx(clap.Help, &common.root_params, common.root_parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer root.deinit();

    try std.testing.expectEqual(@as(usize, 0), root.args.help);
    try std.testing.expectEqualStrings("run", root.positionals[0].?);

    var run = try clap.parseEx(clap.Help, &params, parsers, &iter, .{
        .allocator = gpa,
    });
    defer run.deinit();

    try std.testing.expectEqual(@as(usize, 0), run.args.help);
    try std.testing.expectEqualStrings("adapters", run.args.@"adapter-dir".?);
    try std.testing.expectEqualStrings("recipes/r1.yaml", run.positionals[0].?);
    try std.testing.expect(run.args.preview != 0);
    try std.testing.expect(run.args.@"dry-run" == 0);
}
