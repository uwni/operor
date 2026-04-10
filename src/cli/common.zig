const std = @import("std");
const clap = @import("clap");
const build_options = @import("build_options");

/// The executable name as set in build.zig (e.g. "ordo").
pub const exe_name = build_options.exe_name;

/// Top-level CLI subcommands.
pub const Command = enum {
    run,
    repl,
};

/// Prints "Usage: <name> <usage>" followed by the full help text to `file`.
pub fn usageAndHelpToFile(
    file: std.fs.File,
    name: []const u8,
    comptime Id: type,
    comptime params: []const clap.Param(Id),
) !void {
    var buf: [512]u8 = undefined;
    var w = file.writer(&buf);
    try w.interface.print("Usage: {s} ", .{name});
    try clap.usage(&w.interface, Id, params);
    try w.interface.print("\n\n", .{});
    try clap.help(&w.interface, Id, params, .{});
    try w.interface.flush();
}

/// Writes a human-readable "unknown/missing command" message to stderr,
/// then prints usage + help to stderr.
///
/// `name`  – e.g. "ordo run"
/// `arg`   – the unrecognised token typed by the user (empty string = nothing was typed)
/// `valid` – comma-separated list of accepted values, e.g. "run, instrument, repl"
pub fn reportUnknownPositional(
    comptime Id: type,
    comptime params: []const clap.Param(Id),
    name: []const u8,
    arg: []const u8,
    valid: []const u8,
) !void {
    const stderr = std.fs.File.stderr();
    var buf: [256]u8 = undefined;
    var w = stderr.writer(&buf);
    if (arg.len > 0) {
        try w.interface.print(
            "Unknown command '{s}'. Valid commands: {s}.\n\n",
            .{ arg, valid },
        );
    } else {
        try w.interface.print(
            "Missing command. Valid commands: {s}.\n\n",
            .{valid},
        );
    }
    try w.interface.flush();
    try usageAndHelpToFile(stderr, name, Id, params);
}

/// Parser table for the root command: the positional is parsed as a raw string
/// so we can show the user what they typed before validating it.
pub const root_parsers = .{
    .command = clap.parsers.string,
};

/// Shared top-level CLI parameter definitions.
/// The positional is typed as a plain string; validation happens in root.zig.
pub const root_params = clap.parseParamsComptime(
    \\-h, --help  Display this help and exit.
    \\<command>
    \\        Command to run. Currently supported: run, repl.
    \\
);

/// Small test-only argument iterator compatible with `clap.parseEx`.
pub const SliceArgIter = struct {
    items: []const []const u8,
    index: usize = 0,

    /// Returns the next argument from the fixed slice.
    pub fn next(self: *SliceArgIter) ?[]const u8 {
        if (self.index >= self.items.len) return null;
        const item = self.items[self.index];
        self.index += 1;
        return item;
    }
};

test "main returns help when command missing" {
    const gpa = std.testing.allocator;

    var iter = SliceArgIter{ .items = &.{} };
    var root = try clap.parseEx(clap.Help, &root_params, root_parsers, &iter, .{
        .allocator = gpa,
        .terminating_positional = 0,
    });
    defer root.deinit();

    try std.testing.expectEqual(@as(usize, 0), root.args.help);
    try std.testing.expectEqual(@as(?[]const u8, null), root.positionals[0]);
}
