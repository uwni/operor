const std = @import("std");
const clap = @import("clap");
const build_options = @import("build_options");
const tty = @import("operor").tty;

/// The executable name as set in build.zig (e.g. "operor").
pub const exe_name = build_options.exe_name;

/// The package version from build.zig.zon, injected at build time.
pub const version = build_options.version;

/// Top-level CLI subcommands.
pub const Command = enum {
    run,
    repl,
};

/// Prints "Usage: <name> <usage>" followed by the full help text to `file`.
pub fn usageAndHelpToFile(
    io: std.Io,
    file: std.Io.File,
    name: []const u8,
    comptime Id: type,
    comptime params: []const clap.Param(Id),
) !void {
    var buf: [512]u8 = undefined;
    var w = file.writer(io, &buf);
    try w.interface.print("{s} is an automated experimental workflow engine for VISA-controlled instruments. ({s})\n\n", .{ exe_name, version });
    try w.interface.print("Usage: {s} ", .{name});
    try clap.usage(&w.interface, Id, params);
    try w.interface.print("\n\n", .{});
    try clap.help(&w.interface, Id, params, .{});
    try w.interface.flush();
}

/// Diagnostic for CLI argument validation errors.
pub const CliDiagnostic = struct {
    pub const Error = error{
        MissingCommand,
        UnknownCommand,
        MissingAdapterDirectory,
        MissingRecipePath,
        MissingResourceAddress,
    };

    /// The unrecognised token typed by the user (set for `UnknownCommand`).
    command: ?[]const u8 = null,

    pub fn write(self: *const CliDiagnostic, writer: *std.Io.Writer, err: Error) !void {
        try writer.writeAll(tty.error_prefix);
        switch (err) {
            error.MissingCommand => try writer.print(
                "missing command. Run '{s} --help' for usage.\n",
                .{exe_name},
            ),
            error.UnknownCommand => try writer.print(
                "unknown command '{s}'. Run '{s} --help' for usage.\n",
                .{ self.command orelse "<unknown>", exe_name },
            ),
            error.MissingAdapterDirectory => try writer.print(
                "missing required option '--adapter-dir'. Run '{s} run --help' for usage.\n",
                .{exe_name},
            ),
            error.MissingRecipePath => try writer.print(
                "missing required argument '<recipe>'. Run '{s} run --help' for usage.\n",
                .{exe_name},
            ),
            error.MissingResourceAddress => try writer.print(
                "missing required option '--resource'. Run '{s} repl --help' for usage.\n",
                .{exe_name},
            ),
        }
    }
};

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
