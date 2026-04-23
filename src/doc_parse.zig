const std = @import("std");
const serde = @import("serde");

/// Supported document formats for adapter and recipe configuration files.
pub const DocFormat = enum {
    toml,
    yaml,

    /// Infers a document format from a file extension such as `.yaml`.
    pub fn fromExtension(ext: []const u8) ?DocFormat {
        if (std.ascii.eqlIgnoreCase(ext, ".toml")) return .toml;
        if (std.ascii.eqlIgnoreCase(ext, ".yaml")) return .yaml;
        return null;
    }
};

/// Detects a supported document format from a path or file name.
pub fn detectFormat(path_or_name: []const u8) ?DocFormat {
    return DocFormat.fromExtension(std.fs.path.extension(path_or_name));
}

/// Parses a document with the given format into the specified type T.
/// Caller owns the returned value and is responsible for freeing any allocated memory if T contains owned data.
/// YAML parsing currently allocates intermediate parse state from `allocator`, so callers should prefer an arena when
/// the result does not expose a dedicated `deinit`.
pub fn parseByFormat(comptime T: type, format: DocFormat, allocator: std.mem.Allocator, content: []const u8) !T {
    return switch (format) {
        .toml => try serde.toml.fromSlice(T, allocator, content),
        .yaml => try serde.yaml.fromSlice(T, allocator, content),
    };
}

/// Convenience wrapper for callers that only have a filesystem path.
/// Caller owns the returned value and is responsible for freeing any allocated memory if T contains owned data.
pub fn parseFilePath(comptime T: type, allocator: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize) !T {
    const format = detectFormat(path) orelse return error.UnsupportedFormat;

    const content = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    defer allocator.free(content);

    return parseByFormat(T, format, allocator, content);
}

/// Convenience wrapper for callers that already have an open directory handle.
/// Caller owns the returned value and is responsible for freeing any allocated memory if T contains owned data.
pub fn parseFileInDir(comptime T: type, allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, file_name: []const u8, max_bytes: usize) !T {
    const format = detectFormat(file_name) orelse return error.UnsupportedFormat;

    const content = try dir.readFileAlloc(io, file_name, allocator, .limited(max_bytes));
    defer allocator.free(content);

    return parseByFormat(T, format, allocator, content);
}

test "parse by format from yaml content" {
    const Parsed = struct {
        name: []const u8,
    };

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const parsed = try parseByFormat(Parsed, .yaml, arena.allocator(),
        \\name: psu
    );
    try std.testing.expectEqualStrings("psu", parsed.name);
}

test "parse file path opens file directly" {
    const Parsed = struct {
        name: []const u8,
    };

    const gpa = std.testing.allocator;
    const testing = @import("testing.zig");

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("misc/config_psu.yaml",
        \\name: psu
    );

    const path = try workspace.realpathAlloc("misc/config_psu.yaml");
    defer gpa.free(path);

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    const parsed = try parseFilePath(Parsed, arena.allocator(), std.testing.io, path, 1024);
    try std.testing.expectEqualStrings("psu", parsed.name);
}

test "parse file in dir opens file directly" {
    const Parsed = struct {
        name: []const u8,
    };

    const gpa = std.testing.allocator;
    const testing = @import("testing.zig");

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("misc/config_psu.yaml",
        \\name: psu
    );

    const misc_dir_path = try workspace.realpathAlloc("misc");
    defer gpa.free(misc_dir_path);

    var misc_dir = try std.Io.Dir.openDirAbsolute(std.testing.io, misc_dir_path, .{});
    defer misc_dir.close(std.testing.io);

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    const parsed = try parseFileInDir(Parsed, arena.allocator(), std.testing.io, misc_dir, "config_psu.yaml", 1024);
    try std.testing.expectEqualStrings("psu", parsed.name);
}
