const std = @import("std");

/// Parses a JSON document into the specified type T.
/// Caller owns the returned value via the arena allocator.
pub fn parseFromSlice(comptime T: type, allocator: std.mem.Allocator, content: []const u8) !T {
    return try std.json.parseFromSliceLeaky(T, allocator, content, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

/// Convenience wrapper for callers that only have a filesystem path.
/// Caller owns the returned value and is responsible for freeing any allocated memory if T contains owned data.
pub fn parseFilePath(comptime T: type, allocator: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize) !T {
    const content = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    defer allocator.free(content);

    return parseFromSlice(T, allocator, content);
}

/// Convenience wrapper for callers that already have an open directory handle.
/// Caller owns the returned value and is responsible for freeing any allocated memory if T contains owned data.
pub fn parseFileInDir(comptime T: type, allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, file_name: []const u8, max_bytes: usize) !T {
    const content = try dir.readFileAlloc(io, file_name, allocator, .limited(max_bytes));
    defer allocator.free(content);

    return parseFromSlice(T, allocator, content);
}

test "parse from slice" {
    const Parsed = struct {
        name: []const u8,
    };

    const gpa = std.testing.allocator;

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    const parsed = try parseFromSlice(Parsed, arena.allocator(),
        \\{"name": "psu"}
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

    try workspace.writeFile("misc/config_psu.json",
        \\{"name": "psu"}
    );

    const path = try workspace.realpathAlloc("misc/config_psu.json");
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

    try workspace.writeFile("misc/config_psu.json",
        \\{"name": "psu"}
    );

    const misc_dir_path = try workspace.realpathAlloc("misc");
    defer gpa.free(misc_dir_path);

    var misc_dir = try std.Io.Dir.openDirAbsolute(std.testing.io, misc_dir_path, .{});
    defer misc_dir.close(std.testing.io);

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    const parsed = try parseFileInDir(Parsed, arena.allocator(), std.testing.io, misc_dir, "config_psu.json", 1024);
    try std.testing.expectEqualStrings("psu", parsed.name);
}
