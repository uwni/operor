const std = @import("std");

const io = std.testing.io;

/// Temporary workspace helper for integration-style tests.
pub const TestWorkspace = struct {
    allocator: std.mem.Allocator,
    tmp: std.testing.TmpDir,

    /// Creates an empty temporary workspace.
    pub fn init(allocator: std.mem.Allocator) TestWorkspace {
        return .{
            .allocator = allocator,
            .tmp = std.testing.tmpDir(.{}),
        };
    }

    /// Removes the temporary workspace and all copied fixtures.
    pub fn deinit(self: *TestWorkspace) void {
        self.tmp.cleanup();
    }

    /// Creates a directory path inside the temporary workspace.
    pub fn makePath(self: *const TestWorkspace, sub_path: []const u8) !void {
        try self.tmp.dir.createDirPath(io, sub_path);
    }

    /// Writes a new file into the temporary workspace.
    pub fn writeFile(self: *const TestWorkspace, sub_path: []const u8, data: []const u8) !void {
        try self.ensureParentPath(sub_path);
        try self.tmp.dir.writeFile(io, .{ .sub_path = sub_path, .data = data });
    }

    /// Resolves a workspace-relative path to an absolute path.
    pub fn realpathAlloc(self: *const TestWorkspace, sub_path: []const u8) ![:0]u8 {
        return try self.tmp.dir.realPathFileAlloc(io, sub_path, self.allocator);
    }

    /// Reads a file from the temporary workspace into caller-owned memory.
    pub fn readFileAlloc(
        self: *const TestWorkspace,
        allocator: std.mem.Allocator,
        sub_path: []const u8,
        max_bytes: usize,
    ) ![]u8 {
        return try self.tmp.dir.readFileAlloc(io, sub_path, allocator, std.Io.Limit.limited(max_bytes));
    }

    /// Ensures the parent directory of `sub_path` exists in the temporary workspace.
    fn ensureParentPath(self: *const TestWorkspace, sub_path: []const u8) !void {
        const parent = std.fs.path.dirname(sub_path) orelse return;
        try self.tmp.dir.createDirPath(io, parent);
    }
};
