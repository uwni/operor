const std = @import("std");

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

    /// Writes a new file into the temporary workspace.
    pub fn writeFile(self: *const TestWorkspace, sub_path: []const u8, data: []const u8) !void {
        try self.ensureParentPath(sub_path);
        try self.tmp.dir.writeFile(.{ .sub_path = sub_path, .data = data });
    }

    /// Renames a file or directory inside the temporary workspace.
    pub fn rename(self: *const TestWorkspace, old_sub_path: []const u8, new_sub_path: []const u8) !void {
        try self.ensureParentPath(new_sub_path);
        try self.tmp.dir.rename(old_sub_path, new_sub_path);
    }

    /// Resolves a workspace-relative path to an absolute path.
    pub fn realpathAlloc(self: *const TestWorkspace, sub_path: []const u8) ![]u8 {
        return try self.tmp.dir.realpathAlloc(self.allocator, sub_path);
    }

    /// Reads a file from the temporary workspace into caller-owned memory.
    pub fn readFileAlloc(
        self: *const TestWorkspace,
        allocator: std.mem.Allocator,
        sub_path: []const u8,
        max_bytes: usize,
    ) ![]u8 {
        return try self.tmp.dir.readFileAlloc(allocator, sub_path, max_bytes);
    }

    /// Ensures the parent directory of `sub_path` exists in the temporary workspace.
    fn ensureParentPath(self: *const TestWorkspace, sub_path: []const u8) !void {
        const parent = std.fs.path.dirname(sub_path) orelse return;
        try self.tmp.dir.makePath(parent);
    }
};
