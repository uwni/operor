const std = @import("std");
const common = @import("common.zig");
const loader = @import("loader.zig");

const c = common.c;
/// Default VISA resource manager session and discovery helpers.
const ResourceManager = @This();
const ViSession = common.ViSession;
const ViUInt32 = common.ViUInt32;
const ViFindList = common.ViFindList;
const default_resource_query = "?*INSTR";

/// Arena-owned list of VISA resource names.
/// This struct owns an arena and should have a single logical owner until `deinit`.
pub const ResourceList = struct {
    arena: std.heap.ArenaAllocator,
    /// Resource names owned by `arena`.
    items: []const []const u8,

    /// Releases the arena holding all resource names.
    pub fn deinit(self: *ResourceList) void {
        self.arena.deinit();
    }
};

session: ViSession,
vtable: *const loader.Vtable,

/// Opens the default VISA resource manager using the provided vtable.
pub fn init(vtable: *const loader.Vtable) common.Error!ResourceManager {
    var session: ViSession = undefined;
    try common.checkStatus(vtable.viOpenDefaultRM(&session));
    return .{ .session = session, .vtable = vtable };
}

/// Closes the resource manager session.
pub fn deinit(self: *ResourceManager) void {
    common.checkStatus(self.vtable.viClose(self.session)) catch {};
}

/// Enumerates VISA instrument resources that match the default query.
pub fn listResources(self: *ResourceManager, allocator: std.mem.Allocator) common.Error!ResourceList {
    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    const query_z = try alloc.dupeZ(u8, default_resource_query);

    var find_list: ViFindList = undefined;
    var count: ViUInt32 = 0;
    var buffer: [c.VI_FIND_BUFLEN]u8 = undefined;

    common.checkStatus(self.vtable.viFindRsrc(
        self.session,
        query_z.ptr,
        &find_list,
        &count,
        @ptrCast(&buffer),
    )) catch |err| {
        if (err == error.ResourceNotFound) {
            const empty = try alloc.alloc([]const u8, 0);
            return .{ .arena = arena, .items = empty };
        }
        return err;
    };
    defer common.checkStatus(self.vtable.viClose(find_list)) catch {};

    var resources: std.ArrayList([]const u8) = .empty;

    try resources.append(alloc, try copyBufferString(alloc, buffer[0..]));
    var index: usize = 1;
    while (index < count) : (index += 1) {
        try common.checkStatus(self.vtable.viFindNext(find_list, @ptrCast(&buffer)));
        try resources.append(alloc, try copyBufferString(alloc, buffer[0..]));
    }

    return .{ .arena = arena, .items = try resources.toOwnedSlice(alloc) };
}

/// Copies one NUL-terminated VISA buffer into allocator-owned memory.
fn copyBufferString(allocator: std.mem.Allocator, buffer: []const u8) ![]const u8 {
    const end = std.mem.indexOfScalar(u8, buffer, 0) orelse buffer.len;
    return try allocator.dupe(u8, buffer[0..end]);
}
