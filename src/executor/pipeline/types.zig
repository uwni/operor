const std = @import("std");
const ring_buffer = @import("ring_buffer.zig");

/// One named value captured during a task iteration.
/// Owns `value_owned`; keep a single logical owner.
pub const FrameField = struct {
    name: []const u8,
    value_owned: []u8,

    pub fn deinit(self: *FrameField, allocator: std.mem.Allocator) void {
        allocator.free(self.value_owned);
    }

    pub fn value(self: *const FrameField) []const u8 {
        return self.value_owned;
    }
};

/// Structured task-level frame persisted by sinks and suitable for analysis.
/// Owns `fields_owned` when present; keep a single logical owner.
pub const Frame = struct {
    timestamp_ns: i128,
    task_idx: usize,
    fields_owned: ?[]FrameField = null,

    pub fn empty() Frame {
        return .{
            .timestamp_ns = 0,
            .task_idx = 0,
            .fields_owned = null,
        };
    }

    pub fn deinit(self: *Frame, allocator: std.mem.Allocator) void {
        if (self.fields_owned) |fields| {
            for (fields) |*field| field.deinit(allocator);
            allocator.free(fields);
        }
    }

    pub fn fieldCount(self: *const Frame) usize {
        if (self.fields_owned) |fields| return fields.len;
        return 0;
    }

    pub fn getValue(self: *const Frame, name: []const u8) ?[]const u8 {
        const fields = self.fields_owned orelse return null;
        for (fields) |*field| {
            if (std.mem.eql(u8, field.name, name)) return field.value();
        }
        return null;
    }
};

pub const FrameQueue = ring_buffer.SpscRingBuffer(Frame);

test "frame looks up values by field name" {
    const fields = try std.testing.allocator.alloc(FrameField, 2);
    fields[0] = .{ .name = "voltage", .value_owned = try std.testing.allocator.dupe(u8, "1.23") };
    fields[1] = .{ .name = "current", .value_owned = try std.testing.allocator.dupe(u8, "0.45") };

    var frame = Frame{
        .timestamp_ns = 1,
        .task_idx = 0,
        .fields_owned = fields,
    };
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), frame.fieldCount());
    try std.testing.expectEqualStrings("1.23", frame.getValue("voltage").?);
    try std.testing.expect(frame.getValue("power") == null);
}
