const std = @import("std");
const ring_buffer = @import("ring_buffer.zig");

/// Structured task-level frame persisted by sinks and suitable for analysis.
/// Values are indexed by pipeline column position assigned during precompile.
/// Owns `values_owned` when present; keep a single logical owner.
pub const Frame = struct {
    timestamp_ns: i128,
    task_idx: usize,
    values_owned: ?[]?[]u8 = null,

    pub fn empty() Frame {
        return .{
            .timestamp_ns = 0,
            .task_idx = 0,
            .values_owned = null,
        };
    }

    pub fn deinit(self: *Frame, allocator: std.mem.Allocator) void {
        if (self.values_owned) |values| {
            for (values) |v| {
                if (v) |owned| allocator.free(owned);
            }
            allocator.free(values);
        }
    }

    pub fn fieldCount(self: *const Frame) usize {
        const values = self.values_owned orelse return 0;
        var count: usize = 0;
        for (values) |v| {
            if (v != null) count += 1;
        }
        return count;
    }

    pub fn getColumn(self: *const Frame, column: usize) ?[]const u8 {
        const values = self.values_owned orelse return null;
        if (column >= values.len) return null;
        return values[column];
    }
};

pub const FrameQueue = ring_buffer.SpscRingBuffer(Frame);

test "frame looks up values by column index" {
    const values = try std.testing.allocator.alloc(?[]u8, 2);
    values[0] = try std.testing.allocator.dupe(u8, "1.23");
    values[1] = try std.testing.allocator.dupe(u8, "0.45");

    var frame = Frame{
        .timestamp_ns = 1,
        .task_idx = 0,
        .values_owned = values,
    };
    defer frame.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), frame.fieldCount());
    try std.testing.expectEqualStrings("1.23", frame.getColumn(0).?);
    try std.testing.expectEqualStrings("0.45", frame.getColumn(1).?);
    try std.testing.expect(frame.getColumn(2) == null);
}
