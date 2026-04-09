const std = @import("std");

const cache_line = std.atomic.cache_line;

pub fn SpscRingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        slots: []T,
        capacity: usize,
        mask: u64,
        // Producer-owned hot fields stay together.
        head: std.atomic.Value(u64) align(cache_line) = std.atomic.Value(u64).init(0),
        overflow_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        // Only the producer updates this metric, and readers only observe it after producer join.
        high_watermark: usize = 0,
        // Consumer-owned hot field gets its own cache line to reduce bouncing.
        tail: std.atomic.Value(u64) align(cache_line) = std.atomic.Value(u64).init(0),

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(std.math.isPowerOfTwo(capacity));
            const slots = try allocator.alloc(T, capacity);
            for (slots) |*slot| slot.* = T.empty();
            return .{
                .allocator = allocator,
                .slots = slots,
                .capacity = capacity,
                .mask = @intCast(capacity - 1),
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.slots) |*slot| slot.deinit(self.allocator);
            self.allocator.free(self.slots);
        }

        pub fn push(self: *Self, item: *T) error{BufferOverflow}!void {
            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.acquire);
            const used = head - tail;
            const capacity: u64 = @intCast(self.capacity);
            if (used >= capacity) {
                const current = self.overflow_count.load(.monotonic);
                self.overflow_count.store(current + 1, .monotonic);
                return error.BufferOverflow;
            }

            moveItem(&self.slots[self.slotIndex(head)], item);
            const new_used: usize = @intCast(used + 1);
            if (new_used > self.high_watermark) self.high_watermark = new_used;

            // Publish the slot only after the item payload has been written.
            self.head.store(head + 1, .release);
        }

        pub fn pop(self: *Self, out: *T) bool {
            const tail = self.tail.load(.monotonic);
            const head = self.head.load(.acquire);
            if (tail == head) return false;

            moveItem(out, &self.slots[self.slotIndex(tail)]);
            self.tail.store(tail + 1, .release);
            return true;
        }

        pub fn usage(self: *const Self) usize {
            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.monotonic);
            if (head <= tail) return 0;
            return @intCast(@min(head - tail, @as(u64, @intCast(self.capacity))));
        }

        pub fn overflowCount(self: *const Self) u64 {
            return self.overflow_count.load(.monotonic);
        }

        pub fn highWatermark(self: *const Self) usize {
            return self.high_watermark;
        }

        fn slotIndex(self: *const Self, position: u64) usize {
            return @intCast(position & self.mask);
        }
    };
}

fn moveItem(dst: anytype, src: anytype) void {
    const T = @TypeOf(dst.*);
    dst.* = src.*;
    src.* = T.empty();
}

test "ring buffer reports overflow and tracks high watermark" {
    var ring = try TestQueue.init(std.testing.allocator, 4);
    defer ring.deinit();

    var item = makeTestItem(1);

    try ring.push(&item);
    try ring.push(&item);
    try ring.push(&item);
    try ring.push(&item);
    try std.testing.expectError(error.BufferOverflow, ring.push(&item));
    try std.testing.expectEqual(@as(u64, 1), ring.overflowCount());
    try std.testing.expectEqual(@as(usize, 4), ring.highWatermark());
}

test "ring buffer preserves FIFO across wrap-around" {
    var ring = try TestQueue.init(std.testing.allocator, 4);
    defer ring.deinit();

    for (0..3) |seq| {
        var item = makeTestItem(seq);
        try ring.push(&item);
    }

    var out = TestItem.empty();
    try std.testing.expect(ring.pop(&out));
    try std.testing.expectEqual(@as(i128, 0), out.seq);
    try std.testing.expect(ring.pop(&out));
    try std.testing.expectEqual(@as(i128, 1), out.seq);

    for (3..6) |seq| {
        var item = makeTestItem(seq);
        try ring.push(&item);
    }

    for (2..6) |seq| {
        try std.testing.expect(ring.pop(&out));
        try std.testing.expectEqual(@as(i128, @intCast(seq)), out.seq);
    }
    try std.testing.expect(!ring.pop(&out));
}

test "ring buffer usage is saturated for inconsistent snapshots" {
    var ring = try TestQueue.init(std.testing.allocator, 4);
    defer ring.deinit();

    ring.head.store(2, .monotonic);
    ring.tail.store(3, .monotonic);

    try std.testing.expectEqual(@as(usize, 0), ring.usage());
}

test "ring buffer preserves FIFO under concurrent spsc access" {
    var ring = try TestQueue.init(std.testing.allocator, 64);
    defer ring.deinit();

    var stress = StressContext{ .ring = &ring, .iterations = 10_000 };

    const producer = try std.Thread.spawn(.{}, stressProducerMain, .{&stress});
    const consumer = try std.Thread.spawn(.{}, stressConsumerMain, .{&stress});

    producer.join();
    consumer.join();

    try std.testing.expect(!stress.failed);
    try std.testing.expectEqual(stress.iterations, stress.consumed);
}

test "generic ring buffer works with another payload type" {
    const AnotherItem = struct {
        value: i32 = 0,

        fn empty() @This() {
            return .{};
        }

        fn deinit(_: *@This(), _: std.mem.Allocator) void {}
    };

    const TestRing = SpscRingBuffer(AnotherItem);

    var ring = try TestRing.init(std.testing.allocator, 2);
    defer ring.deinit();

    var one = AnotherItem{ .value = 1 };
    var two = AnotherItem{ .value = 2 };
    try ring.push(&one);
    try ring.push(&two);

    var out = AnotherItem.empty();
    try std.testing.expect(ring.pop(&out));
    try std.testing.expectEqual(@as(i32, 1), out.value);
    try std.testing.expect(ring.pop(&out));
    try std.testing.expectEqual(@as(i32, 2), out.value);
}

const StressContext = struct {
    ring: *TestQueue,
    iterations: usize,
    producer_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    consumed: usize = 0,
    failed: bool = false,
    expected_seq: usize = 0,
    got_seq: i128 = 0,
};

fn stressProducerMain(ctx: *StressContext) void {
    for (0..ctx.iterations) |seq| {
        var item = makeTestItem(seq);
        while (true) {
            ctx.ring.push(&item) catch {
                std.Thread.yield() catch {};
                continue;
            };
            break;
        }
    }
    ctx.producer_done.store(true, .release);
}

fn stressConsumerMain(ctx: *StressContext) void {
    var expected_seq: usize = 0;
    var item = TestItem.empty();

    while (expected_seq < ctx.iterations) {
        if (ctx.ring.pop(&item)) {
            const expected_value: i128 = @intCast(expected_seq);
            if (item.seq != expected_value) {
                ctx.failed = true;
                ctx.expected_seq = expected_seq;
                ctx.got_seq = item.seq;
                return;
            }
            expected_seq += 1;
            continue;
        }

        if (ctx.producer_done.load(.acquire) and ctx.ring.usage() == 0) break;
        std.Thread.yield() catch {};
    }

    ctx.consumed = expected_seq;
}

const TestItem = struct {
    seq: i128 = 0,

    fn empty() TestItem {
        return .{};
    }

    fn deinit(_: *TestItem, _: std.mem.Allocator) void {}
};

const TestQueue = SpscRingBuffer(TestItem);

fn makeTestItem(seq: usize) TestItem {
    return .{ .seq = @intCast(seq) };
}
