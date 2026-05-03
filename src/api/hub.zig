const std = @import("std");
const frame_mod = @import("../executor/pipeline/frame.zig");
const ring_buffer_mod = @import("../executor/pipeline/ring_buffer.zig");

pub const default_client_buffer_size: usize = 256;
const idle_ns: u64 = 100 * std.time.ns_per_us;

// ---------------------------------------------------------------------------
// Client — per-WebSocket-connection state
// ---------------------------------------------------------------------------

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    ws: *std.http.Server.WebSocket,
    columns: []const []const u8,
    queue: ring_buffer_mod.SpscRingBuffer(frame_mod.Frame),
    flush_ms: std.atomic.Value(u64), // 0 = send each frame immediately
    stopped: std.atomic.Value(bool),
    mutex: std.Io.Mutex,
    cond: std.Io.Condition,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        ws: *std.http.Server.WebSocket,
        columns: []const []const u8,
        flush_ms: u64,
        buffer_size: usize,
    ) !*Client {
        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);
        const queue = try ring_buffer_mod.SpscRingBuffer(frame_mod.Frame).init(allocator, buffer_size);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .ws = ws,
            .columns = columns,
            .queue = queue,
            .flush_ms = .init(flush_ms),
            .stopped = .init(false),
            .mutex = .init,
            .cond = .init,
        };
        return self;
    }

    pub fn deinit(self: *Client) void {
        self.queue.deinit();
        self.allocator.destroy(self);
    }

    /// Called by Hub broadcaster (producer side). Non-blocking; drops on overflow.
    pub fn push(self: *Client, frame: *const frame_mod.Frame) void {
        var copy = cloneFrame(self.allocator, frame) catch return;
        self.queue.push(&copy) catch {
            copy.deinit(self.allocator);
            return;
        };
        // Wake up flush loop immediately when in instant mode.
        if (self.flush_ms.load(.acquire) == 0) {
            self.mutex.lockUncancelable(self.io);
            self.cond.signal(self.io);
            self.mutex.unlock(self.io);
        }
    }

    /// Signal the flush loop to exit. Safe to call from any thread.
    pub fn stop(self: *Client) void {
        self.stopped.store(true, .release);
        self.mutex.lockUncancelable(self.io);
        self.cond.signal(self.io);
        self.mutex.unlock(self.io);
    }

    /// Runs on the connection's flush thread. Blocks until stopped.
    pub fn runFlushLoop(self: *Client) void {
        while (!self.stopped.load(.acquire)) {
            const ms = self.flush_ms.load(.acquire);
            if (ms == 0) {
                self.mutex.lockUncancelable(self.io);
                self.cond.waitUncancelable(self.io, &self.mutex);
                self.mutex.unlock(self.io);
            } else {
                self.io.sleep(.fromMilliseconds(@intCast(ms)), .awake) catch {};
            }
            if (!self.stopped.load(.acquire)) self.drainAndSend();
        }
        self.drainAndSend(); // final flush before exit
    }

    fn drainAndSend(self: *Client) void {
        var frames: [default_client_buffer_size]frame_mod.Frame = undefined;
        var count: usize = 0;
        while (count < frames.len) {
            if (!self.queue.pop(&frames[count])) break;
            count += 1;
        }
        if (count == 0) return;
        defer for (frames[0..count]) |*f| f.deinit(self.allocator);

        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();
        const w = &out.writer;

        w.writeAll("{\"frames\":[") catch return;
        for (frames[0..count], 0..) |*frame, i| {
            if (i > 0) w.writeByte(',') catch return;
            writeFrameJson(w, frame, self.columns) catch return;
        }
        w.writeAll("]}") catch return;

        self.ws.writeMessage(out.written(), .text) catch {
            self.stop();
        };
    }
};

// ---------------------------------------------------------------------------
// Hub — central broadcaster
// ---------------------------------------------------------------------------

pub const Hub = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    columns: []const []const u8,
    queue: ring_buffer_mod.SpscRingBuffer(frame_mod.Frame),
    producer_done: std.atomic.Value(bool),

    clients_mutex: std.Io.Mutex,
    clients: std.ArrayListUnmanaged(*Client),

    latest_mutex: std.Io.Mutex,
    latest_frame: frame_mod.Frame,
    frame_count: std.atomic.Value(u64),
    running: std.atomic.Value(bool),

    broadcaster_thread: ?std.Thread,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        columns: []const []const u8,
        buffer_size: usize,
    ) !Hub {
        return .{
            .allocator = allocator,
            .io = io,
            .columns = columns,
            .queue = try ring_buffer_mod.SpscRingBuffer(frame_mod.Frame).init(allocator, buffer_size),
            .producer_done = .init(false),
            .clients_mutex = .init,
            .clients = .empty,
            .latest_mutex = .init,
            .latest_frame = .{},
            .frame_count = .init(0),
            .running = .init(true),
            .broadcaster_thread = null,
        };
    }

    pub fn deinit(self: *Hub) void {
        std.debug.assert(self.broadcaster_thread == null);
        self.latest_frame.deinit(self.allocator);
        self.clients.deinit(self.allocator);
        self.queue.deinit();
    }

    pub fn start(self: *Hub) !void {
        self.broadcaster_thread = try std.Thread.spawn(.{}, broadcasterMain, .{self});
    }

    pub fn stopAndJoin(self: *Hub) void {
        self.producer_done.store(true, .release);
        if (self.broadcaster_thread) |t| {
            t.join();
            self.broadcaster_thread = null;
        }
        self.running.store(false, .release);
    }

    pub fn push(self: *Hub, frame: *const frame_mod.Frame) void {
        var copy = cloneFrame(self.allocator, frame) catch return;
        self.queue.push(&copy) catch {
            copy.deinit(self.allocator);
        };
    }

    pub fn registerClient(self: *Hub, client: *Client) !void {
        self.clients_mutex.lockUncancelable(self.io);
        defer self.clients_mutex.unlock(self.io);
        try self.clients.append(self.allocator, client);
    }

    pub fn unregisterClient(self: *Hub, client: *Client) void {
        self.clients_mutex.lockUncancelable(self.io);
        defer self.clients_mutex.unlock(self.io);
        for (self.clients.items, 0..) |c, i| {
            if (c == client) {
                _ = self.clients.swapRemove(i);
                return;
            }
        }
    }

    /// Writes the latest frame as a JSON object `{"col": "val", ...}` into `w`
    /// while holding the mutex. Returns false if no frame has been received.
    pub fn writeLatestJson(self: *Hub, w: *std.Io.Writer) !bool {
        self.latest_mutex.lockUncancelable(self.io);
        defer self.latest_mutex.unlock(self.io);
        if (self.latest_frame.values.len == 0) return false;
        var first = true;
        for (self.columns, 0..) |col, i| {
            if (self.latest_frame.getColumn(i)) |val| {
                if (!first) try w.writeByte(',');
                first = false;
                try std.json.Stringify.encodeJsonString(col, .{}, w);
                try w.writeByte(':');
                try std.json.Stringify.encodeJsonString(val, .{}, w);
            }
        }
        return true;
    }

    fn broadcasterMain(self: *Hub) void {
        var frame: frame_mod.Frame = .{};
        while (true) {
            if (self.queue.pop(&frame)) {
                self.fanOut(&frame);
                frame.deinit(self.allocator);
                continue;
            }
            if (self.producer_done.load(.acquire)) break;
            self.io.sleep(.fromNanoseconds(idle_ns), .awake) catch break;
        }
        while (self.queue.pop(&frame)) {
            self.fanOut(&frame);
            frame.deinit(self.allocator);
        }
    }

    fn fanOut(self: *Hub, frame: *const frame_mod.Frame) void {
        _ = self.frame_count.fetchAdd(1, .monotonic);

        self.latest_mutex.lockUncancelable(self.io);
        self.latest_frame.deinit(self.allocator);
        self.latest_frame = cloneFrame(self.allocator, frame) catch .{};
        self.latest_mutex.unlock(self.io);

        self.clients_mutex.lockUncancelable(self.io);
        defer self.clients_mutex.unlock(self.io);
        for (self.clients.items) |client| {
            client.push(frame);
        }
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn cloneFrame(allocator: std.mem.Allocator, src: *const frame_mod.Frame) !frame_mod.Frame {
    if (src.values.len == 0) return .{};
    const values = try allocator.alloc(?[]u8, src.values.len);
    var cloned: usize = 0;
    errdefer {
        for (values[0..cloned]) |v| if (v) |s| allocator.free(s);
        allocator.free(values);
    }
    for (src.values, 0..) |v, i| {
        values[i] = if (v) |s| try allocator.dupe(u8, s) else null;
        cloned += 1;
    }
    return .{ .values = values };
}

fn writeFrameJson(w: *std.Io.Writer, frame: *const frame_mod.Frame, columns: []const []const u8) !void {
    try w.writeAll("{\"fields\":[");
    var first = true;
    for (columns, 0..) |name, col| {
        if (frame.getColumn(col)) |value| {
            if (!first) try w.writeByte(',');
            first = false;
            try w.writeAll("{\"name\":");
            try std.json.Stringify.encodeJsonString(name, .{}, w);
            try w.writeAll(",\"value\":");
            try std.json.Stringify.encodeJsonString(value, .{}, w);
            try w.writeByte('}');
        }
    }
    try w.writeAll("]}");
}
