const std = @import("std");
const tty = @import("../../tty.zig");
const config_mod = @import("config.zig");
const sinks = @import("sinks.zig");
const frame_mod = @import("frame.zig");
const api_server_mod = @import("../../api/server.zig");

const idle_sleep_ns: u64 = 100 * std.time.ns_per_us;
const min_log_queue_capacity: usize = 256;

const warn_tag = tty.styledText("[WARN]", .{.yellow});
const error_tag = tty.styledText("[ERROR]", .{.red});
const summary_tag = tty.styledText("[SUMMARY]", .{.aqua});
const hint_tag = tty.styledText("[HINT]", .{.aqua});

pub const monitor_interval_ns: u64 = 250 * std.time.ns_per_ms;

pub const MonitorState = struct {
    high_usage_active: bool = false,
    last_overflow_warned: u64 = 0,
};

pub fn usagePercent(used: usize, capacity: usize) u8 {
    if (capacity == 0) return 0;
    return @intCast((used * 100) / capacity);
}

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: config_mod.ResolvedConfig,
    log_writer: *std.Io.Writer,
    log_is_tty: bool,
    log_queue: sinks.LogQueue,
    log_drop_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    frame_queue: frame_mod.FrameQueue,
    frame_producer_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    log_producer_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    consumer_thread: ?std.Thread = null,
    log_thread: ?std.Thread = null,
    worker_error: ?anyerror = null,
    file_sink: ?sinks.FileSink = null,
    api_server: ?*api_server_mod.ApiServer = null,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: config_mod.ResolvedConfig,
        frame_columns: []const []const u8,
        log_writer: *std.Io.Writer,
        log_is_tty: bool,
    ) !Runtime {
        var log_queue = try sinks.LogQueue.init(allocator, logQueueCapacity(config.buffer_size));
        errdefer log_queue.deinit();

        var frame_queue = try frame_mod.FrameQueue.init(allocator, config.buffer_size);
        errdefer frame_queue.deinit();

        var runtime = Runtime{
            .allocator = allocator,
            .io = io,
            .config = config,
            .log_writer = log_writer,
            .log_is_tty = log_is_tty,
            .log_queue = log_queue,
            .frame_queue = frame_queue,
        };

        if (config.file_path) |path| {
            runtime.file_sink = try sinks.FileSink.init(allocator, io, path, frame_columns);
        }
        errdefer if (runtime.file_sink) |*sink| sink.deinit();

        if (config.api_port) |port| {
            runtime.api_server = try api_server_mod.ApiServer.init(
                allocator,
                io,
                port,
                frame_columns,
                config.buffer_size,
            );
        }
        errdefer if (runtime.api_server) |srv| {
            srv.deinit();
        };

        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        if (self.consumer_thread != null) unreachable;
        if (self.log_thread != null) unreachable;
        if (self.api_server) |srv| {
            srv.stop();
            srv.deinit();
        }
        if (self.file_sink) |*sink| sink.deinit();
        self.frame_queue.deinit();
        self.log_queue.deinit();
    }

    pub fn start(self: *Runtime) !void {
        if (self.api_server) |srv| try srv.start();
        self.consumer_thread = try std.Thread.spawn(.{}, workerMain, .{self});
        errdefer {
            self.frame_producer_done.store(true, .release);
            self.consumer_thread.?.join();
            self.consumer_thread = null;
        }
        self.log_thread = try std.Thread.spawn(.{}, logWorkerMain, .{self});
    }

    pub fn asyncLog(self: *Runtime) sinks.AsyncLog {
        return .{
            .allocator = self.allocator,
            .queue = &self.log_queue,
            .dropped_count = &self.log_drop_count,
        };
    }

    pub fn publish(self: *Runtime, frame: *frame_mod.Frame) bool {
        self.frame_queue.push(frame) catch |err| switch (err) {
            error.BufferOverflow => {
                frame.deinit(self.allocator);
                self.asyncLog().writeAll("[WARN] frame buffer overflow; stopping run\n");
                return false;
            },
        };
        return true;
    }

    pub fn markProducerDone(self: *Runtime) void {
        self.frame_producer_done.store(true, .release);
    }

    pub fn join(self: *Runtime) void {
        if (self.consumer_thread) |thread| {
            thread.join();
            self.consumer_thread = null;
        }
        // Consumer is done; let the broadcaster drain remaining frames and exit.
        if (self.api_server) |srv| srv.hub.stopAndJoin();
    }

    pub fn finishLogs(self: *Runtime) void {
        self.log_producer_done.store(true, .release);
        if (self.log_thread) |thread| {
            thread.join();
            self.log_thread = null;
        }

        const dropped_logs = self.log_queue.overflowCount() + self.log_drop_count.load(.monotonic);
        if (dropped_logs > 0) {
            self.log_writer.print(warn_tag ++ " dropped log messages: {d}\n", .{dropped_logs}) catch {};
        }
    }

    pub fn workerResult(self: *const Runtime) ?anyerror {
        return self.worker_error;
    }

    pub fn emitWarnings(self: *Runtime, state: *MonitorState) void {
        const usage = self.frame_queue.usage();
        const usage_percent = usagePercent(usage, self.frame_queue.capacity);
        if (usage_percent >= self.config.warn_usage_percent) {
            if (!state.high_usage_active) {
                self.asyncLog().print(warn_tag ++ " buffer usage high: {d}%\n", .{usage_percent});
                self.asyncLog().writeAll(hint_tag ++ " increase buffer size or reduce sampling rate\n");
                state.high_usage_active = true;
            }
        } else if (usage_percent + 10 < self.config.warn_usage_percent) {
            state.high_usage_active = false;
        }

        const overflows = self.frame_queue.overflowCount();
        if (overflows > state.last_overflow_warned) {
            self.asyncLog().print(warn_tag ++ " frame buffer overflows: {d}\n", .{overflows});
            self.asyncLog().writeAll(hint_tag ++ " increase buffer size or reduce sampling rate\n");
            state.last_overflow_warned = overflows;
        }
    }

    pub fn writeSummary(self: *Runtime) void {
        const max_usage = self.frame_queue.highWatermark();
        const current_usage = self.frame_queue.usage();
        const overflows = self.frame_queue.overflowCount();

        self.asyncLog().writeAll(summary_tag ++ " overflow strategy: warn_and_stop\n");
        self.asyncLog().print(summary_tag ++ " buffer capacity: {d}\n", .{self.frame_queue.capacity});
        self.asyncLog().print(
            summary_tag ++ " max buffer usage: {d}/{d} ({d}%)\n",
            .{ max_usage, self.frame_queue.capacity, usagePercent(max_usage, self.frame_queue.capacity) },
        );
        self.asyncLog().print(
            summary_tag ++ " current usage ratio: {d}/{d} ({d}%)\n",
            .{ current_usage, self.frame_queue.capacity, usagePercent(current_usage, self.frame_queue.capacity) },
        );
        self.asyncLog().print(summary_tag ++ " frame buffer overflows: {d}\n", .{overflows});
        if (overflows > 0) {
            self.asyncLog().writeAll(hint_tag ++ " increase buffer size or reduce sampling rate\n");
        }
    }

    fn workerMain(self: *Runtime) void {
        self.runWorker() catch |err| {
            self.worker_error = err;
        };
    }

    fn logWorkerMain(self: *Runtime) void {
        self.runLogWorker() catch {};
    }

    fn runWorker(self: *Runtime) !void {
        var frame: frame_mod.Frame = .{};
        while (true) {
            if (self.frame_queue.pop(&frame)) {
                defer frame.deinit(self.allocator);
                try self.processFrame(&frame);
                continue;
            }

            if (self.frame_producer_done.load(.acquire)) break;
            self.io.sleep(.fromNanoseconds(idle_sleep_ns), .awake) catch break;
        }

        while (self.frame_queue.pop(&frame)) {
            defer frame.deinit(self.allocator);
            try self.processFrame(&frame);
        }
    }

    fn runLogWorker(self: *Runtime) !void {
        var message: sinks.LogMessage = .{ .log = &.{} };
        // Heap-allocated copy of the last echo area text, or empty if none.
        var last_echo: []u8 = &.{};
        // Number of newlines in last_echo (= number of lines drawn on screen).
        var echo_lines: usize = 0;
        defer if (last_echo.len > 0) self.allocator.free(last_echo);
        const is_tty = self.log_is_tty;

        while (true) {
            if (self.log_queue.pop(&message)) {
                defer message.deinit(self.allocator);
                switch (message) {
                    .log => |bytes| {
                        // Erase live echo area so the log line appears above it.
                        if (is_tty and echo_lines > 0) {
                            try eraseEchoArea(self.log_writer, echo_lines);
                            echo_lines = 0;
                        }
                        try self.log_writer.writeAll(bytes);
                        // Redraw echo below the new log line.
                        if (is_tty and last_echo.len > 0) {
                            try self.log_writer.writeAll(last_echo);
                            echo_lines = countNewlines(last_echo);
                        }
                    },
                    .echo => |bytes| {
                        if (is_tty) {
                            if (echo_lines > 0) try eraseEchoArea(self.log_writer, echo_lines);
                            // Dupe before freeing old so OOM leaves last_echo intact.
                            const new_echo = try self.allocator.dupe(u8, bytes);
                            if (last_echo.len > 0) self.allocator.free(last_echo);
                            last_echo = new_echo;
                            try self.log_writer.writeAll(last_echo);
                            echo_lines = countNewlines(last_echo);
                        }
                    },
                }
                continue;
            }

            if (self.log_producer_done.load(.acquire)) break;
            self.io.sleep(.fromNanoseconds(idle_sleep_ns), .awake) catch break;
        }

        // Erase the live echo area before draining final log messages.
        if (is_tty and echo_lines > 0) {
            try eraseEchoArea(self.log_writer, echo_lines);
        }

        while (self.log_queue.pop(&message)) {
            defer message.deinit(self.allocator);
            switch (message) {
                .log => |bytes| try self.log_writer.writeAll(bytes),
                .echo => {}, // discard echo updates during shutdown drain
            }
        }
    }

    fn processFrame(self: *Runtime, frame: *const frame_mod.Frame) !void {
        try self.writeFileSink(frame);
        if (self.api_server) |srv| srv.hub.push(frame);
    }

    fn writeFileSink(self: *Runtime, frame: *const frame_mod.Frame) !void {
        if (self.file_sink) |*sink| {
            sink.writeFrame(frame) catch |err| {
                self.asyncLog().print(error_tag ++ " file sink write failed: {s}\n", .{@errorName(err)});
                return error.FileSinkFailed;
            };
        }
    }
};

fn logQueueCapacity(frame_buffer_size: usize) usize {
    return if (frame_buffer_size < min_log_queue_capacity) min_log_queue_capacity else frame_buffer_size;
}

/// Moves the cursor up `lines` lines to the beginning of that line and
/// clears everything from there to the end of the screen, erasing the echo area.
fn eraseEchoArea(writer: *std.Io.Writer, lines: usize) !void {
    try tty.cursor.goUp(writer, lines);
    try tty.clear.toScreenEnd(writer);
}

/// Counts the number of newline characters in `text`.
fn countNewlines(text: []const u8) usize {
    return std.mem.count(u8, text, "\n");
}
