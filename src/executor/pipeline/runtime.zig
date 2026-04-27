const std = @import("std");
const tty = @import("../../tty.zig");
const config_mod = @import("config.zig");
const sinks = @import("sinks.zig");
const frame_mod = @import("frame.zig");

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
    log_queue: sinks.LogQueue,
    log_drop_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    frame_queue: frame_mod.FrameQueue,
    frame_producer_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    log_producer_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    consumer_thread: ?std.Thread = null,
    log_thread: ?std.Thread = null,
    worker_error: ?anyerror = null,
    file_sink: ?sinks.FileSink = null,
    network_sink: ?sinks.NetworkSink = null,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: config_mod.ResolvedConfig,
        frame_columns: []const []const u8,
        log_writer: *std.Io.Writer,
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
            .log_queue = log_queue,
            .frame_queue = frame_queue,
        };

        if (config.file_path) |path| {
            runtime.file_sink = try sinks.FileSink.init(allocator, io, path, frame_columns);
        }
        errdefer if (runtime.file_sink) |*sink| sink.deinit();

        if (config.network_host) |host| {
            runtime.network_sink = try sinks.NetworkSink.init(allocator, io, host, config.network_port.?, frame_columns);
        }
        errdefer if (runtime.network_sink) |*sink| sink.deinit();

        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        if (self.consumer_thread != null) unreachable;
        if (self.log_thread != null) unreachable;
        if (self.network_sink) |*sink| sink.deinit(self.allocator);
        if (self.file_sink) |*sink| sink.deinit();
        self.frame_queue.deinit();
        self.log_queue.deinit();
    }

    pub fn start(self: *Runtime) !void {
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
        var message: sinks.LogMessage = .{};
        while (true) {
            if (self.log_queue.pop(&message)) {
                defer message.deinit(self.allocator);
                try self.log_writer.writeAll(message.text);
                continue;
            }

            if (self.log_producer_done.load(.acquire)) break;
            self.io.sleep(.fromNanoseconds(idle_sleep_ns), .awake) catch break;
        }

        while (self.log_queue.pop(&message)) {
            defer message.deinit(self.allocator);
            try self.log_writer.writeAll(message.text);
        }
    }

    fn processFrame(self: *Runtime, frame: *const frame_mod.Frame) !void {
        try self.writeFileSink(frame);
        try self.writeNetworkSink(frame);
    }

    fn writeFileSink(self: *Runtime, frame: *const frame_mod.Frame) !void {
        if (self.file_sink) |*sink| {
            sink.writeFrame(frame) catch |err| {
                self.asyncLog().print(error_tag ++ " file sink write failed: {s}\n", .{@errorName(err)});
                return error.FileSinkFailed;
            };
        }
    }

    fn writeNetworkSink(self: *Runtime, frame: *const frame_mod.Frame) !void {
        if (self.network_sink) |*sink| {
            sink.writeFrame(frame) catch |err| {
                sink.deinit(self.allocator);
                self.network_sink = null;
                self.asyncLog().print(warn_tag ++ " network sink disabled: {s}\n", .{@errorName(err)});
            };
        }
    }
};

fn logQueueCapacity(frame_buffer_size: usize) usize {
    return if (frame_buffer_size < min_log_queue_capacity) min_log_queue_capacity else frame_buffer_size;
}
