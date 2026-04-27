const std = @import("std");
const session = @import("../session.zig");
const ring_buffer_mod = @import("ring_buffer.zig");
const frame_mod = @import("frame.zig");

/// Queue message that owns its heap-allocated text buffer.
pub const LogMessage = struct {
    text: []u8 = &.{},

    pub fn deinit(self: *LogMessage, allocator: std.mem.Allocator) void {
        if (self.text.len > 0) allocator.free(self.text);
    }
};

pub const LogQueue = ring_buffer_mod.SpscRingBuffer(LogMessage);

pub const AsyncLog = struct {
    allocator: std.mem.Allocator,
    queue: *LogQueue,
    dropped_count: *std.atomic.Value(u64),

    pub fn writeAll(self: AsyncLog, bytes: []const u8) void {
        const owned = self.allocator.dupe(u8, bytes) catch {
            self.recordDrop();
            return;
        };
        self.enqueueOwned(owned);
    }

    pub fn print(self: AsyncLog, comptime fmt: []const u8, args: anytype) void {
        const owned = std.fmt.allocPrint(self.allocator, fmt, args) catch {
            self.recordDrop();
            return;
        };
        self.enqueueOwned(owned);
    }

    fn enqueueOwned(self: AsyncLog, owned: []u8) void {
        var message = LogMessage{ .text = owned };
        self.queue.push(&message) catch |err| switch (err) {
            error.BufferOverflow => {
                message.deinit(self.allocator);
                return;
            },
        };
    }

    fn recordDrop(self: AsyncLog) void {
        _ = self.dropped_count.fetchAdd(1, .monotonic);
    }

    pub fn logSink(self: *AsyncLog) session.LogSink {
        return .{
            .context = @ptrCast(self),
            .writeFn = struct {
                fn write(ctx: *anyopaque, bytes: []const u8) void {
                    const log: *AsyncLog = @ptrCast(@alignCast(ctx));
                    log.writeAll(bytes);
                }
            }.write,
        };
    }
};

pub const FileSink = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    frame_columns: []const []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, path: []const u8, frame_columns: []const []const u8) !FileSink {
        const columns_copy = try allocator.dupe([]const u8, frame_columns);
        errdefer allocator.free(columns_copy);

        var sink = FileSink{
            .allocator = allocator,
            .io = io,
            .file = if (std.fs.path.isAbsolute(path))
                try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true })
            else
                try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true }),
            .frame_columns = columns_copy,
        };
        errdefer sink.file.close(io);

        try sink.writeHeader();
        return sink;
    }

    pub fn deinit(self: *FileSink) void {
        self.allocator.free(self.frame_columns);
        self.file.close(self.io);
    }

    pub fn writeFrame(self: *FileSink, frame: *const frame_mod.Frame) !void {
        var io_buffer: [4096]u8 = undefined;
        var file_writer = self.file.writerStreaming(self.io, &io_buffer);
        const writer = &file_writer.interface;

        var first = true;
        for (0..self.frame_columns.len) |col| {
            if (!first) try writer.writeByte(',');
            first = false;
            if (frame.getColumn(col)) |value| {
                try writeCsvField(writer, value);
            }
        }
        try writer.writeByte('\n');
        try file_writer.interface.flush();
    }

    fn writeHeader(self: *FileSink) !void {
        var io_buffer: [1024]u8 = undefined;
        var file_writer = self.file.writerStreaming(self.io, &io_buffer);
        const writer = &file_writer.interface;

        var first = true;
        for (self.frame_columns) |column_name| {
            if (!first) try writer.writeByte(',');
            first = false;
            try writeCsvField(writer, column_name);
        }
        try writer.writeByte('\n');
        try file_writer.interface.flush();
    }
};

pub const NetworkSink = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    frame_columns: []const []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, host: []const u8, port: u16, frame_columns: []const []const u8) !NetworkSink {
        const columns_copy = try allocator.dupe([]const u8, frame_columns);
        errdefer allocator.free(columns_copy);

        const addr = try std.Io.net.IpAddress.resolve(io, host, port);
        return .{
            .io = io,
            .stream = try addr.connect(io, .{ .mode = .stream }),
            .frame_columns = columns_copy,
        };
    }

    pub fn deinit(self: *NetworkSink, allocator: std.mem.Allocator) void {
        allocator.free(self.frame_columns);
        self.stream.close(self.io);
    }

    pub fn writeFrame(self: *NetworkSink, frame: *const frame_mod.Frame) !void {
        var io_buffer: [2048]u8 = undefined;
        var stream_writer = self.stream.writer(self.io, &io_buffer);
        try writeFrameJson(&stream_writer.interface, frame, self.frame_columns);
        try stream_writer.interface.flush();
    }
};

fn writeFrameJson(writer: *std.Io.Writer, frame: *const frame_mod.Frame, columns: []const []const u8) !void {
    try writer.writeAll("{\"fields\":[");

    var first = true;
    for (columns, 0..) |name, col| {
        if (frame.getColumn(col)) |value| {
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.writeAll("{\"name\":");
            try writeJsonString(writer, name);
            try writer.writeAll(",\"value\":");
            try writeJsonString(writer, value);
            try writer.writeByte('}');
        }
    }

    try writer.writeAll("]}");
    try writer.writeAll("\n");
}

fn writeJsonString(writer: *std.Io.Writer, value: []const u8) !void {
    try std.json.Stringify.encodeJsonString(value, .{}, writer);
}

fn writeCsvField(writer: anytype, field: []const u8) !void {
    try writer.writeByte('"');
    for (field) |byte| {
        if (byte == '"') {
            try writer.writeAll("\"\"");
        } else {
            try writer.writeByte(byte);
        }
    }
    try writer.writeByte('"');
}

test "file sink writes one frame row with all saved fields" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", gpa);
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "samples.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{ "voltage", "current" };
    var sink: FileSink = try .init(gpa, std.testing.io, sink_path, &columns);
    defer sink.deinit();

    const values = try gpa.alloc(?[]u8, 2);
    values[0] = try gpa.dupe(u8, "1.23");
    values[1] = try gpa.dupe(u8, "0.45");

    var frame = frame_mod.Frame{
        .values = values,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(std.testing.io, "samples.csv", gpa, .limited(8 * 1024));
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings(
        "\"voltage\",\"current\"\n\"1.23\",\"0.45\"\n",
        file_data,
    );
}

test "file sink leaves unrelated frame columns blank" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", gpa);
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "partial.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{ "voltage", "current" };
    var sink: FileSink = try .init(gpa, std.testing.io, sink_path, &columns);
    defer sink.deinit();

    const values = try gpa.alloc(?[]u8, 2);
    values[0] = try gpa.dupe(u8, "1.23");
    values[1] = null;

    var frame = frame_mod.Frame{
        .values = values,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(std.testing.io, "partial.csv", gpa, .limited(8 * 1024));
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings(
        "\"voltage\",\"current\"\n\"1.23\",\n",
        file_data,
    );
}

test "file sink escapes csv quotes commas and newlines in frame values" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", gpa);
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "escaped.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{"reading"};
    var sink: FileSink = try .init(gpa, std.testing.io, sink_path, &columns);
    defer sink.deinit();

    const values = try gpa.alloc(?[]u8, 1);
    values[0] = try gpa.dupe(u8, "1,\"2\"\n3");

    var frame = frame_mod.Frame{
        .values = values,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(std.testing.io, "escaped.csv", gpa, .limited(8 * 1024));
    defer gpa.free(file_data);

    try std.testing.expect(std.mem.containsAtLeast(u8, file_data, 1, "\"1,\"\"2\"\"\n3\""));
}

test "writeFrameJson escapes column names and values without heap allocation" {
    const gpa = std.testing.allocator;

    const values = try gpa.alloc(?[]u8, 2);
    values[0] = try gpa.dupe(u8, "1,\"2\"\n3");
    values[1] = null;

    var frame = frame_mod.Frame{
        .values = values,
    };
    defer frame.deinit(gpa);

    const columns = [_][]const u8{ "channel\"name", "unused" };

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try writeFrameJson(&out.writer, &frame, &columns);
    try std.testing.expectEqualStrings(
        "{\"fields\":[{\"name\":\"channel\\\"name\",\"value\":\"1,\\\"2\\\"\\n3\"}]}\n",
        out.written(),
    );
}
