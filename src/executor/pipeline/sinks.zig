const std = @import("std");
const ring_buffer_mod = @import("ring_buffer.zig");
const types = @import("types.zig");

/// Queue message that owns its heap-allocated text buffer.
pub const LogMessage = struct {
    text_owned: ?[]u8 = null,

    pub fn empty() LogMessage {
        return .{};
    }

    pub fn deinit(self: *LogMessage, allocator: std.mem.Allocator) void {
        if (self.text_owned) |owned_text| allocator.free(owned_text);
    }
};

pub const LogQueue = ring_buffer_mod.SpscRingBuffer(LogMessage);

pub const AsyncLog = struct {
    allocator: std.mem.Allocator,
    queue: *LogQueue,
    dropped_count: *std.atomic.Value(u64),

    pub fn writeAll(self: AsyncLog, bytes: []const u8) void {
        const owned = self.allocator.dupe(u8, bytes) catch {
            const current = self.dropped_count.load(.monotonic);
            self.dropped_count.store(current + 1, .monotonic);
            return;
        };
        var message = LogMessage{ .text_owned = owned };
        self.queue.push(&message) catch |err| switch (err) {
            error.BufferOverflow => {
                message.deinit(self.allocator);
                return;
            },
        };
    }

    pub fn print(self: AsyncLog, comptime fmt: []const u8, args: anytype) void {
        const owned = std.fmt.allocPrint(self.allocator, fmt, args) catch {
            const current = self.dropped_count.load(.monotonic);
            self.dropped_count.store(current + 1, .monotonic);
            return;
        };
        var message = LogMessage{ .text_owned = owned };
        self.queue.push(&message) catch |err| switch (err) {
            error.BufferOverflow => {
                message.deinit(self.allocator);
                return;
            },
        };
    }
};

pub const FileSink = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    frame_columns: []const []const u8,

    pub fn init(allocator: std.mem.Allocator, path: []const u8, frame_columns: []const []const u8) !FileSink {
        const columns_copy = try allocator.alloc([]const u8, frame_columns.len);
        errdefer allocator.free(columns_copy);
        std.mem.copyForwards([]const u8, columns_copy, frame_columns);

        var sink = FileSink{
            .allocator = allocator,
            .file = if (std.fs.path.isAbsolute(path))
                try std.fs.createFileAbsolute(path, .{ .truncate = true })
            else
                try std.fs.cwd().createFile(path, .{ .truncate = true }),
            .frame_columns = columns_copy,
        };
        errdefer sink.file.close();
        errdefer sink.allocator.free(sink.frame_columns);

        try sink.writeHeader();
        return sink;
    }

    pub fn deinit(self: *FileSink) void {
        self.allocator.free(self.frame_columns);
        self.file.close();
    }

    pub fn writeFrame(self: *FileSink, frame: *const types.Frame) !void {
        var io_buffer: [4096]u8 = undefined;
        var file_writer = self.file.writerStreaming(&io_buffer);
        const writer = &file_writer.interface;

        try writer.print("{d},{d}", .{ frame.timestamp_ns, frame.task_idx });
        for (self.frame_columns) |column_name| {
            try writer.writeByte(',');
            if (frame.getValue(column_name)) |value| {
                try writeCsvField(writer, value);
            }
        }
        try writer.writeByte('\n');
        try file_writer.interface.flush();
    }

    fn writeHeader(self: *FileSink) !void {
        var io_buffer: [1024]u8 = undefined;
        var file_writer = self.file.writerStreaming(&io_buffer);
        const writer = &file_writer.interface;

        try writer.writeAll("timestamp_ns,task_idx");
        for (self.frame_columns) |column_name| {
            try writer.writeByte(',');
            try writeCsvField(writer, column_name);
        }
        try writer.writeByte('\n');
        try file_writer.interface.flush();
    }
};

pub const NetworkSink = struct {
    stream: std.net.Stream,

    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !NetworkSink {
        return .{
            .stream = try std.net.tcpConnectToHost(allocator, host, port),
        };
    }

    pub fn deinit(self: *NetworkSink) void {
        self.stream.close();
    }

    pub fn writeFrame(self: *NetworkSink, frame: *const types.Frame) !void {
        var io_buffer: [2048]u8 = undefined;
        var stream_writer = self.stream.writer(&io_buffer);
        try writeFrameJson(&stream_writer.interface, frame);
        try stream_writer.interface.flush();
    }
};

fn writeFrameJson(writer: *std.Io.Writer, frame: *const types.Frame) !void {
    const fields = frame.fields_owned orelse &[_]types.FrameField{};

    try writer.writeAll("{\"timestamp_ns\":");
    try writer.print("{d}", .{frame.timestamp_ns});
    try writer.writeAll(",\"task_idx\":");
    try writer.print("{d}", .{frame.task_idx});
    try writer.writeAll(",\"fields\":[");

    for (fields, 0..) |field, idx| {
        if (idx > 0) try writer.writeByte(',');
        try writer.writeAll("{\"name\":");
        try writeJsonString(writer, field.name);
        try writer.writeAll(",\"value\":");
        try writeJsonString(writer, field.value());
        try writer.writeByte('}');
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

    const dir_path = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "samples.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{ "voltage", "current" };
    var sink = try FileSink.init(gpa, sink_path, &columns);
    defer sink.deinit();

    const fields = try gpa.alloc(types.FrameField, 2);
    fields[0] = .{ .name = "voltage", .value_owned = try gpa.dupe(u8, "1.23") };
    fields[1] = .{ .name = "current", .value_owned = try gpa.dupe(u8, "0.45") };

    var frame = types.Frame{
        .timestamp_ns = 10,
        .task_idx = 2,
        .fields_owned = fields,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(gpa, "samples.csv", 8 * 1024);
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings(
        "timestamp_ns,task_idx,\"voltage\",\"current\"\n10,2,\"1.23\",\"0.45\"\n",
        file_data,
    );
}

test "file sink leaves unrelated frame columns blank" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "partial.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{ "voltage", "current" };
    var sink = try FileSink.init(gpa, sink_path, &columns);
    defer sink.deinit();

    const fields = try gpa.alloc(types.FrameField, 1);
    fields[0] = .{ .name = "voltage", .value_owned = try gpa.dupe(u8, "1.23") };

    var frame = types.Frame{
        .timestamp_ns = 11,
        .task_idx = 0,
        .fields_owned = fields,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(gpa, "partial.csv", 8 * 1024);
    defer gpa.free(file_data);

    try std.testing.expectEqualStrings(
        "timestamp_ns,task_idx,\"voltage\",\"current\"\n11,0,\"1.23\",\n",
        file_data,
    );
}

test "file sink escapes csv quotes commas and newlines in frame values" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(dir_path);
    const sink_path = try std.fs.path.join(gpa, &[_][]const u8{ dir_path, "escaped.csv" });
    defer gpa.free(sink_path);

    const columns = [_][]const u8{"reading"};
    var sink = try FileSink.init(gpa, sink_path, &columns);
    defer sink.deinit();

    const fields = try gpa.alloc(types.FrameField, 1);
    fields[0] = .{ .name = "reading", .value_owned = try gpa.dupe(u8, "1,\"2\"\n3") };

    var frame = types.Frame{
        .timestamp_ns = 7,
        .task_idx = 1,
        .fields_owned = fields,
    };
    defer frame.deinit(gpa);

    try sink.writeFrame(&frame);

    const file_data = try tmp.dir.readFileAlloc(gpa, "escaped.csv", 8 * 1024);
    defer gpa.free(file_data);

    try std.testing.expect(std.mem.containsAtLeast(u8, file_data, 1, "\"1,\"\"2\"\"\n3\""));
}

test "writeFrameJson escapes field names and values without heap allocation" {
    const gpa = std.testing.allocator;

    const fields = try gpa.alloc(types.FrameField, 1);
    fields[0] = .{
        .name = "channel\"name",
        .value_owned = try gpa.dupe(u8, "1,\"2\"\n3"),
    };

    var frame = types.Frame{
        .timestamp_ns = 7,
        .task_idx = 1,
        .fields_owned = fields,
    };
    defer frame.deinit(gpa);

    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();

    try writeFrameJson(&out.writer, &frame);
    try std.testing.expectEqualStrings(
        "{\"timestamp_ns\":7,\"task_idx\":1,\"fields\":[{\"name\":\"channel\\\"name\",\"value\":\"1,\\\"2\\\"\\n3\"}]}\n",
        out.written(),
    );
}
