const std = @import("std");
const hub_mod = @import("hub.zig");

const Hub = hub_mod.Hub;
const Client = hub_mod.Client;

pub const ApiServer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    hub: Hub,
    port: u16,
    net_server: std.Io.net.Server,
    accept_thread: ?std.Thread,
    stopped: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        port: u16,
        columns: []const []const u8,
        buffer_size: usize,
    ) !*ApiServer {
        const self = try allocator.create(ApiServer);
        errdefer allocator.destroy(self);

        const hub = try Hub.init(allocator, io, columns, buffer_size);
        errdefer {
            var h = hub;
            h.deinit();
        }

        const addr = try std.Io.net.IpAddress.parse("0.0.0.0", port);
        const net_server = try addr.listen(io, .{ .reuse_address = true });

        self.* = .{
            .allocator = allocator,
            .io = io,
            .hub = hub,
            .port = port,
            .net_server = net_server,
            .accept_thread = null,
            .stopped = .init(false),
        };
        return self;
    }

    pub fn deinit(self: *ApiServer) void {
        std.debug.assert(self.accept_thread == null);
        self.hub.deinit();
        self.allocator.destroy(self);
    }

    pub fn start(self: *ApiServer) !void {
        try self.hub.start();
        self.accept_thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
    }

    pub fn stop(self: *ApiServer) void {
        self.stopped.store(true, .release);
        self.net_server.socket.close(self.io);
        if (self.accept_thread) |t| {
            t.join();
            self.accept_thread = null;
        }
    }

    fn acceptLoop(self: *ApiServer) void {
        while (!self.stopped.load(.acquire)) {
            const stream = self.net_server.accept(self.io) catch break;
            const thread = std.Thread.spawn(.{}, handleConn, .{ self, stream }) catch {
                stream.close(self.io);
                continue;
            };
            thread.detach();
        }
    }

    fn handleConn(self: *ApiServer, stream: std.Io.net.Stream) void {
        defer stream.close(self.io);

        var read_buf: [4096]u8 = undefined;
        var stream_reader = stream.reader(self.io, &read_buf);
        var write_buf: [4096]u8 = undefined;
        var stream_writer = stream.writer(self.io, &write_buf);

        var http_server = std.http.Server.init(&stream_reader.interface, &stream_writer.interface);
        var request = http_server.receiveHead() catch return;

        switch (request.upgradeRequested()) {
            .websocket => |key_opt| {
                const key = key_opt orelse return;
                self.handleWebSocket(&request, key) catch {};
            },
            else => self.handleHttp(&request) catch {},
        }
    }

    // -----------------------------------------------------------------------
    // WebSocket
    // -----------------------------------------------------------------------

    fn handleWebSocket(
        self: *ApiServer,
        request: *std.http.Server.Request,
        key: []const u8,
    ) !void {
        var ws = try request.respondWebSocket(.{ .key = key });
        try ws.flush();

        const client = try Client.init(
            self.allocator,
            self.io,
            &ws,
            self.hub.columns,
            100, // default flush_ms; client can override via {"flush_ms": N}
            hub_mod.default_client_buffer_size,
        );
        defer client.deinit();

        try self.hub.registerClient(client);
        defer self.hub.unregisterClient(client);

        const flush_thread = try std.Thread.spawn(.{}, Client.runFlushLoop, .{client});
        defer {
            client.stop();
            flush_thread.join();
        }

        // Read loop: handle client config messages such as {"flush_ms": 0}.
        while (true) {
            const msg = ws.readSmallMessage() catch break;
            if (msg.opcode != .text and msg.opcode != .binary) continue;

            const parsed = std.json.parseFromSlice(
                struct { flush_ms: ?u64 = null },
                self.allocator,
                msg.data,
                .{ .ignore_unknown_fields = true },
            ) catch continue;
            defer parsed.deinit();

            if (parsed.value.flush_ms) |ms| {
                client.flush_ms.store(ms, .release);
                if (ms == 0) {
                    client.mutex.lockUncancelable(self.io);
                    client.cond.signal(self.io);
                    client.mutex.unlock(self.io);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // HTTP
    // -----------------------------------------------------------------------

    fn handleHttp(self: *ApiServer, request: *std.http.Server.Request) !void {
        // Strip query string for routing.
        const target = request.head.target;
        const path = if (std.mem.indexOfScalar(u8, target, '?')) |q| target[0..q] else target;

        if (std.mem.eql(u8, path, "/status")) {
            try self.serveStatus(request);
        } else if (std.mem.eql(u8, path, "/columns")) {
            try self.serveColumns(request);
        } else {
            try request.respond("{\"error\":\"not found\"}", .{
                .status = .not_found,
                .keep_alive = false,
                .extra_headers = &cors_json_headers,
            });
        }
    }

    fn serveStatus(self: *ApiServer, request: *std.http.Server.Request) !void {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();
        const w = &out.writer;

        try w.print(
            "{{\"running\":{s},\"frame_count\":{d},\"columns\":[",
            .{
                if (self.hub.running.load(.acquire)) "true" else "false",
                self.hub.frame_count.load(.monotonic),
            },
        );
        for (self.hub.columns, 0..) |col, i| {
            if (i > 0) try w.writeByte(',');
            try std.json.Stringify.encodeJsonString(col, .{}, w);
        }
        try w.writeAll("],\"latest\":{");
        _ = try self.hub.writeLatestJson(w);
        try w.writeAll("}}");

        try request.respond(out.written(), .{
            .status = .ok,
            .keep_alive = false,
            .extra_headers = &cors_json_headers,
        });
    }

    fn serveColumns(self: *ApiServer, request: *std.http.Server.Request) !void {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();
        const w = &out.writer;

        try w.writeByte('[');
        for (self.hub.columns, 0..) |col, i| {
            if (i > 0) try w.writeByte(',');
            try std.json.Stringify.encodeJsonString(col, .{}, w);
        }
        try w.writeByte(']');

        try request.respond(out.written(), .{
            .status = .ok,
            .keep_alive = false,
            .extra_headers = &cors_json_headers,
        });
    }
};

const cors_json_headers = [_]std.http.Header{
    .{ .name = "Content-Type", .value = "application/json" },
    .{ .name = "Access-Control-Allow-Origin", .value = "*" },
};
