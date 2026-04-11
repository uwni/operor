const std = @import("std");
const mibu = @import("mibu");
const color = mibu.color;
const events = mibu.events;
const term = mibu.term;
const cursorctl = mibu.cursor;
const visa = @import("visa/root.zig");

const repl_prompt = color.print.fg(.aqua) ++ "repl> " ++ color.print.reset;
const err_label = color.print.fg(.red) ++ "error:" ++ color.print.reset;
const repl_max_line_bytes: usize = 4096;
const repl_whitespace = " \t\r\n";

/// Recommended stdin buffer size for line-oriented REPL input.
pub const stdin_buffer_bytes: usize = repl_max_line_bytes + 1;

/// REPL connection state.
const State = enum { disconnected, connected };

/// Parsed REPL command variants.
const Command = union(enum) {
    help,
    quit,
    list,
    open: ?[]const u8,
    close,
    read,
    write: []const u8,
    query: []const u8,
};

/// User-facing parse errors for REPL input.
const ParseError = error{
    UnknownCommand,
    MissingCommandPayload,
    UnexpectedCommandPayload,
};

/// Opens a VISA resource manager and starts the interactive REPL loop.
pub fn run(
    allocator: std.mem.Allocator,
    resource_addr: ?[]const u8,
    visa_lib: ?[]const u8,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !void {
    const vtable = try visa.loader.load(visa_lib);
    var rm: visa.ResourceManager = try .init(&vtable);
    defer rm.deinit();

    var ctx = ReplContext{
        .rm = &rm,
        .vtable = &vtable,
    };
    defer ctx.close();

    if (resource_addr) |addr| {
        try ctx.open(allocator, addr);
        try out.print(color.print.fg(.green) ++ "Connected to {s}" ++ color.print.reset ++ "\n", .{addr});
    }

    try printHelp(out, ctx.state());
    try out.flush();
    defer out.flush() catch {};
    if (std.posix.isatty(std.fs.File.stdin().handle)) {
        var editor = LineEditor.init(allocator, std.fs.File.stdin(), out);
        defer editor.deinit();
        try runLoop(allocator, reader, out, &ctx, &editor);
    } else {
        try runLoop(allocator, reader, out, &ctx, null);
    }
}

/// Production REPL context wrapping a VISA resource manager and optional instrument.
const ReplContext = struct {
    rm: *visa.ResourceManager,
    vtable: *const visa.loader.Vtable,
    instrument: ?visa.Instrument = null,

    fn state(self: *const ReplContext) State {
        return if (self.instrument != null) .connected else .disconnected;
    }

    fn open(self: *ReplContext, allocator: std.mem.Allocator, addr: []const u8) !void {
        var inst: visa.Instrument = .init(self.rm.session, self.vtable);
        try inst.open(allocator, addr, .{});
        self.instrument = inst;
    }

    fn close(self: *ReplContext) void {
        if (self.instrument) |*i| {
            i.deinit();
        }
        self.instrument = null;
    }

    fn listResources(self: *ReplContext, allocator: std.mem.Allocator) !visa.ResourceList {
        return self.rm.listResources(allocator);
    }

    fn write(self: *ReplContext, payload: []const u8) !void {
        return self.instrument.?.write(payload);
    }

    fn readToOwned(self: *ReplContext, allocator: std.mem.Allocator) ![]u8 {
        return self.instrument.?.readToOwned(allocator);
    }

    fn queryToOwned(self: *ReplContext, allocator: std.mem.Allocator, payload: []const u8) ![]u8 {
        return self.instrument.?.queryToOwned(allocator, payload);
    }
};

/// Interactive line editor using mibu raw mode and event handling.
/// Supports left/right arrow keys, Home/End, Backspace/Delete,
/// Up/Down for command history, and Ctrl+A/E/U/K/L shortcuts.
const LineEditor = struct {
    allocator: std.mem.Allocator,
    stdin: std.fs.File,
    out: *std.Io.Writer,
    buf: [repl_max_line_bytes]u8 = undefined,
    len: usize = 0,
    pos: usize = 0,
    history: std.ArrayList([]u8),
    history_index: ?usize = null,
    saved_buf: [repl_max_line_bytes]u8 = undefined,
    saved_len: usize = 0,

    fn init(allocator: std.mem.Allocator, stdin: std.fs.File, out: *std.Io.Writer) LineEditor {
        return .{
            .allocator = allocator,
            .stdin = stdin,
            .out = out,
            .history = .empty,
        };
    }

    fn deinit(self: *LineEditor) void {
        for (self.history.items) |item| self.allocator.free(item);
        self.history.deinit(self.allocator);
    }

    /// Reads one line of input with full editing support.
    /// Returns the line content, or null on Ctrl+D (empty line) / EOF.
    fn editLine(self: *LineEditor, prompt: []const u8) !?[]const u8 {
        // Flush pending output before entering raw mode, where \n no longer implies \r.
        try self.out.flush();
        var raw = try term.enableRawMode(self.stdin.handle);
        defer raw.disableRawMode() catch {};

        self.len = 0;
        self.pos = 0;
        self.history_index = null;

        try self.out.writeAll(prompt);
        try self.out.flush();

        while (true) {
            const event = try events.next(self.stdin);
            switch (event) {
                .key => |k| {
                    if (k.mods.ctrl) {
                        switch (k.code) {
                            .char => |c| switch (c) {
                                'c' => {
                                    try self.out.writeAll("^C\r\n");
                                    try self.out.flush();
                                    return "";
                                },
                                'd' => {
                                    if (self.len == 0) {
                                        try self.out.writeAll("\r\n");
                                        try self.out.flush();
                                        return null;
                                    }
                                },
                                'a' => try self.moveToPos(0),
                                'e' => try self.moveToPos(self.len),
                                'u' => {
                                    const tail = self.len - self.pos;
                                    std.mem.copyForwards(u8, self.buf[0..tail], self.buf[self.pos..self.len]);
                                    self.len = tail;
                                    self.pos = 0;
                                    try self.refreshLine(prompt);
                                },
                                'k' => {
                                    self.len = self.pos;
                                    try self.refreshLine(prompt);
                                },
                                'l' => {
                                    try self.out.writeAll("\x1b[2J\x1b[H");
                                    try self.refreshLine(prompt);
                                },
                                else => {},
                            },
                            else => {},
                        }
                    } else {
                        switch (k.code) {
                            .char => |c| try self.insertChar(c, prompt),
                            .enter => {
                                try self.out.writeAll("\r\n");
                                try self.out.flush();
                                const line = self.buf[0..self.len];
                                const trimmed = std.mem.trim(u8, line, repl_whitespace);
                                if (trimmed.len > 0) try self.addHistory(trimmed);
                                return line;
                            },
                            .backspace => try self.deleteBack(prompt),
                            .delete => try self.deleteForward(prompt),
                            .left => {
                                if (self.pos > 0) {
                                    self.pos -= 1;
                                    try cursorctl.goLeft(self.out, @as(u16, 1));
                                    try self.out.flush();
                                }
                            },
                            .right => {
                                if (self.pos < self.len) {
                                    self.pos += 1;
                                    try cursorctl.goRight(self.out, @as(u16, 1));
                                    try self.out.flush();
                                }
                            },
                            .up => try self.historyPrev(prompt),
                            .down => try self.historyNext(prompt),
                            .home => try self.moveToPos(0),
                            .end => try self.moveToPos(self.len),
                            else => {},
                        }
                    }
                },
                else => {},
            }
        }
    }

    fn insertChar(self: *LineEditor, ch: u21, prompt: []const u8) !void {
        if (ch > 0x7F or self.len >= repl_max_line_bytes) return;
        const byte: u8 = @intCast(ch);
        if (self.pos < self.len) {
            std.mem.copyBackwards(u8, self.buf[self.pos + 1 .. self.len + 1], self.buf[self.pos..self.len]);
        }
        self.buf[self.pos] = byte;
        self.len += 1;
        self.pos += 1;
        if (self.pos == self.len) {
            try self.out.writeByte(byte);
            try self.out.flush();
        } else {
            try self.refreshLine(prompt);
        }
    }

    fn deleteBack(self: *LineEditor, prompt: []const u8) !void {
        if (self.pos == 0) return;
        std.mem.copyForwards(u8, self.buf[self.pos - 1 .. self.len - 1], self.buf[self.pos..self.len]);
        self.pos -= 1;
        self.len -= 1;
        try self.refreshLine(prompt);
    }

    fn deleteForward(self: *LineEditor, prompt: []const u8) !void {
        if (self.pos >= self.len) return;
        std.mem.copyForwards(u8, self.buf[self.pos .. self.len - 1], self.buf[self.pos + 1 .. self.len]);
        self.len -= 1;
        try self.refreshLine(prompt);
    }

    fn moveToPos(self: *LineEditor, new_pos: usize) !void {
        if (new_pos == self.pos) return;
        if (new_pos < self.pos) {
            try cursorctl.goLeft(self.out, self.pos - new_pos);
        } else {
            try cursorctl.goRight(self.out, new_pos - self.pos);
        }
        self.pos = new_pos;
        try self.out.flush();
    }

    fn historyPrev(self: *LineEditor, prompt: []const u8) !void {
        if (self.history.items.len == 0) return;
        if (self.history_index == null) {
            @memcpy(self.saved_buf[0..self.len], self.buf[0..self.len]);
            self.saved_len = self.len;
            self.history_index = self.history.items.len - 1;
        } else if (self.history_index.? > 0) {
            self.history_index.? -= 1;
        } else {
            return;
        }
        try self.loadHistoryEntry(self.history.items[self.history_index.?], prompt);
    }

    fn historyNext(self: *LineEditor, prompt: []const u8) !void {
        if (self.history_index == null) return;
        if (self.history_index.? + 1 < self.history.items.len) {
            self.history_index.? += 1;
            try self.loadHistoryEntry(self.history.items[self.history_index.?], prompt);
        } else {
            self.history_index = null;
            try self.loadHistoryEntry(self.saved_buf[0..self.saved_len], prompt);
        }
    }

    fn loadHistoryEntry(self: *LineEditor, content: []const u8, prompt: []const u8) !void {
        const copy_len = @min(content.len, repl_max_line_bytes);
        @memcpy(self.buf[0..copy_len], content[0..copy_len]);
        self.len = copy_len;
        self.pos = copy_len;
        try self.refreshLine(prompt);
    }

    fn addHistory(self: *LineEditor, line: []const u8) !void {
        if (self.history.items.len > 0) {
            if (std.mem.eql(u8, self.history.items[self.history.items.len - 1], line)) return;
        }
        const copy = try self.allocator.dupe(u8, line);
        try self.history.append(self.allocator, copy);
    }

    fn refreshLine(self: *LineEditor, prompt: []const u8) !void {
        try self.out.writeAll("\r");
        try self.out.writeAll(prompt);
        try self.out.writeAll(self.buf[0..self.len]);
        try self.out.writeAll("\x1b[K");
        const tail = self.len - self.pos;
        if (tail > 0) try cursorctl.goLeft(self.out, tail);
        try self.out.flush();
    }
};

/// Runs the command prompt loop until EOF or a quit command is received.
/// When `editor` is non-null, uses interactive line editing via mibu;
/// otherwise falls back to plain line-oriented reading from `reader`.
fn runLoop(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
    ctx: anytype,
    editor: ?*LineEditor,
) !void {
    var running = true;
    while (running) {
        const line: ?[]const u8 = if (editor) |ed|
            try ed.editLine(repl_prompt)
        else blk: {
            try out.writeAll(repl_prompt);
            try out.flush();
            break :blk readLine(reader) catch |err| switch (err) {
                error.ReadFailed => return error.ReadFailed,
                error.StreamTooLong => {
                    try out.print(err_label ++ " input line exceeds {d} bytes\n", .{repl_max_line_bytes});
                    continue;
                },
            };
        };

        const input = line orelse {
            if (editor == null) try out.writeAll("\n");
            break;
        };

        const trimmed = std.mem.trim(u8, input, repl_whitespace);
        if (trimmed.len == 0) continue;

        const command = parseCommand(trimmed) catch |err| {
            try printParseError(out, err);
            continue;
        };

        running = executeCommand(allocator, ctx, command, reader, out) catch |err| {
            try out.print(err_label ++ " {any}\n", .{err});
            continue;
        };
    }
}

/// Executes one parsed command and returns whether the REPL should continue running.
fn executeCommand(
    allocator: std.mem.Allocator,
    ctx: anytype,
    command: Command,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !bool {
    switch (command) {
        .help => try printHelp(out, ctx.state()),
        .quit => return false,
        .list => _ = try printResourceList(allocator, ctx, out, false),
        .open => |addr| try handleOpen(allocator, ctx, addr, reader, out),
        .close => try handleClose(ctx, out),
        .write => |payload| try handleWrite(ctx, payload, out),
        .read => try handleRead(allocator, ctx, out),
        .query => |payload| try handleQuery(allocator, ctx, payload, out),
    }
    return true;
}

fn handleOpen(
    allocator: std.mem.Allocator,
    ctx: anytype,
    maybe_addr: ?[]const u8,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !void {
    if (ctx.state() == .connected) {
        try out.writeAll(err_label ++ " already connected; use 'close' first\n");
        return;
    }
    if (maybe_addr) |addr| {
        try ctx.open(allocator, addr);
        try out.print(color.print.fg(.green) ++ "Connected to {s}" ++ color.print.reset ++ "\n", .{addr});
        return;
    }
    // Interactive mode: scan and prompt for index.
    const count = try printResourceList(allocator, ctx, out, true);
    if (count == 0) return;
    try out.writeAll("Enter index: ");
    try out.flush();
    const idx_line = readLine(reader) catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.StreamTooLong => {
            try out.writeAll(err_label ++ " input too long\n");
            return;
        },
    } orelse return;
    const idx_trimmed = std.mem.trim(u8, idx_line, repl_whitespace);
    const index = std.fmt.parseInt(usize, idx_trimmed, 10) catch {
        try out.writeAll(err_label ++ " invalid index\n");
        return;
    };
    if (index == 0 or index > count) {
        try out.writeAll(err_label ++ " index out of range\n");
        return;
    }
    // Re-fetch to get the address at the selected index.
    var resources = try ctx.listResources(allocator);
    defer resources.deinit();
    if (index > resources.items.len) {
        try out.writeAll(err_label ++ " instrument list changed; try again\n");
        return;
    }
    const addr = resources.items[index - 1];
    try ctx.open(allocator, addr);
    try out.print(color.print.fg(.green) ++ "Connected to {s}" ++ color.print.reset ++ "\n", .{addr});
}

fn handleClose(ctx: anytype, out: *std.Io.Writer) !void {
    if (ctx.state() == .disconnected) {
        try out.writeAll(err_label ++ " not connected; use 'open <resource>' to connect first\n");
        return;
    }
    ctx.close();
    try out.writeAll("Disconnected.\n");
}

const not_connected_msg = err_label ++ " not connected; use 'list' to discover instruments, then 'open <resource>' to connect\n";

fn handleWrite(ctx: anytype, payload: []const u8, out: *std.Io.Writer) !void {
    if (ctx.state() == .disconnected) {
        try out.writeAll(not_connected_msg);
        return;
    }
    try ctx.write(payload);
    try out.writeAll("ok\n");
}

fn handleRead(allocator: std.mem.Allocator, ctx: anytype, out: *std.Io.Writer) !void {
    if (ctx.state() == .disconnected) {
        try out.writeAll(not_connected_msg);
        return;
    }
    const response = try ctx.readToOwned(allocator);
    defer allocator.free(response);
    try printResponse(out, response);
}

fn handleQuery(allocator: std.mem.Allocator, ctx: anytype, payload: []const u8, out: *std.Io.Writer) !void {
    if (ctx.state() == .disconnected) {
        try out.writeAll(not_connected_msg);
        return;
    }
    const response = try ctx.queryToOwned(allocator, payload);
    defer allocator.free(response);
    try printResponse(out, response);
}

/// Scans for VISA resources and prints them. When `numbered` is true,
/// each entry is prefixed with a 1-based index. Returns the count.
fn printResourceList(allocator: std.mem.Allocator, ctx: anytype, out: *std.Io.Writer, numbered: bool) !usize {
    var resources = try ctx.listResources(allocator);
    defer resources.deinit();
    if (resources.items.len == 0) {
        try out.writeAll("No instruments found.\n");
        return 0;
    }
    for (resources.items, 1..) |resource, i| {
        if (numbered) {
            try out.print("  {d}) {s}\n", .{ i, resource });
        } else {
            try out.print("  {s}\n", .{resource});
        }
    }
    return resources.items.len;
}

/// Reads one newline-delimited command line from the REPL input stream.
fn readLine(reader: *std.Io.Reader) error{ ReadFailed, StreamTooLong }!?[]const u8 {
    return reader.takeDelimiter('\n') catch |err| switch (err) {
        error.ReadFailed => error.ReadFailed,
        error.StreamTooLong => {
            _ = reader.discardDelimiterInclusive('\n') catch |discard_err| switch (discard_err) {
                error.EndOfStream => {},
                error.ReadFailed => return error.ReadFailed,
            };
            return error.StreamTooLong;
        },
    };
}

/// Parses a trimmed input line into one of the supported REPL commands.
fn parseCommand(line: []const u8) ParseError!Command {
    if (std.mem.eql(u8, line, "help") or std.mem.eql(u8, line, "?")) return .help;
    if (std.mem.eql(u8, line, "quit")) return .quit;
    if (std.mem.eql(u8, line, "list")) return .list;
    if (std.mem.eql(u8, line, "close")) return .close;

    const verb_end = std.mem.indexOfAny(u8, line, repl_whitespace) orelse line.len;
    const verb = line[0..verb_end];
    const payload = std.mem.trimLeft(u8, line[verb_end..], repl_whitespace);

    if (std.mem.eql(u8, verb, "read")) {
        if (payload.len != 0) return error.UnexpectedCommandPayload;
        return .read;
    }
    if (std.mem.eql(u8, verb, "write")) {
        if (payload.len == 0) return error.MissingCommandPayload;
        return .{ .write = payload };
    }
    if (std.mem.eql(u8, verb, "query")) {
        if (payload.len == 0) return error.MissingCommandPayload;
        return .{ .query = payload };
    }
    if (std.mem.eql(u8, verb, "open")) {
        if (payload.len == 0) return .{ .open = null };
        return .{ .open = payload };
    }
    return error.UnknownCommand;
}

/// Prints the list of supported REPL commands based on connection state.
fn printHelp(out: *std.Io.Writer, current_state: State) !void {
    switch (current_state) {
        .disconnected => try out.writeAll(
            \\Commands:
            \\  list              List available VISA instruments.
            \\  open [<resource>]  Connect by address, or scan and pick interactively.
            \\  help              Show this help text.
            \\  quit              Leave the REPL.
            \\
        ),
        .connected => try out.writeAll(
            \\Commands:
            \\  write <command>   Send a command to the instrument.
            \\  read              Read a response from the instrument.
            \\  query <command>   Send a command and then read the response.
            \\  list              List available VISA instruments.
            \\  close             Disconnect from the current instrument.
            \\  help              Show this help text.
            \\  quit              Leave the REPL.
            \\
        ),
    }
}

/// Prints a friendly parse error followed by a usage hint.
fn printParseError(out: *std.Io.Writer, err: ParseError) !void {
    const message = switch (err) {
        error.UnknownCommand => "unknown command",
        error.MissingCommandPayload => "missing command payload",
        error.UnexpectedCommandPayload => "read does not accept a payload",
    };
    try out.print(err_label ++ " {s}; type 'help' for usage\n", .{message});
}

/// Writes a response to the terminal, preserving existing trailing newlines when present.
fn printResponse(out: *std.Io.Writer, response: []const u8) !void {
    if (response.len == 0) {
        try out.writeAll("(empty)\n");
        return;
    }

    try out.writeAll(response);
    if (response[response.len - 1] != '\n') {
        try out.writeAll("\n");
    }
}

/// Mock resource list for testing.
const MockResourceList = struct {
    items: []const []const u8,
    fn deinit(_: *MockResourceList) void {}
};

/// Test double used to exercise the REPL loop without a real VISA session.
const MockContext = struct {
    allocator: std.mem.Allocator,
    writes: std.ArrayList([]u8),
    responses: []const []const u8,
    read_index: usize = 0,
    mock_resources: []const []const u8 = &.{},
    connected: bool = false,

    fn init(allocator: std.mem.Allocator, responses: []const []const u8) MockContext {
        return .{
            .allocator = allocator,
            .writes = .empty,
            .responses = responses,
        };
    }

    fn deinit(self: *MockContext) void {
        for (self.writes.items) |item| self.allocator.free(item);
        self.writes.deinit(self.allocator);
    }

    fn state(self: *const MockContext) State {
        return if (self.connected) .connected else .disconnected;
    }

    fn listResources(self: *MockContext, _: std.mem.Allocator) !MockResourceList {
        return .{ .items = self.mock_resources };
    }

    fn open(self: *MockContext, _: std.mem.Allocator, _: []const u8) !void {
        self.connected = true;
    }

    fn close(self: *MockContext) void {
        self.connected = false;
    }

    fn write(self: *MockContext, payload: []const u8) !void {
        const copy = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(copy);
        try self.writes.append(self.allocator, copy);
    }

    fn readToOwned(self: *MockContext, allocator: std.mem.Allocator) ![]u8 {
        if (self.read_index >= self.responses.len) return error.EndOfStream;
        const response = self.responses[self.read_index];
        self.read_index += 1;
        return try allocator.dupe(u8, response);
    }

    fn queryToOwned(self: *MockContext, allocator: std.mem.Allocator, payload: []const u8) ![]u8 {
        try self.write(payload);
        return self.readToOwned(allocator);
    }
};

test "parse repl commands" {
    const write_cmd = try parseCommand("write MEAS:VOLT?");
    try std.testing.expect(switch (write_cmd) {
        .write => |payload| std.mem.eql(u8, payload, "MEAS:VOLT?"),
        else => false,
    });

    const query_cmd = try parseCommand("query *IDN?");
    try std.testing.expect(switch (query_cmd) {
        .query => |payload| std.mem.eql(u8, payload, "*IDN?"),
        else => false,
    });

    const open_cmd = try parseCommand("open USB0::0x0957::INSTR");
    try std.testing.expect(switch (open_cmd) {
        .open => |addr| if (addr) |a| std.mem.eql(u8, a, "USB0::0x0957::INSTR") else false,
        else => false,
    });

    const open_bare = try parseCommand("open");
    try std.testing.expect(switch (open_bare) {
        .open => |addr| addr == null,
        else => false,
    });

    try std.testing.expectEqual(Command.read, try parseCommand("read"));
    try std.testing.expectEqual(Command.help, try parseCommand("help"));
    try std.testing.expectEqual(Command.quit, try parseCommand("quit"));
    try std.testing.expectEqual(Command.list, try parseCommand("list"));
    try std.testing.expectEqual(Command.close, try parseCommand("close"));
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("write"));
    try std.testing.expectError(error.UnexpectedCommandPayload, parseCommand("read extra"));
    try std.testing.expectError(error.UnknownCommand, parseCommand("ping"));
}

test "repl loop handles open write query read close and quit" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR
        \\write CONF:VOLT 10
        \\query *IDN?
        \\read
        \\close
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{ "TEST,MODEL,123\n", "5.000\n" });
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    try std.testing.expectEqual(@as(usize, 2), ctx.writes.items.len);
    try std.testing.expectEqualStrings("CONF:VOLT 10", ctx.writes.items[0]);
    try std.testing.expectEqualStrings("*IDN?", ctx.writes.items[1]);
    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Connected to USB0::INSTR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "ok\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "TEST,MODEL,123\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "5.000\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Disconnected.\n"));
}

test "repl list command shows resources" {
    const gpa = std.testing.allocator;
    const input =
        \\list
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    ctx.mock_resources = &.{ "USB0::0x0957::INSTR", "TCPIP0::192.168.1.1::INSTR" };
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "  USB0::0x0957::INSTR\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "  TCPIP0::192.168.1.1::INSTR\n"));
    // list should NOT show numbers
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "1)"));
}

test "repl interactive open scans and prompts" {
    const gpa = std.testing.allocator;
    // "open" without args triggers scan, then "2\n" selects the second instrument.
    const input = "open\n2\nclose\nquit\n";

    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    ctx.mock_resources = &.{ "USB0::0x0957::INSTR", "TCPIP0::192.168.1.1::INSTR" };
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "1) USB0::0x0957::INSTR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "2) TCPIP0::192.168.1.1::INSTR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Enter index:"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Connected to TCPIP0::192.168.1.1::INSTR"));
}

test "repl interactive open with invalid index" {
    const gpa = std.testing.allocator;
    const input = "open\n5\nquit\n";

    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    ctx.mock_resources = &.{"USB0::INSTR"};
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "index out of range"));
}

test "repl rejects instrument commands when disconnected" {
    const gpa = std.testing.allocator;
    const input =
        \\write CONF:VOLT 10
        \\read
        \\query *IDN?
        \\close
        \\quit
        \\
    ;

    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 3, "not connected"));
}
