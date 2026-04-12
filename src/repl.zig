const std = @import("std");
const mibu = @import("mibu");
const color = mibu.color;
const style = mibu.style;
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
const State = enum { disconnected, connected, selected };

/// Arguments for the `open` command.
const Open = struct {
    addr: ?[]const u8,
    name: ?[]const u8,
};

/// Write or query command with optional target.
const TargetedPayload = struct {
    target: ?[]const u8,
    payload: []const u8,
};

/// Parsed REPL command variants.
const Command = union(enum) {
    help,
    quit,
    list,
    open: Open,
    close: ?[]const u8,
    select: ?[]const u8,
    read: ?[]const u8,
    write: TargetedPayload,
    query: TargetedPayload,
    set: TargetedPayload,
};

/// Configurable instrument settings exposed by the `set` command.
const Setting = enum {
    timeout_ms,
    read_termination,
    write_termination,
    query_delay_ms,
    chunk_size,

    const map = std.StaticStringMap(Setting).initComptime(.{
        .{ "timeout_ms", .timeout_ms },
        .{ "read_termination", .read_termination },
        .{ "write_termination", .write_termination },
        .{ "query_delay_ms", .query_delay_ms },
        .{ "chunk_size", .chunk_size },
    });

    fn parse(key: []const u8) ?Setting {
        return map.get(key);
    }
};

const Termination = visa.Termination;

/// Tagged value for a parsed `set` command, ready for application.
const SettingValue = union(Setting) {
    timeout_ms: u32,
    read_termination: Termination,
    write_termination: Termination,
    query_delay_ms: u32,
    chunk_size: usize,
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
        .allocator = allocator,
        .rm = &rm,
    };
    defer ctx.closeAll();

    if (resource_addr) |addr| {
        const name = try ctx.openConnection(addr, null);
        try out.print(color.print.fg(.green) ++ "Connected to {s} ({s})" ++ color.print.reset ++ "\n", .{ addr, name });
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

/// A named connection to a VISA instrument.
const NamedConnection = struct {
    name: []u8,
    addr: []u8,
    instrument: visa.Instrument,
    /// Write termination override. Null means no termination.
    write_termination: ?Termination = null,
};

/// Production REPL context wrapping a VISA resource manager and multiple instruments.
const ReplContext = struct {
    allocator: std.mem.Allocator,
    rm: *visa.ResourceManager,
    connections: std.ArrayList(NamedConnection) = .empty,
    selected: ?usize = null,
    next_id: usize = 1,

    fn state(self: *const ReplContext) State {
        if (self.selected != null) return .selected;
        if (self.connections.items.len > 0) return .connected;
        return .disconnected;
    }

    fn selectedName(self: *const ReplContext) ?[]const u8 {
        if (self.selected) |idx| return self.connections.items[idx].name;
        return null;
    }

    fn openConnection(self: *ReplContext, addr: []const u8, name: ?[]const u8) ![]const u8 {
        var inst: visa.Instrument = .init(self.rm.session, self.rm.vtable);
        try inst.open(self.allocator, addr, .{});
        errdefer inst.deinit();
        const addr_copy = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(addr_copy);
        const name_copy = if (name) |n|
            try self.allocator.dupe(u8, n)
        else blk: {
            const auto = try std.fmt.allocPrint(self.allocator, "d{d}", .{self.next_id});
            self.next_id += 1;
            break :blk auto;
        };
        errdefer self.allocator.free(name_copy);
        try self.connections.append(self.allocator, .{ .name = name_copy, .addr = addr_copy, .instrument = inst });
        if (self.selected == null) self.selected = self.connections.items.len - 1;
        return name_copy;
    }

    fn closeByName(self: *ReplContext, target: []const u8) bool {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) {
                if (self.selected) |sel| {
                    if (sel == i) {
                        self.selected = null;
                    } else if (sel > i) {
                        self.selected = sel - 1;
                    }
                }
                var removed = self.connections.orderedRemove(i);
                removed.instrument.deinit();
                self.allocator.free(removed.addr);
                self.allocator.free(removed.name);
                return true;
            }
        }
        return false;
    }

    fn closeAll(self: *ReplContext) void {
        for (self.connections.items) |*conn| {
            conn.instrument.deinit();
            self.allocator.free(conn.addr);
            self.allocator.free(conn.name);
        }
        self.connections.deinit(self.allocator);
        self.selected = null;
    }

    fn findByName(self: *const ReplContext, target: []const u8) ?usize {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) return i;
        }
        return null;
    }

    fn findByAddr(self: *const ReplContext, addr: []const u8) ?usize {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.addr, addr)) return i;
        }
        return null;
    }

    fn selectByName(self: *ReplContext, target: []const u8) bool {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) {
                self.selected = i;
                return true;
            }
        }
        return false;
    }

    fn listResources(self: *ReplContext, allocator: std.mem.Allocator) !visa.ResourceList {
        return self.rm.listResources(allocator);
    }

    fn writeAt(self: *ReplContext, idx: usize, payload: []const u8) !void {
        var conn = &self.connections.items[idx];
        if (conn.write_termination) |wt| {
            const suffix = wt.constSlice();
            const full = try self.allocator.alloc(u8, payload.len + suffix.len);
            defer self.allocator.free(full);
            @memcpy(full[0..payload.len], payload);
            @memcpy(full[payload.len..][0..suffix.len], suffix);
            return conn.instrument.write(full);
        }
        return conn.instrument.write(payload);
    }

    fn readAt(self: *ReplContext, allocator: std.mem.Allocator, idx: usize) ![]u8 {
        return self.connections.items[idx].instrument.readToOwned(allocator);
    }

    fn queryAt(self: *ReplContext, allocator: std.mem.Allocator, idx: usize, payload: []const u8) ![]u8 {
        try self.writeAt(idx, payload);
        self.connections.items[idx].instrument.waitQueryDelay();
        return self.readAt(allocator, idx);
    }

    fn applyOption(self: *ReplContext, _: std.mem.Allocator, idx: usize, sv: SettingValue) !void {
        var conn = &self.connections.items[idx];
        switch (sv) {
            .timeout_ms => |v| {
                conn.instrument.options.timeout_ms = v;
                try conn.instrument.applyOptions();
            },
            .read_termination => |v| {
                conn.instrument.options.read_termination = v;
                try conn.instrument.applyOptions();
            },
            .write_termination => |v| conn.write_termination = v,
            .query_delay_ms => |v| conn.instrument.options.query_delay_ms = v,
            .chunk_size => |v| conn.instrument.options.chunk_size = v,
        }
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
        var prompt_buf: [128]u8 = undefined;
        const prompt = buildPrompt(ctx, &prompt_buf);

        const line: ?[]const u8 = if (editor) |ed|
            try ed.editLine(prompt)
        else blk: {
            try out.writeAll(prompt);
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

fn buildPrompt(ctx: anytype, buf: []u8) []const u8 {
    if (ctx.selectedName()) |name| {
        return std.fmt.bufPrint(buf, "{s}repl[{s}]> {s}", .{
            color.print.fg(.aqua), name, color.print.reset,
        }) catch repl_prompt;
    }
    return repl_prompt;
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
        .list => try handleList(allocator, ctx, out),
        .open => |o| try handleOpen(allocator, ctx, o, reader, out),
        .close => |name| try handleClose(ctx, name, out),
        .select => |name| try handleSelect(ctx, name, out),
        .write => |w| try handleWrite(ctx, w.target, w.payload, out),
        .read => |target| try handleRead(allocator, ctx, target, out),
        .query => |q| try handleQuery(allocator, ctx, q.target, q.payload, out),
        .set => |s| try handleSet(allocator, ctx, s.target, s.payload, out),
    }
    return true;
}

fn handleOpen(
    allocator: std.mem.Allocator,
    ctx: anytype,
    args: Open,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !void {
    if (args.name) |n| {
        if (!isValidName(n)) {
            try out.print(err_label ++ " invalid name '{s}'; use [a-zA-Z][a-zA-Z0-9_]*, must not be a command name\n", .{n});
            return;
        }
        if (ctx.findByName(n) != null) {
            try out.print(err_label ++ " name '{s}' is already in use\n", .{n});
            return;
        }
    }
    if (args.addr) |addr| {
        if (ctx.findByAddr(addr) != null) {
            try out.print(err_label ++ " already connected to {s}\n", .{addr});
            return;
        }
        const name = try ctx.openConnection(addr, args.name);
        try out.print(color.print.fg(.green) ++ "Connected to {s} ({s})" ++ color.print.reset ++ "\n", .{ addr, name });
        return;
    }
    // Interactive mode: scan and prompt for index.
    const count = try printOpenCandidates(allocator, ctx, out);
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
    var resources = try ctx.listResources(allocator);
    defer resources.deinit();
    // Map the user index to the nth unconnected resource.
    var n: usize = 0;
    var addr: ?[]const u8 = null;
    for (resources.items) |resource| {
        if (ctx.findByAddr(resource) != null) continue;
        n += 1;
        if (n == index) {
            addr = resource;
            break;
        }
    }
    const target_addr = addr orelse {
        try out.writeAll(err_label ++ " instrument list changed; try again\n");
        return;
    };
    const name = try ctx.openConnection(target_addr, args.name);
    try out.print(color.print.fg(.green) ++ "Connected to {s} ({s})" ++ color.print.reset ++ "\n", .{ target_addr, name });
}

fn handleClose(ctx: anytype, target: ?[]const u8, out: *std.Io.Writer) !void {
    if (target) |name| {
        if (!ctx.closeByName(name)) {
            try out.print(err_label ++ " unknown connection '{s}'\n", .{name});
            return;
        }
        try out.print("Disconnected from {s}.\n", .{name});
        return;
    }
    // Close selected — save name before freeing.
    const sel = ctx.selectedName() orelse {
        try out.writeAll(err_label ++ " no instrument selected; use 'close <name>'\n");
        return;
    };
    var name_buf: [64]u8 = undefined;
    const len = @min(sel.len, name_buf.len);
    @memcpy(name_buf[0..len], sel[0..len]);
    _ = ctx.closeByName(name_buf[0..len]);
    try out.print("Disconnected from {s}.\n", .{name_buf[0..len]});
}

fn handleSelect(ctx: anytype, target: ?[]const u8, out: *std.Io.Writer) !void {
    const name = target orelse {
        ctx.selected = null;
        try out.writeAll("Deselected.\n");
        return;
    };
    if (!ctx.selectByName(name)) {
        try out.print(err_label ++ " unknown connection '{s}'\n", .{name});
        return;
    }
    try out.print("Selected {s}.\n", .{name});
}

const no_target_msg = err_label ++ " no instrument selected; use 'select <name>' or '<name>.command ...'\n";

fn resolveTarget(ctx: anytype, target: ?[]const u8, out: *std.Io.Writer) !?usize {
    if (target) |name| {
        return ctx.findByName(name) orelse {
            try out.print(err_label ++ " unknown connection '{s}'\n", .{name});
            return null;
        };
    }
    return ctx.selected orelse {
        try out.writeAll(no_target_msg);
        return null;
    };
}

fn handleWrite(ctx: anytype, target: ?[]const u8, payload: []const u8, out: *std.Io.Writer) !void {
    const idx = try resolveTarget(ctx, target, out) orelse return;
    try ctx.writeAt(idx, payload);
    try out.writeAll("ok\n");
}

fn handleRead(allocator: std.mem.Allocator, ctx: anytype, target: ?[]const u8, out: *std.Io.Writer) !void {
    const idx = try resolveTarget(ctx, target, out) orelse return;
    const response = try ctx.readAt(allocator, idx);
    defer allocator.free(response);
    try printResponse(out, response);
}

fn handleQuery(allocator: std.mem.Allocator, ctx: anytype, target: ?[]const u8, payload: []const u8, out: *std.Io.Writer) !void {
    const idx = try resolveTarget(ctx, target, out) orelse return;
    const response = try ctx.queryAt(allocator, idx, payload);
    defer allocator.free(response);
    try printResponse(out, response);
}

fn handleSet(allocator: std.mem.Allocator, ctx: anytype, target: ?[]const u8, payload: []const u8, out: *std.Io.Writer) !void {
    const idx = try resolveTarget(ctx, target, out) orelse return;
    const space = std.mem.indexOfAny(u8, payload, repl_whitespace) orelse {
        try out.writeAll(err_label ++ " usage: set <key> <value>\n");
        return;
    };
    const key = payload[0..space];
    const raw_value = std.mem.trimLeft(u8, payload[space..], repl_whitespace);
    if (raw_value.len == 0) {
        try out.writeAll(err_label ++ " usage: set <key> <value>\n");
        return;
    }
    const setting = Setting.parse(key) orelse {
        try out.print(err_label ++ " unknown setting '{s}'\n", .{key});
        return;
    };
    const sv: SettingValue = switch (setting) {
        .timeout_ms => .{ .timeout_ms = std.fmt.parseInt(u32, raw_value, 10) catch {
            try out.print(err_label ++ " invalid integer '{s}'\n", .{raw_value});
            return;
        } },
        .query_delay_ms => .{ .query_delay_ms = std.fmt.parseInt(u32, raw_value, 10) catch {
            try out.print(err_label ++ " invalid integer '{s}'\n", .{raw_value});
            return;
        } },
        .chunk_size => .{ .chunk_size = std.fmt.parseInt(usize, raw_value, 10) catch {
            try out.print(err_label ++ " invalid integer '{s}'\n", .{raw_value});
            return;
        } },
        .read_termination, .write_termination => blk: {
            const t = unescape(raw_value) catch |err| {
                const msg = switch (err) {
                    error.InvalidEscape => "invalid escape sequence",
                    error.Overflow => "termination too long (max 4 bytes)",
                };
                try out.print(err_label ++ " {s}: '{s}'\n", .{ msg, raw_value });
                return;
            };
            break :blk if (setting == .read_termination)
                .{ .read_termination = t }
            else
                .{ .write_termination = t };
        },
    };
    try ctx.applyOption(allocator, idx, sv);
    try out.writeAll("ok\n");
}

fn unescape(input: []const u8) error{ InvalidEscape, Overflow }!Termination {
    var out: Termination = .{};
    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '\\' and i + 1 < input.len) {
            out.append(switch (input[i + 1]) {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '\\' => '\\',
                else => return error.InvalidEscape,
            }) catch return error.Overflow;
            i += 2;
        } else {
            out.append(input[i]) catch return error.Overflow;
            i += 1;
        }
    }
    return out;
}

/// Handles the `list` command: shows all resources without numbering.
/// Connected resources display their name; selected one is highlighted.
fn handleList(allocator: std.mem.Allocator, ctx: anytype, out: *std.Io.Writer) !void {
    var resources = try ctx.listResources(allocator);
    defer resources.deinit();
    if (resources.items.len == 0) {
        try out.writeAll("No instruments found.\n");
        return;
    }
    for (resources.items) |resource| {
        if (ctx.findByAddr(resource)) |ci| {
            const conn = ctx.connections.items[ci];
            const is_selected = if (ctx.selected) |sel| sel == ci else false;
            if (is_selected) {
                try out.print("  " ++ style.print.bold ++ color.print.fg(.green) ++ "{s}) {s}" ++ color.print.reset ++ "\n", .{ conn.name, resource });
            } else {
                try out.print("  " ++ color.print.fg(.yellow) ++ "{s}) {s}" ++ color.print.reset ++ "\n", .{ conn.name, resource });
            }
        } else {
            try out.print("  {s}\n", .{resource});
        }
    }
}

/// Scans for VISA resources and prints only unconnected ones, numbered for selection.
/// Returns the count of displayed (unconnected) resources.
fn printOpenCandidates(allocator: std.mem.Allocator, ctx: anytype, out: *std.Io.Writer) !usize {
    var resources = try ctx.listResources(allocator);
    defer resources.deinit();
    if (resources.items.len == 0) {
        try out.writeAll("No instruments found.\n");
        return 0;
    }
    var count: usize = 0;
    for (resources.items) |resource| {
        if (ctx.findByAddr(resource) != null) continue;
        count += 1;
        try out.print("  {d}) {s}\n", .{ count, resource });
    }
    if (count == 0) {
        try out.writeAll("All instruments already connected.\n");
    }
    return count;
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

    const verb_end = std.mem.indexOfAny(u8, line, repl_whitespace) orelse line.len;
    const first_token = line[0..verb_end];
    const rest = std.mem.trimLeft(u8, line[verb_end..], repl_whitespace);

    // Check for name.verb pattern.
    if (std.mem.indexOfScalar(u8, first_token, '.')) |dot| {
        const name = first_token[0..dot];
        const verb = first_token[dot + 1 ..];
        if (name.len == 0 or verb.len == 0) return error.UnknownCommand;
        if (std.mem.eql(u8, verb, "read")) {
            if (rest.len != 0) return error.UnexpectedCommandPayload;
            return .{ .read = name };
        }
        if (std.mem.eql(u8, verb, "write")) {
            if (rest.len == 0) return error.MissingCommandPayload;
            return .{ .write = .{ .target = name, .payload = rest } };
        }
        if (std.mem.eql(u8, verb, "query")) {
            if (rest.len == 0) return error.MissingCommandPayload;
            return .{ .query = .{ .target = name, .payload = rest } };
        }
        if (std.mem.eql(u8, verb, "set")) {
            if (rest.len == 0) return error.MissingCommandPayload;
            return .{ .set = .{ .target = name, .payload = rest } };
        }
        return error.UnknownCommand;
    }

    if (std.mem.eql(u8, first_token, "close")) {
        return .{ .close = if (rest.len > 0) rest else null };
    }
    if (std.mem.eql(u8, first_token, "select")) {
        return .{ .select = if (rest.len > 0) rest else null };
    }
    if (std.mem.eql(u8, first_token, "read")) {
        if (rest.len != 0) return error.UnexpectedCommandPayload;
        return .{ .read = null };
    }
    if (std.mem.eql(u8, first_token, "write")) {
        if (rest.len == 0) return error.MissingCommandPayload;
        return .{ .write = .{ .target = null, .payload = rest } };
    }
    if (std.mem.eql(u8, first_token, "query")) {
        if (rest.len == 0) return error.MissingCommandPayload;
        return .{ .query = .{ .target = null, .payload = rest } };
    }
    if (std.mem.eql(u8, first_token, "set")) {
        if (rest.len == 0) return error.MissingCommandPayload;
        return .{ .set = .{ .target = null, .payload = rest } };
    }
    if (std.mem.eql(u8, first_token, "open")) {
        return parseOpenArgs(rest);
    }
    return error.UnknownCommand;
}

fn parseOpenArgs(args: []const u8) Command {
    if (args.len == 0) return .{ .open = .{ .addr = null, .name = null } };
    if (std.mem.indexOf(u8, args, " as ")) |as_pos| {
        const addr = args[0..as_pos];
        const name = std.mem.trimLeft(u8, args[as_pos + 4 ..], repl_whitespace);
        if (name.len == 0) return .{ .open = .{ .addr = if (addr.len > 0) addr else null, .name = null } };
        return .{ .open = .{ .addr = if (addr.len > 0) addr else null, .name = name } };
    }
    return .{ .open = .{ .addr = args, .name = null } };
}

const reserved_names = [_][]const u8{ "help", "quit", "list", "open", "close", "select", "read", "write", "query", "set" };

fn isValidName(name: []const u8) bool {
    if (name.len == 0) return false;
    if (!std.ascii.isAlphabetic(name[0]) and name[0] != '_') return false;
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_') return false;
    }
    for (reserved_names) |r| {
        if (std.mem.eql(u8, name, r)) return false;
    }
    return true;
}

/// Prints the list of supported REPL commands based on connection state.
fn printHelp(out: *std.Io.Writer, current_state: State) !void {
    switch (current_state) {
        .disconnected => try out.writeAll(
            \\Commands:
            \\  list               List current connections.
            \\  open [<addr>]      Connect by address, or scan interactively.
            \\                     Use 'open <addr> as <name>' to assign a name.
            \\  help               Show this help text.
            \\  quit               Leave the REPL.
            \\
        ),
        .connected => try out.writeAll(
            \\Commands:
            \\  <name>.write <cmd> Send a command to a named instrument.
            \\  <name>.read        Read a response from a named instrument.
            \\  <name>.query <cmd> Send a command and read the response.
            \\  <name>.set <k> <v> Set an instrument option (e.g. write_termination \n).
            \\  select <name>      Select an instrument for direct commands.
            \\  list               List current connections.
            \\  open [<addr>]      Connect to another instrument.
            \\  close <name>       Disconnect a named instrument.
            \\  help               Show this help text.
            \\  quit               Leave the REPL.
            \\
        ),
        .selected => try out.writeAll(
            \\Commands:
            \\  write <command>    Send a command to the selected instrument.
            \\  read               Read a response from the selected instrument.
            \\  query <command>    Send a command and read the response.
            \\  set <key> <value>  Set an instrument option (e.g. write_termination \n).
            \\  <name>.write <cmd> Send a command to a named instrument.
            \\  <name>.read        Read a response from a named instrument.
            \\  <name>.query <cmd> Send a command and read the response.
            \\  <name>.set <k> <v> Set an instrument option on a named instrument.
            \\  select [<name>]    Switch or deselect the active instrument.
            \\  list               List current connections.
            \\  open [<addr>]      Connect to another instrument.
            \\  close [<name>]     Disconnect (default: selected).
            \\  help               Show this help text.
            \\  quit               Leave the REPL.
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
    const MockConnection = struct { name: []u8, addr: []u8 };
    const RecordedWrite = struct { target: []u8, payload: []u8 };

    allocator: std.mem.Allocator,
    writes: std.ArrayList(RecordedWrite) = .empty,
    applied_settings: std.ArrayList(SettingValue) = .empty,
    responses: []const []const u8,
    read_index: usize = 0,
    mock_resources: []const []const u8 = &.{},
    connections: std.ArrayList(MockConnection) = .empty,
    selected: ?usize = null,
    next_id: usize = 1,

    fn init(allocator: std.mem.Allocator, responses: []const []const u8) MockContext {
        return .{
            .allocator = allocator,
            .responses = responses,
        };
    }

    fn deinit(self: *MockContext) void {
        for (self.writes.items) |item| {
            self.allocator.free(item.target);
            self.allocator.free(item.payload);
        }
        self.writes.deinit(self.allocator);
        self.applied_settings.deinit(self.allocator);
        for (self.connections.items) |conn| {
            self.allocator.free(conn.name);
            self.allocator.free(conn.addr);
        }
        self.connections.deinit(self.allocator);
    }

    fn state(self: *const MockContext) State {
        if (self.selected != null) return .selected;
        if (self.connections.items.len > 0) return .connected;
        return .disconnected;
    }

    fn selectedName(self: *const MockContext) ?[]const u8 {
        if (self.selected) |idx| return self.connections.items[idx].name;
        return null;
    }

    fn openConnection(self: *MockContext, addr: []const u8, name: ?[]const u8) ![]const u8 {
        const name_copy = if (name) |n|
            try self.allocator.dupe(u8, n)
        else blk: {
            const auto = try std.fmt.allocPrint(self.allocator, "d{d}", .{self.next_id});
            self.next_id += 1;
            break :blk auto;
        };
        errdefer self.allocator.free(name_copy);
        const addr_copy = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(addr_copy);
        try self.connections.append(self.allocator, .{ .name = name_copy, .addr = addr_copy });
        if (self.selected == null) self.selected = self.connections.items.len - 1;
        return name_copy;
    }

    fn closeByName(self: *MockContext, target: []const u8) bool {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) {
                if (self.selected) |sel| {
                    if (sel == i) {
                        self.selected = null;
                    } else if (sel > i) {
                        self.selected = sel - 1;
                    }
                }
                const removed = self.connections.orderedRemove(i);
                self.allocator.free(removed.addr);
                self.allocator.free(removed.name);
                return true;
            }
        }
        return false;
    }

    fn findByName(self: *const MockContext, target: []const u8) ?usize {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) return i;
        }
        return null;
    }

    fn findByAddr(self: *const MockContext, addr: []const u8) ?usize {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.addr, addr)) return i;
        }
        return null;
    }

    fn selectByName(self: *MockContext, target: []const u8) bool {
        for (self.connections.items, 0..) |conn, i| {
            if (std.mem.eql(u8, conn.name, target)) {
                self.selected = i;
                return true;
            }
        }
        return false;
    }

    fn listResources(self: *MockContext, _: std.mem.Allocator) !MockResourceList {
        return .{ .items = self.mock_resources };
    }

    fn writeAt(self: *MockContext, idx: usize, payload: []const u8) !void {
        const t = try self.allocator.dupe(u8, self.connections.items[idx].name);
        errdefer self.allocator.free(t);
        const p = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(p);
        try self.writes.append(self.allocator, .{ .target = t, .payload = p });
    }

    fn readAt(self: *MockContext, allocator: std.mem.Allocator, _: usize) ![]u8 {
        if (self.read_index >= self.responses.len) return error.EndOfStream;
        const response = self.responses[self.read_index];
        self.read_index += 1;
        return try allocator.dupe(u8, response);
    }

    fn queryAt(self: *MockContext, allocator: std.mem.Allocator, idx: usize, payload: []const u8) ![]u8 {
        try self.writeAt(idx, payload);
        return self.readAt(allocator, idx);
    }

    fn applyOption(self: *MockContext, _: std.mem.Allocator, _: usize, sv: SettingValue) !void {
        try self.applied_settings.append(self.allocator, sv);
    }
};

test "parse repl commands" {
    // Basic commands.
    try std.testing.expectEqual(Command.help, try parseCommand("help"));
    try std.testing.expectEqual(Command.quit, try parseCommand("quit"));
    try std.testing.expectEqual(Command.list, try parseCommand("list"));

    // write (no target).
    const write_cmd = try parseCommand("write MEAS:VOLT?");
    try std.testing.expect(switch (write_cmd) {
        .write => |w| w.target == null and std.mem.eql(u8, w.payload, "MEAS:VOLT?"),
        else => false,
    });

    // query (no target).
    const query_cmd = try parseCommand("query *IDN?");
    try std.testing.expect(switch (query_cmd) {
        .query => |q| q.target == null and std.mem.eql(u8, q.payload, "*IDN?"),
        else => false,
    });

    // read (no target).
    try std.testing.expect(switch (try parseCommand("read")) {
        .read => |t| t == null,
        else => false,
    });

    // open with address.
    const open_cmd = try parseCommand("open USB0::0x0957::INSTR");
    try std.testing.expect(switch (open_cmd) {
        .open => |o| if (o.addr) |a| std.mem.eql(u8, a, "USB0::0x0957::INSTR") and o.name == null else false,
        else => false,
    });

    // open bare.
    try std.testing.expect(switch (try parseCommand("open")) {
        .open => |o| o.addr == null and o.name == null,
        else => false,
    });

    // open with name.
    const open_named = try parseCommand("open USB0::INSTR as dmm");
    try std.testing.expect(switch (open_named) {
        .open => |o| blk: {
            const addr_ok = if (o.addr) |a| std.mem.eql(u8, a, "USB0::INSTR") else false;
            const name_ok = if (o.name) |n| std.mem.eql(u8, n, "dmm") else false;
            break :blk addr_ok and name_ok;
        },
        else => false,
    });

    // targeted write (name.write).
    const tw = try parseCommand("dmm.write VOLT 3.3");
    try std.testing.expect(switch (tw) {
        .write => |w| if (w.target) |t| std.mem.eql(u8, t, "dmm") and std.mem.eql(u8, w.payload, "VOLT 3.3") else false,
        else => false,
    });

    // targeted read (name.read).
    try std.testing.expect(switch (try parseCommand("dmm.read")) {
        .read => |t| if (t) |n| std.mem.eql(u8, n, "dmm") else false,
        else => false,
    });

    // targeted query (name.query).
    const tq = try parseCommand("psu.query *IDN?");
    try std.testing.expect(switch (tq) {
        .query => |q| if (q.target) |t| std.mem.eql(u8, t, "psu") and std.mem.eql(u8, q.payload, "*IDN?") else false,
        else => false,
    });

    // select with name.
    try std.testing.expect(switch (try parseCommand("select dmm")) {
        .select => |s| if (s) |n| std.mem.eql(u8, n, "dmm") else false,
        else => false,
    });

    // select bare (deselect).
    try std.testing.expect(switch (try parseCommand("select")) {
        .select => |s| s == null,
        else => false,
    });

    // close with name.
    try std.testing.expect(switch (try parseCommand("close dmm")) {
        .close => |c| if (c) |n| std.mem.eql(u8, n, "dmm") else false,
        else => false,
    });

    // close bare.
    try std.testing.expect(switch (try parseCommand("close")) {
        .close => |c| c == null,
        else => false,
    });

    // Errors.
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("write"));
    try std.testing.expectError(error.UnexpectedCommandPayload, parseCommand("read extra"));
    try std.testing.expectError(error.UnknownCommand, parseCommand("ping"));
    try std.testing.expectError(error.UnknownCommand, parseCommand("dmm.foo"));
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("dmm.write"));
}

test "repl auto-selects first connection" {
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
    try std.testing.expectEqualStrings("d1", ctx.writes.items[0].target);
    try std.testing.expectEqualStrings("CONF:VOLT 10", ctx.writes.items[0].payload);
    try std.testing.expectEqualStrings("d1", ctx.writes.items[1].target);
    try std.testing.expectEqualStrings("*IDN?", ctx.writes.items[1].payload);
    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Connected to USB0::INSTR (d1)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "ok\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "TEST,MODEL,123\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "5.000\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Disconnected from d1.\n"));
}

test "repl multi-connection with targeted commands" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\open TCPIP0::1.1::INSTR as psu
        \\dmm.query *IDN?
        \\psu.write VOLT 3.3
        \\close psu
        \\close dmm
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{"DMM,Model,123\n"});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    try std.testing.expectEqual(@as(usize, 2), ctx.writes.items.len);
    try std.testing.expectEqualStrings("dmm", ctx.writes.items[0].target);
    try std.testing.expectEqualStrings("*IDN?", ctx.writes.items[0].payload);
    try std.testing.expectEqualStrings("psu", ctx.writes.items[1].target);
    try std.testing.expectEqualStrings("VOLT 3.3", ctx.writes.items[1].payload);
}

test "repl select and deselect" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\open TCPIP0::INSTR as psu
        \\select psu
        \\write VOLT 5.0
        \\select
        \\write VOLT 1.0
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    // First write goes to psu (explicitly selected).
    try std.testing.expectEqual(@as(usize, 1), ctx.writes.items.len);
    try std.testing.expectEqualStrings("psu", ctx.writes.items[0].target);
    try std.testing.expectEqualStrings("VOLT 5.0", ctx.writes.items[0].payload);
    // Second write should fail (deselected, no target).
    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "no instrument selected"));
}

test "repl list shows resources with connection status" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\open TCPIP0::INSTR as psu
        \\list
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    ctx.mock_resources = &.{ "USB0::INSTR", "TCPIP0::INSTR", "GPIB0::22::INSTR" };
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    // list shows no numbers; connected resources show name, unconnected show bare address.
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "dmm) USB0::INSTR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "psu) TCPIP0::INSTR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "GPIB0::22::INSTR"));
    // Unconnected resource must NOT have a number prefix.
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "3) GPIB0::22::INSTR"));
}

test "repl interactive open scans and prompts" {
    const gpa = std.testing.allocator;
    const input = "open\n2\nclose d1\nquit\n";

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
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Connected to TCPIP0::192.168.1.1::INSTR (d1)"));
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

test "repl rejects duplicate address" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\open USB0::INSTR as dmm2
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
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "already connected to USB0::INSTR"));
}

test "repl rejects commands when disconnected" {
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
    // write, read, query → "no instrument selected"; close → "no instrument selected"
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 4, "no instrument selected"));
}

test "repl name validation" {
    try std.testing.expect(isValidName("dmm"));
    try std.testing.expect(isValidName("psu_1"));
    try std.testing.expect(isValidName("_hidden"));
    try std.testing.expect(!isValidName(""));
    try std.testing.expect(!isValidName("1abc"));
    try std.testing.expect(!isValidName("a b"));
    try std.testing.expect(!isValidName("open"));
    try std.testing.expect(!isValidName("close"));
    try std.testing.expect(!isValidName("select"));
    try std.testing.expect(!isValidName("set"));
}

test "parse set commands" {
    // set (no target).
    const set_cmd = try parseCommand("set timeout_ms 5000");
    try std.testing.expect(switch (set_cmd) {
        .set => |s| s.target == null and std.mem.eql(u8, s.payload, "timeout_ms 5000"),
        else => false,
    });

    // targeted set (name.set).
    const ts = try parseCommand("dmm.set write_termination \\n");
    try std.testing.expect(switch (ts) {
        .set => |s| if (s.target) |t| std.mem.eql(u8, t, "dmm") and std.mem.eql(u8, s.payload, "write_termination \\n") else false,
        else => false,
    });

    // set requires payload.
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("set"));
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("dmm.set"));
}

test "repl set applies options" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\set timeout_ms 5000
        \\set query_delay_ms 100
        \\set chunk_size 2048
        \\set write_termination \n
        \\set read_termination \r\n
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    // All five set commands should succeed.
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 5, "ok\n"));
    // Verify applied settings.
    try std.testing.expectEqual(@as(usize, 5), ctx.applied_settings.items.len);
    try std.testing.expectEqual(@as(u32, 5000), ctx.applied_settings.items[0].timeout_ms);
    try std.testing.expectEqual(@as(u32, 100), ctx.applied_settings.items[1].query_delay_ms);
    try std.testing.expectEqual(@as(usize, 2048), ctx.applied_settings.items[2].chunk_size);
    try std.testing.expectEqualStrings("\n", ctx.applied_settings.items[3].write_termination.constSlice());
    try std.testing.expectEqualStrings("\r\n", ctx.applied_settings.items[4].read_termination.constSlice());
}

test "repl targeted set with name.set" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\open TCPIP0::INSTR as psu
        \\select psu
        \\dmm.set timeout_ms 3000
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "ok\n"));
    try std.testing.expectEqual(@as(usize, 1), ctx.applied_settings.items.len);
    try std.testing.expectEqual(@as(u32, 3000), ctx.applied_settings.items[0].timeout_ms);
}

test "repl set rejects unknown key" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\set baud_rate 9600
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "unknown setting"));
    try std.testing.expectEqual(@as(usize, 0), ctx.applied_settings.items.len);
}

test "repl set rejects invalid integer" {
    const gpa = std.testing.allocator;
    const input =
        \\open USB0::INSTR as dmm
        \\set timeout_ms abc
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    var ctx: MockContext = .init(gpa, &.{});
    defer ctx.deinit();

    try runLoop(gpa, &reader, &out.writer, &ctx, null);

    const output = out.written();
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "invalid integer"));
    try std.testing.expectEqual(@as(usize, 0), ctx.applied_settings.items.len);
}

test "unescape termination literals" {
    const newline = try unescape("\\n");
    try std.testing.expectEqualStrings("\n", newline.constSlice());

    const crlf = try unescape("\\r\\n");
    try std.testing.expectEqualStrings("\r\n", crlf.constSlice());

    const backslash = try unescape("\\\\");
    try std.testing.expectEqualStrings("\\", backslash.constSlice());

    try std.testing.expectError(error.InvalidEscape, unescape("\\x"));
    try std.testing.expectError(error.Overflow, unescape("abcde"));
}
