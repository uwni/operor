const std = @import("std");
const posix = std.posix;

// ── ANSI attributes ─────────────────────────────────────────────────────

pub const Attr = enum {
    // colors (256-color palette)
    black,
    red,
    green,
    yellow,
    fuchsia,
    aqua,
    white,
    // styles
    bold,

    fn code(comptime self: Attr) []const u8 {
        return switch (self) {
            .black => "\x1b[38;5;0m",
            .red => "\x1b[38;5;9m",
            .green => "\x1b[38;5;2m",
            .yellow => "\x1b[38;5;11m",
            .fuchsia => "\x1b[38;5;13m",
            .aqua => "\x1b[38;5;14m",
            .white => "\x1b[38;5;15m",
            .bold => "\x1b[1m",
        };
    }
};

/// Wrap comptime text with ANSI attributes and a reset suffix.
/// Example: `styledText("[OK]", .{.green})` or `styledText("{s}", .{.green, .bold})`
pub inline fn styledText(comptime text: []const u8, comptime attrs: anytype) []const u8 {
    comptime {
        var codes: []const u8 = "";
        for (@typeInfo(@TypeOf(attrs)).@"struct".fields) |f| {
            codes = codes ++ @as(Attr, @field(attrs, f.name)).code();
        }
        return codes ++ text ++ "\x1b[0m";
    }
}

// ── Cursor movement ─────────────────────────────────────────────────────

pub const cursor = struct {
    pub inline fn goLeft(writer: *std.Io.Writer, n: anytype) !void {
        try writer.print("\x1b[{d}D", .{n});
    }
    pub inline fn goRight(writer: *std.Io.Writer, n: anytype) !void {
        try writer.print("\x1b[{d}C", .{n});
    }
};

// ── Line / screen clearing ──────────────────────────────────────────────

pub const clear = struct {
    /// Erase from cursor to end of line.
    pub inline fn toEndOfLine(writer: *std.Io.Writer) !void {
        try writer.writeAll("\x1b[K");
    }
    /// Clear entire screen and move cursor to top-left.
    pub inline fn screen(writer: *std.Io.Writer) !void {
        try writer.writeAll("\x1b[2J\x1b[H");
    }
    /// Move cursor to beginning of current line (carriage return).
    pub inline fn lineStart(writer: *std.Io.Writer) !void {
        try writer.writeAll("\r");
    }
};

// ── Terminal raw mode ───────────────────────────────────────────────────

pub const RawTerm = struct {
    original: posix.termios,
    handle: posix.fd_t,

    pub fn disableRawMode(self: *RawTerm) !void {
        try posix.tcsetattr(self.handle, .FLUSH, self.original);
    }
};

pub fn enableRawMode(handle: posix.fd_t) !RawTerm {
    const original = try posix.tcgetattr(handle);

    var raw = original;

    raw.iflag.BRKINT = false;
    raw.iflag.ICRNL = false;
    raw.iflag.INPCK = false;
    raw.iflag.ISTRIP = false;
    raw.iflag.IXON = false;

    raw.oflag.OPOST = false;

    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    raw.lflag.IEXTEN = false;
    raw.lflag.ISIG = false;

    raw.cflag.CSIZE = .CS8;

    raw.cc[@intFromEnum(posix.V.MIN)] = 1;
    raw.cc[@intFromEnum(posix.V.TIME)] = 0;

    try posix.tcsetattr(handle, .FLUSH, raw);

    return .{ .original = original, .handle = handle };
}

// ── Keyboard events ─────────────────────────────────────────────────────

pub const Modifiers = struct {
    ctrl: bool = false,
};

pub const KeyCode = union(enum) {
    char: u21,
    enter,
    esc,
    backspace,
    tab,
    up,
    down,
    left,
    right,
    home,
    end,
    delete,
};

pub const Key = struct {
    mods: Modifiers = .{},
    code: KeyCode,
};

pub const Event = union(enum) {
    key: Key,
    none,
};

/// Read the next terminal input event from `file`.
pub fn nextEvent(file: std.Io.File) !Event {
    const c0 = readByte(file.handle) orelse return .none;

    switch (c0) {
        '\x1b' => return parseEscape(file.handle),
        '\r', '\n' => return Event{ .key = .{ .code = .enter } },
        '\t' => return Event{ .key = .{ .code = .tab } },
        127 => return Event{ .key = .{ .code = .backspace } },
        1...8, 11...12, 14...26 => |c| return Event{ .key = .{ .mods = .{ .ctrl = true }, .code = .{ .char = c + 'a' - 1 } } },
        else => |c0_byte| {
            const cp = decodeUtf8(file.handle, c0_byte) orelse return .none;
            return Event{ .key = .{ .code = .{ .char = cp } } };
        },
    }
}

/// Decode a UTF-8 codepoint starting with `first`. Reads continuation bytes from `fd`.
fn decodeUtf8(fd: posix.fd_t, first: u8) ?u21 {
    const len = std.unicode.utf8ByteSequenceLength(first) catch return null;
    if (len == 1) return first;

    var buf: [4]u8 = undefined;
    buf[0] = first;
    for (1..len) |i| {
        buf[i] = readByte(fd) orelse return null;
    }

    return std.unicode.utf8Decode(buf[0..len]) catch null;
}

fn readByte(fd: posix.fd_t) ?u8 {
    var buf: [1]u8 = undefined;
    const n = posix.read(fd, &buf) catch return null;
    if (n == 0) return null;
    return buf[0];
}

fn parseEscape(fd: posix.fd_t) Event {
    if (!hasData(fd)) return Event{ .key = .{ .code = .esc } };

    const c1 = readByte(fd) orelse return Event{ .key = .{ .code = .esc } };

    if (c1 == '[') {
        const c2 = readByte(fd) orelse return .none;
        switch (c2) {
            'A' => return Event{ .key = .{ .code = .up } },
            'B' => return Event{ .key = .{ .code = .down } },
            'C' => return Event{ .key = .{ .code = .right } },
            'D' => return Event{ .key = .{ .code = .left } },
            'H' => return Event{ .key = .{ .code = .home } },
            'F' => return Event{ .key = .{ .code = .end } },
            '3' => {
                const c3 = readByte(fd) orelse return .none;
                if (c3 == '~') return Event{ .key = .{ .code = .delete } };
                return .none;
            },
            '1' => {
                const c3 = readByte(fd) orelse return .none;
                if (c3 == '~') return Event{ .key = .{ .code = .home } };
                return .none;
            },
            '4' => {
                const c3 = readByte(fd) orelse return .none;
                if (c3 == '~') return Event{ .key = .{ .code = .end } };
                return .none;
            },
            else => return .none,
        }
    } else if (c1 == 'O') {
        const c2 = readByte(fd) orelse return .none;
        switch (c2) {
            'H' => return Event{ .key = .{ .code = .home } },
            'F' => return Event{ .key = .{ .code = .end } },
            else => return .none,
        }
    }
    return .none;
}

fn hasData(fd: posix.fd_t) bool {
    var pollfds = [_]posix.pollfd{.{
        .fd = fd,
        .events = posix.POLL.IN,
        .revents = 0,
    }};
    const n = posix.poll(&pollfds, 0) catch return false;
    return n > 0;
}
