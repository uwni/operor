const std = @import("std");
const builtin = @import("builtin");
const is_windows = builtin.os.tag == .windows;
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

pub const RawTerm = if (is_windows) WinRawTerm else PosixRawTerm;

const WinRawTerm = struct {
    original_mode: u32,
    handle: std.os.windows.HANDLE,

    pub fn disableRawMode(self: *WinRawTerm) !void {
        if (SetConsoleMode(self.handle, self.original_mode) == 0)
            return error.Unexpected;
    }
};

const PosixRawTerm = struct {
    original: posix.termios,
    handle: posix.fd_t,

    pub fn disableRawMode(self: *PosixRawTerm) !void {
        try posix.tcsetattr(self.handle, .FLUSH, self.original);
    }
};

pub fn enableRawMode(handle: if (is_windows) std.os.windows.HANDLE else posix.fd_t) !RawTerm {
    if (is_windows) {
        var mode: u32 = 0;
        if (GetConsoleMode(handle, &mode) == 0) return error.Unexpected;
        const raw_mode = mode & ~@as(u32, ENABLE_ECHO_INPUT | ENABLE_LINE_INPUT | ENABLE_PROCESSED_INPUT) | ENABLE_VIRTUAL_TERMINAL_INPUT;
        if (SetConsoleMode(handle, raw_mode) == 0) return error.Unexpected;
        return .{ .original_mode = mode, .handle = handle };
    } else {
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
}

// Windows console mode constants
const ENABLE_ECHO_INPUT: u32 = 0x0004;
const ENABLE_LINE_INPUT: u32 = 0x0002;
const ENABLE_PROCESSED_INPUT: u32 = 0x0001;
const ENABLE_VIRTUAL_TERMINAL_INPUT: u32 = 0x0200;

extern "kernel32" fn GetConsoleMode(hConsoleHandle: std.os.windows.HANDLE, lpMode: *u32) callconv(.winapi) i32;
extern "kernel32" fn SetConsoleMode(hConsoleHandle: std.os.windows.HANDLE, dwMode: u32) callconv(.winapi) i32;
extern "kernel32" fn ReadConsoleInputW(hConsoleInput: std.os.windows.HANDLE, lpBuffer: [*]INPUT_RECORD, nLength: u32, lpNumberOfEventsRead: *u32) callconv(.winapi) i32;
extern "kernel32" fn PeekConsoleInputW(hConsoleInput: std.os.windows.HANDLE, lpBuffer: [*]INPUT_RECORD, nLength: u32, lpNumberOfEventsRead: *u32) callconv(.winapi) i32;

const KEY_EVENT: u16 = 0x0001;

const KEY_EVENT_RECORD = extern struct {
    bKeyDown: i32,
    wRepeatCount: u16,
    wVirtualKeyCode: u16,
    wVirtualScanCode: u16,
    uChar: extern union { UnicodeChar: u16, AsciiChar: u8 },
    dwControlKeyState: u32,
};

const INPUT_RECORD = extern struct {
    EventType: u16,
    _padding: u16 = 0,
    Event: extern union { KeyEvent: KEY_EVENT_RECORD },
};

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
    if (is_windows) {
        return nextEventWindows(file);
    } else {
        return nextEventPosix(file);
    }
}

fn nextEventWindows(file: std.Io.File) !Event {
    while (true) {
        var record: [1]INPUT_RECORD = undefined;
        var count: u32 = 0;
        if (ReadConsoleInputW(file.handle, &record, 1, &count) == 0 or count == 0)
            return .none;
        if (record[0].EventType != KEY_EVENT) continue;
        const key_ev = record[0].Event.KeyEvent;
        if (key_ev.bKeyDown == 0) continue;

        const vk = key_ev.wVirtualKeyCode;
        const ctrl = (key_ev.dwControlKeyState & 0x000C) != 0; // LEFT_CTRL | RIGHT_CTRL
        const uc = key_ev.uChar.UnicodeChar;

        return switch (vk) {
            0x0D => Event{ .key = .{ .code = .enter } },
            0x1B => Event{ .key = .{ .code = .esc } },
            0x08 => Event{ .key = .{ .code = .backspace } },
            0x09 => Event{ .key = .{ .code = .tab } },
            0x26 => Event{ .key = .{ .code = .up } },
            0x28 => Event{ .key = .{ .code = .down } },
            0x25 => Event{ .key = .{ .code = .left } },
            0x27 => Event{ .key = .{ .code = .right } },
            0x24 => Event{ .key = .{ .code = .home } },
            0x23 => Event{ .key = .{ .code = .end } },
            0x2E => Event{ .key = .{ .code = .delete } },
            else => if (ctrl and uc >= 1 and uc <= 26)
                Event{ .key = .{ .mods = .{ .ctrl = true }, .code = .{ .char = uc + 'a' - 1 } } }
            else if (uc != 0)
                Event{ .key = .{ .code = .{ .char = uc } } }
            else
                continue,
        };
    }
}

fn nextEventPosix(file: std.Io.File) !Event {
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
            '1', '3', '4' => |c2_num| {
                const c3 = readByte(fd) orelse return .none;
                if (c3 != '~') return .none;
                return Event{ .key = .{ .code = switch (c2_num) {
                    '1' => .home,
                    '3' => .delete,
                    '4' => .end,
                    else => unreachable,
                } } };
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
