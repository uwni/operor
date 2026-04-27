const std = @import("std");

pub const whitespace = " \t\r\n";

/// REPL connection state.
pub const State = enum { disconnected, connected, selected };

/// Arguments for the `open` command.
pub const Open = struct {
    addr: ?[]const u8,
    name: ?[]const u8,
};

/// Write or query command with optional target.
pub const TargetedPayload = struct {
    target: ?[]const u8,
    payload: []const u8,
};

/// Parsed REPL command variants.
pub const Command = union(enum) {
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

/// User-facing parse errors for REPL input.
pub const ParseError = error{
    UnknownCommand,
    MissingCommandPayload,
    UnexpectedCommandPayload,
};

/// Parses a trimmed input line into one of the supported REPL commands.
pub fn parseCommand(line: []const u8) ParseError!Command {
    if (std.mem.eql(u8, line, "help") or std.mem.eql(u8, line, "?")) return .help;
    if (std.mem.eql(u8, line, "quit")) return .quit;
    if (std.mem.eql(u8, line, "list")) return .list;

    const verb_end = std.mem.indexOfAny(u8, line, whitespace) orelse line.len;
    const first_token = line[0..verb_end];
    const rest = std.mem.trimStart(u8, line[verb_end..], whitespace);

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
        const name = std.mem.trimStart(u8, args[as_pos + 4 ..], whitespace);
        if (name.len == 0) return .{ .open = .{ .addr = if (addr.len > 0) addr else null, .name = null } };
        return .{ .open = .{ .addr = if (addr.len > 0) addr else null, .name = name } };
    }
    return .{ .open = .{ .addr = args, .name = null } };
}

const reserved_names = [_][]const u8{ "help", "quit", "list", "open", "close", "select", "read", "write", "query", "set" };

pub fn isValidName(name: []const u8) bool {
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
