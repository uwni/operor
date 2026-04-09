const std = @import("std");
const visa = @import("visa/root.zig");

const repl_prompt = "repl> ";
const repl_max_line_bytes: usize = 4096;
const repl_whitespace = " \t\r\n";

/// Recommended stdin buffer size for line-oriented REPL input.
pub const stdin_buffer_bytes: usize = repl_max_line_bytes + 1;

/// Parsed REPL command variants.
const Command = union(enum) {
    help,
    exit,
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

/// Opens a VISA instrument and starts the interactive REPL loop.
pub fn run(
    allocator: std.mem.Allocator,
    resource_addr: []const u8,
    visa_lib: ?[]const u8,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
) !void {
    const vtable = try visa.loader.load(visa_lib);
    var rm = try visa.ResourceManager.init(&vtable);
    defer rm.deinit();

    var instrument = visa.Instrument.init(rm.session, &vtable);
    try instrument.open(allocator, resource_addr, .{});
    defer instrument.deinit();

    try out.print("Connected to {s}\n", .{resource_addr});
    try printHelp(out);
    defer out.flush() catch {};
    try loop(allocator, reader, out, &instrument);
}

/// Runs the command prompt loop until EOF or an exit command is received.
fn loop(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    out: *std.Io.Writer,
    instrument: anytype,
) !void {
    var running = true;
    while (running) {
        try out.writeAll(repl_prompt);
        try out.flush();

        const line = readLine(reader) catch |err| switch (err) {
            error.ReadFailed => return error.ReadFailed,
            error.StreamTooLong => {
                try out.print("error: input line exceeds {d} bytes\n", .{repl_max_line_bytes});
                continue;
            },
        } orelse {
            try out.writeAll("\n");
            break;
        };

        const trimmed = std.mem.trim(u8, line, repl_whitespace);
        if (trimmed.len == 0) continue;

        const command = parseCommand(trimmed) catch |err| {
            try printParseError(out, err);
            continue;
        };

        running = executeCommand(allocator, instrument, command, out) catch |err| {
            try out.print("error: {any}\n", .{err});
            continue;
        };
    }
}

/// Executes one parsed command and returns whether the REPL should continue running.
fn executeCommand(
    allocator: std.mem.Allocator,
    instrument: anytype,
    command: Command,
    out: *std.Io.Writer,
) !bool {
    switch (command) {
        .help => {
            try printHelp(out);
            return true;
        },
        .exit => return false,
        .write => |payload| {
            try instrument.write(payload);
            try out.writeAll("ok\n");
            return true;
        },
        .read => {
            const response = try instrument.readToOwned(allocator);
            defer allocator.free(response);
            try printResponse(out, response);
            return true;
        },
        .query => |payload| {
            const response = try instrument.queryToOwned(allocator, payload);
            defer allocator.free(response);
            try printResponse(out, response);
            return true;
        },
    }
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
    if (std.mem.eql(u8, line, "exit") or std.mem.eql(u8, line, "quit")) return .exit;

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
    return error.UnknownCommand;
}

/// Prints the list of supported REPL commands.
fn printHelp(out: *std.Io.Writer) !void {
    try out.writeAll(
        \\Commands:
        \\  write <command>  Send a command to the instrument.
        \\  read             Read a response from the instrument.
        \\  query <command>  Send a command and then read the response.
        \\  help             Show this help text.
        \\  exit | quit      Leave the REPL.
        \\
    );
}

/// Prints a friendly parse error followed by a usage hint.
fn printParseError(out: *std.Io.Writer, err: ParseError) !void {
    const message = switch (err) {
        error.UnknownCommand => "unknown command",
        error.MissingCommandPayload => "missing command payload",
        error.UnexpectedCommandPayload => "read does not accept a payload",
    };
    try out.print("error: {s}; type 'help' for usage\n", .{message});
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

/// Test double used to exercise the REPL loop without a real VISA session.
const MockInstrument = struct {
    allocator: std.mem.Allocator,
    writes: std.ArrayList([]u8),
    responses: []const []const u8,
    read_index: usize = 0,

    /// Creates a mock instrument with a predefined response sequence.
    fn init(allocator: std.mem.Allocator, responses: []const []const u8) MockInstrument {
        return .{
            .allocator = allocator,
            .writes = .empty,
            .responses = responses,
        };
    }

    /// Releases stored write payloads captured by the mock.
    fn deinit(self: *MockInstrument) void {
        for (self.writes.items) |item| self.allocator.free(item);
        self.writes.deinit(self.allocator);
    }

    /// Records a write payload issued by the REPL.
    fn write(self: *MockInstrument, payload: []const u8) !void {
        const copy = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(copy);
        try self.writes.append(self.allocator, copy);
    }

    /// Returns the next predefined response as an owned buffer.
    fn readToOwned(self: *MockInstrument, allocator: std.mem.Allocator) ![]u8 {
        if (self.read_index >= self.responses.len) return error.EndOfStream;
        const response = self.responses[self.read_index];
        self.read_index += 1;
        return try allocator.dupe(u8, response);
    }

    fn queryToOwned(self: *MockInstrument, allocator: std.mem.Allocator, payload: []const u8) ![]u8 {
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

    try std.testing.expectEqual(Command.read, try parseCommand("read"));
    try std.testing.expectEqual(Command.help, try parseCommand("help"));
    try std.testing.expectEqual(Command.exit, try parseCommand("quit"));
    try std.testing.expectError(error.MissingCommandPayload, parseCommand("write"));
    try std.testing.expectError(error.UnexpectedCommandPayload, parseCommand("read extra"));
    try std.testing.expectError(error.UnknownCommand, parseCommand("ping"));
}

test "repl loop handles write query read and quit" {
    const gpa = std.testing.allocator;
    const input =
        \\write CONF:VOLT 10
        \\query *IDN?
        \\read
        \\quit
        \\
    ;

    var reader = std.Io.Reader.fixed(input);
    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();

    var instrument = MockInstrument.init(gpa, &.{ "TEST,MODEL,123\n", "5.000\n" });
    defer instrument.deinit();

    try loop(gpa, &reader, &out.writer, &instrument);

    try std.testing.expectEqual(@as(usize, 2), instrument.writes.items.len);
    try std.testing.expectEqualStrings("CONF:VOLT 10", instrument.writes.items[0]);
    try std.testing.expectEqualStrings("*IDN?", instrument.writes.items[1]);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "ok\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "TEST,MODEL,123\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "5.000\n"));
}
