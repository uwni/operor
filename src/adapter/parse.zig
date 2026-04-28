const std = @import("std");
const Adapter = @import("Adapter.zig");
const diagnostic = @import("../diagnostic.zig");
const doc_parse = @import("../doc_parse.zig");
const schema = @import("schema.zig");
const instrument = @import("../instrument.zig");

const max_adapter_file_size: usize = 512 * 1024;

/// Raw serialized shape of a adapter document.
const AdapterDoc = struct {
    metadata: schema.AdapterMeta = .{},
    instrument: schema.InstrumentSpec = .{},
    commands: std.StringHashMap(CommandDoc),
};

/// Raw serialized shape of a single command entry.
const CommandDoc = struct {
    write: []const u8,
    read: ?[]const u8 = null,
    description: ?[]const u8 = null,
    args: ?std.StringHashMap(schema.ArgSpec) = null,
};

/// Parses a adapter document from an already-open directory.
pub fn parseAdapterInDir(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    file_name: []const u8,
    diagnostics: *diagnostic.Diagnostics,
    context: diagnostic.Context,
) !Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer adapter_arena.deinit();
    const alloc = adapter_arena.allocator();
    const document_alloc = diagnostics.arenaAllocator();

    const parsed = doc_parse.parseFileInDir(AdapterDoc, document_alloc, io, dir, file_name, max_adapter_file_size) catch |err|
        return failDocument(diagnostics, context, file_name, err);
    const path = try dir.realPathFileAlloc(io, file_name, alloc);

    return buildAdapter(&adapter_arena, parsed, path, diagnostics, context);
}

fn failDocument(diagnostics: *diagnostic.Diagnostics, context: diagnostic.Context, file_name: []const u8, err: anyerror) anyerror {
    const message: diagnostic.Message = switch (err) {
        error.FileNotFound => .adapter_not_found,
        error.SyntaxError => .syntax_error,
        error.UnsupportedFormat => .unsupported_format,
        error.WrongType => .wrong_type,
        else => return err,
    };
    return diagnostics.failDiagnostic(.{
        .severity = .fatal,
        .context = context,
        .source_kind = .adapter_document,
        .source = file_name,
        .span = .{ .start = 0, .end = file_name.len },
        .message = message,
    });
}

fn buildAdapter(
    adapter_arena: *std.heap.ArenaAllocator,
    parsed: AdapterDoc,
    path: []const u8,
    diagnostics: *diagnostic.Diagnostics,
    context: diagnostic.Context,
) !Adapter {
    const alloc = adapter_arena.allocator();

    var commands: std.StringHashMap(schema.Command) = .init(alloc);
    var it = parsed.commands.iterator();
    while (it.next()) |entry| {
        const cmd_doc = entry.value_ptr.*;
        var command_context = context;
        command_context.command_name = entry.key_ptr.*;
        var cmd: schema.Command = try .parse(alloc, cmd_doc.write, cmd_doc.read, cmd_doc.description, diagnostics, command_context);
        cmd.args = cmd_doc.args;
        try commands.put(entry.key_ptr.*, cmd);
    }

    const inst = parsed.instrument;
    const write_termination = inst.write_termination orelse "";

    return Adapter{
        .arena = adapter_arena.*,
        .path = path,
        .meta = parsed.metadata,
        .instrument = inst,
        .commands = commands,
        .write_termination = write_termination,
        .options = .{
            .timeout_ms = inst.timeout_ms,
            .read_termination = instrument.Termination.fromSlice(inst.read_termination orelse ""),
            .query_delay_ms = inst.query_delay_ms orelse 0,
            .chunk_size = inst.chunk_size orelse instrument.default_chunk_size,
        },
    };
}

test "parse adapter templates and placeholders" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage},(@{channels})"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expect(cmd.response == null);
    try std.testing.expectEqual(@as(usize, 5), cmd.template.len);

    switch (cmd.template[1]) {
        .placeholder => |name| try std.testing.expectEqualStrings("voltage", name),
        else => return error.TestUnexpectedResult,
    }
    switch (cmd.template[3]) {
        .placeholder => |name| try std.testing.expectEqualStrings("channels", name),
        else => return error.TestUnexpectedResult,
    }
}

test "parse adapter response encoding" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  measure_voltage:
        \\    write: "MEAS:VOLT?"
        \\    read: float
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("measure_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema.Encoding.float, cmd.response.?);
    try std.testing.expectEqual(@as(usize, 1), cmd.template.len);
}

test "parse adapter with write termination" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\metadata:
        \\  version: "1.0"
        \\  description: PSU over serial
        \\instrument:
        \\  write_termination: "\n"
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage}"
    );
    defer adapter.deinit();

    try std.testing.expectEqualStrings("\n", adapter.write_termination);
}

test "parse adapter without write termination defaults to none" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  measure:
        \\    write: "MEAS?"
        \\    read: float
    );
    defer adapter.deinit();

    try std.testing.expectEqualStrings("", adapter.write_termination);
}

test "parse adapter instrument options" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\metadata:
        \\  version: "1.0"
        \\  description: Scope over TCPIP
        \\instrument:
        \\  timeout_ms: 2500
        \\  read_termination: "\n"
        \\  write_termination: "\r\n"
        \\  query_delay_ms: 25
        \\  chunk_size: 4096
        \\commands:
        \\  idn:
        \\    write: "*IDN?"
        \\    read: string
    );
    defer adapter.deinit();

    try std.testing.expectEqual(@as(?u32, 2500), adapter.options.timeout_ms);
    try std.testing.expectEqualStrings("\n", adapter.options.read_termination.constSlice());
    try std.testing.expectEqualStrings("\r\n", adapter.write_termination);
    try std.testing.expectEqual(@as(u32, 25), adapter.options.query_delay_ms);
    try std.testing.expectEqual(@as(usize, 4096), adapter.options.chunk_size);
}

fn parseTestYaml(allocator: std.mem.Allocator, content: []const u8) !Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer adapter_arena.deinit();
    const alloc = adapter_arena.allocator();
    var diagnostics = diagnostic.Diagnostics.init(alloc, "<test>");
    defer diagnostics.deinit();

    const parsed = try doc_parse.parseByFormat(AdapterDoc, .yaml, alloc, content);

    return buildAdapter(&adapter_arena, parsed, try alloc.dupe(u8, "<test>"), &diagnostics, .{ .adapter_name = "<test>" });
}

test "parse args string short form" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage}"
        \\    args:
        \\      voltage: float
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_voltage") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;
    const spec = args.get("voltage") orelse return error.TestUnexpectedResult;
    switch (spec) {
        .string => |s| try std.testing.expectEqualStrings("float", s),
        .object => return error.TestUnexpectedResult,
    }
}

test "parse args object form" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  set_output:
        \\    write: "OUTP {enabled}"
        \\    args:
        \\      enabled:
        \\        type: bool
        \\        "true": "ON"
        \\        "false": "OFF"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_output") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;
    const spec = args.get("enabled") orelse return error.TestUnexpectedResult;
    switch (spec) {
        .object => |obj| {
            try std.testing.expectEqualStrings("bool", obj.type);
            try std.testing.expectEqualStrings("ON", obj.true.?);
            try std.testing.expectEqualStrings("OFF", obj.false.?);
        },
        .string => return error.TestUnexpectedResult,
    }
}

test "parse args object default form" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  select_channel:
        \\    write: "INST {channel}"
        \\    args:
        \\      channel:
        \\        type: string
        \\        default: "1"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("select_channel") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;
    const spec = args.get("channel") orelse return error.TestUnexpectedResult;
    switch (spec) {
        .object => |obj| switch (obj.default.?) {
            .scalar => |scalar| switch (scalar) {
                .string => |s| try std.testing.expectEqualStrings("1", s),
                else => return error.TestUnexpectedResult,
            },
            .list => return error.TestUnexpectedResult,
        },
        .string => return error.TestUnexpectedResult,
    }
}
