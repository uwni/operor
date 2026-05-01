const std = @import("std");
const Adapter = @import("Adapter.zig");
const diagnostic = @import("../diagnostic.zig");
const doc_parse = @import("../doc_parse.zig");
const schema = @import("schema.zig");
const instrument = @import("../instrument.zig");
const testing = @import("../testing.zig");

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
    read: ?schema.ReadSpec = null,
    description: ?[]const u8 = null,
    args: ?std.StringHashMap(schema.ArgSpec) = null,
};

/// Parses a adapter document from an already-open directory.
pub fn parseAdapterInDir(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    file_name: []const u8,
    reporter: diagnostic.Reporter,
) anyerror!Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer |err| if (err != error.AnalysisFail) adapter_arena.deinit();
    const alloc = adapter_arena.allocator();

    const parsed = doc_parse.parseFileInDir(AdapterDoc, alloc, io, dir, file_name, max_adapter_file_size) catch |err| {
        const message = documentMessage(err) orelse return @as(anyerror!Adapter, err);
        try failDocument(reporter, file_name, message);
        return error.AnalysisFail;
    };
    const path = try dir.realPathFileAlloc(io, file_name, alloc);

    return buildAdapter(&adapter_arena, parsed, path, reporter);
}

fn documentMessage(err: anyerror) ?diagnostic.Message {
    return switch (err) {
        error.FileNotFound => .adapter_not_found,
        error.SyntaxError => .syntax_error,
        error.UnsupportedFormat => .unsupported_format,
        error.WrongType => .wrong_type,
        else => null,
    };
}

fn failDocument(reporter: diagnostic.Reporter, file_name: []const u8, message: diagnostic.Message) error{OutOfMemory}!void {
    var context = reporter.context;
    context.adapter_name = file_name;
    try reporter
        .withSourceKind(.adapter_document)
        .withContext(context)
        .add(.fatal, null, message);
}

fn buildAdapter(
    adapter_arena: *std.heap.ArenaAllocator,
    parsed: AdapterDoc,
    path: []const u8,
    reporter: diagnostic.Reporter,
) !Adapter {
    const alloc = adapter_arena.allocator();

    var commands: std.StringHashMap(schema.Command) = .init(alloc);
    var it = parsed.commands.iterator();
    while (it.next()) |entry| {
        const cmd_doc = entry.value_ptr.*;
        var command_context = reporter.context;
        command_context.command_name = entry.key_ptr.*;
        var cmd: schema.Command = try .parse(alloc, cmd_doc.write, cmd_doc.read, cmd_doc.description, reporter.withContext(command_context));
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
        \\    write: "VOLT {voltage:float},(@{channels:list})"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expect(cmd.response == null);
    try std.testing.expectEqual(@as(usize, 5), cmd.template.len);

    switch (cmd.template[1]) {
        .placeholder => |placeholder| {
            try std.testing.expectEqualStrings("voltage", placeholder.name);
            try std.testing.expectEqualStrings("float", placeholder.arg_type);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (cmd.template[3]) {
        .placeholder => |placeholder| {
            try std.testing.expectEqualStrings("channels", placeholder.name);
            try std.testing.expectEqualStrings("list", placeholder.arg_type);
        },
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
    switch (cmd.response.?) {
        .scalar => |encoding| try std.testing.expectEqual(schema.Encoding.float, encoding),
        .list => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), cmd.template.len);
}

test "parse adapter list response encoding" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  measure_all:
        \\    write: "MEAS:ALL?"
        \\    read: [float, float]
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("measure_all") orelse return error.TestUnexpectedResult;
    switch (cmd.response.?) {
        .scalar => return error.TestUnexpectedResult,
        .list => |list| {
            try std.testing.expectEqualStrings(",", list.separator);
            try std.testing.expectEqual(@as(usize, 2), list.items.len);
            try std.testing.expectEqual(schema.Encoding.float, list.items[0]);
            try std.testing.expectEqual(schema.Encoding.float, list.items[1]);
        },
    }
}

test "parse adapter list response with custom separator" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  read_pair:
        \\    write: "PAIR?"
        \\    read:
        \\      split: ";"
        \\      items: [int, string]
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("read_pair") orelse return error.TestUnexpectedResult;
    switch (cmd.response.?) {
        .scalar => return error.TestUnexpectedResult,
        .list => |list| {
            try std.testing.expectEqualStrings(";", list.separator);
            try std.testing.expectEqual(@as(usize, 2), list.items.len);
            try std.testing.expectEqual(schema.Encoding.int, list.items[0]);
            try std.testing.expectEqual(schema.Encoding.string, list.items[1]);
        },
    }
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
        \\    write: "VOLT {voltage:float}"
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

    return buildAdapter(&adapter_arena, parsed, try alloc.dupe(u8, "<test>"), diagnostics.reporter().withContext(.{ .adapter_name = "<test>" }));
}

test "parse adapter result owns document data independent of diagnostics" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/psu.yaml",
        \\metadata:
        \\  description: owned adapter
        \\instrument:
        \\  manufacturer: Acme
        \\commands:
        \\  set_voltage:
        \\    write: "VOLT {voltage:float}"
        \\    args:
        \\      voltage:
        \\        precision: 2
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var diagnostic_arena: std.heap.ArenaAllocator = .init(gpa);
    var diagnostics = diagnostic.Diagnostics.init(diagnostic_arena.allocator(), "recipe.yaml");

    var adapter = try parseAdapterInDir(gpa, std.testing.io, dir, "psu.yaml", diagnostics.reporter().withContext(.{ .adapter_name = "psu.yaml" }));
    defer adapter.deinit();

    diagnostics.deinit();
    diagnostic_arena.deinit();

    try std.testing.expectEqualStrings("owned adapter", adapter.meta.description.?);
    try std.testing.expectEqualStrings("Acme", adapter.instrument.manufacturer.?);

    const cmd = adapter.commands.get("set_voltage") orelse return error.TestUnexpectedResult;
    switch (cmd.template[1]) {
        .placeholder => |placeholder| {
            try std.testing.expectEqualStrings("voltage", placeholder.name);
            try std.testing.expectEqualStrings("float", placeholder.arg_type);
        },
        else => return error.TestUnexpectedResult,
    }
    const args = cmd.args orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?u8, 2), (args.get("voltage") orelse return error.TestUnexpectedResult).precision);
}

test "parse adapter document diagnostic does not invent byte position" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.makePath("adapters");
    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);
    var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, adapter_dir, .{});
    defer dir.close(std.testing.io);

    var parse_arena: std.heap.ArenaAllocator = .init(gpa);
    defer parse_arena.deinit();

    var diagnostics = diagnostic.Diagnostics.init(parse_arena.allocator(), "recipe.yaml");
    defer diagnostics.deinit();

    try std.testing.expectError(
        error.AnalysisFail,
        parseAdapterInDir(
            parse_arena.allocator(),
            std.testing.io,
            dir,
            "missing.yaml",
            diagnostics.reporter().withContext(.{ .instrument_name = "psu", .adapter_name = "missing.yaml" }),
        ),
    );

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try diagnostics.writeAll(&out.writer);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "adapter=missing.yaml: adapter not found\n"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, out.written(), 1, "at byte"));
}

test "parse args object form" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  set_output:
        \\    write: "OUTP {enabled:bool}"
        \\    args:
        \\      enabled:
        \\        true: "ON"
        \\        false: "OFF"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_output") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;
    const obj = args.get("enabled") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("ON", obj.true_text.?);
    try std.testing.expectEqualStrings("OFF", obj.false_text.?);
}

test "parse args object default form" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  select_channel:
        \\    write: "INST {channel:string}"
        \\    args:
        \\      channel:
        \\        default: "1"
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("select_channel") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;
    const obj = args.get("channel") orelse return error.TestUnexpectedResult;
    switch (obj.default.?) {
        .scalar => |scalar| switch (scalar) {
            .string => |s| try std.testing.expectEqualStrings("1", s),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}

test "parse args object precision and option values" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestYaml(gpa,
        \\commands:
        \\  configure:
        \\    write: "WAV {wavelength:float};TRIG:SOUR {source:option}"
        \\    args:
        \\      wavelength:
        \\        precision: 2
        \\      source:
        \\        options: [IMM, BUS, EXT]
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("configure") orelse return error.TestUnexpectedResult;
    const args = cmd.args orelse return error.TestUnexpectedResult;

    try std.testing.expectEqual(@as(?u8, 2), (args.get("wavelength") orelse return error.TestUnexpectedResult).precision);

    const source = args.get("source") orelse return error.TestUnexpectedResult;
    const options = source.options orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), options.len);
    try std.testing.expectEqualStrings("IMM", options[0]);
    try std.testing.expectEqualStrings("BUS", options[1]);
    try std.testing.expectEqualStrings("EXT", options[2]);
}
