const std = @import("std");
const Adapter = @import("Adapter.zig");
const doc_parse = @import("../doc_parse.zig");
const schema = @import("schema.zig");
const instrument = @import("../instrument.zig");

const max_adapter_file_size: usize = 512 * 1024;

/// Raw serialized shape of a adapter document.
const AdapterDoc = struct {
    metadata: schema.AdapterMeta = .{},
    instrument: schema.InstrumentSpec = .{},
    commands: std.json.ArrayHashMap(CommandDoc),
};

/// Raw serialized shape of a single command entry.
const CommandDoc = struct {
    write: []const u8,
    read: ?[]const u8 = null,
    description: ?[]const u8 = null,
    args: ?std.json.ArrayHashMap(schema.ArgSpec) = null,
};

/// Parses a adapter document from an already-open directory.
pub fn parseAdapterInDir(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, file_name: []const u8) !Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer adapter_arena.deinit();
    const alloc = adapter_arena.allocator();

    const parsed = try doc_parse.parseFileInDir(AdapterDoc, alloc, io, dir, file_name, max_adapter_file_size);
    const path = try dir.realPathFileAlloc(io, file_name, alloc);

    return buildAdapter(&adapter_arena, parsed, path);
}

fn buildAdapter(adapter_arena: *std.heap.ArenaAllocator, parsed: AdapterDoc, path: []const u8) !Adapter {
    const alloc = adapter_arena.allocator();

    var commands: std.StringHashMap(schema.Command) = .init(alloc);
    var it = parsed.commands.map.iterator();
    while (it.next()) |entry| {
        const cmd_doc = entry.value_ptr.*;
        var cmd: schema.Command = try .parse(alloc, cmd_doc.write, cmd_doc.read, cmd_doc.description);
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

    var adapter = try parseTestJson(gpa,
        \\{
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage},(@{channels})"
        \\    }
        \\  }
        \\}
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

    var adapter = try parseTestJson(gpa,
        \\{
        \\  "commands": {
        \\    "measure_voltage": {
        \\      "write": "MEAS:VOLT?",
        \\      "read": "float"
        \\    }
        \\  }
        \\}
    );
    defer adapter.deinit();

    const cmd = adapter.commands.get("measure_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema.Encoding.float, cmd.response.?);
    try std.testing.expectEqual(@as(usize, 1), cmd.template.len);
}

test "parse adapter with write termination" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestJson(gpa,
        \\{
        \\  "metadata": {
        \\    "version": "1.0",
        \\    "description": "PSU over serial"
        \\  },
        \\  "instrument": {
        \\    "write_termination": "\n"
        \\  },
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage}"
        \\    }
        \\  }
        \\}
    );
    defer adapter.deinit();

    try std.testing.expectEqualStrings("\n", adapter.write_termination);
}

test "parse adapter without write termination defaults to none" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestJson(gpa,
        \\{
        \\  "commands": {
        \\    "measure": {
        \\      "write": "MEAS?",
        \\      "read": "float"
        \\    }
        \\  }
        \\}
    );
    defer adapter.deinit();

    try std.testing.expectEqualStrings("", adapter.write_termination);
}

test "parse adapter instrument options" {
    const gpa = std.testing.allocator;

    var adapter = try parseTestJson(gpa,
        \\{
        \\  "metadata": {
        \\    "version": "1.0",
        \\    "description": "Scope over TCPIP"
        \\  },
        \\  "instrument": {
        \\    "timeout_ms": 2500,
        \\    "read_termination": "\n",
        \\    "write_termination": "\r\n",
        \\    "query_delay_ms": 25,
        \\    "chunk_size": 4096
        \\  },
        \\  "commands": {
        \\    "idn": {
        \\      "write": "*IDN?",
        \\      "read": "string"
        \\    }
        \\  }
        \\}
    );
    defer adapter.deinit();

    try std.testing.expectEqual(@as(?u32, 2500), adapter.options.timeout_ms);
    try std.testing.expectEqualStrings("\n", adapter.options.read_termination.constSlice());
    try std.testing.expectEqualStrings("\r\n", adapter.write_termination);
    try std.testing.expectEqual(@as(u32, 25), adapter.options.query_delay_ms);
    try std.testing.expectEqual(@as(usize, 4096), adapter.options.chunk_size);
}

fn parseTestJson(allocator: std.mem.Allocator, content: []const u8) !Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer adapter_arena.deinit();
    const alloc = adapter_arena.allocator();

    const parsed = try doc_parse.parseFromSlice(AdapterDoc, alloc, content);

    return buildAdapter(&adapter_arena, parsed, try alloc.dupe(u8, "<test>"));
}
