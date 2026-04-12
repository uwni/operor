const std = @import("std");
const Adapter = @import("Adapter.zig");
const doc_parse = @import("../doc_parse.zig");
const testing = @import("../testing.zig");
const types = @import("types.zig");
const visa = @import("../visa/root.zig");

const max_adapter_file_size: usize = 512 * 1024;

/// Raw serialized shape of a adapter document.
const AdapterDoc = struct {
    metadata: types.AdapterMeta = .{},
    instrument: types.InstrumentSpec = .{},
    commands: std.StringHashMap(CommandDoc),
};

/// Raw serialized shape of a single command entry.
const CommandDoc = struct {
    write: []const u8,
    read: ?[]const u8 = null,
    description: ?[]const u8 = null,
};

/// Parses a adapter document from an already-open directory.
pub fn parseAdapterInDir(allocator: std.mem.Allocator, dir: std.fs.Dir, file_name: []const u8) !Adapter {
    var adapter_arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer adapter_arena.deinit();
    const alloc = adapter_arena.allocator();

    const parsed = try doc_parse.parseFileInDir(AdapterDoc, alloc, dir, file_name, max_adapter_file_size);

    var commands: std.StringHashMap(types.Command) = .init(alloc);
    var it = parsed.commands.iterator();
    while (it.next()) |entry| {
        const cmd_doc = entry.value_ptr.*;
        const cmd = try types.Command.parse(alloc, cmd_doc.write, cmd_doc.read, cmd_doc.description);
        try commands.put(entry.key_ptr.*, cmd);
    }

    const inst = parsed.instrument;
    const write_termination = inst.write_termination orelse "";

    return Adapter{
        .arena = adapter_arena,
        .path = try dir.realpathAlloc(alloc, file_name),
        .meta = parsed.metadata,
        .instrument = inst,
        .commands = commands,
        .write_termination = write_termination,
        .options = .{
            .timeout_ms = inst.timeout_ms,
            .read_termination = visa.Termination.fromSlice(inst.read_termination orelse ""),
            .query_delay_ms = inst.query_delay_ms orelse 0,
            .chunk_size = inst.chunk_size orelse visa.default_chunk_size,
        },
    };
}

test "parse adapter templates and placeholders" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/vendor_psu_set_voltage.toml",
        \\[metadata]
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage},(@{channels})"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var adapter = try parseAdapterInDir(gpa, dir, "vendor_psu_set_voltage.toml");
    defer adapter.deinit();

    const cmd = adapter.commands.get("set_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expect(cmd.response == null);
    try std.testing.expectEqual(@as(usize, 5), cmd.template.len);

    switch (cmd.template[1]) {
        .placeholder => |placeholder| try std.testing.expectEqualStrings("voltage", placeholder.name),
        else => return error.TestUnexpectedResult,
    }
    switch (cmd.template[3]) {
        .placeholder => |placeholder| try std.testing.expectEqualStrings("channels", placeholder.name),
        else => return error.TestUnexpectedResult,
    }
}

test "parse adapter response encoding" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/vendor_dmm_measure_voltage.toml",
        \\[metadata]
        \\
        \\[commands.measure_voltage]
        \\write = "MEAS:VOLT?"
        \\read = "float"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var adapter = try parseAdapterInDir(gpa, dir, "vendor_dmm_measure_voltage.toml");
    defer adapter.deinit();

    const cmd = adapter.commands.get("measure_voltage") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.Encoding.float, cmd.response.?);
    try std.testing.expectEqual(@as(usize, 1), cmd.template.len);
}

test "parse adapter with write termination" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/serial_psu.toml",
        \\[metadata]
        \\version = "1.0"
        \\description = "PSU over serial"
        \\
        \\[instrument]
        \\write_termination = "\n"
        \\
        \\[commands.set_voltage]
        \\write = "VOLT {voltage}"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var adapter = try parseAdapterInDir(gpa, dir, "serial_psu.toml");
    defer adapter.deinit();

    try std.testing.expectEqualStrings("\n", adapter.write_termination);
}

test "parse adapter without write termination defaults to none" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/gpib_dmm.toml",
        \\[metadata]
        \\
        \\[commands.measure]
        \\write = "MEAS?"
        \\read = "float"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var adapter = try parseAdapterInDir(gpa, dir, "gpib_dmm.toml");
    defer adapter.deinit();

    try std.testing.expectEqualStrings("", adapter.write_termination);
}

test "parse adapter instrument options" {
    const gpa = std.testing.allocator;

    var workspace: testing.TestWorkspace = .init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("adapters/tcp_scope.toml",
        \\[metadata]
        \\version = "1.0"
        \\description = "Scope over TCPIP"
        \\
        \\[instrument]
        \\timeout_ms = 2500
        \\read_termination = "\n"
        \\write_termination = "\r\n"
        \\query_delay_ms = 25
        \\chunk_size = 4096
        \\
        \\[commands.idn]
        \\write = "*IDN?"
        \\read = "string"
    );

    const adapter_dir = try workspace.realpathAlloc("adapters");
    defer gpa.free(adapter_dir);

    var dir = try std.fs.openDirAbsolute(adapter_dir, .{});
    defer dir.close();

    var adapter = try parseAdapterInDir(gpa, dir, "tcp_scope.toml");
    defer adapter.deinit();

    try std.testing.expectEqual(@as(?u32, 2500), adapter.options.timeout_ms);
    try std.testing.expectEqualStrings("\n", adapter.options.read_termination.constSlice());
    try std.testing.expectEqualStrings("\r\n", adapter.write_termination);
    try std.testing.expectEqual(@as(u32, 25), adapter.options.query_delay_ms);
    try std.testing.expectEqual(@as(usize, 4096), adapter.options.chunk_size);
}
