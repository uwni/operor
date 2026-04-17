const std = @import("std");
const schema = @import("schema.zig");
const instrument_types = @import("../instrument.zig");

/// Loaded adapter document with parsed command templates and metadata.
/// Owns arena-backed data and should have a single logical owner until `deinit`.
const Adapter = @This();

arena: std.heap.ArenaAllocator,
path: []const u8,
meta: schema.AdapterMeta,
instrument: schema.InstrumentSpec,
commands: std.StringHashMap(Command),
/// Suffix appended to every write command (e.g. "\n", "\r\n").
/// Empty string means no write termination. Owned by the adapter arena.
write_termination: []const u8,
/// Resolved session options derived from adapter metadata, excluding write termination.
options: instrument_types.InstrumentOptions,

/// Parsed command entry from a adapter document.
pub const Command = schema.Command;

/// Supported response encodings declared by adapter commands.
pub const Encoding = schema.Encoding;

/// Releases all arena-owned memory associated with a parsed adapter.
pub fn deinit(self: *Adapter) void {
    self.arena.deinit();
}

test {
    std.testing.refAllDecls(@This());
}
