const std = @import("std");
const types = @import("types.zig");
const visa = @import("../visa/root.zig");

/// Loaded driver document with parsed command templates and metadata.
/// Owns arena-backed data and should have a single logical owner until `deinit`.
const Driver = @This();

arena: std.heap.ArenaAllocator,
path: []const u8,
meta: types.DriverMeta,
instrument: types.InstrumentSpec,
commands: std.StringHashMap(Command),
/// Suffix appended to every write command (e.g. "\n", "\r\n").
/// Empty string means no write termination. Owned by the driver arena.
write_termination: []const u8,
/// Resolved session options derived from driver metadata, excluding write termination.
options: visa.InstrumentOptions,

/// Parsed command entry from a driver document.
pub const Command = types.Command;

/// Supported response encodings declared by driver commands.
pub const Encoding = types.Encoding;

/// Releases all arena-owned memory associated with a parsed driver.
pub fn deinit(self: *Driver) void {
    self.arena.deinit();
}

test {
    std.testing.refAllDecls(@This());
}
