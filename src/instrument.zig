const std = @import("std");

// ── Instrument session options ──────────────────────────────────────────

pub const default_chunk_size: usize = 1024;

/// Fixed-size termination character buffer (up to 4 bytes, e.g. "\r\n").
pub const Termination = struct {
    pub const max_len = 4;

    buf: [max_len]u8 = undefined,
    len: usize = 0,

    pub fn constSlice(self: *const Termination) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn fromSlice(s: []const u8) Termination {
        var t: Termination = .{};
        const n = @min(s.len, max_len);
        @memcpy(t.buf[0..n], s[0..n]);
        t.len = n;
        return t;
    }

    pub fn append(self: *Termination, byte: u8) error{Overflow}!void {
        if (self.len >= max_len) return error.Overflow;
        self.buf[self.len] = byte;
        self.len += 1;
    }
};

/// High-level session configuration applied when opening an instrument.
pub const InstrumentOptions = struct {
    /// Per-operation timeout in milliseconds. Null leaves the backend default unchanged.
    timeout_ms: ?u32 = null,
    /// Response suffix removed from owned reads when present.
    read_termination: Termination = .{},
    /// Delay inserted between a write and the following read in query flows.
    query_delay_ms: u32 = 0,
    /// Size of the temporary chunk buffer used when reading owned responses.
    chunk_size: usize = default_chunk_size,

    pub fn normalizedChunkSize(self: InstrumentOptions) usize {
        return if (self.chunk_size == 0) default_chunk_size else self.chunk_size;
    }
};

// ── Response encoding ───────────────────────────────────────────────────

/// Supported response encodings declared by adapter commands.
pub const Encoding = enum {
    raw,
    float,
    int,
    string,
    bool,

    const map = std.StaticStringMap(Encoding).initComptime(.{
        .{ "raw", .raw },
        .{ "float", .float },
        .{ "int", .int },
        .{ "string", .string },
        .{ "bool", .bool },
    });

    fn parseFromString(tag: []const u8) !Encoding {
        return map.get(tag) orelse error.InvalidValueType;
    }

    /// Converts an optional `read` specification into an encoding enum.
    pub fn resolveFromReadSpec(read_value: ?[]const u8) !?Encoding {
        const spec = read_value orelse return null;
        return try parseFromString(spec);
    }
};
