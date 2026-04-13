const std = @import("std");
const template = @import("template.zig");

/// Human-readable metadata declared in a adapter document.
pub const AdapterMeta = struct {
    /// Optional semantic version string for the adapter definition.
    version: ?[]const u8 = null,
    /// Optional free-form description shown to operators.
    description: ?[]const u8 = null,
    /// Optional author or maintainer of the adapter definition.
    author: ?[]const u8 = null,
};

/// Instrument-level defaults declared in a adapter document.
/// These fields configure VISA session behaviour and identity matching.
pub const InstrumentSpec = struct {
    /// Optional timeout applied to VISA I/O for this adapter's instruments.
    timeout_ms: ?u32 = null,
    /// Optional response suffix removed from owned reads.
    read_termination: ?[]const u8 = null,
    /// Optional suffix appended to every write command (e.g. `"\n"`).
    write_termination: ?[]const u8 = null,
    /// Optional delay inserted between write and read when a command expects a response.
    query_delay_ms: ?u32 = null,
    /// Optional read chunk size for owned response collection.
    chunk_size: ?usize = null,
    /// Manufacturer name expected in `*IDN?` responses (e.g. `"Keysight Technologies"`).
    manufacturer: ?[]const u8 = null,
    /// Optional list of supported device models (e.g. `["PSU-3303", "PSU-3305"]`).
    /// Matched against the model field in `*IDN?` responses.
    models: ?[]const []const u8 = null,
    /// Optional firmware version or pattern for `*IDN?` validation.
    firmware: ?[]const u8 = null,
};

/// Supported response encodings declared by adapter commands.
pub const Encoding = enum {
    raw,
    float,
    int,
    string,

    const map = std.StaticStringMap(Encoding).initComptime(.{
        .{ "raw", .raw },
        .{ "float", .float },
        .{ "int", .int },
        .{ "string", .string },
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

/// Parsed command entry from a adapter document.
/// All owned data lives in the arena passed to `parse`; no individual `deinit` needed.
pub const Command = struct {
    /// Optional human-readable description of the command.
    description: ?[]const u8 = null,
    /// Expected response encoding when the command reads back data.
    response: ?Encoding,
    /// Pre-parsed write template ready for precompilation.
    template: []const template.Segment,

    /// Parses a command from a write template and optional read encoding spec.
    pub fn parse(
        allocator: std.mem.Allocator,
        write_template: []const u8,
        read_value: ?[]const u8,
        description_value: ?[]const u8,
    ) !Command {
        return .{
            .description = if (description_value) |d| try allocator.dupe(u8, d) else null,
            .response = try Encoding.resolveFromReadSpec(read_value),
            .template = try template.parseTemplate(allocator, write_template),
        };
    }

    /// Frees all owned data allocated by `parse`.
    pub fn deinit(self: Command, allocator: std.mem.Allocator) void {
        template.freeSegments(allocator, self.template);
        if (self.description) |d| allocator.free(d);
    }

    /// Returns unique placeholder names referenced by the command template.
    pub fn placeholderNames(self: Command, allocator: std.mem.Allocator) ![]const []const u8 {
        var placeholders = std.ArrayList([]const u8).empty;
        defer placeholders.deinit(allocator);

        for (self.template) |segment| {
            switch (segment) {
                .literal => {},
                .placeholder => |name| {
                    if (containsString(placeholders.items, name)) continue;
                    try placeholders.append(allocator, name);
                },
            }
        }

        return placeholders.toOwnedSlice(allocator);
    }

    fn containsString(haystack: []const []const u8, needle: []const u8) bool {
        for (haystack) |item| {
            if (std.mem.eql(u8, item, needle)) return true;
        }
        return false;
    }
};

test "adapter command parses and reports placeholders" {
    const gpa = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parsed = try Command.parse(alloc, "VOLT {voltage},(@{channels})", null, null);

    const placeholders = try parsed.placeholderNames(alloc);
    defer alloc.free(placeholders);

    try std.testing.expectEqual(@as(usize, 2), placeholders.len);
    try std.testing.expectEqualStrings("voltage", placeholders[0]);
    try std.testing.expectEqualStrings("channels", placeholders[1]);
}
