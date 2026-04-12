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
        const parsed_template = try template.parseTemplate(allocator, write_template);
        defer allocator.free(parsed_template);

        return .{
            .description = if (description_value) |d| try allocator.dupe(u8, d) else null,
            .response = try Encoding.resolveFromReadSpec(read_value),
            .template = try cloneTemplateSegments(allocator, parsed_template),
        };
    }

    /// Duplicates the parsed template into allocator-owned memory.
    pub fn clone(self: Command, allocator: std.mem.Allocator) !Command {
        return .{
            .description = if (self.description) |d| try allocator.dupe(u8, d) else null,
            .response = self.response,
            .template = try cloneTemplateSegments(allocator, self.template),
        };
    }

    /// Releases a command template previously allocated by `parse` or `clone`.
    pub fn deinit(self: Command, allocator: std.mem.Allocator) void {
        if (self.description) |d| allocator.free(d);
        template.freeSegments(allocator, self.template);
    }

    /// Returns unique placeholder names referenced by the command template.
    pub fn placeholderNames(self: Command, allocator: std.mem.Allocator) ![]const []const u8 {
        var placeholders = std.ArrayList([]const u8).empty;
        defer placeholders.deinit(allocator);

        for (self.template) |segment| {
            switch (segment) {
                .literal => {},
                .placeholder => |placeholder| {
                    if (containsString(placeholders.items, placeholder.name)) continue;
                    try placeholders.append(allocator, placeholder.name);
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

    fn cloneTemplateSegments(
        allocator: std.mem.Allocator,
        source_template: []const template.Segment,
    ) ![]template.Segment {
        const cloned = try allocator.alloc(template.Segment, source_template.len);
        errdefer allocator.free(cloned);

        var initialized: usize = 0;
        errdefer {
            for (cloned[0..initialized]) |segment| {
                switch (segment) {
                    .literal => |literal| allocator.free(literal),
                    .placeholder => |placeholder| allocator.free(placeholder.name),
                }
            }
        }

        for (source_template, 0..) |segment, idx| {
            cloned[idx] = switch (segment) {
                .literal => |literal| .{ .literal = try allocator.dupe(u8, literal) },
                .placeholder => |placeholder| .{ .placeholder = .{ .name = try allocator.dupe(u8, placeholder.name) } },
            };
            initialized += 1;
        }

        return cloned;
    }
};

test "adapter command clones and reports placeholders" {
    const gpa = std.testing.allocator;

    const parsed = try Command.parse(gpa, "VOLT {voltage},(@{channels})", null, null);
    defer parsed.deinit(gpa);

    const cloned = try parsed.clone(gpa);
    defer cloned.deinit(gpa);

    const placeholders = try cloned.placeholderNames(gpa);
    defer gpa.free(placeholders);

    try std.testing.expectEqual(@as(usize, 2), placeholders.len);
    try std.testing.expectEqualStrings("voltage", placeholders[0]);
    try std.testing.expectEqualStrings("channels", placeholders[1]);
}
