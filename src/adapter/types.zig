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

/// Configurable string mapping for boolean write/read values.
/// Both fields must be explicitly provided by the adapter author.
pub const BoolFormat = struct {
    true: []const u8,
    false: []const u8,
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
    /// Default bool format for all commands in this adapter.
    /// Must be set explicitly when any command uses bool args.
    bool_format: ?BoolFormat = null,
};

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

/// Argument type specification declared in adapter command `args`.
/// Deserialized directly from JSON; interpretation deferred to precompile.
pub const ArgSpec = union(enum) {
    /// Short form: `"bool"` or `"list"`.
    string: []const u8,
    /// Full form: `{"type": "bool", "true": "1", "false": "0"}`.
    object: ArgSpecObject,
};

/// Object form of an argument type specification.
pub const ArgSpecObject = struct {
    type: []const u8,
    true: ?[]const u8 = null,
    false: ?[]const u8 = null,
    separator: ?[]const u8 = null,
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
    /// Optional argument type specifications from the adapter document.
    args: ?std.json.ArrayHashMap(ArgSpec) = null,

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
};
