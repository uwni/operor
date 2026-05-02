const std = @import("std");
const serde_lib = @import("serde");
const template = @import("template.zig");
const diagnostic = @import("../diagnostic.zig");
const instrument = @import("../instrument.zig");

pub const Encoding = instrument.Encoding;
pub const ResponseSpec = instrument.ResponseSpec;

const default_response_separator = ",";

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
    true_text: []const u8,
    false_text: []const u8,

    pub const serde = .{
        .rename = .{
            .true_text = "true",
            .false_text = "false",
        },
    };
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
    /// Default float precision (decimal places) for all float args in this adapter.
    float_precision: ?u8 = null,
};

/// Literal default value for an adapter command argument.
/// Mirrors recipe argument values but lives in the adapter schema to avoid
/// coupling adapter parsing to recipe parsing.
pub const ArgDefaultScalar = union(enum) {
    string: []const u8,
    int: i64,
    float: f64,
    bool: bool,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

pub const ArgDefault = union(enum) {
    scalar: ArgDefaultScalar,
    list: []const ArgDefaultScalar,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

/// Argument formatting specification declared in adapter command `args`.
/// Deserialized directly from YAML/TOML; interpretation deferred to precompile.
pub const ArgSpec = struct {
    true_text: ?[]const u8 = null,
    false_text: ?[]const u8 = null,
    separator: ?[]const u8 = null,
    precision: ?u8 = null,
    options: ?std.StringHashMap([]const u8) = null,
    default: ?ArgDefault = null,

    pub const serde = .{
        .rename = .{
            .true_text = "true",
            .false_text = "false",
        },
    };
};

/// Response type specification declared in adapter command `read`.
/// Short scalar form: `float`; object/list form: `[float, float]`.
pub const ReadSpec = union(enum) {
    scalar: []const u8,
    list: []const []const u8,
    object: ReadSpecObject,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

/// Extended response list form.
/// Use `items` for a fixed-length heterogeneous list, or `type` for a variable-length homogeneous spread.
pub const ReadSpecObject = struct {
    split: ?[]const u8 = null,
    items: ?[]const []const u8 = null,
    type: ?[]const u8 = null,
};

/// Parsed command entry from a adapter document.
/// All owned data lives in the arena passed to `parse`; no individual `deinit` needed.
pub const Command = struct {
    /// Optional human-readable description of the command.
    description: ?[]const u8 = null,
    /// Expected response encoding when the command reads back data.
    response: ?ResponseSpec,
    /// Pre-parsed write template ready for precompilation.
    template: []const template.Segment,
    /// Optional argument type specifications from the adapter document.
    args: ?std.StringHashMap(ArgSpec) = null,

    /// Parses a command from a write template and optional read encoding spec.
    pub fn parse(
        allocator: std.mem.Allocator,
        write_template: []const u8,
        read_value: ?ReadSpec,
        description_value: ?[]const u8,
        reporter: diagnostic.Reporter,
    ) diagnostic.Error!Command {
        return .{
            .description = if (description_value) |d| try allocator.dupe(u8, d) else null,
            .response = try parseReadType(read_value, allocator, reporter),
            .template = try template.parseTemplate(allocator, write_template, reporter),
        };
    }

    /// Frees all owned data allocated by `parse`.
    pub fn deinit(self: *Command, allocator: std.mem.Allocator) void {
        template.freeSegments(allocator, self.template);
        if (self.response) |response| response.deinit(allocator);
        if (self.description) |d| allocator.free(d);
        self.* = undefined;
    }
};

fn parseReadType(
    read_value: ?ReadSpec,
    allocator: std.mem.Allocator,
    reporter: diagnostic.Reporter,
) diagnostic.Error!?ResponseSpec {
    const read = read_value orelse return null;
    return switch (read) {
        .scalar => |name| {
            if (std.mem.indexOfScalar(u8, name, '{') != null) {
                return .{ .object = try parseObjectReadTemplate(allocator, name, reporter) };
            }
            return .{ .scalar = try parseEncoding(name, reporter) };
        },
        .list => |items| .{ .list = try parseReadList(allocator, default_response_separator, items, reporter) },
        .object => |object| blk: {
            const sep = object.split orelse default_response_separator;
            if (object.type) |t| {
                break :blk .{ .spread = .{
                    .separator = try allocator.dupe(u8, sep),
                    .type = try parseEncoding(t, reporter),
                } };
            }
            break :blk .{ .list = try parseReadList(allocator, sep, object.items orelse &.{}, reporter) };
        },
    };
}

fn parseObjectReadTemplate(
    allocator: std.mem.Allocator,
    source: []const u8,
    reporter: diagnostic.Reporter,
) diagnostic.Error!instrument.ObjectResponseSpec {
    const raw_segments = try template.parseReadTemplate(allocator, source, reporter);
    defer template.freeSegments(allocator, raw_segments);

    const scoped = reporter.withSource(.adapter_read_type, source);

    var out: std.ArrayList(instrument.ObjectSegment) = .empty;
    errdefer {
        for (out.items) |seg| switch (seg) {
            .literal => |lit| allocator.free(lit),
            .field => |f| allocator.free(f.name),
        };
        out.deinit(allocator);
    }

    var has_field = false;
    var last_was_field = false;
    for (raw_segments) |seg| switch (seg) {
        .optional => return scoped.fail(.{ .start = 0, .end = source.len }, .{ .invalid_read_type = .{ .read_type = source } }),
        .literal => |lit| {
            try out.append(allocator, .{ .literal = try allocator.dupe(u8, lit) });
            last_was_field = false;
        },
        .placeholder => |ph| {
            if (last_was_field) return scoped.fail(.{ .start = 0, .end = source.len }, .{ .invalid_read_type = .{ .read_type = source } });
            try out.append(allocator, .{ .field = .{
                .name = try allocator.dupe(u8, ph.name),
                .encoding = try parseEncoding(ph.arg_type, reporter),
            } });
            has_field = true;
            last_was_field = true;
        },
    };

    if (!has_field) {
        return scoped.fail(.{ .start = 0, .end = source.len }, .{ .invalid_read_type = .{ .read_type = source } });
    }

    return .{ .segments = try out.toOwnedSlice(allocator) };
}

fn parseReadList(
    allocator: std.mem.Allocator,
    separator: []const u8,
    items: []const []const u8,
    reporter: diagnostic.Reporter,
) diagnostic.Error!instrument.ListResponseSpec {
    if (items.len == 0) {
        const source = "[]";
        return reporter
            .withSource(.adapter_read_type, source)
            .fail(.{ .start = 0, .end = source.len }, .{ .invalid_read_type = .{ .read_type = source } });
    }
    if (separator.len == 0) {
        return reporter
            .withSource(.adapter_read_type, separator)
            .fail(.{ .start = 0, .end = separator.len }, .{ .invalid_read_type = .{ .read_type = separator } });
    }

    const parsed_items = try allocator.alloc(Encoding, items.len);
    errdefer allocator.free(parsed_items);
    for (items, 0..) |item, idx| {
        parsed_items[idx] = try parseEncoding(item, reporter);
    }

    return .{
        .separator = try allocator.dupe(u8, separator),
        .items = parsed_items,
    };
}

fn parseEncoding(
    read: []const u8,
    reporter: diagnostic.Reporter,
) diagnostic.Error!Encoding {
    return Encoding.fromString(read) orelse return reporter
        .withSource(.adapter_read_type, read)
        .fail(.{ .start = 0, .end = read.len }, .{ .invalid_read_type = .{ .read_type = read } });
}
