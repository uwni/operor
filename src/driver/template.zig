/// Utilities for parsing and rendering command templates, e.g. "VOLT {voltage} {channels}".
/// The template is parsed into segments, which are either literal strings or placeholders.
const std = @import("std");

/// A command template segment, either a literal string or a placeholder for a variable value.
pub const Segment = union(enum) {
    /// A literal string segment that should be included as-is in the rendered command.
    literal: []const u8,

    /// A placeholder segment that should be replaced with a variable value when rendering.
    placeholder: Placeholder,
};

/// A placeholder represents a variable part of the command template.
pub const Placeholder = struct {
    /// Placeholder identifier without braces.
    name: []const u8,
};

/// Errors that can occur while parsing a template string into segments.
pub const TemplateParseError = error{
    MissingClosingBrace,
    InvalidIdentifier,
    EmptyArgument,
    OutOfMemory,
};

/// Errors that can occur while rendering parsed segments with concrete values.
pub const RenderError = error{
    MissingVariable,
    BufferTooSmall,
    OutOfMemory,
};

/// Parse a command template string into segments of literals and placeholders.
pub fn parseTemplate(allocator: std.mem.Allocator, tem_str: []const u8) TemplateParseError![]Segment {
    var segments: std.ArrayList(Segment) = .empty;
    errdefer segments.deinit(allocator);

    var i: usize = 0;
    var literal_start: usize = 0;

    while (i < tem_str.len) : (i += 1) {
        if (tem_str[i] != '{') continue;

        if (i > literal_start) {
            try segments.append(allocator, .{ .literal = tem_str[literal_start..i] });
        }

        var close_idx = i + 1;
        while (close_idx < tem_str.len and tem_str[close_idx] != '}') : (close_idx += 1) {}
        if (close_idx == tem_str.len) return error.MissingClosingBrace;

        const inner = std.mem.trim(u8, tem_str[i + 1 .. close_idx], " \t\r\n");
        if (inner.len == 0) return error.EmptyArgument;
        if (!isIdentifier(inner)) return error.InvalidIdentifier;
        try segments.append(allocator, .{ .placeholder = .{ .name = inner } });

        i = close_idx;
        literal_start = close_idx + 1;
    }

    if (literal_start < tem_str.len) {
        try segments.append(allocator, .{ .literal = tem_str[literal_start..] });
    }

    return segments.toOwnedSlice(allocator);
}

/// Releases allocator-owned template segments produced by cloning (not by `parseTemplate`,
/// which returns slices into the original template string).
pub fn freeSegments(allocator: std.mem.Allocator, segments: []const Segment) void {
    for (segments) |segment| {
        switch (segment) {
            .literal => |literal| allocator.free(literal),
            .placeholder => |placeholder| allocator.free(placeholder.name),
        }
    }
    allocator.free(segments);
}

/// Renders parsed segments into a caller-provided buffer.
pub fn renderInto(
    buffer: []u8,
    segments: []const Segment,
    values: *const std.StringHashMap([]const u8),
) RenderError![]const u8 {
    var used_len: usize = 0;
    for (segments) |seg| {
        const bytes = switch (seg) {
            .literal => |lit| lit,
            .placeholder => |pl| values.get(pl.name) orelse return error.MissingVariable,
        };
        try appendAt(buffer, &used_len, bytes);
    }
    return buffer[0..used_len];
}

/// Renders parsed segments into a newly allocated slice owned by the caller.
pub fn renderAlloc(
    allocator: std.mem.Allocator,
    segments: []const Segment,
    values: *const std.StringHashMap([]const u8),
) RenderError![]u8 {
    return renderAllocWithSuffix(allocator, segments, values, "");
}

/// Renders parsed segments plus a suffix into a newly allocated slice owned by the caller.
pub fn renderAllocWithSuffix(
    allocator: std.mem.Allocator,
    segments: []const Segment,
    values: *const std.StringHashMap([]const u8),
    suffix: []const u8,
) RenderError![]u8 {
    const rendered_len = try renderedLen(segments, values);
    const total_len = std.math.add(usize, rendered_len, suffix.len) catch return error.OutOfMemory;

    const out = try allocator.alloc(u8, total_len);
    errdefer allocator.free(out);

    const rendered = try renderInto(out[0..rendered_len], segments, values);
    std.debug.assert(rendered.len == rendered_len);
    var used_len = rendered_len;
    try appendAt(out, &used_len, suffix);
    std.debug.assert(used_len == total_len);
    return out;
}

/// Appends `src` to `buffer` at `used_len`, advancing it on success.
pub fn appendAt(buffer: []u8, used_len: *usize, src: []const u8) error{BufferTooSmall}!void {
    std.debug.assert(used_len.* <= buffer.len);
    if (buffer.len - used_len.* < src.len) return error.BufferTooSmall;
    @memcpy(buffer[used_len.* .. used_len.* + src.len], src);
    used_len.* += src.len;
}

fn renderedLen(
    segments: []const Segment,
    values: *const std.StringHashMap([]const u8),
) RenderError!usize {
    var total: usize = 0;
    for (segments) |seg| {
        const bytes = switch (seg) {
            .literal => |lit| lit,
            .placeholder => |pl| values.get(pl.name) orelse return error.MissingVariable,
        };
        total = std.math.add(usize, total, bytes.len) catch return error.OutOfMemory;
    }
    return total;
}

/// Returns whether `name` is a valid placeholder identifier.
fn isIdentifier(name: []const u8) bool {
    if (name.len == 0) return false;
    if (!(std.ascii.isAlphabetic(name[0]) or name[0] == '_')) return false;
    for (name[1..]) |c| {
        if (!(std.ascii.isAlphabetic(c) or std.ascii.isDigit(c) or c == '_')) return false;
    }
    return true;
}

test "parse template segments" {
    const gpa = std.testing.allocator;
    const input = "VOLT {voltage} {channels}";
    const segments = try parseTemplate(gpa, input);
    defer gpa.free(segments);

    try std.testing.expectEqual(@as(usize, 4), segments.len);
}

test "render template into buffer" {
    const gpa = std.testing.allocator;
    const input = "MEAS:VOLT? (@{channels})";
    const segments = try parseTemplate(gpa, input);
    defer gpa.free(segments);

    var values = std.StringHashMap([]const u8).init(gpa);
    defer values.deinit();
    try values.put("channels", "1,2,3");

    var buffer: [64]u8 = undefined;
    const rendered = try renderInto(buffer[0..], segments, &values);
    try std.testing.expectEqualStrings("MEAS:VOLT? (@1,2,3)", rendered);
}

test "appendAt advances used length and rejects overflow" {
    var buffer: [8]u8 = undefined;
    var used_len: usize = 0;

    try appendAt(buffer[0..], &used_len, "MEAS");
    try appendAt(buffer[0..], &used_len, "\n");
    try std.testing.expectEqual(@as(usize, 5), used_len);
    try std.testing.expectEqualStrings("MEAS\n", buffer[0..used_len]);

    try std.testing.expectError(error.BufferTooSmall, appendAt(buffer[0..], &used_len, "TOO-LONG"));
}

test "render template allocates with suffix" {
    const gpa = std.testing.allocator;
    const input = "TL{temperature_c}";
    const segments = try parseTemplate(gpa, input);
    defer gpa.free(segments);

    var values = std.StringHashMap([]const u8).init(gpa);
    defer values.deinit();
    try values.put("temperature_c", "23.5");

    const rendered = try renderAllocWithSuffix(gpa, segments, &values, "\r\n");
    defer gpa.free(rendered);
    try std.testing.expectEqualStrings("TL23.5\r\n", rendered);
}
