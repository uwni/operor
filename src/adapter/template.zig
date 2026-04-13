/// Utilities for parsing and rendering command templates, e.g. "VOLT {voltage} {channels}".
/// The template is parsed into segments, which are either literal strings or placeholders.
const std = @import("std");

/// A command template segment, either a literal string or a placeholder for a variable value.
pub const Segment = union(enum) {
    /// A literal string segment that should be included as-is in the rendered command.
    literal: []const u8,

    /// A placeholder name (identifier without braces) to be replaced when rendering.
    placeholder: []const u8,
};

/// Errors that can occur while parsing a template string into segments.
pub const TemplateParseError = error{
    MissingClosingBrace,
    InvalidIdentifier,
    EmptyArgument,
    OutOfMemory,
};

/// Parse a command template string into owned segments of literals and placeholders.
pub fn parseTemplate(allocator: std.mem.Allocator, tem_str: []const u8) TemplateParseError![]Segment {
    var segments: std.ArrayList(Segment) = .empty;
    errdefer {
        for (segments.items) |seg| switch (seg) {
            .literal, .placeholder => |p| allocator.free(p),
        };
        segments.deinit(allocator);
    }

    var i: usize = 0;
    var literal_start: usize = 0;

    while (i < tem_str.len) : (i += 1) {
        if (tem_str[i] != '{') continue;

        if (i > literal_start) {
            try segments.append(allocator, .{ .literal = try allocator.dupe(u8, tem_str[literal_start..i]) });
        }

        var close_idx = i + 1;
        while (close_idx < tem_str.len and tem_str[close_idx] != '}') : (close_idx += 1) {}
        if (close_idx == tem_str.len) return error.MissingClosingBrace;

        const inner = std.mem.trim(u8, tem_str[i + 1 .. close_idx], " \t\r\n");
        if (inner.len == 0) return error.EmptyArgument;
        if (!isIdentifier(inner)) return error.InvalidIdentifier;
        try segments.append(allocator, .{ .placeholder = try allocator.dupe(u8, inner) });

        i = close_idx;
        literal_start = close_idx + 1;
    }

    if (literal_start < tem_str.len) {
        try segments.append(allocator, .{ .literal = try allocator.dupe(u8, tem_str[literal_start..]) });
    }

    return segments.toOwnedSlice(allocator);
}

/// Frees all owned data within segments, then the slice itself.
pub fn freeSegments(allocator: std.mem.Allocator, segments: []const Segment) void {
    for (segments) |seg| switch (seg) {
        .literal => |s| allocator.free(s),
        .placeholder => |p| allocator.free(p),
    };
    allocator.free(segments);
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
    const segments = try parseTemplate(gpa, "VOLT {voltage} {channels}");
    defer freeSegments(gpa, segments);
    try std.testing.expectEqual(@as(usize, 4), segments.len);
}

test "parse template error: missing closing brace" {
    const gpa = std.testing.allocator;
    try std.testing.expectError(error.MissingClosingBrace, parseTemplate(gpa, "VOLT {voltage} {"));
}

test "parse template error: empty argument" {
    const gpa = std.testing.allocator;
    try std.testing.expectError(error.EmptyArgument, parseTemplate(gpa, "VOLT {} done"));
}

test "parse template error: invalid identifier" {
    const gpa = std.testing.allocator;
    try std.testing.expectError(error.InvalidIdentifier, parseTemplate(gpa, "VOLT {123bad}"));
}
