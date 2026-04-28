/// Utilities for parsing and rendering command templates, e.g. "VOLT {voltage} {channels}".
/// The template is parsed into segments, which are either literal strings or placeholders.
const std = @import("std");
const diagnostic = @import("../diagnostic.zig");

/// A command template segment: literal, placeholder, or optional group.
pub const Segment = union(enum) {
    /// A literal string segment that should be included as-is in the rendered command.
    literal: []const u8,

    /// A placeholder name (identifier without braces) to be replaced when rendering.
    placeholder: []const u8,

    /// An optional group: rendered only when at least one inner placeholder is non-empty.
    optional: []const Segment,
};

/// Parse a command template string into owned segments of literals and placeholders.
pub fn parseTemplate(
    allocator: std.mem.Allocator,
    source: []const u8,
    reporter: diagnostic.Reporter,
) diagnostic.Error![]Segment {
    return parseTemplateInner(allocator, source, source, 0, false, reporter.withSource(.adapter_write_template, source));
}

fn parseTemplateInner(
    allocator: std.mem.Allocator,
    source: []const u8,
    tem_str: []const u8,
    source_offset: usize,
    in_optional: bool,
    reporter: diagnostic.Reporter,
) diagnostic.Error![]Segment {
    // Upper bound: the densest packing alternates single-char literals with
    // minimal placeholders `{x}`, e.g. `a{b}c{d}e` → 9 bytes, 5 segments.
    // Each segment consumes at least 2 bytes of input (1-char literal or `{x}`),
    // so the maximum number of segments is ⌈len / 2⌉ = (len + 1) / 2.
    const max_segments = (tem_str.len + 1) / 2;
    var segments: std.ArrayList(Segment) = try .initCapacity(allocator, max_segments);
    errdefer {
        freeSegmentList(allocator, segments.items);
        segments.deinit(allocator);
    }

    var i: usize = 0;
    var literal_start: usize = 0;

    while (i < tem_str.len) : (i += 1) {
        switch (tem_str[i]) {
            '[' => {
                if (in_optional) return reporter.fail(.at(source_offset + i), .nested_optional_group);

                if (i > literal_start) {
                    try segments.append(allocator, .{ .literal = try allocator.dupe(u8, tem_str[literal_start..i]) });
                }

                var depth: usize = 1;
                var close_idx = i + 1;
                while (close_idx < tem_str.len) : (close_idx += 1) {
                    if (tem_str[close_idx] == '[') depth += 1;
                    if (tem_str[close_idx] == ']') {
                        depth -= 1;
                        if (depth == 0) break;
                    }
                }
                if (depth != 0) {
                    return reporter.fail(.{
                        .start = source_offset + i,
                        .end = source.len,
                    }, .missing_closing_bracket);
                }

                const inner_segments = try parseTemplateInner(
                    allocator,
                    source,
                    tem_str[i + 1 .. close_idx],
                    source_offset + i + 1,
                    true,
                    reporter,
                );
                try segments.append(allocator, .{ .optional = inner_segments });

                i = close_idx;
                literal_start = close_idx + 1;
            },
            '{' => {
                if (i > literal_start) {
                    try segments.append(allocator, .{ .literal = try allocator.dupe(u8, tem_str[literal_start..i]) });
                }

                var close_idx = i + 1;
                while (close_idx < tem_str.len and tem_str[close_idx] != '}') : (close_idx += 1) {}
                if (close_idx == tem_str.len) {
                    return reporter.fail(.{
                        .start = source_offset + i,
                        .end = source.len,
                    }, .missing_closing_brace);
                }

                const inner = std.mem.trim(u8, tem_str[i + 1 .. close_idx], " \t\r\n");
                if (inner.len == 0) {
                    return reporter.fail(.{
                        .start = source_offset + i + 1,
                        .end = source_offset + close_idx,
                    }, .empty_argument);
                }
                if (!isIdentifier(inner)) {
                    return reporter.fail(.{
                        .start = source_offset + i + 1,
                        .end = source_offset + close_idx,
                    }, .{ .invalid_identifier = inner });
                }
                try segments.append(allocator, .{ .placeholder = try allocator.dupe(u8, inner) });

                i = close_idx;
                literal_start = close_idx + 1;
            },
            else => {},
        }
    }

    if (literal_start < tem_str.len) {
        try segments.append(allocator, .{ .literal = try allocator.dupe(u8, tem_str[literal_start..]) });
    }

    return segments.toOwnedSlice(allocator);
}

/// Frees all owned data within segments, then the slice itself.
pub fn freeSegments(allocator: std.mem.Allocator, segments: []const Segment) void {
    freeSegmentList(allocator, segments);
    allocator.free(segments);
}

fn freeSegmentList(allocator: std.mem.Allocator, segments: []const Segment) void {
    for (segments) |seg| switch (seg) {
        .literal => |s| allocator.free(s),
        .placeholder => |p| allocator.free(p),
        .optional => |inner| freeSegments(allocator, inner),
    };
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
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    const segments = try parseTemplate(gpa, "VOLT {voltage} {channels}", diagnostics.reporter());
    defer freeSegments(gpa, segments);
    try std.testing.expectEqual(@as(usize, 4), segments.len);
}

test "parse template error: missing closing brace" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {voltage} {", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.missing_closing_brace, diagnostics.items.items[0].message);
}

test "parse template error: empty argument" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {} done", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.empty_argument, diagnostics.items.items[0].message);
}

test "parse template error: invalid identifier" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {123bad}", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqualStrings("123bad", diagnostics.items.items[0].message.invalid_identifier);
}

test "parse template with optional group" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    const segments = try parseTemplate(gpa, "OUTP {state}[,(@{channels})]", diagnostics.reporter());
    defer freeSegments(gpa, segments);
    // "OUTP ", {state}, optional[",(@", {channels}, ")"]
    try std.testing.expectEqual(@as(usize, 3), segments.len);
    try std.testing.expect(segments[2] == .optional);
    try std.testing.expectEqual(@as(usize, 3), segments[2].optional.len);
}

test "parse template error: missing closing bracket" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "OUTP [,(@{ch})", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.missing_closing_bracket, diagnostics.items.items[0].message);
}

test "parse template error: nested optional group" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "OUTP [[{a}]]", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.nested_optional_group, diagnostics.items.items[0].message);
}
