/// Utilities for parsing and rendering command templates, e.g. "VOLT {voltage:float} {channels:list}".
/// The template is parsed into segments, which are either literal strings or placeholders.
const std = @import("std");
const diagnostic = @import("../diagnostic.zig");

pub const Placeholder = struct {
    name: []const u8,
    arg_type: []const u8,
};

/// A command template segment: literal, placeholder, or optional group.
pub const Segment = union(enum) {
    /// A literal string segment that should be included as-is in the rendered command.
    literal: []const u8,

    /// A placeholder name and optional type annotation to be replaced when rendering.
    placeholder: Placeholder,

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
    // Conservative upper bound: typed placeholders are longer than the old
    // `{x}` form, so this overestimates segment count for modern templates.
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
                const colon_idx = std.mem.indexOfScalar(u8, inner, ':') orelse {
                    return reporter.fail(.{
                        .start = source_offset + i + 1,
                        .end = source_offset + close_idx,
                    }, .missing_argument_type);
                };
                const name = std.mem.trim(u8, inner[0..colon_idx], " \t\r\n");
                if (!isIdentifier(name)) {
                    return reporter.fail(.{
                        .start = source_offset + i + 1,
                        .end = source_offset + close_idx,
                    }, .{ .invalid_identifier = .{ .identifier = name } });
                }
                const type_name = std.mem.trim(u8, inner[colon_idx + 1 ..], " \t\r\n");
                if (!isIdentifier(type_name)) {
                    return reporter.fail(.{
                        .start = source_offset + i + 1 + colon_idx + 1,
                        .end = source_offset + close_idx,
                    }, .{ .invalid_argument_type = .{ .arg_type = type_name } });
                }
                try segments.append(allocator, .{ .placeholder = .{
                    .name = try allocator.dupe(u8, name),
                    .arg_type = try allocator.dupe(u8, type_name),
                } });

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
        .placeholder => |p| {
            allocator.free(p.name);
            allocator.free(p.arg_type);
        },
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

    const segments = try parseTemplate(gpa, "VOLT {voltage:float} {channels:list}", diagnostics.reporter());
    defer freeSegments(gpa, segments);
    try std.testing.expectEqual(@as(usize, 4), segments.len);
}

test "parse typed template placeholder" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    const segments = try parseTemplate(gpa, "VOLT { voltage : float }", diagnostics.reporter());
    defer freeSegments(gpa, segments);
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    switch (segments[1]) {
        .placeholder => |placeholder| {
            try std.testing.expectEqualStrings("voltage", placeholder.name);
            try std.testing.expectEqualStrings("float", placeholder.arg_type);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse template error: missing closing brace" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {voltage:float} {", diagnostics.reporter()));
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

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {123bad:float}", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqualStrings("123bad", diagnostics.items.items[0].message.invalid_identifier.identifier);
}

test "parse template error: missing argument type" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {voltage}", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.missing_argument_type, diagnostics.items.items[0].message);
}

test "parse template error: invalid argument type" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "VOLT {voltage:}", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqualStrings("", diagnostics.items.items[0].message.invalid_argument_type.arg_type);
}

test "parse template with optional group" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    const segments = try parseTemplate(gpa, "OUTP {state:bool}[,(@{channels:list})]", diagnostics.reporter());
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

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "OUTP [,(@{ch:list})", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.missing_closing_bracket, diagnostics.items.items[0].message);
}

test "parse template error: nested optional group" {
    const gpa = std.testing.allocator;
    var diagnostics = diagnostic.Diagnostics.init(gpa, "<test>");
    defer diagnostics.deinit();

    try std.testing.expectError(error.AnalysisFail, parseTemplate(gpa, "OUTP [[{a:string}]]", diagnostics.reporter()));
    try std.testing.expectEqual(@as(usize, 1), diagnostics.items.items.len);
    try std.testing.expectEqual(diagnostic.Message.nested_optional_group, diagnostics.items.items[0].message);
}
