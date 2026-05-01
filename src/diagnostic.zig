const std = @import("std");
const tty = @import("tty.zig");
const message_formats = @import("diagnostic_messages.zon");

pub const Span = struct {
    start: usize,
    end: usize,

    pub fn at(pos: usize) Span {
        return .{ .start = pos, .end = pos };
    }

    pub fn cover(lhs: Span, rhs: Span) Span {
        return .{ .start = lhs.start, .end = rhs.end };
    }
};

pub const Context = struct {
    task_idx: ?usize = null,
    step_idx: ?usize = null,
    instrument_name: ?[]const u8 = null,
    adapter_name: ?[]const u8 = null,
    command_name: ?[]const u8 = null,
    argument_name: ?[]const u8 = null,
    variable_name: ?[]const u8 = null,
};

pub const Severity = enum {
    warning,
    fatal,
};

pub const Error = error{
    AnalysisFail,
    OutOfMemory,
};

pub const SourceKind = enum {
    recipe_document,
    adapter_document,
    expression,
    argument_expression,
    adapter_write_template,
    adapter_read_type,
};

pub const Message = union(enum) {
    file_not_found,
    syntax_error,
    unsupported_format,
    wrong_type,
    partial_bool_map,
    missing_pipeline,
    missing_record_config,
    invalid_pipeline_config,
    nested_parallel_step,
    duplicate_parallel_instrument: struct { instrument: []const u8 },
    adapter_not_found,
    instrument_not_found: struct { instrument: []const u8 },
    command_not_found: struct {
        instrument: []const u8,
        command: []const u8,
    },
    invalid_call_format: struct { call: []const u8 },
    missing_command_argument: struct { argument: []const u8 },
    unexpected_command_argument: struct { argument: []const u8 },
    record_const_not_recordable: struct { variable: []const u8 },
    unknown_variable: struct { variable: []const u8 },
    assign_to_const: struct { variable: []const u8 },
    builtin_variable_conflict: struct { variable: []const u8 },
    duplicate_variable: struct { variable: []const u8 },
    duplicate_record_column: struct { column: []const u8 },
    invalid_expression,
    division_by_zero,

    expected_expression,
    expected_variable,
    expected_token: struct { token: []const u8 },
    unexpected_token,
    invalid_number: struct { number: []const u8 },
    unknown_function: struct { name: []const u8 },
    unterminated_string,
    unbound_variable,
    const_runtime_value,
    negative_list_index: struct { index: i64 },
    list_index_out_of_bounds: struct {
        index: i64,
        len: usize,
    },
    nested_list_value,
    invalid_stack_shape,
    stack_too_deep,

    missing_closing_brace,
    missing_closing_bracket,
    nested_optional_group,
    empty_argument,
    missing_argument_type,
    invalid_identifier: struct { identifier: []const u8 },
    invalid_argument_type: struct { arg_type: []const u8 },
    invalid_read_type: struct { read_type: []const u8 },
    missing_option_values,
    invalid_option_value,
    conflicting_argument_type,
};

pub const Diagnostic = struct {
    severity: Severity,
    context: Context = .{},
    source_kind: ?SourceKind = null,
    source: ?[]const u8 = null,
    span: ?Span = null,
    message: Message,
};

pub const Reporter = struct {
    diagnostics: *anyopaque,
    vtable: *const VTable,
    context: Context = .{},
    source_kind: ?SourceKind = null,
    source: ?[]const u8 = null,

    pub const VTable = struct {
        add: *const fn (*anyopaque, Diagnostic) error{OutOfMemory}!void,
    };

    pub fn withContext(self: Reporter, context: Context) Reporter {
        var out = self;
        out.context = context;
        return out;
    }

    pub fn withSource(self: Reporter, source_kind: SourceKind, source: []const u8) Reporter {
        var out = self;
        out.source_kind = source_kind;
        out.source = source;
        return out;
    }

    pub fn withSourceKind(self: Reporter, source_kind: SourceKind) Reporter {
        var out = self;
        out.source_kind = source_kind;
        out.source = null;
        return out;
    }

    pub fn add(self: Reporter, severity: Severity, span: ?Span, message: Message) error{OutOfMemory}!void {
        try self.vtable.add(self.diagnostics, .{
            .severity = severity,
            .context = self.context,
            .source_kind = self.source_kind,
            .source = self.source,
            .span = span,
            .message = message,
        });
    }

    pub fn fail(self: Reporter, span: ?Span, message: Message) Error {
        self.add(.fatal, span, message) catch |err| return err;
        return error.AnalysisFail;
    }

    pub fn warn(self: Reporter, span: ?Span, message: Message) error{OutOfMemory}!void {
        try self.add(.warning, span, message);
    }
};

pub const Diagnostics = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    items: std.ArrayList(Diagnostic) = .empty,

    /// Diagnostic payload slices are borrowed and must outlive writeAll.
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) Diagnostics {
        return .{
            .allocator = allocator,
            .file_path = file_path,
        };
    }

    pub fn deinit(self: *Diagnostics) void {
        self.items.deinit(self.allocator);
    }

    pub fn reporter(self: *Diagnostics) Reporter {
        return .{
            .diagnostics = @ptrCast(self),
            .vtable = &reporter_vtable,
        };
    }

    pub fn add(self: *Diagnostics, diagnostic: Diagnostic) error{OutOfMemory}!void {
        try self.items.append(self.allocator, diagnostic);
    }

    pub fn writeAll(self: *const Diagnostics, writer: *std.Io.Writer) !void {
        for (self.items.items) |item| {
            try self.writeItem(writer, item);
        }
    }

    fn writeItem(self: *const Diagnostics, writer: *std.Io.Writer, item: Diagnostic) !void {
        try writer.writeAll(switch (item.severity) {
            .fatal => tty.error_prefix,
            .warning => comptime tty.styledText("warning: ", .{.yellow}),
        });
        try writer.print("'{s}'", .{self.file_path});
        if (item.context.task_idx) |task_idx| {
            if (item.context.step_idx) |step_idx| {
                try writer.print(" at task {d} step {d}", .{ task_idx, step_idx });
            } else {
                try writer.print(" at task {d}", .{task_idx});
            }
        }
        if (item.context.instrument_name) |instrument_name| {
            try writer.print(" instrument={s}", .{instrument_name});
        }
        if (item.context.adapter_name) |adapter_name| {
            try writer.print(" adapter={s}", .{adapter_name});
        }
        if (item.context.command_name) |command_name| {
            try writer.print(" command={s}", .{command_name});
        }
        if (item.context.argument_name) |argument_name| {
            try writer.print(" arg={s}", .{argument_name});
        }
        if (item.context.variable_name) |variable_name| {
            try writer.print(" var={s}", .{variable_name});
        }
        try writer.writeAll(": ");
        try writeMessage(writer, item);
        try writer.writeByte('\n');
    }

    const reporter_vtable = Reporter.VTable{ .add = reportAdd };

    fn reportAdd(ctx: *anyopaque, diagnostic: Diagnostic) error{OutOfMemory}!void {
        const self: *Diagnostics = @ptrCast(@alignCast(ctx));
        try self.add(diagnostic);
    }
};

pub const EmptyDiagnostics = struct {
    pub fn init() EmptyDiagnostics {
        return .{};
    }

    pub fn deinit(_: *EmptyDiagnostics) void {}

    pub fn reporter(self: *EmptyDiagnostics) Reporter {
        return .{
            .diagnostics = @ptrCast(self),
            .vtable = &reporter_vtable,
        };
    }

    pub fn writeAll(_: *const EmptyDiagnostics, _: *std.Io.Writer) !void {}

    const reporter_vtable = Reporter.VTable{ .add = reportAdd };

    fn reportAdd(_: *anyopaque, _: Diagnostic) error{OutOfMemory}!void {}
};

fn writeMessage(writer: *std.Io.Writer, item: Diagnostic) !void {
    var has_source_prefix = false;
    if (item.source_kind) |source_kind| switch (source_kind) {
        .expression => {
            try writer.writeAll("invalid expression");
            has_source_prefix = true;
        },
        .argument_expression => {
            try writer.writeAll("invalid argument expression");
            has_source_prefix = true;
        },
        .adapter_write_template => {
            try writer.writeAll("invalid adapter write template");
            has_source_prefix = true;
        },
        .adapter_read_type => {
            try writer.writeAll("invalid adapter read type");
            has_source_prefix = true;
        },
        .recipe_document, .adapter_document => {},
    };
    if (item.source) |source| {
        if (has_source_prefix) try writer.writeByte(' ');
        try writer.print("'{s}'", .{source});
        has_source_prefix = true;
    }
    if (item.span) |span| {
        if (item.source == null or span.start <= item.source.?.len) {
            if (has_source_prefix) try writer.writeByte(' ');
            try writer.print("at byte {d}", .{span.start});
            has_source_prefix = true;
        }
    }
    if (has_source_prefix) {
        try writer.writeAll(": ");
    }

    switch (item.message) {
        inline else => |payload, tag| {
            const args = switch (@typeInfo(@TypeOf(payload))) {
                .void => .{},
                .@"struct" => payload,
                else => @compileError("diagnostic message payloads must be void or struct"),
            };
            const styled_fmt = comptime tty.styledPlaceholders(@field(message_formats, @tagName(tag)), .{.underline});
            try writer.print(styled_fmt, args);
        },
    }
}

test "document diagnostics do not add duplicate source separator" {
    const gpa = std.testing.allocator;

    var diagnostics = Diagnostics.init(gpa, "new.yaml");
    defer diagnostics.deinit();

    try diagnostics.add(.{
        .severity = .fatal,
        .source_kind = .recipe_document,
        .message = .{ .file_not_found = {} },
    });

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try diagnostics.writeAll(&out.writer);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "'new.yaml': file not found\n"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, out.written(), 1, ": :"));
}

test "diagnostic message payloads are underlined" {
    const gpa = std.testing.allocator;

    var diagnostics = Diagnostics.init(gpa, "recipe.yaml");
    defer diagnostics.deinit();

    try diagnostics.add(.{
        .severity = .fatal,
        .message = .{ .duplicate_parallel_instrument = .{ .instrument = "smu" } },
    });

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try diagnostics.writeAll(&out.writer);
    try std.testing.expect(std.mem.containsAtLeast(
        u8,
        out.written(),
        1,
        "parallel steps cannot use instrument '\x1b[4msmu\x1b[0m' more than once\n",
    ));
}

test "diagnostic escaped braces are not styled as placeholders" {
    const gpa = std.testing.allocator;

    var diagnostics = Diagnostics.init(gpa, "recipe.yaml");
    defer diagnostics.deinit();

    try diagnostics.add(.{
        .severity = .fatal,
        .message = .{ .missing_closing_brace = {} },
    });

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try diagnostics.writeAll(&out.writer);
    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "'recipe.yaml': missing closing '}'\n"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, out.written(), 1, "\x1b[4m"));
}

test "empty diagnostics has no output" {
    const gpa = std.testing.allocator;

    var diagnostics = EmptyDiagnostics.init();
    defer diagnostics.deinit();
    const reporter = diagnostics.reporter();

    try reporter.add(.warning, null, .{ .duplicate_record_column = .{ .column = "v" } });
    try std.testing.expectEqual(error.AnalysisFail, reporter.fail(null, .{ .missing_pipeline = {} }));

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();

    try diagnostics.writeAll(&out.writer);
    try std.testing.expectEqual(@as(usize, 0), out.written().len);
}
