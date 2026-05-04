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
    object_assign_to_var: struct { variable: []const u8 },
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
    missing_list_separator,
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
    writer: ?*std.Io.Writer,
    file_path: []const u8,
    count: usize = 0,

    pub fn init(writer: ?*std.Io.Writer, file_path: []const u8) Diagnostics {
        return .{ .writer = writer, .file_path = file_path };
    }

    pub fn deinit(_: *Diagnostics) void {}

    pub fn reporter(self: *Diagnostics) Reporter {
        return .{
            .diagnostics = @ptrCast(self),
            .vtable = &reporter_vtable,
        };
    }

    pub fn add(self: *Diagnostics, d: Diagnostic) error{OutOfMemory}!void {
        self.count += 1;
        const w = self.writer orelse return;
        self.writeItem(w, d) catch {};
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

    fn reportAdd(ctx: *anyopaque, d: Diagnostic) error{OutOfMemory}!void {
        const self: *Diagnostics = @ptrCast(@alignCast(ctx));
        try self.add(d);
    }
};

fn writeMessage(writer: *std.Io.Writer, item: Diagnostic) !void {
    // Comptime array: indexed by SourceKind ordinal, null means no prefix.
    const source_prefix = comptime std.enums.directEnumArray(SourceKind, ?[]const u8, 0, .{
        .recipe_document = null,
        .adapter_document = null,
        .expression = "invalid expression",
        .argument_expression = "invalid argument expression",
        .adapter_write_template = "invalid adapter write template",
        .adapter_read_type = "invalid adapter read type",
    });

    var has_source_prefix = false;
    if (item.source_kind) |sk| {
        if (source_prefix[@intFromEnum(sk)]) |prefix| {
            try writer.writeAll(prefix);
            has_source_prefix = true;
        }
    }
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

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    var diagnostics = Diagnostics.init(&out.writer, "new.yaml");

    try diagnostics.add(.{
        .severity = .fatal,
        .source_kind = .recipe_document,
        .message = .{ .file_not_found = {} },
    });

    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "'new.yaml': file not found\n"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, out.written(), 1, ": :"));
}

test "diagnostic message payloads are underlined" {
    const gpa = std.testing.allocator;

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    var diagnostics = Diagnostics.init(&out.writer, "recipe.yaml");

    try diagnostics.add(.{
        .severity = .fatal,
        .message = .{ .duplicate_parallel_instrument = .{ .instrument = "smu" } },
    });

    try std.testing.expect(std.mem.containsAtLeast(
        u8,
        out.written(),
        1,
        "parallel steps cannot use instrument '\x1b[4msmu\x1b[0m' more than once\n",
    ));
}

test "diagnostic escaped braces are not styled as placeholders" {
    const gpa = std.testing.allocator;

    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    var diagnostics = Diagnostics.init(&out.writer, "recipe.yaml");

    try diagnostics.add(.{
        .severity = .fatal,
        .message = .{ .missing_closing_brace = {} },
    });

    try std.testing.expect(std.mem.containsAtLeast(u8, out.written(), 1, "'recipe.yaml': missing closing '}'\n"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, out.written(), 1, "\x1b[4m"));
}

test "null writer discards output" {
    var diagnostics = Diagnostics.init(null, "");
    const reporter = diagnostics.reporter();

    try reporter.add(.warning, null, .{ .duplicate_record_column = .{ .column = "v" } });
    try std.testing.expectEqual(error.AnalysisFail, reporter.fail(null, .{ .missing_pipeline = {} }));

    try std.testing.expectEqual(@as(usize, 2), diagnostics.count);
}
