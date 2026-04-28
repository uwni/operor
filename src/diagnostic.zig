const std = @import("std");
const tty = @import("tty.zig");

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
    record_variable_not_found: struct { variable: []const u8 },
    unknown_variable: struct { variable: []const u8 },
    assign_to_const: struct { variable: []const u8 },
    builtin_variable_conflict: struct { variable: []const u8 },
    duplicate_variable: struct { variable: []const u8 },
    duplicate_record_column: struct { column: []const u8 },
    invalid_expression,
    division_by_zero,

    expected_expression,
    expected_variable,
    expected_token: []const u8,
    unexpected_token,
    invalid_number: []const u8,
    unknown_function: []const u8,
    unterminated_string,
    unbound_variable,
    const_runtime_value,
    negative_list_index: i64,
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
    invalid_identifier: []const u8,
    invalid_read_type: []const u8,
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
            .warning => tty.styledText("warning: ", .{.yellow}),
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
        .file_not_found => try writer.writeAll("file not found"),
        .syntax_error => try writer.writeAll("invalid configuration syntax"),
        .unsupported_format => try writer.writeAll("unsupported configuration file format"),
        .wrong_type => try writer.writeAll("invalid configuration value type"),
        .partial_bool_map => try writer.writeAll("bool argument map must define both true and false"),
        .missing_pipeline => try writer.writeAll("recipe is missing required 'pipeline' section"),
        .missing_record_config => try writer.writeAll("pipeline is missing required 'record' field"),
        .invalid_pipeline_config => try writer.writeAll("invalid pipeline configuration"),
        .nested_parallel_step => try writer.writeAll("parallel steps cannot be nested"),
        .duplicate_parallel_instrument => |p| try writer.print("parallel steps cannot use instrument '{s}' more than once", .{p.instrument}),
        .adapter_not_found => try writer.writeAll("adapter not found"),
        .instrument_not_found => |p| try writer.print("instrument '{s}' is not declared in recipe", .{p.instrument}),
        .command_not_found => |p| try writer.print("command not found in adapter: '{s}.{s}'", .{ p.instrument, p.command }),
        .invalid_call_format => |p| try writer.print("call '{s}' must use instrument.command format", .{p.call}),
        .missing_command_argument => |p| try writer.print("missing required command argument '{s}'", .{p.argument}),
        .unexpected_command_argument => |p| try writer.print("unexpected command argument '{s}'", .{p.argument}),
        .record_variable_not_found => |p| try writer.print("pipeline record references variable '{s}' not assigned by any step", .{p.variable}),
        .unknown_variable => |p| try writer.print("variable '{s}' is not declared in recipe 'vars' section", .{p.variable}),
        .assign_to_const => |p| try writer.print("cannot assign to const variable '{s}'", .{p.variable}),
        .builtin_variable_conflict => |p| try writer.print("variable name '{s}' conflicts with a built-in variable", .{p.variable}),
        .duplicate_variable => |p| try writer.print("const and var sections both define variable '{s}'", .{p.variable}),
        .duplicate_record_column => |p| try writer.print("pipeline record lists duplicate column '{s}'", .{p.column}),
        .invalid_expression => try writer.writeAll("invalid expression"),
        .division_by_zero => try writer.writeAll("division by zero"),
        .expected_expression => try writer.writeAll("expected expression"),
        .expected_variable => try writer.writeAll("expected variable reference"),
        .expected_token => |token| try writer.print("expected '{s}'", .{token}),
        .unexpected_token => try writer.writeAll("unexpected token"),
        .invalid_number => |text| try writer.print("invalid number '{s}'", .{text}),
        .unknown_function => |name| try writer.print("unknown function '{s}'", .{name}),
        .unterminated_string => try writer.writeAll("unterminated string literal"),
        .unbound_variable => try writer.writeAll("expression variable was not bound before lowering"),
        .const_runtime_value => try writer.writeAll("const list value cannot be used with a runtime-dependent operation"),
        .negative_list_index => |index| try writer.print("list index {d} is negative", .{index}),
        .list_index_out_of_bounds => |p| try writer.print("list index {d} is out of bounds for list of length {d}", .{ p.index, p.len }),
        .nested_list_value => try writer.writeAll("nested list value is not valid in this expression"),
        .invalid_stack_shape => try writer.writeAll("expression lowered to invalid bytecode"),
        .stack_too_deep => try writer.writeAll("expression stack is too deep"),
        .missing_closing_brace => try writer.writeAll("missing closing '}'"),
        .missing_closing_bracket => try writer.writeAll("missing closing ']'"),
        .nested_optional_group => try writer.writeAll("optional groups cannot be nested"),
        .empty_argument => try writer.writeAll("template placeholder cannot be empty"),
        .invalid_identifier => |name| try writer.print("template placeholder '{s}' is not a valid identifier", .{name}),
        .invalid_read_type => |tag| try writer.print("read type '{s}' is not supported", .{tag}),
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
