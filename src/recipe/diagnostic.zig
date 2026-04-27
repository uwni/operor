const std = @import("std");
const tty = @import("../tty.zig");

pub const DiagnosticContext = struct {
    task_idx: ?usize = null,
    step_idx: ?usize = null,
    instrument_name: ?[]const u8 = null,
    adapter_name: ?[]const u8 = null,
    command_name: ?[]const u8 = null,
    argument_name: ?[]const u8 = null,
    argument_spec: ?[]const u8 = null,
    variable_name: ?[]const u8 = null,
};

pub const Severity = enum {
    warning,
    fatal,
};

const DiagnosticItem = struct {
    severity: Severity,
    context: DiagnosticContext = .{},
    message: []const u8,

    fn deinit(self: *DiagnosticItem, allocator: std.mem.Allocator) void {
        inline for (.{
            &self.context.instrument_name,
            &self.context.adapter_name,
            &self.context.command_name,
            &self.context.argument_name,
            &self.context.argument_spec,
            &self.context.variable_name,
        }) |field| {
            if (field.*) |s| allocator.free(@constCast(s));
        }
        allocator.free(@constCast(self.message));
    }
};

/// Accumulates user-facing precompile diagnostics.
pub const PrecompileDiagnostic = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    items: std.ArrayList(DiagnosticItem) = .empty,

    /// Creates an empty diagnostic accumulator owned by `allocator`.
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) PrecompileDiagnostic {
        return .{ .allocator = allocator, .file_path = file_path };
    }

    /// Releases all captured diagnostic strings.
    pub fn deinit(self: *PrecompileDiagnostic) void {
        self.freeItems();
        self.items.deinit(self.allocator);
    }

    /// Clears all accumulated diagnostics.
    pub fn reset(self: *PrecompileDiagnostic) void {
        self.freeItems();
        self.items.clearRetainingCapacity();
    }

    pub fn add(
        self: *PrecompileDiagnostic,
        severity: Severity,
        context: DiagnosticContext,
        comptime issue: @EnumLiteral(),
        args: anytype,
    ) !void {
        var item: DiagnosticItem = .{
            .severity = severity,
            .context = try self.dupeContext(context),
            .message = try self.renderIssue(issue, args),
        };
        errdefer item.deinit(self.allocator);

        try self.items.append(self.allocator, item);
    }

    pub fn hasWarnings(self: *const PrecompileDiagnostic) bool {
        for (self.items.items) |item| {
            if (item.severity == .warning) return true;
        }
        return false;
    }

    pub fn hasFatal(self: *const PrecompileDiagnostic) bool {
        for (self.items.items) |item| {
            if (item.severity == .fatal) return true;
        }
        return false;
    }

    pub fn writeAll(self: *const PrecompileDiagnostic, writer: *std.Io.Writer) !void {
        for (self.items.items) |item| {
            try self.writeItem(writer, item);
        }
    }

    fn dupeContext(self: *PrecompileDiagnostic, context: DiagnosticContext) !DiagnosticContext {
        return .{
            .task_idx = context.task_idx,
            .step_idx = context.step_idx,
            .instrument_name = try self.dupeOptional(context.instrument_name),
            .adapter_name = try self.dupeOptional(context.adapter_name),
            .command_name = try self.dupeOptional(context.command_name),
            .argument_name = try self.dupeOptional(context.argument_name),
            .argument_spec = try self.dupeOptional(context.argument_spec),
            .variable_name = try self.dupeOptional(context.variable_name),
        };
    }

    fn dupeOptional(self: *PrecompileDiagnostic, value: ?[]const u8) !?[]const u8 {
        const s = value orelse return null;
        return try self.allocator.dupe(u8, s);
    }

    fn renderIssue(self: *PrecompileDiagnostic, comptime issue: @EnumLiteral(), args: anytype) ![]const u8 {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        errdefer out.deinit();
        try writeIssue(&out.writer, issue, args);
        return out.toOwnedSlice() catch error.OutOfMemory;
    }

    fn freeItems(self: *PrecompileDiagnostic) void {
        for (self.items.items) |*item| item.deinit(self.allocator);
    }

    fn writeItem(self: *const PrecompileDiagnostic, writer: *std.Io.Writer, item: DiagnosticItem) !void {
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
        if (item.context.argument_spec) |argument_spec| {
            try writer.print(" arg_spec={s}", .{argument_spec});
        }
        if (item.context.variable_name) |variable_name| {
            try writer.print(" var={s}", .{variable_name});
        }
        try writer.writeAll(": ");
        try writer.writeAll(item.message);
        try writer.writeByte('\n');
    }
};

fn writeIssue(writer: *std.Io.Writer, comptime issue: @EnumLiteral(), args: anytype) !void {
    const fmt = comptime switch (issue) {
        // 纯文本：编译时最终会被优化为 writeAll
        .file_not_found => "file not found",
        .syntax_error => "invalid configuration syntax",
        .unsupported_format => "unsupported configuration file format",
        .wrong_type => "invalid configuration value type",
        .partial_bool_map => "bool argument map must define both true and false",
        .missing_pipeline => "recipe is missing required 'pipeline' section",
        .missing_record_config => "pipeline is missing required 'record' field",
        .invalid_pipeline_config => "invalid pipeline configuration",
        .invalid_record_config => "pipeline record must be \"all\" or an array of variable names",
        .nested_parallel_step => "parallel steps cannot be nested",

        // 带参数的文本：使用 Zig 的命名占位符 {[字段名]}s
        .adapter_not_found => "adapter '{[adapter]s}' not found",
        .invalid_adapter_config => "adapter '{[adapter]s}' has invalid configuration",
        .instrument_not_found => "instrument '{[instrument]s}' is not declared in recipe",
        .command_not_found => "command not found in adapter: '{[instrument]s}.{[command]s}'",
        .invalid_call_format => "call '{[call]s}' must use instrument.command format",
        .missing_command_argument => "missing required command argument '{[argument]s}'",
        .unexpected_command_argument => "unexpected command argument '{[argument]s}'",
        .invalid_argument => "invalid syntax for step argument '{[argument]s}'",
        .invalid_duration => "invalid duration string '{[duration]s}'",
        .record_variable_not_found => "pipeline record references variable '{[variable]s}' not assigned by any step",
        .undeclared_variable => "variable '{[variable]s}' is not declared in recipe 'vars' section",
        .assign_to_const => "cannot assign to const variable '{[variable]s}'",
        .builtin_variable_conflict => "variable name '{[variable]s}' conflicts with a built-in variable",
        .duplicate_variable => "const and var sections both define variable '{[variable]s}'",
        .duplicate_record_column => "pipeline record lists duplicate column '{[column]s}'",
        .invalid_expression => "invalid expression '{[source]s}'",
        else => @compileError("unknown diagnostic issue"),
    };
    try writer.print(fmt, args);
}
