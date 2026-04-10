const std = @import("std");

/// Lightweight context used while building diagnostics during precompile.
pub const DiagnosticContext = struct {
    task_idx: ?usize = null,
    step_idx: ?usize = null,
    instrument_name: ?[]const u8 = null,
    adapter_name: ?[]const u8 = null,
    command_name: ?[]const u8 = null,
    argument_name: ?[]const u8 = null,
    argument_spec: ?[]const u8 = null,
};

/// Context captured for the most recent precompile failure.
pub const PrecompileDiagnostic = struct {
    allocator: std.mem.Allocator,
    task_idx: ?usize = null,
    step_idx: ?usize = null,
    instrument_name: ?[]u8 = null,
    adapter_name: ?[]u8 = null,
    command_name: ?[]u8 = null,
    argument_name: ?[]u8 = null,
    argument_spec: ?[]u8 = null,

    /// Creates an empty diagnostic object owned by `allocator`.
    pub fn init(allocator: std.mem.Allocator) PrecompileDiagnostic {
        return .{ .allocator = allocator };
    }

    /// Releases any captured diagnostic strings.
    pub fn deinit(self: *PrecompileDiagnostic) void {
        self.reset();
    }

    /// Clears all currently captured diagnostic fields.
    pub fn reset(self: *PrecompileDiagnostic) void {
        self.freeTextField(&self.instrument_name);
        self.freeTextField(&self.adapter_name);
        self.freeTextField(&self.command_name);
        self.freeTextField(&self.argument_name);
        self.freeTextField(&self.argument_spec);
        self.task_idx = null;
        self.step_idx = null;
    }

    /// Replaces the current diagnostic state with a new contextual snapshot.
    pub fn capture(self: *PrecompileDiagnostic, context: DiagnosticContext) !void {
        self.reset();
        self.task_idx = context.task_idx;
        self.step_idx = context.step_idx;
        try self.setTextField(&self.instrument_name, context.instrument_name);
        try self.setTextField(&self.adapter_name, context.adapter_name);
        try self.setTextField(&self.command_name, context.command_name);
        try self.setTextField(&self.argument_name, context.argument_name);
        try self.setTextField(&self.argument_spec, context.argument_spec);
    }

    /// Writes a human-readable diagnostic line for a precompile error.
    pub fn write(self: *const PrecompileDiagnostic, writer: *std.Io.Writer, err: anyerror) !void {
        try writer.writeAll("precompile failed");
        if (self.task_idx) |task_idx| {
            if (self.step_idx) |step_idx| {
                try writer.print(" at task {d} step {d}", .{ task_idx, step_idx });
            } else {
                try writer.print(" at task {d}", .{task_idx});
            }
        }
        if (self.instrument_name) |instrument_name| {
            try writer.print(" instrument={s}", .{instrument_name});
        }
        if (self.adapter_name) |adapter_name| {
            try writer.print(" adapter={s}", .{adapter_name});
        }
        if (self.command_name) |command_name| {
            try writer.print(" command={s}", .{command_name});
        }
        if (self.argument_name) |argument_name| {
            try writer.print(" arg={s}", .{argument_name});
        }
        if (self.argument_spec) |argument_spec| {
            try writer.print(" arg_spec={s}", .{argument_spec});
        }
        try writer.writeAll(": ");

        switch (err) {
            error.AdapterNotFound => try writer.writeAll("adapter not found"),
            error.InstrumentNotFound => try writer.writeAll("instrument not declared in recipe"),
            error.CommandNotFound => try writer.writeAll("command not found in adapter"),
            error.MissingCommandArgument => try writer.writeAll("missing required command argument"),
            error.UnexpectedCommandArgument => try writer.writeAll("unexpected command argument"),
            error.InvalidArgument => try writer.writeAll("invalid step argument syntax"),
            error.MissingTaskInterval => try writer.writeAll("task is missing every/every_ms interval"),
            error.InvalidDuration => try writer.writeAll("invalid duration string"),
            error.MissingPipeline => try writer.writeAll("recipe is missing required 'pipeline' section"),
            error.MissingRecordConfig => try writer.writeAll("pipeline is missing required 'record' field"),
            error.InvalidPipelineConfig => try writer.writeAll("invalid pipeline configuration"),
            error.InvalidRecordConfig => try writer.writeAll("pipeline record must be \"all\" or an array of variable names"),
            error.RecordVariableNotFound => try writer.writeAll("pipeline record references unknown save_as variable"),
            error.UndeclaredVariable => try writer.writeAll("variable used but not declared in recipe 'vars' section"),
            else => try writer.print("{s}", .{@errorName(err)}),
        }
        try writer.writeByte('\n');
    }

    /// Stores an optional text field as allocator-owned memory.
    fn setTextField(self: *PrecompileDiagnostic, field: *?[]u8, value: ?[]const u8) !void {
        if (value) |text| {
            field.* = try self.allocator.dupe(u8, text);
        } else {
            field.* = null;
        }
    }

    /// Frees one optional allocator-owned text field.
    fn freeTextField(self: *PrecompileDiagnostic, field: *?[]u8) void {
        if (field.*) |text| {
            self.allocator.free(text);
            field.* = null;
        }
    }
};
