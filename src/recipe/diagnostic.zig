const std = @import("std");
const tty = @import("../tty.zig");

/// Context captured for the most recent precompile failure.
pub const PrecompileDiagnostic = struct {
    allocator: std.mem.Allocator,
    task_idx: ?usize = null,
    step_idx: ?usize = null,
    instrument_name: ?[]const u8 = null,
    adapter_name: ?[]const u8 = null,
    command_name: ?[]const u8 = null,
    argument_name: ?[]const u8 = null,
    argument_spec: ?[]const u8 = null,
    owned: bool = false,

    /// Creates an empty diagnostic object owned by `allocator`.
    pub fn init(allocator: std.mem.Allocator) PrecompileDiagnostic {
        return .{ .allocator = allocator };
    }

    /// Releases any captured diagnostic strings.
    pub fn deinit(self: *PrecompileDiagnostic) void {
        self.release();
    }

    /// Clears all currently captured diagnostic fields.
    pub fn reset(self: *PrecompileDiagnostic) void {
        self.release();
        self.* = .{ .allocator = self.allocator };
    }

    /// Takes ownership of borrowed slices by duplicating them.
    /// Must be called before the source data is freed.
    pub fn snapshot(self: *PrecompileDiagnostic) void {
        if (self.owned) return;
        self.instrument_name = self.dupeOrNull(self.instrument_name);
        self.adapter_name = self.dupeOrNull(self.adapter_name);
        self.command_name = self.dupeOrNull(self.command_name);
        self.argument_name = self.dupeOrNull(self.argument_name);
        self.argument_spec = self.dupeOrNull(self.argument_spec);
        self.owned = true;
    }

    /// Writes a human-readable diagnostic line for a precompile error.
    pub fn write(self: *const PrecompileDiagnostic, writer: *std.Io.Writer, err: anyerror) !void {
        try writer.writeAll(tty.error_prefix);
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
            error.InvalidDuration => try writer.writeAll("invalid duration string"),
            error.MissingPipeline => try writer.writeAll("recipe is missing required 'pipeline' section"),
            error.MissingRecordConfig => try writer.writeAll("pipeline is missing required 'record' field"),
            error.InvalidPipelineConfig => try writer.writeAll("invalid pipeline configuration"),
            error.InvalidRecordConfig => try writer.writeAll("pipeline record must be \"all\" or an array of variable names"),
            error.RecordVariableNotFound => try writer.writeAll("pipeline record references unknown assign variable"),
            error.UndeclaredVariable => try writer.writeAll("variable used but not declared in recipe 'vars' section"),
            error.NestedParallelStep => try writer.writeAll("parallel steps cannot be nested"),
            else => try writer.print("{s}", .{@errorName(err)}),
        }
        try writer.writeByte('\n');
    }

    fn release(self: *PrecompileDiagnostic) void {
        if (!self.owned) return;
        inline for (.{ &self.instrument_name, &self.adapter_name, &self.command_name, &self.argument_name, &self.argument_spec }) |field| {
            if (field.*) |s| self.allocator.free(@constCast(s));
        }
    }

    fn dupeOrNull(self: *PrecompileDiagnostic, value: ?[]const u8) ?[]const u8 {
        return if (value) |s| self.allocator.dupe(u8, s) catch null else null;
    }
};
