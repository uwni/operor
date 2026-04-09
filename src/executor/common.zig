const std = @import("std");
const recipe_types = @import("../recipe/types.zig");
const visa = @import("../visa/root.zig");
const expr = @import("../expr.zig");

/// Runtime options for recipe execution.
pub const ExecOptions = struct {
    /// Directory containing driver documents and the registry cache.
    driver_dir: []const u8,
    /// Path to the recipe document to execute.
    recipe_path: []const u8,
    /// If true, rendered commands are logged instead of being sent to instruments.
    dry_run: bool = true,
    /// Optional maximum runtime in milliseconds; null means run a single pass of all tasks.
    max_duration_ms: ?u64 = null,
    /// Optional runtime override for the ring buffer size.
    pipeline_buffer_size: ?usize = null,
    /// Optional runtime override for the pipeline mode preset.
    pipeline_mode: ?recipe_types.PipelineMode = null,
    /// Optional runtime override for the buffer usage warning threshold.
    pipeline_warn_usage_percent: ?u8 = null,
    /// Writer for logs.
    log: *std.Io.Writer,
    /// Optional path to the VISA shared library. When null the platform default
    /// locations are searched (e.g. /Library/Frameworks/VISA.framework/VISA on macOS).
    visa_lib: ?[]const u8 = null,
};

/// Runtime state associated with one precompiled instrument.
pub const InstrumentRuntime = struct {
    handle: ?visa.Instrument,
};

/// Execution-time value store used for `${name}` substitutions and `save_as` outputs.
pub const Context = struct {
    allocator: std.mem.Allocator,
    values: std.StringHashMap(ContextValue),

    const ContextValue = struct {
        buffer: []u8,
        len: usize,
    };

    /// Creates an empty execution context.
    pub fn init(allocator: std.mem.Allocator) Context {
        return .{ .allocator = allocator, .values = std.StringHashMap(ContextValue).init(allocator) };
    }

    /// Releases all context-owned keys and values.
    pub fn deinit(self: *Context) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.buffer);
        }
        self.values.deinit();
    }

    /// Stores or replaces a named runtime value.
    pub fn set(self: *Context, key: []const u8, value: []const u8) !void {
        if (self.values.getPtr(key)) |stored| {
            if (stored.buffer.len < value.len) {
                const replacement = try self.allocator.alloc(u8, value.len);
                self.allocator.free(stored.buffer);
                stored.buffer = replacement;
            }
            std.mem.copyForwards(u8, stored.buffer[0..value.len], value);
            stored.len = value.len;
            return;
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const value_copy = try self.allocator.alloc(u8, value.len);
        errdefer self.allocator.free(value_copy);
        std.mem.copyForwards(u8, value_copy, value);

        try self.values.put(key_copy, .{
            .buffer = value_copy,
            .len = value.len,
        });
    }

    /// Returns a previously stored runtime value.
    pub fn get(self: *const Context, key: []const u8) ?[]const u8 {
        const stored = self.values.get(key) orelse return null;
        return stored.buffer[0..stored.len];
    }

    /// Returns a VarResolver that reads values from this Context.
    pub fn varResolver(self: *const Context) expr.VarResolver {
        return .{
            .ctx = @ptrCast(self),
            .resolveFn = struct {
                fn resolve(ctx_ptr: *const anyopaque, name: []const u8) ?[]const u8 {
                    const ctx: *const Context = @ptrCast(@alignCast(ctx_ptr));
                    return ctx.get(name);
                }
            }.resolve,
        };
    }
};
