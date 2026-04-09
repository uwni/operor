const std = @import("std");
const expr = @import("../expr.zig");

pub const Value = union(enum) {
    float: f64,
    int: i64,
    bool: bool,
    string: []const u8,

    fn toResolvedValue(self: Value) expr.ResolvedValue {
        return switch (self) {
            .float => |f| .{ .number = f },
            .int => |i| .{ .number = @floatFromInt(i) },
            .bool => |b| .{ .number = if (b) 1.0 else 0.0 },
            .string => |s| .{ .string = s },
        };
    }

    pub fn format(self: Value, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .float => |f| try writer.print("{d}", .{f}),
            .int => |i| try writer.print("{d}", .{i}),
            .bool => |b| try writer.writeAll(if (b) "true" else "false"),
            .string => |s| try writer.writeAll(s),
        }
    }
};

/// Render-time value used by command templates.
pub const RenderValue = union(enum) {
    scalar: Value,
    list: []const Value,

    pub fn format(self: RenderValue, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .scalar => |value| try value.format(writer),
            .list => |items| {
                for (items, 0..) |item, idx| {
                    if (idx > 0) try writer.writeByte(',');
                    try item.format(writer);
                }
            },
        }
    }
};

/// Execution-time value store used for `${name}` substitutions and `save_as` outputs.
pub const Context = struct {
    allocator: std.mem.Allocator,
    start_ns: i128 = 0,
    iteration: u64 = 0,
    task_idx: usize = 0,
    values: std.StringHashMap(ContextValue),

    const ContextValue = union(enum) {
        float: f64,
        int: i64,
        bool: bool,
        string: struct {
            buffer: []u8,
            len: usize,
        },
    };

    /// Creates an empty execution context.
    pub fn init(allocator: std.mem.Allocator) Context {
        return .{
            .allocator = allocator,
            .values = std.StringHashMap(ContextValue).init(allocator),
        };
    }

    /// Releases all context-owned keys and values.
    pub fn deinit(self: *Context) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            switch (entry.value_ptr.*) {
                .string => |s| self.allocator.free(s.buffer),
                else => {},
            }
        }
        self.values.deinit();
    }

    /// Stores or replaces a named runtime value.
    pub fn set(self: *Context, key: []const u8, value: Value) !void {
        if (self.values.getPtr(key)) |stored| {
            switch (value) {
                .string => |s| {
                    if (stored.* == .string) {
                        if (stored.string.buffer.len < s.len) {
                            const replacement = try self.allocator.alloc(u8, s.len);
                            self.allocator.free(stored.string.buffer);
                            stored.string.buffer = replacement;
                        }
                        std.mem.copyForwards(u8, stored.string.buffer[0..s.len], s);
                        stored.string.len = s.len;
                    } else {
                        const buffer = try self.allocator.alloc(u8, s.len);
                        std.mem.copyForwards(u8, buffer, s);
                        stored.* = .{ .string = .{ .buffer = buffer, .len = s.len } };
                    }
                },
                .float => |f| {
                    if (stored.* == .string) self.allocator.free(stored.string.buffer);
                    stored.* = .{ .float = f };
                },
                .int => |i| {
                    if (stored.* == .string) self.allocator.free(stored.string.buffer);
                    stored.* = .{ .int = i };
                },
                .bool => |b| {
                    if (stored.* == .string) self.allocator.free(stored.string.buffer);
                    stored.* = .{ .bool = b };
                },
            }
            return;
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const val = switch (value) {
            .string => |s| blk: {
                const buffer = try self.allocator.alloc(u8, s.len);
                std.mem.copyForwards(u8, buffer, s);
                break :blk ContextValue{ .string = .{ .buffer = buffer, .len = s.len } };
            },
            .float => |f| ContextValue{ .float = f },
            .int => |i| ContextValue{ .int = i },
            .bool => |b| ContextValue{ .bool = b },
        };

        try self.values.put(key_copy, val);
    }

    /// Returns a previously stored runtime value.
    pub fn get(self: *const Context, key: []const u8) ?Value {
        if (builtinValue(self, key)) |value| return value;

        const stored = self.values.get(key) orelse return null;
        return switch (stored) {
            .float => |f| .{ .float = f },
            .int => |i| .{ .int = i },
            .bool => |b| .{ .bool = b },
            .string => |s| .{ .string = s.buffer[0..s.len] },
        };
    }

    fn builtinValue(self: *const Context, key: []const u8) ?Value {
        if (std.mem.eql(u8, key, "$ITER")) return .{ .int = @intCast(self.iteration) };
        if (std.mem.eql(u8, key, "$TASK_IDX")) return .{ .int = @intCast(self.task_idx) };
        return null;
    }

    fn resolve(ctx_ptr: *const anyopaque, name: []const u8) ?expr.ResolvedValue {
        const self: *const Context = @ptrCast(@alignCast(ctx_ptr));
        const val = self.get(name) orelse return null;
        return val.toResolvedValue();
    }

    /// Returns an expression resolver over user values plus built-in execution state.
    pub fn varResolver(self: *const Context) expr.VarResolver {
        return .{
            .ctx = @ptrCast(self),
            .resolveFn = resolve,
        };
    }
};

test "Value and RenderValue format support formatter specifier" {
    const testing = std.testing;
    const list = [_]Value{
        .{ .int = 1 },
        .{ .float = 2.5 },
        .{ .string = "ch3" },
    };

    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();

    try out.writer.print("{f}|{f}|{f}|{f}|{f}", .{
        Value{ .float = 1.25 },
        Value{ .int = 42 },
        Value{ .bool = true },
        Value{ .string = "ok" },
        RenderValue{ .list = list[0..] },
    });

    try testing.expectEqualStrings("1.25|42|true|ok|1,2.5,ch3", out.written());
}

test "Context exposes built-ins alongside stored values" {
    const testing = std.testing;
    const expr_mod = @import("../expr.zig");

    var ctx = Context.init(testing.allocator);
    defer ctx.deinit();

    try ctx.set("voltage", .{ .float = 3.3 });
    ctx.task_idx = 2;
    ctx.iteration = 7;

    try testing.expectEqualDeep(Value{ .int = 7 }, ctx.get("$ITER").?);
    try testing.expectEqualDeep(Value{ .int = 2 }, ctx.get("$TASK_IDX").?);
    try testing.expectEqualDeep(Value{ .float = 3.3 }, ctx.get("voltage").?);
    try testing.expect(ctx.get("missing") == null);

    try testing.expectApproxEqAbs(@as(f64, 9.0), try expr_mod.eval(testing.allocator, "$ITER + $TASK_IDX", ctx.varResolver()), 1e-9);
}

test "Context stores run start state separately from iteration state" {
    const testing = std.testing;

    var ctx = Context.init(testing.allocator);
    defer ctx.deinit();

    ctx.start_ns = 1234;
    ctx.task_idx = 5;
    ctx.iteration = 9;

    try testing.expectEqual(@as(i128, 1234), ctx.start_ns);
    try testing.expectEqual(@as(usize, 5), ctx.task_idx);
    try testing.expectEqual(@as(u64, 9), ctx.iteration);
}
