const std = @import("std");
const serde_lib = @import("serde");
const doc_parse = @import("../doc_parse.zig");
const types = @import("types.zig");

/// Scalar document value accepted in recipe argument objects.
pub const ArgScalarDoc = union(enum) {
    /// String literal preserved as-is.
    string: []const u8,
    /// Integer literal formatted back to decimal text for runtime rendering.
    int: i64,
    /// Floating-point literal formatted back to decimal text for runtime rendering.
    float: f64,
    /// Boolean literal formatted as `true` or `false`.
    bool: bool,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

/// Recipe argument document value that may be a scalar or a list of scalars.
pub const ArgValueDoc = union(enum) {
    /// Single scalar argument value.
    scalar: ArgScalarDoc,
    /// Ordered list argument value.
    list: []const ArgScalarDoc,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

/// Parsed instrument object straight from the recipe document.
pub const InstrumentConfig = struct {
    driver: []const u8,
    resource: []const u8,
};

/// Parsed step object before precompile validation.
///
/// A step is either an instrument `call` or a local `compute` expression.
/// Both variants support an optional `when` guard expression.
pub const StepConfig = union(enum) {
    call: CallStepConfig,
    compute: ComputeStepConfig,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

pub const CallStepConfig = struct {
    /// Instrument command name.
    call: []const u8,
    /// Target instrument name.
    instrument: []const u8,
    /// Arguments forwarded to the driver command template.
    args: ?std.StringHashMap(ArgValueDoc) = null,
    /// Context key that receives the step result (response or computed value).
    save_as: ?[]const u8 = null,
    /// Guard expression; step is skipped when the result is falsy (0.0).
    when: ?[]const u8 = null,
};

pub const ComputeStepConfig = struct {
    /// Expression to evaluate locally.
    compute: []const u8,
    /// Context key that receives the step result.
    save_as: []const u8,
    /// Guard expression; step is skipped when the result is falsy (0.0).
    when: ?[]const u8 = null,
};

/// Parsed task object before interval normalization.
pub const TaskConfig = struct {
    every_ms: ?u64 = null,
    every: ?[]const u8 = null,
    steps: []StepConfig,
};

/// Parsed pipeline configuration before execution-time normalization.
pub const PipelineConfig = types.PipelineConfig;

/// Parsed stop condition object from the recipe document.
pub const StopWhenConfig = struct {
    time_elapsed: ?[]const u8 = null,
    max_iterations: ?u64 = null,
};

/// Parsed top-level recipe document.
pub const RecipeConfig = struct {
    instruments: std.StringHashMap(InstrumentConfig),
    tasks: []TaskConfig,
    pipeline: ?PipelineConfig = null,
    stop_when: ?StopWhenConfig = null,
    vars: ?std.StringHashMap(ArgScalarDoc) = null,
};

test "parse recipe arg object values" {
    const Parsed = struct {
        args: std.StringHashMap(ArgValueDoc),
    };

    const gpa = std.testing.allocator;
    const content =
        \\{
        \\  "args": {
        \\    "voltage": "5",
        \\    "channels": [1, 2],
        \\    "enabled": true
        \\  }
        \\}
    ;

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const parsed = try doc_parse.parseByFormat(Parsed, .json, arena.allocator(), content);

    const voltage = parsed.args.get("voltage") orelse return error.TestUnexpectedResult;
    switch (voltage) {
        .scalar => |scalar| switch (scalar) {
            .string => |value| try std.testing.expectEqualStrings("5", value),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }

    const channels = parsed.args.get("channels") orelse return error.TestUnexpectedResult;
    switch (channels) {
        .scalar => return error.TestUnexpectedResult,
        .list => |items| {
            try std.testing.expectEqual(@as(usize, 2), items.len);
            switch (items[0]) {
                .int => |value| try std.testing.expectEqual(@as(i64, 1), value),
                else => return error.TestUnexpectedResult,
            }
            switch (items[1]) {
                .int => |value| try std.testing.expectEqual(@as(i64, 2), value),
                else => return error.TestUnexpectedResult,
            }
        },
    }

    const enabled = parsed.args.get("enabled") orelse return error.TestUnexpectedResult;
    switch (enabled) {
        .scalar => |scalar| switch (scalar) {
            .bool => |value| try std.testing.expect(value),
            else => return error.TestUnexpectedResult,
        },
        .list => return error.TestUnexpectedResult,
    }
}
