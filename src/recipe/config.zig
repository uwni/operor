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
    adapter: []const u8,
    resource: []const u8,
};

/// Boolean expression source that accepts a YAML string or bool.
///
/// Allows writing `while: true` instead of `while: "true"` in YAML.
pub const BooleanExpr = union(enum) {
    string: []const u8,
    bool: bool,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };

    /// Returns the expression string, or a fixed literal for booleans.
    pub fn source(self: BooleanExpr) []const u8 {
        return switch (self) {
            .string => |s| s,
            .bool => |b| if (b) "1" else "0",
        };
    }
};

/// Parsed step object before precompile validation.
///
/// A step is either an instrument `call`, a local `compute` expression, or a `sleep_ms` pause.
/// Both call and compute variants support an optional `if` guard expression.
pub const StepConfig = union(enum) {
    call: CallStepConfig,
    compute: ComputeStepConfig,
    sleep_ms: SleepStepConfig,

    pub const serde = .{
        .tag = serde_lib.UnionTag.untagged,
    };
};

pub const CallStepConfig = struct {
    /// Instrument command name.
    call: []const u8,
    /// Target instrument name.
    instrument: []const u8,
    /// Arguments forwarded to the adapter command template.
    args: ?std.StringHashMap(ArgValueDoc) = null,
    /// Context key that receives the step result (response or computed value).
    save_as: ?[]const u8 = null,
    /// Guard expression; step is skipped when the result is falsy (0.0).
    @"if": ?BooleanExpr = null,
};

pub const ComputeStepConfig = struct {
    /// Expression to evaluate locally.
    compute: []const u8,
    /// Context key that receives the step result.
    save_as: []const u8,
    /// Guard expression; step is skipped when the result is falsy (0.0).
    @"if": ?BooleanExpr = null,
};

pub const SleepStepConfig = struct {
    /// Duration in milliseconds.
    sleep_ms: u64,
    /// Guard expression; step is skipped when the result is falsy (0.0).
    @"if": ?BooleanExpr = null,
};

/// Parsed task object supporting loop, sequential, and conditional variants.
pub const TaskConfig = struct {
    /// Steps to execute.
    steps: []StepConfig,
    /// When present, task loops while this expression is truthy.
    @"while": ?BooleanExpr = null,
    /// Guard expression; task steps execute only when truthy.
    @"if": ?BooleanExpr = null,
};

/// Parsed pipeline configuration before execution-time normalization.
pub const PipelineConfig = types.PipelineConfig;

/// Parsed top-level recipe document.
pub const RecipeConfig = struct {
    instruments: std.StringArrayHashMap(InstrumentConfig),
    tasks: []TaskConfig,
    pipeline: ?PipelineConfig = null,
    stop_when: ?BooleanExpr = null,
    vars: ?std.StringArrayHashMap(ArgScalarDoc) = null,
    expected_iterations: ?u64 = null,
};

test "parse recipe arg object values" {
    const Parsed = struct {
        args: std.StringHashMap(ArgValueDoc),
    };

    const gpa = std.testing.allocator;
    const content =
        \\args:
        \\  voltage: "5"
        \\  channels:
        \\    - 1
        \\    - 2
        \\  enabled: true
    ;

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    const parsed = try doc_parse.parseByFormat(Parsed, .yaml, arena.allocator(), content);

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
