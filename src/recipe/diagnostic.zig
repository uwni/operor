const common = @import("../diagnostic.zig");

pub const DiagnosticContext = common.Context;
pub const Severity = common.Severity;
pub const ExprSourceKind = enum {
    expression,
    argument,

    pub fn sourceKind(self: ExprSourceKind) common.SourceKind {
        return switch (self) {
            .expression => .expression,
            .argument => .argument_expression,
        };
    }
};
pub const Message = common.Message;
pub const PrecompileDiagnostic = common.Diagnostics;
