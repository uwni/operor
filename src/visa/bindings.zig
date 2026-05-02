const std = @import("std");
const instrument = @import("../instrument.zig");

/// Raw VISA C bindings translated from `visa_c.h`.
pub const c = @import("visa_c");

pub const default_chunk_size = instrument.default_chunk_size;

/// Opaque VISA session handle.
pub const ViSession = c.ViSession;
/// Numeric VISA status code.
pub const ViStatus = c.ViStatus;
/// Unsigned 32-bit VISA integer.
pub const ViUInt32 = c.ViUInt32;
/// VISA resource search handle.
pub const ViFindList = c.ViFindList;
/// VISA attribute state type.
pub const ViAttrState = c.ViAttrState;
/// VISA asynchronous job identifier.
pub const ViJobId = c.ViJobId;
/// VISA event type code.
pub const ViEventType = c.ViEventType;
/// VISA event handle.
pub const ViEvent = c.ViEvent;

/// Project-level error set used by the VISA wrapper layer.
pub const Error = error{
    OutOfMemory,
    VisaError,
    ParseError,
    BufferTooSmall,
    ConnectionError,
    TimeoutError,
    InvalidSession,
    ResourceNotFound,
};

pub const Termination = instrument.Termination;
pub const InstrumentOptions = instrument.InstrumentOptions;

/// Converts a raw VISA status code into a Zig error when the call failed.
pub fn checkStatus(status: ViStatus) Error!void {
    if (status >= c.VI_SUCCESS) return;

    return switch (status) {
        c.VI_ERROR_TMO => Error.TimeoutError,
        c.VI_ERROR_CONN_LOST => Error.ConnectionError,
        c.VI_ERROR_INV_OBJECT => Error.InvalidSession,
        c.VI_ERROR_RSRC_NFOUND, c.VI_ERROR_INV_RSRC_NAME => Error.ResourceNotFound,
        else => Error.VisaError,
    };
}
