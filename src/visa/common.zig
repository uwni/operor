const std = @import("std");

/// Raw VISA C bindings imported from `visa.h`.
pub const c = @cImport({
    // The VISA library uses `__int64` in some APIs, but this type is not defined by default in Zig's C importer. We define it here to ensure the bindings compile correctly.
    @cDefine("__int64", "long long");
    @cInclude("visa.h");
});

pub const default_chunk_size: usize = 1024;

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

/// High-level session configuration applied when opening an instrument.
pub const InstrumentOptions = struct {
    /// Per-operation timeout in milliseconds. Null leaves the backend default unchanged.
    timeout_ms: ?u32 = null,
    /// Response suffix removed from owned reads when present.
    read_termination: []const u8 = "",
    /// Delay inserted between a write and the following read in query flows.
    query_delay_ms: u32 = 0,
    /// Size of the temporary chunk buffer used when reading owned responses.
    chunk_size: usize = default_chunk_size,

    pub fn normalizedChunkSize(self: InstrumentOptions) usize {
        return if (self.chunk_size == 0) default_chunk_size else self.chunk_size;
    }
};

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
