const common = @import("common.zig");
/// Resource manager API for VISA discovery.
pub const ResourceManager = @import("ResourceManager.zig");
/// Instrument API for VISA reads, writes, and queries.
pub const Instrument = @import("Instrument.zig");
/// Runtime VISA library loader — call `loader.load()` once at startup.
pub const loader = @import("loader.zig");

/// Re-exported raw VISA C bindings.
pub const c = common.c;
/// Re-exported project-level VISA error set.
pub const Error = common.Error;
/// Re-exported instrument session option set.
pub const InstrumentOptions = common.InstrumentOptions;
/// Re-exported fixed-size termination buffer type.
pub const Termination = common.Termination;
/// Re-exported VISA async job identifier type.
pub const ViJobId = common.ViJobId;
/// Default chunk size used by instrument-owned reads.
pub const default_chunk_size = common.default_chunk_size;
/// Re-exported resource list result type.
pub const ResourceList = ResourceManager.ResourceList;
