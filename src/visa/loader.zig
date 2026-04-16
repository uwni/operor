const std = @import("std");
const c = @import("common.zig").c;

// ---------------------------------------------------------------------------
// Vtable — resolved once at startup by `load()`.
// ---------------------------------------------------------------------------

pub const Vtable = struct {
    // Required — every VISA implementation must provide these.
    viOpenDefaultRM: *const fn (c.ViPSession) callconv(.c) c.ViStatus,
    viFindRsrc: *const fn (c.ViSession, c.ViConstString, c.ViPFindList, c.ViPUInt32, [*]u8) callconv(.c) c.ViStatus,
    viFindNext: *const fn (c.ViFindList, [*]u8) callconv(.c) c.ViStatus,
    viOpen: *const fn (c.ViSession, c.ViConstRsrc, c.ViAccessMode, c.ViUInt32, c.ViPSession) callconv(.c) c.ViStatus,
    viClose: *const fn (c.ViObject) callconv(.c) c.ViStatus,
    viRead: *const fn (c.ViSession, c.ViPBuf, c.ViUInt32, c.ViPUInt32) callconv(.c) c.ViStatus,
    viWrite: *const fn (c.ViSession, c.ViConstBuf, c.ViUInt32, c.ViPUInt32) callconv(.c) c.ViStatus,
    viSetAttribute: *const fn (c.ViObject, c.ViAttr, c.ViAttrState) callconv(.c) c.ViStatus,

    // Async I/O — null when the VISA library does not provide async support.
    viWriteAsync: ?*const fn (c.ViSession, c.ViConstBuf, c.ViUInt32, c.ViPJobId) callconv(.c) c.ViStatus = null,
    viReadAsync: ?*const fn (c.ViSession, c.ViPBuf, c.ViUInt32, c.ViPJobId) callconv(.c) c.ViStatus = null,
    viEnableEvent: ?*const fn (c.ViSession, c.ViEventType, c.ViUInt16, c.ViEventFilter) callconv(.c) c.ViStatus = null,
    viDisableEvent: ?*const fn (c.ViSession, c.ViEventType, c.ViUInt16) callconv(.c) c.ViStatus = null,
    viWaitOnEvent: ?*const fn (c.ViSession, c.ViEventType, c.ViUInt32, c.ViPEventType, c.ViPEvent) callconv(.c) c.ViStatus = null,
    viTerminate: ?*const fn (c.ViObject, c.ViUInt16, c.ViJobId) callconv(.c) c.ViStatus = null,
    viGetAttribute: ?*const fn (c.ViObject, c.ViAttr, ?*anyopaque) callconv(.c) c.ViStatus = null,
};

pub const Error = error{ VisaLibraryNotFound, VisaSymbolMissing };

pub const LoadDiagnostic = struct {
    /// Explicit path that was tried (null when platform defaults were searched).
    path: ?[]const u8 = null,
    /// Name of the missing symbol (only set for `VisaSymbolMissing`).
    symbol: ?[]const u8 = null,

    pub fn write(self: *const LoadDiagnostic, writer: *std.Io.Writer, err: Error) !void {
        switch (err) {
            error.VisaLibraryNotFound => if (self.path) |p|
                try writer.print("VISA library not found at '{s}'.\n", .{p})
            else
                try writer.writeAll(
                    "VISA library not found. Install a VISA implementation " ++
                        "(e.g. NI-VISA, Keysight IO Libraries) or specify the path with --visa-lib.\n",
                ),
            error.VisaSymbolMissing => try writer.print(
                "VISA library found but symbol '{s}' is missing. " ++
                    "Try reinstalling your VISA implementation.\n",
                .{self.symbol.?},
            ),
        }
    }
};

/// Platform-specific VISA shared library names tried in order when no explicit path is given.
pub const default_lib_names: []const []const u8 = switch (@import("builtin").os.tag) {
    .macos => &.{
        "/Library/Frameworks/VISA.framework/VISA",
        "/Library/Frameworks/VISA.framework/Versions/Current/VISA",
    },
    .windows => &.{ "visa64.dll", "visa32.dll" },
    .linux => &.{ "libvisa.so", "libvisa.so.0" },
    else => &.{}, // unsupported platform — load() will always return error.VisaLibraryNotFound
};

/// Opens the VISA shared library and resolves all required symbols.
///
/// `path` — if non-null, only that path is tried; otherwise `default_lib_names`
/// are tried in order.
///
/// Returns `error.VisaLibraryNotFound` when the library cannot be opened, or
/// `error.VisaSymbolMissing` when a required symbol is not found.
///
/// The library handle is intentionally never closed: VISA must remain loaded
/// for the entire process lifetime because the returned function pointers
/// become invalid after dlclose.
pub fn load(path: ?[]const u8, diag: ?*LoadDiagnostic) Error!Vtable {
    var lib: std.DynLib = if (path) |p| blk: {
        break :blk std.DynLib.open(p) catch {
            if (diag) |d| d.* = .{ .path = p };
            return error.VisaLibraryNotFound;
        };
    } else for (default_lib_names) |name| {
        break std.DynLib.open(name) catch continue;
    } else {
        if (diag) |d| d.* = .{};
        return error.VisaLibraryNotFound;
    };

    var vtable: Vtable = undefined;

    inline for (std.meta.fields(Vtable)) |field| {
        const ti = @typeInfo(field.type);
        const T = if (ti == .optional) ti.optional.child else field.type;
        const ptr = lib.lookup(T, field.name);
        @field(vtable, field.name) = if (ti == .optional) ptr else ptr orelse {
            if (diag) |d| d.* = .{ .symbol = field.name };
            return error.VisaSymbolMissing;
        };
    }

    return vtable;
}
