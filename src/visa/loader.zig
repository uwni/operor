const std = @import("std");
const c = @import("common.zig").c;

// ---------------------------------------------------------------------------
// Function-pointer types for each VISA symbol we use.
// Calling conventions follow the platform ABI; on macOS/Linux _VI_FUNC is
// empty so the default C calling convention applies.
// ---------------------------------------------------------------------------

const FnOpenDefaultRM = *const fn (vi: c.ViPSession) callconv(.c) c.ViStatus;
const FnFindRsrc = *const fn (
    sesn: c.ViSession,
    expr: c.ViConstString,
    vi: c.ViPFindList,
    retCnt: c.ViPUInt32,
    desc: [*]u8,
) callconv(.c) c.ViStatus;
const FnFindNext = *const fn (vi: c.ViFindList, desc: [*]u8) callconv(.c) c.ViStatus;
const FnOpen = *const fn (
    sesn: c.ViSession,
    name: c.ViConstRsrc,
    mode: c.ViAccessMode,
    timeout: c.ViUInt32,
    vi: c.ViPSession,
) callconv(.c) c.ViStatus;
const FnClose = *const fn (vi: c.ViObject) callconv(.c) c.ViStatus;
const FnRead = *const fn (
    vi: c.ViSession,
    buf: c.ViPBuf,
    cnt: c.ViUInt32,
    retCnt: c.ViPUInt32,
) callconv(.c) c.ViStatus;
const FnWrite = *const fn (
    vi: c.ViSession,
    buf: c.ViConstBuf,
    cnt: c.ViUInt32,
    retCnt: c.ViPUInt32,
) callconv(.c) c.ViStatus;
const FnSetAttribute = *const fn (
    vi: c.ViObject,
    attrName: c.ViAttr,
    attrValue: c.ViAttrState,
) callconv(.c) c.ViStatus;

// ---------------------------------------------------------------------------
// Vtable — resolved once at startup by `load()`.
// ---------------------------------------------------------------------------

pub const Vtable = struct {
    openDefaultRM: FnOpenDefaultRM,
    findRsrc: FnFindRsrc,
    findNext: FnFindNext,
    open: FnOpen,
    close: FnClose,
    read: FnRead,
    write: FnWrite,
    setAttribute: FnSetAttribute,
};

pub const Error = error{VisaLibraryNotFound};

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
/// Returns `error.VisaLibraryNotFound` with a diagnostic message printed to
/// stderr when the library cannot be found or a symbol is missing.
///
/// The library handle is intentionally never closed: VISA must remain loaded
/// for the entire process lifetime because the returned function pointers
/// become invalid after dlclose.
pub fn load(path: ?[]const u8) Error!Vtable {
    var lib: std.DynLib = if (path) |p| blk: {
        break :blk std.DynLib.open(p) catch {
            printError("error: VISA library not found at '{s}'.\n", .{p});
            return error.VisaLibraryNotFound;
        };
    } else for (default_lib_names) |name| {
        break std.DynLib.open(name) catch continue;
    } else {
        printError(
            "error: VISA library not found. Install a VISA implementation (e.g. NI-VISA, " ++
                "Keysight IO Libraries) or specify the path with --visa-lib.\n",
            .{},
        );
        return error.VisaLibraryNotFound;
    };

    return .{
        .openDefaultRM = lib.lookup(FnOpenDefaultRM, "viOpenDefaultRM") orelse return missingSymbol("viOpenDefaultRM"),
        .findRsrc = lib.lookup(FnFindRsrc, "viFindRsrc") orelse return missingSymbol("viFindRsrc"),
        .findNext = lib.lookup(FnFindNext, "viFindNext") orelse return missingSymbol("viFindNext"),
        .open = lib.lookup(FnOpen, "viOpen") orelse return missingSymbol("viOpen"),
        .close = lib.lookup(FnClose, "viClose") orelse return missingSymbol("viClose"),
        .read = lib.lookup(FnRead, "viRead") orelse return missingSymbol("viRead"),
        .write = lib.lookup(FnWrite, "viWrite") orelse return missingSymbol("viWrite"),
        .setAttribute = lib.lookup(FnSetAttribute, "viSetAttribute") orelse return missingSymbol("viSetAttribute"),
    };
}

fn missingSymbol(name: []const u8) error{VisaLibraryNotFound} {
    printError(
        "error: VISA library found but symbol '{s}' is missing. " ++
            "Try reinstalling your VISA implementation.\n",
        .{name},
    );
    return error.VisaLibraryNotFound;
}

fn printError(comptime fmt: []const u8, args: anytype) void {
    const stderr = std.fs.File.stderr();
    var buf: [512]u8 = undefined;
    var w = stderr.writer(&buf);
    w.interface.print(fmt, args) catch {};
    w.interface.flush() catch {};
}
