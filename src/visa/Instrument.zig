const std = @import("std");
const bindings = @import("bindings.zig");
const loader = @import("loader.zig");

const c = bindings.c;
/// Open VISA instrument session and convenience helpers for reads and writes.
const Instrument = @This();
const ViSession = bindings.ViSession;
const ViUInt32 = bindings.ViUInt32;
const ViAttrState = bindings.ViAttrState;

const read_stack_chunk_bytes: usize = bindings.default_chunk_size;

instrument: ViSession,
rm: ViSession,
options: bindings.InstrumentOptions,
vtable: *const loader.Vtable,

/// Result of a single read operation.
pub const ReadResult = struct {
    data: []const u8,
    /// True when the buffer was filled and more data may be available.
    more: bool,
};

/// Creates an unopened instrument bound to a resource manager session.
pub fn init(rm: ViSession, vtable: *const loader.Vtable) Instrument {
    return .{
        .instrument = undefined,
        .rm = rm,
        .options = .{},
        .vtable = vtable,
    };
}

/// Opens the instrument session for the given VISA resource address.
pub fn open(
    self: *Instrument,
    allocator: std.mem.Allocator,
    resource_addr: []const u8,
    options: bindings.InstrumentOptions,
) bindings.Error!void {
    const resource_addr_z = allocator.dupeZ(u8, resource_addr) catch return bindings.Error.OutOfMemory;
    defer allocator.free(resource_addr_z);

    self.options = options;
    try bindings.checkStatus(self.vtable.viOpen(self.rm, resource_addr_z.ptr, c.VI_NULL, c.VI_NULL, &self.instrument));
    self.applyOptions() catch |err| {
        self.close(self.instrument) catch {};
        return err;
    };
}

/// Closes the underlying VISA instrument session.
pub fn deinit(self: *Instrument) void {
    self.close(self.instrument) catch {};
}

/// Streams repeated reads into a writer until VISA reports the response is complete.
pub fn readToWriter(
    self: *Instrument,
    writer: *std.Io.Writer,
    chunk_buffer: []u8,
) (bindings.Error || std.Io.Writer.Error)!usize {
    var total_read: usize = 0;

    while (true) {
        const result = try self.read(chunk_buffer);

        if (result.data.len > 0) {
            try writer.writeAll(result.data);
            total_read += result.data.len;
        }

        if (!result.more) break;
    }

    return total_read;
}

/// Reads a complete response into a newly allocated slice.
pub fn readToOwned(self: *Instrument, allocator: std.mem.Allocator) bindings.Error![]u8 {
    const chunk_size = self.options.normalizedChunkSize();
    var stack_chunk: [read_stack_chunk_bytes]u8 = undefined;

    if (chunk_size <= stack_chunk.len) {
        return self.readToOwnedWithChunk(allocator, stack_chunk[0..chunk_size]);
    }

    const chunk_buffer = try allocator.alloc(u8, chunk_size);
    defer allocator.free(chunk_buffer);
    return self.readToOwnedWithChunk(allocator, chunk_buffer);
}

fn readToOwnedWithChunk(self: *Instrument, allocator: std.mem.Allocator, chunk_buffer: []u8) bindings.Error![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);

    while (true) {
        const result = try self.read(chunk_buffer);

        if (result.data.len > 0) {
            try out.appendSlice(allocator, result.data);
        }

        if (!result.more) break;
    }

    trimReadTermination(&out, self.options.read_termination.constSlice());
    return out.toOwnedSlice(allocator);
}

/// Waits for the configured query delay before a follow-up read.
pub fn waitQueryDelay(self: *const Instrument) void {
    if (self.options.query_delay_ms == 0) return;
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();
    io.sleep(.fromMilliseconds(@as(i64, self.options.query_delay_ms)), .awake) catch {};
}

/// Writes a command and reads the complete response using the configured query delay.
pub fn queryToOwned(self: *Instrument, allocator: std.mem.Allocator, command: []const u8) bindings.Error![]u8 {
    try self.write(command);
    self.waitQueryDelay();
    return self.readToOwned(allocator);
}

/// Writes a command and returns the first read chunk directly into `buffer`.
pub fn queryRaw(self: *Instrument, command: []const u8, buffer: []u8) bindings.Error!ReadResult {
    try self.write(command);
    self.waitQueryDelay();
    return self.read(buffer);
}

// ---------------------------------------------------------------------------
// Async I/O — used by the parallel executor's poll loop.
// These are only available when the VISA library exposes the async symbols.
// ---------------------------------------------------------------------------

pub const AsyncError = error{AsyncNotSupported} || bindings.Error;

/// Returns true when the loaded VISA library supports async I/O primitives.
pub fn hasAsyncSupport(self: *const Instrument) bool {
    const vt = self.vtable;
    return vt.viWriteAsync != null and vt.viReadAsync != null and
        vt.viEnableEvent != null and vt.viDisableEvent != null and
        vt.viWaitOnEvent != null and vt.viGetAttribute != null;
}

/// Enables VI_EVENT_IO_COMPLETION queuing on this session.
pub inline fn enableAsyncEvents(self: *const Instrument) AsyncError!void {
    try self.enableEvent(c.VI_EVENT_IO_COMPLETION, c.VI_QUEUE);
}

/// Disables VI_EVENT_IO_COMPLETION queuing on this session.
pub inline fn disableAsyncEvents(self: *const Instrument) AsyncError!void {
    try self.disableEvent(c.VI_EVENT_IO_COMPLETION, c.VI_QUEUE);
}

/// Polls for a completed I/O event with timeout (0 = non-blocking).
/// Returns the event handle on success, or null on timeout.
pub inline fn waitEvent(self: *const Instrument, timeout_ms: bindings.ViUInt32) AsyncError!?bindings.ViEvent {
    return self.waitOnEvent(c.VI_EVENT_IO_COMPLETION, timeout_ms);
}

/// Retrieves the actual byte count from a completed I/O event.
pub fn eventRetCount(self: *const Instrument, event: bindings.ViEvent) AsyncError!bindings.ViUInt32 {
    var count: bindings.ViUInt32 = 0;
    try self.getAttribute(event, c.VI_ATTR_RET_COUNT_32, @ptrCast(&count));
    return count;
}

/// Checks whether a completed async read has more data available.
/// Returns true when the I/O completed with VI_SUCCESS_MAX_CNT (buffer full, more pending).
pub fn eventIoHasMore(self: *const Instrument, event: bindings.ViEvent) AsyncError!bool {
    var io_status: bindings.ViStatus = undefined;
    try self.getAttribute(event, c.VI_ATTR_STATUS, @ptrCast(&io_status));
    return switch (io_status) {
        c.VI_SUCCESS_MAX_CNT => true,
        else => {
            try bindings.checkStatus(io_status);
            return false;
        },
    };
}

pub fn applyOptions(self: *Instrument) bindings.Error!void {
    if (self.options.timeout_ms) |timeout_ms| {
        try self.setAttribute(c.VI_ATTR_TMO_VALUE, @as(ViAttrState, @intCast(timeout_ms)));
    }
    try self.applyReadTermination();
}

fn applyReadTermination(self: *Instrument) bindings.Error!void {
    const read_termination = self.options.read_termination.constSlice();
    if (read_termination.len == 0) {
        return self.setAttribute(c.VI_ATTR_TERMCHAR_EN, 0);
    }

    try self.setAttribute(c.VI_ATTR_TERMCHAR, @as(ViAttrState, read_termination[read_termination.len - 1]));
    try self.setAttribute(c.VI_ATTR_TERMCHAR_EN, 1);
}

// ---------------------------------------------------------------------------
// VISA vtable wrappers — Zig-style 1:1 wrappers for each C function.
// All ViStatus return values are converted to Zig errors.
// ---------------------------------------------------------------------------

/// Closes a VISA object handle (instrument session or event).
pub fn close(self: *const Instrument, obj: c.ViObject) bindings.Error!void {
    try bindings.checkStatus(self.vtable.viClose(obj));
}

/// Writes a command buffer to the instrument.
pub fn write(self: *const Instrument, buf: []const u8) bindings.Error!void {
    var ret_count: ViUInt32 = undefined;
    try bindings.checkStatus(self.vtable.viWrite(self.instrument, buf.ptr, @intCast(buf.len), &ret_count));
}

/// Reads one chunk of bytes into a caller-provided buffer.
pub fn read(self: *const Instrument, buf: []u8) bindings.Error!ReadResult {
    var ret_count: ViUInt32 = undefined;
    const status = self.vtable.viRead(self.instrument, buf.ptr, @intCast(buf.len), &ret_count);
    return switch (status) {
        c.VI_SUCCESS, c.VI_SUCCESS_TERM_CHAR => .{ .data = buf[0..ret_count], .more = false },
        c.VI_SUCCESS_MAX_CNT => .{ .data = buf[0..ret_count], .more = true },
        else => {
            try bindings.checkStatus(status);
            unreachable;
        },
    };
}

fn setAttribute(self: *const Instrument, attr: c.ViAttr, value: ViAttrState) bindings.Error!void {
    try bindings.checkStatus(self.vtable.viSetAttribute(self.instrument, attr, value));
}

fn getAttribute(self: *const Instrument, obj: c.ViObject, attr: c.ViAttr, out: ?*anyopaque) AsyncError!void {
    const f = self.vtable.viGetAttribute orelse return error.AsyncNotSupported;
    try bindings.checkStatus(f(obj, attr, out));
}

/// Submits an asynchronous write. Returns the job ID for later polling.
pub fn writeAsync(self: *const Instrument, buf: []const u8) AsyncError!bindings.ViJobId {
    const f = self.vtable.viWriteAsync orelse return error.AsyncNotSupported;
    var job_id: bindings.ViJobId = undefined;
    try bindings.checkStatus(f(self.instrument, buf.ptr, @intCast(buf.len), &job_id));
    return job_id;
}

/// Submits an asynchronous read into a caller-provided buffer. Returns the job ID.
pub fn readAsync(self: *const Instrument, buf: []u8) AsyncError!bindings.ViJobId {
    const f = self.vtable.viReadAsync orelse return error.AsyncNotSupported;
    var job_id: bindings.ViJobId = undefined;
    try bindings.checkStatus(f(self.instrument, buf.ptr, @intCast(buf.len), &job_id));
    return job_id;
}

/// Enables event notification for the given event type and mechanism.
pub fn enableEvent(self: *const Instrument, event_type: bindings.ViEventType, mechanism: c.ViUInt16) AsyncError!void {
    const f = self.vtable.viEnableEvent orelse return error.AsyncNotSupported;
    try bindings.checkStatus(f(self.instrument, event_type, mechanism, c.VI_NULL));
}

/// Disables event notification for the given event type and mechanism.
pub fn disableEvent(self: *const Instrument, event_type: bindings.ViEventType, mechanism: c.ViUInt16) AsyncError!void {
    const f = self.vtable.viDisableEvent orelse return error.AsyncNotSupported;
    try bindings.checkStatus(f(self.instrument, event_type, mechanism));
}

/// Waits for an event of the given type. Returns null on timeout.
pub fn waitOnEvent(self: *const Instrument, event_type: bindings.ViEventType, timeout_ms: bindings.ViUInt32) AsyncError!?bindings.ViEvent {
    const f = self.vtable.viWaitOnEvent orelse return error.AsyncNotSupported;
    var out_event_type: bindings.ViEventType = undefined;
    var out_context: bindings.ViEvent = undefined;
    const status = f(self.instrument, event_type, timeout_ms, &out_event_type, &out_context);
    if (status == c.VI_ERROR_TMO) return null;
    try bindings.checkStatus(status);
    return out_context;
}

/// Cancels an in-flight async I/O job.
pub fn terminate(self: *const Instrument, job_id: bindings.ViJobId) AsyncError!void {
    const f = self.vtable.viTerminate orelse return error.AsyncNotSupported;
    try bindings.checkStatus(f(self.instrument, c.VI_NULL, job_id));
}

fn trimReadTermination(out: *std.ArrayList(u8), read_termination: []const u8) void {
    if (read_termination.len == 0) return;
    if (!std.mem.endsWith(u8, out.items, read_termination)) return;
    out.items.len -= read_termination.len;
}

test "trim read termination removes configured suffix only at the end" {
    const gpa = std.testing.allocator;

    var out = std.ArrayList(u8).empty;
    defer out.deinit(gpa);
    try out.appendSlice(gpa, "TEST,MODEL,123\r\n");
    trimReadTermination(&out, "\r\n");
    try std.testing.expectEqualStrings("TEST,MODEL,123", out.items);

    out.clearRetainingCapacity();
    try out.appendSlice(gpa, "5.000\npartial");
    trimReadTermination(&out, "\n");
    try std.testing.expectEqualStrings("5.000\npartial", out.items);
}
