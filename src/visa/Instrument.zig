const std = @import("std");
const common = @import("common.zig");
const loader = @import("loader.zig");

const c = common.c;
/// Open VISA instrument session and convenience helpers for reads and writes.
const Instrument = @This();
const ViSession = common.ViSession;
const ViStatus = common.ViStatus;
const ViUInt32 = common.ViUInt32;
const ViAttrState = common.ViAttrState;

const read_stack_chunk_bytes: usize = common.default_chunk_size;

instrument: ViSession,
rm: ViSession,
status: ViStatus,
options: common.InstrumentOptions,
vtable: *const loader.Vtable,

/// Creates an unopened instrument bound to a resource manager session.
pub fn init(rm: ViSession, vtable: *const loader.Vtable) Instrument {
    return .{
        .instrument = undefined,
        .rm = rm,
        .status = 0,
        .options = .{},
        .vtable = vtable,
    };
}

/// Opens the instrument session for the given VISA resource address.
pub fn open(
    self: *Instrument,
    allocator: std.mem.Allocator,
    resource_addr: []const u8,
    options: common.InstrumentOptions,
) common.Error!void {
    const resource_addr_z = allocator.dupeZ(u8, resource_addr) catch return common.Error.OutOfMemory;
    defer allocator.free(resource_addr_z);

    self.options = options;
    self.status = self.vtable.open(self.rm, resource_addr_z.ptr, c.VI_NULL, c.VI_NULL, &self.instrument);
    try self.checkInstrumentStatus();
    self.applyOptions() catch |err| {
        _ = self.vtable.close(self.instrument);
        return err;
    };
}

/// Closes the underlying VISA instrument session.
pub fn deinit(self: *Instrument) void {
    _ = self.vtable.close(self.instrument);
}

/// Checks the most recent instrument status code.
fn checkInstrumentStatus(self: *const Instrument) common.Error!void {
    try common.checkStatus(self.status);
}

/// Returns the raw VISA status reported by the most recent instrument call.
pub fn lastStatus(self: *const Instrument) ViStatus {
    return self.status;
}

/// Writes a command buffer to the instrument.
pub fn write(self: *Instrument, command: []const u8) common.Error!void {
    var ret_count: ViUInt32 = undefined;
    self.status = self.vtable.write(self.instrument, command.ptr, @intCast(command.len), &ret_count);
    try self.checkInstrumentStatus();
}

/// Reads one chunk of bytes into a caller-provided buffer.
pub fn read(self: *Instrument, buffer: []u8) common.Error![]const u8 {
    var ret_count: ViUInt32 = undefined;
    self.status = self.vtable.read(self.instrument, buffer.ptr, @intCast(buffer.len), &ret_count);
    try self.checkInstrumentStatus();
    return buffer[0..ret_count];
}

/// Streams repeated reads into a writer until VISA reports the response is complete.
pub fn readToWriter(
    self: *Instrument,
    writer: *std.Io.Writer,
    chunk_buffer: []u8,
) (common.Error || std.Io.Writer.Error)!usize {
    var total_read: usize = 0;

    while (true) {
        const chunk = try self.read(chunk_buffer);

        if (chunk.len > 0) {
            try writer.writeAll(chunk);
            total_read += chunk.len;
        }

        switch (self.status) {
            c.VI_SUCCESS, c.VI_SUCCESS_TERM_CHAR => break,
            c.VI_SUCCESS_MAX_CNT => continue,
            else => break,
        }
    }

    return total_read;
}

/// Reads a complete response into a newly allocated slice.
pub fn readToOwned(self: *Instrument, allocator: std.mem.Allocator) common.Error![]u8 {
    const chunk_size = self.options.normalizedChunkSize();
    var stack_chunk: [read_stack_chunk_bytes]u8 = undefined;

    if (chunk_size <= stack_chunk.len) {
        return self.readToOwnedWithChunk(allocator, stack_chunk[0..chunk_size]);
    }

    const chunk_buffer = try allocator.alloc(u8, chunk_size);
    defer allocator.free(chunk_buffer);
    return self.readToOwnedWithChunk(allocator, chunk_buffer);
}

fn readToOwnedWithChunk(self: *Instrument, allocator: std.mem.Allocator, chunk_buffer: []u8) common.Error![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);

    while (true) {
        const chunk = try self.read(chunk_buffer);

        if (chunk.len > 0) {
            try out.appendSlice(allocator, chunk);
        }

        switch (self.status) {
            c.VI_SUCCESS, c.VI_SUCCESS_TERM_CHAR => break,
            c.VI_SUCCESS_MAX_CNT => continue,
            else => break,
        }
    }

    trimReadTermination(&out, self.options.read_termination.constSlice());
    return out.toOwnedSlice(allocator);
}

/// Waits for the configured query delay before a follow-up read.
pub fn waitQueryDelay(self: *const Instrument) void {
    if (self.options.query_delay_ms == 0) return;
    std.Thread.sleep(@as(u64, self.options.query_delay_ms) * std.time.ns_per_ms);
}

/// Writes a command and reads the complete response using the configured query delay.
pub fn queryToOwned(self: *Instrument, allocator: std.mem.Allocator, command: []const u8) common.Error![]u8 {
    try self.write(command);
    self.waitQueryDelay();
    return self.readToOwned(allocator);
}

/// Writes a command and returns the first read chunk directly into `buffer`.
pub fn queryRaw(self: *Instrument, command: []const u8, buffer: []u8) common.Error![]const u8 {
    try self.write(command);
    self.waitQueryDelay();
    return self.read(buffer);
}

pub fn applyOptions(self: *Instrument) common.Error!void {
    if (self.options.timeout_ms) |timeout_ms| {
        try self.setAttribute(c.VI_ATTR_TMO_VALUE, @as(ViAttrState, @intCast(timeout_ms)));
    }
    try self.applyReadTermination();
}

fn applyReadTermination(self: *Instrument) common.Error!void {
    const read_termination = self.options.read_termination.constSlice();
    if (read_termination.len == 0) {
        return self.setAttribute(c.VI_ATTR_TERMCHAR_EN, 0);
    }

    try self.setAttribute(c.VI_ATTR_TERMCHAR, @as(ViAttrState, read_termination[read_termination.len - 1]));
    try self.setAttribute(c.VI_ATTR_TERMCHAR_EN, 1);
}

fn setAttribute(self: *Instrument, attr: c.ViAttr, value: ViAttrState) common.Error!void {
    self.status = self.vtable.setAttribute(self.instrument, attr, value);
    try self.checkInstrumentStatus();
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
