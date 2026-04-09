const DriverRegistry = @This();

const std = @import("std");
const serde = @import("serde");
const Driver = @import("Driver.zig");
const doc_parse = @import("../doc_parse.zig");
const parse_mod = @import("parse.zig");
const testing = @import("../testing.zig");

const max_index_file_size: usize = 256 * 1024;
const index_cache_file_name = "index.json";
const IndexCacheDoc = std.StringHashMap([]const u8);

/// Entry stored in the driver index cache.
const Entry = struct {
    /// Driver document file name within the registry directory.
    file_name: []const u8,
};

const EntriesMap = std.StringHashMap(Entry);

/// Registry that discovers driver files and maintains a self-healing disk index.
allocator: std.mem.Allocator,
dir: std.fs.Dir,
entries: EntriesMap,

/// Opens a registry for `dir_path`, loading or rebuilding the on-disk index cache.
pub fn init(allocator: std.mem.Allocator, dir_path: []const u8) !DriverRegistry {
    var registry = DriverRegistry{
        .allocator = allocator,
        .dir = try openRegistryDir(dir_path),
        .entries = EntriesMap.init(allocator),
    };
    errdefer registry.deinit();

    registry.loadIndex() catch {
        try registry.rebuild();
    };
    return registry;
}

/// Releases the registry directory handle and cached index entries.
pub fn deinit(self: *DriverRegistry) void {
    freeRegistryEntries(self.allocator, &self.entries);
    self.dir.close();
}

/// Parses a driver document by canonical name using the current registry index.
pub fn parseDriverByName(self: *const DriverRegistry, allocator: std.mem.Allocator, name: []const u8) !Driver {
    const entry = self.entries.get(name) orelse return error.DriverNotFound;
    return try loadVerifiedDriverByFileName(allocator, name, self.dir, entry.file_name);
}

/// Rebuilds the on-disk driver index by scanning the registry directory.
pub fn rebuild(self: *DriverRegistry) !void {
    var fresh_entries = self.newEntriesMap();
    errdefer freeRegistryEntries(self.allocator, &fresh_entries);

    var scan_dir = try self.dir.openDir(".", .{ .iterate = true });
    defer scan_dir.close();

    var it = scan_dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.eql(u8, entry.name, index_cache_file_name)) continue;
        _ = doc_parse.detectFormat(entry.name) orelse continue;

        var loaded = try parse_mod.parseDriverInDir(self.allocator, self.dir, entry.name);
        defer loaded.deinit();

        if (fresh_entries.contains(loaded.meta.name)) return error.DuplicateDriverName;

        const key = try self.allocator.dupe(u8, loaded.meta.name);
        errdefer self.allocator.free(key);

        const file_name = try self.allocator.dupe(u8, entry.name);
        errdefer self.allocator.free(file_name);

        try fresh_entries.put(key, .{ .file_name = file_name });
    }

    try writeIndex(self.allocator, self.dir, &fresh_entries);
    self.replaceRegistryEntriesWith(fresh_entries);
}

/// Loads cached registry entries from `index.json`.
fn loadIndex(self: *DriverRegistry) !void {
    var scratch_arena = std.heap.ArenaAllocator.init(self.allocator);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();

    const content = try self.dir.readFileAlloc(scratch, index_cache_file_name, max_index_file_size);
    const parsed = try serde.json.fromSlice(IndexCacheDoc, scratch, content);

    var fresh_entries = self.newEntriesMap();
    errdefer freeRegistryEntries(self.allocator, &fresh_entries);

    var it = parsed.iterator();
    while (it.next()) |entry| {
        const key = try self.allocator.dupe(u8, entry.key_ptr.*);
        errdefer self.allocator.free(key);

        const file_name = try self.allocator.dupe(u8, entry.value_ptr.*);
        errdefer self.allocator.free(file_name);

        try fresh_entries.put(key, .{ .file_name = file_name });
    }

    self.replaceRegistryEntriesWith(fresh_entries);
}

/// Swaps in a freshly built entries map and frees the previous one.
fn replaceRegistryEntriesWith(self: *DriverRegistry, fresh_entries: EntriesMap) void {
    freeRegistryEntries(self.allocator, &self.entries);
    self.entries = fresh_entries;
}

/// Constructs an empty entries map using the registry allocator.
fn newEntriesMap(self: *const DriverRegistry) EntriesMap {
    return EntriesMap.init(self.allocator);
}

/// Frees all keys and file names stored in an entries map.
fn freeRegistryEntries(allocator: std.mem.Allocator, entries: *EntriesMap) void {
    var it = entries.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        allocator.free(entry.value_ptr.file_name);
    }
    entries.deinit();
}

/// Opens the registry directory from either an absolute or cwd-relative path.
fn openRegistryDir(dir_path: []const u8) !std.fs.Dir {
    if (std.fs.path.isAbsolute(dir_path)) {
        return std.fs.openDirAbsolute(dir_path, .{});
    }

    return std.fs.cwd().openDir(dir_path, .{});
}

/// Loads a driver from a verified file name and requires the metadata name to match `expected_name`.
fn loadVerifiedDriverByFileName(
    allocator: std.mem.Allocator,
    expected_name: []const u8,
    dir: std.fs.Dir,
    file_name: []const u8,
) !Driver {
    var loaded = try parse_mod.parseDriverInDir(allocator, dir, file_name);
    errdefer loaded.deinit();

    if (!std.mem.eql(u8, loaded.meta.name, expected_name)) return error.DriverNotFound;
    return loaded;
}

/// Writes the current registry entries to the on-disk JSON index cache.
fn writeIndex(allocator: std.mem.Allocator, dir: std.fs.Dir, entries: *const EntriesMap) !void {
    var cache_doc = IndexCacheDoc.init(allocator);
    defer cache_doc.deinit();

    var it = entries.iterator();
    while (it.next()) |entry| {
        try cache_doc.put(entry.key_ptr.*, entry.value_ptr.file_name);
    }

    const encoded = try serde.json.toSlice(allocator, cache_doc);
    defer allocator.free(encoded);

    try dir.writeFile(.{ .sub_path = index_cache_file_name, .data = encoded });
}

test "driver registry creates index for non-stem file names" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/bench_supply.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": "1.2.3",
        \\    "description": "bench supply"
        \\  },
        \\  "commands": {}
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectEqual(@as(usize, 1), registry.entries.count());

    const entry = registry.entries.get("psu") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("bench_supply.json", entry.file_name);

    const index_bytes = try workspace.readFileAlloc(gpa, "drivers/index.json", max_index_file_size);
    defer gpa.free(index_bytes);
    try std.testing.expect(std.mem.containsAtLeast(u8, index_bytes, 1, "\"psu\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, index_bytes, 1, "bench_supply.json"));
}

test "driver registry rebuild refreshes stale file path" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage},(@{channels})",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try workspace.rename("drivers/vendor_psu_set_voltage.json", "drivers/renamed_psu.json");
    try registry.rebuild();

    var loaded = try registry.parseDriverByName(gpa, "psu");
    defer loaded.deinit();

    try std.testing.expectEqualStrings("psu", loaded.meta.name);
    const entry = registry.entries.get("psu") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("renamed_psu.json", entry.file_name);
}

test "driver registry rebuild refreshes when new driver is added" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage},(@{channels})",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try workspace.writeFile("drivers/vendor_dmm_measure_voltage.json",
        \\{
        \\  "metadata": {
        \\    "name": "dmm",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "measure_voltage": {
        \\      "write": "MEAS:VOLT?",
        \\      "read": "float"
        \\    }
        \\  }
        \\}
    );
    try registry.rebuild();

    var loaded = try registry.parseDriverByName(gpa, "dmm");
    defer loaded.deinit();

    try std.testing.expectEqualStrings("dmm", loaded.meta.name);
    const entry = registry.entries.get("dmm") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("vendor_dmm_measure_voltage.json", entry.file_name);
}

test "driver registry rebuilds corrupted index cache" {
    const gpa = std.testing.allocator;

    var workspace = testing.TestWorkspace.init(gpa);
    defer workspace.deinit();

    try workspace.writeFile("drivers/vendor_psu_set_voltage.json",
        \\{
        \\  "metadata": {
        \\    "name": "psu",
        \\    "version": null,
        \\    "description": null
        \\  },
        \\  "commands": {
        \\    "set_voltage": {
        \\      "write": "VOLT {voltage},(@{channels})",
        \\      "read": null
        \\    }
        \\  }
        \\}
    );
    try workspace.writeFile("drivers/index.json", "{not valid json");

    const driver_dir = try workspace.realpathAlloc("drivers");
    defer gpa.free(driver_dir);

    var registry = try DriverRegistry.init(gpa, driver_dir);
    defer registry.deinit();

    try std.testing.expectEqual(@as(usize, 1), registry.entries.count());

    var loaded = try registry.parseDriverByName(gpa, "psu");
    defer loaded.deinit();
    try std.testing.expectEqualStrings("psu", loaded.meta.name);

    const rebuilt = try workspace.readFileAlloc(gpa, "drivers/index.json", max_index_file_size);
    defer gpa.free(rebuilt);

    try std.testing.expect(std.mem.containsAtLeast(u8, rebuilt, 1, "\"psu\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, rebuilt, 1, "vendor_psu_set_voltage.json"));
}
