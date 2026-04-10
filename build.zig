const std = @import("std");
const pkg = @import("build.zig.zon");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const test_filters = b.option([]const []const u8, "test-filter", "Skip tests that do not match filters") orelse &.{};

    const serde_dep = b.dependency("serde", .{
        .target = target,
        .optimize = optimize,
    });

    const ordo_mod = b.addModule("ordo", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .link_libc = true,
    });
    ordo_mod.addIncludePath(b.path("include/"));
    ordo_mod.addImport("serde", serde_dep.module("serde"));

    const semver = std.SemanticVersion.parse(pkg.version) catch unreachable;

    // Create the executable
    const exe = b.addExecutable(.{
        .name = "ordo",
        .version = semver,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "ordo", .module = ordo_mod },
            },
            .link_libc = true,
        }),
    });

    // Inject build-time constants (exe name, version, etc.) into the CLI module.
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "exe_name", exe.name);

    build_options.addOption([]const u8, "version", pkg.version);
    exe.root_module.addOptions("build_options", build_options);

    b.installArtifact(exe);

    const clap = b.dependency("clap", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("clap", clap.module("clap"));

    const run_step = b.step("run", "Run ordo");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Tests for the ordo module
    const mod_tests = b.addTest(.{
        .root_module = ordo_mod,
        .filters = test_filters,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Tests for the executable
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
        .filters = test_filters,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
