const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module
    const kvdb_mod = b.createModule(.{
        .root_source_file = b.path("src/kvdb.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Library
    const lib = b.addLibrary(.{
        .name = "kvdb",
        .root_module = kvdb_mod,
    });
    b.installArtifact(lib);

    // CLI executable
    const exe = b.addExecutable(.{
        .name = "kvdb-cli",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("kvdb", kvdb_mod);
    b.installArtifact(exe);

    // Tests
    const lib_tests = b.addTest(.{
        .root_module = kvdb_mod,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_lib_tests.step);

    // Example
    const example = b.addExecutable(.{
        .name = "example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/basic.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    example.root_module.addImport("kvdb", kvdb_mod);
    b.installArtifact(example);

    // Install git hooks
    const install_hooks = b.step("install-hooks", "Install git pre-commit hook");

    const install_precommit = b.addInstallFile(
        b.path("scripts/pre-commit"),
        ".git/hooks/pre-commit",
    );
    install_hooks.dependOn(&install_precommit.step);

    // Make hook executable (using a custom step)
    const make_executable = b.addSystemCommand(&.{
        "chmod", "+x", ".git/hooks/pre-commit",
    });
    make_executable.step.dependOn(&install_precommit.step);
    install_hooks.dependOn(&make_executable.step);
}
