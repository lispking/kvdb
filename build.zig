const std = @import("std");

/// Read version from build.zig.zon
fn readVersion() []const u8 {
    const zon = @import("build.zig.zon");
    return zon.version;
}

/// Get git commit id (short format)
fn getGitCommit(b: *std.Build) []const u8 {
    const result = std.process.Child.run(.{
        .allocator = b.allocator,
        .argv = &.{ "git", "rev-parse", "--short", "HEAD" },
    }) catch return "unknown";

    if (result.term.Exited != 0) return "unknown";

    // Remove trailing newline
    const commit = result.stdout;
    if (commit.len > 0 and commit[commit.len - 1] == '\n') {
        return commit[0 .. commit.len - 1];
    }
    return commit;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Read version from build.zig.zon
    const version = readVersion();

    // Get git commit id
    const git_commit = getGitCommit(b);

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

    // Create options module for version info
    const options = b.addOptions();
    options.addOption([]const u8, "version", version);
    options.addOption([]const u8, "git_commit", git_commit);

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
    exe.root_module.addOptions("config", options);
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

    // Benchmarks
    const benchmark = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/benchmark.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    benchmark.root_module.addImport("kvdb", kvdb_mod);
    b.installArtifact(benchmark);

    const run_benchmark = b.addRunArtifact(benchmark);
    const benchmark_step = b.step("bench", "Run benchmark suite");
    benchmark_step.dependOn(&run_benchmark.step);

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
