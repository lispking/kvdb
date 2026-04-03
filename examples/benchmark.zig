const std = @import("std");
const kvdb = @import("kvdb");

const Workload = enum {
    sequential_inserts,
    random_inserts,
    point_lookups,
    scans,
    updates,
    deletes,
    compaction,
};

const BenchPolicy = enum {
    always,
    batch,
};

const BenchResult = struct {
    policy: BenchPolicy,
    workload: Workload,
    operations: usize,
    elapsed_ns: u64,
};

const DefaultConfig = struct {
    operation_count: usize = 64,
    scan_repetitions: usize = 20,
    key_space: usize = 64,
    seed: u64 = 0xC0FFEE,
};

/// Run a reproducible suite of storage microbenchmarks against the current engine.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = DefaultConfig{};
    const root_dir = "/tmp/kvdb_bench";
    std.fs.cwd().makePath(root_dir) catch {};

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print("KVDB benchmark suite\n", .{});
    try stdout.print("  operations: {d}\n", .{config.operation_count});
    try stdout.print("  scan repetitions: {d}\n", .{config.scan_repetitions});
    try stdout.print("  key space: {d}\n", .{config.key_space});
    try stdout.print("  seed: 0x{x}\n\n", .{config.seed});

    const policies = [_]BenchPolicy{ .always, .batch };
    for (policies) |policy| {
        const results = [_]BenchResult{
            try runSequentialInsertBenchmark(allocator, root_dir, config, policy),
            try runRandomInsertBenchmark(allocator, root_dir, config, policy),
            try runPointLookupBenchmark(allocator, root_dir, config, policy),
            try runScanBenchmark(allocator, root_dir, config, policy),
            try runUpdateBenchmark(allocator, root_dir, config, policy),
            try runDeleteBenchmark(allocator, root_dir, config, policy),
            try runCompactionBenchmark(allocator, root_dir, config, policy),
        };

        try stdout.print("policy: {s}\n", .{policyName(policy)});
        try stdout.print("{s: <10} {s: <20} {s: >12} {s: >14} {s: >16}\n", .{ "policy", "workload", "operations", "elapsed ms", "ops/sec" });
        for (results) |result| {
            const elapsed_ms = @as(f64, @floatFromInt(result.elapsed_ns)) / std.time.ns_per_ms;
            const ops_per_sec = if (result.elapsed_ns == 0)
                0.0
            else
                @as(f64, @floatFromInt(result.operations)) * std.time.ns_per_s / @as(f64, @floatFromInt(result.elapsed_ns));
            try stdout.print("{s: <10} {s: <20} {d: >12} {d: >14.3} {d: >16.2}\n", .{
                policyName(result.policy),
                workloadName(result.workload),
                result.operations,
                elapsed_ms,
                ops_per_sec,
            });
        }
        try stdout.print("\n", .{});
    }
    try stdout.flush();
}

/// Return a readable stable label for each benchmark row.
fn workloadName(workload: Workload) []const u8 {
    return switch (workload) {
        .sequential_inserts => "sequential-insert",
        .random_inserts => "random-insert",
        .point_lookups => "point-lookup",
        .scans => "scan",
        .updates => "update",
        .deletes => "delete",
        .compaction => "compact",
    };
}

/// Return a readable label for each fsync policy run.
fn policyName(policy: BenchPolicy) []const u8 {
    return switch (policy) {
        .always => "always",
        .batch => "batch",
    };
}

/// Map benchmark policy labels onto the public database option enum.
fn fsyncPolicy(policy: BenchPolicy) kvdb.FsyncPolicy {
    return switch (policy) {
        .always => .always,
        .batch => .batch,
    };
}

/// Run inserts with sorted keys to measure append-like tree growth.
fn runSequentialInsertBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "sequential_insert", policy);
    defer db.close();

    const start = std.time.nanoTimestamp();
    try insertRange(&db, 0, config.operation_count);
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .sequential_inserts, .operations = config.operation_count, .elapsed_ns = elapsed_ns };
}

/// Run inserts with a deterministic shuffled key order so results stay reproducible.
fn runRandomInsertBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "random_insert", policy);
    defer db.close();

    const ids = try allocator.alloc(usize, config.operation_count);
    defer allocator.free(ids);
    for (ids, 0..) |*slot, i| {
        slot.* = i;
    }

    var prng = std.Random.DefaultPrng.init(config.seed);
    const random = prng.random();
    random.shuffle(usize, ids);

    const start = std.time.nanoTimestamp();
    for (ids) |id| {
        try putNumbered(&db, id, id);
    }
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .random_inserts, .operations = config.operation_count, .elapsed_ns = elapsed_ns };
}

/// Measure repeated point reads from a preloaded key space.
fn runPointLookupBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "point_lookup", policy);
    defer db.close();
    try insertRange(&db, 0, config.key_space);

    var prng = std.Random.DefaultPrng.init(config.seed ^ 0x1111);
    const random = prng.random();

    const start = std.time.nanoTimestamp();
    for (0..config.operation_count) |_| {
        const id = random.intRangeLessThan(usize, 0, config.key_space);
        const value = try getNumbered(&db, allocator, id);
        defer allocator.free(value.?);
    }
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .point_lookups, .operations = config.operation_count, .elapsed_ns = elapsed_ns };
}

/// Measure full-tree iteration cost by scanning the same loaded dataset repeatedly.
fn runScanBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "scan", policy);
    defer db.close();
    try insertRange(&db, 0, config.key_space);

    const start = std.time.nanoTimestamp();
    for (0..config.scan_repetitions) |_| {
        var iter = try db.iterator();
        defer iter.deinit();
        var seen: usize = 0;
        while (try iter.next()) |_| {
            seen += 1;
        }
        try std.testing.expectEqual(config.key_space, seen);
    }
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .scans, .operations = config.key_space * config.scan_repetitions, .elapsed_ns = elapsed_ns };
}

/// Measure overwrite-heavy traffic on an existing dataset.
fn runUpdateBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "update", policy);
    defer db.close();
    try insertRange(&db, 0, config.key_space);

    const start = std.time.nanoTimestamp();
    for (0..config.operation_count) |i| {
        try putNumbered(&db, i % config.key_space, config.key_space + i);
    }
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .updates, .operations = config.operation_count, .elapsed_ns = elapsed_ns };
}

/// Measure deleting a deterministic prefix from a preloaded dataset.
fn runDeleteBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "delete", policy);
    defer db.close();
    try insertRange(&db, 0, config.key_space);

    const delete_count = @min(config.operation_count, config.key_space);
    const start = std.time.nanoTimestamp();
    for (0..delete_count) |i| {
        try deleteNumbered(&db, i);
    }
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .deletes, .operations = delete_count, .elapsed_ns = elapsed_ns };
}

/// Measure end-to-end compaction after churn leaves dead space behind.
fn runCompactionBenchmark(allocator: std.mem.Allocator, root_dir: []const u8, config: DefaultConfig, policy: BenchPolicy) !BenchResult {
    var db = try openBenchDb(allocator, root_dir, "compact", policy);
    defer db.close();
    try insertRange(&db, 0, config.key_space);

    // Create a predictable mix of updates and deletes before compaction.
    for (0..config.key_space / 2) |i| {
        try putNumbered(&db, i, config.key_space * 2 + i);
    }
    for (0..config.key_space / 4) |i| {
        try deleteNumbered(&db, i * 2);
    }

    const start = std.time.nanoTimestamp();
    _ = try db.compact();
    const elapsed_ns = elapsedSince(start);

    return .{ .policy = policy, .workload = .compaction, .operations = 1, .elapsed_ns = elapsed_ns };
}

/// Open a fresh benchmark database under the shared temp root.
fn openBenchDb(allocator: std.mem.Allocator, root_dir: []const u8, name: []const u8, policy: BenchPolicy) !kvdb.Database {
    const db_path = try std.fmt.allocPrint(allocator, "{s}/{s}_{s}.db", .{ root_dir, name, policyName(policy) });
    defer allocator.free(db_path);
    const wal_path = try std.fmt.allocPrint(allocator, "{s}.wal", .{db_path});
    defer allocator.free(wal_path);

    std.fs.cwd().deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};

    return kvdb.Database.open(allocator, db_path, .{ .fsync_policy = fsyncPolicy(policy) });
}

/// Insert a contiguous range of numeric keys and values.
fn insertRange(db: *kvdb.Database, start_id: usize, count: usize) !void {
    for (0..count) |offset| {
        const id = start_id + offset;
        try putNumbered(db, id, id);
    }
}

/// Put one deterministic key/value pair using stack buffers to avoid allocator noise.
fn putNumbered(db: *kvdb.Database, key_id: usize, value_id: usize) !void {
    var key_buf: [32]u8 = undefined;
    var value_buf: [48]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "bench-key-{d:0>8}", .{key_id});
    const value = try std.fmt.bufPrint(&value_buf, "bench-value-{d:0>12}", .{value_id});
    try db.put(key, value);
}

/// Get one deterministic key and return the owned value copy.
fn getNumbered(db: *kvdb.Database, allocator: std.mem.Allocator, key_id: usize) !?[]const u8 {
    _ = allocator;
    var key_buf: [32]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "bench-key-{d:0>8}", .{key_id});
    return db.get(key);
}

/// Delete one deterministic key.
fn deleteNumbered(db: *kvdb.Database, key_id: usize) !void {
    var key_buf: [32]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "bench-key-{d:0>8}", .{key_id});
    try db.delete(key);
}

/// Convert a start timestamp into elapsed nanoseconds.
fn elapsedSince(start_ns: i128) u64 {
    return @intCast(std.time.nanoTimestamp() - start_ns);
}
