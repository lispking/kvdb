const std = @import("std");
const constants = @import("../constants.zig");
const wal_mod = @import("../wal.zig");

const Wal = wal_mod.Wal;
const WalRecordType = constants.WalRecordType;
const Error = constants.Error;

test "wal: basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_wal";
    const wal_path = "test_wal.wal";

    // Remove both the synthetic DB path and the WAL file so the test always
    // starts from an empty log.
    defer {
        std.fs.cwd().deleteFile(test_path) catch {};
        std.fs.cwd().deleteFile(wal_path) catch {};
    }

    // First pass appends a small transaction-shaped sequence to disk.
    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        try wal.logInsert("key1", "value1");
        try wal.logInsert("key2", "value2");
        try wal.logDelete("key1");
        try wal.logCommit();
    }

    // Second pass reopens the same WAL and verifies ordered decoding.
    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        var iter = wal.iterator();

        // The first record should be the original insert payload.
        const record1 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.insert, record1.record_type);
        try std.testing.expectEqualStrings("key1", record1.key);
        try std.testing.expectEqualStrings("value1", record1.value.?);
        defer {
            allocator.free(record1.key);
            allocator.free(record1.value.?);
        }

        // The second record should preserve the next insert exactly.
        const record2 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.insert, record2.record_type);
        try std.testing.expectEqualStrings("key2", record2.key);
        try std.testing.expectEqualStrings("value2", record2.value.?);
        defer {
            allocator.free(record2.key);
            allocator.free(record2.value.?);
        }

        // Deletes carry only the key and no value payload.
        const record3 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.delete, record3.record_type);
        try std.testing.expectEqualStrings("key1", record3.key);
        try std.testing.expect(record3.value == null);
        defer allocator.free(record3.key);

        // The trailing boundary record marks the committed batch.
        const record4 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.commit, record4.record_type);
        defer allocator.free(record4.key);
    }
}

test "wal: checksum mismatch is corruption" {
    const allocator = std.testing.allocator;
    const test_path = "test_wal_checksum";
    const wal_path = "test_wal_checksum.wal";

    // Clean up the synthetic DB path and WAL file used by this corruption test.
    defer {
        std.fs.cwd().deleteFile(test_path) catch {};
        std.fs.cwd().deleteFile(wal_path) catch {};
    }

    // Write one valid record so the corruption step mutates a realistic header.
    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        try wal.logInsert("key", "value");
    }

    // Clobber the stored checksum so iterator validation must reject the record.
    {
        var file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
        defer file.close();

        try file.seekTo(0);
        try file.writeAll(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    }

    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        var iter = wal.iterator();
        try std.testing.expectError(Error.WalCorrupted, iter.next());
    }
}

test "wal: clear truncates and resets offset" {
    const allocator = std.testing.allocator;
    const test_path = "test_wal_clear";
    const wal_path = "test_wal_clear.wal";

    defer {
        std.fs.cwd().deleteFile(test_path) catch {};
        std.fs.cwd().deleteFile(wal_path) catch {};
    }

    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        try wal.logInsert("key", "value");
        try std.testing.expect(wal.current_offset > 0);
        try wal.clear();
        try std.testing.expectEqual(@as(u64, 0), wal.current_offset);
    }

    {
        var wal = try Wal.init(allocator, test_path, .always);
        defer wal.deinit();

        try std.testing.expectEqual(@as(u64, 0), wal.current_offset);
        var iter = wal.iterator();
        try std.testing.expect((try iter.next()) == null);
    }
}
