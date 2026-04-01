const std = @import("std");
const constants = @import("constants.zig");
const PageId = constants.PageId;
const WalRecordType = constants.WalRecordType;
const WalRecordHeader = constants.WalRecordHeader;
const Error = constants.Error;

/// CRC32 checksum calculation for WAL record integrity verification.
///
/// Uses the standard CRC32 polynomial to compute a checksum over the
/// record data. This allows detection of corrupted WAL records during
/// recovery.
///
/// Parameters:
///   - data: Byte slice to compute checksum over
///
/// Returns: 32-bit CRC checksum
fn crc32(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

/// Write-Ahead Log (WAL) for database durability.
///
/// The WAL provides crash recovery by logging all modifications before
/// they are applied to the main database file. On restart, uncommitted
/// transactions can be replayed or rolled back.
///
/// WAL Record Format:
/// [WalRecordHeader][key bytes][value bytes]
///
/// The header contains:
/// - checksum: CRC32 of record content (for corruption detection)
/// - record_type: Type of operation (insert/delete/commit/abort)
/// - key_len: Length of key data
/// - value_len: Length of value data (0 for deletes)
pub const Wal = struct {
    /// Memory allocator for internal use
    allocator: std.mem.Allocator,

    /// File handle for the WAL file
    file: std.fs.File,

    /// Path to the WAL file (stored for clear operation)
    file_path: []const u8,

    /// Current write offset in the WAL file
    current_offset: u64,

    /// Initialize a new WAL for the given database path.
    ///
    /// Opens or creates a WAL file at `{db_path}.wal`. If the file exists,
    /// the WAL is opened for appending and the offset is set to the end.
    ///
    /// Parameters:
    ///   - allocator: Memory allocator
    ///   - db_path: Path to the database file (WAL will be at db_path.wal)
    ///
    /// Returns: Initialized WAL ready for logging
    pub fn init(allocator: std.mem.Allocator, db_path: []const u8) !Wal {
        // Construct WAL file path: database.db -> database.db.wal
        const wal_path = try std.fmt.allocPrint(allocator, "{s}.wal", .{db_path});
        defer allocator.free(wal_path);

        // Open or create WAL file with read-write access
        const file = try std.fs.cwd().createFile(wal_path, .{
            .read = true,
            .truncate = false, // Don't truncate - we may need to replay existing records
        });
        errdefer file.close();

        // Get current file size to determine append offset
        const stat = try file.stat();

        return .{
            .allocator = allocator,
            .file = file,
            .file_path = try allocator.dupe(u8, wal_path),
            .current_offset = stat.size,
        };
    }

    /// Clean up resources and close the WAL file.
    pub fn deinit(self: *Wal) void {
        self.file.close();
        self.allocator.free(self.file_path);
    }

    /// Append a record to the WAL.
    ///
    /// Internal helper that constructs the record, computes checksum,
    /// and writes to disk. Forces sync to ensure durability.
    ///
    /// Parameters:
    ///   - record_type: Type of operation being logged
    ///   - key: Key data (must not be empty)
    ///   - value: Value data (null for deletes)
    fn appendRecord(self: *Wal, record_type: WalRecordType, key: []const u8, value: ?[]const u8) !void {
        const value_len: u32 = if (value) |v| @intCast(v.len) else 0;

        // Build header with placeholder checksum
        var header = WalRecordHeader{
            .checksum = 0, // Will be computed below
            .record_type = record_type,
            .key_len = @intCast(key.len),
            .value_len = value_len,
        };

        // Serialize header and data for checksum calculation
        // We exclude the checksum field itself from the checksum
        const header_bytes = std.mem.asBytes(&header);
        var checksum_data: std.ArrayList(u8) = .empty;
        defer checksum_data.deinit(self.allocator);

        // Add header content (skipping checksum field)
        try checksum_data.appendSlice(self.allocator, header_bytes[@sizeOf(u32)..]);
        try checksum_data.appendSlice(self.allocator, key);
        if (value) |v| {
            try checksum_data.appendSlice(self.allocator, v);
        }

        // Compute and store checksum
        header.checksum = crc32(checksum_data.items);

        // Write record to WAL file
        try self.file.seekFromEnd(0);

        // Write header with actual checksum
        const final_header_bytes = std.mem.asBytes(&header);
        try self.file.writeAll(final_header_bytes);

        // Write key
        try self.file.writeAll(key);

        // Write value (if present)
        if (value) |v| {
            try self.file.writeAll(v);
        }

        // CRITICAL: Force sync to disk for durability
        // Without this, a crash could lose recent writes
        try self.file.sync();

        // Update offset tracking
        self.current_offset += @sizeOf(WalRecordHeader) + key.len + value_len;
    }

    /// Log an insert operation.
    ///
    /// Records the insertion of a key-value pair.
    ///
    /// Parameters:
    ///   - key: The key being inserted
    ///   - value: The value being associated with the key
    pub fn logInsert(self: *Wal, key: []const u8, value: []const u8) !void {
        try self.appendRecord(.insert, key, value);
    }

    /// Log a delete operation.
    ///
    /// Records the deletion of a key.
    ///
    /// Parameters:
    ///   - key: The key being deleted
    pub fn logDelete(self: *Wal, key: []const u8) !void {
        try self.appendRecord(.delete, key, null);
    }

    /// Log a transaction commit.
    ///
    /// Marks that all operations in the current transaction have been
    /// successfully applied to the database.
    pub fn logCommit(self: *Wal) !void {
        try self.appendRecord(.commit, &.{}, null);
    }

    /// Log a transaction abort/rollback.
    ///
    /// Marks that the current transaction should be discarded.
    pub fn logAbort(self: *Wal) !void {
        try self.appendRecord(.abort, &.{}, null);
    }

    /// Represents a single WAL record after reading from disk.
    pub const Record = struct {
        /// Type of operation
        record_type: WalRecordType,
        /// Key data (owned, must be freed)
        key: []const u8,
        /// Value data (owned, must be freed if present)
        value: ?[]const u8,
    };

    /// Iterator for reading WAL records sequentially.
    pub const Iterator = struct {
        /// Reference to WAL being iterated
        wal: *Wal,
        /// Current read position in file
        offset: u64,

        /// Read the next record from the WAL.
        ///
        /// Validates checksum and returns the record data.
        /// Caller is responsible for freeing key and value memory.
        ///
        /// Returns: Record struct, or null at end of file
        pub fn next(self: *Iterator) !?Record {
            if (self.offset >= self.wal.current_offset) {
                return null;
            }

            try self.wal.file.seekTo(self.offset);

            // Read header
            var header: WalRecordHeader = undefined;
            const header_bytes = std.mem.asBytes(&header);
            const header_read = try self.wal.file.read(header_bytes);
            if (header_read < header_bytes.len) {
                return null; // Incomplete record
            }

            // Read key
            const key = try self.wal.allocator.alloc(u8, header.key_len);
            errdefer self.wal.allocator.free(key);

            const key_read = try self.wal.file.read(key);
            if (key_read < header.key_len) {
                self.wal.allocator.free(key);
                return Error.WalCorrupted;
            }

            // Read value (if present)
            var value: ?[]u8 = null;
            if (header.value_len > 0) {
                value = try self.wal.allocator.alloc(u8, header.value_len);
                errdefer if (value) |v| self.wal.allocator.free(v);

                const value_read = try self.wal.file.read(value.?);
                if (value_read < header.value_len) {
                    self.wal.allocator.free(key);
                    if (value) |v| self.wal.allocator.free(v);
                    return Error.WalCorrupted;
                }
            }

            // Verify checksum for corruption detection
            var checksum_data: std.ArrayList(u8) = .empty;
            defer checksum_data.deinit(self.wal.allocator);

            const header_without_checksum = std.mem.asBytes(&header)[@sizeOf(u32)..];
            try checksum_data.appendSlice(self.wal.allocator, header_without_checksum);
            try checksum_data.appendSlice(self.wal.allocator, key);
            if (value) |v| {
                try checksum_data.appendSlice(self.wal.allocator, v);
            }

            const computed_checksum = crc32(checksum_data.items);
            if (computed_checksum != header.checksum) {
                self.wal.allocator.free(key);
                if (value) |v| self.wal.allocator.free(v);
                return Error.WalCorrupted;
            }

            // Advance offset for next read
            self.offset += @sizeOf(WalRecordHeader) + header.key_len + header.value_len;

            return Record{
                .record_type = header.record_type,
                .key = key,
                .value = value,
            };
        }
    };

    /// Create an iterator for reading all WAL records.
    ///
    /// Starts from the beginning of the WAL file.
    pub fn iterator(self: *Wal) Iterator {
        return .{
            .wal = self,
            .offset = 0,
        };
    }

    /// Replay WAL records for crash recovery.
    ///
    /// Iterates through all records and calls the appropriate callback
    /// for each operation type. Used during database startup to recover
    /// uncommitted transactions.
    ///
    /// Type Parameters:
    ///   - Callback: Struct with onInsert, onDelete, onCommit, onAbort methods
    ///
    /// Parameters:
    ///   - callback: Instance of callback struct
    pub fn replay(self: *Wal, comptime Callback: type, callback: Callback) !void {
        var iter = self.iterator();

        while (try iter.next()) |record| {
            defer {
                // Clean up allocated memory for each record
                self.allocator.free(record.key);
                if (record.value) |v| self.allocator.free(v);
            }

            // Dispatch to appropriate callback based on record type
            switch (record.record_type) {
                .insert => {
                    if (record.value) |value| {
                        try callback.onInsert(record.key, value);
                    }
                },
                .delete => {
                    try callback.onDelete(record.key);
                },
                .commit => {
                    try callback.onCommit();
                },
                .abort => {
                    try callback.onAbort();
                },
            }
        }
    }

    /// Clear all WAL records.
    ///
    /// Truncates the WAL file to zero length and resets the offset.
    /// Called after a successful transaction commit to prevent
    /// replaying already-committed operations.
    pub fn clear(self: *Wal) !void {
        try self.file.setEndPos(0);
        self.current_offset = 0;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "WAL basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_wal";
    const wal_path = "test_wal.wal";

    // Cleanup any existing test files
    defer {
        std.fs.cwd().deleteFile(test_path) catch {};
        std.fs.cwd().deleteFile(wal_path) catch {};
    }

    // Test 1: Write records to WAL
    {
        var wal = try Wal.init(allocator, test_path);
        defer wal.deinit();

        try wal.logInsert("key1", "value1");
        try wal.logInsert("key2", "value2");
        try wal.logDelete("key1");
        try wal.logCommit();
    }

    // Test 2: Read back and verify records
    {
        var wal = try Wal.init(allocator, test_path);
        defer wal.deinit();

        var iter = wal.iterator();

        // Record 1: Insert key1=value1
        const record1 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.insert, record1.record_type);
        try std.testing.expectEqualStrings("key1", record1.key);
        try std.testing.expectEqualStrings("value1", record1.value.?);
        defer {
            allocator.free(record1.key);
            allocator.free(record1.value.?);
        }

        // Record 2: Insert key2=value2
        const record2 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.insert, record2.record_type);
        try std.testing.expectEqualStrings("key2", record2.key);
        try std.testing.expectEqualStrings("value2", record2.value.?);
        defer {
            allocator.free(record2.key);
            allocator.free(record2.value.?);
        }

        // Record 3: Delete key1
        const record3 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.delete, record3.record_type);
        try std.testing.expectEqualStrings("key1", record3.key);
        try std.testing.expect(record3.value == null);
        defer allocator.free(record3.key);

        // Record 4: Commit
        const record4 = (try iter.next()).?;
        try std.testing.expectEqual(WalRecordType.commit, record4.record_type);
        defer allocator.free(record4.key);
    }
}
