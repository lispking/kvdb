const std = @import("std");
const constants = @import("../constants.zig");
const iterator_mod = @import("iterator.zig");

const WalRecordType = constants.WalRecordType;
const WalRecordHeader = constants.WalRecordHeader;
const FsyncPolicy = @import("../kvdb/types.zig").FsyncPolicy;

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
pub fn crc32(data: []const u8) u32 {
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
    /// Represents a single WAL record after reading from disk.
    pub const Record = iterator_mod.Record;
    /// Iterator for reading WAL records sequentially.
    pub const Iterator = iterator_mod.Iterator;

    /// Memory allocator for internal use
    allocator: std.mem.Allocator,

    /// File handle for the WAL file
    file: std.fs.File,

    /// Path to the WAL file (stored for clear operation)
    file_path: []const u8,

    /// Current write offset in the WAL file
    current_offset: u64,

    /// Controls whether WAL durability boundaries force a file sync.
    fsync_policy: FsyncPolicy,

    /// Initialize a new WAL for the given database path.
    ///
    /// Opens or creates a WAL file at `{db_path}.wal`. If the file exists,
    /// the WAL is opened for appending and the offset is set to the end.
    ///
    /// Parameters:
    ///   - allocator: Memory allocator
    ///   - db_path: Path to the database file (WAL will be at db_path.wal)
    ///   - fsync_policy: Whether WAL durability boundaries force a file sync
    ///
    /// Returns: Initialized WAL ready for logging
    pub fn init(allocator: std.mem.Allocator, db_path: []const u8, fsync_policy: FsyncPolicy) !Wal {
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
            .fsync_policy = fsync_policy,
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
    /// and appends it to the WAL. Durability boundaries are chosen by callers.
    ///
    /// Parameters:
    ///   - record_type: Type of operation being logged
    ///   - key: Key data (must not be empty)
    ///   - value: Value data (null for deletes)
    fn appendRecord(self: *Wal, record_type: WalRecordType, key: []const u8, value: ?[]const u8) !void {
        const value_len: u32 = if (value) |v| @intCast(v.len) else 0;

        // Build header with placeholder checksum
        var header = WalRecordHeader{
            .checksum = 0,
            .record_type = record_type,
            .key_len = @intCast(key.len),
            .value_len = value_len,
        };

        // Compute CRC32 using stack buffer (no heap allocation).
        // Max record: sizeof(header_without_checksum)=11 + MAX_KEY_SIZE(1024) + MAX_VALUE_SIZE(2048) = 3083
        var record_buf: [4096]u8 = undefined;
        const header_without_checksum = std.mem.asBytes(&header)[@sizeOf(u32)..];
        var stream = std.io.fixedBufferStream(&record_buf);
        try stream.writer().writeAll(header_without_checksum);
        try stream.writer().writeAll(key);
        if (value) |v| {
            try stream.writer().writeAll(v);
        }
        header.checksum = crc32(stream.getWritten());

        // Serialize full record into the stack buffer and write in one syscall.
        var full_stream = std.io.fixedBufferStream(&record_buf);
        const w = full_stream.writer();
        try w.writeAll(std.mem.asBytes(&header));
        try w.writeAll(key);
        if (value) |v| {
            try w.writeAll(v);
        }

        try self.file.writeAll(full_stream.getWritten());

        // Update offset tracking
        self.current_offset += @sizeOf(WalRecordHeader) + key.len + value_len;
    }

    /// Force the current WAL contents to stable storage when the active policy
    /// requires an explicit durability boundary.
    pub fn sync(self: *Wal) !void {
        if (self.fsync_policy == .always) {
            try self.file.sync();
        }
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
    /// Writes a commit boundary record to the WAL. The caller is responsible
    /// for coordinating this marker with page flushing and WAL clearing.
    pub fn logCommit(self: *Wal) !void {
        try self.appendRecord(.commit, &.{}, null);
    }

    /// Log a transaction abort/rollback.
    ///
    /// Writes an abort boundary record to the WAL. The caller is responsible
    /// for deciding how in-memory state is discarded or restored.
    pub fn logAbort(self: *Wal) !void {
        try self.appendRecord(.abort, &.{}, null);
    }

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
    /// for each operation type. Recovery policy is decided by the caller,
    /// which can buffer operations until `commit`, discard them on `abort`,
    /// or fail fast on corruption.
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
                // Clean up allocated memory for each record.
                self.allocator.free(record.key);
                if (record.value) |v| self.allocator.free(v);
            }

            // Dispatch to the matching recovery callback for this record type.
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
    /// Truncates the WAL file to zero length, optionally syncs the truncation,
    /// and resets the offset. Called after a successful checkpoint so reopened
    /// processes do not observe stale committed batches again.
    pub fn clear(self: *Wal) !void {
        try self.file.setEndPos(0);
        if (self.fsync_policy == .always) {
            try self.file.sync();
        }
        self.current_offset = 0;
    }
};
