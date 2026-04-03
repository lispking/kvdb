const std = @import("std");
const constants = @import("../constants.zig");
const core = @import("core.zig");

const WalRecordType = constants.WalRecordType;
const WalRecordHeader = constants.WalRecordHeader;
const Error = constants.Error;

/// Represents a single WAL record after reading from disk.
pub const Record = struct {
    /// Type of operation.
    record_type: WalRecordType,
    /// Key data owned by the caller after iteration.
    key: []const u8,
    /// Optional value data owned by the caller after iteration.
    value: ?[]const u8,
};

/// Iterator for reading WAL records sequentially.
pub const Iterator = struct {
    /// Reference to WAL being iterated.
    wal: *core.Wal,
    /// Current read position in file.
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

        // Read header.
        var header: WalRecordHeader = undefined;
        const header_bytes = std.mem.asBytes(&header);
        const header_read = try self.wal.file.read(header_bytes);
        if (header_read < header_bytes.len) {
            // A short header read at EOF is treated as an incomplete tail.
            // Higher-level recovery policy decides whether that is ignored
            // or surfaced as an error for the current workload.
            return null;
        }

        // Read key.
        const key = try self.wal.allocator.alloc(u8, header.key_len);
        var value: ?[]u8 = null;
        var owns_record_buffers = true;
        defer {
            if (owns_record_buffers) {
                self.wal.allocator.free(key);
                if (value) |v| self.wal.allocator.free(v);
            }
        }

        const key_read = try self.wal.file.read(key);
        if (key_read < header.key_len) {
            return Error.WalCorrupted;
        }

        // Read value (if present).
        if (header.value_len > 0) {
            value = try self.wal.allocator.alloc(u8, header.value_len);

            const value_read = try self.wal.file.read(value.?);
            if (value_read < header.value_len) {
                return Error.WalCorrupted;
            }
        }

        // Rebuild the protected payload exactly as the writer hashed it.
        // Use stack buffer to avoid heap allocation during CRC validation.
        var crc_buf: [4096]u8 = undefined;
        var stream = std.io.fixedBufferStream(&crc_buf);
        const header_without_checksum = std.mem.asBytes(&header)[@sizeOf(u32)..];
        try stream.writer().writeAll(header_without_checksum);
        try stream.writer().writeAll(key);
        if (value) |v| {
            try stream.writer().writeAll(v);
        }

        const computed_checksum = core.crc32(stream.getWritten());
        if (computed_checksum != header.checksum) {
            return Error.WalCorrupted;
        }

        // Advance offset for the next record only after validation succeeds.
        self.offset += @sizeOf(WalRecordHeader) + header.key_len + header.value_len;
        owns_record_buffers = false;

        return Record{
            .record_type = header.record_type,
            .key = key,
            .value = value,
        };
    }
};
