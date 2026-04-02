const std = @import("std");
const constants = @import("../constants.zig");
const root = @import("../kvdb.zig");
const Database = root.Database;
const Error = constants.Error;

/// Binary export header magic for import/export files.
pub const EXPORT_MAGIC = "KVDBX1";

/// Versioned header stored at the front of export/import files.
///
/// The fixed-size header lets future format revisions reject incompatible dumps
/// before any record payload bytes are consumed.
pub const ExportHeader = extern struct {
    magic: [EXPORT_MAGIC.len]u8,
    version: u8,
};

/// Stream every logical key/value pair into the binary export format.
pub fn exportToWriter(self: *Database, writer: anytype) !usize {
    try writeExportHeader(writer);

    var iter = try self.iterator();
    defer iter.deinit();
    var exported: usize = 0;
    while (try iter.next()) |entry| {
        // Export preserves exact logical key/value bytes in sorted order.
        try writeExportRecord(writer, entry.key, entry.value);
        exported += 1;
    }

    return exported;
}

/// Read the export stream and apply its logical entries as one transaction.
pub fn importFromReader(self: *Database, reader: anytype) !usize {
    try readExportHeader(reader);

    const txn = try self.beginTransaction();
    errdefer {
        if (txn.state == .active) {
            txn.abort() catch {};
        }
    }

    var imported: usize = 0;
    while (try readExportRecord(reader, self.allocator)) |record| {
        defer {
            self.allocator.free(record.key);
            self.allocator.free(record.value);
        }

        // Import reuses the normal write path so validation, update semantics,
        // and WAL behavior remain identical to ordinary client writes.
        try self.put(record.key, record.value);
        imported += 1;
    }

    try txn.commit();
    return imported;
}

/// Write the fixed export header before streaming records.
pub fn writeExportHeader(writer: anytype) !void {
    const header = ExportHeader{
        .magic = EXPORT_MAGIC.*,
        .version = 1,
    };
    try writerWriteAll(writer, std.mem.asBytes(&header));
}

/// Read and validate the fixed export header from an import stream.
pub fn readExportHeader(reader: anytype) !void {
    const header = try readerReadStruct(reader, ExportHeader);

    if (!std.mem.eql(u8, &header.magic, EXPORT_MAGIC) or header.version != 1) {
        return Error.InvalidArgument;
    }
}

/// Write one key/value record into the export stream.
///
/// Length prefixes keep the format binary-safe for arbitrary key/value bytes.
pub fn writeExportRecord(writer: anytype, key: []const u8, value: []const u8) !void {
    try writerWriteInt(writer, u16, @intCast(key.len), .little);
    try writerWriteInt(writer, u32, @intCast(value.len), .little);
    try writerWriteAll(writer, key);
    try writerWriteAll(writer, value);
}

/// Read one key/value record from the import stream.
pub fn readExportRecord(reader: anytype, allocator: std.mem.Allocator) !?struct { key: []u8, value: []u8 } {
    const key_len = readerReadInt(reader, u16, .little) catch |err| switch (err) {
        error.EndOfStream => return null,
        else => return err,
    };
    const value_len = try readerReadInt(reader, u32, .little);

    if (key_len == 0 or key_len > constants.MAX_KEY_SIZE or value_len > constants.MAX_VALUE_SIZE) {
        return Error.InvalidArgument;
    }

    const key = try allocator.alloc(u8, key_len);
    errdefer allocator.free(key);
    const value = try allocator.alloc(u8, value_len);
    errdefer allocator.free(value);

    try readerReadNoEof(reader, key);
    try readerReadNoEof(reader, value);

    return .{ .key = key, .value = value };
}

/// Bridge old and new Zig writer APIs behind one helper.
fn writerWriteAll(writer: anytype, bytes: []const u8) !void {
    const WriterType = switch (@typeInfo(@TypeOf(writer))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(writer),
    };
    if (@hasDecl(WriterType, "writeAll")) {
        try writer.writeAll(bytes);
    } else {
        @compileError("unsupported writer type");
    }
}

/// Bridge old and new Zig writer integer APIs behind one helper.
fn writerWriteInt(writer: anytype, comptime T: type, value: T, endian: std.builtin.Endian) !void {
    const WriterType = switch (@typeInfo(@TypeOf(writer))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(writer),
    };
    if (@hasDecl(WriterType, "writeInt")) {
        try writer.writeInt(T, value, endian);
    } else {
        @compileError("unsupported writer type");
    }
}

/// Bridge old and new Zig reader APIs for exact byte reads.
fn readerReadNoEof(reader: anytype, buffer: []u8) !void {
    const ReaderType = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(reader),
    };
    if (@hasDecl(ReaderType, "readNoEof")) {
        try reader.readNoEof(buffer);
    } else if (@hasDecl(ReaderType, "take")) {
        const bytes = try reader.take(buffer.len);
        @memcpy(buffer, bytes);
    } else {
        @compileError("unsupported reader type");
    }
}

/// Bridge old and new Zig reader integer APIs behind one helper.
fn readerReadInt(reader: anytype, comptime T: type, endian: std.builtin.Endian) !T {
    const ReaderType = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(reader),
    };
    if (@hasDecl(ReaderType, "readInt")) {
        return try reader.readInt(T, endian);
    }
    if (@hasDecl(ReaderType, "takeInt")) {
        return try reader.takeInt(T, endian);
    }
    @compileError("unsupported reader type");
}

/// Bridge old and new Zig reader struct APIs behind one helper.
fn readerReadStruct(reader: anytype, comptime T: type) !T {
    const ReaderType = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(reader),
    };
    if (@hasDecl(ReaderType, "takeStruct")) {
        return try reader.takeStruct(T, .little);
    }

    var value: T = undefined;
    try readerReadNoEof(reader, std.mem.asBytes(&value));
    return value;
}
