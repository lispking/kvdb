const std = @import("std");
const root = @import("../kvdb.zig");

const Database = root.Database;
const Error = root.Error;
const KVDB_Status = root.KVDB_Status;

/// Opaque handle type for C code.
pub const KVDB_Handle = *Database;

/// Convert a Zig error into a stable C ABI status code.
pub fn kvdbStatusFromError(err: anyerror) KVDB_Status {
    return switch (err) {
        Error.InvalidArgument => .invalid_argument,
        Error.KeyNotFound => .not_found,
        Error.TransactionAlreadyActive, Error.NoActiveTransaction, Error.TransactionConflict => .transaction_conflict,
        Error.WalCorrupted, Error.WalReplayFailed => .wal_error,
        Error.DiskFull,
        Error.CorruptedData,
        Error.InvalidPageId,
        Error.PageNotFound,
        Error.PageOverflow,
        Error.KeyAlreadyExists,
        Error.NodeFull,
        Error.NodeEmpty,
        Error.IoError,
        Error.DatabaseClosed,
        error.OutOfMemory,
        => .storage_error,
        else => .internal_error,
    };
}

/// Open a database (C API).
pub fn kvdb_open(path: [*c]const u8, path_len: usize) ?KVDB_Handle {
    if (path_len == 0 or path == null) {
        return null;
    }

    const allocator = std.heap.page_allocator;
    const path_slice = path[0..path_len];

    const db = allocator.create(Database) catch return null;
    db.* = Database.open(allocator, path_slice, .{}) catch {
        allocator.destroy(db);
        return null;
    };

    return db;
}

/// Return the numeric value of one stable FFI status code.
pub fn kvdb_status_code(status: KVDB_Status) c_int {
    return @intFromEnum(status);
}

/// Close a database (C API).
pub fn kvdb_close(handle: ?KVDB_Handle) void {
    const allocator = std.heap.page_allocator;
    if (handle) |db| {
        db.close();
        allocator.destroy(db);
    }
}

/// Get a value by key (C API).
pub fn kvdb_get(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize, value_len: *usize) [*c]u8 {
    value_len.* = 0;

    if (handle == null or key == null or key_len == 0) {
        return null;
    }

    const key_slice = key[0..key_len];
    const value = handle.?.get(key_slice) catch return null;

    if (value) |v| {
        value_len.* = v.len;
        return @constCast(v.ptr);
    }
    return null;
}

/// Free a value buffer previously returned by kvdb_get.
pub fn kvdb_free(value: [*c]u8, value_len: usize) void {
    const allocator = std.heap.page_allocator;
    if (value != null) {
        allocator.free(value[0..value_len]);
    }
}

/// Store a key-value pair (C API).
pub fn kvdb_put(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize, value: [*c]const u8, value_len: usize) c_int {
    if (handle == null or key == null or key_len == 0 or value == null) {
        return @intFromEnum(KVDB_Status.invalid_argument);
    }

    const key_slice = key[0..key_len];
    const value_slice = value[0..value_len];

    handle.?.put(key_slice, value_slice) catch |err| return @intFromEnum(kvdbStatusFromError(err));
    return @intFromEnum(KVDB_Status.ok);
}

/// Delete a key (C API).
pub fn kvdb_delete(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize) c_int {
    if (handle == null or key == null or key_len == 0) {
        return @intFromEnum(KVDB_Status.invalid_argument);
    }

    const key_slice = key[0..key_len];

    handle.?.delete(key_slice) catch |err| return @intFromEnum(kvdbStatusFromError(err));
    return @intFromEnum(KVDB_Status.ok);
}
