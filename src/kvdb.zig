const std = @import("std");
const constants = @import("constants.zig");
const types = @import("kvdb/types.zig");
const maintenance = @import("kvdb/maintenance.zig");
const transaction_mod = @import("kvdb/transaction.zig");
const ffi = @import("kvdb/ffi.zig");
const database_mod = @import("kvdb/database.zig");
const _tests = @import("kvdb/tests.zig");

pub const Error = constants.Error;
/// Stable C ABI status enum shared by the exported FFI entry points.
pub const KVDB_Status = types.KVDB_Status;
/// Database open-time configuration options.
pub const Options = types.Options;
/// Opaque database handle type exposed to C callers.
pub const KVDB_Handle = ffi.KVDB_Handle;
/// Explicit transaction handle returned by `Database.beginTransaction()`.
pub const Transaction = transaction_mod.Transaction;
/// Verification summary returned by `Database.verify()`.
pub const VerifyStats = maintenance.VerifyStats;
/// Structural summary returned by `Database.inspect()`.
pub const InspectStats = maintenance.InspectStats;
/// Main database API type.
pub const Database = database_mod.Database;

/// Exported C ABI entry point for opening a database handle.
pub export fn kvdb_open(path: [*c]const u8, path_len: usize) ?KVDB_Handle {
    return ffi.kvdb_open(path, path_len);
}

/// Exported C ABI helper for retrieving stable numeric status values.
pub export fn kvdb_status_code(status: KVDB_Status) c_int {
    return ffi.kvdb_status_code(status);
}

/// Exported C ABI entry point for closing a database handle.
pub export fn kvdb_close(handle: ?KVDB_Handle) void {
    ffi.kvdb_close(handle);
}

/// Exported C ABI entry point for fetching one value buffer.
pub export fn kvdb_get(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize, value_len: *usize) [*c]u8 {
    return ffi.kvdb_get(handle, key, key_len, value_len);
}

/// Exported C ABI entry point for freeing one buffer returned by `kvdb_get`.
pub export fn kvdb_free(value: [*c]u8, value_len: usize) void {
    ffi.kvdb_free(value, value_len);
}

/// Exported C ABI entry point for storing one key/value pair.
pub export fn kvdb_put(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize, value: [*c]const u8, value_len: usize) c_int {
    return ffi.kvdb_put(handle, key, key_len, value, value_len);
}

/// Exported C ABI entry point for deleting one key.
pub export fn kvdb_delete(handle: ?KVDB_Handle, key: [*c]const u8, key_len: usize) c_int {
    return ffi.kvdb_delete(handle, key, key_len);
}

/// Convenience function to open a database with default options.
///
/// Parameters:
///   - allocator: Memory allocator
///   - path: Database file path
///
/// Returns: Initialized Database
pub fn open(allocator: std.mem.Allocator, path: []const u8) !Database {
    return Database.open(allocator, path, .{});
}
