const std = @import("std");

/// Stable C ABI status codes returned by mutating FFI calls.
pub const KVDB_Status = enum(c_int) {
    ok = 0,
    invalid_argument = 1,
    not_found = 2,
    transaction_conflict = 3,
    storage_error = 4,
    wal_error = 5,
    internal_error = 255,
};

/// Configuration options for database initialization.
pub const Options = struct {
    /// Enable Write-Ahead Logging record writing and startup recovery.
    enable_wal: bool = true,
};
