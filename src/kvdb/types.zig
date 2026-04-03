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

/// File sync policy controlling the tradeoff between durability and latency.
pub const FsyncPolicy = enum {
    /// Force commit/checkpoint writes to stable storage before returning.
    always,
    /// Preserve write ordering but rely on OS buffering instead of explicit syncs.
    batch,
};

/// Configuration options for database initialization.
pub const Options = struct {
    /// Enable Write-Ahead Logging record writing and startup recovery.
    enable_wal: bool = true,
    /// Choose whether commit/checkpoint paths force data to stable storage.
    fsync_policy: FsyncPolicy = .always,
    /// Maximum number of pages kept in the in-memory cache.
    /// Larger values reduce disk I/O at the cost of higher memory usage.
    /// Defaults to 256.
    page_cache_size: usize = 256,
};
