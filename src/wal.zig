/// Public facade for the split WAL implementation.
///
/// This file keeps the stable import surface while the actual implementation
/// lives under `src/wal/`.
const core = @import("wal/core.zig");
const iterator = @import("wal/iterator.zig");
const _tests = @import("wal/tests.zig");

/// File-backed write-ahead log used for durability and recovery.
pub const Wal = core.Wal;
/// Convenience re-export of one decoded WAL record.
pub const Record = iterator.Record;
/// Convenience re-export of the sequential WAL iterator type.
pub const Iterator = iterator.Iterator;
