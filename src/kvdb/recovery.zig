const std = @import("std");
const root = @import("../kvdb.zig");
const constants = @import("../constants.zig");

const Database = root.Database;
const Error = root.Error;

/// Buffered WAL operation used during startup recovery.
pub const PendingWalOp = union(enum) {
    insert: struct {
        key: []u8,
        value: []u8,
    },
    delete: []u8,
};

/// Free buffered WAL operations accumulated during recovery.
pub fn clearPendingWalOps(allocator: std.mem.Allocator, pending: *std.ArrayList(PendingWalOp)) void {
    for (pending.items) |op| {
        switch (op) {
            .insert => |entry| {
                allocator.free(entry.key);
                allocator.free(entry.value);
            },
            .delete => |key| allocator.free(key),
        }
    }
    pending.clearRetainingCapacity();
}

/// Apply an insert/update directly to the tree without writing a WAL record.
///
/// Recovery uses the same update semantics as live writes so replaying repeated
/// inserts for one key still benefits from dense leaf repacking.
pub fn applyInsertNoWal(self: *Database, key: []const u8, value: []const u8) !void {
    if (key.len == 0 or key.len > constants.MAX_KEY_SIZE) {
        return Error.InvalidArgument;
    }
    try Database.validateValue(value);

    const exists = try self.contains(key);
    if (exists) {
        // Mirror live overwrite semantics so replay does not create duplicate keys.
        try self.btree.delete(&self.pager, key);
    }

    try self.btree.put(&self.pager, key, value);
}

/// Apply a delete directly to the tree without writing a WAL record.
///
/// Missing keys are ignored here because an earlier committed batch may already
/// have removed them before the remaining WAL tail is replayed.
pub fn applyDeleteNoWal(self: *Database, key: []const u8) !void {
    if (key.len == 0) {
        return Error.InvalidArgument;
    }

    self.btree.delete(&self.pager, key) catch |err| switch (err) {
        Error.KeyNotFound => {},
        else => return err,
    };
}

/// Replay any existing WAL records during startup.
///
/// Recovery buffers operations until commit markers are seen so incomplete or
/// aborted batches can be discarded without mutating the tree.
pub fn recoverFromWal(self: *Database) !void {
    if (self.wal == null) {
        return;
    }

    var wal_ref = &self.wal.?;
    if (wal_ref.current_offset == 0) {
        return;
    }

    const RecoveryCallback = struct {
        db: *Database,
        pending: *std.ArrayList(PendingWalOp),
        allocator: std.mem.Allocator,

        pub fn onInsert(callback_self: @This(), key: []const u8, value: []const u8) !void {
            // Copy record payloads out of the WAL iterator so they remain
            // valid until we either commit or discard the buffered batch.
            try callback_self.pending.append(callback_self.allocator, .{
                .insert = .{
                    .key = try callback_self.allocator.dupe(u8, key),
                    .value = try callback_self.allocator.dupe(u8, value),
                },
            });
        }

        pub fn onDelete(callback_self: @This(), key: []const u8) !void {
            // Buffer deletes until the transaction boundary is known.
            try callback_self.pending.append(callback_self.allocator, .{
                .delete = try callback_self.allocator.dupe(u8, key),
            });
        }

        pub fn onCommit(callback_self: @This()) !void {
            // Apply only the operations belonging to the committed batch.
            for (callback_self.pending.items) |op| {
                switch (op) {
                    .insert => |entry| try applyInsertNoWal(callback_self.db, entry.key, entry.value),
                    .delete => |key| try applyDeleteNoWal(callback_self.db, key),
                }
            }
            // Persist replayed changes before truncating the WAL.
            try callback_self.db.pager.flush();
            clearPendingWalOps(callback_self.allocator, callback_self.pending);
        }

        pub fn onAbort(callback_self: @This()) !void {
            // Aborted batches are ignored during recovery.
            clearPendingWalOps(callback_self.allocator, callback_self.pending);
        }
    };

    var pending: std.ArrayList(PendingWalOp) = .empty;
    defer pending.deinit(self.allocator);
    defer clearPendingWalOps(self.allocator, &pending);

    const callback = RecoveryCallback{
        .db = self,
        .pending = &pending,
        .allocator = self.allocator,
    };

    wal_ref.replay(RecoveryCallback, callback) catch |err| switch (err) {
        Error.WalCorrupted => return Error.WalReplayFailed,
        else => return err,
    };

    try wal_ref.clear();
}
