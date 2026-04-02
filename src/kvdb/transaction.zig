const root = @import("../kvdb.zig");

const Database = root.Database;
const Error = root.Error;

/// States a transaction can be in.
const TransactionState = enum {
    /// No transaction active.
    none,
    /// Transaction in progress.
    active,
    /// Transaction successfully committed.
    committed,
    /// Transaction rolled back.
    aborted,
};

/// Represents an active database transaction.
pub const Transaction = struct {
    /// Reference to the database this transaction belongs to.
    db: *Database,

    /// Current state of the transaction.
    state: TransactionState,

    /// Commit the transaction.
    ///
    /// This writes the commit marker, performs any policy-controlled WAL sync,
    /// flushes dirty pages, clears the checkpointed WAL, and detaches the handle
    /// from the database.
    pub fn commit(self: *Transaction) !void {
        if (self.state != .active) {
            return Error.NoActiveTransaction;
        }

        if (self.db.wal) |*w| {
            try w.logCommit();
            try w.sync();
        }

        try self.db.pager.flush();

        if (self.db.wal) |*w| {
            try w.clear();
        }

        self.state = .committed;
        self.db.transaction = null;
    }

    /// Abort/rollback the transaction.
    ///
    /// This records the abort boundary in the WAL, clears pending log records,
    /// reloads pager state from disk, and detaches the transaction handle.
    pub fn abort(self: *Transaction) !void {
        if (self.state != .active) {
            return Error.NoActiveTransaction;
        }

        if (self.db.wal) |*w| {
            try w.logAbort();
            try w.clear();
        }

        try self.db.reload();

        self.state = .aborted;
        self.db.transaction = null;
    }
};
