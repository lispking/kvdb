const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");
const btree = @import("../btree.zig");
const wal = @import("../wal.zig");
const types = @import("types.zig");
const transfer = @import("transfer.zig");
const recovery = @import("recovery.zig");
const maintenance = @import("maintenance.zig");
const transaction_mod = @import("transaction.zig");

const Pager = pager.Pager;
const BTree = btree.BTree;
const Wal = wal.Wal;
const MetaData = constants.MetaData;
const Error = constants.Error;
const Options = types.Options;
const Transaction = transaction_mod.Transaction;
const VerifyStats = maintenance.VerifyStats;
const InspectStats = maintenance.InspectStats;

/// Main database structure providing key-value storage.
///
/// Database manages:
/// - Page storage through the Pager
/// - Sorted page-local indexing through the current B-tree implementation
/// - Write-ahead logging records
/// - Explicit commit/abort APIs for handle-scoped writes
///
/// Usage:
/// ```zig
/// var db = try kvdb.open(allocator, "/path/to/db");
/// defer db.close();
/// try db.put("key", "value");
/// const value = try db.get("key");
/// ```
pub const Database = struct {
    /// Memory allocator for all database operations
    allocator: std.mem.Allocator,

    /// Page manager for disk I/O
    pager: Pager,

    /// B-tree index for key-value storage
    btree: BTree,

    /// Write-ahead log (null if WAL is disabled)
    wal: ?Wal,

    /// Configuration options
    options: Options,

    /// Currently active transaction (null if none)
    transaction: ?Transaction,

    /// Path to the database file (stored for reopen during rollback)
    db_path: []const u8,

    /// Open or create a database at the specified path.
    ///
    /// If the file exists, opens it and validates the metadata.
    /// If the file doesn't exist, creates a new database with initial structure.
    ///
    /// Parameters:
    ///   - allocator: Memory allocator for database operations
    ///   - path: File path for the database
    ///   - options: Configuration options
    ///
    /// Returns: Initialized Database ready for use
    pub fn open(allocator: std.mem.Allocator, path: []const u8, options: Options) !Database {
        // Initialize pager which handles all page I/O
        var p = try Pager.init(allocator, path, options.fsync_policy);
        var pager_owned = true;
        errdefer if (pager_owned) p.deinit();

        // Read existing metadata or initialize new database
        var metadata = try p.readMetadata();
        if (!metadata.isValid()) {
            // New database - initialize fresh metadata
            metadata = MetaData.init();
            try p.writeMetadata(metadata);
            try p.flush();
        }

        // Initialize WAL if enabled
        var wal_instance: ?Wal = null;
        if (options.enable_wal) {
            wal_instance = try Wal.init(allocator, path, options.fsync_policy);
        }
        errdefer {
            if (wal_instance) |*w| w.deinit();
        }

        // Store database path for later reopen (needed for rollback)
        const db_path = try allocator.dupe(u8, path);

        var db = Database{
            .allocator = allocator,
            .pager = p,
            .btree = BTree.init(metadata.root_page),
            .wal = wal_instance,
            .options = options,
            .transaction = null,
            .db_path = db_path,
        };
        // Ownership of the WAL handle has moved into `db`, so disable the
        // earlier init-time cleanup path before using `db.close()` on failure.
        pager_owned = false;
        wal_instance = null;
        errdefer db.close();

        // Complete any committed work recorded in the WAL before exposing the
        // database handle to callers.
        try recovery.recoverFromWal(&db);

        return db;
    }

    /// Close the database and release all resources.
    ///
    /// If a transaction is active, it will be aborted.
    /// Dirty pages are then flushed to disk before closing, so standalone writes
    /// can still persist at close even when they were not wrapped in an explicit
    /// transaction commit.
    pub fn close(self: *Database) void {
        // Abort any active transaction
        if (self.transaction) |*txn| {
            _ = txn.abort() catch {};
        }

        // Flush any remaining dirty pages using the configured durability mode.
        self.pager.flush() catch {};

        // Clean up WAL
        if (self.wal) |*w| {
            w.deinit();
        }

        self.allocator.free(self.db_path);
        self.pager.deinit();
    }

    /// Reload the database from disk.
    ///
    /// Used during transaction rollback to restore the database state.
    /// Reopens the pager while preserving the WAL.
    pub fn reload(self: *Database) !void {
        // Preserve WAL reference
        const wal_instance = self.wal;
        self.wal = null;

        // Close and reopen pager to discard in-memory changes
        self.pager.deinit();
        self.pager = try Pager.init(self.allocator, self.db_path, self.options.fsync_policy);

        // Restore WAL
        self.wal = wal_instance;
    }

    /// Validate a value against the current engine limits.
    pub fn validateValue(value: []const u8) !void {
        if (value.len > constants.MAX_VALUE_SIZE) {
            return Error.InvalidArgument;
        }
    }

    /// Begin a new transaction.
    ///
    /// Only one transaction can be active at a time. Call commit() or abort()
    /// to end the transaction before starting another. This marks an explicit
    /// commit/abort boundary for the current handle; it does not create
    /// snapshots or an isolated view of the database.
    ///
    /// Returns: Pointer to active transaction
    /// Errors: TransactionAlreadyActive if another transaction is in progress
    pub fn beginTransaction(self: *Database) !*Transaction {
        if (self.transaction != null) {
            return Error.TransactionAlreadyActive;
        }

        self.transaction = Transaction{
            .db = self,
            .state = .active,
        };

        return &self.transaction.?;
    }

    /// Retrieve a value by key.
    ///
    /// Looks up the key in the B-tree index and returns a copy of the
    /// associated value. Caller is responsible for freeing the returned value.
    ///
    /// Parameters:
    ///   - key: The key to look up
    ///
    /// Returns: Allocated copy of value, or null if key not found
    pub fn get(self: *Database, key: []const u8) !?[]const u8 {
        return self.btree.get(&self.pager, key, self.allocator);
    }

    /// Store a key-value pair.
    ///
    /// If the key already exists, its value is updated. Otherwise, a new
    /// entry is created. The operation is logged to WAL if enabled.
    ///
    /// This method does not require an active transaction. It updates the
    /// current handle's in-memory pages immediately. Explicit transaction
    /// commit remains the atomic durable boundary; outside a transaction,
    /// persistence still depends on a later flush such as commit() or close().
    ///
    /// Parameters:
    ///   - key: The key to store (must not be empty)
    ///   - value: The value to associate with the key
    ///
    /// Errors:
    ///   - InvalidArgument if key is empty or sizes exceed limits
    ///   - NodeFull if the B-tree node is full
    pub fn put(self: *Database, key: []const u8, value: []const u8) !void {
        // Validate parameters
        if (key.len == 0 or key.len > constants.MAX_KEY_SIZE) {
            return Error.InvalidArgument;
        }
        if (value.len > constants.MAX_VALUE_SIZE) {
            return Error.InvalidArgument;
        }

        // Check if this is an update to existing key
        const exists = try self.contains(key);

        // Log to WAL for durability
        if (self.wal) |*w| {
            try w.logInsert(key, value);
        }

        if (exists) {
            // Update existing values in place so repeated overwrites repack the
            // leaf instead of accumulating dead payload bytes through delete+insert.
            try self.btree.update(&self.pager, key, value);
            return;
        }

        // Insert new key-value pair
        try self.btree.put(&self.pager, key, value);
    }

    /// Delete a key-value pair.
    ///
    /// Removes the key and its associated value from the database.
    /// Operation is logged to WAL if enabled.
    ///
    /// This method does not require an active transaction. It updates the
    /// current handle immediately in memory. Explicit transaction commit remains
    /// the atomic durable boundary; outside a transaction, persistence still
    /// depends on a later flush such as commit() or close().
    ///
    /// Parameters:
    ///   - key: The key to delete
    ///
    /// Errors: KeyNotFound if key doesn't exist
    pub fn delete(self: *Database, key: []const u8) !void {
        if (key.len == 0) {
            return Error.InvalidArgument;
        }

        // Log deletion to WAL
        if (self.wal) |*w| {
            try w.logDelete(key);
        }

        // Remove from B-tree
        try self.btree.delete(&self.pager, key);
    }

    /// Check if a key exists in the database.
    ///
    /// More efficient than get() when you only need to check existence.
    ///
    /// Parameters:
    ///   - key: The key to check
    ///
    /// Returns: true if key exists, false otherwise
    pub fn contains(self: *Database, key: []const u8) !bool {
        const value = try self.get(key);
        if (value) |v| {
            self.allocator.free(v);
            return true;
        }
        return false;
    }

    /// Create an iterator over all key-value pairs.
    ///
    /// Iterates in sorted key order across the current multi-page tree shape.
    /// Caller should deinit the iterator after use to release traversal state.
    ///
    /// Returns: Iterator positioned at first entry
    pub fn iterator(self: *Database) !btree.BTree.Iterator {
        return self.btree.iterator(&self.pager);
    }

    /// Verify metadata, reachable tree structure, freelist integrity, and WAL readability.
    pub fn verify(self: *Database) !VerifyStats {
        return maintenance.verify(self);
    }

    /// Summarize metadata, file usage, and reachable tree shape for CLI inspection.
    pub fn inspect(self: *Database) !InspectStats {
        return maintenance.inspect(self);
    }

    /// Get database statistics.
    ///
    /// Returns information about database size and structure.
    pub fn stats(self: *Database) Stats {
        return maintenance.stats(self);
    }

    /// Stream all logical entries into a binary export writer.
    pub fn exportToWriter(self: *Database, writer: anytype) !usize {
        return transfer.exportToWriter(self, writer);
    }

    /// Load logical entries from a binary import reader as one transaction.
    pub fn importFromReader(self: *Database, reader: anytype) !usize {
        return transfer.importFromReader(self, reader);
    }

    pub const Stats = maintenance.Stats;

    /// Compact the database by removing deleted entries.
    ///
    /// This operation creates a new database file with only the valid
    /// key-value pairs, then atomically replaces the original file.
    /// The WAL is cleared after successful compaction.
    ///
    /// Returns: New database statistics after compaction
    /// Errors: Returns any filesystem or database error
    pub fn compact(self: *Database) !Stats {
        return maintenance.compact(self);
    }
};
