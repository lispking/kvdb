const std = @import("std");
const constants = @import("constants.zig");
const pager = @import("pager.zig");
const btree = @import("btree.zig");
const wal = @import("wal.zig");
const Pager = pager.Pager;
const BTree = btree.BTree;
const Wal = wal.Wal;
const MetaData = constants.MetaData;
const Error = constants.Error;
const PageId = constants.PageId;
const ROOT_PAGE_ID = constants.ROOT_PAGE_ID;

/// Configuration options for database initialization.
pub const Options = struct {
    /// Size of each database page (should match PAGE_SIZE constant)
    page_size: usize = constants.PAGE_SIZE,

    /// Enable Write-Ahead Logging for durability and crash recovery
    enable_wal: bool = true,

    /// Enable compression for stored values (not yet implemented)
    enable_compression: bool = false,
};

/// States a transaction can be in.
const TransactionState = enum {
    /// No transaction active
    none,
    /// Transaction in progress
    active,
    /// Transaction successfully committed
    committed,
    /// Transaction rolled back
    aborted,
};

/// Represents an active database transaction.
///
/// Transactions provide atomicity for database operations. All operations
/// within a transaction are either all committed or all rolled back.
pub const Transaction = struct {
    /// Reference to the database this transaction belongs to
    db: *Database,

    /// Current state of the transaction
    state: TransactionState,

    /// Commit the transaction.
    ///
    /// All changes made during this transaction are persisted to disk.
    /// The WAL is cleared after successful commit.
    ///
    /// Returns: Error.NoActiveTransaction if transaction not in active state
    pub fn commit(self: *Transaction) !void {
        if (self.state != .active) {
            return Error.NoActiveTransaction;
        }

        // Write commit marker to WAL
        if (self.db.wal) |*w| {
            try w.logCommit();
        }

        // Ensure all dirty pages are written to disk
        try self.db.pager.flush();

        // Clear WAL since changes are now persisted
        if (self.db.wal) |*w| {
            try w.clear();
        }

        self.state = .committed;
        self.db.transaction = null;
    }

    /// Abort/rollback the transaction.
    ///
    /// Discards all changes made during this transaction and restores
    /// the database to its state before the transaction began.
    ///
    /// Returns: Error.NoActiveTransaction if transaction not in active state
    pub fn abort(self: *Transaction) !void {
        if (self.state != .active) {
            return Error.NoActiveTransaction;
        }

        // Write abort marker and clear WAL
        if (self.db.wal) |*w| {
            try w.logAbort();
            try w.clear();
        }

        // Reload database to undo any changes
        // Note: Simplified implementation - production code would use
        // WAL replay to selectively undo changes
        try self.db.reload();

        self.state = .aborted;
        self.db.transaction = null;
    }
};

/// Main database structure providing key-value storage.
///
/// Database manages:
/// - Page storage through the Pager
/// - B-tree indexing for efficient lookups
/// - Write-ahead logging for durability
/// - Transaction management for atomicity
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
        var p = try Pager.init(allocator, path);
        errdefer p.deinit();

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
            wal_instance = try Wal.init(allocator, path);
        }
        errdefer {
            if (wal_instance) |*w| w.deinit();
        }

        // Store database path for later reopen (needed for rollback)
        const db_path = try allocator.dupe(u8, path);
        errdefer allocator.free(db_path);

        return Database{
            .allocator = allocator,
            .pager = p,
            .btree = BTree.init(metadata.root_page),
            .wal = wal_instance,
            .options = options,
            .transaction = null,
            .db_path = db_path,
        };
    }

    /// Close the database and release all resources.
    ///
    /// If a transaction is active, it will be aborted.
    /// All pending changes are flushed to disk before closing.
    pub fn close(self: *Database) void {
        // Abort any active transaction
        if (self.transaction) |*txn| {
            _ = txn.abort() catch {};
        }

        // Flush any remaining dirty pages
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
    fn reload(self: *Database) !void {
        // Preserve WAL reference
        const wal_instance = self.wal;
        self.wal = null;

        // Close and reopen pager to discard in-memory changes
        self.pager.deinit();
        self.pager = try Pager.init(self.allocator, self.db_path);

        // Restore WAL
        self.wal = wal_instance;
    }

    /// Begin a new transaction.
    ///
    /// Only one transaction can be active at a time. Call commit() or abort()
    /// to end the transaction before starting another.
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
            // Update: delete old value first (simplified approach)
            try self.btree.delete(&self.pager, key);
        }

        // Insert new key-value pair
        try self.btree.put(&self.pager, key, value);
    }

    /// Delete a key-value pair.
    ///
    /// Removes the key and its associated value from the database.
    /// Operation is logged to WAL if enabled.
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
    /// Iterates in sorted key order (B-tree traversal).
    /// Currently only supports single-page iteration (simplified implementation).
    ///
    /// Returns: Iterator positioned at first entry
    pub fn iterator(self: *Database) !btree.BTree.Iterator {
        return self.btree.iterator(&self.pager);
    }

    /// Get database statistics.
    ///
    /// Returns information about database size and structure.
    pub fn stats(self: *Database) Stats {
        return .{
            .page_count = self.pager.pageCount(),
            .page_size = constants.PAGE_SIZE,
            .db_size = self.pager.pageCount() * constants.PAGE_SIZE,
        };
    }

    /// Database statistics structure.
    pub const Stats = struct {
        /// Total number of pages allocated
        page_count: PageId,
        /// Size of each page in bytes
        page_size: usize,
        /// Total database file size in bytes
        db_size: u64,
    };

    /// Compact the database by removing deleted entries.
    ///
    /// This operation creates a new database file with only the valid
    /// key-value pairs, then atomically replaces the original file.
    /// The WAL is cleared after successful compaction.
    ///
    /// Returns: New database statistics after compaction
    /// Errors: Returns any filesystem or database error
    pub fn compact(self: *Database) !Stats {
        // Cannot compact during an active transaction
        if (self.transaction != null) {
            return Error.TransactionAlreadyActive;
        }

        // Save the database path (close() will free it)
        const db_path = try self.allocator.dupe(u8, self.db_path);
        defer self.allocator.free(db_path);

        // Save options for reopening
        const options = self.options;

        // Create temporary file path
        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{db_path});
        defer self.allocator.free(tmp_path);

        // Create new database with WAL disabled (we'll clean it at the end)
        var new_db = try Database.open(self.allocator, tmp_path, .{
            .enable_wal = false,
        });
        errdefer new_db.close();

        // Copy all key-value pairs to new database
        var iter = try self.iterator();
        while (iter.next()) |entry| {
            try new_db.put(entry.key, entry.value);
        }

        // Close both databases to release file handles
        new_db.close();
        self.close();

        // Atomically replace original file with compacted version
        try std.fs.cwd().rename(tmp_path, db_path);

        // Delete the old WAL file if it exists
        const wal_path = try std.fmt.allocPrint(self.allocator, "{s}.wal", .{db_path});
        defer self.allocator.free(wal_path);
        std.fs.cwd().deleteFile(wal_path) catch {};

        // Reopen the compacted database
        self.* = try Database.open(self.allocator, db_path, options);

        return self.stats();
    }

    /// C-compatible API for FFI (Foreign Function Interface).
    ///
    /// These functions allow the database to be used from other languages
    /// such as C, Python, etc.
    pub const C_API = struct {
        /// Opaque handle type for C code
        pub const KVDB_Handle = *Database;

        /// Open a database (C API).
        ///
        /// Parameters:
        ///   - path: Database file path
        ///   - path_len: Length of path string
        ///
        /// Returns: Database handle, or null on error
        export fn kvdb_open(path: [*c]const u8, path_len: usize) ?KVDB_Handle {
            const allocator = std.heap.page_allocator;
            const path_slice = path[0..path_len];

            const db = allocator.create(Database) catch return null;
            db.* = Database.open(allocator, path_slice, .{}) catch {
                allocator.destroy(db);
                return null;
            };

            return db;
        }

        /// Close a database (C API).
        ///
        /// Parameters:
        ///   - handle: Database handle from kvdb_open
        export fn kvdb_close(handle: KVDB_Handle) void {
            const allocator = std.heap.page_allocator;
            handle.close();
            allocator.destroy(handle);
        }

        /// Get a value by key (C API).
        ///
        /// Parameters:
        ///   - handle: Database handle
        ///   - key: Key to look up
        ///   - key_len: Length of key
        ///   - value_len: Output parameter for value length
        ///
        /// Returns: Pointer to value data (owned by caller, must free),
        ///          or null if key not found
        export fn kvdb_get(handle: KVDB_Handle, key: [*c]const u8, key_len: usize, value_len: *usize) ?[*c]const u8 {
            const key_slice = key[0..key_len];
            const value = handle.get(key_slice) catch return null;

            if (value) |v| {
                value_len.* = v.len;
                // Note: Memory ownership transfers to C caller
                // In production, should use a dedicated allocator or memory pool
                return v.ptr;
            }
            return null;
        }

        /// Store a key-value pair (C API).
        ///
        /// Parameters:
        ///   - handle: Database handle
        ///   - key: Key to store
        ///   - key_len: Length of key
        ///   - value: Value to store
        ///   - value_len: Length of value
        ///
        /// Returns: 0 on success, -1 on error
        export fn kvdb_put(handle: KVDB_Handle, key: [*c]const u8, key_len: usize, value: [*c]const u8, value_len: usize) c_int {
            const key_slice = key[0..key_len];
            const value_slice = value[0..value_len];

            handle.put(key_slice, value_slice) catch return -1;
            return 0;
        }

        /// Delete a key (C API).
        ///
        /// Parameters:
        ///   - handle: Database handle
        ///   - key: Key to delete
        ///   - key_len: Length of key
        ///
        /// Returns: 0 on success, -1 on error
        export fn kvdb_delete(handle: KVDB_Handle, key: [*c]const u8, key_len: usize) c_int {
            const key_slice = key[0..key_len];

            handle.delete(key_slice) catch return -1;
            return 0;
        }
    };
};

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

// =============================================================================
// Tests
// =============================================================================

test "Database basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_kvdb.db";

    // Cleanup after test
    defer std.fs.cwd().deleteFile(test_path) catch {};
    defer std.fs.cwd().deleteFile("test_kvdb.db.wal") catch {};

    // Test 1: Create and manipulate database
    {
        var db = try Database.open(allocator, test_path, .{});
        defer db.close();

        // Insert entries
        try db.put("name", "Alice");
        try db.put("age", "30");
        try db.put("city", "Beijing");

        // Query single entry
        const name = try db.get("name");
        try std.testing.expect(name != null);
        try std.testing.expectEqualStrings("Alice", name.?);
        defer allocator.free(name.?);

        // Update existing entry
        try db.put("age", "31");
        const age = try db.get("age");
        try std.testing.expectEqualStrings("31", age.?);
        defer allocator.free(age.?);

        // Delete entry
        try db.delete("city");
        try std.testing.expect(!(try db.contains("city")));

        // Verify statistics
        const stats = db.stats();
        try std.testing.expect(stats.page_count >= 2);
    }

    // Test 2: Verify persistence by reopening
    {
        var db = try Database.open(allocator, test_path, .{});
        defer db.close();

        const name = try db.get("name");
        try std.testing.expect(name != null);
        try std.testing.expectEqualStrings("Alice", name.?);
        defer allocator.free(name.?);
    }
}

test "Database transaction" {
    const allocator = std.testing.allocator;
    const test_path = "test_txn.db";

    // Cleanup after test
    defer std.fs.cwd().deleteFile(test_path) catch {};
    defer std.fs.cwd().deleteFile("test_txn.db.wal") catch {};

    var db = try Database.open(allocator, test_path, .{});
    defer db.close();

    // Begin transaction
    const txn = try db.beginTransaction();

    // Insert within transaction
    try db.put("key1", "value1");
    try db.put("key2", "value2");

    // Commit transaction
    try txn.commit();

    // Verify committed data
    const value = try db.get("key1");
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("value1", value.?);
    defer allocator.free(value.?);
}
