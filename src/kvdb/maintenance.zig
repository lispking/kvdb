const std = @import("std");
const root = @import("../kvdb.zig");
const constants = @import("../constants.zig");

const Database = root.Database;
const Error = root.Error;
const PageId = constants.PageId;
const ROOT_PAGE_ID = constants.ROOT_PAGE_ID;

/// Validation report returned by `Database.verify()`.
pub const VerifyStats = struct {
    /// Number of reachable B-tree pages checked, including the root.
    checked_tree_pages: usize,
    /// Number of logical entries visited while validating sorted traversal.
    checked_entries: usize,
    /// Number of WAL records decoded successfully.
    checked_wal_records: usize,
};

/// Structural summary returned by `Database.inspect()`.
pub const InspectStats = struct {
    /// Total number of pages currently tracked by the pager.
    page_count: PageId,
    /// Fixed database page size in bytes.
    page_size: usize,
    /// Total database file size in bytes.
    db_size: u64,
    /// Root page ID recorded in metadata.
    root_page_id: PageId,
    /// Head of the on-disk freelist, or INVALID_PAGE_ID if empty.
    freelist_page: PageId,
    /// Number of pages currently reachable from the freelist head.
    freelist_page_count: usize,
    /// Highest page ID ever grown from the file.
    last_page_id: PageId,
    /// Metadata WAL offset field reserved in the on-disk header.
    wal_offset: u64,
    /// Tree height measured in levels including the root.
    tree_height: usize,
    /// Total reachable B-tree node count.
    node_count: usize,
    /// Reachable leaf node count.
    leaf_count: usize,
    /// Reachable internal node count.
    internal_count: usize,
    /// Total reachable logical entry count.
    entry_count: usize,
};

/// Database statistics structure.
pub const Stats = struct {
    /// Total number of pages allocated.
    page_count: PageId,
    /// Size of each page in bytes.
    page_size: usize,
    /// Total database file size in bytes.
    db_size: u64,
};

/// Compare two databases entry-by-entry in sorted order.
pub fn verifySameLogicalContents(self: *Database, other: *Database) !void {
    var left_iter = try self.iterator();
    defer left_iter.deinit();
    var right_iter = try other.iterator();
    defer right_iter.deinit();

    while (true) {
        const left_entry = try left_iter.next();
        const right_entry = try right_iter.next();

        if (left_entry == null and right_entry == null) {
            return;
        }
        if (left_entry == null or right_entry == null) {
            return Error.CorruptedData;
        }

        // Compaction should preserve the exact logical key/value stream even
        // if the rebuilt tree shape or page allocation differs internally.
        if (!std.mem.eql(u8, left_entry.?.key, right_entry.?.key) or
            !std.mem.eql(u8, left_entry.?.value, right_entry.?.value))
        {
            return Error.CorruptedData;
        }
    }
}

/// Verify metadata, reachable tree structure, freelist integrity, and WAL readability.
pub fn verify(self: *Database) !VerifyStats {
    const metadata = try self.pager.readMetadata();
    if (!metadata.isValid()) {
        return Error.CorruptedData;
    }
    if (metadata.root_page != self.btree.root_page_id or metadata.root_page != ROOT_PAGE_ID) {
        return Error.CorruptedData;
    }
    if (metadata.last_page_id + 1 != self.pager.pageCount()) {
        return Error.CorruptedData;
    }

    try self.pager.verifyFreelist();

    const tree_stats = try self.btree.verify(&self.pager);

    var iter = try self.iterator();
    defer iter.deinit();
    var previous_key: ?[]u8 = null;
    defer if (previous_key) |key| self.allocator.free(key);
    var iter_entries: usize = 0;
    while (try iter.next()) |entry| {
        if (previous_key) |prev| {
            if (std.mem.order(u8, prev, entry.key) != .lt) {
                return Error.CorruptedData;
            }
            self.allocator.free(prev);
        }
        previous_key = try self.allocator.dupe(u8, entry.key);
        iter_entries += 1;
    }
    if (iter_entries != tree_stats.checked_entries) {
        return Error.CorruptedData;
    }

    var wal_records: usize = 0;
    if (self.wal) |*w| {
        var wal_iter = w.iterator();
        while (try wal_iter.next()) |record| {
            defer {
                self.allocator.free(record.key);
                if (record.value) |value| self.allocator.free(value);
            }

            switch (record.record_type) {
                .insert => if (record.value == null) return Error.CorruptedData,
                .delete, .commit, .abort => {},
            }
            wal_records += 1;
        }
    }

    return .{
        .checked_tree_pages = tree_stats.checked_pages,
        .checked_entries = tree_stats.checked_entries,
        .checked_wal_records = wal_records,
    };
}

/// Summarize metadata, file usage, and reachable tree shape for CLI inspection.
pub fn inspect(self: *Database) !InspectStats {
    const metadata = try self.pager.readMetadata();
    const tree_stats = try self.btree.inspect(&self.pager);
    const db_stats = stats(self);

    return .{
        .page_count = db_stats.page_count,
        .page_size = db_stats.page_size,
        .db_size = db_stats.db_size,
        .root_page_id = metadata.root_page,
        .freelist_page = metadata.freelist_page,
        .freelist_page_count = try self.pager.freelistPageCount(),
        .last_page_id = metadata.last_page_id,
        .wal_offset = metadata.wal_offset,
        .tree_height = tree_stats.tree_height,
        .node_count = tree_stats.node_count,
        .leaf_count = tree_stats.leaf_count,
        .internal_count = tree_stats.internal_count,
        .entry_count = tree_stats.entry_count,
    };
}

/// Get database statistics.
pub fn stats(self: *Database) Stats {
    return .{
        .page_count = self.pager.pageCount(),
        .page_size = constants.PAGE_SIZE,
        .db_size = self.pager.pageCount() * constants.PAGE_SIZE,
    };
}

/// Compact the database by removing deleted entries.
pub fn compact(self: *Database) !Stats {
    if (self.transaction != null) {
        return Error.TransactionAlreadyActive;
    }

    const db_path = try self.allocator.dupe(u8, self.db_path);
    defer self.allocator.free(db_path);

    const options = self.options;

    const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{db_path});
    defer self.allocator.free(tmp_path);

    var new_db = try Database.open(self.allocator, tmp_path, .{
        .enable_wal = false,
    });
    errdefer new_db.close();

    var iter = try self.iterator();
    defer iter.deinit();
    while (try iter.next()) |entry| {
        try new_db.put(entry.key, entry.value);
    }

    try verifySameLogicalContents(self, &new_db);

    try new_db.pager.flush();
    new_db.close();

    self.close();

    try std.fs.cwd().rename(tmp_path, db_path);

    const wal_path = try std.fmt.allocPrint(self.allocator, "{s}.wal", .{db_path});
    defer self.allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};

    self.* = try Database.open(self.allocator, db_path, options);

    return stats(self);
}
