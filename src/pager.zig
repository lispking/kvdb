const std = @import("std");
const constants = @import("constants.zig");
const PAGE_SIZE = constants.PAGE_SIZE;
const PageId = constants.PageId;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;
const Error = constants.Error;

/// Header stored inside pages that have been returned to the freelist.
const FreePageHeader = packed struct {
    /// Next free page in the singly linked freelist.
    next_free_page: PageId,
};

/// Represents a single database page in memory.
/// Each page is a fixed-size block of data (4KB) with associated metadata.
pub const Page = struct {
    /// Unique identifier for this page within the database file.
    id: PageId,

    /// The actual data content of the page.
    /// Fixed size array matching PAGE_SIZE (4096 bytes).
    data: [PAGE_SIZE]u8,

    /// Dirty flag indicating whether this page has been modified
    /// and needs to be written back to disk.
    is_dirty: bool,

    /// Initialize a new page with the given ID.
    /// Data is left uninitialized for performance (will be loaded from disk or cleared).
    pub fn init(id: PageId) Page {
        return .{
            .id = id,
            .data = undefined,
            .is_dirty = false,
        };
    }

    /// Clear the page data to all zeros and mark as dirty.
    /// Used when allocating a new page.
    pub fn clear(self: *Page) void {
        @memset(&self.data, 0);
        self.is_dirty = true;
    }

    /// Return a mutable slice of the page data.
    /// Useful for reading or modifying page content.
    pub fn asSlice(self: *Page) []u8 {
        return &self.data;
    }

    /// Mark this page as dirty (modified).
    /// Called after any modification to ensure the page is persisted.
    pub fn markDirty(self: *Page) void {
        self.is_dirty = true;
    }
};

/// Entry in the page cache tracking a loaded page.
/// Used by the Pager to maintain the in-memory cache of recently accessed pages.
const CacheEntry = struct {
    /// ID of the cached page
    page_id: PageId,
    /// Pointer to the allocated page structure
    page: *Page,
};

/// Fast lookup table mapping page IDs to cache entry indexes.
const CacheIndex = std.AutoHashMap(PageId, usize);

/// Metadata bootstrapping reserves page 0 and page 1 before a valid metadata
/// header exists, so those first allocations must bypass freelist lookups.
fn isBootstrappingReservedPages(next_page_id: PageId) bool {
    return next_page_id <= constants.ROOT_PAGE_ID;
}

/// Page manager responsible for all page-level I/O operations.
///
/// The Pager provides:
/// - Page allocation and deallocation
/// - In-memory page caching to reduce disk I/O
/// - Reading pages from disk
/// - Writing dirty pages back to disk (flush)
/// - Metadata management
///
/// This is a core component that sits between the B-tree index and the
/// underlying storage file.
pub const Pager = struct {
    /// Memory allocator for page structures and cache management
    allocator: std.mem.Allocator,

    /// File handle for the database file
    file: std.fs.File,

    /// Size of each page (typically PAGE_SIZE = 4096)
    page_size: usize,

    /// In-memory cache of loaded pages.
    cache: std.ArrayList(CacheEntry),

    /// Fast page-id-to-cache-index lookup to avoid linear scans on every getPage.
    cache_index: CacheIndex,

    /// Next page ID to allocate.
    /// Incremented when creating new pages. Initialized from file size on open.
    next_page_id: PageId,

    /// Initialize a new Pager for the database file at the given path.
    ///
    /// If the file doesn't exist, creates it and initializes metadata pages.
    /// If the file exists, opens it and loads existing page information.
    ///
    /// Parameters:
    ///   - allocator: Memory allocator for internal structures
    ///   - file_path: Path to the database file
    ///
    /// Returns: Initialized Pager ready for use
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) !Pager {
        // Open or create the database file with read-write access
        const file = try std.fs.cwd().createFile(file_path, .{
            .read = true,
            .truncate = false, // Don't truncate existing files
        });
        errdefer file.close();

        // Get current file size to determine if this is a new database
        const stat = try file.stat();
        const file_size = stat.size;

        // Initialize the page cache with some initial capacity
        var cache: std.ArrayList(CacheEntry) = .empty;
        try cache.ensureTotalCapacity(allocator, 16);
        var cache_index = CacheIndex.init(allocator);
        errdefer cache_index.deinit();

        var pager = Pager{
            .allocator = allocator,
            .file = file,
            .page_size = PAGE_SIZE,
            .cache = cache,
            .cache_index = cache_index,
            .next_page_id = if (file_size == 0) 0 else @intCast(file_size / PAGE_SIZE),
        };

        // For new databases, initialize the metadata and root pages
        // A zero-length file means Pager.init is creating the database from
        // scratch, so it must reserve both the metadata page and the initial
        // empty root page before callers can use the tree.
        if (file_size == 0) {
            try pager.allocateMetadataPages();
        } else {
            const metadata = try pager.readMetadata();
            // Once metadata is valid, its last_page_id becomes the durable source
            // of truth for future file growth instead of re-deriving from size.
            if (metadata.isValid()) {
                pager.next_page_id = metadata.last_page_id + 1;
            }
        }

        return pager;
    }

    /// Initialize the metadata pages for a new database.
    ///
    /// Creates:
    /// - Page 0: Metadata page containing database header
    /// - Page 1: Root page of the B-tree (initialized as empty leaf node)
    ///
    /// Both pages are immediately flushed to disk.
    fn allocateMetadataPages(self: *Pager) !void {
        // Allocate metadata page (page 0)
        const meta_page = try self.allocatePage();
        std.debug.assert(meta_page.id == constants.META_PAGE_ID);

        // Allocate root node page (page 1)
        const root_page = try self.allocatePage();
        std.debug.assert(root_page.id == constants.ROOT_PAGE_ID);

        // Initialize root node as empty leaf node
        const header = constants.NodeHeader{
            .node_type = .leaf,
            .num_keys = 0,
        };
        self.writeNodeHeader(root_page, header);

        // Seed page 0 with the durable metadata header after both reserved pages
        // exist so the initial last_page_id already matches on-disk layout.
        try self.writeMetadata(constants.MetaData.init());

        // Persist the initial pages to disk
        try self.flush();
    }

    /// Clean up resources and close the database file.
    ///
    /// Note: Does NOT automatically flush dirty pages!
    /// Call flush() before deinit() if you need to persist changes.
    pub fn deinit(self: *Pager) void {
        // Free all cached page structures
        for (self.cache.items) |entry| {
            self.allocator.destroy(entry.page);
        }
        self.cache.deinit(self.allocator);
        self.cache_index.deinit();
        self.file.close();
    }

    /// Search for a page in the in-memory cache.
    fn findInCache(self: *Pager, page_id: PageId) ?*Page {
        const index = self.cache_index.get(page_id) orelse return null;
        return self.cache.items[index].page;
    }

    /// Create and cache a fresh in-memory page wrapper for the given page ID.
    fn createCachedPage(self: *Pager, page_id: PageId) !*Page {
        const page = try self.allocator.create(Page);
        errdefer self.allocator.destroy(page);
        page.* = Page.init(page_id);

        const cache_index = self.cache.items.len;
        try self.cache.append(self.allocator, .{
            .page_id = page_id,
            .page = page,
        });
        errdefer _ = self.cache.pop();
        try self.cache_index.put(page_id, cache_index);
        return page;
    }

    /// Return an existing cached page, or cache a new wrapper for this page ID.
    fn ensureCachedPage(self: *Pager, page_id: PageId) !*Page {
        if (self.findInCache(page_id)) |page| {
            return page;
        }
        return self.createCachedPage(page_id);
    }

    /// Read the freelist head from metadata once bootstrapping has finished.
    fn readFreelistHead(self: *Pager) !PageId {
        const metadata = try self.readMetadata();
        return metadata.freelist_page;
    }

    /// Persist a new freelist head in metadata.
    fn writeFreelistHead(self: *Pager, freelist_page: PageId) !void {
        var metadata = try self.readMetadata();
        metadata.freelist_page = freelist_page;
        try self.writeMetadata(metadata);
    }

    /// Persist the last allocated page ID so metadata mirrors file growth.
    fn writeLastPageId(self: *Pager, last_page_id: PageId) !void {
        var metadata = try self.readMetadata();
        metadata.last_page_id = last_page_id;
        try self.writeMetadata(metadata);
    }

    /// Return a recycled page to callers after popping it from the freelist.
    fn allocateFromFreelist(self: *Pager, page_id: PageId) !*Page {
        const page = try self.getPage(page_id);
        const header_bytes = page.asSlice()[0..@sizeOf(FreePageHeader)];
        const header = std.mem.bytesToValue(FreePageHeader, header_bytes);

        // Removing the freelist head first keeps metadata authoritative even if
        // the caller later repurposes the recycled page for normal node contents.
        try self.writeFreelistHead(header.next_free_page);
        page.clear();
        return page;
    }

    /// Record a page on the freelist so later allocations can reuse it.
    pub fn freePage(self: *Pager, page_id: PageId) !void {
        if (page_id <= constants.ROOT_PAGE_ID or page_id >= self.next_page_id) {
            return Error.InvalidPageId;
        }

        const page = try self.getPage(page_id);
        const freelist_head = try self.readFreelistHead();
        const header = FreePageHeader{
            .next_free_page = freelist_head,
        };

        // Freed pages store their next pointer inline so the freelist survives
        // restarts without needing any separate allocation metadata pages.
        @memset(&page.data, 0);
        const bytes = std.mem.asBytes(&header);
        @memcpy(page.asSlice()[0..bytes.len], bytes);
        page.markDirty();

        try self.writeFreelistHead(page_id);
    }

    /// Verify that the persisted freelist only references reusable page IDs.
    pub fn verifyFreelist(self: *Pager) !void {
        var visited = std.AutoHashMap(PageId, void).init(self.allocator);
        defer visited.deinit();

        var page_id = (try self.readMetadata()).freelist_page;
        while (page_id != INVALID_PAGE_ID) {
            // Freelist pages must never point at reserved metadata/root pages or
            // past the highest allocated page recorded by the pager.
            if (page_id <= constants.ROOT_PAGE_ID or page_id >= self.next_page_id) {
                return Error.CorruptedData;
            }

            const entry = try visited.getOrPut(page_id);
            if (entry.found_existing) {
                return Error.CorruptedData;
            }
            entry.value_ptr.* = {};

            const page = try self.getPage(page_id);
            const header_bytes = page.asSlice()[0..@sizeOf(FreePageHeader)];
            const header = std.mem.bytesToValue(FreePageHeader, header_bytes);
            page_id = header.next_free_page;
        }
    }

    /// Count pages currently linked from the persisted freelist head.
    pub fn freelistPageCount(self: *Pager) !usize {
        var count: usize = 0;
        var page_id = (try self.readMetadata()).freelist_page;

        while (page_id != INVALID_PAGE_ID) {
            if (page_id <= constants.ROOT_PAGE_ID or page_id >= self.next_page_id) {
                return Error.CorruptedData;
            }

            const page = try self.getPage(page_id);
            const header_bytes = page.asSlice()[0..@sizeOf(FreePageHeader)];
            const header = std.mem.bytesToValue(FreePageHeader, header_bytes);
            page_id = header.next_free_page;
            count += 1;
        }

        return count;
    }

    /// Retrieve a page by its ID.
    ///
    /// First checks the in-memory cache. If not found, loads the page
    /// from disk, allocates memory for it, and adds it to the cache.
    ///
    /// Parameters:
    ///   - page_id: The ID of the page to retrieve
    ///
    /// Returns: Pointer to the page structure
    pub fn getPage(self: *Pager, page_id: PageId) !*Page {
        // Check cache first for fast retrieval
        if (self.findInCache(page_id)) |page| {
            return page;
        }

        // Page not in cache - need to load from disk
        const page = try self.ensureCachedPage(page_id);

        // Calculate file offset for this page
        const offset = page_id * self.page_size;
        try self.file.seekTo(offset);

        // Read page data from disk
        var buf: [PAGE_SIZE]u8 = undefined;
        const bytes_read = try self.file.read(&buf);

        if (bytes_read == 0) {
            // New page that hasn't been written yet - initialize to zeros
            @memset(&page.data, 0);
        } else if (bytes_read != PAGE_SIZE) {
            // Partial read indicates file corruption
            return Error.CorruptedData;
        } else {
            // Successfully read full page
            page.data = buf;
        }

        return page;
    }

    /// Allocate a new page.
    ///
    /// Assigns the next available page ID, creates a zero-initialized page,
    /// and adds it to the cache. The page is marked dirty so it will be
    /// written to disk on the next flush().
    ///
    /// Returns: Pointer to the newly allocated page
    pub fn allocatePage(self: *Pager) !*Page {
        if (!isBootstrappingReservedPages(self.next_page_id)) {
            const freelist_page = try self.readFreelistHead();
            if (freelist_page != INVALID_PAGE_ID) {
                return self.allocateFromFreelist(freelist_page);
            }
        }

        const page_id = self.next_page_id;
        self.next_page_id += 1;

        const page = try self.ensureCachedPage(page_id);
        page.clear(); // Zero-initialize and mark dirty

        if (!isBootstrappingReservedPages(self.next_page_id)) {
            // Metadata tracks the highest page ID ever allocated from file growth,
            // not recycled page IDs that came back from the freelist.
            try self.writeLastPageId(page_id);
        }

        return page;
    }

    /// Write all dirty pages back to disk.
    ///
    /// Iterates through the cache and writes any pages marked as dirty.
    /// After writing, clears the dirty flag on each page.
    /// Finally syncs the file to ensure data is persisted.
    ///
    /// This should be called:
    /// - After completing a transaction
    /// - Before closing the database
    /// - Periodically to ensure durability
    pub fn flush(self: *Pager) !void {
        for (self.cache.items) |entry| {
            const page = entry.page;
            if (page.is_dirty) {
                // Dirty pages are flushed in place; Pager keeps no write-ahead
                // staging of its own, so higher layers must decide when a flush
                // creates a durable boundary.
                const offset = page.id * self.page_size;
                try self.file.seekTo(offset);
                try self.file.writeAll(&page.data);
                page.is_dirty = false;
            }
        }
        // Ensure data is physically written to disk
        try self.file.sync();
    }

    /// Read database metadata from page 0.
    ///
    /// Returns the MetaData structure stored in the first page.
    /// This should be called on database open to verify format and
    /// get database state.
    pub fn readMetadata(self: *Pager) !constants.MetaData {
        const meta_page = try self.getPage(constants.META_PAGE_ID);
        const bytes = meta_page.asSlice()[0..@sizeOf(constants.MetaData)];
        return std.mem.bytesToValue(constants.MetaData, bytes);
    }

    /// Update database metadata on page 0.
    ///
    /// Writes the given metadata structure to page 0 and marks it dirty.
    /// Call flush() afterwards to persist the changes.
    pub fn writeMetadata(self: *Pager, metadata: constants.MetaData) !void {
        const meta_page = try self.getPage(constants.META_PAGE_ID);
        // Metadata lives in page 0 and is rewritten through the page cache so
        // header updates follow the same dirty-page lifecycle as normal pages.
        const bytes = std.mem.asBytes(&metadata);
        @memcpy(meta_page.asSlice()[0..bytes.len], bytes);
        meta_page.markDirty();
    }

    /// Read the B-tree node header from a page.
    ///
    /// Parameters:
    ///   - page: The page containing a B-tree node
    ///
    /// Returns: The NodeHeader structure from the beginning of the page
    pub fn readNodeHeader(_self: *Pager, page: *Page) constants.NodeHeader {
        _ = _self;
        const bytes = page.asSlice()[0..@sizeOf(constants.NodeHeader)];
        return std.mem.bytesToValue(constants.NodeHeader, bytes);
    }

    /// Write a B-tree node header to a page.
    ///
    /// Parameters:
    ///   - page: The page to write the header to
    ///   - header: The NodeHeader structure to write
    pub fn writeNodeHeader(self: *Pager, page: *Page, header: constants.NodeHeader) void {
        const bytes = std.mem.asBytes(&header);
        @memcpy(page.asSlice()[0..bytes.len], bytes);
        page.markDirty();
        _ = self;
    }

    /// Get the total number of pages allocated in the database.
    ///
    /// Returns the next page ID to be allocated, which equals the
    /// total number of pages (since IDs start at 0).
    pub fn pageCount(self: *Pager) PageId {
        return self.next_page_id;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "pager: basic operations" {
    const allocator = std.testing.allocator;

    // Use a dedicated file so the test can validate creation and reopen paths
    // against the same on-disk state.
    const test_path = "test_pager.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    // First open should bootstrap the reserved metadata/root pages and persist
    // any newly allocated pages when flushed.
    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        // A fresh database starts with exactly page 0 metadata and page 1 root.
        try std.testing.expectEqual(@as(PageId, 2), p.pageCount());

        // Page 0 must remain addressable through the normal page-loading path.
        const meta_page = try p.getPage(0);
        try std.testing.expectEqual(@as(PageId, 0), meta_page.id);

        // The next allocation should continue immediately after the bootstrap pages.
        const new_page = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), new_page.id);

        // Flush so the reopen phase observes the same page count from disk.
        try p.flush();
    }

    // Reopening the file should recover the exact page count written above.
    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        try std.testing.expectEqual(@as(PageId, 3), p.pageCount());
    }
}

test "pager: freed pages are reused before file growth" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_freelist.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        // Grow beyond the reserved pages so a later free has a reusable target.
        const first = try p.allocatePage();
        const second = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), first.id);
        try std.testing.expectEqual(@as(PageId, 3), second.id);

        // Returning page 2 to the freelist should make it the next allocation.
        try p.freePage(first.id);
        const recycled = try p.allocatePage();
        try std.testing.expectEqual(first.id, recycled.id);

        // Reusing a page must not grow the durable page count or leave freelist head behind.
        try std.testing.expectEqual(@as(PageId, 4), p.pageCount());
        const metadata = try p.readMetadata();
        try std.testing.expectEqual(INVALID_PAGE_ID, metadata.freelist_page);
        try std.testing.expectEqual(@as(PageId, 3), metadata.last_page_id);
    }
}

test "pager: cache lookup returns same page instance" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_cache_lookup.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    const first = try p.getPage(constants.ROOT_PAGE_ID);
    const second = try p.getPage(constants.ROOT_PAGE_ID);

    // Pager.init bootstraps both metadata and root pages, so repeated root lookups
    // should not create any additional cache entries beyond that initial state.
    try std.testing.expect(first == second);
    try std.testing.expectEqual(@as(usize, 2), p.cache_index.count());
}

test "pager: freelist survives reopen" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_freelist_reopen.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        // Free one allocated page, then flush so the reopen phase must recover
        // both the freelist head and the last allocated page ID from metadata.
        const reusable = try p.allocatePage();
        _ = try p.allocatePage();
        try p.freePage(reusable.id);
        try p.flush();
    }

    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        const recycled = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), recycled.id);
        try std.testing.expectEqual(@as(PageId, 4), p.pageCount());

        const metadata = try p.readMetadata();
        try std.testing.expectEqual(INVALID_PAGE_ID, metadata.freelist_page);
        try std.testing.expectEqual(@as(PageId, 3), metadata.last_page_id);
    }
}
