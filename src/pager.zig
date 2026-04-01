const std = @import("std");
const constants = @import("constants.zig");
const PAGE_SIZE = constants.PAGE_SIZE;
const PageId = constants.PageId;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;
const Error = constants.Error;

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
    /// Uses ArrayList instead of HashMap for simplicity and to avoid
    /// alignment issues with certain Zig versions.
    cache: std.ArrayList(CacheEntry),

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

        var pager = Pager{
            .allocator = allocator,
            .file = file,
            .page_size = PAGE_SIZE,
            .cache = cache,
            .next_page_id = if (file_size == 0) 0 else @intCast(file_size / PAGE_SIZE),
        };

        // For new databases, initialize the metadata and root pages
        if (file_size == 0) {
            try pager.allocateMetadataPages();
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
        self.file.close();
    }

    /// Search for a page in the in-memory cache.
    ///
    /// Performs linear search through the cache.
    /// Returns pointer to the page if found, null otherwise.
    fn findInCache(self: *Pager, page_id: PageId) ?*Page {
        for (self.cache.items) |entry| {
            if (entry.page_id == page_id) {
                return entry.page;
            }
        }
        return null;
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
        const page = try self.allocator.create(Page);
        errdefer self.allocator.destroy(page);

        page.* = Page.init(page_id);

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

        // Add to cache for future access
        try self.cache.append(self.allocator, .{
            .page_id = page_id,
            .page = page,
        });
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
        const page_id = self.next_page_id;
        self.next_page_id += 1;

        const page = try self.allocator.create(Page);
        page.* = Page.init(page_id);
        page.clear(); // Zero-initialize and mark dirty

        try self.cache.append(self.allocator, .{
            .page_id = page_id,
            .page = page,
        });
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

test "Pager basic operations" {
    const allocator = std.testing.allocator;

    // Use a temporary file for testing
    const test_path = "/tmp/test_kvdb_pager.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    // Test 1: Create new database
    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        // New database should have 2 initial pages (metadata + root)
        try std.testing.expectEqual(@as(PageId, 2), p.pageCount());

        // Verify we can retrieve metadata page
        const meta_page = try p.getPage(0);
        try std.testing.expectEqual(@as(PageId, 0), meta_page.id);

        // Allocate a new page
        const new_page = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), new_page.id);

        // Ensure changes are persisted before closing
        try p.flush();
    }

    // Test 2: Reopen existing database
    {
        var p = try Pager.init(allocator, test_path);
        defer p.deinit();

        // Should see all 3 pages from previous session
        try std.testing.expectEqual(@as(PageId, 3), p.pageCount());
    }
}
