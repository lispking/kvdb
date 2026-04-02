const std = @import("std");
const constants = @import("../constants.zig");
const pager_types = @import("types.zig");
const page_mod = @import("page.zig");
const cache = @import("cache.zig");
const metadata = @import("metadata.zig");
const freelist = @import("freelist.zig");
const FsyncPolicy = @import("../kvdb/types.zig").FsyncPolicy;

const PAGE_SIZE = constants.PAGE_SIZE;
const PageId = constants.PageId;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;
const Error = constants.Error;
const Page = page_mod.Page;
const CacheEntry = pager_types.CacheEntry;
const CacheIndex = pager_types.CacheIndex;
const PAGE_CACHE_LIMIT = pager_types.PAGE_CACHE_LIMIT;

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

    /// Flush-time cache pruning keeps resident memory bounded between durable
    /// boundaries without invalidating raw page pointers mid-operation.
    cache_limit: usize,

    /// Controls whether flush boundaries force data to stable storage.
    fsync_policy: FsyncPolicy,

    /// Next page ID to allocate.
    /// Incremented when creating new pages. Initialized from file size on open.
    next_page_id: PageId,

    /// Initialize a new Pager for the database file at the given path.
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8, fsync_policy: FsyncPolicy) !Pager {
        const file = try std.fs.cwd().createFile(file_path, .{
            .read = true,
            .truncate = false,
        });
        errdefer file.close();

        const stat = try file.stat();
        const file_size = stat.size;

        var cache_list: std.ArrayList(CacheEntry) = .empty;
        try cache_list.ensureTotalCapacity(allocator, 16);
        var cache_index = CacheIndex.init(allocator);
        errdefer cache_index.deinit();

        var pager = Pager{
            .allocator = allocator,
            .file = file,
            .page_size = PAGE_SIZE,
            .cache = cache_list,
            .cache_index = cache_index,
            .cache_limit = PAGE_CACHE_LIMIT,
            .fsync_policy = fsync_policy,
            .next_page_id = if (file_size == 0) 0 else @intCast(file_size / PAGE_SIZE),
        };

        if (file_size == 0) {
            try metadata.allocateMetadataPages(&pager);
        } else {
            const db_metadata = try metadata.readMetadata(&pager);
            if (db_metadata.isValid()) {
                pager.next_page_id = db_metadata.last_page_id + 1;
            }
        }

        return pager;
    }

    /// Clean up resources and close the database file.
    pub fn deinit(self: *Pager) void {
        for (self.cache.items) |entry| {
            self.allocator.destroy(entry.page);
        }
        self.cache.deinit(self.allocator);
        self.cache_index.deinit();
        self.file.close();
    }

    /// Record a page on the freelist so later allocations can reuse it.
    pub fn freePage(self: *Pager, page_id: PageId) !void {
        return freelist.freePage(self, page_id);
    }

    /// Verify that the persisted freelist only references reusable page IDs.
    pub fn verifyFreelist(self: *Pager) !void {
        return freelist.verifyFreelist(self);
    }

    /// Count pages currently linked from the persisted freelist head.
    pub fn freelistPageCount(self: *Pager) !usize {
        return freelist.freelistPageCount(self);
    }

    /// Retrieve a page by its ID.
    pub fn getPage(self: *Pager, page_id: PageId) !*Page {
        if (cache.findInCache(self, page_id)) |page| {
            return page;
        }

        const page = try cache.ensureCachedPage(self, page_id);
        const offset = page_id * self.page_size;
        try self.file.seekTo(offset);

        var buf: [PAGE_SIZE]u8 = undefined;
        const bytes_read = try self.file.read(&buf);

        if (bytes_read == 0) {
            @memset(&page.data, 0);
        } else if (bytes_read != PAGE_SIZE) {
            return Error.CorruptedData;
        } else {
            page.data = buf;
        }

        return page;
    }

    /// Allocate a new page.
    pub fn allocatePage(self: *Pager) !*Page {
        if (!pager_types.isBootstrappingReservedPages(self.next_page_id)) {
            const freelist_page = try metadata.readFreelistHead(self);
            if (freelist_page != INVALID_PAGE_ID) {
                return freelist.allocateFromFreelist(self, freelist_page);
            }
        }

        const page_id = self.next_page_id;
        self.next_page_id += 1;

        const page = try cache.ensureCachedPage(self, page_id);
        page.clear();

        if (!pager_types.isBootstrappingReservedPages(self.next_page_id)) {
            try metadata.writeLastPageId(self, page_id);
        }

        return page;
    }

    /// Write all dirty pages back to disk.
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
        if (self.fsync_policy == .always) {
            try self.file.sync();
        }
        cache.pruneCache(self);
    }

    /// Read database metadata from page 0.
    pub fn readMetadata(self: *Pager) !constants.MetaData {
        return metadata.readMetadata(self);
    }

    /// Update database metadata on page 0.
    pub fn writeMetadata(self: *Pager, db_metadata: constants.MetaData) !void {
        return metadata.writeMetadata(self, db_metadata);
    }

    /// Read the B-tree node header from a page.
    pub fn readNodeHeader(self: *Pager, page: *Page) constants.NodeHeader {
        return metadata.readNodeHeader(self, page);
    }

    /// Write a B-tree node header to a page.
    pub fn writeNodeHeader(self: *Pager, page: *Page, header: constants.NodeHeader) void {
        return metadata.writeNodeHeader(self, page, header);
    }

    /// Get the total number of pages allocated in the database.
    pub fn pageCount(self: *Pager) PageId {
        return self.next_page_id;
    }
};
