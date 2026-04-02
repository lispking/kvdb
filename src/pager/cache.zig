const constants = @import("../constants.zig");
const page_mod = @import("page.zig");
const pager_types = @import("types.zig");

const Page = page_mod.Page;
const PageId = constants.PageId;
const CacheEntry = pager_types.CacheEntry;

/// Search for a page in the in-memory cache.
pub fn findInCache(self: anytype, page_id: PageId) ?*Page {
    const index = self.cache_index.get(page_id) orelse return null;
    return self.cache.items[index].page;
}

/// Create and cache a fresh in-memory page wrapper for the given page ID.
pub fn createCachedPage(self: anytype, page_id: PageId) !*Page {
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
pub fn ensureCachedPage(self: anytype, page_id: PageId) !*Page {
    if (findInCache(self, page_id)) |page| {
        return page;
    }
    return createCachedPage(self, page_id);
}

/// Return true when this page must stay resident in the cache.
pub fn isReservedCachePage(_self: anytype, page_id: PageId) bool {
    _ = _self;
    return page_id <= constants.ROOT_PAGE_ID;
}

/// Return whether this cache entry can be safely evicted after a flush.
pub fn isEvictableCacheEntry(self: anytype, entry: CacheEntry) bool {
    return !entry.page.is_dirty and !isReservedCachePage(self, entry.page_id);
}

/// Remove one cached page and keep `cache_index` aligned with the packed list.
pub fn removeCacheEntryAt(self: anytype, index: usize) void {
    const removed = self.cache.items[index];
    _ = self.cache_index.remove(removed.page_id);
    self.allocator.destroy(removed.page);

    const last_index = self.cache.items.len - 1;
    if (index != last_index) {
        const moved = self.cache.items[last_index];
        self.cache.items[index] = moved;
        self.cache_index.put(moved.page_id, index) catch unreachable;
    }
    _ = self.cache.pop();
}

/// Drop old clean pages after a durable boundary so cache growth stays bounded.
pub fn pruneCache(self: anytype) void {
    if (self.cache.items.len <= self.cache_limit) {
        return;
    }

    var index = self.cache.items.len;
    while (self.cache.items.len > self.cache_limit and index > 0) {
        index -= 1;
        if (!isEvictableCacheEntry(self, self.cache.items[index])) {
            continue;
        }
        removeCacheEntryAt(self, index);
        if (index > self.cache.items.len) {
            index = self.cache.items.len;
        }
    }
}
