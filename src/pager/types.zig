const std = @import("std");
const constants = @import("../constants.zig");
const page_mod = @import("page.zig");

pub const Page = page_mod.Page;
pub const PageId = constants.PageId;

/// Flush-time pruning keeps cache growth bounded without evicting pages while
/// callers may still hold raw `*Page` pointers during tree traversal.
pub const PAGE_CACHE_LIMIT: usize = 256;

/// Header stored inside pages that have been returned to the freelist.
pub const FreePageHeader = packed struct {
    /// Next free page in the singly linked freelist.
    next_free_page: PageId,
};

/// Entry in the page cache tracking a loaded page.
/// Used by the Pager to maintain the in-memory cache of recently accessed pages.
pub const CacheEntry = struct {
    /// ID of the cached page
    page_id: PageId,
    /// Pointer to the allocated page structure
    page: *Page,
};

/// Fast lookup table mapping page IDs to cache entry indexes.
pub const CacheIndex = std.AutoHashMap(PageId, usize);

/// Metadata bootstrapping reserves page 0 and page 1 before a valid metadata
/// header exists, so those first allocations must bypass freelist lookups.
pub fn isBootstrappingReservedPages(next_page_id: PageId) bool {
    return next_page_id <= constants.ROOT_PAGE_ID;
}
