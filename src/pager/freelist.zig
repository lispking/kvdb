const std = @import("std");
const constants = @import("../constants.zig");
const page_mod = @import("page.zig");
const pager_types = @import("types.zig");
const metadata = @import("metadata.zig");

const Error = constants.Error;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;
const Page = page_mod.Page;
const PageId = constants.PageId;
const FreePageHeader = pager_types.FreePageHeader;

/// Return a recycled page to callers after popping it from the freelist.
pub fn allocateFromFreelist(self: anytype, page_id: PageId) !*Page {
    const page = try self.getPage(page_id);
    const header_bytes = page.asSlice()[0..@sizeOf(FreePageHeader)];
    const header = std.mem.bytesToValue(FreePageHeader, header_bytes);

    try metadata.writeFreelistHead(self, header.next_free_page);
    page.clear();
    return page;
}

/// Record a page on the freelist so later allocations can reuse it.
pub fn freePage(self: anytype, page_id: PageId) !void {
    if (page_id <= constants.ROOT_PAGE_ID or page_id >= self.next_page_id) {
        return Error.InvalidPageId;
    }

    const page = try self.getPage(page_id);
    const freelist_head = try metadata.readFreelistHead(self);
    const header = FreePageHeader{
        .next_free_page = freelist_head,
    };

    @memset(&page.data, 0);
    const bytes = std.mem.asBytes(&header);
    @memcpy(page.asSlice()[0..bytes.len], bytes);
    page.markDirty();

    try metadata.writeFreelistHead(self, page_id);
}

/// Verify that the persisted freelist only references reusable page IDs.
pub fn verifyFreelist(self: anytype) !void {
    var visited = std.AutoHashMap(PageId, void).init(self.allocator);
    defer visited.deinit();

    var page_id = (try metadata.readMetadata(self)).freelist_page;
    while (page_id != INVALID_PAGE_ID) {
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
pub fn freelistPageCount(self: anytype) !usize {
    var count: usize = 0;
    var page_id = (try metadata.readMetadata(self)).freelist_page;

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
