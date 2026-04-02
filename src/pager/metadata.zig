const std = @import("std");
const constants = @import("../constants.zig");
const page_mod = @import("page.zig");

const Page = page_mod.Page;

/// Initialize the metadata pages for a new database.
///
/// Creates:
/// - Page 0: Metadata page containing database header
/// - Page 1: Root page of the B-tree (initialized as empty leaf node)
///
/// Both pages are immediately flushed to disk.
pub fn allocateMetadataPages(self: anytype) !void {
    const meta_page = try self.allocatePage();
    std.debug.assert(meta_page.id == constants.META_PAGE_ID);

    const root_page = try self.allocatePage();
    std.debug.assert(root_page.id == constants.ROOT_PAGE_ID);

    const header = constants.NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    writeNodeHeader(self, root_page, header);

    try writeMetadata(self, constants.MetaData.init());
    try self.flush();
}

/// Read the freelist head from metadata once bootstrapping has finished.
pub fn readFreelistHead(self: anytype) !constants.PageId {
    const metadata = try readMetadata(self);
    return metadata.freelist_page;
}

/// Persist a new freelist head in metadata.
pub fn writeFreelistHead(self: anytype, freelist_page: constants.PageId) !void {
    var metadata = try readMetadata(self);
    metadata.freelist_page = freelist_page;
    try writeMetadata(self, metadata);
}

/// Persist the last allocated page ID so metadata mirrors file growth.
pub fn writeLastPageId(self: anytype, last_page_id: constants.PageId) !void {
    var metadata = try readMetadata(self);
    metadata.last_page_id = last_page_id;
    try writeMetadata(self, metadata);
}

/// Read database metadata from page 0.
pub fn readMetadata(self: anytype) !constants.MetaData {
    const meta_page = try self.getPage(constants.META_PAGE_ID);
    const bytes = meta_page.asSlice()[0..@sizeOf(constants.MetaData)];
    return std.mem.bytesToValue(constants.MetaData, bytes);
}

/// Update database metadata on page 0.
pub fn writeMetadata(self: anytype, metadata: constants.MetaData) !void {
    const meta_page = try self.getPage(constants.META_PAGE_ID);
    const bytes = std.mem.asBytes(&metadata);
    @memcpy(meta_page.asSlice()[0..bytes.len], bytes);
    meta_page.markDirty();
}

/// Read the B-tree node header from a page.
pub fn readNodeHeader(_self: anytype, page: *Page) constants.NodeHeader {
    _ = _self;
    const bytes = page.asSlice()[0..@sizeOf(constants.NodeHeader)];
    return std.mem.bytesToValue(constants.NodeHeader, bytes);
}

/// Write a B-tree node header to a page.
pub fn writeNodeHeader(self: anytype, page: *Page, header: constants.NodeHeader) void {
    const bytes = std.mem.asBytes(&header);
    @memcpy(page.asSlice()[0..bytes.len], bytes);
    page.markDirty();
    _ = self;
}
