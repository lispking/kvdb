const std = @import("std");
const constants = @import("../constants.zig");

const PAGE_SIZE = constants.PAGE_SIZE;
const PageId = constants.PageId;

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
