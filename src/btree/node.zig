const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");
const layout = @import("layout.zig");

const Page = pager.Page;
const PageId = constants.PageId;
const Error = constants.Error;
const NodeType = constants.NodeType;
const NodeHeader = constants.NodeHeader;
const PAGE_SIZE = constants.PAGE_SIZE;
const KeyInfo = layout.KeyInfo;
const HEADER_SIZE = layout.HEADER_SIZE;
const MAX_KEYS = layout.MAX_KEYS;
const KEY_INFO_SIZE = layout.KEY_INFO_SIZE;
const DATA_START_OFFSET = layout.DATA_START_OFFSET;

/// Represents a B-tree node stored in a database page.
///
/// A B-tree node can be either:
/// - Leaf node: Contains actual key-value pairs
/// - Internal node: Contains keys and child page references
///
/// This structure provides operations for searching, inserting, and
/// deleting key-value pairs within a single node.
pub const BTreeNode = struct {
    /// The database page containing this node's data
    page: *Page,

    /// Cached copy of the node header
    header: NodeHeader,

    /// Initialize a BTreeNode from a page.
    ///
    /// Reads the node header from the beginning of the page data.
    /// Uses memcpy for safe unaligned memory access.
    ///
    /// Parameters:
    ///   - page: The page containing the B-tree node
    pub fn init(page: *Page) BTreeNode {
        var header: NodeHeader = undefined;
        @memcpy(std.mem.asBytes(&header), page.data[0..HEADER_SIZE]);
        return .{
            .page = page,
            .header = header,
        };
    }

    /// Write the cached header back to the page.
    ///
    /// Call this after modifying the header (e.g., after insert/delete)
    /// to persist the changes to the page data.
    pub fn saveHeader(self: *BTreeNode) void {
        const header_bytes = std.mem.asBytes(&self.header);
        @memcpy(self.page.data[0..HEADER_SIZE], header_bytes);
        self.page.markDirty();
    }

    /// Get a slice of the valid KeyInfo entries.
    ///
    /// Returns a slice containing only the entries that are currently
    /// in use (from index 0 to num_keys-1).
    pub fn getKeyInfoSlice(self: *BTreeNode) []KeyInfo {
        const ptr: [*]KeyInfo = @ptrCast(@alignCast(&self.page.data[HEADER_SIZE]));
        return ptr[0..self.header.num_keys];
    }

    /// Get a raw pointer to the KeyInfo array.
    ///
    /// This provides access to all MAX_KEYS slots, including unused ones, and is
    /// intended for structural rewrites that need to shift or overwrite entries
    /// in place.
    pub fn getKeyInfoPtr(self: *BTreeNode) [*]KeyInfo {
        return @ptrCast(@alignCast(&self.page.data[HEADER_SIZE]));
    }

    /// Calculate the next available data offset.
    ///
    /// Scans all existing entries to find the highest used offset, then returns
    /// that offset as the place to write new data. Because deletes and updates
    /// can leave dead payload bytes behind, this reports the next append point
    /// rather than compacting holes.
    ///
    /// Returns: Byte offset in page where new key data can be written
    pub fn getNextDataOffset(self: *BTreeNode) usize {
        if (self.header.num_keys == 0) {
            return DATA_START_OFFSET;
        }
        // Find the maximum end offset among all existing entries
        const key_infos = self.getKeyInfoSlice();
        var max_end: usize = DATA_START_OFFSET;
        for (key_infos) |ki| {
            const end = ki.value_offset + ki.value_len;
            if (end > max_end) {
                max_end = end;
            }
        }
        return max_end;
    }

    /// Get the child page ID at a given index.
    ///
    /// Internal nodes store child page references at the end of the page.
    /// For a node with N keys, there are N+1 children.
    ///
    /// Parameters:
    ///   - index: Index of the child (0 to num_keys)
    ///
    /// Returns: PageId of the child page
    pub fn getChildPageId(self: *BTreeNode, index: u16) PageId {
        std.debug.assert(self.header.node_type == .internal);
        std.debug.assert(index <= self.header.num_keys);

        const child_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1) + @as(usize, index) * @sizeOf(PageId);
        const bytes = self.page.data[child_offset..][0..@sizeOf(PageId)];
        return std.mem.bytesToValue(PageId, bytes);
    }

    /// Set the child page ID at a given index.
    ///
    /// Parameters:
    ///   - index: Index where to store the child reference
    ///   - page_id: The page ID to store
    pub fn setChildPageId(self: *BTreeNode, index: u16, page_id: PageId) void {
        std.debug.assert(self.header.node_type == .internal);
        const child_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1) + @as(usize, index) * @sizeOf(PageId);
        const bytes = std.mem.asBytes(&page_id);
        @memcpy(self.page.data[child_offset..][0..@sizeOf(PageId)], bytes);
        self.page.markDirty();
    }

    /// Retrieve a key-value pair by index.
    ///
    /// Copies the key and value data into the provided buffers.
    ///
    /// Parameters:
    ///   - index: Index of the entry to retrieve
    ///   - key_buffer: Buffer to copy key data into (must be large enough)
    ///   - value_buffer: Buffer to copy value data into (must be large enough)
    ///
    /// Returns: Struct with slices pointing to the copied data in buffers,
    ///          or null if index is out of range
    pub fn getKeyValue(self: *BTreeNode, index: u16, key_buffer: []u8, value_buffer: []u8) ?struct { key: []u8, value: []u8 } {
        if (index >= self.header.num_keys) return null;

        const key_info = self.getKeyInfoSlice()[index];

        const key = self.page.data[key_info.key_offset..][0..key_info.key_len];
        const value = self.page.data[key_info.value_offset..][0..key_info.value_len];

        @memcpy(key_buffer[0..key.len], key);
        @memcpy(value_buffer[0..value.len], value);

        return .{
            .key = key_buffer[0..key.len],
            .value = value_buffer[0..value.len],
        };
    }

    /// Binary search for a key in this node.
    ///
    /// Searches through the sorted key entries to find the target key.
    ///
    /// Parameters:
    ///   - target_key: The key to search for
    ///
    /// Returns: Struct containing:
    ///   - found: true if key exists, false otherwise
    ///   - index: Position where key was found (if found) or should be inserted (if not found)
    pub fn findKey(self: *BTreeNode, target_key: []const u8) struct { found: bool, index: u16 } {
        var left: u16 = 0;
        var right = self.header.num_keys;

        while (left < right) {
            const mid = (left + right) / 2;
            const key_info = self.getKeyInfoSlice()[mid];
            const key = self.page.data[key_info.key_offset..][0..key_info.key_len];

            const cmp = std.mem.order(u8, key, target_key);
            if (cmp == .eq) {
                return .{ .found = true, .index = mid };
            } else if (cmp == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return .{ .found = false, .index = left };
    }

    /// Check if this node has reached maximum capacity.
    ///
    /// Returns: true if no more keys can be inserted
    pub fn isFull(self: *BTreeNode) bool {
        return self.header.num_keys >= MAX_KEYS;
    }

    /// Insert a key-value pair into a leaf node.
    ///
    /// Preconditions:
    ///   - Node must be a leaf node
    ///   - Node must not be full
    ///   - Key must not already exist
    ///
    /// Parameters:
    ///   - key: The key to insert (must not exceed MAX_KEY_SIZE)
    ///   - value: The value to insert (must not exceed MAX_VALUE_SIZE)
    pub fn insertLeaf(self: *BTreeNode, key: []const u8, value: []const u8) !void {
        std.debug.assert(self.header.node_type == .leaf);
        std.debug.assert(!self.isFull());
        std.debug.assert(key.len <= constants.MAX_KEY_SIZE);
        std.debug.assert(value.len <= constants.MAX_VALUE_SIZE);

        const result = self.findKey(key);
        if (result.found) {
            return Error.KeyAlreadyExists;
        }

        const insert_pos = result.index;

        // Get pointer to KeyInfo array (all MAX_KEYS slots)
        const key_infos = self.getKeyInfoPtr();

        // Shift existing entries to make room for new entry
        var i: u16 = self.header.num_keys;
        while (i > insert_pos) : (i -= 1) {
            key_infos[i] = key_infos[i - 1];
        }

        // Calculate where to write the new key-value data
        // The current layout appends payload bytes instead of compacting holes,
        // so deletion/update churn can eventually exhaust page space even before
        // the node reaches MAX_KEYS.
        const key_offset = self.getNextDataOffset();
        const value_offset = key_offset + key.len;

        // Ensure we don't exceed page bounds
        if (value_offset + value.len > PAGE_SIZE) {
            return Error.PageOverflow;
        }

        // Store key data
        @memcpy(self.page.data[key_offset..][0..key.len], key);

        // Store value data
        @memcpy(self.page.data[value_offset..][0..value.len], value);

        // Update KeyInfo for the new entry
        key_infos[insert_pos] = .{
            .key_offset = @intCast(key_offset),
            .key_len = @intCast(key.len),
            .value_offset = @intCast(value_offset),
            .value_len = @intCast(value.len),
        };

        self.header.num_keys += 1;
        self.saveHeader();
    }

    /// Delete a key-value pair from a leaf node.
    ///
    /// Removes the key and its associated value. This only removes the KeyInfo
    /// entry and updates the header; the actual key/value payload bytes remain
    /// in the page, leaving holes that later inserts will not reuse directly.
    ///
    /// Parameters:
    ///   - key: The key to delete
    pub fn deleteLeaf(self: *BTreeNode, key: []const u8) !void {
        std.debug.assert(self.header.node_type == .leaf);

        const result = self.findKey(key);
        if (!result.found) {
            return Error.KeyNotFound;
        }

        const delete_pos = result.index;
        const key_infos = self.getKeyInfoPtr();

        // Shift entries to cover the deleted one
        var i: u16 = delete_pos;
        while (i < self.header.num_keys - 1) : (i += 1) {
            key_infos[i] = key_infos[i + 1];
        }

        self.header.num_keys -= 1;
        self.saveHeader();
    }

    /// Split this node into two nodes.
    ///
    /// Called when a node overflows. This function only redistributes the local
    /// entries between the current page and `new_page`; parent insertion and root
    /// promotion are handled separately by the higher-level tree logic.
    ///
    /// Parameters:
    ///   - new_page: The page for the new node (must be allocated by caller)
    ///
    /// Returns: The "split key" - the first key that moved to the new node
    pub fn split(self: *BTreeNode, new_page: *Page) ![]const u8 {
        std.debug.assert(self.isFull());

        // This simplified split only redistributes existing entries between two
        // pages. Higher-level root promotion and parent updates are implemented
        // separately when multi-page tree growth is added.
        const mid: u16 = self.header.num_keys / 2;

        // Initialize the new node
        _ = BTreeNode.init(new_page);
        @memset(new_page.data[@as(usize, mid) * KEY_INFO_SIZE + HEADER_SIZE ..], 0);

        // Copy second half of entries to new node
        const self_key_infos = self.getKeyInfoPtr();
        const new_key_infos: [*]KeyInfo = @ptrCast(@alignCast(&new_page.data[HEADER_SIZE]));

        var i: u16 = mid;
        while (i < self.header.num_keys) : (i += 1) {
            new_key_infos[i - mid] = self_key_infos[i];
        }

        // Update headers
        const old_count = self.header.num_keys;
        self.header.num_keys = mid;
        self.saveHeader();

        var new_header = self.header;
        new_header.num_keys = old_count - mid;
        @memcpy(new_page.data[0..HEADER_SIZE], std.mem.asBytes(&new_header));
        new_page.markDirty();

        // Return the split key (first key in new node)
        const mid_key_info = self_key_infos[mid];
        return self.page.data[mid_key_info.key_offset..][0..mid_key_info.key_len];
    }
};
