const std = @import("std");
const constants = @import("constants.zig");
const pager = @import("pager.zig");
const Pager = pager.Pager;
const Page = pager.Page;
const PageId = constants.PageId;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;
const Error = constants.Error;
const NodeType = constants.NodeType;
const NodeHeader = constants.NodeHeader;
const PAGE_SIZE = constants.PAGE_SIZE;

/// B-tree node storage layout:
///
/// [NodeHeader][KeyInfo * MAX_KEYS][key/value data starting at DATA_START_OFFSET]
///
/// The key/value data grows from DATA_START_OFFSET upwards. Each entry stores:
/// - KeyInfo: { key_offset, key_len, value_offset, value_len }
/// - Key data at key_offset
/// - Value data immediately following key at value_offset
///
/// KeyInfo entries are kept sorted by key for binary search.
/// Information about a single key-value pair stored in a B-tree node.
/// Each entry occupies 8 bytes (packed struct).
const KeyInfo = packed struct {
    /// Offset in page where key data starts
    key_offset: u16,
    /// Length of key in bytes
    key_len: u16,
    /// Offset in page where value data starts
    value_offset: u16,
    /// Length of value in bytes
    value_len: u16,
};

/// Size of the B-tree node header in bytes
const HEADER_SIZE = @sizeOf(NodeHeader);

/// Size of each KeyInfo entry in bytes
const KEY_INFO_SIZE = @sizeOf(KeyInfo);

/// Maximum number of key-value pairs per node
const MAX_KEYS: u16 = 64;

/// Offset where key/value data starts (after header and all KeyInfo slots)
const DATA_START_OFFSET = HEADER_SIZE + KEY_INFO_SIZE * MAX_KEYS;

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
    fn saveHeader(self: *BTreeNode) void {
        const header_bytes = std.mem.asBytes(&self.header);
        @memcpy(self.page.data[0..HEADER_SIZE], header_bytes);
        self.page.markDirty();
    }

    /// Get a slice of the valid KeyInfo entries.
    ///
    /// Returns a slice containing only the entries that are currently
    /// in use (from index 0 to num_keys-1).
    fn getKeyInfoSlice(self: *BTreeNode) []KeyInfo {
        const ptr: [*]KeyInfo = @ptrCast(@alignCast(&self.page.data[HEADER_SIZE]));
        return ptr[0..self.header.num_keys];
    }

    /// Get a raw pointer to the KeyInfo array.
    ///
    /// This provides access to all MAX_KEYS slots, including unused ones.
    /// Needed for insertion when we need to shift entries.
    fn getKeyInfoPtr(self: *BTreeNode) [*]KeyInfo {
        return @ptrCast(@alignCast(&self.page.data[HEADER_SIZE]));
    }

    /// Calculate the next available data offset.
    ///
    /// Scans all existing entries to find the highest used offset,
    /// then returns that offset as the place to write new data.
    ///
    /// Returns: Byte offset in page where new key data can be written
    fn getNextDataOffset(self: *BTreeNode) usize {
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
    fn getChildPageId(self: *BTreeNode, index: u16) PageId {
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
    fn setChildPageId(self: *BTreeNode, index: u16, page_id: PageId) void {
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
    /// Removes the key and its associated value. Note that this only
    /// removes the KeyInfo entry and updates the header - the actual
    /// key/value data remains in the page (simpler implementation).
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
    /// Called when a node overflows. Divides the entries evenly between
    /// this node and a new node.
    ///
    /// Parameters:
    ///   - new_page: The page for the new node (must be allocated by caller)
    ///
    /// Returns: The "split key" - the first key that moved to the new node
    pub fn split(self: *BTreeNode, new_page: *Page) ![]const u8 {
        std.debug.assert(self.isFull());

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

/// B-tree index structure.
///
/// Manages a B-tree index for efficient key-value lookups.
/// Currently implements a simplified version with single-level leaf nodes.
pub const BTree = struct {
    /// Page ID of the root node
    root_page_id: PageId,

    /// Initialize a new B-tree with the given root page.
    ///
    /// Parameters:
    ///   - root_page_id: Page ID of the B-tree root node
    pub fn init(root_page_id: PageId) BTree {
        return .{
            .root_page_id = root_page_id,
        };
    }

    /// Recursive helper for key lookup.
    ///
    /// Traverses the B-tree from the given page, searching for the key.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading pages
    ///   - page_id: Current page to search
    ///   - key: Key to find
    ///   - allocator: Allocator for allocating result value
    ///
    /// Returns: Allocated copy of the value, or null if not found
    fn searchNode(self: *BTree, pager_ref: *Pager, page_id: PageId, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);

        const result = node.findKey(key);

        if (node.header.node_type == .leaf) {
            if (result.found) {
                var key_buffer: [constants.MAX_KEY_SIZE]u8 = undefined;
                var value_buffer: [constants.MAX_VALUE_SIZE]u8 = undefined;
                const kv = node.getKeyValue(result.index, &key_buffer, &value_buffer).?;
                return try allocator.dupe(u8, kv.value);
            }
            return null;
        } else {
            // Internal node: recurse to appropriate child
            const child_index = if (result.found) result.index + 1 else result.index;
            const child_page_id = node.getChildPageId(child_index);
            return self.searchNode(pager_ref, child_page_id, key, allocator);
        }
    }

    /// Public interface for key lookup.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading pages
    ///   - key: Key to look up
    ///   - allocator: Allocator for result value
    ///
    /// Returns: Allocated copy of value, or null if key not found
    pub fn get(self: *BTree, pager_ref: *Pager, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        return self.searchNode(pager_ref, self.root_page_id, key, allocator);
    }

    /// Insert a key-value pair.
    ///
    /// Note: This simplified version only supports single-level leaf nodes.
    /// Will return NodeFull error if the root node becomes full.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading/saving pages
    ///   - key: Key to insert
    ///   - value: Value to associate with key
    pub fn put(self: *BTree, pager_ref: *Pager, key: []const u8, value: []const u8) !void {
        const root_page = try pager_ref.getPage(self.root_page_id);
        var root_node = BTreeNode.init(root_page);

        if (root_node.isFull()) {
            return Error.NodeFull;
        }

        try root_node.insertLeaf(key, value);
    }

    /// Delete a key-value pair.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading/saving pages
    ///   - key: Key to delete
    pub fn delete(self: *BTree, pager_ref: *Pager, key: []const u8) !void {
        const root_page = try pager_ref.getPage(self.root_page_id);
        var root_node = BTreeNode.init(root_page);

        if (root_node.header.node_type != .leaf) {
            return Error.InvalidArgument;
        }

        try root_node.deleteLeaf(key);
    }

    /// Iterator for traversing all key-value pairs in the B-tree.
    ///
    /// Currently only supports single-page iteration (simplified implementation).
    pub const Iterator = struct {
        /// Pager for accessing pages
        pager_ref: *Pager,
        /// Current page being iterated
        current_page: ?*Page,
        /// Current position in page
        current_index: u16,
        /// Buffer for key data
        key_buffer: [constants.MAX_KEY_SIZE]u8,
        /// Buffer for value data
        value_buffer: [constants.MAX_VALUE_SIZE]u8,

        /// Get the next key-value pair.
        ///
        /// Returns: Entry with key and value slices, or null at end
        pub fn next(self: *Iterator) ?struct { key: []const u8, value: []const u8 } {
            const page = self.current_page orelse return null;
            var node = BTreeNode.init(page);

            if (self.current_index >= node.header.num_keys) {
                return null;
            }

            const kv = node.getKeyValue(self.current_index, &self.key_buffer, &self.value_buffer).?;
            self.current_index += 1;

            return .{
                .key = kv.key,
                .value = kv.value,
            };
        }
    };

    /// Create an iterator over all entries in the B-tree.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading pages
    ///
    /// Returns: Iterator positioned at first entry
    pub fn iterator(self: *BTree, pager_ref: *Pager) !Iterator {
        const root = try pager_ref.getPage(self.root_page_id);
        return .{
            .pager_ref = pager_ref,
            .current_page = root,
            .current_index = 0,
            .key_buffer = undefined,
            .value_buffer = undefined,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "BTree basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Get root page and initialize as leaf
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert key-value pairs
    try tree.put(&p, "hello", "world");
    try tree.put(&p, "foo", "bar");
    try tree.put(&p, "zig", "awesome");

    // Query and verify
    const value1 = try tree.get(&p, "hello", allocator);
    try std.testing.expect(value1 != null);
    try std.testing.expectEqualStrings("world", value1.?);
    defer allocator.free(value1.?);

    // Query foo
    const foo_value = try tree.get(&p, "foo", allocator);
    try std.testing.expect(foo_value != null);
    try std.testing.expectEqualStrings("bar", foo_value.?);
    defer allocator.free(foo_value.?);

    // Delete
    try tree.delete(&p, "foo");
    const value2 = try tree.get(&p, "foo", allocator);
    try std.testing.expect(value2 == null);
}
