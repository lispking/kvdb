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
/// The fixed-width header and `KeyInfo` region stay at the front of the page.
/// Variable-length key/value payloads are packed after that region and grow
/// upward as entries are inserted.
///
/// Each entry stores:
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

/// B-tree index structure.
///
/// Manages a B-tree index for efficient key-value lookups.
/// Supports recursive insert/search across multiple levels, while delete and
/// iteration still keep the simpler pre-rebalance semantics for now.
pub const BTree = struct {
    /// Temporary split result bubbled upward during recursive insert.
    const SplitResult = struct {
        right_page_id: PageId,
        separator_len: usize,
        separator_buf: [constants.MAX_KEY_SIZE]u8,

        /// Return the promoted separator key as a normal slice.
        fn key(self: *const SplitResult) []const u8 {
            return self.separator_buf[0..self.separator_len];
        }
    };

    /// Summary returned after verifying reachable B-tree structure.
    pub const VerifyStats = struct {
        checked_pages: usize,
        checked_entries: usize,
    };

    /// Summary returned after inspecting reachable B-tree structure.
    pub const InspectStats = struct {
        tree_height: usize,
        node_count: usize,
        leaf_count: usize,
        internal_count: usize,
        entry_count: usize,
    };

    const VerifyResult = struct {
        min_key: ?[]u8,
        max_key: ?[]u8,
        page_count: usize,
        entry_count: usize,
    };

    const InspectResult = struct {
        tree_height: usize,
        node_count: usize,
        leaf_count: usize,
        internal_count: usize,
        entry_count: usize,
    };

    /// Owned copy of a leaf entry used while rebuilding split pages.
    const LeafEntry = struct {
        key: []u8,
        value: []u8,
    };

    /// Outcome of attempting to update an existing key in-place at tree level.
    const UpdateResult = enum {
        updated,
        not_found,
    };

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

    /// Free verification min/max key copies after a recursive check.
    fn deinitVerifyResult(allocator: std.mem.Allocator, result: *VerifyResult) void {
        if (result.min_key) |key| allocator.free(key);
        if (result.max_key) |key| allocator.free(key);
    }

    /// Duplicate one optional key if present.
    fn dupeOptionalKey(allocator: std.mem.Allocator, key: ?[]const u8) !?[]u8 {
        if (key) |slice| {
            return try allocator.dupe(u8, slice);
        }
        return null;
    }

    /// Verify one subtree and bubble up ordering bounds plus counts.
    fn verifyRecursive(self: *BTree, pager_ref: *Pager, allocator: std.mem.Allocator, page_id: PageId, visited: *std.AutoHashMap(PageId, void)) !VerifyResult {
        if (page_id >= pager_ref.pageCount()) {
            return Error.CorruptedData;
        }

        const visit = try visited.getOrPut(page_id);
        if (visit.found_existing) {
            return Error.CorruptedData;
        }
        visit.value_ptr.* = {};

        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);

        if (node.header.node_type == .leaf) {
            var previous_key: ?[]const u8 = null;
            for (0..node.header.num_keys) |i| {
                const current_key = getKey(&node, @intCast(i));
                if (previous_key) |prev| {
                    if (std.mem.order(u8, prev, current_key) != .lt) {
                        return Error.CorruptedData;
                    }
                }
                previous_key = current_key;
            }

            return .{
                .min_key = try dupeOptionalKey(allocator, if (node.header.num_keys == 0) null else getKey(&node, 0)),
                .max_key = try dupeOptionalKey(allocator, if (node.header.num_keys == 0) null else getKey(&node, node.header.num_keys - 1)),
                .page_count = 1,
                .entry_count = node.header.num_keys,
            };
        }

        var min_key: ?[]u8 = null;
        errdefer if (min_key) |key| allocator.free(key);
        var max_key: ?[]u8 = null;
        errdefer if (max_key) |key| allocator.free(key);
        var page_count: usize = 1;
        var entry_count: usize = 0;

        const child_count: u16 = node.header.num_keys + 1;
        for (0..child_count) |child_index_usize| {
            const child_index: u16 = @intCast(child_index_usize);
            const child_page_id = node.getChildPageId(child_index);
            var child_result = try self.verifyRecursive(pager_ref, allocator, child_page_id, visited);
            defer deinitVerifyResult(allocator, &child_result);

            if (child_result.min_key == null and child_result.max_key == null and child_result.entry_count == 0) {
                return Error.CorruptedData;
            }

            if (child_index > 0) {
                const separator = getKey(&node, child_index - 1);
                if (child_result.min_key == null or !std.mem.eql(u8, separator, child_result.min_key.?)) {
                    return Error.CorruptedData;
                }
            }

            if (child_index > 0) {
                if (max_key) |prev_max| {
                    if (child_result.min_key == null or std.mem.order(u8, prev_max, child_result.min_key.?) != .lt) {
                        return Error.CorruptedData;
                    }
                }
            }

            if (min_key == null and child_result.min_key != null) {
                min_key = try allocator.dupe(u8, child_result.min_key.?);
            }
            if (child_result.max_key != null) {
                if (max_key) |existing| allocator.free(existing);
                max_key = try allocator.dupe(u8, child_result.max_key.?);
            }
            page_count += child_result.page_count;
            entry_count += child_result.entry_count;
        }

        return .{
            .min_key = min_key,
            .max_key = max_key,
            .page_count = page_count,
            .entry_count = entry_count,
        };
    }

    /// Inspect one subtree and accumulate structural counts plus height.
    fn inspectRecursive(self: *BTree, pager_ref: *Pager, page_id: PageId) !InspectResult {
        if (page_id >= pager_ref.pageCount()) {
            return Error.CorruptedData;
        }

        const page = try pager_ref.getPage(page_id);
        const header = pager_ref.readNodeHeader(page);

        if (header.node_type == .leaf) {
            return .{
                .tree_height = 1,
                .node_count = 1,
                .leaf_count = 1,
                .internal_count = 0,
                .entry_count = header.num_keys,
            };
        }

        var node = BTreeNode.init(page);
        var max_child_height: usize = 0;
        var node_count: usize = 1;
        var leaf_count: usize = 0;
        var internal_count: usize = 1;
        var entry_count: usize = 0;

        // Aggregate every child subtree so inspect can report the full reachable
        // tree shape instead of only root-local header information.
        for (0..@as(usize, node.header.num_keys) + 1) |child_index_usize| {
            const child_page_id = node.getChildPageId(@intCast(child_index_usize));
            const child_stats = try self.inspectRecursive(pager_ref, child_page_id);
            max_child_height = @max(max_child_height, child_stats.tree_height);
            node_count += child_stats.node_count;
            leaf_count += child_stats.leaf_count;
            internal_count += child_stats.internal_count;
            entry_count += child_stats.entry_count;
        }

        return .{
            .tree_height = max_child_height + 1,
            .node_count = node_count,
            .leaf_count = leaf_count,
            .internal_count = internal_count,
            .entry_count = entry_count,
        };
    }

    /// Duplicate a key into temporary split storage.
    fn dupeKey(allocator: std.mem.Allocator, key: []const u8) ![]u8 {
        return allocator.dupe(u8, key);
    }

    /// Free temporary key storage gathered for an internal-node rebuild.
    fn freeOwnedKeys(allocator: std.mem.Allocator, keys: *std.ArrayList([]u8)) void {
        for (keys.items) |key| {
            allocator.free(key);
        }
        keys.deinit(allocator);
    }

    /// Free temporary leaf-entry storage gathered for a leaf rebuild.
    fn freeLeafEntries(allocator: std.mem.Allocator, entries: *std.ArrayList(LeafEntry)) void {
        for (entries.items) |entry| {
            allocator.free(entry.key);
            allocator.free(entry.value);
        }
        entries.deinit(allocator);
    }

    /// Return a key slice stored at the given index inside the node.
    fn getKey(node: *BTreeNode, index: u16) []const u8 {
        const key_info = node.getKeyInfoSlice()[index];
        return node.page.data[key_info.key_offset..][0..key_info.key_len];
    }

    /// Return a value slice stored at the given leaf index.
    fn getValue(node: *BTreeNode, index: u16) []const u8 {
        const key_info = node.getKeyInfoSlice()[index];
        return node.page.data[key_info.value_offset..][0..key_info.value_len];
    }

    /// Collect a leaf node plus one new entry into a sorted temporary list.
    fn collectLeafEntriesWithInsert(node: *BTreeNode, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !std.ArrayList(LeafEntry) {
        var entries: std.ArrayList(LeafEntry) = .empty;
        try entries.ensureTotalCapacity(allocator, node.header.num_keys + 1);
        errdefer freeLeafEntries(allocator, &entries);

        var inserted = false;
        for (0..node.header.num_keys) |i| {
            const existing_key = getKey(node, @intCast(i));
            const existing_value = getValue(node, @intCast(i));

            if (!inserted and std.mem.order(u8, key, existing_key) == .lt) {
                try entries.append(allocator, .{
                    .key = try allocator.dupe(u8, key),
                    .value = try allocator.dupe(u8, value),
                });
                inserted = true;
            }

            if (std.mem.eql(u8, existing_key, key)) {
                return Error.KeyAlreadyExists;
            }

            try entries.append(allocator, .{
                .key = try allocator.dupe(u8, existing_key),
                .value = try allocator.dupe(u8, existing_value),
            });
        }

        if (!inserted) {
            try entries.append(allocator, .{
                .key = try allocator.dupe(u8, key),
                .value = try allocator.dupe(u8, value),
            });
        }

        return entries;
    }

    /// Rewrite a page as a packed leaf node containing the provided entries.
    fn writeLeafNode(node: *BTreeNode, entries: []const LeafEntry) !void {
        node.page.clear();
        node.header = .{
            .node_type = .leaf,
            .num_keys = 0,
        };
        node.saveHeader();

        for (entries) |entry| {
            try node.insertLeaf(entry.key, entry.value);
        }
    }

    /// Repack a leaf node from its current logical entries plus one replacement.
    fn rewriteLeafWithUpdatedValue(node: *BTreeNode, allocator: std.mem.Allocator, update_index: u16, value: []const u8) !void {
        var entries: std.ArrayList(LeafEntry) = .empty;
        try entries.ensureTotalCapacity(allocator, node.header.num_keys);
        defer freeLeafEntries(allocator, &entries);

        for (0..node.header.num_keys) |i| {
            const existing_key = getKey(node, @intCast(i));
            const existing_value = if (i == update_index) value else getValue(node, @intCast(i));
            try entries.append(allocator, .{
                .key = try allocator.dupe(u8, existing_key),
                .value = try allocator.dupe(u8, existing_value),
            });
        }

        // Rebuild the leaf densely so update churn does not leave dead payload
        // bytes behind after repeated overwrite operations.
        try writeLeafNode(node, entries.items);
    }

    /// Collect all separator keys and child pointers from an internal node.
    fn collectInternalState(node: *BTreeNode, allocator: std.mem.Allocator, keys: *std.ArrayList([]u8), child_ids: *std.ArrayList(PageId)) !void {
        try keys.ensureTotalCapacity(allocator, node.header.num_keys + 1);
        try child_ids.ensureTotalCapacity(allocator, node.header.num_keys + 2);

        for (0..node.header.num_keys) |i| {
            try child_ids.append(allocator, node.getChildPageId(@intCast(i)));
            try keys.append(allocator, try dupeKey(allocator, getKey(node, @intCast(i))));
        }
        try child_ids.append(allocator, node.getChildPageId(node.header.num_keys));
    }

    /// Insert one promoted separator and right-child pointer into temp state.
    fn insertInternalState(keys: *std.ArrayList([]u8), child_ids: *std.ArrayList(PageId), allocator: std.mem.Allocator, insert_index: usize, key: []const u8, right_child_id: PageId) !void {
        std.debug.assert(insert_index <= keys.items.len);
        std.debug.assert(insert_index + 1 <= child_ids.items.len);

        try keys.insert(allocator, insert_index, try dupeKey(allocator, key));
        try child_ids.insert(allocator, insert_index + 1, right_child_id);
    }

    /// Rewrite a page as an internal node with packed keys and child pointers.
    fn writeInternalNode(node: *BTreeNode, keys: []const []const u8, child_ids: []const PageId) !void {
        std.debug.assert(child_ids.len == keys.len + 1);

        node.page.clear();
        node.header = .{
            .node_type = .internal,
            .num_keys = @intCast(keys.len),
        };
        node.saveHeader();

        const child_area_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1);
        const key_infos = node.getKeyInfoPtr();
        var next_offset: usize = DATA_START_OFFSET;

        for (keys, 0..) |key, i| {
            if (next_offset + key.len > child_area_offset) {
                return Error.PageOverflow;
            }

            @memcpy(node.page.data[next_offset..][0..key.len], key);
            key_infos[i] = .{
                .key_offset = @intCast(next_offset),
                .key_len = @intCast(key.len),
                .value_offset = @intCast(next_offset + key.len),
                .value_len = 0,
            };
            next_offset += key.len;
        }

        for (child_ids, 0..) |child_id, i| {
            node.setChildPageId(@intCast(i), child_id);
        }
    }

    /// Return the minimum key reachable from the given subtree.
    fn minKeyInSubtree(self: *BTree, pager_ref: *Pager, page_id: PageId) ![]const u8 {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);

        if (node.header.node_type == .leaf) {
            std.debug.assert(node.header.num_keys > 0);
            return getKey(&node, 0);
        }

        return self.minKeyInSubtree(pager_ref, node.getChildPageId(0));
    }

    /// Recompute all internal separators from their current child subtrees.
    fn normalizeInternalSeparators(self: *BTree, node: *BTreeNode, pager_ref: *Pager) !void {
        if (node.header.node_type != .internal or node.header.num_keys == 0) {
            return;
        }

        var keys: std.ArrayList([]u8) = .empty;
        defer freeOwnedKeys(pager_ref.allocator, &keys);
        var child_ids: std.ArrayList(PageId) = .empty;
        defer child_ids.deinit(pager_ref.allocator);

        try child_ids.ensureTotalCapacity(pager_ref.allocator, node.header.num_keys + 1);
        try keys.ensureTotalCapacity(pager_ref.allocator, node.header.num_keys);

        try child_ids.append(pager_ref.allocator, node.getChildPageId(0));
        for (0..node.header.num_keys) |i| {
            const child_page_id = node.getChildPageId(@intCast(i + 1));
            try child_ids.append(pager_ref.allocator, child_page_id);
            try keys.append(pager_ref.allocator, try dupeKey(pager_ref.allocator, try self.minKeyInSubtree(pager_ref, child_page_id)));
        }

        // Rebuild the node from child minima so random inserts keep every parent
        // separator aligned with the first key reachable in its right subtree.
        try writeInternalNode(node, keys.items, child_ids.items);
    }

    /// Split a full leaf after including the pending insert.
    fn splitLeafAndInsert(self: *BTree, node: *BTreeNode, pager_ref: *Pager, key: []const u8, value: []const u8) !SplitResult {
        _ = self;

        var entries = try collectLeafEntriesWithInsert(node, pager_ref.allocator, key, value);
        defer freeLeafEntries(pager_ref.allocator, &entries);

        const right_page = try pager_ref.allocatePage();
        var right_node = BTreeNode.init(right_page);
        const split_index = entries.items.len / 2;

        // Repack both halves so the split leaves own compact payload layouts.
        try writeLeafNode(node, entries.items[0..split_index]);
        try writeLeafNode(&right_node, entries.items[split_index..]);

        var split = SplitResult{
            .right_page_id = right_page.id,
            .separator_len = entries.items[split_index].key.len,
            .separator_buf = undefined,
        };
        @memcpy(split.separator_buf[0..split.separator_len], entries.items[split_index].key);
        return split;
    }

    /// Split a full internal node after inserting one more separator.
    fn splitInternalAndInsert(self: *BTree, node: *BTreeNode, pager_ref: *Pager, child_index: usize, key: []const u8, right_child_id: PageId) !SplitResult {
        _ = self;

        var keys: std.ArrayList([]u8) = .empty;
        errdefer freeOwnedKeys(pager_ref.allocator, &keys);
        var child_ids: std.ArrayList(PageId) = .empty;
        errdefer child_ids.deinit(pager_ref.allocator);

        try collectInternalState(node, pager_ref.allocator, &keys, &child_ids);
        try insertInternalState(&keys, &child_ids, pager_ref.allocator, child_index, key, right_child_id);

        const promote_index = keys.items.len / 2;
        const promoted_key = keys.items[promote_index];
        const right_page = try pager_ref.allocatePage();
        var right_node = BTreeNode.init(right_page);

        // Keep the promoted separator only in the parent; the right child starts
        // with the keys that remain strictly to the right of that boundary.
        try writeInternalNode(node, keys.items[0..promote_index], child_ids.items[0 .. promote_index + 1]);
        try writeInternalNode(&right_node, keys.items[promote_index + 1 ..], child_ids.items[promote_index + 1 ..]);

        var split = SplitResult{
            .right_page_id = right_page.id,
            .separator_len = promoted_key.len,
            .separator_buf = undefined,
        };
        @memcpy(split.separator_buf[0..split.separator_len], promoted_key);

        freeOwnedKeys(pager_ref.allocator, &keys);
        child_ids.deinit(pager_ref.allocator);
        return split;
    }

    /// Insert a promoted separator into an internal node that still has room.
    fn insertIntoInternal(self: *BTree, node: *BTreeNode, pager_ref: *Pager, child_index: usize, key: []const u8, right_child_id: PageId) !void {
        _ = self;

        var keys: std.ArrayList([]u8) = .empty;
        defer freeOwnedKeys(pager_ref.allocator, &keys);
        var child_ids: std.ArrayList(PageId) = .empty;
        defer child_ids.deinit(pager_ref.allocator);

        try collectInternalState(node, pager_ref.allocator, &keys, &child_ids);
        try insertInternalState(&keys, &child_ids, pager_ref.allocator, child_index, key, right_child_id);
        try writeInternalNode(node, keys.items, child_ids.items);
    }

    /// Promote a split root into a fresh internal root that keeps the root page ID.
    fn promoteRootSplit(self: *BTree, pager_ref: *Pager, split: SplitResult) !void {
        const root_page = try pager_ref.getPage(self.root_page_id);
        const left_page = try pager_ref.allocatePage();
        @memcpy(left_page.data[0..], root_page.data[0..]);
        left_page.markDirty();

        var root_node = BTreeNode.init(root_page);
        const root_keys = [_][]const u8{split.key()};
        const root_children = [_]PageId{ left_page.id, split.right_page_id };

        // Rewrite the original root page in place so metadata can keep pointing
        // at the same root page ID even after the tree gains a new level.
        try writeInternalNode(&root_node, root_keys[0..], root_children[0..]);
        try self.normalizeInternalSeparators(&root_node, pager_ref);
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

    /// Recursive helper for update that repacks leaf payloads in place.
    fn updateRecursive(self: *BTree, pager_ref: *Pager, page_id: PageId, key: []const u8, value: []const u8) !UpdateResult {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);
        const result = node.findKey(key);

        if (node.header.node_type == .leaf) {
            if (!result.found) {
                return .not_found;
            }

            try rewriteLeafWithUpdatedValue(&node, pager_ref.allocator, result.index, value);
            return .updated;
        }

        const child_index = if (result.found) result.index + 1 else result.index;
        return self.updateRecursive(pager_ref, node.getChildPageId(child_index), key, value);
    }

    /// Recursive helper for insert that bubbles split information upward.
    fn insertRecursive(self: *BTree, pager_ref: *Pager, page_id: PageId, key: []const u8, value: []const u8) !?SplitResult {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);

        if (node.header.node_type == .leaf) {
            if (!node.isFull()) {
                try node.insertLeaf(key, value);
                return null;
            }
            return try self.splitLeafAndInsert(&node, pager_ref, key, value);
        }

        const result = node.findKey(key);
        const child_index = if (result.found) result.index + 1 else result.index;
        const child_page_id = node.getChildPageId(child_index);
        const child_split = try self.insertRecursive(pager_ref, child_page_id, key, value);

        if (child_split) |split| {
            if (!node.isFull()) {
                try self.insertIntoInternal(&node, pager_ref, child_index, split.key(), split.right_page_id);
                try self.normalizeInternalSeparators(&node, pager_ref);
                return null;
            }

            const node_split = try self.splitInternalAndInsert(&node, pager_ref, child_index, split.key(), split.right_page_id);
            try self.normalizeInternalSeparators(&node, pager_ref);
            var right_node = BTreeNode.init(try pager_ref.getPage(node_split.right_page_id));
            try self.normalizeInternalSeparators(&right_node, pager_ref);
            return node_split;
        }

        // Even without a structural split, a child may have gained a new minimum
        // key, so parent separators must be refreshed after the recursive insert.
        try self.normalizeInternalSeparators(&node, pager_ref);
        return null;
    }

    /// Recursive helper for delete without rebalancing.
    fn deleteRecursive(self: *BTree, pager_ref: *Pager, page_id: PageId, key: []const u8) !void {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);
        const result = node.findKey(key);

        if (node.header.node_type == .leaf) {
            // Until borrow/merge/root-shrink land, deleting the last entry from a
            // non-root leaf would create an underflow the current tree cannot fix.
            if (result.found and page_id != self.root_page_id and node.header.num_keys == 1) {
                return Error.NodeEmpty;
            }

            try node.deleteLeaf(key);
            return;
        }

        // Continue following the same separator-routing rule used by lookup and
        // insert. Rebalancing and root shrink remain later work, but successful
        // deletes still refresh parent separators so searches stay correct.
        const child_index = if (result.found) result.index + 1 else result.index;
        try self.deleteRecursive(pager_ref, node.getChildPageId(child_index), key);
        try self.normalizeInternalSeparators(&node, pager_ref);
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
    /// Parameters:
    ///   - pager_ref: Pager for loading/saving pages
    ///   - key: Key to insert
    ///   - value: Value to associate with key
    pub fn put(self: *BTree, pager_ref: *Pager, key: []const u8, value: []const u8) !void {
        if (try self.insertRecursive(pager_ref, self.root_page_id, key, value)) |split| {
            try self.promoteRootSplit(pager_ref, split);
        }
    }

    /// Update an existing key-value pair without delete+insert churn.
    pub fn update(self: *BTree, pager_ref: *Pager, key: []const u8, value: []const u8) !void {
        switch (try self.updateRecursive(pager_ref, self.root_page_id, key, value)) {
            .updated => {},
            .not_found => return Error.KeyNotFound,
        }
    }

    /// Delete a key-value pair.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading/saving pages
    ///   - key: Key to delete
    pub fn delete(self: *BTree, pager_ref: *Pager, key: []const u8) !void {
        try self.deleteRecursive(pager_ref, self.root_page_id, key);
    }

    /// Verify that reachable B-tree pages remain ordered and structurally consistent.
    pub fn verify(self: *BTree, pager_ref: *Pager) !VerifyStats {
        var visited = std.AutoHashMap(PageId, void).init(pager_ref.allocator);
        defer visited.deinit();

        var result = try self.verifyRecursive(pager_ref, pager_ref.allocator, self.root_page_id, &visited);
        defer deinitVerifyResult(pager_ref.allocator, &result);

        return .{
            .checked_pages = result.page_count,
            .checked_entries = result.entry_count,
        };
    }

    /// Inspect the reachable B-tree shape without performing full validation.
    pub fn inspect(self: *BTree, pager_ref: *Pager) !InspectStats {
        const result = try self.inspectRecursive(pager_ref, self.root_page_id);
        return .{
            .tree_height = result.tree_height,
            .node_count = result.node_count,
            .leaf_count = result.leaf_count,
            .internal_count = result.internal_count,
            .entry_count = result.entry_count,
        };
    }

    /// Iterator for traversing all key-value pairs in the B-tree.
    ///
    /// Uses an explicit page stack so iteration stays sorted across every leaf in
    /// a multi-level tree without requiring leaf-link pointers.
    pub const Iterator = struct {
        const StackFrame = struct {
            page: *Page,
            next_child_index: u16,
        };

        /// Pager for accessing pages
        pager_ref: *Pager,
        /// Stack of internal nodes still being traversed
        stack: std.ArrayList(StackFrame),
        /// Current leaf page being iterated
        current_page: ?*Page,
        /// Current position in page
        current_index: u16,
        /// Buffer for key data
        key_buffer: [constants.MAX_KEY_SIZE]u8,
        /// Buffer for value data
        value_buffer: [constants.MAX_VALUE_SIZE]u8,

        /// Release iterator-owned traversal state.
        pub fn deinit(self: *Iterator) void {
            self.stack.deinit(self.pager_ref.allocator);
        }

        /// Descend to the leftmost leaf reachable from the given page.
        fn descendToLeaf(self: *Iterator, start_page: *Page) !void {
            var page = start_page;

            while (true) {
                const node = BTreeNode.init(page);
                if (node.header.node_type == .leaf) {
                    self.current_page = page;
                    self.current_index = 0;
                    return;
                }

                // Keep the internal node on the stack so later calls can resume
                // from its next child after the current subtree is exhausted.
                try self.stack.append(self.pager_ref.allocator, .{
                    .page = page,
                    .next_child_index = 1,
                });
                const child_page_id = (@constCast(&node)).getChildPageId(0);
                page = try self.pager_ref.getPage(child_page_id);
            }
        }

        /// Advance to the next leaf in sorted order.
        fn advanceToNextLeaf(self: *Iterator) !void {
            while (self.stack.items.len > 0) {
                var frame = &self.stack.items[self.stack.items.len - 1];
                var node = BTreeNode.init(frame.page);

                if (frame.next_child_index <= node.header.num_keys) {
                    const child_page_id = node.getChildPageId(frame.next_child_index);
                    const child_page = try self.pager_ref.getPage(child_page_id);
                    frame.next_child_index += 1;
                    try self.descendToLeaf(child_page);
                    return;
                }

                _ = self.stack.pop();
            }

            self.current_page = null;
            self.current_index = 0;
        }

        /// Get the next key-value pair.
        ///
        /// Returns: Entry with key and value slices, or null at end
        pub fn next(self: *Iterator) !?struct { key: []const u8, value: []const u8 } {
            while (true) {
                const page = self.current_page orelse return null;
                var node = BTreeNode.init(page);

                if (self.current_index < node.header.num_keys) {
                    const kv = node.getKeyValue(self.current_index, &self.key_buffer, &self.value_buffer).?;
                    self.current_index += 1;

                    return .{
                        .key = kv.key,
                        .value = kv.value,
                    };
                }

                try self.advanceToNextLeaf();
            }
        }
    };

    /// Create an iterator over all entries in the B-tree.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading pages
    ///
    /// Returns: Iterator positioned at first entry
    pub fn iterator(self: *BTree, pager_ref: *Pager) !Iterator {
        var iter = Iterator{
            .pager_ref = pager_ref,
            .stack = .empty,
            .current_page = null,
            .current_index = 0,
            .key_buffer = undefined,
            .value_buffer = undefined,
        };
        errdefer iter.deinit();

        const root = try pager_ref.getPage(self.root_page_id);
        try iter.descendToLeaf(root);
        return iter;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "btree: basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reinitialize the reserved root page as an empty leaf so the test can
    // exercise the single-page insert/search/delete path directly.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Seed a few entries so lookups cover sorted insertion and retrieval.
    try tree.put(&p, "hello", "world");
    try tree.put(&p, "foo", "bar");
    try tree.put(&p, "zig", "awesome");

    // Confirm the first inserted key can be found with its stored payload.
    const value1 = try tree.get(&p, "hello", allocator);
    try std.testing.expect(value1 != null);
    try std.testing.expectEqualStrings("world", value1.?);
    defer allocator.free(value1.?);

    // Check an additional key so the test does not only validate one slot.
    const foo_value = try tree.get(&p, "foo", allocator);
    try std.testing.expect(foo_value != null);
    try std.testing.expectEqualStrings("bar", foo_value.?);
    defer allocator.free(foo_value.?);

    // Remove one key and verify the leaf-level delete path makes it disappear.
    try tree.delete(&p, "foo");
    const value2 = try tree.get(&p, "foo", allocator);
    try std.testing.expect(value2 == null);
}

test "btree: root split handles first overflow" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_root_split.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Start from an empty leaf root so the test drives the first overflow path.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Fill the original root leaf to capacity with ordered keys.
    for (0..MAX_KEYS) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>3}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>3}", .{i});
        try tree.put(&p, key, value);
    }

    // One more insert should trigger the first root split instead of NodeFull.
    try tree.put(&p, "k999", "v999");

    // The original root page should now be an internal node.
    const new_root = BTreeNode.init(try p.getPage(constants.ROOT_PAGE_ID));
    try std.testing.expectEqual(NodeType.internal, new_root.header.node_type);
    try std.testing.expectEqual(@as(u16, 1), new_root.header.num_keys);

    // Reads should still succeed from both children after the split.
    const low = try tree.get(&p, "k000", allocator);
    try std.testing.expect(low != null);
    try std.testing.expectEqualStrings("v000", low.?);
    defer allocator.free(low.?);

    const high = try tree.get(&p, "k999", allocator);
    try std.testing.expect(high != null);
    try std.testing.expectEqualStrings("v999", high.?);
    defer allocator.free(high.?);
}

test "btree: internal insert split propagation keeps tree searchable" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_internal_split.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reset the reserved root page so this test exercises multi-level growth
    // from a clean leaf root under deterministic key ordering.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert enough ordered keys to force repeated child splits and at least one
    // internal-root split into a three-level tree.
    for (0..260) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // The root should remain internal once the tree has grown past one level.
    const new_root = BTreeNode.init(try p.getPage(constants.ROOT_PAGE_ID));
    try std.testing.expectEqual(NodeType.internal, new_root.header.node_type);
    try std.testing.expect(new_root.header.num_keys >= 1);

    // Boundary and interior keys should still resolve correctly after multiple
    // promoted separators and internal-node rewrites.
    const first = try tree.get(&p, "k0000", allocator);
    try std.testing.expect(first != null);
    try std.testing.expectEqualStrings("v0000", first.?);
    defer allocator.free(first.?);

    const middle = try tree.get(&p, "k0128", allocator);
    try std.testing.expect(middle != null);
    try std.testing.expectEqualStrings("v0128", middle.?);
    defer allocator.free(middle.?);

    const last = try tree.get(&p, "k0259", allocator);
    try std.testing.expect(last != null);
    try std.testing.expectEqualStrings("v0259", last.?);
    defer allocator.free(last.?);
}

test "btree: ordered multi-level search hits separator boundaries" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_ordered_boundaries.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Start from a clean leaf root so ordered inserts alone determine every
    // separator promoted into the internal levels.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Drive the tree deep enough that searches must cross several promoted
    // boundaries instead of staying in the original root split.
    for (0..320) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // Probe keys around likely split boundaries plus a miss between them to make
    // sure child selection stays stable at separator edges.
    const probe_keys = [_][]const u8{ "k0000", "k0063", "k0064", "k0127", "k0128", "k0191", "k0192", "k0255", "k0256", "k0319" };
    const probe_values = [_][]const u8{ "v0000", "v0063", "v0064", "v0127", "v0128", "v0191", "v0192", "v0255", "v0256", "v0319" };
    for (probe_keys, probe_values) |probe_key, expected_value| {
        const value = try tree.get(&p, probe_key, allocator);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings(expected_value, value.?);
        defer allocator.free(value.?);
    }

    const missing = try tree.get(&p, "k0325", allocator);
    try std.testing.expect(missing == null);
}

test "btree: mixed-order multi-level inserts remain searchable" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_mixed_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reset the root so a non-monotonic insert order can exercise recursive
    // routing across multiple branches from a clean tree state.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert evens first and odds second so the tree grows across mixed ranges
    // instead of only along a purely ordered append pattern.
    for (0..100) |i| {
        const id = i * 2;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }
    for (0..100) |i| {
        const id = i * 2 + 1;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }

    // Re-read a spread of inserted keys to confirm mixed-order routing still
    // lands in the correct leaf after internal-node promotions.
    const check_ids = [_]u16{ 0, 7, 31, 63, 64, 99, 127, 128, 173, 199 };
    for (check_ids) |id| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const expected = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        const value = try tree.get(&p, key, allocator);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings(expected, value.?);
        defer allocator.free(value.?);
    }
}

test "btree: iterator traverses multi-page tree in sorted order" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_iterator_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reset the root so iterator coverage starts from a clean tree that must
    // grow beyond a single page before traversal begins.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert enough ordered rows to force multiple leaves and internal levels.
    for (0..150) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "it{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    var iter = try tree.iterator(&p);
    defer iter.deinit();

    var expected_index: usize = 0;
    while (try iter.next()) |entry| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const expected_key = try std.fmt.bufPrint(&key_buf, "it{d:0>4}", .{expected_index});
        const expected_value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{expected_index});
        try std.testing.expectEqualStrings(expected_key, entry.key);
        try std.testing.expectEqualStrings(expected_value, entry.value);
        expected_index += 1;
    }

    // The iterator should visit every inserted row exactly once.
    try std.testing.expectEqual(@as(usize, 150), expected_index);
}

test "btree: multi-page delete updates surviving separator search paths" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_delete_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reset the root so delete coverage starts from a deterministic multi-page
    // tree instead of inheriting earlier structural state.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Build a multi-page tree large enough that deleting one boundary key forces
    // parent separators to be refreshed for the surviving right subtree.
    for (0..130) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "d{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // Remove the first key from a non-root right subtree and ensure nearby keys
    // remain searchable through the updated separator chain.
    try tree.delete(&p, "d0064");

    const removed = try tree.get(&p, "d0064", allocator);
    try std.testing.expect(removed == null);

    const next_value = try tree.get(&p, "d0065", allocator);
    try std.testing.expect(next_value != null);
    try std.testing.expectEqualStrings("val0065", next_value.?);
    defer allocator.free(next_value.?);

    const far_value = try tree.get(&p, "d0129", allocator);
    try std.testing.expect(far_value != null);
    try std.testing.expectEqualStrings("val0129", far_value.?);
    defer allocator.free(far_value.?);
}

test "btree: multi-page delete rejects unsupported leaf underflow" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_delete_underflow.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Reset the root so this test can create a small manual multi-page layout
    // with a single-entry non-root leaf and no rebalance support.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    root_page.clear();
    var root_node = BTreeNode.init(root_page);
    root_node.header = .{
        .node_type = .internal,
        .num_keys = 1,
    };
    root_node.saveHeader();

    const left_page = try p.allocatePage();
    var left_page_node = BTreeNode.init(left_page);
    left_page_node.header = .{
        .node_type = .leaf,
        .num_keys = 0,
    };
    left_page_node.saveHeader();
    try left_page_node.insertLeaf("a000", "left");

    const right_page = try p.allocatePage();
    var right_page_node = BTreeNode.init(right_page);
    right_page_node.header = .{
        .node_type = .leaf,
        .num_keys = 0,
    };
    right_page_node.saveHeader();
    try right_page_node.insertLeaf("z000", "right");

    const separator_key = "z000";
    const key_infos = root_node.getKeyInfoPtr();
    @memcpy(root_page.data[DATA_START_OFFSET..][0..separator_key.len], separator_key);
    key_infos[0] = .{
        .key_offset = @intCast(DATA_START_OFFSET),
        .key_len = @intCast(separator_key.len),
        .value_offset = @intCast(DATA_START_OFFSET + separator_key.len),
        .value_len = 0,
    };
    root_page.markDirty();
    root_node.setChildPageId(0, left_page.id);
    root_node.setChildPageId(1, right_page.id);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Deleting the lone entry from a non-root leaf would require borrow/merge,
    // so the current staged implementation should reject it cleanly.
    try std.testing.expectError(Error.NodeEmpty, tree.delete(&p, "z000"));

    const value = try tree.get(&p, "z000", allocator);
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("right", value.?);
    defer allocator.free(value.?);
}

test "btree: repeated updates repack leaf payloads" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_update_repack.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try Pager.init(allocator, test_path);
    defer p.deinit();

    // Start from a clean leaf root so repeated updates exercise the repack path
    // without multi-page structure affecting the payload layout assertions.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try tree.put(&p, "name", "alpha");

    // Repeatedly overwrite the same key with larger payloads; repacking should
    // keep only one logical entry and leave the latest value readable.
    for (0..40) |i| {
        var value_buf: [96]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "updated-value-{d:0>2}-with-extra-padding", .{i});
        try tree.update(&p, "name", value);
    }

    const page = try p.getPage(constants.ROOT_PAGE_ID);
    var node = BTreeNode.init(page);
    try std.testing.expectEqual(@as(u16, 1), node.header.num_keys);

    const next_offset = node.getNextDataOffset();
    try std.testing.expect(next_offset < DATA_START_OFFSET + 96);

    const value = try tree.get(&p, "name", allocator);
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("updated-value-39-with-extra-padding", value.?);
    defer allocator.free(value.?);
}
