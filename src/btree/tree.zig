const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");
const layout = @import("layout.zig");
const node_mod = @import("node.zig");
const iterator_mod = @import("iterator.zig");

const Pager = pager.Pager;
const PageId = constants.PageId;
const Error = constants.Error;
const BTreeNode = node_mod.BTreeNode;
const MAX_KEYS = layout.MAX_KEYS;
const DATA_START_OFFSET = layout.DATA_START_OFFSET;
const KeyInfo = layout.KeyInfo;
const PAGE_SIZE = constants.PAGE_SIZE;

/// B-tree index structure.
///
/// Manages a B-tree index for efficient key-value lookups.
/// Supports recursive insert/search across multiple levels, while delete and
/// iteration still keep the simpler pre-rebalance semantics for now.
pub const BTree = struct {
    /// Sorted iterator type returned by `BTree.iterator()`.
    pub const Iterator = iterator_mod.Iterator;

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
        /// Number of reachable B-tree pages visited during validation.
        checked_pages: usize,
        /// Number of logical entries traversed while checking ordering.
        checked_entries: usize,
    };

    /// Summary returned after inspecting reachable B-tree structure.
    pub const InspectStats = struct {
        /// Tree height measured in levels including the root.
        tree_height: usize,
        /// Total reachable node count.
        node_count: usize,
        /// Reachable leaf node count.
        leaf_count: usize,
        /// Reachable internal node count.
        internal_count: usize,
        /// Total reachable logical entry count.
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
        try node.validate(pager_ref.pageCount());

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
        var node = BTreeNode.init(page);
        try node.validate(pager_ref.pageCount());

        if (node.header.node_type == .leaf) {
            return .{
                .tree_height = 1,
                .node_count = 1,
                .leaf_count = 1,
                .internal_count = 0,
                .entry_count = node.header.num_keys,
            };
        }

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

    /// Update a leaf entry in place when the new value fits the old slot.
    /// Returns true if in-place update succeeded, false if full rebuild is needed.
    fn tryInPlaceUpdate(node: *BTreeNode, update_index: u16, value: []const u8) bool {
        const key_infos = node.getKeyInfoPtr();
        const old = key_infos[update_index];
        if (value.len > old.value_len) {
            return false;
        }

        @memcpy(node.page.data[old.value_offset..][0..value.len], value);
        if (value.len < old.value_len) {
            // Zero out unused tail so stale bytes cannot be mistaken for valid data.
            @memset(node.page.data[old.value_offset + value.len ..][0 .. old.value_len - value.len], 0);
            key_infos[update_index].value_len = @intCast(value.len);
        }
        node.page.markDirty();
        return true;
    }

    /// Repack a leaf node from its current logical entries plus one replacement.
    fn rewriteLeafWithUpdatedValue(node: *BTreeNode, allocator: std.mem.Allocator, update_index: u16, value: []const u8) !void {
        if (tryInPlaceUpdate(node, update_index, value)) {
            return;
        }

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

        const child_area_offset = constants.PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1);
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
    /// Performs the insert in-place on the page without heap allocations.
    fn insertIntoInternal(node: *BTreeNode, pager_ref: *Pager, child_index: usize, key: []const u8, right_child_id: PageId) !void {
        _ = pager_ref;

        const num_keys: usize = node.header.num_keys;
        const key_infos = node.getKeyInfoPtr();

        // Compute source ranges for KeyInfo memmove.
        const src_count = num_keys - child_index;
        if (src_count > 0) {
            // Shift entries after insert position right by one.
            std.mem.copyBackwards(KeyInfo, key_infos[child_index + 1 .. num_keys + 1], key_infos[child_index..][0..src_count]);
        }

        // Find free space in the current key data area using only the
        // original valid entries. The slot at `num_keys` is newly opened by
        // the shift above and does not contain valid metadata yet.
        var max_offset: usize = DATA_START_OFFSET;
        var i: usize = 0;
        while (i < num_keys) : (i += 1) {
            const end = key_infos[i].key_offset + key_infos[i].key_len;
            if (end > max_offset) max_offset = end;
        }

        const child_area_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1);
        if (max_offset + key.len > child_area_offset) {
            return Error.PageOverflow;
        }

        // Write new key data.
        @memcpy(node.page.data[max_offset..][0..key.len], key);

        // Write new KeyInfo entry.
        key_infos[child_index] = .{
            .key_offset = @intCast(max_offset),
            .key_len = @intCast(key.len),
            .value_offset = @intCast(max_offset + key.len),
            .value_len = 0,
        };

        // Shift child pointers after insert position.
        const child_count = num_keys + 1;
        var child_src = child_count;
        var child_dst: u16 = @intCast(child_count + 1);
        while (child_src > child_index) {
            child_dst -= 1;
            child_src -= 1;
            const cid = node.getChildPageId(@intCast(child_src));
            node.setChildPageId(child_dst, cid);
        }
        node.setChildPageId(@intCast(child_index + 1), right_child_id);

        node.header.num_keys += 1;
        node.saveHeader();
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
                try insertIntoInternal(&node, pager_ref, child_index, split.key(), split.right_page_id);
                return null;
            }

            const node_split = try self.splitInternalAndInsert(&node, pager_ref, child_index, split.key(), split.right_page_id);
            return node_split;
        }

        return null;
    }

    /// Merge two child pages and the separator into the left child.
    /// After merge, the right child page is freed.
    fn mergeChildren(self: *BTree, pager_ref: *Pager, parent_node: *BTreeNode, left_child_idx: u16, parent_page_id: PageId) !void {
        const left_page_id = parent_node.getChildPageId(left_child_idx);
        const right_page_id = parent_node.getChildPageId(left_child_idx + 1);

        const left_page = try pager_ref.getPage(left_page_id);
        const right_page = try pager_ref.getPage(right_page_id);
        var left_child = BTreeNode.init(left_page);
        var right_child = BTreeNode.init(right_page);

        const separator_key = getKey(parent_node, left_child_idx);

        if (left_child.header.node_type == .leaf) {
            // Merge right leaf into left leaf.
            // Collect all entries from left child, separator, and right child.
            var entries: std.ArrayList(LeafEntry) = .empty;
            try entries.ensureTotalCapacity(pager_ref.allocator, left_child.header.num_keys + 1 + right_child.header.num_keys);
            defer freeLeafEntries(pager_ref.allocator, &entries);

            for (0..left_child.header.num_keys) |i| {
                try entries.append(pager_ref.allocator, .{
                    .key = try pager_ref.allocator.dupe(u8, getKey(&left_child, @intCast(i))),
                    .value = try pager_ref.allocator.dupe(u8, getValue(&left_child, @intCast(i))),
                });
            }
            try entries.append(pager_ref.allocator, .{
                .key = try pager_ref.allocator.dupe(u8, separator_key),
                .value = try pager_ref.allocator.dupe(u8, &.{}),
            });
            for (0..right_child.header.num_keys) |i| {
                try entries.append(pager_ref.allocator, .{
                    .key = try pager_ref.allocator.dupe(u8, getKey(&right_child, @intCast(i))),
                    .value = try pager_ref.allocator.dupe(u8, getValue(&right_child, @intCast(i))),
                });
            }

            try writeLeafNode(&left_child, entries.items);
        } else {
            // Merge two internal nodes with separator.
            var keys: std.ArrayList([]u8) = .empty;
            defer freeOwnedKeys(pager_ref.allocator, &keys);
            var child_ids: std.ArrayList(PageId) = .empty;
            defer child_ids.deinit(pager_ref.allocator);

            // Collect left child's state
            try collectInternalState(&left_child, pager_ref.allocator, &keys, &child_ids);
            // Add separator
            try keys.append(pager_ref.allocator, try pager_ref.allocator.dupe(u8, separator_key));
            try child_ids.append(pager_ref.allocator, right_page_id);
            // Add right child's state
            var right_keys: std.ArrayList([]u8) = .empty;
            defer freeOwnedKeys(pager_ref.allocator, &right_keys);
            var right_child_ids: std.ArrayList(PageId) = .empty;
            defer right_child_ids.deinit(pager_ref.allocator);
            try collectInternalState(&right_child, pager_ref.allocator, &right_keys, &right_child_ids);
            for (right_keys.items) |k| {
                try keys.append(pager_ref.allocator, try pager_ref.allocator.dupe(u8, k));
            }
            for (right_child_ids.items) |cid| {
                try child_ids.append(pager_ref.allocator, cid);
            }

            try writeInternalNode(&left_child, keys.items, child_ids.items);
        }

        // Remove the right child from the parent node.
        // Delete the separator key and shift child pointers.
        try deleteInternalSeparator(parent_node, left_child_idx);

        // Free the right page.
        pager_ref.freePage(right_page_id) catch {};

        // Root shrink: if root becomes a single child, make it the new root.
        if (parent_page_id == self.root_page_id and parent_node.header.num_keys == 0 and parent_node.header.node_type == .internal) {
            const sole_child = parent_node.getChildPageId(0);
            const sole_page = try pager_ref.getPage(sole_child);
            const root_page = try pager_ref.getPage(self.root_page_id);
            @memcpy(root_page.data[0..PAGE_SIZE], sole_page.data[0..PAGE_SIZE]);
            root_page.markDirty();
            pager_ref.freePage(sole_child) catch {};
        }
    }

    /// Delete the separator key at `index` from an internal node and shift child pointers.
    fn deleteInternalSeparator(node: *BTreeNode, index: u16) !void {
        std.debug.assert(node.header.node_type == .internal);
        std.debug.assert(index < node.header.num_keys);

        const key_infos = node.getKeyInfoPtr();
        const num_keys: u16 = node.header.num_keys;

        // Shift KeyInfo entries left
        var i: u16 = index;
        while (i < num_keys - 1) : (i += 1) {
            key_infos[i] = key_infos[i + 1];
        }

        // Shift child pointers left (child at index+1 moves to index, etc.)
        var j: u16 = index + 1;
        while (j < num_keys) : (j += 1) {
            const cid = node.getChildPageId(j + 1);
            node.setChildPageId(j, cid);
        }

        node.header.num_keys -= 1;
        node.saveHeader();
    }

    /// Try to borrow an entry from a sibling. Returns true if borrow succeeded.
    fn tryBorrowFromLeft(pager_ref: *Pager, node: *BTreeNode, child_idx: u16, underflow_page_id: PageId) !bool {
        if (child_idx == 0) return false;

        const sibling_id = node.getChildPageId(child_idx - 1);
        const sibling_page = try pager_ref.getPage(sibling_id);
        var sibling = BTreeNode.init(sibling_page);

        if (sibling.header.num_keys <= 1) return false;

        const separator_key = getKey(node, child_idx - 1);
        const underflow_page = try pager_ref.getPage(underflow_page_id);
        var underflow_node = BTreeNode.init(underflow_page);

        if (underflow_node.header.node_type == .leaf) {
            // Borrow the rightmost entry from the left sibling.
            const last_idx: u16 = sibling.header.num_keys - 1;
            const borrowed_key = getKey(&sibling, last_idx);
            const borrowed_value = getValue(&sibling, last_idx);

            // Insert into the underflow node.
            try insertLeafAtEnd(&underflow_node, borrowed_key, borrowed_value);

            // Update separator to the new rightmost key of sibling.
            const new_separator_idx: u16 = last_idx - 1;
            const new_separator = getKey(&sibling, new_separator_idx);
            try node.updateKey(child_idx - 1, new_separator);

            // Remove borrowed entry from sibling.
            try deleteLastFromNode(&sibling);
        } else {
            // Borrow from internal sibling — move separator down and sibling's rightmost key up.
            const last_idx: u16 = sibling.header.num_keys - 1;
            const borrowed_child = sibling.getChildPageId(last_idx + 1);

            // Insert separator into underflow node as first entry.
            try insertLeafAtStart(&underflow_node, separator_key, &.{});
            underflow_node.setChildPageId(0, borrowed_child);
            underflow_node.header.node_type = .internal;
            underflow_node.saveHeader();

            // Update separator in parent.
            const new_separator = getKey(&sibling, last_idx - 1);
            try node.updateKey(child_idx - 1, new_separator);

            // Remove entry from sibling.
            try deleteLastFromNode(&sibling);
        }

        return true;
    }

    fn tryBorrowFromRight(pager_ref: *Pager, node: *BTreeNode, child_idx: u16, underflow_page_id: PageId) !bool {
        if (child_idx >= node.header.num_keys) return false;

        const sibling_id = node.getChildPageId(child_idx + 1);
        const sibling_page = try pager_ref.getPage(sibling_id);
        var sibling = BTreeNode.init(sibling_page);

        if (sibling.header.num_keys <= 1) return false;

        const separator_key = getKey(node, child_idx);
        const underflow_page = try pager_ref.getPage(underflow_page_id);
        var underflow_node = BTreeNode.init(underflow_page);

        if (underflow_node.header.node_type == .leaf) {
            // Borrow the leftmost entry from the right sibling.
            const first_key = getKey(&sibling, 0);
            const first_value = getValue(&sibling, 0);

            try insertLeafAtEnd(&underflow_node, first_key, first_value);

            // Update separator in parent to sibling's new leftmost.
            if (sibling.header.num_keys > 0) {
                const new_separator = getKey(&sibling, 0);
                try node.updateKey(child_idx, new_separator);
            }

            // Remove first entry from sibling.
            try deleteFirstFromNode(&sibling);
        } else {
            const first_child = sibling.getChildPageId(0);
            try insertLeafAtStart(&underflow_node, separator_key, &.{});
            const last_pos = underflow_node.header.num_keys - 1;
            underflow_node.setChildPageId(@intCast(last_pos + 1), first_child);
            underflow_node.header.node_type = .internal;
            underflow_node.saveHeader();

            const new_separator = getKey(&sibling, 0);
            try node.updateKey(child_idx, new_separator);

            try deleteFirstFromNode(&sibling);
        }

        return true;
    }

    fn insertLeafAtEnd(node: *BTreeNode, key: []const u8, value: []const u8) !void {
        const offset = node.getNextDataOffset();
        const child_area_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1);
        const total = offset + key.len + value.len;
        if (node.header.node_type == .internal or total > child_area_offset) {
            return Error.PageOverflow;
        }
        const key_infos = node.getKeyInfoPtr();
        @memcpy(node.page.data[offset..][0..key.len], key);
        @memcpy(node.page.data[offset + key.len ..][0..value.len], value);
        key_infos[node.header.num_keys] = .{
            .key_offset = @intCast(offset),
            .key_len = @intCast(key.len),
            .value_offset = @intCast(offset + key.len),
            .value_len = @intCast(value.len),
        };
        node.header.num_keys += 1;
        node.saveHeader();
    }

    fn insertLeafAtStart(node: *BTreeNode, key: []const u8, value: []const u8) !void {
        const offset = node.getNextDataOffset();
        const child_area_offset = PAGE_SIZE - @sizeOf(PageId) * (MAX_KEYS + 1);
        if (node.header.node_type != .leaf and node.header.node_type != .internal) {}
        if (offset + key.len + value.len > child_area_offset) {
            return Error.PageOverflow;
        }
        const key_infos = node.getKeyInfoPtr();
        // Shift existing entries right.
        var i: u16 = node.header.num_keys;
        while (i > 0) : (i -= 1) {
            key_infos[i] = key_infos[i - 1];
        }
        @memcpy(node.page.data[offset..][0..key.len], key);
        @memcpy(node.page.data[offset + key.len ..][0..value.len], value);
        key_infos[0] = .{
            .key_offset = @intCast(offset),
            .key_len = @intCast(key.len),
            .value_offset = @intCast(offset + key.len),
            .value_len = @intCast(value.len),
        };
        node.header.num_keys += 1;
        node.saveHeader();
    }

    fn deleteLastFromNode(node: *BTreeNode) !void {
        std.debug.assert(node.header.num_keys > 0);
        node.header.num_keys -= 1;
        node.saveHeader();
    }

    fn deleteFirstFromNode(node: *BTreeNode) !void {
        std.debug.assert(node.header.num_keys > 0);
        const key_infos = node.getKeyInfoPtr();
        var i: u16 = 0;
        while (i < node.header.num_keys - 1) : (i += 1) {
            key_infos[i] = key_infos[i + 1];
        }
        node.header.num_keys -= 1;
        node.saveHeader();
    }

    /// Recursive helper for delete with rebalancing.
    fn deleteRecursive(self: *BTree, pager_ref: *Pager, page_id: PageId, key: []const u8) !void {
        const page = try pager_ref.getPage(page_id);
        var node = BTreeNode.init(page);
        const result = node.findKey(key);

        if (node.header.node_type == .leaf) {
            try node.deleteLeaf(key);
            return;
        }

        // Internal node: recurse into the correct child.
        const child_index = if (result.found) result.index + 1 else result.index;
        const child_page_id = node.getChildPageId(child_index);
        try self.deleteRecursive(pager_ref, child_page_id, key);

        // After return, check if the child underflowed.
        const child_page = try pager_ref.getPage(child_page_id);
        const child = BTreeNode.init(child_page);

        // A node underflows when it has fewer than ceil(MAX_KEYS/2) entries.
        // For simplicity, we check if num_keys < MAX_KEYS / 4 (more aggressive threshold).
        const min_keys: u16 = @intCast(@max(1, MAX_KEYS / 4));

        if (child.header.num_keys < min_keys) {
            // Try to borrow from left sibling first.
            if (try tryBorrowFromLeft(pager_ref, &node, child_index, child_page_id)) {
                try self.normalizeInternalSeparators(&node, pager_ref);
                return;
            }
            // Try to borrow from right sibling.
            if (try tryBorrowFromRight(pager_ref, &node, child_index, child_page_id)) {
                try self.normalizeInternalSeparators(&node, pager_ref);
                return;
            }
            // Must merge. Prefer merging with left sibling if possible.
            if (child_index > 0) {
                try self.mergeChildren(pager_ref, &node, child_index - 1, page_id);
            } else {
                try self.mergeChildren(pager_ref, &node, child_index, page_id);
            }
        }

        try self.normalizeInternalSeparators(&node, pager_ref);
    }

    /// Check if a key exists without allocating a value buffer.
    pub fn containsKey(self: *BTree, pager_ref: *Pager, key: []const u8) !bool {
        var page_id = self.root_page_id;
        while (true) {
            const page = try pager_ref.getPage(page_id);
            var node = BTreeNode.init(page);
            const result = node.findKey(key);

            if (node.header.node_type == .leaf) {
                return result.found;
            }

            const child_index = if (result.found) result.index + 1 else result.index;
            page_id = node.getChildPageId(child_index);
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
    /// Recursively descends to the destination leaf, bubbling split information
    /// back upward when full nodes need to be divided. If the root splits, this
    /// rewrites the original root page as a new internal root.
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
    ///
    /// Only existing keys can be updated. The destination leaf is rebuilt densely
    /// so repeated overwrites do not keep accumulating dead payload bytes.
    pub fn update(self: *BTree, pager_ref: *Pager, key: []const u8, value: []const u8) !void {
        switch (try self.updateRecursive(pager_ref, self.root_page_id, key, value)) {
            .updated => {},
            .not_found => return Error.KeyNotFound,
        }
    }

    /// Delete a key-value pair.
    ///
    /// This follows the staged delete behavior implemented by `deleteRecursive`:
    /// successful deletes refresh parent separators, while deletes that would
    /// empty a non-root leaf currently fail with `Error.NodeEmpty`.
    ///
    /// Parameters:
    ///   - pager_ref: Pager for loading/saving pages
    ///   - key: The key to delete
    pub fn delete(self: *BTree, pager_ref: *Pager, key: []const u8) !void {
        try self.deleteRecursive(pager_ref, self.root_page_id, key);
    }

    /// Verify that reachable B-tree pages remain ordered and structurally consistent.
    ///
    /// This checks page reachability, separator ordering, and the min/max bounds
    /// implied by each internal node's child subtrees.
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
    ///
    /// This summarizes the current tree height and node counts without checking
    /// every ordering invariant that `verify()` enforces.
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

    /// Create an iterator over all entries in the B-tree.
    ///
    /// The iterator yields logical entries in sorted key order across every leaf
    /// currently reachable from the root.
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
