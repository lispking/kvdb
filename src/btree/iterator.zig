const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");
const node_mod = @import("node.zig");

const Pager = pager.Pager;
const Page = pager.Page;
const BTreeNode = node_mod.BTreeNode;

/// Sorted iterator over logical B-tree entries.
///
/// The iterator keeps its own traversal stack and scratch buffers so callers
/// can walk every reachable leaf without requiring leaf-link pointers.
pub const Iterator = struct {
    /// Saved traversal state for one internal node on the descent stack.
    const StackFrame = struct {
        /// Internal page whose remaining children still need visiting.
        page: *Page,
        /// Next child index to descend into after the current subtree finishes.
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

    /// Descend from the given page to the leftmost reachable leaf.
    ///
    /// While descending, internal nodes are pushed onto the stack so later
    /// iteration can resume from the next child after each subtree finishes.
    pub fn descendToLeaf(self: *Iterator, start_page: *Page) !void {
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

    /// Advance from the exhausted current leaf to the next leaf in sorted order.
    pub fn advanceToNextLeaf(self: *Iterator) !void {
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
    /// Returned slices point into iterator-owned buffers and are overwritten by
    /// the next call to `next()`.
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
