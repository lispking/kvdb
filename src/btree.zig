/// Public facade for the split B-tree implementation.
///
/// This file keeps the stable import surface while the actual implementation
/// lives under `src/btree/`.
const layout = @import("btree/layout.zig");
const node = @import("btree/node.zig");
const tree = @import("btree/tree.zig");
const _tests = @import("btree/tests.zig");

/// Packed metadata describing one key/value entry stored inside a node page.
pub const KeyInfo = layout.KeyInfo;
/// Size in bytes of the on-page B-tree node header.
pub const HEADER_SIZE = layout.HEADER_SIZE;
/// Size in bytes of one `KeyInfo` slot.
pub const KEY_INFO_SIZE = layout.KEY_INFO_SIZE;
/// Maximum number of logical keys stored in one node page.
pub const MAX_KEYS = layout.MAX_KEYS;
/// First byte offset where variable-length key/value payload data begins.
pub const DATA_START_OFFSET = layout.DATA_START_OFFSET;
/// Low-level page-backed node operations used by the tree implementation.
pub const BTreeNode = node.BTreeNode;
/// Recursive multi-page B-tree API used by the database layer.
pub const BTree = tree.BTree;
/// Convenience re-export of the sorted B-tree iterator type.
pub const Iterator = tree.BTree.Iterator;
