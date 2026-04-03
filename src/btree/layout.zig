const constants = @import("../constants.zig");

/// Shared on-page layout definitions for B-tree nodes.
///
/// This module centralizes the packed metadata and byte offsets used by both
/// node-local page operations and higher-level tree logic.
pub const NodeHeader = constants.NodeHeader;
/// Fixed database page size used when laying out node contents.
pub const PAGE_SIZE = constants.PAGE_SIZE;

/// Information about a single key-value pair stored in a B-tree node.
/// Each entry occupies 8 bytes (packed struct).
pub const KeyInfo = packed struct {
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
pub const HEADER_SIZE = @sizeOf(NodeHeader);

/// Size of each KeyInfo entry in bytes
pub const KEY_INFO_SIZE = @sizeOf(KeyInfo);

/// Maximum number of key-value pairs per node.
/// Limited by page layout: each entry needs 8 bytes KeyInfo plus key+value data.
/// For 4KB pages with ~40-byte entries, 64 provides good fanout while leaving room.
pub const MAX_KEYS: u16 = 64;

/// Offset where key/value data starts (after header and all KeyInfo slots)
pub const DATA_START_OFFSET = HEADER_SIZE + KEY_INFO_SIZE * MAX_KEYS;
