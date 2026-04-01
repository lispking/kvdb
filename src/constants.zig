const std = @import("std");

/// Error set for all database operations.
/// These errors cover storage issues, transaction problems, B-tree operations,
/// WAL (Write-Ahead Log) failures, and general I/O errors.
pub const Error = error{
    // Storage errors

    /// Disk has no space left for new data
    DiskFull,
    /// Data on disk is corrupted or invalid
    CorruptedData,
    /// Page ID is out of valid range
    InvalidPageId,
    /// Requested page does not exist in the database
    PageNotFound,
    /// Data exceeds page capacity
    PageOverflow,

    // Transaction errors

    /// Cannot start a new transaction while one is active
    TransactionAlreadyActive,
    /// Operation requires an active transaction
    NoActiveTransaction,
    /// Concurrent transaction conflict detected
    TransactionConflict,

    // B-tree errors

    /// The requested key does not exist in the database
    KeyNotFound,
    /// Cannot insert key that already exists
    KeyAlreadyExists,
    /// B-tree node has reached maximum capacity
    NodeFull,
    /// B-tree node is unexpectedly empty
    NodeEmpty,

    // WAL (Write-Ahead Log) errors

    /// WAL file is corrupted
    WalCorrupted,
    /// Failed to replay WAL during recovery
    WalReplayFailed,

    // I/O errors

    /// General I/O operation failure
    IoError,

    // Other errors

    /// Function called with invalid parameters
    InvalidArgument,
    /// Operation attempted on closed database
    DatabaseClosed,
};

/// Size of a database page in bytes.
/// Uses 4KB pages which is standard for most file systems and provides
/// good balance between I/O efficiency and memory usage.
pub const PAGE_SIZE: usize = 4096;

/// Type for page identifiers.
/// Using u64 allows for extremely large databases (2^64 pages).
pub const PageId = u64;

/// Sentinel value representing an invalid or null page ID.
/// Used to indicate the absence of a page reference.
pub const INVALID_PAGE_ID: PageId = std.math.maxInt(PageId);

/// Page ID for the metadata page.
/// Page 0 always contains the database metadata header.
pub const META_PAGE_ID: PageId = 0;

/// Initial page ID for the B-tree root node.
/// Page 1 is reserved for the root of the B-tree index.
pub const ROOT_PAGE_ID: PageId = 1;

/// Maximum allowed size for a key in bytes.
/// Limited to 1KB to ensure reasonable memory usage and performance.
pub const MAX_KEY_SIZE: usize = 1024;

/// Maximum allowed size for a value in bytes.
/// Limited to half a page to leave room for B-tree node splitting.
pub const MAX_VALUE_SIZE: usize = PAGE_SIZE / 2;

/// Database file format version number.
/// Incremented when making breaking changes to the on-disk format.
pub const DB_VERSION: u32 = 1;

/// Magic number for identifying KVDB database files.
/// ASCII "KVDB_ZIG" encoded as a u64 for quick file format verification.
pub const MAGIC: u64 = 0x4B5644425F5A4947;

/// Database metadata structure stored in page 0.
/// Uses extern struct for predictable C-compatible memory layout.
/// This structure contains essential database state information
/// that must be persisted across sessions.
pub const MetaData = extern struct {
    /// Magic number for file format verification
    magic: u64,
    /// Database format version
    version: u32,
    /// Size of each page (should match PAGE_SIZE)
    page_size: u32,
    /// Page ID of B-tree root node
    root_page: PageId,
    /// Page ID of free page list head (INVALID_PAGE_ID if none)
    freelist_page: PageId,
    /// Highest allocated page ID
    last_page_id: PageId,
    /// Current write offset in WAL file
    wal_offset: u64,

    /// Initialize a new metadata structure with default values.
    /// Returns a properly configured MetaData for a new database.
    pub fn init() MetaData {
        return .{
            .magic = MAGIC,
            .version = DB_VERSION,
            .page_size = PAGE_SIZE,
            .root_page = ROOT_PAGE_ID,
            .freelist_page = INVALID_PAGE_ID,
            .last_page_id = ROOT_PAGE_ID,
            .wal_offset = 0,
        };
    }

    /// Validate that this metadata structure is correct.
    /// Checks magic number and version for file format compatibility.
    /// Returns true if the metadata is valid and the database can be opened.
    pub fn isValid(self: MetaData) bool {
        return self.magic == MAGIC and self.version == DB_VERSION;
    }
};

/// Types of records stored in the Write-Ahead Log (WAL).
/// Each transaction operation is logged with one of these types.
pub const WalRecordType = enum(u8) {
    /// Insert a new key-value pair
    insert = 1,
    /// Delete an existing key
    delete = 2,
    /// Mark transaction as committed
    commit = 3,
    /// Mark transaction as aborted (rolled back)
    abort = 4,
};

/// Header structure for each WAL record.
/// Stored as packed struct for compact binary representation.
pub const WalRecordHeader = packed struct {
    /// CRC32 checksum of record data (excluding this field)
    checksum: u32,
    /// Type of operation being logged
    record_type: WalRecordType,
    /// Length of key data following header
    key_len: u16,
    /// Length of value data following key (0 for deletes)
    value_len: u32,
};

/// Types of nodes in the B-tree index.
pub const NodeType = enum(u8) {
    /// Leaf node containing actual key-value pairs
    leaf = 1,
    /// Internal node containing keys and child page references
    internal = 2,
};

/// Header structure for B-tree nodes stored in pages.
/// Each B-tree page begins with this header followed by key-value data.
pub const NodeHeader = packed struct {
    /// Whether this is a leaf or internal node
    node_type: NodeType,
    /// Number of key-value pairs currently stored
    num_keys: u16,
    /// Padding for alignment (reserved for future use)
    _reserved: u16 = 0,
};

// =============================================================================
// Tests
// =============================================================================

test "constants" {
    // Verify page size matches standard expectation
    try std.testing.expectEqual(4096, PAGE_SIZE);

    // Verify metadata structure size is reasonable
    // (should be small enough to fit in a page with room for other data)
    try std.testing.expectEqual(48, @sizeOf(MetaData));

    // Ensure metadata can fit within a single page
    try std.testing.expect(@sizeOf(MetaData) <= PAGE_SIZE);
}
