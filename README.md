# KVDB - Embedded Key-Value Database in Zig

[![CI](https://github.com/lispking/kvdb/actions/workflows/ci.yml/badge.svg)](https://github.com/lispking/kvdb/actions/workflows/ci.yml)
[![Release](https://github.com/lispking/kvdb/actions/workflows/release.yml/badge.svg)](https://github.com/lispking/kvdb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange.svg)](https://ziglang.org)

An experimental embedded key-value database written in Zig, built around page-based storage, sorted leaf-page indexing, WAL record logging, and a small transaction API.

## Table of Contents

- [Project Status](#project-status)
- [Features](#features)
- [Architecture](#architecture)
- [Download](#download)
- [Quick Start](#quick-start)
- [API Usage](#api-usage)
- [CLI Tool](#cli-tool)
- [Building and Testing](#building-and-testing)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Performance Considerations](#performance-considerations)
- [License](#license)

## Project Status

KVDB is currently best viewed as an early-stage embedded storage engine.

What works today:

- persistent page-based storage with CRUD operations
- multi-page B-tree insert/search/iteration support
- WAL record writing with CRC32 checksums
- explicit transaction APIs plus compaction and CLI tooling
- basic automated tests and CI

Important current limitations:

- multi-page deletes work only while each touched non-root leaf keeps at least one entry; borrow/merge/root-shrink are not implemented yet
- automatic startup recovery replays committed WAL records during `Database.open`
- rollback is implemented by reloading the database file, not by targeted undo
- compression is not implemented

## Features

### Implemented Today

- **Page-Based Storage**: 4KB fixed-size pages optimized for simple file I/O
- **Sorted Leaf-Page Indexing**: Keys are kept sorted inside the current root leaf page and searched with binary search
- **Basic Transaction API**: One active transaction per database with explicit `commit()` / `abort()` methods
- **WAL Record Logging**: Insert/delete/commit/abort records include CRC32 checksums
- **In-Memory Page Cache**: Reduces repeated disk reads for loaded pages
- **Database Compaction**: Rebuilds the database into a fresh file, validates logical contents, then atomically replaces the original
- **Zero External Dependencies**: Pure Zig implementation with no external libraries required

### Data Limits

- **Arbitrary Binary Data**: Store any byte sequence as keys and values
- **Maximum Key Size**: 1KB per key
- **Maximum Value Size**: 2KB per value (half page size)
- **64-bit Page Identifiers**: On-disk structures use 64-bit page IDs

### Current Constraints

- **Multi-Page Inserts**: Root and internal-node split propagation now allow the tree to grow beyond one page
- **Multi-Page Deletes**: Deletes work while each touched non-root leaf keeps at least one entry; deletes that would require borrow/merge/root-shrink currently return `NodeEmpty`
- **Leaf Update Repacking**: Repeated overwrites now repack the target leaf so updates do not keep leaking dead payload bytes
- **Recovery Wiring**: `Database.open` replays committed WAL records and clears the WAL after successful recovery
- **Simplified Rollback**: `abort()` reloads the database instead of doing targeted undo

## Architecture

### Storage Architecture

```
+---------------------------------------------------+
|                  Database File                     |
+---------------------------------------------------+
|  Page 0 (Metadata)  |  Page 1+ (B-tree Pages)      |
|  - Magic number     |  - Node headers              |
|  - Version          |  - Key/value data            |
|  - Root page ID     |  - Child page references     |
|  - Reserved fields  |                              |
+---------------------------------------------------+
```

The metadata format now tracks both the freelist head and the highest page ID grown from the file, so freed pages can be reused before the database extends on disk.

### B-Tree Structure

The on-disk page format is now wired for multi-page growth during insert, while delete remains intentionally staged until rebalancing lands.

- **Leaf Nodes**: Store key-value pairs directly
- **Internal Nodes**: Multi-page insert/search/iteration now route through separator keys and child pointers
- **Current Root Capacity**: The original root still starts as a leaf page, then promotes in place to an internal root on overflow
- **Sorted Keys**: Keys are maintained in sorted order for binary search inside each node
- **Space Management**: Key/value data grows upward from the end of the page
- **Delete Limitation**: Deletes that would empty a non-root leaf are rejected with `NodeEmpty` until borrow/merge/root-shrink support is implemented

Each B-tree node layout:
```
[Node Header (4 bytes)]
[KeyInfo array (64 * 8 bytes)]
[Key/Value data (grows upward from end)]
```

### Write-Ahead Log (WAL)

The WAL currently records write operations and stores enough information for validation and future recovery work:

```
[WAL Record Header (11 bytes)]
  - checksum: u32      (CRC32 of record data)
  - record_type: u8    (insert/delete/commit/abort)
  - key_len: u16       (length of key)
  - value_len: u32     (length of value)
[Key data (variable)]
[Value data (variable, optional)]
```

**Record Types:**
- `insert`: Log a key-value insertion
- `delete`: Log a key deletion
- `commit`: Mark transaction as committed
- `abort`: Mark transaction as rolled back

Automatic startup recovery now replays committed WAL records during `Database.open`; uncommitted and aborted WAL entries are discarded. A truncated tail header is ignored as an incomplete final record, while checksum mismatches and partial record payloads fail recovery.

### Page Cache

The pager maintains an in-memory cache of recently accessed pages:

- Pages are loaded from disk on first access
- Modified pages marked as "dirty" and flushed on commit
- Cache reduces disk I/O for frequently accessed data

## Download

Pre-compiled binaries are available on the [Releases](https://github.com/lispking/kvdb/releases) page.

### Pre-built Binaries

| Platform | Architecture | Download |
|----------|--------------|----------|
| Linux | x86_64 | [kvdb-latest-x86_64-linux.tar.gz](https://github.com/lispking/kvdb/releases/latest) |
| Linux | ARM64 | [kvdb-latest-aarch64-linux.tar.gz](https://github.com/lispking/kvdb/releases/latest) |
| macOS | x86_64 | [kvdb-latest-x86_64-macos.tar.gz](https://github.com/lispking/kvdb/releases/latest) |
| macOS | Apple Silicon | [kvdb-latest-aarch64-macos.tar.gz](https://github.com/lispking/kvdb/releases/latest) |
| Windows | x86_64 | [kvdb-latest-x86_64-windows.zip](https://github.com/lispking/kvdb/releases/latest) |

### Quick Install

**Linux/macOS:**
```bash
# Download latest release
curl -LO https://github.com/lispking/kvdb/releases/latest/download/kvdb-latest-x86_64-linux.tar.gz

# Extract
tar xzvf kvdb-latest-x86_64-linux.tar.gz
cd kvdb-latest-x86_64-linux

# Run
./kvdb-cli --help
```

**Windows (PowerShell):**
```powershell
# Download latest release
Invoke-WebRequest -Uri "https://github.com/lispking/kvdb/releases/latest/download/kvdb-latest-x86_64-windows.zip" -OutFile "kvdb.zip"

# Extract
Expand-Archive -Path "kvdb.zip" -DestinationPath "."
cd kvdb-latest-x86_64-windows

# Run
.\kvdb-cli.exe --help
```

## Quick Start

### Prerequisites

If building from source:
- Zig 0.15.2 or later
- Unix-like system (macOS, Linux) or Windows

### Installation

```bash
# Clone the repository
git clone https://github.com/lispking/kvdb.git
cd kvdb

# Install git hooks (optional, for development)
zig build install-hooks

# Build the library and CLI tool
zig build
```

### Basic Usage

```zig
const std = @import("std");
const kvdb = @import("kvdb");

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open database (creates if not exists)
    var db = try kvdb.open(allocator, "mydb.db");
    defer db.close();

    // Store data
    try db.put("name", "Alice");
    try db.put("age", "30");

    // Retrieve data
    if (try db.get("name")) |value| {
        defer allocator.free(value);
        std.debug.print("name = {s}\n", .{value});
    }

    // Delete data
    try db.delete("age");
}
```

## API Usage

### Opening and Closing

```zig
// Open with default options
var db = try kvdb.open(allocator, "path/to/db");
defer db.close();

// Open with custom options
var db = try Database.open(allocator, "path/to/db", .{
    .enable_wal = true,
    .fsync_policy = .always,
});
```

### Basic Operations

```zig
// Insert or update a key-value pair
try db.put("key", "value");

// Retrieve a value (returns null if not found)
if (try db.get("key")) |value| {
    defer allocator.free(value);  // Caller must free
    // use value...
}

// Check if key exists
const exists = try db.contains("key");

// Delete a key
try db.delete("key");
```

**Current Write Behavior:**
- `put()` and `delete()` do not require `beginTransaction()`
- writes update the current database handle immediately in memory
- durability happens later when dirty pages are flushed, such as during `commit()` or `close()`
- reads through the same handle observe the updated in-memory state immediately

### Transactions

```zig
// Begin transaction
const txn = try db.beginTransaction();

try {
    // Perform operations
    try db.put("key1", "value1");
    try db.put("key2", "value2");
    
    // Commit changes
    try txn.commit();
} catch |_| {
    // Abort on error
    try txn.abort();
}
```

**Current Transaction Semantics:**
- Only one active transaction at a time per database handle
- `beginTransaction()` does not create snapshots or isolation; it only establishes an explicit commit/abort boundary for the current handle
- `put()` and `delete()` still modify the current handle's in-memory pages immediately
- `commit()` appends a commit marker, flushes the handle's dirty pages, and clears the WAL
- `abort()` appends an abort marker, clears the WAL, and reloads the database file, discarding the handle's unflushed in-memory changes
- startup recovery treats WAL records as logical batches: `insert` / `delete` records stay pending until `commit`, `abort` discards the pending batch, and trailing incomplete batches are ignored
- automatic startup recovery replays committed WAL records during `Database.open`
- a truncated final WAL header is treated as an incomplete tail, while checksum mismatches and partial record payloads fail recovery
- multi-page deletes succeed only when no touched non-root leaf becomes empty; deletes that would require borrow/merge/root-shrink currently fail with `NodeEmpty`

**Recommendation:** For predictable behavior in the current engine, group related writes inside an explicit transaction and avoid mixing earlier unflushed writes with a later `abort()`.

### Iteration

```zig
var iter = try db.iterator();
var count: usize = 0;

while (iter.next()) |entry| {
    std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
    count += 1;
}
```

**Note:** Current implementation iterates in sorted order across all reachable leaf pages.

### Statistics

```zig
const stats = db.stats();
std.debug.print("Pages: {d}\n", .{stats.page_count});
std.debug.print("Page size: {d} bytes\n", .{stats.page_size});
std.debug.print("Total size: {d} bytes\n", .{stats.db_size});
```

## CLI Tool

The `kvdb-cli` tool provides command-line access to the database.

### Usage

```bash
kvdb-cli <database-file> <command> [args...]
```

### Commands

| Command | Description | Example |
|---------|-------------|---------|
| `get <key>` | Get value by key | `kvdb-cli my.db get name` |
| `put <key> <value>` | Set key-value pair | `kvdb-cli my.db put name Bob` |
| `delete <key>` | Delete a key | `kvdb-cli my.db delete name` |
| `list` | List all entries | `kvdb-cli my.db list` |
| `stats` | Show database statistics | `kvdb-cli my.db stats` |
| `inspect` | Show metadata and structural tree summary | `kvdb-cli my.db inspect` |
| `export <file>` | Stream all logical entries into a portable binary dump | `kvdb-cli my.db export backup.kvdbx` |
| `import <file>` | Load entries from a portable binary dump in one transaction | `kvdb-cli my.db import backup.kvdbx` |
| `compact` | Rewrite live data into a compacted database file | `kvdb-cli my.db compact` |
| `verify` | Validate metadata, tree ordering, freelist, and WAL records | `kvdb-cli my.db verify` |

### Examples

```bash
# Create database and insert data
kvdb-cli my.db put language Zig
kvdb-cli my.db put version "0.15.0"

# Query data
kvdb-cli my.db get language
# Output: Zig

# List all entries
kvdb-cli my.db list
# Output:
# language = Zig
# version = 0.15.0
#
# Total: 2 entries

# Check statistics
kvdb-cli my.db stats
# Output:
# Database Statistics:
#   Pages: 2
#   Page Size: 4096 bytes
#   Database Size: 8192 bytes (0.01 MB)

# Verify on-disk structure
kvdb-cli my.db verify
# Output:
# Verification OK
#   Tree pages checked: 1
#   Entries checked: 2
#   WAL records checked: 0

# Inspect metadata and tree shape
kvdb-cli my.db inspect
# Output:
# Database
#   Pages: 2
#   Page Size: 4096 bytes
#   Database Size: 8192 bytes (0.01 MB)
# Metadata
#   Root Page ID: 1
#   Freelist Head: 18446744073709551615
#   Freelist Pages: 0
#   Last Page ID: 1
# B-Tree
#   Height: 1
#   Nodes: 1
#   Leaf Nodes: 1
#   Internal Nodes: 0
#   Entries: 2

# Export logical contents to a binary-safe dump file
kvdb-cli my.db export backup.kvdbx
# Output:
# Exported 2 entries

# Import the dump into a fresh database
kvdb-cli restored.db import backup.kvdbx
# Output:
# Imported 2 entries
```

## Building and Testing

### Build Commands

```bash
# Build library and CLI tool
zig build

# Build in release mode
zig build -Doptimize=ReleaseFast

# Build specific target
zig build -Dtarget=x86_64-linux-gnu
```

### Testing

```bash
# Run all tests
zig build test

# Run with verbose output
zig test src/kvdb.zig
```

### Benchmark Suite

```bash
# Run the benchmark suite
zig build bench

# Build the benchmark binary without running it
zig build
./zig-out/bin/benchmark
```

The current benchmark binary reports these deterministic workloads under both `fsync_policy = .always` and `fsync_policy = .batch`:
- sequential inserts
- random inserts
- point lookups
- scans
- updates
- deletes
- compaction

Each run uses fixed operation counts and a fixed PRNG seed so results are comparable across changes. Comparing the paired policy rows shows the durability/latency tradeoff without changing workload shape.

### FFI Example

A minimal C example lives at `examples/c_api_example.c` and uses the exported C API directly.
A minimal Python `ctypes` example lives at `examples/python_ctypes_example.py`.

```bash
# Build the static library
zig build

# Compile the C example against the installed archive
zig cc examples/c_api_example.c zig-out/lib/libkvdb.a -o c_api_example

# Run it
./c_api_example

# Run the Python ctypes example
python3 examples/python_ctypes_example.py
```

The example calls `kvdb_open`, `kvdb_put`, `kvdb_get`, `kvdb_free`, `kvdb_close`, and `kvdb_status_code`.
When `kvdb_get` returns a non-null pointer, the caller owns that buffer and must
release it with `kvdb_free`.

Mutating C API calls return stable numeric status codes instead of a generic `-1`:
- `KVDB_STATUS_OK` = 0
- `KVDB_STATUS_INVALID_ARGUMENT` = 1
- `KVDB_STATUS_NOT_FOUND` = 2
- `KVDB_STATUS_TRANSACTION_CONFLICT` = 3
- `KVDB_STATUS_STORAGE_ERROR` = 4
- `KVDB_STATUS_WAL_ERROR` = 5
- `KVDB_STATUS_INTERNAL_ERROR` = 255

Use `kvdb_status_code(...)` from C to compare against the exported enum values without hard-coding numbers in callers.

### Storage Test Matrix

| Area | Core invariants | Current coverage | Missing high-risk coverage |
|------|------------------|------------------|----------------------------|
| `constants.zig` | Metadata layout stays stable and fits within one page | `constants: metadata layout` | Format-compatibility checks beyond struct size |
| `pager.zig` | Page allocation, freelist reuse, and persisted page count survive reopen | `pager: basic operations` | Dirty-page edge cases, invalid page handling, deeper reclaim integration |
| `btree.zig` | Root leaf inserts, lookups, deletes, and sorted order remain correct | `btree: basic operations` | Duplicate update/delete flows, `NodeFull`, key/value size boundaries, multi-page search |
| `wal.zig` | WAL records append and decode correctly with record-type boundaries | `wal: basic operations`, `wal: checksum mismatch is corruption`, `wal: truncated value payload is corruption` | Invalid record type handling, broader replay-path coverage |
| `kvdb.zig` | End-to-end CRUD, reopen persistence, compaction validation, and explicit commit path stay correct | `kvdb: basic operations`, `kvdb: transaction commit`, `kvdb: replay committed wal on open`, `kvdb: ignore uncommitted wal on open`, `kvdb: ignore aborted wal on open`, `kvdb: replay delete on open`, `kvdb: replay mixed wal batches on open`, `kvdb: replay is idempotent across reopen`, `kvdb: ignore truncated wal tail on open`, `kvdb: checksum corruption fails recovery on open`, `kvdb: compact preserves live key-value pairs` | Abort reload behavior, broader compaction correctness, WAL-disabled mode |

### Storage Test Checklist

Use this checklist when changing storage code:

- verify restart persistence for any path that changes on-disk state
- cover duplicate insert/update/delete flows when touching write logic
- cover key/value boundary sizes and invalid arguments when touching validation
- cover `NodeFull` behavior when touching B-tree insertion or page layout
- cover checksum failure and truncated-tail handling when touching WAL parsing or recovery
- compare live data before/after compaction or reopen when touching file replacement, freelist reuse, or reload paths
- name tests with a subsystem prefix such as `test "kvdb: ..."` or `test "wal: ..."`

### Development Setup

```bash
# Install pre-commit hooks
zig build install-hooks

# Format code
zig fmt src/ examples/

# Check formatting (read-only)
zig fmt --check src/ examples/
```

### Pre-commit Hook

The pre-commit hook automatically:
1. Checks code formatting
2. Builds the project
3. Runs all tests

If any check fails, the commit is blocked. To bypass (not recommended):
```bash
git commit --no-verify -m "message"
```

### Creating Releases

To create a new release with pre-built binaries:

```bash
# Tag a new version
git tag -a v1.0.0 -m "Release version 1.0.0"

# Push the tag (triggers release workflow)
git push origin v1.0.0
```

The [Release workflow](.github/workflows/release.yml) will automatically:
1. Build binaries for all supported platforms (Linux x86_64/ARM64, macOS x86_64/Apple Silicon, Windows x86_64)
2. Create a GitHub Release with downloadable archives
3. Generate release notes with installation instructions

**Release Artifacts:**
| File | Description |
|------|-------------|
| `kvdb-vX.Y.Z-x86_64-linux.tar.gz` | Linux x86_64 binary |
| `kvdb-vX.Y.Z-aarch64-linux.tar.gz` | Linux ARM64 binary |
| `kvdb-vX.Y.Z-x86_64-macos.tar.gz` | macOS Intel binary |
| `kvdb-vX.Y.Z-aarch64-macos.tar.gz` | macOS Apple Silicon binary |
| `kvdb-vX.Y.Z-x86_64-windows.zip` | Windows x86_64 binary |

## Project Structure

```
kvdb/
├── build.zig              # Build configuration
├── build.zig.zon          # Package manifest
├── README.md              # This file
├── .gitignore             # Git ignore patterns
├── scripts/
│   └── pre-commit         # Git pre-commit hook
├── src/
│   ├── kvdb.zig           # Main database API (public interface)
│   ├── constants.zig      # Error codes, constants, data structures
│   ├── pager.zig          # Page management and I/O
│   ├── btree.zig          # B-tree index implementation
│   ├── wal.zig            # Write-ahead logging
│   └── cli.zig            # Command-line interface
└── examples/
    └── basic.zig          # Usage example
```

### Module Descriptions

| Module | Description | Key Types |
|--------|-------------|-----------|
| `kvdb.zig` | Public API and database management | `Database`, `Transaction`, `Options` |
| `constants.zig` | Constants and error definitions | `Error`, `PageId`, `MetaData`, `WalRecordType` |
| `pager.zig` | Page-level storage operations | `Pager`, `Page`, `CacheEntry` |
| `btree.zig` | B-tree index operations | `BTree`, `BTreeNode`, `Iterator` |
| `wal.zig` | Write-ahead logging for durability | `Wal`, `Record`, `Iterator` |
| `cli.zig` | Command-line tool | CLI argument parsing and commands |

## Configuration

### Database Options

```zig
pub const Options = struct {
    /// Enable Write-Ahead Logging record writing and startup recovery.
    enable_wal: bool = true,
    /// Choose whether commit/checkpoint paths force data to stable storage.
    fsync_policy: FsyncPolicy = .always,
};
```

`enable_wal` controls whether KVDB writes WAL records and performs startup replay at all.

`fsync_policy` controls how aggressively KVDB asks the OS to persist durability boundaries:
- `.always`: call `file.sync()` at WAL commit/checkpoint and page-flush boundaries for the strongest current durability behavior
- `.batch`: keep the same write ordering but skip explicit sync calls, which can improve throughput while allowing recent writes to remain in OS buffers after a crash or power loss

**Note:** WAL changes replay semantics only when enabled; `fsync_policy` changes durability cost, not logical recovery rules.

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `PAGE_SIZE` | 4096 | Size of each database page |
| `MAX_KEY_SIZE` | 1024 | Maximum key size in bytes |
| `MAX_VALUE_SIZE` | 2048 | Maximum value size in bytes |
| `MAX_KEYS` | 64 | Maximum entries per B-tree node |
| `DB_VERSION` | 1 | Database format version |
| `MAGIC` | 0x4B5644425F5A4947 | File format identifier ("KVDB_ZIG") |

## Performance Considerations

### Optimization Tips

1. **Batch Operations**: Group multiple puts in a single transaction
2. **Reuse Connections**: Keep database open for multiple operations
3. **Page Size**: The current engine uses fixed 4KB pages internally
4. **Disable WAL**: Only disable WAL if you intentionally want to skip WAL record logging
5. **Tune fsync policy**: Use `.always` for the strongest current durability behavior and `.batch` when benchmark throughput matters more than surviving the most recent buffered writes

### Known Limitations

- Inserts and deletes currently operate on a single root leaf page
- Iteration is limited to the current root page
- Automatic WAL replay runs during `Database.open`
- Rollback is implemented via full reload, not targeted undo
- No compression support yet
- No encryption support
- Single-writer model (no concurrent transactions)

## Error Handling

The database uses a comprehensive error set:

```zig
pub const Error = error{
    // Storage errors
    DiskFull, CorruptedData, InvalidPageId, PageNotFound, PageOverflow,
    // Transaction errors  
    TransactionAlreadyActive, NoActiveTransaction, TransactionConflict,
    // B-tree errors
    KeyNotFound, KeyAlreadyExists, NodeFull, NodeEmpty,
    // WAL errors
    WalCorrupted, WalReplayFailed,
    // I/O errors
    IoError, InvalidArgument, DatabaseClosed,
};
```

All operations return `Error!T` and should be handled with `try` or `catch`.

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please ensure:

1. Code follows Zig style guidelines (`zig fmt`)
2. All tests pass (`zig build test`)
3. New features include tests
4. Documentation is updated

## Acknowledgments

- Inspired by SQLite's architecture and B-tree implementation
- Built with Zig 0.15.2
- Thanks to the Zig community for feedback and support
