# KVDB - Embedded Key-Value Database in Zig

[![CI](https://github.com/lispking/kvdb/actions/workflows/ci.yml/badge.svg)](https://github.com/lispking/kvdb/actions/workflows/ci.yml)
[![Release](https://github.com/lispking/kvdb/actions/workflows/release.yml/badge.svg)](https://github.com/lispking/kvdb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange.svg)](https://ziglang.org)

A lightweight, high-performance embedded key-value database written in Zig, featuring B-tree indexing, Write-Ahead Logging (WAL) for durability, and ACID transactions.

## Table of Contents

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

## Features

### Core Features

- **B-Tree Indexing**: Efficient O(log n) lookups, insertions, and deletions with balanced tree structure
- **ACID Transactions**: Full atomicity, consistency, isolation, and durability support
- **Write-Ahead Logging (WAL)**: Crash recovery through durable transaction logs with CRC32 checksums
- **Page-Based Storage**: 4KB fixed-size pages optimized for file system alignment
- **In-Memory Page Cache**: Reduces disk I/O through intelligent caching
- **Zero External Dependencies**: Pure Zig implementation with no external libraries required

### Data Capabilities

- **Arbitrary Binary Data**: Store any byte sequence as keys and values
- **Maximum Key Size**: 1KB per key
- **Maximum Value Size**: 2KB per value (half page size)
- **Large Database Support**: 64-bit page addressing supports exabyte-scale storage
- **Iteration Support**: Traverse all key-value pairs in sorted order

### Safety and Reliability

- **CRC32 Checksums**: Detect data corruption in WAL records
- **Magic Number Verification**: Prevent accidental opening of non-database files
- **Automatic Crash Recovery**: Replay WAL on startup to recover uncommitted transactions
- **Comprehensive Error Handling**: Detailed error types for all failure modes

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
|  - Free list        |                              |
+---------------------------------------------------+
```

### B-Tree Structure

The database uses a B-tree index for efficient key-value storage:

- **Node Types**: Leaf nodes store data; internal nodes store keys and child pointers
- **Node Capacity**: Up to 64 key-value pairs per node
- **Sorted Keys**: Keys maintained in sorted order for binary search
- **Space Management**: Key/value data grows upward from end of page

Each B-tree node layout:
```
[Node Header (4 bytes)]
[KeyInfo array (64 * 8 bytes)]
[Key/Value data (grows upward from end)]
```

### Write-Ahead Log (WAL)

The WAL ensures durability and crash recovery:

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

On startup, if uncommitted WAL records exist, they are replayed to restore database state.

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
    .page_size = 4096,        // Page size (default: 4096)
    .enable_wal = true,       // Enable WAL (default: true)
    .enable_compression = false,  // Compression (not yet implemented)
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

**Transaction Rules:**
- Only one active transaction at a time per database
- Changes are invisible until commit
- WAL records are cleared after successful commit
- Abort reloads database to pre-transaction state

### Iteration

```zig
var iter = try db.iterator();
var count: usize = 0;

while (iter.next()) |entry| {
    std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
    count += 1;
}
```

**Note:** Current implementation supports single-page iteration only.

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
    /// Size of each database page (default: 4096)
    page_size: usize = 4096,

    /// Enable Write-Ahead Logging (default: true)
    /// Disabling WAL improves performance but loses durability guarantees
    enable_wal: bool = true,

    /// Enable compression (default: false)
    /// Not yet implemented
    enable_compression: bool = false,
};
```

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
3. **Appropriate Sizing**: Use default page size (4KB) for most workloads
4. **Disable WAL**: For non-critical data, disable WAL for better performance

### Known Limitations

- Single-page B-tree iteration only
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
