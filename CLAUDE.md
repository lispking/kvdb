# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KVDB is a toy key-value database written in **Zig** (not Rust). It features page-based storage with a B-tree index, WAL-based crash recovery, transaction support (commit/abort), C FFI bindings, and a CLI tool.

## Build & Test Commands

```bash
zig build                          # Build all artifacts
zig build -Doptimize=ReleaseFast   # Release build
zig build test                     # Run all tests
zig build bench                    # Run benchmark suite
zig fmt --check                    # Check formatting
zig build -Dtarget=<triple>        # Cross-compile (e.g. x86_64-linux)
```

Pre-commit hook (`scripts/pre-commit`): runs `zig fmt --check` → `zig build` → `zig build test` in sequence.

## Architecture

### Module Structure

```
src/kvdb.zig (public entry point + C FFI exports)
├── constants.zig         — Error codes, PAGE_SIZE (4096), PageId, MetaData, WalRecordType, NodeHeader
├── pager.zig (facade)
│   ├── page.zig          — Page struct (4KB data, dirty flag)
│   ├── io.zig            — Pager: page I/O, cache, allocation, flush
│   ├── cache.zig         — Cache operations (find, create, prune, evict)
│   ├── metadata.zig      — Metadata page read/write
│   ├── freelist.zig      — Freelist allocation/free/verify
│   └── types.zig         — CacheEntry, CacheIndex, FreePageHeader, PAGE_CACHE_LIMIT=32
├── btree.zig (facade)
│   ├── layout.zig        — KeyInfo packed struct, HEADER_SIZE, MAX_KEYS=64, constants
│   ├── node.zig          — BTreeNode: leaf/internal operations
│   ├── tree.zig          — BTree: recursive insert/search/delete, verify, inspect
│   └── iterator.zig      — Stack-based multi-page iterator
├── wal.zig (facade)
│   ├── core.zig          — Wal: append, logInsert, logDelete, logCommit, logAbort, replay
│   └── iterator.zig      — WAL record iterator with CRC32 validation
└── kvdb/
    ├── types.zig          — KVDB_Status (C ABI enum), FsyncPolicy, Options
    ├── database.zig       — Database: open, get, put, delete, contains, iterator, reload
    ├── transaction.zig    — Transaction: commit (flush + clear WAL), abort (reload from disk)
    ├── recovery.zig       — WAL recovery on open (buffer ops, apply on commit)
    ├── maintenance.zig    — verify(), inspect(), stats(), compact()
    ├── transfer.zig       — exportToWriter(), importFromReader() (KVDBX1 binary dump format)
    └── ffi.zig            — C FFI: kvdb_open, kvdb_close, kvdb_get, kvdb_put, kvdb_delete, etc.
```

### Key Design Decisions

- **Page-based storage**: Fixed 4KB pages. Page 0 = metadata, Pages 1+ = B-tree nodes.
- **B-tree**: Multi-page with recursive insert (split propagation, root promotion). Max 64 keys per node. Keys sorted within nodes, binary search. No rebalancing on delete yet.
- **WAL**: File-based with CRC32 checksums. Record types: insert, delete, commit, abort. Recovery buffers operations until commit boundary.
- **Transactions**: One active transaction per database. Abort reloads database from disk.
- **Export/Import**: Binary dump format (`KVDBX1` magic, version 1) with length-prefixed records.

### Tests

Colocated in `*_tests.zig` files within each submodule, all imported from the corresponding facade. Running `zig build test` executes everything through `src/kvdb.zig` as the test root. The randomized test uses 4 stable seeds (`0xA11CE`, `0xBEEF`, `0xC0FFEE`, `0xDEADBEEF`) with a `ReferenceModel` for lockstep comparison.

## CI/CD

- **CI** (`.github/workflows/ci.yml`): Builds and tests on Ubuntu/macOS/Windows with Zig 0.15.2. Includes lint and WAL recovery test jobs.
- **Release** (`.github/workflows/release.yml`): Multi-platform builds on `v*` tag push for 5 targets.

## Known Limitations

- B-tree delete does not rebalance nodes (no underflow handling).
- One transaction at a time (no concurrent transactions).
- No range query API exposed at the top level (iterator exists internally).
