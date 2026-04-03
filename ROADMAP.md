# KVDB Roadmap: From Toy to World-Class Storage Engine

## Vision

Build the fastest embeddable key-value store in Zig — a production-grade alternative to RocksDB that leverages Zig's zero-cost abstractions, compile-time metaprogramming, and manual memory management for maximum performance and transparency.

## North Star Metrics

| Metric | Current | v0.2 | v0.3 | v0.5 | v1.0 (RocksDB-competitive) |
|--------|---------|------|------|------|---------------------------|
| sequential insert ops/sec | ~220K | 500K | 1M | 2M+ | 5M+ |
| point lookup ops/sec | ~64K | 200K | 500K | 1M+ | 3M+ |
| update ops/sec | ~900 | 50K | 200K | 500K+ | 2M+ |
| Max value size | 2KB | 64KB | 1MB | unlimited | unlimited |
| Max key size | 1KB | 4KB | 8KB | 64KB | 64KB |
| Keys per node | 64 | 128 | 256 | 512 | configurable |
| Page cache limit | 32 | 256 | 1024 | configurable | configurable |
| Concurrency | single-thread | read lock | MVCC | multi-writer | full MVCC |
| Compression | none | optional | LZ4/Zstd | LZ4/Zstd/Snappy | all + custom |
| Bloom filters | none | basic | block-level | partitioned | partitioned + full |
| Encryption | none | — | — | optional AES-GCM | full at-rest |
| Test coverage | ~50 | 80% | 90% | 95% | 95%+ |

## Phase 0: Solidify Foundations (Current — ✅ Done)

- [x] Page-based storage with metadata and freelist
- [x] Multi-page B-tree with insert/search/split propagation
- [x] WAL with CRC32 checksums and crash recovery
- [x] Transaction API (commit/abort)
- [x] C FFI bindings
- [x] CLI tool with export/import/compact/verify
- [x] CI/CD across Linux/macOS/Windows
- [x] Pre-commit hook (fmt + build + test)
- [x] Performance pass #1: eliminate hot-path allocations, pread, stack-buffer WAL, in-place updates

## Phase 1: Correctness & Completeness (Next)

### 1.1 B-tree Completeness

- [ ] **B-tree delete with borrow/merge/root-shrink** — currently `NodeEmpty` on underflow. Implement left-borrow, right-borrow, left-merge, right-merge, and root-shrink when the root becomes a single-key internal node.
- [ ] **Larger node capacity** — increase `MAX_KEYS` from 64 → 256+ (or make it page-size-aware). More fanout = shallower tree = fewer disk reads.
- [ ] **Prefix key compression** — store only the differing suffix in internal node separators to maximize fanout.
- [ ] **Overflow pages for large values** — values exceeding half-page size get stored on overflow chains. Lift the 2KB value limit.

### 1.2 I/O Subsystem

- [ ] **Buffered I/O with write-back cache** — batch multiple page writes into a single `io_uring` (Linux) / `IOCP` (Windows) / `preadv/pwritev` (POSIX) operation.
- [ ] **Direct I/O support** — `O_DIRECT` bypass page cache for workload-specific tuning.
- [ ] **Configurable page cache** — currently hardcoded to 32 entries. Make it runtime-configurable (default 256+).
- [ ] **LRU-K page replacement** — replace simple eviction with LRU-2 or Clock-Sweep for better cache hit rates under realistic workloads.
- [ ] **Adaptive read-ahead** — detect sequential scan patterns and pre-fetch adjacent pages.

### 1.3 WAL Improvements

- [ ] **Group commit** — batch multiple WAL appends into a single `writeAll` + `sync` call for throughput.
- [ ] **WAL pre-allocation** — pre-allocate WAL file in chunks to reduce filesystem fragmentation and allocation syscalls.
- [ ] **Double-buffered WAL** — write to a new WAL segment while replaying the old one, reducing open-close churn.

### 1.4 Testing & Fuzzing

- [ ] **Deterministic crash testing** — inject failures (power loss, disk full, corruption) at every write boundary, verify recovery.
- [ ] **Property-based fuzzing** — use `zig fuzz` to generate random key/value sequences and verify invariants.
- [ ] **WAL corruption fuzzing** — random bit-flips, truncations, duplicate records, out-of-order records.
- [ ] **Benchmarks with realistic data** — add Zipfian distribution workloads, read-heavy (95/5), write-heavy (5/95), scan-heavy, mixed-size key/value.
- [ ] **Compare against RocksDB** — run the same workloads (db_bench defaults) and publish comparative numbers.

## Phase 2: Performance — B+ Tree Foundation (v0.2)

### 2.1 B+ Tree Conversion

Convert from B-tree to B+ tree:
- [ ] **Data on leaves only** — internal nodes store only keys + child pointers, doubling internal node fanout
- [ ] **Leaf page linked list** — doubly-linked leaf pages for O(1) forward/backward range scans
- [ ] **Sequential read optimization** — leaf pages are accessed sequentially, enabling large sequential reads

### 2.2 Bloom Filter

- [ ] **Per-page bloom filter** — attach a small Bloom filter to each leaf page header. Before loading a page for point lookups, check the bloom filter.
- [ ] **Whole-database bloom filter** — load a global bloom filter at startup from a metadata page. Eliminates disk reads for ~99% of misses.
- [ ] **Configurable false-positive rate** — default 1%, tunable per database open.

### 2.3 Memtable + Flush (LSM-adjacent)

- [ ] **In-memory sorted skip list / Arena** — new writes go to an in-memory memtable first (zero disk I/O). Flushes to disk as sorted leaf pages when full.
- [ ] **Immutable memtable** — once full, the memtable becomes immutable and is flushed in background while new writes go to a fresh memtable.
- [ ] **WAL → memtable mapping** — WAL entries map directly to memtable insertions, eliminating double-write during normal operation.

### 2.4 Compression

- [ ] **Pluggable compression interface** — trait-like API for compression algorithms.
- [ ] **LZ4** — fastest, lowest compression ratio. Default for page-level compression.
- [ ] **Zstandard** — best compression/throughput tradeoff. Optional.
- [ ] **Page-level vs block-level compression** — pages are compressed before disk write, decompressed on cache load.

### 2.5 Performance Targets

After Phase 2, target:
- Sequential insert: 500K+ ops/sec
- Random insert: 400K+ ops/sec
- Point lookup: 200K+ ops/sec
- Update: 50K+ ops/sec

## Phase 3: Production Features (v0.3)

### 3.1 MVCC (Multi-Version Concurrency Control)

- [ ] **Snapshot isolation** — readers see a consistent snapshot without blocking writers
- [ ] **Concurrent readers** — multiple readers can access the database simultaneously
- [ ] **Single writer with optimistic concurrency** — writer uses optimistic locking, retries on conflict
- [ ] **Garbage collection of old versions** — background reclamation of unreachable versions

### 3.2 Column Families

- [ ] **Multiple logical keyspaces** — like RocksDB column families, each with independent compaction and configuration
- [ ] **Atomic multi-column-family writes** — single transaction can write to multiple column families atomically
- [ ] **Cross-column-family iteration** — merge iterators for scanning across families

### 3.3 Advanced Query Support

- [ ] **Range queries** — `scan(start_key, end_key, limit)` with forward and backward iteration
- [ ] **Prefix seek** — `seekPrefix("user:123:")` jumps to the first key matching a prefix
- [ ] **Reverse iteration** — backward scans via leaf page linked list
- [ ] **Merge operator** — user-defined associative merge functions (counters, lists, etc.)

### 3.4 Compaction Strategies

- [ ] **Level compaction** — sort runs into levels, compact level N into N+1 (RocksDB default)
- [ ] **Universal compaction** — sorted runs of increasing size, merge when ratio exceeds threshold
- [ ] **FIFO compaction** — delete oldest files when total size exceeds limit (log-like workloads)
- [ ] **Configurable compaction priority** — background, idle-priority, or forced

### 3.5 Backup & Restore

- [ ] **Online backup** — consistent backup while database is live
- [ ] **Incremental backup** — only copy changed files since last backup
- [ ] **Point-in-time restore** — restore to a specific WAL sequence number

## Phase 4: World-Class (v0.5+)

### 4.1 Storage Engine Options

- [ ] **B+ tree mode** — for read-heavy, random-access workloads
- [ ] **LSM-tree mode** — for write-heavy workloads with sorted runs
- [ ] **Unified engine** — automatically switch strategy based on workload detection

### 4.2 Advanced Features

- [ ] **TTL (Time-To-Live)** — automatic key expiration
- [ ] **Prefix bloom filters** — partitioned bloom filters for prefix scans
- [ ] **Write batching API** — `writeBatch([{put, k, v}, {delete, k}...])` for atomic multi-ops
- [ ] **Secondary indexes** — user-defined indexes on values
- [ ] **Full-text search integration** — inverted index module

### 4.3 Observability

- [ ] **Prometheus metrics** — ops/sec, latency percentiles (p50/p99), cache hit rate, compaction stats
- [ ] **Structured logging** — JSON logs with trace IDs for production debugging
- [ ] **Slow query log** — configurable threshold for logging slow operations
- [ ] **Built-in profiler** — CPU and memory profiling endpoints

### 4.4 Encryption

- [ ] **Transparent data encryption (TDE)** — AES-256-GCM at rest
- [ ] **Encrypted WAL** — WAL records encrypted before write
- [ ] **Key management** — external KMS integration, key rotation

### 4.5 Distributed Mode (Future — v1.0+)

- [ ] **Raft consensus** — multi-node replication with strong consistency
- [ ] **Sharding** — automatic range-based or hash-based sharding
- [ ] **Cross-region replication** — async or sync replication for disaster recovery

## Non-Functional Goals

### Documentation

- [ ] **Architecture guide** — deep-dive into storage engine internals
- [ ] **API documentation** — auto-generated from doc comments
- [ ] **Performance tuning guide** — how to configure for your workload
- [ ] **Migration guide** — from RocksDB/LevelDB to KVDB

### Ecosystem

- [ ] **Zig package manager support** — `zig fetch` compatible
- [ ] **Language bindings** — Python, Go, Rust, Node.js (beyond current C FFI)
- [ ] **Benchmark harness** — reproducible benchmarking against RocksDB, LevelDB, LMDB
- [ ] **Integration tests** — test against real applications (key-value cache, session store, etc.)

### Quality

- [ ] **100% test coverage** of all code paths
- [ ] **Chaos testing** — randomized failure injection in CI
- [ ] **Fuzzing in CI** — continuous fuzzing with regression corpus
- [ ] **Formal verification** — prove B-tree invariants for critical paths (long-term)

## Milestones & Release Plan

| Version | Focus | Target Date |
|---------|-------|-------------|
| **v0.1** | Toy database with basic CRUD, WAL, B-tree | ✅ Done |
| **v0.1.1** | Performance pass #1 — eliminate hot-path allocations | ✅ Done |
| **v0.2** | B+ tree, bloom filter, memtable, compression | Q2 2026 |
| **v0.3** | MVCC, column families, compaction strategies | Q3 2026 |
| **v0.5** | Pluggable engine (B+ tree / LSM), observability | Q4 2026 |
| **v1.0** | Production-ready, distributed mode, ecosystem | Q1 2027 |

## Comparison: What RocksDB Has That KVDB Needs

| Feature | RocksDB | KVDB Now | Priority |
|---------|---------|----------|----------|
| LSM-tree storage | Yes | No (B-tree) | High — Phase 2 |
| Bloom filters | Yes | No | High — Phase 2 |
| Compression (LZ4/Zstd) | Yes | No | High — Phase 2 |
| Multi-threaded reads | Yes | No | Medium — Phase 3 |
| Snapshots | Yes | No | Medium — Phase 3 |
| Column families | Yes | No | Medium — Phase 3 |
| Merge operators | Yes | No | Medium — Phase 3 |
| Compaction strategies | 3 types | None yet | Medium — Phase 3 |
| Write batching | Yes | No | Medium — Phase 3 |
| Range queries | Yes | Iterator only | Low — Phase 3 |
| Backup/Restore | Yes | Export/Import | Low — Phase 3 |
| Encryption | No (external) | No | Low — Phase 4 |
| TTL | Yes | No | Low — Phase 4 |

## KVDB's Competitive Advantages

What can KVDB do **better** than RocksDB?

1. **Simpler codebase** — RocksDB is 500K+ lines of C++. KVDB targets < 20K lines of Zig. Easier to audit, maintain, and embed.
2. **Compile-time configuration** — Zig's `comptime` lets the compiler eliminate dead code paths for your specific configuration.
3. **No hidden allocations** — Every allocation is explicit and auditable. No surprises in production.
4. **Single binary** — No dependencies, no dynamic linking. One static library.
5. **B-tree by default** — For read-heavy random-access workloads, B-trees outperform LSM-trees on point lookups (no SST file traversal).
6. **Smaller footprint** — Target < 500KB binary vs RocksDB's 5MB+.
