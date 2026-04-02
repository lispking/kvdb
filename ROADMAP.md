# KVDB Roadmap

> Last updated: 2026-04-01
> 
> This roadmap is based on the current implementation state in `src/btree.zig`, `src/kvdb.zig`, `src/wal.zig`, `src/pager.zig`, `src/constants.zig`, `src/cli.zig`, and `README.md`.

## Current State Snapshot

The project already has a usable foundation:

- page-based file storage
- single-page B-tree leaf storage
- WAL record writing and iteration
- basic transaction API
- CLI for get/put/delete/list/stats/compact
- basic tests and CI

The main gaps are:

1. **B-tree is still single-level**  
   `src/btree.zig:360`, `src/btree.zig:422`, `src/btree.zig:458`
2. **WAL replay exists but is not wired into open/recovery flow**  
   `src/wal.zig:295`, `src/kvdb.zig:151`
3. **Transaction rollback is simplified and reload-based**  
   `src/kvdb.zig:82`, `src/kvdb.zig:95`
4. **Update/delete path causes page-local fragmentation**  
   `src/kvdb.zig:292`, `src/btree.zig:286`
5. **Metadata fields and options are only partially used**  
   `src/constants.zig:111`, `src/constants.zig:113`, `src/constants.zig:115`, `src/kvdb.zig:17`, `src/kvdb.zig:23`
6. **README promises more than the implementation currently delivers**  
   `README.md:27`, `README.md:28`, `README.md:46`, `README.md:101`

---

## Guiding Principles

- **Reliability before features**: finish crash safety and correctness before adding advanced APIs.
- **Make implementation match documentation**: either reduce claims or complete missing behavior.
- **Prefer milestone-complete increments**: each phase should end in a demonstrably better database.
- **Test every storage invariant**: recovery, corruption, restart, and boundary behavior need explicit coverage.

---

## Phase 0 — Stabilize the Contract

**Goal:** Make the project honest, testable, and ready for deeper storage work.

### 0.1 Align documentation with reality
- [x] Audit README feature claims against implementation
- [x] Mark unfinished capabilities as planned/partial
- [x] Document known limitations explicitly:
  - single-level B-tree
  - single-page iteration
  - simplified rollback semantics
  - partial WAL lifecycle
- [x] Add a short “current maturity” section

**Deliverable:** README no longer over-promises.

### 0.2 Clarify transaction semantics
- [x] Decide whether writes are:
  - always transactional, or
  - auto-wrapped in implicit single-operation transactions
- [x] Define commit ordering and durability contract
- [x] Define what rollback guarantees today
- [x] Update API docs and examples to match actual behavior

**Deliverable:** A clear write/commit/abort contract.

### 0.3 Fix configuration surface mismatch
- [x] Remove or mark experimental config fields that are not implemented
- [x] Decide whether `page_size` is fixed for now or truly configurable
- [x] Decide whether `enable_compression` stays in API or moves to backlog

**Deliverable:** Public options only expose supported behavior.

### 0.4 Establish a storage test matrix
- [x] Add explicit test categories:
  - restart persistence
  - boundary sizes
  - duplicate insert/update/delete flows
  - node full behavior
  - WAL corruption/truncation
  - compaction correctness
- [x] Add test naming convention by subsystem
- [x] Add a small test checklist to the repository

**Deliverable:** A test plan covering storage invariants.

**Exit criteria for Phase 0**
- Docs reflect reality
- Transaction semantics are explicitly defined
- Unsupported options are cleaned up
- Test coverage plan is in place

---

## Phase 1 — Make WAL Recovery Real

**Goal:** Close the durability loop so recovery actually happens on startup.

### 1.1 Wire WAL replay into database open
- [x] Detect non-empty WAL during `Database.open`
- [x] Replay records before normal operation begins
- [x] Ensure replay does not recurse into WAL logging again
- [x] Define recovery behavior on clean vs dirty shutdown

### 1.2 Add transaction boundary handling to recovery
- [x] Group replay by logical transaction boundaries
- [x] Replay only committed records
- [x] Ignore or discard aborted records
- [x] Handle incomplete tail records safely

### 1.3 Handle corruption and partial writes
- [x] Validate CRC during replay
- [x] Decide behavior for:
  - bad checksum mid-log
  - truncated last record
  - invalid record type
- [x] Return meaningful recovery errors when needed

### 1.4 Add recovery tests
- [x] Restart after committed write
- [x] Restart after uncommitted write
- [x] Restart after abort
- [x] Restart with truncated WAL tail
- [x] Restart with corrupted checksum

**Deliverable:** Startup recovery that can be demonstrated with tests.

**Exit criteria for Phase 1**
- WAL replay is part of open path
- Recovery obeys commit/abort boundaries
- Corruption handling is tested

---

## Phase 2 — Upgrade the B-Tree from Toy to Real

**Goal:** Remove the single-page root limitation and support growth.

### 2.1 Implement root split path
- [x] Detect full root during insert
- [x] Allocate new pages for split result
- [x] Create internal root node
- [x] Update metadata/root pointer if needed

### 2.2 Implement internal node insertion
- [x] Route inserts to correct child
- [x] Split child nodes when full
- [x] Promote separator keys upward
- [x] Persist child pointers correctly

### 2.3 Add multi-level search correctness
- [x] Validate child selection logic for internal nodes
- [x] Add tests for multiple tree levels
- [x] Add tests for ordered and random insert patterns

### 2.4 Extend iterator to multi-page traversal
- [x] Define traversal strategy
- [x] Support in-order traversal across multiple leaf pages
- [x] Add tests for sorted iteration across page boundaries

### 2.5 Plan deletion for multi-page trees
- [x] First ship safe deletion with clear limitations if needed
- [ ] Then implement rebalance strategy:
  - borrow from sibling
  - merge nodes
  - root shrink

**Deliverable:** Database can store more than one full root page of keys.

**Exit criteria for Phase 2**
- Insert/search works across multiple levels
- Iterator can traverse multi-page trees
- Capacity is no longer limited to one leaf page

---

## Phase 3 — Improve Space Management

**Goal:** Reduce fragmentation and reclaim space instead of only growing files.

### 3.1 Fix node-local fragmentation
- [x] Redesign update path to avoid delete+insert where possible
- [x] Add node compaction/defragmentation routine
- [ ] Repack key/value payloads when fragmentation becomes too high

### 3.2 Introduce free page reuse
- [x] Implement freelist page format
- [x] Track freed pages in metadata
- [x] Reuse free pages before extending file
- [x] Update `last_page_id` lifecycle consistently

### 3.3 Connect metadata to real storage behavior
- [x] Make `freelist_page` meaningful
- [x] Make `last_page_id` authoritative
- [x] Revisit whether `wal_offset` belongs in metadata or recovery state

### 3.4 Strengthen compaction
- [x] Validate compacted output against source DB
- [x] Add recovery-safe compaction flow
- [x] Decide whether compaction should rebuild tree structure or preserve it

**Deliverable:** Database growth becomes more controlled and storage reuse exists.

**Exit criteria for Phase 3**
- Repeated updates no longer waste page space aggressively
- Freed pages can be reused
- Compaction is correctness-tested

---

## Phase 4 — Tooling and Operability

**Goal:** Make the database easier to inspect, trust, and use.

### 4.1 Expand CLI beyond happy-path operations
- [x] Add `verify` command
- [x] Add `inspect` command for metadata/page/tree summary
- [x] Add `dump` / `export` command
- [x] Add `load` / `import` command
- [x] Consider range or prefix scan commands after iterator work lands

### 4.2 Add consistency verification tooling
- [x] Validate metadata header
- [x] Validate page/node headers
- [x] Validate B-tree key ordering
- [x] Validate WAL checksums and record structure

### 4.3 Improve error reporting UX
- [x] Map internal errors to clearer CLI output
- [x] Differentiate corruption vs not-found vs invalid usage
- [x] Return actionable messages for recovery-related failures

**Deliverable:** Users can inspect and validate DB state without reading raw files.

**Exit criteria for Phase 4**
- CLI can verify and inspect databases
- Operational failures are easier to diagnose

---

## Phase 5 — FFI and External Integration

**Goal:** Make the library safer to consume from other languages.

### 5.1 Complete C API lifecycle
- [x] Add explicit memory release API for returned buffers
- [x] Define stable error codes for FFI callers
- [x] Clarify ownership rules in comments/docs
- [x] Decide whether FFI returns owned buffers, borrowed buffers, or status+out-pointer

### 5.2 Add integration examples
- [x] Provide C example
- [x] Provide minimal Python `ctypes` or `cffi` example
- [x] Document build and link instructions

### 5.3 Harden FFI boundaries
- [x] Validate null pointers where needed
- [x] Validate length arguments defensively
- [x] Add tests around invalid FFI inputs where possible

**Deliverable:** External consumers can use the library without guessing ownership rules.

**Exit criteria for Phase 5**
- FFI memory contract is explicit
- Example consumers work end-to-end

---

## Phase 6 — Performance and Engineering Quality

**Goal:** Measure behavior and tighten the engineering loop.

### 6.1 Build a benchmark suite
- [x] Sequential insert benchmark
- [x] Random insert benchmark
- [x] Point lookup benchmark
- [x] Scan benchmark
- [x] Update/delete benchmark
- [x] Compaction benchmark

### 6.2 Improve cache and write behavior
- [x] Evaluate page cache lookup strategy
- [x] Add eviction policy if cache growth becomes unbounded
- [x] Revisit flush frequency and WAL sync strategy
- [x] Compare correctness/performance tradeoffs for fsync policy

### 6.3 Fix CI rough edges
- [x] Fix `lint` workflow matrix/config mismatch
- [x] Add benchmark or smoke-test job if useful
- [x] Make recovery/invariant tests part of CI
- [x] Keep release build green across platforms

### 6.4 Consider fuzz/property testing
- [x] Add randomized operation sequences
- [x] Compare results against in-memory reference model
- [x] Preserve failing seeds for regression tests

**Deliverable:** Performance decisions become evidence-based instead of intuitive.

**Exit criteria for Phase 6**
- Benchmarks exist and are repeatable
- CI covers critical correctness paths

---

## Optional Phase 7 — Advanced Features

**Goal:** Add higher-level capabilities only after core storage is solid.

### Candidate items
- [ ] Compression
- [ ] Range scans and prefix scans
- [ ] Snapshot or read-only transaction support
- [ ] Concurrent readers/writers model
- [ ] Secondary indexes
- [ ] Page checksums for main DB file
- [ ] Background checkpointing

**Rule:** Do not start this phase before Phases 1–3 are complete.

---

## Suggested Execution Order

If only the highest-value work should be done first, use this order:

1. **Phase 0** — stabilize docs/contracts/tests
2. **Phase 1** — real WAL recovery
3. **Phase 2** — real multi-page B-tree
4. **Phase 3** — space management and reuse
5. **Phase 4** — verify/inspect/export tooling
6. **Phase 5** — FFI hardening
7. **Phase 6** — benchmarks and CI quality
8. **Phase 7** — advanced features

---

## Near-Term Milestone Plan

### Milestone A — Honest and Recoverable
- [ ] Finish Phase 0
- [ ] Finish Phase 1

**Outcome:** The project is honest about its guarantees and survives restart/recovery scenarios.

### Milestone B — Scalable Core Storage
- [ ] Finish Phase 2
- [ ] Finish core parts of Phase 3

**Outcome:** Capacity is no longer capped by one root leaf page, and storage reuse begins to work.

### Milestone C — Usable Developer Tooling
- [ ] Finish Phase 4
- [ ] Finish Phase 5

**Outcome:** The database is easier to inspect, debug, and embed.

### Milestone D — Measured Quality
- [ ] Finish Phase 6

**Outcome:** Correctness and performance work become repeatable and measurable.

---

## Recommended First 10 Issues

1. [ ] Add README section documenting current limitations
2. [ ] Define and document transaction/durability semantics
3. [ ] Remove or mark unsupported `Options` fields
4. [ ] Wire WAL replay into `Database.open`
5. [ ] Add replay logic for commit/abort boundaries
6. [ ] Add recovery tests for restart/truncation/corruption
7. [ ] Implement root split on full leaf root
8. [ ] Implement internal node insert and split propagation
9. [ ] Extend iterator to multi-page traversal
10. [ ] Add page/node verification CLI command

---

## Definition of Done for “v1 usable core”

Treat the following as the minimum bar for a credible embedded database core:

- [ ] Restart recovery works and is tested
- [ ] B-tree supports multi-page growth
- [ ] Iterator supports cross-page traversal
- [ ] Storage reuse exists or compaction is trustworthy
- [ ] Docs match behavior
- [ ] CLI can verify integrity
- [ ] CI covers restart/corruption/boundary cases

Once these are complete, the project moves from “toy storage engine” to “small but defensible embedded KV store.”
