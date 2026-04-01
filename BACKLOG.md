# KVDB Backlog

> Derived from `ROADMAP.md`.
> 
> Use this file as the short-to-medium term execution queue. `ROADMAP.md` explains the why and sequencing; this file focuses on actionable work items.

## Priority Legend

- **P0** — blocks correctness or core credibility
- **P1** — core capability needed for real usability
- **P2** — important quality/tooling work
- **P3** — optional or later-stage improvements

## Recommended Execution Order

1. BG-001 README reality check
2. BG-002 transaction semantics contract
3. BG-003 public options cleanup
4. BG-004 storage test matrix
5. BG-005 WAL replay in open path
6. BG-006 transaction-aware WAL recovery
7. BG-007 WAL corruption/truncation handling
8. BG-008 recovery test suite
9. BG-009 root split
10. BG-010 internal-node insert/split propagation
11. BG-011 multi-level search tests
12. BG-012 multi-page iterator

---

## Milestone A — Honest and Recoverable

### BG-001 — Align README with implementation
- **Priority:** P0
- **Depends on:** none
- **Why:** `README.md` currently promises more than the code guarantees.
- **Relevant files:** `README.md`, `src/btree.zig`, `src/kvdb.zig`, `src/wal.zig`

#### Scope
- Audit feature claims related to:
  - B-tree completeness
  - ACID/transaction guarantees
  - crash recovery
  - iteration support
- Add a short limitations/maturity section.
- Reword unsupported or partial features as planned/partial.

#### Acceptance Criteria
- [x] README no longer claims full balanced-tree behavior if only single-level leaf storage exists.
- [x] README no longer claims startup recovery unless the implementation actually performs it.
- [x] README explicitly documents current known limitations.
- [x] Quick-start examples remain valid after edits.

---

### BG-002 — Define and document transaction semantics
- **Priority:** P0
- **Depends on:** none
- **Why:** Current transaction API exists, but durability and rollback semantics are not clearly specified.
- **Relevant files:** `src/kvdb.zig`, `README.md`, `examples/basic.zig`

#### Scope
- Decide and document:
  - whether writes require explicit transactions
  - whether single writes are implicit transactions
  - what `commit()` guarantees
  - what `abort()` guarantees today
  - what happens on crash before/after commit marker

#### Acceptance Criteria
- [x] Write semantics are described in code comments and README.
- [x] `commit` and `abort` guarantees are explicitly documented.
- [x] Any mismatch between docs and behavior is removed.
- [x] Example code does not imply stronger guarantees than implementation provides.

---

### BG-003 — Clean up unsupported public options
- **Priority:** P0
- **Depends on:** BG-002
- **Why:** `Options` currently exposes fields that are not fully honored.
- **Relevant files:** `src/kvdb.zig`, `src/constants.zig`, `README.md`

#### Scope
- Decide the fate of:
  - `page_size`
  - `enable_compression`
- Either:
  - remove unsupported options, or
  - clearly mark them experimental/non-functional.

#### Acceptance Criteria
- [x] Public API does not suggest support for behavior that is not implemented.
- [x] README and API docs match the final option surface.
- [x] Tests/examples compile and use the updated options API.

---

### BG-004 — Add storage test matrix and checklist
- **Priority:** P0
- **Depends on:** none
- **Why:** Storage code needs explicit invariant coverage, not just happy-path tests.
- **Relevant files:** `src/*.zig`, `README.md` or `CONTRIBUTING`-style doc if later added

#### Scope
- Define test buckets for:
  - restart persistence
  - duplicate insert/update/delete
  - node full behavior
  - WAL corruption and truncation
  - compaction correctness
  - key/value size boundaries
- Add a lightweight checklist document or section.
- Normalize naming of tests by subsystem.

#### Acceptance Criteria
- [x] A documented storage test matrix exists in the repo.
- [x] Missing high-risk scenarios are listed explicitly.
- [x] Contributors can tell what invariants each subsystem must satisfy.

---

## Milestone B — Real WAL Recovery

### BG-005 — Wire WAL replay into `Database.open`
- **Priority:** P0
- **Depends on:** BG-002, BG-003
- **Why:** WAL replay code exists, but startup recovery is not connected to open flow.
- **Relevant files:** `src/kvdb.zig`, `src/wal.zig`

#### Scope
- Detect a non-empty WAL during open.
- Replay log records before normal reads/writes begin.
- Prevent replay from re-logging recovered operations.
- Define clean startup behavior after replay completes.

#### Acceptance Criteria
- [x] Opening a DB with pending WAL state triggers recovery automatically.
- [x] Replay does not append new WAL records while replaying.
- [x] Open succeeds on clean WAL and recovered WAL cases.

---

### BG-006 — Make WAL recovery transaction-aware
- **Priority:** P0
- **Depends on:** BG-005
- **Why:** Recovery should follow commit/abort boundaries, not blindly apply every record.
- **Relevant files:** `src/wal.zig`, `src/kvdb.zig`, `src/constants.zig`

#### Scope
- Define how logical transactions are recognized in WAL.
- Replay committed operations only.
- Ignore/discard aborted operations.
- Handle empty transactions and edge ordering cleanly.

#### Acceptance Criteria
- [x] Recovery behavior is deterministic for insert/delete/commit/abort sequences.
- [x] Aborted operations do not appear after reopen.
- [x] Committed operations do appear after reopen.

---

### BG-007 — Handle corrupted and truncated WAL safely
- **Priority:** P0
- **Depends on:** BG-005
- **Why:** Real recovery must handle bad tails and checksum failures predictably.
- **Relevant files:** `src/wal.zig`, `src/kvdb.zig`

#### Scope
- Define handling for:
  - truncated tail record
  - bad checksum
  - invalid record type
  - partial value payload
- Decide what is recoverable vs fatal.
- Surface meaningful errors when recovery cannot continue.

#### Acceptance Criteria
- [x] Tail truncation behavior is defined and tested.
- [x] Checksum failure behavior is defined and tested.
- [x] Fatal recovery failures produce explicit errors.

---

### BG-008 — Add restart/recovery test suite
- **Priority:** P0
- **Depends on:** BG-005, BG-006, BG-007
- **Why:** Recovery work is not credible without restart-oriented tests.
- **Relevant files:** `src/kvdb.zig`, `src/wal.zig`, `build.zig`

#### Scope
- Add tests for:
  - committed write survives restart
  - aborted write does not survive restart
  - uncommitted write handling
  - truncated WAL tail
  - corrupted checksum

#### Acceptance Criteria
- [x] Recovery tests run under `zig build test`.
- [x] Recovery regressions are caught automatically.
- [x] At least one test covers each failure mode defined in BG-007.

---

## Milestone C — Multi-Page B-Tree

### BG-009 — Implement full-root split path
- **Priority:** P1
- **Depends on:** BG-008
- **Why:** Current tree stops growing once the root leaf is full.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`, `src/constants.zig`

#### Scope
- Detect full root on insert.
- Allocate split destination page(s).
- Create a new internal root.
- Persist root metadata changes if root page identity changes.

#### Acceptance Criteria
- [x] Inserting past one leaf-page capacity no longer returns `NodeFull` for the first overflow case.
- [x] Root becomes internal after split.
- [x] Existing keys remain readable after split.

---

### BG-010 — Implement internal-node insert and split propagation
- **Priority:** P1
- **Depends on:** BG-009
- **Why:** Root split alone is not enough; inserts must continue through multiple levels.
- **Relevant files:** `src/btree.zig`, `src/pager.zig`, `src/constants.zig`

#### Scope
- Route insert to child pages.
- Split child nodes when full.
- Promote separator keys upward.
- Persist child page references correctly.

#### Acceptance Criteria
- [x] Tree grows beyond two levels when needed.
- [x] Ordered inserts and random inserts both remain searchable.
- [x] Parent/child links remain consistent after splits.

---

### BG-011 — Add multi-level search correctness tests
- **Priority:** P1
- **Depends on:** BG-010
- **Why:** Tree growth needs dedicated correctness tests, not just compile success.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`

#### Scope
- Add tests for:
  - ordered insert growth
  - random insert growth
  - search across internal nodes
  - separator-boundary lookups

#### Acceptance Criteria
- [x] Searches succeed across multiple levels.
- [x] Tests cover boundary keys around split points.
- [x] Results match inserted data for random and sequential patterns.

---

### BG-012 — Extend iterator to multi-page traversal
- **Priority:** P1
- **Depends on:** BG-010
- **Why:** Iteration is currently limited to a single page.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`, `src/cli.zig`

#### Scope
- Choose traversal design:
  - leaf chaining, or
  - stack-based traversal
- Iterate in sorted order across leaf pages.
- Preserve current API if possible, or revise it cleanly.

#### Acceptance Criteria
- [x] Iteration returns all keys across multiple pages.
- [x] Output order is sorted.
- [x] Existing CLI list operation works with multi-page trees.

---

### BG-013 — Design safe multi-page delete behavior
- **Priority:** P2
- **Depends on:** BG-010
- **Why:** Multi-page delete can ship in stages, but behavior must be explicit.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`

#### Scope
- Decide short-term supported behavior.
- Later implement:
  - sibling borrow
  - merge
  - root shrink
- Add tests for underflow paths.

#### Acceptance Criteria
- [x] Supported delete behavior is documented.
- [x] Underflow behavior is either safely implemented or explicitly rejected.

---

## Milestone D — Space Management

### BG-014 — Reduce node-local fragmentation
- **Priority:** P1
- **Depends on:** BG-010
- **Why:** Current update path does delete+insert and leaves dead payload bytes behind.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`

#### Scope
- Rework update logic to avoid unnecessary payload churn.
- Add node defragmentation or repacking routine.
- Trigger compaction when fragmentation crosses a threshold.

#### Acceptance Criteria
- [x] Repeated updates do not exhaust page payload space as quickly as before.
- [x] Page-local payload packing remains valid after repack.
- [x] Tests cover update-heavy workloads.

---

### BG-015 — Implement free-page reuse via freelist
- **Priority:** P1
- **Depends on:** BG-010
- **Why:** Without page reuse, file growth is one-directional.
- **Relevant files:** `src/pager.zig`, `src/constants.zig`, `src/kvdb.zig`

#### Scope
- Define freelist page format.
- Track freed pages in metadata.
- Reuse freed pages before extending file.
- Keep `last_page_id` semantics coherent.

#### Acceptance Criteria
- [x] Deleted/reclaimed pages can be reused.
- [x] Metadata reflects freelist state correctly.
- [x] New allocation prefers freelist pages when available.

---

### BG-016 — Strengthen compaction correctness
- **Priority:** P2
- **Depends on:** BG-014, BG-015
- **Why:** Compaction should be trustworthy, not just convenient.
- **Relevant files:** `src/kvdb.zig`, `src/cli.zig`

#### Scope
- Validate compacted DB contents against source DB.
- Define crash-safety expectations during compaction.
- Decide whether compaction rebuilds or preserves structure.

#### Acceptance Criteria
- [x] Compaction preserves all live key/value pairs.
- [x] Tests compare source and compacted DB contents.
- [x] Failure behavior during compaction is documented.

---

## Milestone E — Tooling and Operability

### BG-017 — Add `verify` command
- **Priority:** P2
- **Depends on:** BG-008, BG-010
- **Why:** Users need a way to validate a DB without inspecting raw files manually.
- **Relevant files:** `src/cli.zig`, `src/kvdb.zig`, `src/wal.zig`, `src/constants.zig`

#### Scope
- Verify:
  - metadata header
  - node headers
  - B-tree ordering
  - WAL structure/checksums
- Return non-zero on failure.

#### Acceptance Criteria
- [x] `kvdb-cli <db> verify` reports success/failure clearly.
- [x] Broken metadata/tree/WAL cases are detectable.

---

### BG-018 — Add `inspect` command
- **Priority:** P2
- **Depends on:** BG-010
- **Why:** Tree and file introspection will make development/debugging much easier.
- **Relevant files:** `src/cli.zig`, `src/kvdb.zig`

#### Scope
- Print summary such as:
  - page count
  - root page id
  - tree height
  - node counts
  - optional freelist info

#### Acceptance Criteria
- [x] `inspect` prints a concise structural summary.
- [x] Output helps debug tree shape and file usage.

---

### BG-019 — Add export/import commands
- **Priority:** P3
- **Depends on:** BG-012
- **Why:** Useful for debugging, migration, and backup workflows.
- **Relevant files:** `src/cli.zig`, `src/kvdb.zig`

#### Scope
- Add `dump`/`export` command.
- Add `load`/`import` command.
- Define portable serialization format.

#### Acceptance Criteria
- [x] Exported data can be loaded into a fresh DB.
- [x] Round-trip preserves all keys and values.

---

## Milestone F — FFI Hardening

### BG-020 — Complete C API memory ownership model
- **Priority:** P2
- **Depends on:** BG-002
- **Why:** Current FFI story is incomplete for returned buffers.
- **Relevant files:** `src/kvdb.zig`

#### Scope
- Add explicit free function or redesign result API.
- Define stable ownership rules.
- Clarify which allocator owns returned memory.

#### Acceptance Criteria
- [x] FFI caller can free returned data safely.
- [x] Ownership rules are documented next to exported functions.

---

### BG-021 — Add FFI examples and basic boundary checks
- **Priority:** P3
- **Depends on:** BG-020
- **Why:** Integration should be demonstrable, not inferred.
- **Relevant files:** `src/kvdb.zig`, `examples/`

#### Scope
- Add one C example.
- Optionally add one Python `ctypes` or `cffi` example.
- Harden exported functions against obvious bad inputs.
- Define stable error/status codes for FFI callers.
- Add direct tests for invalid null/length FFI inputs.

#### Acceptance Criteria
- [x] At least one non-Zig example works end-to-end.
- [x] FFI API docs explain build/link usage.
- [x] Python `ctypes` example works end-to-end.
- [x] Invalid FFI input handling is covered by tests.

---

## Milestone G — Performance and CI Quality

### BG-022 — Add benchmark suite
- **Priority:** P2
- **Depends on:** BG-012, BG-014
- **Why:** Performance work should be based on measurements.
- **Relevant files:** `build.zig`, `examples/` or future benchmark dir

#### Scope
- Add benchmarks for:
  - sequential inserts
  - random inserts
  - point lookups
  - scans
  - update/delete
  - compaction

#### Acceptance Criteria
- [x] Benchmarks are runnable with a documented command.
- [x] Results are reproducible enough to compare changes.

---

### BG-023 — Fix CI rough edges and add recovery coverage
- **Priority:** P2
- **Depends on:** BG-008
- **Why:** CI should validate the most failure-prone paths.
- **Relevant files:** `.github/workflows/ci.yml`, `build.zig`

#### Scope
- Fix workflow configuration issues.
- Ensure recovery/corruption tests run in CI.
- Keep release builds green across supported platforms.
- Improve pager cache lookup so repeated page access does not pay a linear scan cost.

#### Acceptance Criteria
- [x] CI configuration is valid and consistent.
- [x] Recovery tests are part of automated validation.
- [x] Build/test jobs pass on supported platforms.

---

### BG-024 — Add randomized/property-style storage tests
- **Priority:** P3
- **Depends on:** BG-010, BG-014
- **Why:** Long random sequences are good at exposing structural bugs.
- **Relevant files:** `src/btree.zig`, `src/kvdb.zig`

#### Scope
- Generate random operation sequences.
- Compare results against an in-memory reference model.
- Preserve failing seeds for regression.

#### Acceptance Criteria
- [x] Randomized tests can reproduce failures deterministically.
- [x] Storage behavior matches the reference model over long sequences.

---

## Parking Lot

These are useful, but should wait until the core is credible:

- compression
- range scans
- prefix scans
- snapshots
- concurrent readers/writers
- secondary indexes
- page checksums for main DB file
- background checkpointing

---

## Suggested Labels

- `priority/p0`
- `priority/p1`
- `priority/p2`
- `priority/p3`
- `area/docs`
- `area/wal`
- `area/btree`
- `area/pager`
- `area/cli`
- `area/ffi`
- `area/ci`
- `area/tests`
- `milestone/honest-and-recoverable`
- `milestone/scalable-core-storage`
- `milestone/tooling`

---

## Ready Queue

If you want to start implementation immediately, the clean first slice is:

1. BG-001
2. BG-002
3. BG-003
4. BG-004

That gives the project a clear contract before touching recovery and tree structure.
