---
type: Spec
parent_flow: storage-completeness
chapter_id: ch04-arrow-shm-and-docs
title: 'Chapter 04: Strict Arrow SHM and Bounded Storage Codecs'
description: R8 hardens named SHM allocation and ArrowCursor IPC, then implements
  bounded read and write codec bridges on public sync/async storage helpers. The three
  tasks separately own strict allocation, read lifecycle, and write lifecycle.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
- ch01-native-engine-and-auth
- ch02-orm-and-sql-storage-contracts
- ch03-litestar-and-range-streaming
replan_trigger: null
current_task: storage-completeness:ch04:t01
plan_revision: 9
plan_commit: 222cbf99
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T00:00:00Z'
sources:
- id: store
  resource: src/py/vertebra/storage/_store.py::AsyncObjectStore
  title: Native async storage facade
- id: pool
  resource: src/py/vertebra/utils/sync_tools.py::SyncPool
  title: Codec worker execution
- id: cursor
  resource: src/py/vertebra/db/arrow.py::ArrowCursor
  title: Arrow IPC cursor
tags:
- storage
- arrow
---
# Chapter 04: Strict Arrow SHM and Bounded Storage Codecs

<!-- truth: start -->
- Native asynchronous storage I/O remains on the caller loop; existing SyncPool executes synchronous PyArrow codecs [^pool].
- Public ArrowCursor methods and native buffer ownership remain stable while required named allocations fail closed [^cursor].
- Storage batch methods and SQLSpec consumers retain bounded demand by default and deterministic cancellation cleanup [^store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

R8 hardens named SHM allocation and ArrowCursor IPC, then implements bounded read and write codec bridges on public sync/async storage helpers. The three tasks separately own strict allocation, read lifecycle, and write lifecycle.

Plan baseline is `222cbf99`. This chapter depends on ch01-native-engine-and-auth, ch02-orm-and-sql-storage-contracts, ch03-litestar-and-range-streaming. The native storage facade supplies stream/range I/O and multipart abort; PyArrow 26.0.1 supplies synchronous callbacks, dictionary memo, and codec operations. Sources below anchor the public seams. Skills are local runtime-ipc and pyo3-bridge for allocation/lifetime changes, python-framework for facade contracts, and review-tests for public behavior evidence.

## 2. Requirements, Concrete Contracts & Architecture

R8.1 preserves `ArrowCursor.from_rows(rows, columns=None, *, threshold_rows=256)`, buffer_view and to_arrow_table, removes JSON pseudo-IPC, and propagates Unix named-allocation failure without Python anonymous mmap fallback. R8.2 implements public iterators with worker-only synchronous PyArrow callbacks and caller-loop native async I/O. R8.3 adds bounded encoder sinks and native multipart writes. The allocator stays private with normalized-name/aligned-size inputs; public frame writer is write_shm_arrow_frame. VTBARROW headers and retained-view lifetime stay unchanged.

Caller -> public storage method -> private storage/_arrow.py codec adapter -> native asynchronous stream/range/multipart API. The existing SyncPool is a codec scheduler; no synchronous remote store is called by async helpers. The sync API uses native sync store operations. Shared signatures are `iter_arrow_batches(key)`, `iter_parquet_batches(key, *, batch_size=65536, columns=None)`, `write_arrow(key, data, *, batch_size=65536) -> PutResult`, and `write_parquet(key, batches, *, schema=None, compression="zstd", row_group_size=65536) -> PutResult`; async writes are awaitable and async reads return AsyncIterator directly.

| Failure boundary | Required behavior |
| --- | --- |
| Missing pyarrow | MissingDependencyError with extra="arrow" before upload/read side effects. |
| Worker waiting for remote async I/O | Caller loop stays responsive; cancellation cancels pending coroutine and waits for worker cleanup. |
| Corrupt codec input or producer error | Original exception propagates; sources close and incomplete uploads abort. |
| Empty data | Schema-bearing output is valid; schema-less empty batch iterable raises ValueError on writes. |
| Consumer stops early | Explicit close/aclose stops demand and closes backing resources once. |

| Alternative | Decision and rationale |
| --- | --- |
| Per-demand SyncPool codec adapter | Selected: existing scheduler, native async I/O, persistent codec dictionary state, no producer queue. |
| Full-file BytesIO/tempfile staging | Rejected: delays first-batch delivery and stages whole datasets. |
| AsyncReadableFile passed directly to PyArrow | Rejected: synchronous Python callbacks cannot await its coroutine reads. |
| Custom sparse rowgroup planner/native codec binding | Rejected: adds format/FFI surface unnecessary for the existing PyArrow callback seam. |

Parquet batch readers construct `ParquetFile` with `pre_buffer=False` by default, including all new public batch helpers. SQLSpec explicitly supplied `pre_buffer=True` remains honored for API compatibility and opts out of the bounded-prefetch guarantee; do not silently discard it. Fixed-size footer read-ahead is permitted. Full-Table read helpers preserve caller options and intentionally materialize their Table return.

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/3 files documented]`. The original documentation manifest is preserved in ch05:t02: docs/usage/storage.md, docs/usage/storage-auth-and-signing.md, and .agents/bundles/knowledge/domains/storage/object-storage.md. This chapter supplies tested symbols and lifetime behavior for that documentation.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Verification |
| --- | --- | --- |
| R8 | [storage-completeness:ch04:t01](tasks/t01-strict-shm-and-arrow-ipc-cursor.md) | behavior_tdd; `src/rs/vertebra-db/src/arrow_shm.rs`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| R8 | [storage-completeness:ch04:t02](tasks/t02-streaming-arrow-parquet-storage.md) | behavior_tdd; `src/py/tests/unit/storage/test_storage_store.py` |
| R8 | [storage-completeness:ch04:t03](tasks/t03-streaming-arrow-parquet-writes.md) | behavior_tdd; `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch04:t01 — Preserve ArrowCursor APIs while enforcing named SHM and real IPC (tasks/t01-strict-shm-and-arrow-ipc-cursor.md)
- [ ] storage-completeness:ch04:t02 — Read Arrow and Parquet through a bounded async codec bridge (tasks/t02-streaming-arrow-parquet-storage.md)
- [ ] storage-completeness:ch04:t03 — Write Arrow and Parquet with bounded multipart codec output (tasks/t03-streaming-arrow-parquet-writes.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make docs` exits 0; correctness review precedes fresh quality review.

## 6. Current State & Remaining Plan Snapshot

- Plan baseline: `222cbf99`, revision 9; 3 pending tasks and no completed implementation claimed.
- Dependency tasks finish before dependent tasks begin; every worksheet owns one isolated execution and atomic commit.
- Documentation remains explicitly tracked in ch05:t02; no original R8 requirement is deferred.

[^store]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Native storage facade.
[^pool]: `src/py/vertebra/utils/sync_tools.py::SyncPool` — Existing worker execution.
[^cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — IPC cursor and ownership.
