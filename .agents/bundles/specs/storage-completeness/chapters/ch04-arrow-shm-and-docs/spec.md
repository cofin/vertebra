---
type: Spec
parent_flow: storage-completeness
chapter_id: ch04-arrow-shm-and-docs
title: 'Chapter 04: Strict Arrow SHM and Bounded Storage Codecs'
description: R8 hardens named SHM allocation and ArrowCursor IPC, adds bounded storage read/write codecs, and enforces S3 multipart minimum parts. Four tasks own allocation, reads, writes, and S3 completion.
status: stable
state: active
planning_status: complete
depends_on_chapters:
- ch01-native-engine-and-auth
- ch02-orm-and-sql-storage-contracts
- ch03-litestar-and-range-streaming
replan_trigger: null
current_task: storage-completeness:ch04:t04
plan_revision: 10
plan_commit: '78293774'
state_revision: 5
last_verified_checkpoint: '78293774'
last_reviewed_commit: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T17:25:49.002660Z'
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
- Storage batch methods retain bounded demand by default and deterministic cancellation cleanup [^store]. SQLSpec delegation remains owned by ch05.
<!-- truth: end -->

## 1. Specification, Context & Provenance

R8 hardens named SHM allocation and ArrowCursor IPC, then implements bounded read and write codec bridges on public sync/async storage helpers. The four tasks separately own strict allocation, read lifecycle, write lifecycle, and S3 minimum-part compliance.

The active chapter owns plan revision 10 at baseline `78293774`; completed tasks t01–t03 retain their owned revision 9 at `222cbf99`. This chapter depends on ch01-native-engine-and-auth, ch02-orm-and-sql-storage-contracts, ch03-litestar-and-range-streaming. The native storage facade supplies stream/range I/O and multipart abort; PyArrow 26.0.1 supplies synchronous callbacks, dictionary memo, and codec operations. Sources below anchor the public seams. Skills are local runtime-ipc and pyo3-bridge for allocation/lifetime changes, python-framework for facade contracts, and review-tests for public behavior evidence.

## 2. Requirements, Concrete Contracts & Architecture

R8.1 preserves `ArrowCursor.from_rows(rows, columns=None, *, threshold_rows=256)`, buffer_view and to_arrow_table, removes JSON pseudo-IPC, and propagates Unix named-allocation failure without Python anonymous mmap fallback. R8.2 implements public iterators with worker-only synchronous PyArrow callbacks and caller-loop native async I/O. R8.3 adds bounded encoder sinks and native multipart writes. R1/R8.4 extends the native 8 MiB accumulator to normalized S3/B2 uploads so non-final parts meet S3 minimum-part rules. The allocator stays private with normalized-name/aligned-size inputs; public frame writer is write_shm_arrow_frame. VTBARROW headers and retained-view lifetime stay unchanged.

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

Progress: `[1/1 files documented in ch04]`. `.agents/bundles/knowledge/domains/storage/object-storage.md` documents strict named SHM allocation, real Arrow IPC, bounded codec helpers, native async I/O, and cancellation ownership. The original three-file consumer documentation manifest remains owned by `ch05:t02`: `docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`, and the storage knowledge chapter. The root consumer-guide manifest remains `[3/5 files documented]`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Verification |
| --- | --- | --- |
| R8 | [storage-completeness:ch04:t01](tasks/t01-strict-shm-and-arrow-ipc-cursor.md) | behavior_tdd; `src/rs/vertebra-db/src/arrow_shm.rs`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| R8 | [storage-completeness:ch04:t02](tasks/t02-streaming-arrow-parquet-storage.md) | behavior_tdd; `src/py/tests/unit/storage/test_storage_store.py` |
| R8 | [storage-completeness:ch04:t03](tasks/t03-streaming-arrow-parquet-writes.md) | behavior_tdd; `src/py/tests/unit/storage/test_storage_store.py` |
| R1, R8 | [storage-completeness:ch04:t04](tasks/t04-s3-multipart-minimum-parts.md) | behavior_tdd; actual S3 completion enforcing non-final minimum-part sizes |

## 5. Implementation Plan

- [x] [2e60f224] storage-completeness:ch04:t01 — Preserve ArrowCursor APIs while enforcing named SHM and real IPC (tasks/t01-strict-shm-and-arrow-ipc-cursor.md)
- [x] [ee35d9d4] storage-completeness:ch04:t02 — Read Arrow and Parquet through a bounded async codec bridge (tasks/t02-streaming-arrow-parquet-storage.md)
- [x] [78293774] storage-completeness:ch04:t03 — Write Arrow and Parquet with bounded multipart codec output (tasks/t03-streaming-arrow-parquet-writes.md)
- [~] storage-completeness:ch04:t04 — Coalesce native S3 multipart chunks into valid bounded parts (tasks/t04-s3-multipart-minimum-parts.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make docs` exits 0; correctness review precedes fresh quality review.

## 6. Current State & Remaining Plan Snapshot

- Three of four tasks are complete at their recorded source commits. Chapter state is `active` on t04, which enforces S3 minimum-part sizes through the existing native accumulator.
- Strict named allocation and real IPC pass locked Rust tests and Clippy, an all-five-extension rebuild, native Python cursor tests, and five-extension stubtest. The canonical storage suite passes 111 cases, including native HTTP GET/PUT cancellation barriers and worker-exit ownership checks; focused storage MyPy, Ruff, formatting, and whitespace checks pass.
- S3 successful completion under minimum-part enforcement and the full workspace recovery checkpoint remain unverified. Correctness review and fresh quality review follow t04 and the aggregate checkpoint.
- Chapter knowledge covers implemented allocation and codec behavior. Three tasks remain in the full plan: S3 multipart compliance in ch04, then SQLSpec codec delegation and the original consumer documentation manifest in ch05.

[^store]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Native storage facade.
[^pool]: `src/py/vertebra/utils/sync_tools.py::SyncPool` — Existing worker execution.
[^cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — IPC cursor and ownership.
