---
type: Spec
parent_flow: storage-completeness
chapter_id: ch05-sqlspec-arrow-and-docs
title: 'Chapter 05: SQLSpec Batch Consumers and Storage Documentation'
description: R4/R8 integrate completed batch helpers into installed SQLSpec public
  protocol methods and synchronize the original storage/auth/OKF documentation manifest.
  SQLSpec full-Table reads retain their public return contract; streaming consumers
  remain incremental.
status: stable
state: active
planning_status: complete
depends_on_chapters:
- ch04-arrow-shm-and-docs
replan_trigger: null
current_task: storage-completeness:ch05:t01
plan_revision: 10
plan_commit: '78293774'
state_revision: 1
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T18:01:58.609035Z'
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
# Chapter 05: SQLSpec Batch Consumers and Storage Documentation

<!-- truth: start -->
- Native asynchronous storage I/O remains on the caller loop; existing SyncPool executes synchronous PyArrow codecs [^pool].
- Public ArrowCursor methods and native buffer ownership remain stable while required named allocations fail closed [^cursor].
- Storage batch methods and SQLSpec consumers retain bounded demand by default and deterministic cancellation cleanup [^store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

R4/R8 integrate completed batch helpers into installed SQLSpec public protocol methods and synchronize the original storage/auth/OKF documentation manifest. SQLSpec full-Table reads retain their public return contract; streaming consumers remain incremental.

Plan baseline is `78293774`. This chapter depends on ch04-arrow-shm-and-docs. The native storage facade supplies stream/range I/O and multipart abort; PyArrow 26.0.1 supplies synchronous callbacks, dictionary memo, and codec operations. Sources below anchor the public seams. Skills are local runtime-ipc and pyo3-bridge for allocation/lifetime changes, python-framework for facade contracts, and review-tests for public behavior evidence.

## 2. Requirements, Concrete Contracts & Architecture

Preserve ObjectStoreBackend read_arrow_sync/async, write_arrow_sync/async, write_arrow_cursor_sync/async and stream_arrow_sync/async signatures. stream_arrow_async returns AsyncIterator directly; preserve namespace/glob resolution, explicit file_format precedence, batch_size, projection, and accepted codec kwargs. `_iter_arrow_async` delegates native async batch helpers without whole-list collection. Full-Table methods may materialize their required Table return but do not stage full encoded objects or convert through row dictionaries. Documentation records actual provider signing, token cache exports, framework integration and the shipped Arrow method names.

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

Progress: `[1/3 files documented]`. ch05:t02 owns docs/usage/storage.md, docs/usage/storage-auth-and-signing.md, and .agents/bundles/knowledge/domains/storage/object-storage.md. Product tech-stack typing documentation remains complete and unchanged. Read docs/usage/storage-parquet-and-orm.md for cross-link consistency; canonical docs/tooling and review-check gates validate the three owned files.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Verification |
| --- | --- | --- |
| R8/R4 | [storage-completeness:ch05:t01](tasks/t01-sqlspec-arrow-stream-integration.md) | behavior_tdd; `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R8/R4 | [storage-completeness:ch05:t02](tasks/t02-storage-docs-and-okf-sync.md) | documentation_validation; `src/py/tests/unit/tools/test_docs_tooling.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch05:t01 — Integrate bounded batches with SQLSpec public storage methods (tasks/t01-sqlspec-arrow-stream-integration.md)
- [ ] storage-completeness:ch05:t02 — Synchronize storage usage guides and OKF object-storage knowledge chapter (tasks/t02-storage-docs-and-okf-sync.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make docs` exits 0; correctness review precedes fresh quality review.

## 6. Current State & Remaining Plan Snapshot

- Plan baseline: `78293774`, revision 10; 2 pending tasks and no completed implementation claimed.
- Dependency tasks finish before dependent tasks begin; every worksheet owns one isolated execution and atomic commit.
- Documentation remains explicitly tracked in ch05:t02; no original R8 requirement is deferred.

[^store]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Native storage facade.
[^pool]: `src/py/vertebra/utils/sync_tools.py::SyncPool` — Existing worker execution.
[^cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — IPC cursor and ownership.
