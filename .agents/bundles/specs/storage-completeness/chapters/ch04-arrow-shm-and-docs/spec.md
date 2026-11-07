---
type: Spec
parent_flow: storage-completeness
chapter_id: ch04-arrow-shm-and-docs
title: "Chapter 04: Strict Arrow SHM Allocation, Parquet Streaming, and OKF Sync"
description: Enforce fail-loud named ShmRegion allocation, eliminate JSON pseudo-Arrow fallback in ArrowCursor.from_rows, wire streaming Arrow/Parquet ObjectStore helpers, and synchronize storage documentation and OKF chapters.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-native-engine-and-auth
  - ch02-orm-and-sql-storage-contracts
  - ch03-litestar-and-range-streaming
replan_trigger: null
current_task: "storage-completeness:ch04:t01"
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: Named ShmRegion allocator for VTBARROW zero-copy frames
  - id: db-arrow
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Python ArrowCursor IPC wrapper and from_rows constructor
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: OKF domain knowledge chapter for Vertebra object storage
tags:
  - storage
  - arrow
  - parquet
  - shm
  - docs
  - okf
---

# Chapter 04: Strict Arrow SHM Allocation, Parquet Streaming, and OKF Sync

<!-- truth: start -->
- **Fail-Loud Named SHM Allocation**: On Unix, `vertebra_db::arrow_shm::allocate_shm_region` returns `Err(ArrowShmError)` immediately when `ShmRegion::create_named` fails instead of falling back to an anonymous `ShmRegion::create` segment unreachable by peer workers [^arrow-shm].
- **Real Arrow IPC Only**: `ArrowCursor.from_rows` always writes valid Arrow IPC stream bytes and raises `MissingDependencyError` when `pyarrow` is unavailable, removing the `encode_json_bytes` pseudo-Arrow fallback [^db-arrow].
- **Streaming Parquet/Arrow & OKF Parity**: `SyncObjectStore`, `AsyncObjectStore`, and `ObjectStoreBackend._iter_arrow_async` stream Arrow IPC and Parquet record batches over `SyncReadableFile` / `AsyncReadableFile` and `put_stream` without whole-table list materialization, and usage docs plus `object-storage.md` reflect the shipped contracts [^okf-storage].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` silently falls back to `ShmRegion::create` on `ShmRegion::create_named` failure, creating an anonymous region that cross-process `VTBARROW` readers cannot open by name [^arrow-shm]. In Python, `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows` falls back to `encode_json_bytes(dict_list)` when `pyarrow` is absent, corrupting callers expecting Arrow IPC framing [^db-arrow]. Finally, Arrow/Parquet object-storage helpers require batch-streaming integration over `SyncReadableFile` / `AsyncReadableFile` and `put_stream`, and `docs/usage/storage*.md` plus `.agents/bundles/knowledge/domains/storage/object-storage.md` require final synchronization across `ch02`–`ch04` framework and Arrow/Parquet contracts [^okf-storage].

### 1.2 Non-Goals

- Broker cross-worker `VTBARROW` RPC framing for SQLSpec adapters (owned by `broker-pool-sharing` and `sqlspec-adapter-parity`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); depends on Chapters 01, 02, and 03. Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) and [object-storage.md](../../../../knowledge/domains/storage/object-storage.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8.1 (`ch04:t01`)**: Introduce `pub type ArrowShmError = String` (or typed `ArrowShmError` implementing `Display` + `Error` convertible to `String`) in `src/rs/vertebra-db/src/arrow_shm.rs` and make `allocate_shm_region` return `Err(...)` whenever `ShmRegion::create_named` fails on `#[cfg(unix)]`; in `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows`, delete the `encode_json_bytes` fallback so `from_rows` emits valid Arrow IPC stream bytes (even for empty `[]` with explicit `schema` or empty struct stream) and raises `MissingDependencyError("pyarrow", install_package="pyarrow", extra="arrow")` when `pyarrow` is uninstalled.
- **R8.2 (`ch04:t02`)**: Add batch-streaming `write_arrow`, `read_arrow_batches`, `write_parquet`, and `read_parquet_batches` on `SyncObjectStore` and `AsyncObjectStore` (`src/py/vertebra/storage/_store.py`) backed by `put_stream` and `SyncReadableFile` / `AsyncReadableFile`, and wire `ObjectStoreBackend._iter_arrow_async` in `src/py/vertebra/extensions/sqlspec/storage.py` to yield Arrow batches incrementally without buffering whole files in memory.
- **R8.3 (`ch04:t03`)**: Update `docs/usage/storage.md` and `docs/usage/storage-auth-and-signing.md` and extend `.agents/bundles/knowledge/domains/storage/object-storage.md` (which already documents `ch01` `RustTaskAwaitable`, `notify.rs` provenance, unified `vertebra._vertebra` GCP auth, and fail-closed construction) with `ch02`–`ch04` ORM/Litestar contracts and Arrow/Parquet batch streaming.

### 2.2 Edge-Case & Failure-Mode Matrix

| Boundary | Deterministic Behavior |
| --- | --- |
| Invalid or uncreatable POSIX SHM name in `allocate_shm_region` | Returns `Err` containing normalized segment name and OS error; zero anonymous fallback. |
| `ArrowCursor.from_rows(rows)` when `pyarrow` is missing | Raises `MissingDependencyError` rather than storing JSON bytes inside `_ipc_bytes`. |
| `ObjectStoreBackend._iter_arrow_async` over multi-row-group Parquet object | Yields one `pyarrow.RecordBatch` per chunk/row-group via `AsyncReadableFile` range reads without reading entire object into a single `bytes` object when seekable metadata is used. |

### 2.3 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| `ArrowCursor.from_rows` without `pyarrow` | Fail closed with `MissingDependencyError` | Encoding JSON bytes into `ArrowCursor` — violates Arrow IPC wire invariant (`ARROW1` / continuation marker) and breaks downstream `to_arrow()` / `VTBARROW` consumers. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/3 files documented]` (`object-storage.md` documents `ch01`; `storage-completeness:ch04:t03` completes user guides and extends `object-storage.md` for `ch02`–`ch04`):

* `docs/usage/storage.md` — Streaming `get_stream`/`put_stream`, conditional `put`, pagination, `rename`, fail-closed config, Litestar `Store`/static/Range streaming, and Arrow/Parquet helpers.
* `docs/usage/storage-auth-and-signing.md` — Unified `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}` and GCS/S3/Azure/OCI presigning.
* `.agents/bundles/knowledge/domains/storage/object-storage.md` — Extend existing `ch01` chapter with `ch02`–`ch04` SQLSpec/Alchemy/Django/Litestar contracts and Arrow/Parquet batch streaming.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R8.1 | [t01](tasks/t01-strict-shm-and-arrow-ipc-cursor.md) | `behavior_tdd` | `src/rs/vertebra-db/src/arrow_shm.rs`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| R8.2 | [t02](tasks/t02-streaming-arrow-parquet-storage.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py`, `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R8.3 | [t03](tasks/t03-storage-docs-and-okf-sync.md) | `documentation_validation` | `src/py/tests/unit/tools/test_docs_tooling.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch04:t01 — Enforce fail-loud allocate_shm_region and valid Arrow IPC in ArrowCursor.from_rows (tasks/t01-strict-shm-and-arrow-ipc-cursor.md)
- [ ] storage-completeness:ch04:t02 — Wire batch-streaming Arrow and Parquet helpers across SyncObjectStore, AsyncObjectStore, and SQLSpec (tasks/t02-streaming-arrow-parquet-storage.md)
- [ ] storage-completeness:ch04:t03 — Synchronize storage usage guides and OKF object-storage knowledge chapter (tasks/t03-storage-docs-and-okf-sync.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make docs` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ cdb978d3`; `depends_on_chapters: [ch01, ch02, ch03]`, `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Documentation & OKF Status**: `[1/3 files documented]` (`object-storage.md` updated for `ch01`; remaining `2` user guides + `ch02`–`ch04` OKF extensions completed in `t03` prior to final flow review).

[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named shared-memory segment allocator for Arrow IPC frames.
[^db-arrow]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Python Arrow IPC result wrapper.
[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — OKF storage domain knowledge chapter.
