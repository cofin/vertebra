---
type: Task
id: "storage-completeness:ch04:t02"
chapter_id: ch04-arrow-shm-and-docs
title: Wire batch-streaming Arrow and Parquet helpers across SyncObjectStore, AsyncObjectStore, and SQLSpec
description: Implement batch-streaming Arrow IPC and Parquet read/write helpers on SyncObjectStore and AsyncObjectStore and non-buffering _iter_arrow_async in extensions/sqlspec/storage.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-completeness:ch04:t01"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/db/arrow.py
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 195
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
sources:
  - id: store-arrow-helpers
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore Arrow and Parquet helpers
  - id: sqlspec-arrow-iter
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend Arrow/Parquet streaming methods
  - id: arrow-cursor
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: ArrowCursor batch and IPC stream conversion
tags:
  - storage
  - arrow
  - parquet
  - sqlspec
  - streaming
---

# Task t02: Wire Batch-Streaming Arrow and Parquet Helpers Across `SyncObjectStore`, `AsyncObjectStore`, and SQLSpec

## Objective & Scope Boundary

Add batch-streaming Arrow IPC and Parquet read/write helpers on `SyncObjectStore` and `AsyncObjectStore` (`src/py/vertebra/storage/_store.py`) backed by `put_stream`, `get_stream`, and range-backed `SyncReadableFile` / `AsyncReadableFile`, and update `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` (`_iter_arrow_async` / `read_arrow_async` / `write_arrow_async`) so multi-batch Parquet and Arrow IPC datasets stream batch-by-batch without whole-table `to_pylist()` or full-file buffering [^store-arrow-helpers] [^sqlspec-arrow-iter] [^arrow-cursor].

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, and `AsyncReadableFile` [^store-arrow-helpers].
- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend` Arrow/Parquet methods [^sqlspec-arrow-iter].
- `src/py/vertebra/db/arrow.py` — `ArrowCursor` (`from_rows`, `as_bytes`, `to_arrow`) [^arrow-cursor].
- `src/py/tests/unit/storage/test_storage_store.py` — Storage Arrow/Parquet round-trip tests.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec storage Arrow streaming tests.

## Symbol & Signature Contracts

- `src/py/vertebra/storage/_store.py`:
  - `SyncObjectStore` & `AsyncObjectStore`:
    - `write_arrow(self, key: str, data: ArrowCursor | Any, *, batch_size: int = 65536) -> ObjectMetadata` (streams Arrow IPC chunks via `put_stream` with `content_type="application/vnd.apache.arrow.stream"`)
    - `iter_arrow_batches(self, key: str) -> Iterator[Any]` / `AsyncIterator[Any]` (reads Arrow IPC stream incrementally into `pyarrow.RecordBatch` items)
    - `write_parquet(self, key: str, batches: Any, *, schema: Any = None, compression: str = "zstd", row_group_size: int = 65536) -> ObjectMetadata` (streams encoded Parquet row-group buffers through a chunked sink writer into `put_stream` with `content_type="application/vnd.apache.parquet"`)
    - `iter_parquet_batches(self, key: str, *, batch_size: int = 65536, columns: Sequence[str] | None = None) -> Iterator[Any]` / `AsyncIterator[Any]` (uses `SyncReadableFile` / range-fetched row groups over `AsyncObjectStore` to yield `pyarrow.RecordBatch` items incrementally)
- `src/py/vertebra/extensions/sqlspec/storage.py`:
  - Wire `ObjectStoreBackend._iter_arrow_async` (and sync `_iter_arrow` / `write_arrow_async` where defined by SQLSpec `ObjectStoreProtocol`) to delegate directly to `AsyncObjectStore.iter_parquet_batches` / `iter_arrow_batches` (detecting `.parquet` vs `.arrow`/`.ipc` format) without materializing all batches into a `list` first.

## Edge Cases & Error Semantics

- Calling any Arrow/Parquet storage helper when `pyarrow` is uninstalled raises `MissingDependencyError("pyarrow", install_package="pyarrow", extra="arrow")`.
- Writing a 3-row-group Parquet dataset (`row_group_size=100`, 250 rows) and iterating via `iter_parquet_batches(..., batch_size=100, columns=["id"])` yields 3 `RecordBatch` instances containing only column `"id"` (`100`, `100`, `50` rows).
- Zero `asyncio.to_thread` calls in `src/py/vertebra/storage/_store.py`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_storage_store.py` and `src/py/tests/unit/extensions/test_data_storage_integrations.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Async Parquet multi-row-group projection | Write 250 rows (`id`, `payload`) via `await store.write_parquet("ds.parquet", table, row_group_size=100)`; read `store.iter_parquet_batches("ds.parquet", batch_size=100, columns=["id"])` | Yields 3 `pa.RecordBatch` objects (`100`, `100`, `50` rows) with `batch.schema.names == ["id"]` and exact IDs `0..249`. |
| Async Arrow IPC `ArrowCursor` round-trip | `cursor = ArrowCursor.from_rows([{"k": "a", "v": 1}, {"k": "b", "v": 2}])`; `await store.write_arrow("ds.arrow", cursor)` -> `iter_arrow_batches("ds.arrow")` | Yields valid `RecordBatch`es decoding to `[{"k": "a", "v": 1}, {"k": "b", "v": 2}]`. |
| SQLSpec `ObjectStoreBackend._iter_arrow_async` | Stream batches from `backend._iter_arrow_async("ds.parquet")` | Returns an async generator yielding `RecordBatch`es incrementally without pre-loading a list. |
| Missing `pyarrow` guard | Mask `pyarrow` unavailable and call `store.write_parquet("x.parquet", ...)` | Raises `MissingDependencyError`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -k "arrow or parquet" -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [ ] `SyncObjectStore` and `AsyncObjectStore` stream Arrow IPC and Parquet batches via `put_stream`, `get_stream`, and range reads with zero `asyncio.to_thread` calls (R8).
- [ ] `ObjectStoreBackend._iter_arrow_async` yields `RecordBatch` items lazily without whole-dataset list materialization (R4, R8).

## Notes & Discoveries

- [2026-11-07 04:15] Parquet footer metadata resides in the trailing 8 bytes (`footer_len` + `PAR1`); `AsyncObjectStore.get_range` reads footer + selected row-group byte ranges cleanly without blocking.

[^store-arrow-helpers]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Object store Arrow and Parquet streaming methods.
[^sqlspec-arrow-iter]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec Arrow/Parquet storage integration.
[^arrow-cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Arrow IPC cursor primitive.
