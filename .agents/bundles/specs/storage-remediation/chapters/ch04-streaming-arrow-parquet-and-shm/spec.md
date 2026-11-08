---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch04-streaming-arrow-parquet-and-shm
title: "Chapter 04: Streaming HTTP, Parquet/Arrow Options, and Zero-Copy SHM Arrow"
description: Stream StreamingStorageResponse.iter_chunks() and range-slice to_fastpath_tuple(), enforce storage/_arrow.py module layout, __slots__, zero-copy readinto, Buffer IPC writer support, and Parquet/Arrow encoding option passthrough (use_dictionary, use_byte_stream_split, options), and make read_arrow_shm zero-copy over memoryview with lazy ArrowCursor.__len__ and columnar _table_to_rows_and_cols.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch03-bulk-file-io-compose-and-streams
replan_trigger: null
current_task: "storage-remediation:ch04:t01"
plan_revision: 1
plan_commit: fd571e3a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T20:55:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: py-streaming
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: HTTP 200/206/416 streaming storage response
  - id: py-storage-arrow
    resource: src/py/vertebra/storage/_arrow.py::iter_batches_sync
    title: Bounded Arrow IPC and Parquet storage readers and writers
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore Parquet/Arrow methods
  - id: py-db-arrow
    resource: src/py/vertebra/db/arrow.py::read_arrow_shm
    title: Shared-memory VTBARROW hydration and columnar row helpers
tags:
  - storage
  - streaming
  - arrow
  - parquet
  - shm
---

# Chapter 04: Streaming HTTP, Parquet/Arrow Options, and Zero-Copy SHM Arrow

<!-- truth: start -->
- **True Streaming `StreamingStorageResponse`**: `iter_chunks(chunk_size=None)` streams full `200` responses via `store.get_stream` and `206` byte-range responses via bounded `store.get_range` windows yielding `memoryview` slices without materializing the entire object via `get_view()`; `to_fastpath_tuple()` fetches only the requested `get_range` slice on `206` responses while preserving `list[tuple[bytes, bytes]]` headers [^py-streaming].
- **Clean `storage/_arrow.py` Layout, Zero-Copy `readinto`, & Parquet Encoding Options**: `storage/_arrow.py` declares explicit `__all__`, places public functions (`iter_batches_sync`, `iter_batches_async`, `read_table_sync`, `read_table_async`, `write_batches_sync`, `write_batches_async`) above private `_...` classes/helpers (`_CodecBridge`, `_RangeFile`, `_StreamFile`, `_BatchReader`, `_OutputSink`, `_BatchWriter`), adds `__slots__`, copies directly into caller `readinto` buffers without extra intermediate `bytes` copies, preserves demand-driven row-group range reads (`pre_buffer=False` on `iter_batches_*`, `pre_buffer=True` default on materialized `read_table_*` unless overridden in `options`), accepts generic `Buffer` IPC streams in `_BatchWriter.open`, and wires `use_dictionary`, `use_byte_stream_split`, and `options` through `SyncObjectStore` and `AsyncObjectStore` [^py-storage-arrow] [^py-store].
- **Zero-Copy `read_arrow_shm` & Columnar `_table_to_rows_and_cols`**: `read_arrow_shm(region, length=None)` auto-detects the 16-byte `VTBARROW` frame header vs offset-0 IPC streams over `pyarrow.py_buffer(payload_view)` without `bytes(...)` copying or eager `to_pylist()` hydration, `ArrowCursor.__len__` inspects `_rows`, `_shm_cursor`, or Arrow IPC batch metadata without populating `_rows`, and `_table_to_rows_and_cols` transposes `to_pydict()` columns (`zip(*col_lists)`) on Arrow tables while preserving `(list[tuple[Any, ...]], tuple[str, ...])` [^py-db-arrow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Three hot-path serialization and streaming modules still copy or buffer full payloads unnecessarily:
1. `StreamingStorageResponse.iter_chunks()` and `to_fastpath_tuple()` call `self.store.get_view(self.key)` and slice in memory even for `206 Partial Content` byte ranges or multi-hundred-megabyte downloads [^py-streaming].
2. `src/py/vertebra/storage/_arrow.py` lacks `__all__`, defines private `_...` classes (`_CodecBridge`, `_RangeFile`, `_StreamFile`, `_BatchReader`, `_OutputSink`, `_BatchWriter`) above public functions (`iter_batches_*`, `read_table_*`, `write_batches_*`), omits `__slots__`, allocates extra intermediate `bytes` in `_StreamFile.readinto`, lacks direct `Buffer` IPC stream ingestion in `_BatchWriter.open` (forcing `sqlspec/storage.py` to carry `_IPCBuffer`), and omits `use_dictionary`, `use_byte_stream_split`, and `options` passthrough on `write_parquet` / `iter_parquet_batches` / `write_arrow` / `iter_arrow_batches` [^py-storage-arrow] [^py-store].
3. `src/py/vertebra/db/arrow.py::read_arrow_shm` copies `bytes(raw_view)` and eagerly materializes `tbl.to_pylist()`, `ArrowCursor.__len__` returns `0` for inline IPC buffers unless rows were eagerly materialized, and `_table_to_rows_and_cols` allocates a dict per row via `data.to_pylist()` [^py-db-arrow].

**Non-Goals**:
- Framework extension storage adapters (Chapter 05).

### 1.2 Upstream Execution Baseline

- Depends on `ch03-bulk-file-io-compose-and-streams`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| HTTP Streaming Response | `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` [^py-streaming] | In-process/local-substitutable HTTP range and chunk iterator over `SyncObjectStore`. |
| Parquet & Arrow Storage | `src/py/vertebra/storage/_arrow.py::iter_batches_sync` [^py-storage-arrow], `src/py/vertebra/storage/_store.py::SyncObjectStore` [^py-store] | Public `write_parquet`, `iter_parquet_batches`, `write_arrow`, and `iter_arrow_batches` methods on sync/async stores. |
| SHM Arrow Hydration | `src/py/vertebra/db/arrow.py::read_arrow_shm` [^py-db-arrow] | In-process shared-memory `VTBARROW` IPC reader and `ArrowCursor`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`object-storage.md`](../../../../knowledge/domains/storage/object-storage.md), [`module-and-class-layout.md`](../../../../knowledge/patterns/python/module-and-class-layout.md), [`python-framework`](../../../../../skills/python-framework/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R4.1 (`t01`)**: Update `StreamingStorageResponse.iter_chunks(self, chunk_size: int | None = None) -> Iterator[memoryview]` to stream `200` responses via `self.store.get_stream(self.key, chunk_size=step)` and `206` responses via bounded `self.store.get_range` windows (`<= step`), and update `to_fastpath_tuple()` (`tuple[int, list[tuple[bytes, bytes]], bytes | memoryview]`) to call `self.store.get_range(self.key, start, end - start)` on `206` responses.
- **R4.2 (`t02`)**: Reorganize `src/py/vertebra/storage/_arrow.py` (`__all__`, public functions above private `_...` classes/helpers, `__slots__`, zero-copy `_StreamFile.readinto` over `memoryview(buffer).cast("B")`, `pre_buffer=False` preserved on streaming `iter_batches_*` and `pre_buffer=True` defaulted on materialized `read_table_*`, raw `Buffer` IPC support in `_BatchWriter.open`), and wire `use_dictionary: bool | Sequence[str] = True`, `use_byte_stream_split: bool | Sequence[str] = False`, and `options: Mapping[str, Any] | None = None` on `write_parquet` plus `options: Mapping[str, Any] | None = None` on `iter_parquet_batches`, `write_arrow`, and `iter_arrow_batches` in `_store.py`.
- **R4.3 (`t03`)**: Update `src/py/vertebra/db/arrow.py` so `read_arrow_shm(region: Any, length: int | None = None) -> ArrowCursor` auto-detects `VTBARROW` vs raw IPC over `pa.py_buffer(payload_view)` with an `_shm_anchor` slot and zero `bytes(...)` copy, `ArrowCursor.__len__` computes row count lazily without populating `_rows`, and `_table_to_rows_and_cols` converts Arrow `to_pydict()` columns directly to `(list[tuple[Any, ...]], tuple[str, ...])`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- `SyncObjectStore.write_parquet` / `AsyncObjectStore.write_parquet`:
  `(self, key: str, batches: Any, *, schema: Any = None, compression: str = "zstd", row_group_size: int = 65536, use_dictionary: bool | Sequence[str] = True, use_byte_stream_split: bool | Sequence[str] = False, options: Mapping[str, Any] | None = None) -> PutResult`
- `iter_parquet_batches(self, key: str, *, batch_size: int = 65536, columns: Sequence[str] | None = None, options: Mapping[str, Any] | None = None)`
- `write_arrow(self, key: str, data: Any, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None) -> PutResult`
- `iter_arrow_batches(self, key: str, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None)`

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `StreamingStorageResponse.iter_chunks` -> `store.get_stream` (`200`) / `store.get_range` (`206`).
- `store.write_parquet(..., use_dictionary=False, use_byte_stream_split=["embedding"])` -> `write_batches_sync` / `write_batches_async` -> `pq.ParquetWriter(..., use_dictionary=..., use_byte_stream_split=..., **extra_opts)`.
- `read_arrow_shm(shm_region)` -> `pa.ipc.open_stream(pa.py_buffer(payload_slice))` -> `ArrowCursor`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| `416` or empty `0`-byte `StreamingStorageResponse` | `status_code == 416` or `total_size == 0` | `iter_chunks()` yields nothing (`b""` in `to_fastpath_tuple()`) without calling `get_range` | RFC 9110 range semantics. |
| High-cardinality float/UUID Parquet write | `write_parquet("f.parquet", table, use_dictionary=["category"], use_byte_stream_split=["score"])` | Parquet column metadata reflects plain + `BYTE_STREAM_SPLIT` on `"score"` and `RLE_DICTIONARY` on `"category"` | Column-level Parquet encoding control. |
| `len(arrow_cursor)` before row materialization | `cursor = read_arrow_shm(region); len(cursor)` | Returns `num_rows` immediately with `cursor._rows == []` | Zero eager Python tuple/dict materialization. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** a `StreamingStorageResponse` for `Range: bytes=100-199` on a `10 MiB` object, **When** `list(resp.iter_chunks(chunk_size=64))` and `resp.to_fastpath_tuple()` execute, **Then** `store.get_view` is never called and only `100` bytes are fetched via `get_range`.
2. **Given** a `pyarrow.Table` with float and string columns, **When** `store.write_parquet("t.parquet", table, use_dictionary=False, use_byte_stream_split=["val"])` runs, **Then** `pq.read_metadata` confirms `BYTE_STREAM_SPLIT` encoding and disabled dictionary encoding, and `iter_parquet_batches` streams batches on demand over the `4 MiB` read-ahead `open_reader`.

### 2.6 Alternatives Considered

- **Chosen**: Explicit `use_dictionary` and `use_byte_stream_split` keyword parameters alongside an open `options: Mapping[str, Any] | None = None` passthrough dict.
- **Rejected**: Hiding common Parquet column encodings exclusively inside untyped `**kwargs` (loses IDE autocompletion and static type-checking on the two primary Parquet encoding knobs).

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch04]` (consolidated in Chapter 06 `ch06:t01` and `ch06:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test |
| --- | --- | --- | --- |
| R4.1 | `ch04 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R4.2 | `ch04 / t02` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R4.3 | `ch04 / t03` | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |

## 5. Implementation Plan

- [ ] storage-remediation:ch04:t01 — Stream `StreamingStorageResponse.iter_chunks()` and range-slice `to_fastpath_tuple()` without full-blob materialization (tasks/t01-streaming-storage-response.md)
- [ ] storage-remediation:ch04:t02 — Reorganize `storage/_arrow.py` layout, `__slots__`, `readinto`, and `Buffer` IPC support, and wire Parquet/Arrow encoding options (tasks/t02-arrow-parquet-layout-and-options.md)
- [ ] storage-remediation:ch04:t03 — Make `read_arrow_shm` zero-copy over `memoryview`, keep `ArrowCursor.__len__` lazy, and transpose `_table_to_rows_and_cols` column-wise (tasks/t03-zero-copy-shm-arrow-cursor.md)
- **Chapter / Phase Recovery Checkpoint**: `uv run --no-sync ruff check src/py/vertebra/storage src/py/vertebra/db/arrow.py && uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/db/arrow.py && uv run --no-sync pytest src/py/tests/unit/storage src/py/tests/unit/db/test_enterprise_olap_drivers.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `fd571e3a` — `0/3` tasks completed in `ch04`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`.
- **Remaining Chapters**: `ch01`–`ch06`.

[^py-streaming]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — HTTP `200`/`206`/`416` streaming storage response.
[^py-storage-arrow]: `src/py/vertebra/storage/_arrow.py::iter_batches_sync` — Bounded Arrow IPC and Parquet storage readers and writers.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — `SyncObjectStore` and `AsyncObjectStore` Parquet/Arrow methods.
[^py-db-arrow]: `src/py/vertebra/db/arrow.py::read_arrow_shm` — Shared-memory `VTBARROW` hydration and columnar row helpers.
