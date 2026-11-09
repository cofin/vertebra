---
type: Task
id: "storage-remediation:ch04:t02"
chapter_id: ch04-streaming-arrow-parquet-and-shm
title: Reorganize storage/_arrow.py layout, __slots__, readinto, and Buffer IPC support, and wire Parquet/Arrow encoding options
description: Enforce __all__, public-first layout, __slots__, zero-copy _StreamFile.readinto, and raw Buffer IPC support in storage/_arrow.py, and wire use_dictionary, use_byte_stream_split, and options through SyncObjectStore and AsyncObjectStore.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch04:t01"
read_set:
  - src/py/vertebra/storage/_arrow.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_storage_store.py
files:
  - src/py/vertebra/storage/_arrow.py
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 215
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T00:56:40Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T01:03:30Z"
last_verified_commit: f4606043
verification_evidence: "pytest src/py/tests/unit/storage + test_data_storage_integrations.py (395 passed); ruff check/format and mypy on src/py/vertebra/storage clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:03:30Z"
commit: f4606043
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T01:03:30Z"
    commit: f4606043
sources:
  - id: py-storage-arrow
    resource: src/py/vertebra/storage/_arrow.py::iter_batches_sync
    title: Arrow IPC and Parquet streaming readers and writers
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore.write_parquet
    title: SyncObjectStore and AsyncObjectStore Arrow/Parquet methods
tags:
  - storage
  - arrow
  - parquet
  - performance
  - tdd
---

# Task t02: `storage/_arrow.py` Module Layout, Zero-Copy `readinto`, `Buffer` IPC Ingestion, and Parquet/Arrow Options

## Objective & Scope Boundary

Update `src/py/vertebra/storage/_arrow.py` [^py-storage-arrow] and `src/py/vertebra/storage/_store.py` [^py-store] (`2` files):
1. Add explicit `__all__ = ("iter_batches_async", "iter_batches_sync", "read_table_async", "read_table_sync", "write_batches_async", "write_batches_sync")` to `storage/_arrow.py`, move all public functions strictly above private `_...` classes/helpers (`_CodecBridge`, `_RangeFile`, `_StreamFile`, `_BatchReader`, `_OutputSink`, `_BatchWriter`, `_validate_batch_size`, `_read_table`) (`PY-01`), add `__slots__` to `_CodecBridge`, `_BatchReader`, and `_BatchWriter` (plus `_RangeFile`, `_StreamFile`, `_OutputSink`), copy chunks directly into `memoryview(buffer).cast("B")` inside `_StreamFile.readinto` without allocating an intermediate `bytearray`/`bytes`, preserve `pre_buffer=False` on demand-driven `iter_batches_*` while defaulting `pre_buffer=True` on materialized Parquet `read_table_*` (overridable via `options`), and accept raw `collections.abc.Buffer` IPC streams directly in `_BatchWriter.open` (so `sqlspec/storage.py` can delete `_IPCBuffer` in `ch05:t01`).
2. Add `use_dictionary: bool | Sequence[str] = True`, `use_byte_stream_split: bool | Sequence[str] = False`, and `options: Mapping[str, Any] | None = None` to `SyncObjectStore.write_parquet` and `AsyncObjectStore.write_parquet`, and add `options: Mapping[str, Any] | None = None` to `iter_parquet_batches`, `write_arrow`, and `iter_arrow_batches` on `SyncObjectStore` and `AsyncObjectStore` (forwarding merged options cleanly to `iter_batches_*` and `write_batches_*`).

Out of scope: `src/py/vertebra/db/arrow.py` (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — `iter_batches_sync`, `iter_batches_async`, `read_table_sync`, `read_table_async`, `write_batches_sync`, `write_batches_async`, `_RangeFile`, `_StreamFile`, `_BatchReader`, `_OutputSink`, `_BatchWriter` [^py-storage-arrow].
- `src/py/vertebra/storage/_store.py` — `write_parquet`, `iter_parquet_batches`, `write_arrow`, `iter_arrow_batches` on `SyncObjectStore` and `AsyncObjectStore` [^py-store].
- `src/py/tests/unit/storage/test_storage_store.py` — Parquet and Arrow unit tests.

## Symbol & Signature Contracts

- `write_parquet(self, key: str, batches: Any, *, schema: Any = None, compression: str = "zstd", row_group_size: int = 65536, use_dictionary: bool | Sequence[str] = True, use_byte_stream_split: bool | Sequence[str] = False, options: Mapping[str, Any] | None = None) -> PutResult`
- `iter_parquet_batches(self, key: str, *, batch_size: int = 65536, columns: Sequence[str] | None = None, options: Mapping[str, Any] | None = None)`
- `write_arrow(self, key: str, data: Any, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None) -> PutResult`
- `iter_arrow_batches(self, key: str, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None)`

## Edge Cases & Error Semantics

- Passing `pyarrow.py_buffer(ipc_bytes)` or `memoryview(ipc_bytes)` directly into `write_batches_sync` / `write_parquet` opens the Arrow IPC stream via `pa.ipc.open_stream(pa.py_buffer(data))` without requiring a `.buffer_view()` wrapper.
- Passing conflicting keys in `options` (e.g., `options={"compression": "snappy"}`) lets explicit `options` override defaults cleanly without `TypeError: got multiple values for keyword argument`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Parquet `use_dictionary=False` & `use_byte_stream_split=["metric"]` | `pyarrow.Table` with `id: int64`, `tag: string`, `metric: float64` written via sync & async `write_parquet` | `pq.read_metadata` shows `"BYTE_STREAM_SPLIT"` in `metric` encodings and round-trips exact rows via `iter_parquet_batches`. |
| Arrow IPC compression `options={"compression": "zstd"}` | `store.write_arrow("t.arrow", table, options={"compression": "zstd"})` -> `iter_arrow_batches("t.arrow")` | Round-trips compressed IPC stream cleanly via `pa.ipc.IpcWriteOptions(compression="zstd")`. |
| Raw `Buffer` IPC payload into `write_batches_sync` | Serialized IPC `pa.Buffer` passed directly to `store.write_parquet("from_buf.parquet", ipc_buf)` | Decodes IPC stream and writes valid Parquet output. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_storage_store.py
uv run --no-sync mypy src/py/vertebra/storage/_arrow.py src/py/vertebra/storage/_store.py
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -k "parquet or arrow" -q
```

## Acceptance Criteria

- [x] [f4606043] `storage/_arrow.py` declares `__all__`, defines public functions before private helpers/classes, uses `__slots__`, copies directly in `_StreamFile.readinto`, preserves demand-driven row-group reads on `iter_batches_*`, and supports raw `Buffer` IPC inputs in `_BatchWriter.open` (R4.2).
- [x] [f4606043] `use_dictionary`, `use_byte_stream_split`, and `options` work across sync and async `write_parquet`, `iter_parquet_batches`, `write_arrow`, and `iter_arrow_batches` (R4.2).

## Notes & Discoveries

- [2026-11-08 20:55] Zero inline `#` comments in Python; document all behavior in Google-style docstrings.

[^py-storage-arrow]: `src/py/vertebra/storage/_arrow.py::iter_batches_sync` — Arrow IPC and Parquet streaming readers and writers.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore.write_parquet` — `SyncObjectStore` and `AsyncObjectStore` Arrow/Parquet methods.
