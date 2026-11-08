---
type: Task
id: storage-completeness:ch04:t03
chapter_id: ch04-arrow-shm-and-docs
title: Write Arrow and Parquet with bounded multipart codec output
description: Add bounded sync/async Arrow IPC and Parquet writers using the reader bridge lifecycle primitives and native put_stream. Keep encoded output incremental and native multipart abort semantics intact.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch04:t02
read_set:
- src/py/vertebra/storage/_arrow.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/utils/sync_tools.py
- src/py/vertebra/db/arrow.py
- src/py/tests/unit/storage/test_storage_store.py
files:
- src/py/vertebra/storage/_arrow.py
- src/py/vertebra/storage/_store.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 220
plan_revision: 9
plan_commit: 222cbf99
state_revision: 2
claimed_by: agent:arrow-writer-executor
claimed_at: '2026-11-08T17:06:25.072586Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T17:20:39.168001Z'
last_verified_commit: '78293774'
verification_evidence:
  red:
    exit_code: 1
    result: Both public format roundtrips fail because write_arrow and write_parquet are absent.
  green:
    command: UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q
    exit_code: 0
    result: 111 passed, including 44 writer cases and real native S3 PUT cancellation barriers for both formats.
  typing:
    command: UV_NO_SYNC=1 uv run --no-sync mypy src/py/vertebra/storage
    exit_code: 0
    result: 4 source files pass.
  lint:
    command: UV_NO_SYNC=1 uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_storage_store.py
    exit_code: 0
  format_diff:
    exit_code: 0
    result: Focused Ruff format and git diff whitespace checks pass.
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T17:20:39.168001Z'
commit: '78293774'
sources:
- id: store
  resource: src/py/vertebra/storage/_store.py::AsyncObjectStore.put_stream
  title: Native multipart lifetime
- id: pool
  resource: src/py/vertebra/utils/sync_tools.py::SyncPool
  title: Codec execution
tags:
- storage
- arrow
---
# Task t03: Write Arrow and Parquet with bounded multipart codec output

## Objective & Scope Boundary

Add bounded sync/async Arrow IPC and Parquet writers using the reader bridge lifecycle primitives and native put_stream. Keep encoded output incremental and native multipart abort semantics intact.

The implementation uses src/py/vertebra/storage/_store.py::AsyncObjectStore.put_stream [^store].

The implementation uses src/py/vertebra/utils/sync_tools.py::SyncPool [^pool].

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/storage/_store.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/utils/sync_tools.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/db/arrow.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owning implementation, collaborator contract, or canonical verification surface.

## Symbol & Signature Contracts

- Add `write_arrow(self, key: str, data: Any, *, batch_size: int = 65536) -> PutResult` and async equivalent; add `write_parquet(self, key: str, batches: Any, *, schema: Any = None, compression: str = "zstd", row_group_size: int = 65536) -> PutResult` and async equivalent.
- Accept Table, RecordBatch, ArrowCursor, and synchronous RecordBatch iterable. ArrowCursor decoding uses buffer_view with one persistent IPC reader, never to_pylist. Table.to_batches(max_chunksize=...) is input already owned by caller; process batches lazily. For empty iterable require schema; a Table supplies schema even when empty. Infer schema from first nonempty available batch; validate subsequent batch schema using PyArrow.
- `_arrow.py` adds an append-only sink implementing write(data)->int, tell()->int, writable()->True, closed/close, and drain()->list[Buffer]. Drain current encoded batch output after each writer operation; maintain absolute tell independently of drained buffers. Bytes handed to multipart remain owned until native write_part completion.
- Caller -> sync/async write helper -> incremental encoder -> store.put_stream(content_type=...) -> native multipart session. IPC media type is application/vnd.apache.arrow.stream; Parquet is application/vnd.apache.parquet. Use persistent ipc.new_stream or pq.ParquetWriter and emit final EOS/footer on close.
- Async encoding init/write_batch/close runs through the existing SyncPool cancellation-safe one-operation mechanism from t02. The async chunk iterator yields all drained buffers before advancing input. Split Table batches to row_group_size for predictable 100/100/50 rowgroups. Never accumulate whole-object BufferOutputStream or all batches.
- Codec failure/cancellation closes the encoder and input iterator, cancels/waits any active worker before dropping its sink, and propagates the original exception through put_stream so multipart abort runs. Do not emit successful finalization after failure. Sync helper mirrors cleanup with finally.

- Private helpers `write_batches_sync(store, key, data, *, file_format: str, batch_size: int = 65536, schema: Any = None, options: Mapping[str, Any] | None = None) -> PutResult` and async `write_batches_async` accept SQLSpec codec options. Public stores delegate their declared parameters; SQLSpec uses these helpers to forward its existing kwargs without adding public storage parameters. Route ParquetWriter constructor options separately from row_group_size on write_batch; IPC writer options go to ipc.new_stream. Unsupported options raise their original TypeError and abort an already-created upload.

## Edge Cases & Error Semantics

batch_size/row_group_size < 1 raises ValueError before multipart creation. Empty iterable without schema raises ValueError; empty schema-bearing input produces readable zero-row IPC/Parquet. Missing pyarrow raises MissingDependencyError. Schema mismatch, producer exceptions, codec errors, and cancellation abort the upload; no completed object appears. Metadata return is native PutResult.

## Public-Seam Test Blueprint

Canonical tests: `src/py/tests/unit/storage/test_storage_store.py`. Real installed PyArrow and explicit memory/local native stores supply codec and I/O behavior; narrow spies observe requests and cleanup.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Rowgroup output | Table with 250 IDs, row_group_size=100 | Parquet metadata has three rowgroups 100,100,50; projected reader returns exact IDs. |
| Cursor IPC | ArrowCursor with two rows and batch_size=1 | IPC round-trip retains both rows without to_pylist. |
| Incremental demand | Producer gates second batch; observe native write_part | First encoded batch reaches multipart before second input is requested. |
| Failure cleanup | Batch producer raises ValueError("producer failed") after one batch | Original ValueError propagates; multipart aborts; no object exists. |
| Async cancellation | Cancel while native write_part is gated | CancelledError propagates; codec worker/source close and multipart abort complete. |
| Empty/schema | Empty RecordBatch iterable with explicit schema id:int64 | Both output formats decode to zero rows with id schema. |

## Verification

Red: `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -k "arrow or parquet" -q`. Green: `uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_storage_store.py && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q`.

## Acceptance Criteria

- [x] R8 writers emit batch-bounded IPC/Parquet through native put_stream and return PutResult.
- [x] R8 cancellation/failure aborts multipart and closes codec/input without whole-file buffering.

## Notes & Discoveries

- [2026-11-08 00:00] PyArrow 26.0.1 exposes synchronous Python file callbacks. Async native I/O stays on the caller loop; existing `SyncPool` executes codecs. Documentation targets belong to ch05:t02.

[^store]: `src/py/vertebra/storage/_store.py::AsyncObjectStore.put_stream` — Native multipart lifetime.
[^pool]: `src/py/vertebra/utils/sync_tools.py::SyncPool` — Codec execution.
