---
type: Task
id: storage-completeness:ch04:t02
chapter_id: ch04-arrow-shm-and-docs
title: Read Arrow and Parquet through a bounded async codec bridge
description: Add sync/async batch readers using native stream/range reads and existing
  SyncPool codec execution. Preserve backpressure, dictionary state, projection, caller-loop
  responsiveness, and deterministic cancellation cleanup.
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch04:t01
read_set:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/utils/sync_tools.py
- src/py/vertebra/db/arrow.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/vertebra/_storage.pyi
- src/py/vertebra/storage/_arrow.py
files:
- src/py/vertebra/storage/_arrow.py
- src/py/vertebra/storage/_store.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 240
plan_revision: 9
plan_commit: 222cbf99
state_revision: 1
claimed_by: agent:arrow-reader-executor
claimed_at: '2026-11-08T16:55:18.904122Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T16:55:18.904122Z'
commit: null
sources:
- id: store
  resource: src/py/vertebra/storage/_store.py::AsyncReadableFile
  title: Native async readable file
- id: pool
  resource: src/py/vertebra/utils/sync_tools.py::SyncPool
  title: Existing codec worker pool
tags:
- storage
- arrow
---
# Task t02: Read Arrow and Parquet through a bounded async codec bridge

## Objective & Scope Boundary

Add sync/async batch readers using native stream/range reads and existing SyncPool codec execution. Preserve backpressure, dictionary state, projection, caller-loop responsiveness, and deterministic cancellation cleanup.

The implementation uses src/py/vertebra/storage/_store.py::AsyncReadableFile [^store].

The implementation uses src/py/vertebra/utils/sync_tools.py::SyncPool [^pool].

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/utils/sync_tools.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/db/arrow.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/_storage.pyi` — owning implementation, collaborator contract, or canonical verification surface.

- `src/py/vertebra/storage/_arrow.py` — new private codec module owned by this task.

## Symbol & Signature Contracts

- Public sync `iter_arrow_batches(self, key: str) -> Iterator[Any]` and async `iter_arrow_batches(self, key: str) -> AsyncIterator[Any]`; Parquet variants add keyword-only `batch_size: int = 65536, columns: Sequence[str] | None = None`. Yield real RecordBatch objects.
- `_arrow.py` owns codec helpers and synchronous `io.RawIOBase` adapters; `_store.py` only delegates these four methods. Avoid runtime circular imports with TYPE_CHECKING store types.
- Caller -> async iterator -> `SyncPool.default().submit(fn, ...)` -> one PyArrow initialize/next/close operation at a time. Await its concurrent future with asyncio.wrap_future. Return `(done, batch)` from next operations; never deliver StopIteration through a Future.
- Capture the running caller loop. Worker-only file `read(n)` calls `asyncio.run_coroutine_threadsafe(reader.read(n), loop).result()`; seek/tell and known size delegate the serialized AsyncReadableFile state. Resolve size by awaiting store.head before ParquetFile creation. Arrow stream callbacks consume the native async get_stream iterator and hold only a chunk remainder; PyArrow open_stream retains schema/dictionary memo across batches.
- Sync helpers use SyncReadableFile for Parquet and a sequential get_stream adapter for IPC. No full get/get_view, temporary file staging, list(all_batches), or table.to_pylist.
- Adapter tracks a closed flag and its one pending cross-thread I/O future under a lock. Consumer cancellation/close marks closed, cancels pending I/O, awaits the active worker without synchronous join, then closes codec/source. Reader/writer ownership is released only after worker exit. Early break is supported via explicit async generator aclose/contextlib.aclosing.
- Async I/O never uses SyncObjectStore or asyncio.to_thread. SyncPool is codec execution; the RustTaskAwaitable I/O executes on the original loop. Memory is bounded by decoder requests/current batch or rowgroup plus footer/dictionary metadata, not total dataset.

- Private bridge entry points are `iter_batches_sync(store: SyncObjectStore, key: str, *, file_format: str, batch_size: int = 65536, columns: Sequence[str] | None = None, options: Mapping[str, Any] | None = None) -> Iterator[Any]`, matching async `iter_batches_async(...) -> AsyncIterator[Any]`, and `read_table_sync(...) -> Any` / `async read_table_async(...) -> Any` with the same key/format/options parameters. Public storage methods delegate these helpers with empty options; SQLSpec calls these helpers for accepted codec options. Forward options to their original PyArrow recipient (ParquetFile/iter_batches or read_table); unsupported keywords retain PyArrow TypeError.
- Table helpers keep a persistent reader, preserving schema for zero batches and collecting only the required decoded Table return. IPC-file read fallback uses the seekable range adapter and ipc.open_file after open_stream rejects file framing; do not fetch the complete encoded file. IPC stream iterators preserve their streaming format behavior. Test a zero-row schema-bearing IPC file and IPC-file read fallback alongside streaming tests.

- Parquet batch readers construct `ParquetFile` with `pre_buffer=False` by default, including all new public batch helpers. SQLSpec explicitly supplied `pre_buffer=True` remains honored for API compatibility and opts out of the bounded-prefetch guarantee; do not silently discard it. Fixed-size footer read-ahead is permitted. Full-Table read helpers preserve caller options and intentionally materialize their Table return.

## Edge Cases & Error Semantics

All helpers require pyarrow and raise MissingDependencyError with extra="arrow" when absent. batch_size < 1 raises ValueError before I/O. Empty IPC/schema-only datasets yield no batches. Invalid/truncated IPC or Parquet preserves the PyArrow exception. Close is idempotent; cancellation propagates CancelledError after pending I/O and worker cleanup. PyArrow can request a footer window that covers an entire small object; tests distinguish bounded requests from whole-object get APIs.

## Public-Seam Test Blueprint

Canonical tests: `src/py/tests/unit/storage/test_storage_store.py`. Real installed PyArrow and explicit memory/local native stores supply codec and I/O behavior; narrow spies observe requests and cleanup.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Projection/rowgroups | 250 rows, 100-row groups; batch_size=100, columns=["id"] | Batch lengths 100,100,50; IDs 0..249, only id. |
| Large-file rowgroup demand | Real uncompressed Parquet with three rowgroups, each payload >64 KiB; spy get_range using metadata-derived rowgroup offsets | With default pre_buffer=False, first batch arrives before any later-rowgroup data read; fixed footer-window reads may overlap the file tail and are excluded explicitly. Request next batch only after verifying the gate. |
| Explicit prefetch | Same dataset through private helper options={"pre_buffer": True} | Constructor receives True; round-trip works, bounded-prefetch assertion does not apply. |
| Dictionary IPC | Two dictionary-encoded batches with repeated labels | Both decode correctly through one persistent stream reader. |
| Demand | Pause consumer after first batch; spy native range/stream requests | No next-batch work is scheduled until next anext; no get/get_view or sync native calls. |
| Responsive loop | Gate an async remote range while another task runs | Event-loop task progresses before releasing range gate. |
| Cancellation | Cancel anext while native async range is gated | Pending I/O canceled, worker exits, reader closes, no dangling task/future. |
| Early close and corruption | aclose after first batch; truncated IPC bytes | Sources close once; corruption raises original codec error. |

## Verification

Red: `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -k "arrow or parquet" -q`. Green: `uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_storage_store.py && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q`.

## Acceptance Criteria

- [ ] R8 sync/async public iterators yield real batches lazily through bounded native reads.
- [ ] R8 codec execution stays off-loop and cancellation releases source and worker without full-file materialization.

## Notes & Discoveries

- [2026-11-08 00:00] PyArrow 26.0.1 exposes synchronous Python file callbacks. Async native I/O stays on the caller loop; existing `SyncPool` executes codecs. Documentation targets belong to ch05:t02.

[^store]: `src/py/vertebra/storage/_store.py::AsyncReadableFile` — Native async readable file.
[^pool]: `src/py/vertebra/utils/sync_tools.py::SyncPool` — Existing codec worker pool.
