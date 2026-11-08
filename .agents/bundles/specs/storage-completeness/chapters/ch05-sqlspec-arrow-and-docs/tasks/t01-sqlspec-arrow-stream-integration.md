---
type: Task
id: storage-completeness:ch05:t01
chapter_id: ch05-sqlspec-arrow-and-docs
title: Integrate bounded batches with SQLSpec public storage methods
description: Delegate SQLSpec Arrow reads/writes/streaming to completed public storage
  helpers while preserving protocol signatures, format selection, namespaced URI/glob
  patterns, ArrowCursor handling, and kwargs.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch04:t03
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- .venv/lib/python3.12/site-packages/sqlspec/protocols.py
files:
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 210
plan_revision: 10
plan_commit: '78293774'
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T17:39:10.695721Z'
commit: null
sources:
- id: sql
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
  title: SQLSpec public adapter
- id: protocol
  resource: .venv/lib/python3.12/site-packages/sqlspec/protocols.py::ObjectStoreProtocol
  title: Installed contract
tags:
- storage
- arrow
---
# Task t01: Integrate bounded batches with SQLSpec public storage methods

## Objective & Scope Boundary

Delegate SQLSpec Arrow reads/writes/streaming to completed public storage helpers while preserving protocol signatures, format selection, namespaced URI/glob patterns, ArrowCursor handling, and kwargs.

The implementation uses src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend [^sql].

The implementation uses .venv/lib/python3.12/site-packages/sqlspec/protocols.py::ObjectStoreProtocol [^protocol].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/storage/_store.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/storage/_arrow.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owning implementation, collaborator contract, or canonical verification surface.
- `.venv/lib/python3.12/site-packages/sqlspec/protocols.py` — owning implementation, collaborator contract, or canonical verification surface.

## Symbol & Signature Contracts

- Preserve ObjectStoreProtocol signatures: read_arrow_sync/async(path: str | Path, **kwargs)->ArrowTable; write_arrow_sync/async(path, table, **kwargs)->None; stream_arrow_sync/async(pattern: str, *, file_format: Literal["parquet"]="parquet", batch_size: int=65536, **kwargs)->Iterator/AsyncIterator[ArrowRecordBatch]. stream_arrow_async is a normal def returning async iterator, never a coroutine.
- Caller -> stream_arrow_async -> _iter_arrow_async -> resolve native async glob matches -> qualify each relative key -> AsyncObjectStore.iter_parquet_batches or iter_arrow_batches. Preserve explicit file_format precedence; infer extension only where existing method contract does so. Keep .arrow/.ipc/.feather recognition and IPC-file read fallback for existing read_arrow methods.
- Public read_arrow returns complete Table via pa.Table.from_batches: whole-table return is its contract, but no whole-file get/get_view/to_pylist intermediary. Preserve empty schema using reader metadata, not untyped empty lists. Writes delegate public batch writers and discard PutResult to retain None return.
- Preserve write_arrow_cursor_sync/async; IPC streams decode from cursor.buffer_view without copying to rows. Keep option forwarding: columns/batch_size for batch readers, compression/row_group_size for Parquet writes, existing accepted PyArrow options. Use the predeclared private storage._arrow iter_batches/read_table/write_batches helpers when forwarding codec options or IPC-file fallback; ordinary cases delegate public store helpers. Unsupported options retain PyArrow TypeError. This task changes only the SQLSpec adapter.
- Remove whole-list _iter_arrow_async and asyncio.to_thread codec paths in touched methods. Reuse bridge codec offload; asynchronous pattern listing uses native async listing rather than calling glob_sync in a worker. Do not change build_duckdb_gcs_secret_sql.

- Parquet batch readers construct `ParquetFile` with `pre_buffer=False` by default, including all new public batch helpers. SQLSpec explicitly supplied `pre_buffer=True` remains honored for API compatibility and opts out of the bounded-prefetch guarantee; do not silently discard it. Fixed-size footer read-ahead is permitted. Full-Table read helpers preserve caller options and intentionally materialize their Table return.

- Existing write_arrow_cursor methods also accept raw IPC bytes/memoryview through the buffer protocol. Preserve this by an adapter-owned view wrapper exposing buffer_view to the existing private writer, so persistent IPC decoding remains in its codec worker. Do not iterate raw buffers as integer batches.
- Preserve unspecified SQLSpec Parquet compression defaults by forwarding unchanged options to private write_batches; the public store writer's explicit zstd default does not replace the adapter's existing PyArrow default.
- Reuse the private reader's existing constructor/iteration option routing, including explicit pre_buffer=True. Resolve async patterns with glob_async/exists_async and close each inner async batch iterator when the outer pattern iterator closes or cancels.

## Edge Cases & Error Semantics

Empty pattern matches yields no batches. URI for other bucket or parent traversal keeps existing error. Explicit file_format overrides extension. Reader/writer errors propagate and active async iterator closes on cancellation. Dictionary state is per-file; finish one stream before opening the next. IPC-file reads preserve existing open_file fallback; stream methods retain stream format contract.

## Public-Seam Test Blueprint

Canonical tests: `src/py/tests/unit/extensions/test_data_storage_integrations.py`. Real installed PyArrow and explicit memory/local native stores supply codec and I/O behavior; narrow spies observe requests and cleanup.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Public SQLSpec consumer | backend.stream_arrow_async("tables/*.parquet", batch_size=100, columns=["id"]) | AsyncIterator directly; exact projection across 250-row file, no list preload. |
| Namespace | URI-qualified pattern matching two prefixed objects | Relative glob matches and qualification preserve both datasets once. |
| Public read/write | Existing SQLSpec pipeline async read/write plus IPC cursor methods | Complete Table/None signatures and real rows unchanged. |
| Laziness | Gate later file/batch; call anext once | First batch arrives before gate; no synchronous native methods called. |
| Parquet prefetch compatibility | Public stream_arrow_async on >64 KiB three-rowgroup Parquet, default options vs explicit pre_buffer=True | Default first batch precedes later-rowgroup data reads (footer window allowed); explicit True reaches PyArrow and opts out of bounded-prefetch assertions. Full-Table read options remain unchanged. |
| Cancellation/kwargs | Cancel streaming at pending range; pass compression="zstd" | Reader cleanup completes; write respects format options. |

## Verification

Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k "arrow or pipeline" -q`. Green: `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`.

## Acceptance Criteria

- [ ] R4/R8 SQLSpec public streams preserve patterns/protocol and yield lazily through async native storage.
- [ ] R4/R8 complete-Table consumers and write options retain installed SQLSpec contracts; ch05:t02 documents shipped behavior.

## Notes & Discoveries

- [2026-11-08 00:00] PyArrow 26.0.1 exposes synchronous Python file callbacks. Async native I/O stays on the caller loop; existing `SyncPool` executes codecs. Documentation targets belong to ch05:t02.

[^sql]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec public adapter.
[^protocol]: `.venv/lib/python3.12/site-packages/sqlspec/protocols.py::ObjectStoreProtocol` — Installed contract.
