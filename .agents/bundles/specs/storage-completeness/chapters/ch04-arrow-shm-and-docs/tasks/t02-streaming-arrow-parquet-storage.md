---
type: Task
id: storage-completeness:ch04:t02
chapter_id: ch04-arrow-shm-and-docs
title: Accept complete streaming Arrow and Parquet facade and SQLSpec contracts
description: Integrate bounded sync codecs, async native I/O bridge and SQLSpec streaming through their public APIs.
status: draft
state: blocked
priority: P1
verification_strategy: integration_acceptance
depends_on:
- storage-completeness:ch04:t01
- storage-completeness:ch17:t02
- storage-completeness:ch18:t03
- storage-completeness:ch19:t02
read_set:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/db/arrow.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
- src/py/vertebra/storage/_store.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 100
plan_revision: 4
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: Native key identity, typed CAS conflicts and atomic conditional publication are awaiting repair and independent
  review.
unblock_condition: ch16 native acceptance passes, ch01 recloses with correctness and fresh quality reviews, and ch02 correctness/quality
  review completes.
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-07T04:15:00Z'
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

# Accept complete streaming Arrow and Parquet facade and SQLSpec contracts

## Objective & Scope Boundary

Preserve original ch04:t02 and accept the complete R8 streaming contract after bounded implementation chapters ch17–ch19. Own only final facade integration and canonical consumer regressions; do not reimplement codecs or compress all sync/async Arrow/Parquet behavior into this task. [^facade]

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — public sync/async facade methods.
- `src/py/vertebra/storage/_arrow.py` — private bounded codecs and worker/native-I/O bridge.
- `src/py/vertebra/extensions/sqlspec/storage.py` — protocol integration.
- `src/py/vertebra/db/arrow.py` — unchanged public ArrowCursor conversion/signatures.
- `src/py/tests/unit/storage/test_storage_store.py` — public facade regressions.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec public consumer regressions.

## Symbol & Signature Contracts

SyncObjectStore and AsyncObjectStore expose write_arrow, iter_arrow_batches, read_arrow, write_parquet, iter_parquet_batches and read_parquet with identical options and sync versus awaitable/AsyncIterator return forms defined by ch17/ch18. SQLSpec read/write/cursor/stream APIs preserve installed ObjectStoreProtocol signatures. PyArrow is optional, imports remain lazy, and missing dependency raises MissingDependencyError. Iterators retain only current batches/range cache, use native async I/O and close helper-owned reader/upload resources; caller store remains open. Table convenience outputs may materialize requested result data. No whole encoded-input bytes/list/mmap spool, no synchronous cloud I/O or PyArrow worker callback on event loop, and zero asyncio.to_thread in _store.py.

## Edge Cases & Error Semantics

Schema/dictionary replacement, empty stream/explicit schema, projected Parquet column ranges, missing dependency, invalid row-group/batch size, corrupt encoding, producer failure, wrong owning loop, early iterator close and cancellation have the exact ch17–ch19 typed/cleanup behavior. Do not swallow validation or provider errors as unsupported capability.

## Public-Seam Test Blueprint

| Case | Input | Expected result |
| --- | --- | --- |
| Sync and async projection |250rows id/payload, rowgroups100/100/50, columns=["id"]|Three batches100/100/50, schema id and IDs0..249 through facade and SQLSpec.|
| IPC dictionaries and empty stream |Dictionary replacement batches and empty explicit schema|Valid PyArrow decode with exact values/schema; zero-batch iteration ends cleanly.|
| Consumer backpressure |Gated native multipart and producer/consumer barriers|Producer advances at most current bounded batch; paused reader reserves no blocked worker.|
| Failure/cancellation/lifetime |Raise producer at batch2; cancel reader/writer while native I/O pending; reuse store|Original error/cancellation, awaited abort/reader cleanup, no dangling MPU/new I/O, caller store operational.|
| Optional dependency and wrong loop |Fresh process masks PyArrow; bridge invoked from other loop|MissingDependencyError or explicit RuntimeError before I/O.|

## Verification

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/storage/_arrow.py src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/extensions/sqlspec/storage.py`
- `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [ ] Every public API and negative/lifetime row passes with actual native Memory/Local stores and dictionary/projection fixtures.
- [ ] Canonical tests demonstrate bounded progression and actual values, not source-text checks or generic mocks.
- [ ] Task evidence records exact focused results and final native build checkpoint; docs/OKF follow through original ch04:t03.

## Notes & Discoveries

Private codec and async bridge responsibilities belong to ch17–ch19. This original task retains R8 integration acceptance and its stable identity. No implementation or review result is inferred from plan structure.

[^facade]: `src/py/vertebra/storage/_store.py::SyncObjectStore` and `AsyncObjectStore` — Public object storage boundary.
