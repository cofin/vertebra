---
type: Task
id: "storage-integrity-and-streaming:ch02:t01"
flow_id: storage-integrity-and-streaming
chapter_id: ch02-streaming-arrow-writes-and-integrations
title: "Add Streaming Arrow Digest Forwarding and SQLSpec Direct Arrow Write Capability"
description: "Forward checksum and expected_* digests through vertebra.storage._arrow and SQLSpec ObjectStoreBackend, set supports_direct_arrow_write = True, and verify abort-safety and bounded memory."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch01:t03"
files:
  - src/py/vertebra/storage/_arrow.py
  - src/py/vertebra/extensions/sqlspec/storage.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
read_set:
  - src/py/vertebra/storage/_arrow.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 175
tags:
  - storage
  - arrow
  - sqlspec
  - integrity
sources:
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: "Incremental Arrow IPC and Parquet multipart streaming codecs"
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: "SQLSpec ObjectStoreBackend write_arrow_sync/async and _write_options"
---

# Task `t01`: Add Streaming Arrow Digest Forwarding and SQLSpec Direct Arrow Write Capability

## Objective & Scope Boundary

Thread `checksum` and `expected_{sha256,crc32c,md5}` options through `write_batches_sync` and `write_batches_async` in `src/py/vertebra/storage/_arrow.py` and declare `supports_direct_arrow_write: ClassVar[bool] = True` plus pipeline option normalization and digest forwarding on SQLSpec `ObjectStoreBackend` in `src/py/vertebra/extensions/sqlspec/storage.py` per [`../spec.md` §2](../spec.md) [^py-arrow] [^sqlspec-backend]. Out of scope: Advanced Alchemy and Django adapters (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — `write_batches_sync` and `write_batches_async` [^py-arrow].
- `src/py/vertebra/storage/_store.py` — `put_stream` keyword arguments from `ch01:t03`.
- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend`, `_normalize_arrow_write_options`, and `_write_options` [^sqlspec-backend].
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec storage adapter conventions.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec storage fixtures.

## Contracts & Edge Cases

- `write_batches_sync` / `write_batches_async`: forward `checksum`, `expected_sha256`, `expected_crc32c`, and `expected_md5` to `store.put_stream(...)` without using `asyncio.to_thread`.
- SQLSpec `ObjectStoreBackend`: set `supports_direct_arrow_write: ClassVar[bool] = True`; normalize `compression`, `format_hint`, and `write_options` kwargs in `_normalize_arrow_write_options` (allowing unknown codec kwargs to raise `TypeError` as today); forward `checksum` and `expected_{sha256,crc32c,md5}` through `_write_options` on `write_bytes_sync/async` and `write_arrow_sync/async`, and `expected_sha256` on `read_bytes_sync/async`.
- When a `pyarrow.RecordBatchReader` raises mid-stream after emitting an initial batch, the multipart upload aborts and any prior object at `path` remains intact; buffered chunk sizes stay bounded well below full table size.

## Test Matrix

- Parametrized tests in `test_storage_store.py` and `test_data_storage_integrations.py` across `("sync", "async")` and `("parquet", "arrow")`: `ObjectStoreBackend.supports_direct_arrow_write is True`; failing `RecordBatchReader` raises `RuntimeError` while preserving prior `"t.parquet"` bytes; `200_000`-row table (`row_group_size=10_000`) emits `>= 20` chunks with `max(len(c)) < table.nbytes / 5`; `compression="zstd"` produces `ZSTD` Parquet metadata; mismatched `expected_sha256` on `write_bytes_*`, `write_arrow_*`, and `read_bytes_*` raises `vertebra.typing.IntegrityError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync mypy src/py/vertebra/storage/_arrow.py src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k "direct_arrow or sqlspec" -q
```

## Acceptance Criteria

- [ ] `vertebra.storage._arrow` and SQLSpec `ObjectStoreBackend` stream Arrow/Parquet writes with bounded memory, abort safety, `supports_direct_arrow_write = True`, and `IntegrityError` digest enforcement (`R4`).
- [ ] Zero regressions in `test_data_storage_integrations.py`.

[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Incremental Arrow IPC and Parquet multipart streaming codecs.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` `write_arrow_sync`/`async` and `_write_options`.
