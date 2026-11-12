---
type: Task
id: "storage-integrity-and-streaming:ch03:t01"
chapter_id: ch03-arrow-streaming-and-integrations
title: SQLSpec direct Arrow write capability and abort safety
description: Declare supports_direct_arrow_write on the SQLSpec ObjectStoreBackend and prove that write_arrow_sync/async abort on codec failure, preserve prior objects, and keep buffered memory bounded.
status: draft
state: pending
priority: P1
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch02:t04"
read_set:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/storage/_arrow.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - .venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py
files:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py::test_sqlspec_direct_arrow_write_abort_preserves_prior
  - src/py/tests/unit/extensions/test_data_storage_integrations.py::test_sqlspec_direct_arrow_write_bounded_parts
estimated_loc: 140
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: write_arrow_sync/async
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: Incremental codec uploads
  - id: upstream-pipeline
    resource: .venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py
    title: SyncStoragePipeline.write_arrow and AsyncStoragePipeline.write_arrow (sqlspec 0.71.1)
tags:
  - sqlspec
  - arrow
---

# Task t01: SQLSpec Direct Arrow Write Capability and Abort Safety

## Objective & Scope Boundary

1. `ObjectStoreBackend`: add `supports_direct_arrow_write: ClassVar[bool] = True` with a class docstring line stating SQLSpec pipelines that honor the flag call `write_arrow_sync` / `write_arrow_async` directly instead of encoding full payloads [^sqlspec-backend].
2. `write_arrow_sync` / `write_arrow_async` accept the pipeline's `compression` and `write_options` keyword names (as passed by `SyncStoragePipeline.write_arrow` [^upstream-pipeline]) by mapping them inside `_normalize_arrow_write_options`; unknown keys raise `TypeError` from the codec as today.
3. `ObjectStoreBackend._write_options` passes `checksum` and `expected_sha256` through to `put` / `put_stream` so `write_bytes_sync/async` forward digest options (tested in `t02`).
4. Tests prove abort safety and bounded parts through the public backend.

Out of scope: upstream pipeline changes (ch04/t03).

## Bounded Read Manifest

- `extensions/sqlspec/storage.py` — `ObjectStoreBackend`, `write_arrow_sync`, `write_arrow_async`, `_normalize_arrow_write_options`, `_write_options`, `write_bytes_sync` [^sqlspec-backend].
- `_arrow.py` — `write_batches_sync`, `write_batches_async` [^py-arrow].
- Upstream `pipeline.py` — `write_arrow` kwargs (`compression`, `format_hint`, `write_options`) [^upstream-pipeline].
- Integration test file fixtures.

## Symbol & Signature Contracts

- `ObjectStoreBackend.supports_direct_arrow_write: ClassVar[bool] = True`.
- `write_arrow_sync(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None` unchanged.

## Edge Cases & Error Semantics

- Injected failure: a `pyarrow.RecordBatchReader` whose second batch raises `RuntimeError("codec boom")`; the error propagates, the multipart upload is aborted, the prior object remains.
- Bounded memory: memory store records chunk sizes passed to `put_stream`; no single chunk exceeds the codec batch output and the total is never materialized as one buffer.

## Public-Seam Test Blueprint

- **Canonical gate**: `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k direct_arrow -v`, parametrized over `("sync", "async")` and `("parquet", "arrow")`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Flag | `SQLSpecObjectStoreBackend.supports_direct_arrow_write` | `True` |
| Abort preserves prior | `write_bytes_sync("t.parquet", b"prior")`; failing reader | `RuntimeError("codec boom")`; `read_bytes_sync("t.parquet") == b"prior"` |
| Bounded parts | 200_000-row int64 table, `row_group_size=10_000`, spy wrapping `SyncObjectStore.put_stream` | spy sees ≥ 20 chunks; `max(len(c)) < table.nbytes / 5` |
| Pipeline kwargs | `write_arrow_sync("t.parquet", table, compression="zstd")` | `pyarrow.parquet.read_metadata` reports `ZSTD` |

## Verification

- `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k direct_arrow -v`
- `uv run mypy src/py/vertebra/extensions/sqlspec/storage.py`

## Acceptance Criteria

- [ ] Backend advertises direct Arrow writes; failures abort and preserve prior objects; buffering stays bounded (R6).

## Notes & Discoveries

- [2026-11-12 01:30] SQLSpec 0.71.1 `SyncStoragePipeline.write_arrow` calls `_encode_arrow_payload` then `_write_bytes`; the hook itself is upstream work.

[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Arrow uploads.
[^upstream-pipeline]: `.venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py` — Upstream pipeline.
