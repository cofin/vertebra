---
type: Task
id: "sqlspec-adapter-parity:ch10-sqlspec-cutover-storage-events-docs:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch10-sqlspec-cutover-storage-events-docs
title: "Upgrade extensions/sqlspec/storage.py::ObjectStoreBackend onto ObjectStoreBase"
description: "Make ObjectStoreBackend subclass sqlspec ObjectStoreBase implementing all sync/async methods, Parquet Arrow streaming, and alias registration."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-sqlspec-core-driver-and-artifact-ref:t03"
files:
  - src/py/vertebra/extensions/sqlspec/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
read_set:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/_arrow.py
estimated_loc: 215
tags:
  - sqlspec
  - storage
  - arrow
sources:
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: "SQLSpec ObjectStoreBackend integration"
  - id: vtb-storage
    resource: src/py/vertebra/storage/_store.py
    title: "Native Rust SyncObjectStore and AsyncObjectStore"
---

# Task `t01`: Upgrade extensions/sqlspec/storage.py::ObjectStoreBackend onto ObjectStoreBase

## Objective & Scope Boundary

Upgrade `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` to subclass `sqlspec.storage.backends.base.ObjectStoreBase` per [`../spec.md`](../spec.md) [^sqlspec-storage] [^vtb-storage].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — existing `ObjectStoreBackend` methods.
- `src/py/vertebra/storage/_store.py` & `_arrow.py` — native Tokio awaitables (zero `asyncio.to_thread` per `PYO3-01`) and Arrow Parquet streaming.

## Contracts & Edge Cases

- Subclass `ObjectStoreBase` implementing full sync/async surface (`glob_*`, `is_object_*`, `is_path_*`, `move_*`, `copy_*`, `stream_arrow_sync`/`stream_arrow_async`, overloaded `sign_sync`/`sign_async` for `str | list[str]`, and `register_storage_alias` integration with `SyncStoragePipeline` / `AsyncStoragePipeline`).
- Use native `AsyncObjectStore` Rust Tokio awaitables on all `*_async` methods (`PYO3-01`).

## Test Matrix

- Parametrized sync/async `ObjectStoreBackend` contract tests covering `glob`, `move`, `stream_arrow_*` Parquet round-trips, batch URL signing, and `storage_registry` alias resolution.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `ObjectStoreBackend` subclasses `ObjectStoreBase` and passes all sync/async storage pipeline checks without `asyncio.to_thread`.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreBackend integration.
[^vtb-storage]: `src/py/vertebra/storage/_store.py` — Native Rust SyncObjectStore and AsyncObjectStore.
