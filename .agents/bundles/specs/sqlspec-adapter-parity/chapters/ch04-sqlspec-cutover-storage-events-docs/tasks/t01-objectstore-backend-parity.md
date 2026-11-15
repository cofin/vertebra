---
type: Task
id: sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t01
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-cutover-storage-events-docs
title: Upgrade ObjectStoreBackend(ObjectStoreBase) to Full SQLSpec Storage Parity
description: Subclass sqlspec.storage.ObjectStoreBase in extensions/sqlspec/storage.py with async/sync streaming, copy/move, and signed URLs.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t04
  - sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t04
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
    title: SQLSpec ObjectStoreBackend integration
  - id: vtb-storage
    resource: src/py/vertebra/storage/_store.py
    title: Native Rust SyncObjectStore and AsyncObjectStore
verification_strategy: regression_tdd
---

# Task `t01`: Upgrade ObjectStoreBackend(ObjectStoreBase) to Full SQLSpec Storage Parity

## Objective & Scope Boundary

Upgrade `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` to subclass `sqlspec.storage.backends.base.ObjectStoreBase` per [`../spec.md`](../spec.md) [^sqlspec-storage] [^vtb-storage].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — existing `ObjectStoreBackend` methods and `register_storage_alias`.
- `src/py/vertebra/storage/_store.py` — native Tokio awaitables (zero `asyncio.to_thread` per `PYO3-01`).
- `src/py/vertebra/storage/_arrow.py` — Arrow Parquet streaming and batch helpers.

## Contracts & Edge Cases

- Rebase `ObjectStoreBackend` (base-less today, with every listed method already present) onto `ObjectStoreBase` and close any abstract-method gaps, keeping upstream signatures (`sign_sync(paths, expires_in=3600, for_upload=False)`, `stream_arrow_sync(pattern, *, file_format="parquet", batch_size=65_536, **kwargs)`) across the full sync/async surface (`glob_sync`, `is_object_sync`, `is_path_sync`, `move_sync`, `move_async`, `copy_sync`, `copy_async`, `stream_arrow_sync`/`stream_arrow_async`, `supports_signing`, overloaded `sign_sync`/`sign_async` for `str | list[str]`, and `register_storage_alias` integration against `sqlspec.storage.registry.storage_registry` with `SyncStoragePipeline` / `AsyncStoragePipeline`).
- Use native `AsyncObjectStore` Rust Tokio awaitables on all `*_async` methods (`PYO3-01`).

## Test Matrix

- Parametrized sync/async `ObjectStoreBackend` contract tests covering `glob`, `move`, `stream_arrow_*` Parquet round-trips, batch URL signing, and `storage_registry` alias resolution.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `ObjectStoreBackend` subclasses `ObjectStoreBase` and passes all sync/async storage pipeline checks without `asyncio.to_thread`.
- [ ] Zero regressions in the fast verification gate.

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreBackend integration.
[^vtb-storage]: `src/py/vertebra/storage/_store.py` — Native Rust SyncObjectStore and AsyncObjectStore.
