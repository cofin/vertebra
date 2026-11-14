---
type: Task
id: sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t03
flow_id: sqlspec-adapter-parity
chapter_id: ch01-sqlspec-core-driver-and-artifact-ref
title: Cut SQLSpec Provider Over to VTBARROW ArrowCursor and Add vertebra.storage.ArtifactReference
description: Replace SharedArrowBuffer/SharedArrowTable with VTBARROW ArrowCursor and add vertebra.storage.ArtifactReference.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t02
files:
- src/py/vertebra/extensions/sqlspec/provider.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
- src/py/tests/unit/storage/test_storage_store.py
read_set:
- src/py/vertebra/extensions/sqlspec/provider.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/__init__.py
- src/py/vertebra/db/arrow.py
estimated_loc: 210
tags:
- sqlspec
- arrow
- shm
- storage
sources:
- id: sqlspec-provider
  resource: src/py/vertebra/extensions/sqlspec/provider.py
  title: SQLSpecSyncProvider and SQLSpecAsyncProvider Arrow result surface
- id: storage-store
  resource: src/py/vertebra/storage/_store.py
  title: ObjectMeta and PutResult storage value structs
---

# Task `t03`: Cut SQLSpec Provider Over to VTBARROW ArrowCursor and Add vertebra.storage.ArtifactReference

## Objective & Scope Boundary

Replace `SharedArrowBuffer`/`SharedArrowTable` with Rust `VTBARROW` `ArrowCursor` frames in `provider.py`, drop `gc=False` on container structs, and define `vertebra.storage.ArtifactReference` per [`../spec.md`](../spec.md) [^sqlspec-provider] [^storage-store].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/provider.py` — `SQLSpecSyncProvider`, `SQLSpecAsyncProvider`, `BrokerRpcRequest`, `BrokerRpcResponse`.
- `src/py/vertebra/storage/_store.py` & `__init__.py` — `ObjectMeta`, `PutResult`, and public exports.
- `src/py/vertebra/db/arrow.py` — `ArrowCursor` `VTBARROW` SHM framing.

## Contracts & Edge Cases

- Replace `SharedArrowBuffer` and `SharedArrowTable` in `provider.py` with `ArrowCursor` (`VTBARROW` SHM frames whose unlink lifecycle is tied strictly to lease release), and remove `gc=False` from `BrokerRpcRequest` and `BrokerRpcResponse`.
- Remove `gc=False` from `ObjectMeta` and `PutResult` in `storage/_store.py` (which hold container metadata dicts).
- Define `class ArtifactReference(msgspec.Struct, frozen=True):` (`uri: str`, `version: int`, `content_type: str | None = None`, `size: int | None = None`, `updated: datetime | None = None`, `signed_url: str | None = None`) in `storage/_store.py` and export from `vertebra.storage` with zero SQL or SQLSpec imports.

## Test Matrix

- Unit tests verifying `SQLSpecSyncProvider`/`SQLSpecAsyncProvider` zero-copy `VTBARROW` `ArrowCursor` transport, lease-scoped SHM unlink, GC cycle collection on `ObjectMeta`/`PutResult`/`BrokerRpc*`, and `ArtifactReference` immutability.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/storage/test_storage_store.py
```

## Acceptance Criteria

- [ ] `SharedArrowBuffer` and `SharedArrowTable` are removed from `provider.py` in favor of `VTBARROW` `ArrowCursor`.
- [ ] `vertebra.storage.ArtifactReference` is exported and `gc=False` is removed from container-bearing structs.
- [ ] Zero regressions in the fast verification gate.

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py` — SQLSpecSyncProvider and SQLSpecAsyncProvider Arrow result surface.
[^storage-store]: `src/py/vertebra/storage/_store.py` — ObjectMeta and PutResult storage value structs.
