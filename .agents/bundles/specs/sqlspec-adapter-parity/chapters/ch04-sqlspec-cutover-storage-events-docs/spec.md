---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-cutover-storage-events-docs
title: 'Chapter 04: ObjectStoreBase Parity, ChangeFeed Event Backends, Legacy Cutover & Docs/OKF'
description: Upgrade ObjectStoreBackend(ObjectStoreBase), harden ChangeFeed*EventBackend with durable checkpoint/spill, delete monolithic {adapters,bigquery,spanner}.py, and synchronize docs and OKF chapters.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- sqlspec
- storage
- events
- documentation
- okf
sources:
- id: sqlspec-storage
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: SQLSpec ObjectStoreBackend
- id: sqlspec-events
  resource: src/py/vertebra/extensions/sqlspec/events.py
  title: SQLSpec ChangeFeed event backends
- id: sqlspec-plugin
  resource: src/py/vertebra/extensions/sqlspec/plugin.py
  title: SQLSpecPlugin lifecycle
- id: okf-dialects
  resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  title: SQLSpec OKF chapter
---

# Chapter 04: ObjectStoreBase Parity, ChangeFeed Event Backends, Legacy Cutover & Docs/OKF

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/sqlspec/{adapters,bigquery,spanner}.py` coexist with incomplete `ObjectStoreBackend` and uncheckpointed `ChangeFeed*EventBackend` classes [^sqlspec-storage] [^sqlspec-events] [^sqlspec-plugin].
- **Target Architecture**: `ObjectStoreBackend` subclasses `ObjectStoreBase`, `ChangeFeed*EventBackend` persists checkpoints and spills `> 8000`B notify payloads, monolithic `{adapters,bigquery,spanner}.py` are deleted (with zero `_compat.py`), and docs/OKF chapters are synchronized [^okf-dialects].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This final chapter completes `ObjectStoreBackend(ObjectStoreBase)` parity, hardens `ChangeFeedSyncEventBackend`/`ChangeFeedAsyncEventBackend`, deletes the legacy monolithic SQLSpec modules, and updates public docs and OKF chapters [^sqlspec-storage] [^sqlspec-plugin].

## 2. Requirements, Concrete Contracts & Architecture

- **R7 (`ObjectStoreBackend` & `ChangeFeed*EventBackend`)**: Full `ObjectStoreBase` subclassing over native Tokio awaitables (`PYO3-01`) and durable checkpointing/spilling on CDC event backends.
- **R8 (Clean Cutover & Docs/OKF)**: Delete `adapters.py`, `bigquery.py`, and `spanner.py`; ensure generic modules hold zero vendor dispatch; synchronize `docs/` and `<= 200`-line OKF chapters.

```python
class ObjectStoreBackend(ObjectStoreBase):
    def stream_arrow_sync(self, path: str, *, batch_size: int = 65536) -> Iterator[pa.RecordBatch]: ...
    def stream_arrow_async(self, path: str, *, batch_size: int = 65536) -> AsyncIterator[pa.RecordBatch]: ...
    def sign_sync(self, paths: str | list[str], *, expires_in: int = 3600) -> str | list[str]: ...
    async def sign_async(self, paths: str | list[str], *, expires_in: int = 3600) -> str | list[str]: ...

class ChangeFeedSyncEventBackend:
    def publish(self, channel: str, payload: Mapping[str, Any]) -> str: ...
    def iter_events(self, channel: str, *, resume_token: str | None = None) -> Iterator[ dict[str, Any]]: ...

class ChangeFeedAsyncEventBackend:
    async def publish(self, channel: str, payload: Mapping[str, Any]) -> str: ...
    def iter_events(self, channel: str, *, resume_token: str | None = None) -> AsyncIterator[dict[str, Any]]: ...
```

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/extensions.md`, [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), and [database-driver-limitations.md](../../../../knowledge/domains/data/database-driver-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Upgrade ObjectStoreBackend(ObjectStoreBase) to Full SQLSpec Storage Parity | `src/py/vertebra/extensions/sqlspec/storage.py` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| `t02` | Harden ChangeFeedSyncEventBackend and ChangeFeedAsyncEventBackend with Checkpoints & Object Spill | `src/py/vertebra/extensions/sqlspec/events.py` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| `t03` | Delete Legacy SQLSpec Modules and Cut Over SQLSpecPlugin and Public Exports | `src/py/vertebra/extensions/sqlspec/adapters/__init__.py`, `src/py/vertebra/extensions/sqlspec/__init__.py`, `src/py/vertebra/extensions/sqlspec/plugin.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |
| `t04` | Synchronize SQLSpec Adapter Parity Documentation and OKF Knowledge Chapters | `docs/usage/extensions.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py` |

## 5. Implementation Plan

- [ ] [t01-objectstore-backend-parity.md](tasks/t01-objectstore-backend-parity.md) — Subclass sqlspec.storage.ObjectStoreBase in extensions/sqlspec/storage.py with async/sync streaming, copy/move, and signed URLs.
- [ ] [t02-changefeed-event-backends.md](tasks/t02-changefeed-event-backends.md) — Add durable checkpoint persistence and ArtifactReference large-payload spill to extensions/sqlspec/events.py.
- [ ] [t03-legacy-cutover-and-plugin-cleanup.md](tasks/t03-legacy-cutover-and-plugin-cleanup.md) — Delete extensions/sqlspec/{adapters,bigquery,spanner}.py and rewire plugin.py and __init__.py onto per-engine packages.
- [ ] [t04-sqlspec-docs-and-okf-sync.md](tasks/t04-sqlspec-docs-and-okf-sync.md) — Update docs/usage/extensions.md, docs/reference/extensions.md, sqlspec-alchemy-and-dialects.md, and database-driver-limitations.md.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch04-sqlspec-cutover-storage-events-docs` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreBackend.
[^sqlspec-events]: `src/py/vertebra/extensions/sqlspec/events.py` — SQLSpec ChangeFeed event backends.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py` — SQLSpecPlugin lifecycle.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec OKF chapter.
