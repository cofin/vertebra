---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch04-permanent-native-postgres-drivers
title: "Chapter 04: Permanent Native Sync and Async PostgreSQL Drivers"
description: "Build permanent SyncPostgreSQLDriver (_sync.py) and AsyncPostgreSQLDriver (_async.py) over vertebra._postgres pools, pipelines, Arrow, and event hooks."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T05:35:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - python
  - postgres
  - driver
  - arrow
  - pipeline
sources:
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: "Native Sync/Async client, pool, pipeline, copy, and listener stubs"
  - id: pg-pipeline
    resource: "src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline"
    title: "Adapter pipeline context managers"
  - id: pg-arrow
    resource: "src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync"
    title: "Adapter Arrow COPY import helpers"
  - id: db-driver-base
    resource: src/py/vertebra/db/driver/__init__.py
    title: "SyncDatabaseDriver and AsyncDatabaseDriver contracts"
---

# Chapter 04: Permanent Native Sync and Async PostgreSQL Drivers

<!-- truth: start -->
- `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` execute directly over `SyncPostgresPool`/`SyncPostgresClient` and `AsyncPostgresPool`/`AsyncPostgresClient` with zero `asyncio.to_thread` wrappers [^pg-pyi] [^db-driver-base].
- Both drivers wire `SyncPostgresPipeline`/`AsyncPostgresPipeline` (`pipeline.py`), `VTBARROW` `ArrowCursor` + binary `COPY` (`arrow.py`), shielded transactions/savepoints, RLS tenant scopes, and `EventSessionRegistry` close hooks in a single chapter batch [^pg-pipeline] [^pg-arrow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Writes `_sync.py` and `_async.py` cleanly once before `ch05` switches the public `driver.py` barrel.

## 2. Requirements, Concrete Contracts & Architecture

- `SyncPostgreSQLDriver` (`_sync.py`) & `AsyncPostgreSQLDriver` (`_async.py`): full `SyncDatabaseDriver` / `AsyncDatabaseDriver` + `SyncPipelineCapable` / `AsyncPipelineCapable` + `PostgresNotificationDriver` / `PostgresReplicationDriver` surfaces, resolving `vertebra._postgres` via `ensure_module('vertebra._postgres', package='vertebra-core[postgres]', extra='postgres')`.
- `AsyncPostgreSQLDriver.transaction()` shields `ROLLBACK` under `anyio.CancelScope(shield=True)` on `BaseException` and evicts broken sessions.

## 3. Documentation & OKF Knowledge Plan

Public barrel activation and OKF/user docs follow in `ch05`, `ch08`, and `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R3` `SyncPostgreSQLDriver` (`_sync.py`) | `t01` | `_sync.py` | `pytest test_postgres_native_adapters.py` |
| `R3` `AsyncPostgreSQLDriver` (`_async.py`) | `t02` | `_async.py` | `pytest test_postgres_native_adapters.py` |
| `R3` Pipelines, `VTBARROW` & event hooks | `t03` | `_sync.py`, `_async.py` | `pytest test_postgres_native_adapter_{pipeline,arrow}.py test_postgres_event_hooks.py` |

## 5. Implementation Plan

- [x] [50d7a1ef] [t01-permanent-sync-postgresql-driver.md](tasks/t01-permanent-sync-postgresql-driver.md) — Build SyncPostgreSQLDriver over SyncPostgresPool/SyncPostgresClient with queries, transactions/savepoints, RLS scopes, and binary COPY.
- [x] [50d7a1ef] [t02-permanent-async-postgresql-driver.md](tasks/t02-permanent-async-postgresql-driver.md) — Build native AsyncPostgreSQLDriver over AsyncPostgresPool/AsyncPostgresClient with shielded rollback and zero asyncio.to_thread.
- [x] [50d7a1ef] [t03-driver-pipelines-arrow-and-event-hooks.md](tasks/t03-driver-pipelines-arrow-and-event-hooks.md) — Connect pipeline.py, arrow.py, listen_notifications, open_replication, and EventSessionRegistry close hooks in _sync.py and _async.py.

## 6. Current State & Remaining Plan Snapshot

Completed (`3/3` tasks at `50d7a1ef`). Offline `< 25s` unit + Ruff + Mypy gate over `postgres_ports.py` doubles.

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Native Sync/Async client, pool, pipeline, copy, and listener stubs.
[^pg-pipeline]: `src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline` — Adapter pipeline context managers.
[^pg-arrow]: `src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync` — Adapter Arrow COPY import helpers.
[^db-driver-base]: `src/py/vertebra/db/driver/__init__.py` — SyncDatabaseDriver and AsyncDatabaseDriver contracts.
