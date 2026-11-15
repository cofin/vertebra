---
type: Task
id: sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t01
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-relational-adapters
title: Deliver Feature-Complete PostgreSQL SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/postgres/ ($1 NUMERIC, Arrow COPY, portals, DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_postgres').
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t03
files:
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/litestar.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/storage/_store.py
estimated_loc: 245
tags:
  - sqlspec
  - postgres
  - data-dictionary
  - adk
  - events
  - litestar
  - config
sources:
  - id: postgres-drv
    resource: src/py/vertebra/db/adapters/postgres/_sync.py
    title: Native PostgreSQL sync and async drivers
  - id: postgres-core
    resource: src/py/vertebra/db/adapters/postgres/core.py
    title: Native PostgreSQL core parameter and wire helpers
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: PostgreSQL notification source
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: Storage-neutral ArtifactReference value struct
verification_strategy: regression_tdd
---

# Task `t01`: Deliver Feature-Complete PostgreSQL SQLSpec Adapter Package

## Objective & Scope Boundary

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/postgres/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`PostgresSyncDriver`/`PostgresAsyncDriver`, `PostgresSyncDataDictionary`/`PostgresAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `PostgresSyncConfig`/`PostgresAsyncConfig`) per [`../spec.md`](../spec.md) [^postgres-drv] [^postgres-core] [^pg-notify] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — native PostgreSQL sync and async execution drivers.
- `src/py/vertebra/db/adapters/postgres/core.py` — native PostgreSQL parameter and wire helpers.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` channels.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy PostgreSQL SQLSpec logic to replace and duplicate cleanly into `adapters/postgres/`.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='postgres')` with default and execution `ParameterStyle.NUMERIC` (`$1, $2` positional sequences matching `vertebra._postgres`, as today; the target `PostgresSyncDriver`/`PostgresSyncConfig` names rename the existing `PostgreSQLSyncDriver`/`PostgreSQLSyncConfig` with zero aliases), native Arrow `COPY BINARY` `select_to_arrow`, server-side portal `dispatch_select_stream`, pipeline `execute_stack`, savepoints, and `PostgresSyncDataDictionary`/`PostgresAsyncDataDictionary` executing upstream `sqlspec/data_dictionary/dialects/postgres/sql/*.sql` catalog queries. Keep all PostgreSQL SQL/parameter helpers strictly inside `adapters/postgres/core.py` (zero cross-adapter helper modules).
- Subclass SDK-free upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, pgvector cosine/HNSW + `tsvector` `BaseSyncADKMemoryStore`/`BaseAsyncADKMemoryStore`, `BaseSyncADKArtifactStore`/`BaseAsyncADKArtifactStore` returning `ArtifactReference` on the shared surface, `BaseEventQueueStore` + `PostgresSyncEventsBackend`/`PostgresAsyncEventsBackend` over `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and `BaseSQLSpecStore` `ON CONFLICT (key) DO UPDATE`) executing exclusively via `self._config.provide_session()` with zero `psycopg` or `sqlspec.adapters.*` imports.
- Define `PostgresSyncConfig` and `PostgresAsyncConfig` declaring `adapter_name = 'vertebra_postgres'`, `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class`, using PEP 562 `__getattr__` in `__init__.py` so `google.adk` loads lazily and guards via `MissingDependencyError`, verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, `PostgresSyncDataDictionary`/`PostgresAsyncDataDictionary` catalog tests, store contract tests (ADK session/memory search, `ArtifactReference`, events, Litestar TTL/upsert), PEP 562 lazy import tests (zero eager `google.adk` import), and `resolve_adapter_name` / `resolve_extension_store_class` resolver checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `PostgresSyncDriver` and `PostgresAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/postgres/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `PostgresSyncConfig` and `PostgresAsyncConfig` resolve `adapter_name = 'vertebra_postgres'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — Native PostgreSQL sync and async drivers.
[^postgres-core]: `src/py/vertebra/db/adapters/postgres/core.py` — Native PostgreSQL core parameter and wire helpers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
