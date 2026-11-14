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
plan_commit: 104fd6fa
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
- src/py/vertebra/db/adapters/postgres/driver.py
- src/py/vertebra/db/adapters/postgres/core.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py
- src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py
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
  resource: src/py/vertebra/db/adapters/postgres/driver.py
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
---

# Task `t01`: Deliver Feature-Complete PostgreSQL SQLSpec Adapter Package

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{core,driver,data_dictionary}.py` (`PostgresSyncDriver`, `PostgresAsyncDriver`, `PostgresSyncDataDictionary`, `PostgresAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^postgres-drv] [^postgres-core] [^pg-notify].

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^postgres-drv] [^storage-art] [^pg-notify].

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{config,__init__}.py` declaring `adapter_name = 'vertebra_postgres'` and explicit store class attributes per [`../spec.md`](../spec.md) [^postgres-drv] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/postgres/driver.py` & `src/py/vertebra/db/adapters/postgres/core.py` — native PostgreSQL execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py` — PostgreSQL SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='postgres')` with default and execution `ParameterStyle.NUMERIC` (`$1, $2` positional sequences matching `vertebra._postgres` instead of legacy `%s`), native Arrow `COPY BINARY` `select_to_arrow`, server-side portal `dispatch_select_stream`, pipeline `execute_stack`, and savepoints. Duplicate all PostgreSQL SQL/parameter helpers locally inside `adapters/postgres/core.py` (zero cross-adapter helper modules).
- `PostgresSyncDataDictionary(SyncDataDictionaryBase)` and `PostgresAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/postgres/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Subclass SDK-free upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, pgvector cosine/HNSW + `tsvector` `BaseSyncADKMemoryStore`/`BaseAsyncADKMemoryStore`, `BaseSyncADKArtifactStore`/`BaseAsyncADKArtifactStore` returning `ArtifactReference` on the shared surface, `BaseEventQueueStore` + `PostgresSyncEventsBackend`/`PostgresAsyncEventsBackend` over `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and `BaseSQLSpecStore` `ON CONFLICT (key) DO UPDATE`) with zero `psycopg` imports.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `PostgresSyncConfig` and `PostgresAsyncConfig` declaring `adapter_name = 'vertebra_postgres'`, `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, using PEP 562 `__getattr__` in `__init__.py` so `google.adk` is loaded lazily and guarded via `MissingDependencyError`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_postgres'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `PostgresSyncDataDictionary`/`PostgresAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `PostgresSyncADKStore`/`PostgresAsyncADKStore`, memory search, `ArtifactReference` metadata, `PostgresEventQueueStore`, and `PostgresLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

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

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^postgres-core]: `src/py/vertebra/db/adapters/postgres/core.py` — Native PostgreSQL core parameter and wire helpers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
