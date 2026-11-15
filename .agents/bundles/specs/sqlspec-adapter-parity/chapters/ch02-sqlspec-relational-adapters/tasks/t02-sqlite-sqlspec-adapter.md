---
type: Task
id: sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t02
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-relational-adapters
title: Deliver Feature-Complete SQLite SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/sqlite/ (QMARK + NAMED_COLON, WAL pragmas, DataDictionary, FTS5 ADK/Events/Litestar stores, adapter_name='vertebra_sqlite').
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t03
files:
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/data_dictionary.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/adk.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/events.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/litestar.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/config.py
- src/py/vertebra/extensions/sqlspec/adapters/sqlite/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlspec/_driver.py
- src/py/vertebra/db/adapters/sqlite/driver.py
- src/py/vertebra/db/adapters/sqlite/core.py
- src/py/vertebra/db/adapters/sqlite/config.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/storage/_store.py
estimated_loc: 245
tags:
- sqlspec
- sqlite
- data-dictionary
- adk
- events
- litestar
- config
sources:
- id: sqlite-drv
  resource: src/py/vertebra/db/adapters/sqlite/driver.py
  title: Native SQLite sync and async drivers
- id: sqlite-core
  resource: src/py/vertebra/db/adapters/sqlite/core.py
  title: Native SQLite core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Existing SQLite SQLSpec adapter
- id: storage-art
  resource: src/py/vertebra/storage/_store.py
  title: Storage-neutral ArtifactReference value struct
---

# Task `t02`: Deliver Feature-Complete SQLite SQLSpec Adapter Package

## Objective & Scope Boundary

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`SQLiteSyncDriver`/`SQLiteAsyncDriver`, `SQLiteSyncDataDictionary`/`SQLiteAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `SQLiteSyncConfig`/`SQLiteAsyncConfig`) per [`../spec.md`](../spec.md) [^sqlite-drv] [^sqlite-core] [^sqlspec-adapters] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/sqlite/driver.py` — native SQLite sync and async execution drivers.
- `src/py/vertebra/db/adapters/sqlite/core.py` — native SQLite parameter and pragma helpers.
- `src/py/vertebra/db/adapters/sqlite/config.py` — `SQLiteConfig` connection and `:memory:` options.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy SQLite SQLSpec logic to replace cleanly.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='sqlite')` supporting execution `{ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}` (`?` tuples and `:name` dicts without unnecessary rewriting), `PRAGMA` row-returning classification, WAL/foreign_keys/busy_timeout pragmas, savepoints, and `SQLiteSyncDataDictionary`/`SQLiteAsyncDataDictionary` executing upstream `sqlspec/data_dictionary/dialects/sqlite/sql/*.sql` catalog queries. Keep all SQLite SQL/parameter helpers strictly inside `adapters/sqlite/core.py`.
- Implement `SQLiteSyncADKStore`/`SQLiteAsyncADKStore`, `FTS5`-backed `SQLiteSyncADKMemoryStore`/`SQLiteAsyncADKMemoryStore`, `SQLiteSyncADKArtifactStore`/`SQLiteAsyncADKArtifactStore` (`ArtifactReference`), `SQLiteEventQueueStore`, and `SQLiteLitestarStore` (`ON CONFLICT(key) DO UPDATE`) executing exclusively via `self._config.provide_session()` over native `SyncSQLiteDriver`/`AsyncSQLiteDriver` with zero `sqlspec.adapters.*` or third-party DB SDK imports.
- Define `SQLiteSyncConfig` and `SQLiteAsyncConfig` declaring `adapter_name = 'vertebra_sqlite'`, explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class`, shared `:memory:` URI handling, and PEP 562 lazy `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, `SQLiteSyncDataDictionary`/`SQLiteAsyncDataDictionary` catalog tests, store contract tests (ADK/FTS5 memory/`ArtifactReference`/events/Litestar TTL/upsert), PEP 562 lazy `google.adk` import tests, and resolver contract checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `SQLiteSyncDriver` and `SQLiteAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/sqlite/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `SQLiteSyncConfig` and `SQLiteAsyncConfig` resolve `adapter_name = 'vertebra_sqlite'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Native SQLite sync and async drivers.
[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py` — Native SQLite core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing SQLite SQLSpec adapter.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
