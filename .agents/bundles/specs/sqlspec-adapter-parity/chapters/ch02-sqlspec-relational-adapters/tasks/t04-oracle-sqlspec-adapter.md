---
type: Task
id: sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t04
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-relational-adapters
title: Deliver Feature-Complete Oracle SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/oracle/ (:name/:1 binds, 23ai VECTOR/OSON DataDictionary, TxEventQ Events/ADK/Litestar stores, adapter_name='vertebra_oracle').
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t03
files:
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/data_dictionary.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/litestar.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/oracle/_sync.py
  - src/py/vertebra/db/adapters/oracle/_async.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/storage/_store.py
estimated_loc: 245
tags:
  - sqlspec
  - oracle
  - data-dictionary
  - adk
  - events
  - litestar
  - config
sources:
  - id: oracle-drv
    resource: src/py/vertebra/db/adapters/oracle/_sync.py
    title: Native Oracle sync and async drivers
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py
    title: Native Oracle core parameter and wire helpers
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: Oracle TxEventQ notification source
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: Storage-neutral ArtifactReference value struct
verification_strategy: regression_tdd
---

# Task `t04`: Deliver Feature-Complete Oracle SQLSpec Adapter Package

## Objective & Scope Boundary

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/oracle/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`OracleSyncDriver`/`OracleAsyncDriver`, `OracleSyncDataDictionary`/`OracleAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `OracleSyncConfig`/`OracleAsyncConfig`) per [`../spec.md`](../spec.md) [^oracle-drv] [^oracle-core] [^ora-notify] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — native Oracle sync and async execution drivers.
- `src/py/vertebra/db/adapters/oracle/core.py` — native Oracle parameter, JSON, and `array('f')` `VECTOR` wire helpers.
- `src/py/vertebra/db/adapters/oracle/notify.py` — `SyncOracleNotificationSource` and `AsyncOracleNotificationSource` TxEventQ channels.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy Oracle SQLSpec logic to replace cleanly.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='oracle')` supporting `{ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}` (`:name` dicts and `:1` tuples), unwrap `TypedParameter` before `OracleCore` JSON/`array('f')` `VECTOR` encoding, map `ORA-00001`/`ORA-02291`/`ORA-00060`/`ORA-08177`, and implement `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` (23ai `OSON` JSON vs 19c `BLOB CHECK`) executing upstream `sqlspec/data_dictionary/dialects/oracle/sql/*.sql` catalog queries. Keep all helpers local to `adapters/oracle/core.py`.
- Implement SDK-free `OracleSyncADKStore`/`OracleAsyncADKStore` (`MERGE INTO` upserts, `ORA-00955`/`ORA-00942` DDL guards), 23ai `VECTOR_DISTANCE`/RRF `OracleSyncADKMemoryStore`/`OracleAsyncADKMemoryStore`, `OracleSyncADKArtifactStore`/`OracleAsyncADKArtifactStore` (`ArtifactReference`), `OracleEventQueueStore` + `OracleSyncEventsBackend`/`OracleAsyncEventsBackend` over `SyncOracleNotificationSource`/`AsyncOracleNotificationSource`, and `OracleLitestarStore` executing via `self._config.provide_session()` with zero `oracledb` or `sqlspec.adapters.*` imports.
- Define `OracleSyncConfig` and `OracleAsyncConfig` declaring `adapter_name = 'vertebra_oracle'`, explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class`, and PEP 562 lazy `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized `:name`/`:1` placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` catalog tests, store contract tests (ADK `MERGE INTO`/23ai `VECTOR_DISTANCE`/`ArtifactReference`/TxEventQ/Litestar), PEP 562 lazy `google.adk` import tests, and resolver contract checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `OracleSyncDriver` and `OracleAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/oracle/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `OracleSyncConfig` and `OracleAsyncConfig` resolve `adapter_name = 'vertebra_oracle'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — Native Oracle sync and async drivers.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Native Oracle core parameter and wire helpers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
