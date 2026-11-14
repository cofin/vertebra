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
plan_commit: 104fd6fa
depends_on:
- sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t03
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
- src/py/vertebra/db/adapters/oracle/driver.py
- src/py/vertebra/db/adapters/oracle/core.py
- src/py/vertebra/db/adapters/oracle/notify.py
- src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py
- src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
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
  resource: src/py/vertebra/db/adapters/oracle/driver.py
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
---

# Task `t04`: Deliver Feature-Complete Oracle SQLSpec Adapter Package

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{core,driver,data_dictionary}.py` (`OracleSyncDriver`, `OracleAsyncDriver`, `OracleSyncDataDictionary`, `OracleAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^oracle-drv] [^oracle-core] [^ora-notify].

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^oracle-drv] [^storage-art] [^ora-notify].

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{config,__init__}.py` declaring `adapter_name = 'vertebra_oracle'` and explicit store class attributes per [`../spec.md`](../spec.md) [^oracle-drv] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/oracle/driver.py` & `src/py/vertebra/db/adapters/oracle/core.py` — native Oracle execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py` — Oracle SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='oracle')` supporting `{ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}` (`:name` dicts and `:1` tuples), unwrap `TypedParameter` before `OracleCore` JSON/`array('f')` `VECTOR` encoding, map `ORA-00001`/`ORA-02291`/`ORA-00060`/`ORA-08177`, and implement `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` (23ai `OSON` JSON vs 19c `BLOB CHECK`). Keep all helpers local to `adapters/oracle/core.py`.
- `OracleSyncDataDictionary(SyncDataDictionaryBase)` and `OracleAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/oracle/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement SDK-free `OracleSyncADKStore`/`OracleAsyncADKStore` (`MERGE INTO` upserts, `ORA-00955`/`ORA-00942` DDL guards), 23ai `VECTOR_DISTANCE`/RRF `OracleSyncADKMemoryStore`/`OracleAsyncADKMemoryStore`, `OracleSyncADKArtifactStore`/`OracleAsyncADKArtifactStore`, `OracleEventQueueStore` + `OracleSyncEventsBackend`/`OracleAsyncEventsBackend` over `SyncOracleNotificationSource`/`AsyncOracleNotificationSource` (zero `oracledb` imports), and `OracleLitestarStore`.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `OracleSyncConfig` and `OracleAsyncConfig` declaring `adapter_name = 'vertebra_oracle'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_oracle'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `OracleSyncADKStore`/`OracleAsyncADKStore`, memory search, `ArtifactReference` metadata, `OracleEventQueueStore`, and `OracleLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

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

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Native Oracle core parameter and wire helpers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
