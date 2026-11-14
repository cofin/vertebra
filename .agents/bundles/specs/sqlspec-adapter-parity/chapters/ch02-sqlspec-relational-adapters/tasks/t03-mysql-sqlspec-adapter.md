---
type: Task
id: sqlspec-adapter-parity:ch02-sqlspec-relational-adapters:t03
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-relational-adapters
title: Deliver Feature-Complete MySQL SQLSpec Adapter Package with QMARK ? Binds
description: Build extensions/sqlspec/adapters/mysql/ (fixing %s -> ParameterStyle.QMARK ?, MariaDB/MySQL DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_mysql').
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
- src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/adk.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/events.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/litestar.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlspec/_driver.py
- src/py/vertebra/db/adapters/mysql/driver.py
- src/py/vertebra/db/adapters/mysql/core.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py
- src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py
estimated_loc: 245
tags:
- sqlspec
- mysql
- data-dictionary
- adk
- events
- litestar
- config
sources:
- id: mysql-drv
  resource: src/py/vertebra/db/adapters/mysql/driver.py
  title: Native MySQL & MariaDB sync and async drivers
- id: mysql-core
  resource: src/py/vertebra/db/adapters/mysql/core.py
  title: Native MySQL & MariaDB core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Legacy MySQL SQLSpec adapter setting broken %s execution style
- id: storage-art
  resource: src/py/vertebra/storage/_store.py
  title: Storage-neutral ArtifactReference value struct
---

# Task `t03`: Deliver Feature-Complete MySQL SQLSpec Adapter Package with QMARK ? Binds

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{core,driver,data_dictionary}.py` (`MySQLSyncDriver`, `MySQLAsyncDriver`, `MySQLSyncDataDictionary`, `MySQLAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^mysql-drv] [^mysql-core] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^mysql-drv] [^storage-art] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{config,__init__}.py` declaring `adapter_name = 'vertebra_mysql'` and explicit store class attributes per [`../spec.md`](../spec.md) [^mysql-drv] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/mysql/driver.py` & `src/py/vertebra/db/adapters/mysql/core.py` — native MySQL & MariaDB execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py` — MySQL & MariaDB SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/mysql/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/mysql/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/mysql/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Fix the critical `%s` bug by setting `StatementConfig(dialect='mysql')` default and execution style to `ParameterStyle.QMARK` (`?` positional tuples required by `vertebra._mysql` binary prepared statements), map errno `1062`/`1451`/`1452`/`1213` to `sqlspec.exceptions.*`, and implement `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` (MySQL 8/9 vs MariaDB JSON/version flags). Duplicate MySQL-family helpers strictly inside `adapters/mysql/core.py` (zero cross-adapter shared helper modules).
- `MySQLSyncDataDictionary(SyncDataDictionaryBase)` and `MySQLAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/mysql/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement SDK-free `MySQLSyncADKStore`/`MySQLAsyncADKStore`, `FULLTEXT` / MySQL 9 `VECTOR` `MySQLSyncADKMemoryStore`/`MySQLAsyncADKMemoryStore`, `MySQLSyncADKArtifactStore`/`MySQLAsyncADKArtifactStore` (`ArtifactReference`), `MySQLEventQueueStore` (companion `ALTER TABLE ... ADD INDEX` DDL), and MariaDB-compatible `MySQLLitestarStore` (`ON DUPLICATE KEY UPDATE`) with zero `pymysql`/`asyncmy` imports.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `MySQLSyncConfig` and `MySQLAsyncConfig` declaring `adapter_name = 'vertebra_mysql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_mysql'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `MySQLSyncADKStore`/`MySQLAsyncADKStore`, memory search, `ArtifactReference` metadata, `MySQLEventQueueStore`, and `MySQLLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `MySQLSyncDriver` and `MySQLAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/mysql/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/mysql/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `MySQLSyncConfig` and `MySQLAsyncConfig` resolve `adapter_name = 'vertebra_mysql'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^mysql-drv]: `src/py/vertebra/db/adapters/mysql/driver.py` — Native MySQL & MariaDB sync and async drivers.
[^mysql-core]: `src/py/vertebra/db/adapters/mysql/core.py` — Native MySQL & MariaDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Legacy MySQL SQLSpec adapter setting broken %s execution style.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
