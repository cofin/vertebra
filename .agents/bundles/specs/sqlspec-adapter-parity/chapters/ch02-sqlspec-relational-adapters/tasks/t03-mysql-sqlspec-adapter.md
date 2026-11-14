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
plan_commit: 1b191388
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
- src/py/vertebra/db/adapters/mysql/config.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/storage/_store.py
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

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/mysql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`MySQLSyncDriver`/`MySQLAsyncDriver`, `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `MySQLSyncConfig`/`MySQLAsyncConfig`) per [`../spec.md`](../spec.md) [^mysql-drv] [^mysql-core] [^sqlspec-adapters] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/mysql/driver.py` — native MySQL & MariaDB sync and async execution drivers.
- `src/py/vertebra/db/adapters/mysql/core.py` — native MySQL & MariaDB binary `?` placeholder and parameter helpers.
- `src/py/vertebra/db/adapters/mysql/config.py` — `MySQLConfig` connection options.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy MySQL adapter setting `%s` execution style to fix and remove.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Fix the critical `%s` bug by setting `StatementConfig(dialect='mysql')` default and execution style to `ParameterStyle.QMARK` (`?` positional tuples required by `vertebra._mysql` binary prepared statements), map errno `1062`/`1451`/`1452`/`1213` to `sqlspec.exceptions.*`, and implement `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` (MySQL 8/9 vs MariaDB JSON/version flags) executing upstream `sqlspec/data_dictionary/dialects/mysql/sql/*.sql` catalog queries. Duplicate MySQL-family helpers strictly inside `adapters/mysql/core.py` (zero cross-adapter shared helper modules).
- Implement SDK-free `MySQLSyncADKStore`/`MySQLAsyncADKStore`, `FULLTEXT` / MySQL 9 `VECTOR` `MySQLSyncADKMemoryStore`/`MySQLAsyncADKMemoryStore`, `MySQLSyncADKArtifactStore`/`MySQLAsyncADKArtifactStore` (`ArtifactReference`), `MySQLEventQueueStore` (companion `ALTER TABLE ... ADD INDEX` DDL), and MariaDB-compatible `MySQLLitestarStore` (`ON DUPLICATE KEY UPDATE`) executing via `self._config.provide_session()` with zero `pymysql`/`asyncmy`/`sqlspec.adapters.*` imports.
- Define `MySQLSyncConfig` and `MySQLAsyncConfig` declaring `adapter_name = 'vertebra_mysql'`, explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class`, and PEP 562 lazy `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized QMARK `?` compilation, `TypedParameter` execution, zero-row `SELECT`, DML, `StatementConfig` override preservation, `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` catalog tests, store contract tests (ADK/FULLTEXT/`ArtifactReference`/events/Litestar `ON DUPLICATE KEY UPDATE`), PEP 562 lazy `google.adk` import tests, and resolver contract checks.

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
