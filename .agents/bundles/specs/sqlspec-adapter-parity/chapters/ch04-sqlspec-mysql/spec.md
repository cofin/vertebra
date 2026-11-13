---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-mysql
title: "Chapter 04: SQLSpec MySQL & MariaDB Adapter Package (mysql)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/mysql/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_mysql' configs)."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:50:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:50:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:50:00Z"
    commit: 18baadcd
tags:
  - sqlspec
  - mysql
  - adk
  - events
  - litestar
sources:
  - id: mysql-drv
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: "Native MySQL & MariaDB sync and async drivers"
  - id: mysql-core
    resource: src/py/vertebra/db/adapters/mysql/core.py
    title: "Native MySQL & MariaDB core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Legacy MySQL SQLSpec adapter setting broken %s execution style"
---

# Chapter 04: SQLSpec MySQL & MariaDB Adapter Package (mysql)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: MySQL & MariaDB SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/mysql/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/mysql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/mysql/driver.py` with `adapter_name = 'vertebra_mysql'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^mysql-drv] [^mysql-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/mysql/` SQLSpec adapter package over Vertebra's native MySQL & MariaDB driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py` [^mysql-drv] [^mysql-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Fix the critical `%s` bug by setting `StatementConfig(dialect='mysql')` default and execution style to `ParameterStyle.QMARK` (`?` positional tuples required by `vertebra._mysql` binary prepared statements), map errno `1062`/`1451`/`1452`/`1213` to `sqlspec.exceptions.*`, and implement `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` (MySQL 8/9 vs MariaDB JSON/version flags). Duplicate MySQL-family helpers strictly inside `adapters/mysql/core.py` (zero cross-adapter shared helper modules).
- **Sub-Extension Stores**: Implement SDK-free `MySQLSyncADKStore`/`MySQLAsyncADKStore`, `FULLTEXT` / MySQL 9 `VECTOR` `MySQLSyncADKMemoryStore`/`MySQLAsyncADKMemoryStore`, `MySQLSyncADKArtifactStore`/`MySQLAsyncADKArtifactStore` (`ArtifactReference`), `MySQLEventQueueStore` (companion `ALTER TABLE ... ADD INDEX` DDL), and MariaDB-compatible `MySQLLitestarStore` (`ON DUPLICATE KEY UPDATE`) with zero `pymysql`/`asyncmy` imports.
- **Config & Lazy Exports**: Define `MySQLSyncConfig` and `MySQLAsyncConfig` declaring `adapter_name = 'vertebra_mysql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class MySQLSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "mysql"

class MySQLAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "mysql"

class MySQLSyncDriver(BaseSyncDriver):
    dialect = "mysql"

class MySQLAsyncDriver(BaseAsyncDriver):
    dialect = "mysql"

class MySQLSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mysql"
    driver_type = MySQLSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class MySQLAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mysql"
    driver_type = MySQLAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| MySQL & MariaDB driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/mysql/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| MySQL & MariaDB ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/mysql/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| MySQL & MariaDB configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/mysql/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-mysql-driver-and-dictionary.md](tasks/t01-mysql-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{core,driver,data_dictionary}.py`.
- [ ] [t02-mysql-extension-stores.md](tasks/t02-mysql-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{adk,events,litestar}.py`.
- [ ] [t03-mysql-config-and-barrel.md](tasks/t03-mysql-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{config,__init__}.py` (`adapter_name = 'vertebra_mysql'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch04-sqlspec-mysql` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^mysql-drv]: `src/py/vertebra/db/adapters/mysql/driver.py` — Native MySQL & MariaDB sync and async drivers.
[^mysql-core]: `src/py/vertebra/db/adapters/mysql/core.py` — Native MySQL & MariaDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Legacy MySQL SQLSpec adapter setting broken %s execution style.
