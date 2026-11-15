---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-relational-adapters
title: 'Chapter 02: Per-Engine SQLSpec Adapter Packages for PostgreSQL, SQLite, MySQL, and Oracle'
description: Deliver feature-complete per-engine SQLSpec packages (core, sync/async drivers, DataDictionary, ADK/Events/Litestar stores, and explicit adapter_name configs) for PostgreSQL, SQLite, MySQL (fixing ? qmark binds), and Oracle.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlspec
- postgres
- adk
- events
- litestar
- sqlite
- mysql
- oracle
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
- id: sqlite-drv
  resource: src/py/vertebra/db/adapters/sqlite/driver.py
  title: Native SQLite sync and async drivers
- id: sqlite-core
  resource: src/py/vertebra/db/adapters/sqlite/core.py
  title: Native SQLite core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Existing SQLite SQLSpec adapter
- id: mysql-drv
  resource: src/py/vertebra/db/adapters/mysql/driver.py
  title: Native MySQL & MariaDB sync and async drivers
- id: mysql-core
  resource: src/py/vertebra/db/adapters/mysql/core.py
  title: Native MySQL & MariaDB core parameter and wire helpers
- id: oracle-drv
  resource: src/py/vertebra/db/adapters/oracle/driver.py
  title: Native Oracle sync and async drivers
- id: oracle-core
  resource: src/py/vertebra/db/adapters/oracle/core.py
  title: Native Oracle core parameter and wire helpers
- id: ora-notify
  resource: src/py/vertebra/db/adapters/oracle/notify.py
  title: Oracle TxEventQ notification source
---

# Chapter 02: Per-Engine SQLSpec Adapter Packages for PostgreSQL, SQLite, MySQL, and Oracle

<!-- truth: start -->
- **Baseline (`18baadcd`)**: PostgreSQL SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/postgres/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^pg-notify].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/postgres/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/postgres/driver.py` with `adapter_name = 'vertebra_postgres'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^postgres-drv] [^postgres-core].
- **Baseline (`18baadcd`)**: SQLite SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/sqlite/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/sqlite/driver.py` with `adapter_name = 'vertebra_sqlite'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^sqlite-drv] [^sqlite-core].
- **Baseline (`18baadcd`)**: MySQL & MariaDB SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/mysql/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/mysql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/mysql/driver.py` with `adapter_name = 'vertebra_mysql'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^mysql-drv] [^mysql-core].
- **Baseline (`18baadcd`)**: Oracle SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/oracle/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^ora-notify].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/oracle/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/oracle/driver.py` with `adapter_name = 'vertebra_oracle'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^oracle-drv] [^oracle-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/postgres/` SQLSpec adapter package over Vertebra's native PostgreSQL driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py` [^postgres-drv] [^postgres-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/sqlite/` SQLSpec adapter package over Vertebra's native SQLite driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py` [^sqlite-drv] [^sqlite-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/mysql/` SQLSpec adapter package over Vertebra's native MySQL & MariaDB driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py` [^mysql-drv] [^mysql-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/oracle/` SQLSpec adapter package over Vertebra's native Oracle driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py` [^oracle-drv] [^oracle-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='postgres')` with default and execution `ParameterStyle.NUMERIC` (`$1, $2` positional sequences matching `vertebra._postgres` instead of legacy `%s`), native Arrow `COPY BINARY` `select_to_arrow`, server-side portal `dispatch_select_stream`, pipeline `execute_stack`, and savepoints. Duplicate all PostgreSQL SQL/parameter helpers locally inside `adapters/postgres/core.py` (zero cross-adapter helper modules).
- **Sub-Extension Stores**: Subclass SDK-free upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, pgvector cosine/HNSW + `tsvector` `BaseSyncADKMemoryStore`/`BaseAsyncADKMemoryStore`, `BaseSyncADKArtifactStore`/`BaseAsyncADKArtifactStore` returning `ArtifactReference` on the shared surface, `BaseEventQueueStore` + `PostgresSyncEventsBackend`/`PostgresAsyncEventsBackend` over `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and `BaseSQLSpecStore` `ON CONFLICT (key) DO UPDATE`) with zero `psycopg` imports.
- **Config & Lazy Exports**: Define `PostgresSyncConfig` and `PostgresAsyncConfig` declaring `adapter_name = 'vertebra_postgres'`, `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, using PEP 562 `__getattr__` in `__init__.py` so `google.adk` is loaded lazily and guarded via `MissingDependencyError`.

```python
class PostgresSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "postgres"

class PostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "postgres"

class PostgresSyncDriver(BaseSyncDriver):
    dialect = "postgres"

class PostgresAsyncDriver(BaseAsyncDriver):
    dialect = "postgres"

class PostgresSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_postgres"
    driver_type = PostgresSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class PostgresAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_postgres"
    driver_type = PostgresAsyncDriver
```

- **Driver & DataDictionary**: Set `StatementConfig(dialect='sqlite')` supporting execution `{ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}` (`?` tuples and `:name` dicts without unnecessary rewriting), `PRAGMA` row-returning classification, WAL/foreign_keys/busy_timeout pragmas, and savepoints. Keep all SQLite SQL/parameter helpers strictly inside `adapters/sqlite/core.py`.
- **Sub-Extension Stores**: Implement `SQLiteSyncADKStore`/`SQLiteAsyncADKStore`, `FTS5`-backed `SQLiteSyncADKMemoryStore`/`SQLiteAsyncADKMemoryStore`, `SQLiteSyncADKArtifactStore`/`SQLiteAsyncADKArtifactStore` (`ArtifactReference`), `SQLiteEventQueueStore`, and `SQLiteLitestarStore` (`ON CONFLICT(key) DO UPDATE`) executing exclusively via `provide_session()` over native `SyncSQLiteDriver`/`AsyncSQLiteDriver`.
- **Config & Lazy Exports**: Define `SQLiteSyncConfig` and `SQLiteAsyncConfig` declaring `adapter_name = 'vertebra_sqlite'`, explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, shared `:memory:` URI handling, and PEP 562 lazy `__init__.py`.

```python
class SQLiteSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "sqlite"

class SQLiteAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "sqlite"

class SQLiteSyncDriver(BaseSyncDriver):
    dialect = "sqlite"

class SQLiteAsyncDriver(BaseAsyncDriver):
    dialect = "sqlite"

class SQLiteSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_sqlite"
    driver_type = SQLiteSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class SQLiteAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_sqlite"
    driver_type = SQLiteAsyncDriver
```

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

- **Driver & DataDictionary**: Set `StatementConfig(dialect='oracle')` supporting `{ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}` (`:name` dicts and `:1` tuples), unwrap `TypedParameter` before `OracleCore` JSON/`array('f')` `VECTOR` encoding, map `ORA-00001`/`ORA-02291`/`ORA-00060`/`ORA-08177`, and implement `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` (23ai `OSON` JSON vs 19c `BLOB CHECK`). Keep all helpers local to `adapters/oracle/core.py`.
- **Sub-Extension Stores**: Implement SDK-free `OracleSyncADKStore`/`OracleAsyncADKStore` (`MERGE INTO` upserts, `ORA-00955`/`ORA-00942` DDL guards), 23ai `VECTOR_DISTANCE`/RRF `OracleSyncADKMemoryStore`/`OracleAsyncADKMemoryStore`, `OracleSyncADKArtifactStore`/`OracleAsyncADKArtifactStore`, `OracleEventQueueStore` + `OracleSyncEventsBackend`/`OracleAsyncEventsBackend` over `SyncOracleNotificationSource`/`AsyncOracleNotificationSource` (zero `oracledb` imports), and `OracleLitestarStore`.
- **Config & Lazy Exports**: Define `OracleSyncConfig` and `OracleAsyncConfig` declaring `adapter_name = 'vertebra_oracle'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class OracleSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "oracle"

class OracleAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "oracle"

class OracleSyncDriver(BaseSyncDriver):
    dialect = "oracle"

class OracleAsyncDriver(BaseAsyncDriver):
    dialect = "oracle"

class OracleSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_oracle"
    driver_type = OracleSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class OracleAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_oracle"
    driver_type = OracleAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch04-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Deliver Feature-Complete PostgreSQL SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t02` | Deliver Feature-Complete SQLite SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/sqlite/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Deliver Feature-Complete MySQL SQLSpec Adapter Package with QMARK ? Binds | `src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t04` | Deliver Feature-Complete Oracle SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-postgres-sqlspec-adapter.md](tasks/t01-postgres-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/postgres/ ($1 NUMERIC, Arrow COPY, portals, DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_postgres').
- [ ] [t02-sqlite-sqlspec-adapter.md](tasks/t02-sqlite-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/sqlite/ (QMARK + NAMED_COLON, WAL pragmas, DataDictionary, FTS5 ADK/Events/Litestar stores, adapter_name='vertebra_sqlite').
- [ ] [t03-mysql-sqlspec-adapter.md](tasks/t03-mysql-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/mysql/ (fixing %s -> ParameterStyle.QMARK ?, MariaDB/MySQL DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_mysql').
- [ ] [t04-oracle-sqlspec-adapter.md](tasks/t04-oracle-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/oracle/ (:name/:1 binds, 23ai VECTOR/OSON DataDictionary, TxEventQ Events/ADK/Litestar stores, adapter_name='vertebra_oracle').

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-sqlspec-relational-adapters` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^postgres-core]: `src/py/vertebra/db/adapters/postgres/core.py` — Native PostgreSQL core parameter and wire helpers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Native SQLite sync and async drivers.
[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py` — Native SQLite core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing SQLite SQLSpec adapter.
[^mysql-drv]: `src/py/vertebra/db/adapters/mysql/driver.py` — Native MySQL & MariaDB sync and async drivers.
[^mysql-core]: `src/py/vertebra/db/adapters/mysql/core.py` — Native MySQL & MariaDB core parameter and wire helpers.
[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Native Oracle core parameter and wire helpers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
