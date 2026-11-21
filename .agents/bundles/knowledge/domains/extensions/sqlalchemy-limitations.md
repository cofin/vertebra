---
type: Knowledge
title: SQLAlchemy Dialects, DB-API & Alembic Limitations
description: Operational boundaries for greenlet-required SQLAlchemy execution, statement-autocommit backends, disabled Spangres two-phase commit, required dialect extras, and online Spanner DDL batching.
status: stable
tags:
  - domains
  - extensions
  - sqlalchemy
  - dbapi
  - alembic
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T05:55:00Z"
last_reviewed_commit: 06e99fbf
verified:
  - by: agent:bb-executor
    at: "2026-11-21T01:17:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-17T05:55:00Z"
    commit: 5a833832
sources:
  - id: src-extensions-sqlalchemy-async-adapter
    resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py
    title: SQLAlchemy asyncio DBAPI adapter with greenlet and termination support
  - id: src-extensions-sqlalchemy-dialects-base
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
    title: Shared SQLAlchemy 2.1 dialect mixins and disconnect detection
  - id: src-extensions-sqlalchemy-pool
    resource: src/py/vertebra/extensions/sqlalchemy/pool.py
    title: BrokerPool and AsyncBrokerPool SQLAlchemy Pool implementations
  - id: src-extensions-sqlalchemy-duckdb
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py
    title: DuckDB dialect requiring duckdb_engine via sqlalchemy-duckdb extra
  - id: src-extensions-sqlalchemy-bigquery
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py
    title: BigQuery dialect requiring sqlalchemy_bigquery via sqlalchemy-gcp extra
  - id: src-extensions-sqlalchemy-spanner
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py
    title: Spanner and Spangres dialects and disabled two-phase commit
  - id: src-extensions-sqlalchemy-oracle
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py
    title: Oracle dialect version lookup and cursor.var() RETURNING execution context
  - id: src-extensions-sqlalchemy-mssql
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py
    title: SQL Server dialect version lookup and OUTPUT nextset() execution context
  - id: src-extensions-sqlalchemy-sqlite
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
    title: SQLite StaticPool selection and transaction-free do_ping
  - id: src-extensions-sqlalchemy-postgresql
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py
    title: PostgreSQL dialect cast colspecs and base insert_executemany_returning
  - id: src-extensions-sqlalchemy-alembic-impl
    resource: src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
    title: Alembic DefaultImpl subclasses and Spanner online DDL batching
  - id: src-db-dbapi-sync
    resource: src/py/vertebra/db/dbapi/_sync.py
    title: Synchronous PEP 249 module, native cursor handoff, transaction state, and native exception re-exports
  - id: src-db-dbapi-async
    resource: src/py/vertebra/db/dbapi/_async.py
    title: Asynchronous PEP 249 module, native cursor handoff, and transaction control
---

# SQLAlchemy Dialects, DB-API & Alembic Limitations

<!-- truth: start -->
- `SyncDBAPIModule` and `AsyncDBAPIModule` supply module globals, type objects, constructors, `connect`, and transaction control, while `connection.cursor()` returns the native cursor; autocommit follows the connection's transaction state and no Python code scans SQL text [^src-db-dbapi-sync] [^src-db-dbapi-async].
- DB-API exception names are the `vertebra._vertebra` classes and the extension raises them with no translation; this deviates from PEP 249 because `InterfaceError` is a subclass of `DatabaseError` and `Error` is `DatabaseError` [^src-db-dbapi-sync].
- `SyncEngineAdapter` and `attach_serverless_disconnect_guard` were removed because SQLAlchemy 2.1 provides no greenlet-free async engine seam; async engines require `AsyncAdapt_vertebra_connection` inside greenlets, and disconnect recovery uses `is_disconnect()` plus `pool_pre_ping` [^src-extensions-sqlalchemy-async-adapter] [^src-extensions-sqlalchemy-dialects-base].
- `BigQuery`, `Spanner`, and `Spangres` use statement autocommit and reject `autocommit=False` with PEP 249 `NotSupportedError`, while `Spangres` dialects also disable two-phase commit (`supports_twophase = False`) [^src-db-dbapi-sync] [^src-extensions-sqlalchemy-spanner].
- DuckDB, BigQuery, and Spanner dialects require `vertebra-core[sqlalchemy-duckdb]` or `vertebra-core[sqlalchemy-gcp]` via `ensure_module` and fail loudly with `MissingDependencyError` when absent, with zero `DefaultDialect` fallbacks [^src-extensions-sqlalchemy-duckdb] [^src-extensions-sqlalchemy-bigquery] [^src-extensions-sqlalchemy-spanner].
- `SpannerAlembicImpl._exec` batches `DDLElement` statements into `update_ddl` only in online mode, whereas `--sql` offline mode (`as_sql=True`) emits unbatched SQL statements [^src-extensions-sqlalchemy-alembic-impl].
- `OracleExecutionContext_vtb` requires `cursor.var()` out-binds for `RETURNING ... INTO`, `MSExecutionContext_vtb` caps `nextset()` buffering at 16 steps, and SQLite `do_ping` runs under temporary `autocommit=True` [^src-extensions-sqlalchemy-oracle] [^src-extensions-sqlalchemy-mssql] [^src-extensions-sqlalchemy-sqlite].
- PostgreSQL `insert_executemany_returning`, `do_begin`, `DELETE ... USING`, built-in Alembic `DefaultImpl` classes (PostgreSQL, MySQL, SQLite, Oracle, MSSQL), and `BrokerPool.reset_on_return` delegate directly to SQLAlchemy 2.1 and Alembic 1.18+ [^src-extensions-sqlalchemy-postgresql] [^src-extensions-sqlalchemy-pool] [^src-extensions-sqlalchemy-alembic-impl].
- Removing `disallow_subclassing_any = false` from `pyproject.toml` surfaces `mypy` `[misc]` errors on `SyncSpannerDialect`/`AsyncSpannerDialect` and `SyncBigQueryDialect`/`AsyncBigQueryDialect` because `google.cloud.sqlalchemy_spanner` and `sqlalchemy_bigquery` remain under `ignore_missing_imports = true` [^src-extensions-sqlalchemy-spanner] [^src-extensions-sqlalchemy-bigquery].
<!-- truth: end -->

## Operational Boundaries

- **DB-API Native Cursor Seam, Autocommit & Exception Hierarchy**: [`SyncDBAPIModule`](../../../../../src/py/vertebra/db/dbapi/_sync.py)[^src-db-dbapi-sync] and [`AsyncDBAPIModule`](../../../../../src/py/vertebra/db/dbapi/_async.py)[^src-db-dbapi-async] supply module globals, type objects, constructors, `connect`, and transaction control, while `connection.cursor()` returns the native cursor directly. Autocommit follows the connection's transaction state and no Python code scans SQL text. DB-API exception names are the `vertebra._vertebra` classes, which the extension raises with no translation; this deviates from PEP 249 because `InterfaceError` is a subclass of `DatabaseError` and `Error` is `DatabaseError`.
- **Greenlet Requirement and Disconnect Recovery**: SQLAlchemy 2.1 async execution always enters through `AsyncAdapt_vertebra_connection` (`AsyncAdapt_terminate`) and SQLAlchemy's greenlet bridge (`in_greenlet()`). `SyncEngineAdapter` and `attach_serverless_disconnect_guard` (`engine.py`) were deleted; configure `pool_pre_ping=True` on engines so `is_disconnect()` checks SQLSTATE (`08*`, `57P01`–`57P03`, MSSQL `08S01`/`HYT00`/`HY010`) and vendor error codes (`1927`, `2006`, `2013`, `ORA-03113`, `ORA-03114`, `ORA-01012`, gRPC `UNAVAILABLE`) before message text [^src-extensions-sqlalchemy-async-adapter] [^src-extensions-sqlalchemy-dialects-base].
- **Statement-Autocommit Backends and Spangres Two-Phase Commit**: `BigQuery`, `Spanner`, and `Spangres` connections in [`vertebra.db.dbapi`](../../../../../src/py/vertebra/db/dbapi/_sync.py)[^src-db-dbapi-sync] commit per statement or RPC and raise `NotSupportedError` when `autocommit=False` is requested [^src-db-dbapi-async]. [`SyncSpangresDialect`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py)[^src-extensions-sqlalchemy-spanner] and `AsyncSpangresDialect` set `supports_twophase = False` and raise `sqlalchemy.exc.NotSupportedError` on `do_begin_twophase`, `do_prepare_twophase`, `do_commit_twophase`, `do_rollback_twophase`, and `do_recover_twophase`.
- **Mandatory Dialect Extras**: [`duckdb.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py)[^src-extensions-sqlalchemy-duckdb] requires `duckdb_engine` (`vertebra-core[sqlalchemy-duckdb]`), while [`bigquery.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py)[^src-extensions-sqlalchemy-bigquery] and [`spanner.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py)[^src-extensions-sqlalchemy-spanner] require `sqlalchemy_bigquery` and `google.cloud.sqlalchemy_spanner` (`vertebra-core[sqlalchemy-gcp]`). Absent packages raise `MissingDependencyError` rather than falling back to `DefaultDialect`.
- **Online Spanner DDL Batching vs. Offline `--sql` Output**: [`SpannerAlembicImpl._exec`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/impl.py)[^src-extensions-sqlalchemy-alembic-impl] and `SpangresAlembicImpl._exec` buffer `schema.DDLElement` instances in online mode (`as_sql=False`) and flush them through `update_ddl` on `emit_commit()` or before non-DDL execution. When Alembic runs with `--sql` (`as_sql=True`), statements emit immediately through `DefaultImpl._exec` without batching.
- **Oracle Out-Binds, MSSQL `nextset()` Buffering, and SQLite Pool/Ping**: [`OracleExecutionContext_vtb`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py)[^src-extensions-sqlalchemy-oracle] binds `INSERT ... RETURNING ... INTO` parameters via `cursor.var()` and queries `product_component_version` when connection metadata lacks `server_version_info`. [`MSExecutionContext_vtb`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py)[^src-extensions-sqlalchemy-mssql] advances up to 16 `nextset()` result sets to buffer `OUTPUT` rows. [`_SQLiteDialectMixin`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py)[^src-extensions-sqlalchemy-sqlite] selects `StaticPool` for `:memory:` and `mode=memory` URLs and toggles `autocommit=True` during `do_ping` so no transaction remains open.
- **Delegated Base Dialect and Pool Behavior**: [`postgresql.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py)[^src-extensions-sqlalchemy-postgresql] relies on SQLAlchemy `PGDialect` for `insert_executemany_returning` and `DELETE ... USING`, while PostgreSQL, MySQL, SQLite, Oracle, and MSSQL rely on Alembic's built-in `DefaultImpl` subclasses. [`BrokerPool`](../../../../../src/py/vertebra/extensions/sqlalchemy/pool.py)[^src-extensions-sqlalchemy-pool] and `AsyncBrokerPool` leave `reset_on_return` handling to `sqlalchemy.pool.Pool` and guard async broker operations with `in_greenlet()`.

[^src-extensions-sqlalchemy-async-adapter]: `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — SQLAlchemy asyncio DBAPI adapter with greenlet and termination support.
[^src-extensions-sqlalchemy-dialects-base]: `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — Shared SQLAlchemy 2.1 dialect mixins and disconnect detection.
[^src-extensions-sqlalchemy-pool]: `src/py/vertebra/extensions/sqlalchemy/pool.py` — `BrokerPool` and `AsyncBrokerPool` SQLAlchemy `Pool` implementations.
[^src-extensions-sqlalchemy-duckdb]: `src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py` — DuckDB dialect requiring `duckdb_engine` via `sqlalchemy-duckdb` extra.
[^src-extensions-sqlalchemy-bigquery]: `src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py` — BigQuery dialect requiring `sqlalchemy_bigquery` via `sqlalchemy-gcp` extra.
[^src-extensions-sqlalchemy-spanner]: `src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py` — Spanner and Spangres dialects and disabled two-phase commit.
[^src-extensions-sqlalchemy-oracle]: `src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py` — Oracle dialect version lookup and `cursor.var()` `RETURNING` execution context.
[^src-extensions-sqlalchemy-mssql]: `src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py` — SQL Server dialect version lookup and `OUTPUT` `nextset()` execution context.
[^src-extensions-sqlalchemy-sqlite]: `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py` — SQLite `StaticPool` selection and transaction-free `do_ping`.
[^src-extensions-sqlalchemy-postgresql]: `src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py` — PostgreSQL dialect cast colspecs and base `insert_executemany_returning`.
[^src-extensions-sqlalchemy-alembic-impl]: `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py` — Alembic `DefaultImpl` subclasses and Spanner online DDL batching.
[^src-db-dbapi-sync]: `src/py/vertebra/db/dbapi/_sync.py` — Synchronous PEP 249 module, native cursor handoff, transaction state, and native exception re-exports.
[^src-db-dbapi-async]: `src/py/vertebra/db/dbapi/_async.py` — Asynchronous PEP 249 module, native cursor handoff, and transaction control.
