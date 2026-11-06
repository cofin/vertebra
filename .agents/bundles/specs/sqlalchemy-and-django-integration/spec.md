---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: SQLAlchemy, Advanced Alchemy, and Django DB Integration
description: Split the multi-engine Sync/Async SQLAlchemy dialect module onto built-in and official GCP/DuckDB dialect bases over the shared Vertebra SyncDBAPI / AsyncDBAPI module, replace the custom Advanced Alchemy cache region with a dogpile backend, and build Django database backends on Django built-ins and django_spanner.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-04T01:19:00Z
tags: [sqlalchemy, advanced-alchemy, django, dialects, dbapi]
---

# Flow Charter: SQLAlchemy, Advanced Alchemy, and Django DB Integration

## Charter

SQLAlchemy and Advanced Alchemy are the primary user path. Vertebra's `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes and Django backends must behave like complete dialects and backends (statement/type compilers, identifier quoting, reflection, schema editing, migrations) while executing through Vertebra's native driver wheels and shared `extensions/sqlalchemy/dbapi.py` PEP 249 module (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`, extracted in `broker-pool-sharing`).

## Scope

1. **Shared DB-API hardening (`extensions/sqlalchemy/dbapi.py`).**
   - Build on `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`, and PEP 249 `DatabaseError` mapping extracted in `broker-pool-sharing`, ensuring `SyncDBAPIConnection` / `AsyncDBAPIConnection` raise `InterfaceError` / `ProgrammingError` on invalid URLs instead of falling back to `SQLiteConfig(":memory:")` and serve both SQLAlchemy dialects and Django database wrappers.
2. **SQLAlchemy dialects & Alembic.**
   - Split `extensions/sqlalchemy/dialects.py` into `extensions/sqlalchemy/dialects/<engine>.py` (`Sync<Engine>Dialect` and `Async<Engine>Dialect`) and `extensions/sqlalchemy/engine.py` (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`), preserving `pyproject.toml` `[project.entry-points."sqlalchemy.dialects"]`.
   - `SyncBigQueryDialect` / `AsyncBigQueryDialect` subclass `sqlalchemy_bigquery.BigQueryDialect`, `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` subclass `google.cloud.sqlalchemy_spanner.SpannerDialect`, and `SyncDuckDBDialect` / `AsyncDuckDBDialect` subclass `duckdb_engine.Dialect` (optional extras), overriding only `import_dbapi`, `get_pool_class`, and `create_connect_args`.
   - `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect`, `SyncMySQLDialect` / `AsyncMySQLDialect`, `SyncOracleDialect` / `AsyncOracleDialect`, `SyncMSSQLDialect` / `AsyncMSSQLDialect`, and `SyncSQLiteDialect` / `AsyncSQLiteDialect` (`+vtb` / `+vtb_async`) subclass SQLAlchemy's built-in dialect classes in the same way.
   - Remove standalone helpers invoked only by synthetic tests (`compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, `extract_interleave_options`) unless wired into a dialect compiler.
   - Split `extensions/sqlalchemy/alembic.py` into per-dialect Alembic `DefaultImpl` modules.
3. **Advanced Alchemy cache & URL normalization.**
   - Replace `extensions/alchemy/cache.py::SQLAlchemyCacheBackend` with a `dogpile.cache` `CacheBackend` registered as `vertebra.valkey` / `vertebra.redis` so Advanced Alchemy's stock `CacheManager` works directly; retain `AsyncCacheManager` strictly for native async L2 caching.
   - Remove unused `shm_name`, dead `TYPE_CHECKING` blocks, the process-local fake `shm://` scheme in `extensions/alchemy/cache.py`, and the URL-as-backend-name bug in `create_cache_config`, and delete the `extensions/sqlalchemy/cache.py` re-export shim.
   - Update `extensions/alchemy/plugin.py::normalize_alchemy_url` to read `BaseDatabaseConfig.url_schemes` from `db-layout-and-contract`.
4. **Django database backends**.
   - `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django's built-in backends and swap in `extensions/sqlalchemy/dbapi.py` (`SyncDBAPIModule` / `SyncDBAPIConnection`).
   - `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (googleapis `django-google-spanner`, Django 5.2/6.0) and swaps `google.cloud.spanner_dbapi` for Vertebra's Rust-backed `SyncDBAPIModule`.
   - Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/` (Django ships no built-in `bigquery`, `duckdb`, or `mssql` backends).
   - Move vendor config resolution out of `backends/__init__.py::DatabaseWrapperMixin._build_native_config` into each `<vendor>/base.py`.
5. **Docs & OKF.** Update `docs/usage/extensions.md`, `docs/reference/extensions.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, and `.agents/bundles/knowledge/domains/extensions/litestar.md`.

## Out of Scope

- SQLSpec adapters (Chapter 4 `sqlspec-adapter-parity`), Django `django.tasks` backend (Chapter 5 `queues-tasks-and-channels`), `alchemy/broker.py::StatementCache` deletion and `BrokerPool` extraction (`native-data-platform/broker-pool-sharing`), and `alchemy/storage.py` `StorageBackend` registration (`native-data-platform/storage-completeness`).

## Public Seams

Dialect entry points (`<engine>+vtb`, `<engine>+vtb_async`, `spangres+vtb`, `spangres+vtb_async` -> `Sync<Engine>Dialect` / `Async<Engine>Dialect`), `create_cache_config`, `dogpile.cache` backend names (`vertebra.valkey`, `vertebra.redis`), and Django `ENGINE` paths.

## Dependencies

`native-data-platform` `db-layout-and-contract`, `broker-pool-sharing`, and `storage-completeness` (`main @ f741701`); `integration-remediation` `db-engine-primitives`.

## Acceptance Boundary

- SQLAlchemy compilation and reflection tests pass per dialect (including GoogleSQL backtick quoting and `STRING` types).
- Alembic `autogenerate` passes on PostgreSQL and Spanner.
- Django `migrate` passes on PostgreSQL, SQLite, and the Spanner emulator.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 Harden shared `extensions/sqlalchemy/dbapi.py` (`SyncDBAPI*` / `AsyncDBAPI*`, strict URL parsing, Django/SQLAlchemy cursor compatibility).
- t02 Split relational `Sync<Engine>Dialect` / `Async<Engine>Dialect` modules onto SQLAlchemy built-in dialect bases.
- t03 Rebase `SyncBigQueryDialect`/`AsyncBigQueryDialect`, `SyncSpannerDialect`/`AsyncSpannerDialect`/`SyncSpangresDialect`/`AsyncSpangresDialect`, and `SyncDuckDBDialect`/`AsyncDuckDBDialect` onto official SQLAlchemy dialect bases.
- t04 Split per-dialect Alembic `DefaultImpl` modules.
- t05 Implement `dogpile.cache` backend (`vertebra.valkey` / `vertebra.redis`), remove fake `shm://` cache scheme, and delete `sqlalchemy/cache.py`.
- t06 Wire `alchemy/plugin.py::normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes`.
- t07 Rebasing Django PostgreSQL, MySQL, Oracle, and SQLite backends onto Django built-ins.
- t08 Rebase Django Spanner backend onto `django_spanner`, remove `bigquery`/`duckdb`/`mssql` Django backends, and update docs/OKF.
