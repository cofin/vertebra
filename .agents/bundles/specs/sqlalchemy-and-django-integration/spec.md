---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: SQLAlchemy, Advanced Alchemy, and Django DB Integration
description: Split the multi-engine Sync/Async SQLAlchemy dialect module onto built-in and official GCP/DuckDB dialect bases over the shared Vertebra SyncDBAPI / AsyncDBAPI module, replace the custom Advanced Alchemy cache region with a dogpile backend, and build Django database backends on Django built-ins and django_spanner.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: "2026-11-05T06:20:28Z"
tags: [sqlalchemy, advanced-alchemy, django, dialects, dbapi]
---

# Flow Charter: SQLAlchemy, Advanced Alchemy, and Django DB Integration

## Charter

SQLAlchemy and Advanced Alchemy are the primary user path. Vertebra's `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes and Django backends must behave like complete dialects and backends (statement/type compilers, identifier quoting, reflection, schema editing, migrations) while executing through Vertebra's native driver wheels and shared `vertebra.db.dbapi` PEP 249 module (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`). Native drivers remain in `vertebra.db.adapters.<engine>`; SQLAlchemy dialects and Django backends adapt those contracts without owning or duplicating native driver implementations. SQLAlchemy-specific async and greenlet adaptation stays in `extensions/sqlalchemy/async_adapter.py`.

## Scope

1. **Shared DB-API ownership and hardening (`db/dbapi/`).**
   - Harden the PEP 249 implementation extracted directly into `db/dbapi/{__init__,_sync,_async,_errors}.py` by `broker-pool-sharing`, exporting `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, and `AsyncDBAPICursor`. `_sync.py` and `_async.py` own their respective connection, cursor, and module implementations; `_errors.py` owns PEP 249 exception adaptation over the canonical `vertebra.db.driver.DatabaseError` hierarchy. Consume this final package location without a second extraction or relocation; `broker-pool-sharing` owns removal of the original implementation and establishment of `extensions/sqlalchemy/async_adapter.py`.
   - The shared package depends on generic `DatabaseRegistry`, `BaseDatabaseConfig`, and synchronous/asynchronous driver contracts. It imports no `vertebra.extensions`, `vertebra.framework`, `vertebra.agent`, SQLAlchemy, Django, or engine-specific adapter modules, directly or through engine-specific helpers. URL parsing remains `DatabaseRegistry`'s responsibility; the declarative registry/config registration remains the composition boundary.
   - `SyncDBAPIConnection` / `AsyncDBAPIConnection` raise `InterfaceError` / `ProgrammingError` on invalid URLs and arguments. They never substitute SQLite, open a second Python `sqlite3` connection, parse vendor DSNs themselves, or select behavior by engine names, URL schemes, or `isinstance` checks against concrete engine config classes.
   - Native driver/config implementations own engine transaction and autocommit behavior. Reuse existing generic contract/config metadata where it expresses required PEP 249 behavior; when an observed DB-API requirement needs metadata, declare only that engine-neutral requirement on the shared contract and provide its value on the owning adapter. The DB-API consumes the same contract for every engine. Do not add an engine capability registry, policy class hierarchy, or speculative flags. Preserve explicit failures for unsupported transaction modes.
   - Harden the SQLAlchemy `await_only`, greenlet, and SQLAlchemy adapted connection/cursor integration established by `broker-pool-sharing` in `extensions/sqlalchemy/async_adapter.py`. It wraps the shared async DB-API without introducing SQLAlchemy imports or SQLAlchemy-specific behavior into `db/dbapi/`. Django synchronous backends consume `vertebra.db.dbapi` directly and never import the SQLAlchemy extension.
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
   - `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django's built-in backends and swap in `vertebra.db.dbapi` (`SyncDBAPIModule` / `SyncDBAPIConnection`).
   - `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (googleapis `django-google-spanner`, Django 5.2/6.0) and swaps `google.cloud.spanner_dbapi` for Vertebra's Rust-backed `SyncDBAPIModule`.
   - Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/` (Django ships no built-in `bigquery`, `duckdb`, or `mssql` backends).
   - Move Django-settings-to-vendor-config resolution out of `backends/__init__.py::DatabaseWrapperMixin._build_native_config` into each `<vendor>/base.py`. Each backend constructs its owning engine config and uses the shared DB-API; the generic mixin has no vendor dispatch or fallback backend.
5. **Docs & OKF.** Update `docs/usage/extensions.md`, `docs/reference/extensions.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, and `.agents/bundles/knowledge/domains/extensions/litestar.md`.

## Out of Scope

- SQLSpec adapters (Chapter 4 `sqlspec-adapter-parity`), Django `django.tasks` backend (Chapter 5 `queues-tasks-and-channels`), `alchemy/broker.py::StatementCache` deletion and `BrokerPool` extraction (`native-data-platform/broker-pool-sharing`), and `alchemy/storage.py` `StorageBackend` registration (`native-data-platform/storage-completeness`).

## Public Seams

`vertebra.db.dbapi.{SyncDBAPIModule,AsyncDBAPIModule,SyncDBAPIConnection,AsyncDBAPIConnection,SyncDBAPICursor,AsyncDBAPICursor}`, dialect entry points (`<engine>+vtb`, `<engine>+vtb_async`, `spangres+vtb`, `spangres+vtb_async` -> `Sync<Engine>Dialect` / `Async<Engine>Dialect`), `create_cache_config`, `dogpile.cache` backend names (`vertebra.valkey`, `vertebra.redis`), and Django `ENGINE` paths.

## Dependencies

`native-data-platform` `db-layout-and-contract` (`main @ 21c99f95`), `broker-pool-sharing`, and `storage-completeness`; `integration-remediation` `db-engine-primitives` (`main @ 80ff8a65`).

## Acceptance Boundary

- Shared DB-API connection/cursor tests exercise real native driver results, canonical error mapping, transaction mode failures, invalid URLs, and absence of fallback database creation. A dependency check proves `db/dbapi/` imports only generic DB contracts and imports no framework, integration, or engine adapter modules; Django backend imports do not load SQLAlchemy.
- Sync and async SQLAlchemy integration tests exercise `extensions/sqlalchemy/async_adapter.py` separately from the framework-neutral async DB-API, including cancellation and exception propagation.
- SQLAlchemy compilation and reflection tests pass per dialect (including GoogleSQL backtick quoting and `STRING` types).
- Alembic `autogenerate` passes on PostgreSQL and Spanner.
- Django `migrate` passes on PostgreSQL, SQLite, and the Spanner emulator.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 Harden `broker-pool-sharing`'s shared `db/dbapi/{__init__,_sync,_async,_errors}.py` (`SyncDBAPI*` / `AsyncDBAPI*`, generic registry/driver contracts, canonical errors, strict URL handling, engine-owned transaction behavior) and its SQLAlchemy-only `extensions/sqlalchemy/async_adapter.py` across supported native engines and Django cursor requirements; preserve their established package ownership.
- t02 Split relational `Sync<Engine>Dialect` / `Async<Engine>Dialect` modules onto SQLAlchemy built-in dialect bases.
- t03 Rebase `SyncBigQueryDialect`/`AsyncBigQueryDialect`, `SyncSpannerDialect`/`AsyncSpannerDialect`/`SyncSpangresDialect`/`AsyncSpangresDialect`, and `SyncDuckDBDialect`/`AsyncDuckDBDialect` onto official SQLAlchemy dialect bases.
- t04 Split per-dialect Alembic `DefaultImpl` modules.
- t05 Implement `dogpile.cache` backend (`vertebra.valkey` / `vertebra.redis`), remove fake `shm://` cache scheme, and delete `sqlalchemy/cache.py`.
- t06 Wire `alchemy/plugin.py::normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes`.
- t07 Rebasing Django PostgreSQL, MySQL, Oracle, and SQLite backends onto Django built-ins.
- t08 Rebase Django Spanner backend onto `django_spanner`, remove `bigquery`/`duckdb`/`mssql` Django backends, and update docs/OKF.
