---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: "SQLAlchemy, Advanced Alchemy, and Django DB Integration"
description: "Harden the shared Vertebra SyncDBAPI / AsyncDBAPI module (removing its second sqlite3 connection and concrete config-class dispatch), split the multi-engine Sync/Async SQLAlchemy dialect module onto built-in and official GCP/DuckDB dialect bases, replace the custom Advanced Alchemy cache region with a dogpile backend, and build Django database backends on Django built-ins and django_spanner."
status: draft
state: draft
has_chapters: false
parent_prd: integration-remediation
depends_on_flows:
  - broker-pool-sharing
  - db-layout-and-contract
  - db-engine-primitives
  - driver-fail-loud
  - storage-completeness
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 4
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: Current multi-engine SQLAlchemy dialect and DB-API module slated for extraction and per-engine split
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: PEP 249 connection with a second sqlite3 connection, a default SQLite URL, and isinstance engine dispatch
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: Alembic integration resolving migration URLs through isinstance checks on concrete configs
  - id: entry-points
    resource: pyproject.toml
    title: sqlalchemy.dialects entry points registering both vtb and vertebra driver names
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: Advanced Alchemy cache integration slated for dogpile.cache backend
  - id: django-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: Django database backend wrapper mixin
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud baseline raising InterfaceError on DB-API URL parse failure
tags:
  - sqlalchemy
  - advanced-alchemy
  - django
  - dialects
  - dbapi
---

# Flow Charter: SQLAlchemy, Advanced Alchemy, and Django DB Integration

<!-- truth: start -->
- **Current DB-API State**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` live in `extensions/sqlalchemy/dialects.py`, default `url` to `sqlite+vtb:///:memory:`, open a second Python `sqlite3` connection (`_sqlite_conn`) beside the registry driver for SQLite URLs without a broker, and pick autocommit and transaction rules with `isinstance` checks against `BigQueryConfig`, `SpannerConfig`, and `SQLiteConfig` [^sa-dbapi].
- **Current Entry Points**: `pyproject.toml` registers every dialect twice, as `<engine>.vtb` / `<engine>.vtb_async` and `<engine>.vertebra` / `<engine>.vertebra_async` [^entry-points].
- **Upstream Dialect & Backend Inheritance (target state)**: `Sync<Engine>Dialect` / `Async<Engine>Dialect` subclass SQLAlchemy built-ins and official `sqlalchemy_bigquery` / `sqlalchemy_spanner` / `duckdb_engine` bases over `vertebra.db.dbapi`, while Django backends subclass Django built-ins and `django_spanner` [^sa-dialects] [^django-backends].
- **Layer Purity (target state)**: `vertebra.db.dbapi` remains free of SQLAlchemy, Django, or engine-specific imports and dispatch; SQLAlchemy greenlet adaptation stays isolated in `extensions/sqlalchemy/async_adapter.py`.
- **Real Dogpile Cache Backend (target state)**: `extensions/alchemy/cache.py` registers `dogpile.cache` backends (`vertebra.valkey`, `vertebra.redis`) and deletes the process-local `shm://` cache scheme and the `extensions/sqlalchemy/cache.py` shim [^aa-cache].
<!-- truth: end -->

## Charter

SQLAlchemy and Advanced Alchemy are the primary user path. Vertebra's `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes and Django backends behave like complete dialects and backends (statement/type compilers, identifier quoting, reflection, schema editing, migrations) while executing through Vertebra's native driver wheels and the shared `vertebra.db.dbapi` PEP 249 module (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`). Native drivers remain in `vertebra.db.adapters.<engine>`; SQLAlchemy dialects and Django backends adapt those contracts without owning or duplicating native driver implementations. SQLAlchemy-specific async and greenlet adaptation stays in `extensions/sqlalchemy/async_adapter.py`.

## Known Defects

- **Second `sqlite3` connection.** For SQLite URLs without a broker, `SyncDBAPIConnection.__init__` calls `sqlite3.connect(...)` and routes `cursor()`, `execute_with_description`, `commit`, `rollback`, and `close` through it, while also building a registry `SyncSQLiteDriver`. Statements therefore run on a connection the native driver does not own; for `:memory:` URLs the two connections are separate databases, and the registry driver's pool sharing, Arrow results, and error mapping are bypassed [^sa-dbapi].
- **Concrete config-class dispatch.** Autocommit defaults and the `autocommit=False` rejection key on `isinstance(self._config, (BigQueryConfig, SpannerConfig))` in both connection classes, and `extensions/sqlalchemy/alembic.py` resolves migration URLs through `isinstance` checks on `PostgreSQLConfig` and `MySQLConfig`. Adding or changing an engine requires editing the generic module [^sa-alembic].
- **Implicit default database.** The connection default `url="sqlite+vtb:///:memory:"` plus the `database=` override silently create an in-memory SQLite database when callers pass no URL.

## Scope

1. **Shared DB-API ownership and hardening (`db/dbapi/`).**
   - Harden the PEP 249 implementation that `broker-pool-sharing` extracts into `db/dbapi/{__init__,_sync,_async,_errors}.py`, exporting `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, and `AsyncDBAPICursor`. `_sync.py` and `_async.py` own their connection, cursor, and module implementations; `_errors.py` owns PEP 249 exception adaptation over the canonical `vertebra.db.driver.DatabaseError` hierarchy. Consume this final package location without a second extraction or relocation; `broker-pool-sharing` owns removal of the original implementation and establishment of `extensions/sqlalchemy/async_adapter.py`.
   - The shared package depends on generic `DatabaseRegistry`, `BaseDatabaseConfig`, and synchronous/asynchronous driver contracts. It imports no `vertebra.extensions`, `vertebra.framework`, `vertebra.agent`, SQLAlchemy, Django, or engine-specific adapter modules, directly or through engine-specific helpers. URL parsing remains `DatabaseRegistry`'s responsibility; the declarative registry/config registration remains the composition boundary.
   - `driver-fail-loud` makes both connections raise `InterfaceError`, chained from the parse exception, when `DatabaseRegistry.parse_url` fails [^fail-loud-baseline]. This flow owns the broader error mapping: both connections raise `InterfaceError` / `ProgrammingError` on invalid arguments and map native driver errors through `_errors.py`.
   - Remove `_sqlite_conn` and the module-level `sqlite3` import: SQLite URLs execute through the registry-provided SQLite driver like every other engine, and `sqlite_version` / `sqlite_version_info` dialect attributes come from the native SQLite driver rather than stdlib `sqlite3`.
   - Replace the `isinstance` checks on `BigQueryConfig`, `SpannerConfig`, and `SQLiteConfig` (and the Alembic `PostgreSQLConfig` / `MySQLConfig` checks) with engine-neutral metadata declared on `BaseDatabaseConfig` or the driver contract and set by each owning adapter (for example a statement-autocommit requirement and a direct migration DSN). Reuse existing generic metadata (`effective_direct_dsn`-style properties) where it already expresses the requirement. Do not add an engine capability registry, policy class hierarchy, or speculative flags. Preserve explicit failures for unsupported transaction modes.
   - Connections require an explicit URL: `SyncDBAPIModule.connect()` / `AsyncDBAPIModule.connect()` and direct `SyncDBAPIConnection()` / `AsyncDBAPIConnection()` construction with no URL, or with an empty URL, raise `InterfaceError` and never open SQLite. The `sqlite+vtb:///:memory:` default and the `database=` override are removed, and `create_connect_args` passes the full URL.
   - Harden the SQLAlchemy `await_only`, greenlet, and SQLAlchemy adapted connection/cursor integration in `extensions/sqlalchemy/async_adapter.py`. It wraps the shared async DB-API without introducing SQLAlchemy imports or SQLAlchemy-specific behavior into `db/dbapi/`. Django synchronous backends consume `vertebra.db.dbapi` directly and never import the SQLAlchemy extension.
2. **SQLAlchemy dialects & Alembic.**
   - Split `extensions/sqlalchemy/dialects.py` into `extensions/sqlalchemy/dialects/<engine>.py` (`Sync<Engine>Dialect` and `Async<Engine>Dialect`) and `extensions/sqlalchemy/engine.py` (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`), and rewrite `pyproject.toml` `[project.entry-points."sqlalchemy.dialects"]` to the new module paths while keeping every existing driver name: `<engine>+vertebra`, `<engine>+vtb`, `<engine>+vertebra_async`, and `<engine>+vtb_async` for `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, and `spangres` (for example `postgresql+vtb_async://`). `vertebra` / `vtb` both resolve to `Sync<Engine>Dialect` and `vertebra_async` / `vtb_async` both resolve to `Async<Engine>Dialect`; these are documented public driver names, not compatibility aliases.
   - `SyncBigQueryDialect` / `AsyncBigQueryDialect` subclass `sqlalchemy_bigquery.BigQueryDialect`, `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` subclass `google.cloud.sqlalchemy_spanner.SpannerDialect`, and `SyncDuckDBDialect` / `AsyncDuckDBDialect` subclass `duckdb_engine.Dialect` (optional extras), overriding only `import_dbapi`, `get_pool_class`, and `create_connect_args`.
   - `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect`, `SyncMySQLDialect` / `AsyncMySQLDialect`, `SyncOracleDialect` / `AsyncOracleDialect`, `SyncMSSQLDialect` / `AsyncMSSQLDialect`, and `SyncSQLiteDialect` / `AsyncSQLiteDialect` subclass SQLAlchemy's built-in dialect classes in the same way. Each `create_connect_args` passes the full Vertebra URL to the DB-API connection.
   - Remove standalone helpers invoked only by synthetic tests (`compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, `extract_interleave_options`) unless wired into a dialect compiler.
   - Split `extensions/sqlalchemy/alembic.py` into per-dialect Alembic `DefaultImpl` modules, with migration URL resolution read from the engine-neutral config metadata above.
3. **Advanced Alchemy cache & URL normalization.**
   - Replace `extensions/alchemy/cache.py::SQLAlchemyCacheBackend` with a `dogpile.cache` `CacheBackend` registered as `vertebra.valkey` / `vertebra.redis` so Advanced Alchemy's stock `CacheManager` works directly; retain `AsyncCacheManager` strictly for native async L2 caching.
   - Remove unused `shm_name`, dead `TYPE_CHECKING` blocks, the process-local fake `shm://` scheme in `extensions/alchemy/cache.py`, and the URL-as-backend-name bug in `create_cache_config`, and delete the `extensions/sqlalchemy/cache.py` re-export shim.
   - Update `extensions/alchemy/plugin.py::normalize_alchemy_url` to read `BaseDatabaseConfig.url_schemes`.
4. **Django database backends**.
   - `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django's built-in backends and swap in `vertebra.db.dbapi` (`SyncDBAPIModule` / `SyncDBAPIConnection`).
   - `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (googleapis `django-google-spanner`, Django 5.2/6.0) and swaps `google.cloud.spanner_dbapi` for Vertebra's Rust-backed `SyncDBAPIModule`.
   - Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/` (Django ships no built-in `bigquery`, `duckdb`, or `mssql` backends).
   - Move Django-settings-to-vendor-config resolution out of `backends/__init__.py::DatabaseWrapperMixin._build_native_config` into each `<vendor>/base.py`. Each backend constructs its owning engine config and uses the shared DB-API; the generic mixin has no vendor dispatch or fallback backend.
5. **Docs & OKF.** Update `docs/usage/extensions.md`, `docs/reference/extensions.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, and `.agents/bundles/knowledge/domains/extensions/litestar.md`, document the entry-point and URL changes in the changelog, and record caveats in a limitations chapter under `.agents/bundles/knowledge/domains/extensions/`.

## Out of Scope

- SQLSpec adapters (`sqlspec-adapter-parity`), Django `django.tasks` backend (`queues-tasks-and-channels`), `alchemy/broker.py::StatementCache` deletion and `BrokerPool` extraction (`broker-pool-sharing`), the native SQLite wheel (`native-sqlite-driver`), and `alchemy/storage.py` `StorageBackend` registration (merged in `storage-completeness`).

## Public Seams

`vertebra.db.dbapi.{SyncDBAPIModule,AsyncDBAPIModule,SyncDBAPIConnection,AsyncDBAPIConnection,SyncDBAPICursor,AsyncDBAPICursor}`, dialect entry points (`<engine>+vertebra` / `<engine>+vtb` -> `Sync<Engine>Dialect` and `<engine>+vertebra_async` / `<engine>+vtb_async` -> `Async<Engine>Dialect`, including `spangres`), `create_cache_config`, `dogpile.cache` backend names (`vertebra.valkey`, `vertebra.redis`), and Django `ENGINE` paths.

## Dependencies

- `broker-pool-sharing` (draft, behind `native-postgres-driver`): extracts `vertebra.db.dbapi` and `extensions/sqlalchemy/async_adapter.py`. This flow cannot start before that extraction lands.
- Merged: `db-layout-and-contract` (`21c99f95`), integration-remediation `db-engine-primitives` (`80ff8a65`), `driver-fail-loud` (`af1b3d30`, removing the DB-API SQLite parse fallback), and `storage-completeness` (`067fa901`, [object-storage.md](../../knowledge/domains/storage/object-storage.md)).

## Acceptance Boundary

- Shared DB-API connection/cursor tests exercise real native driver results, canonical error mapping, transaction mode failures, invalid and missing URLs, and absence of fallback database creation. A SQLite `:memory:` test writes and reads through one DB-API connection and sees its own rows. A dependency check proves `db/dbapi/` imports only generic DB contracts and imports no framework, integration, engine adapter, or `sqlite3` modules, and `git grep -n -E "isinstance\([^)]*(BigQueryConfig|SpannerConfig|SQLiteConfig|PostgreSQLConfig|MySQLConfig)" -- src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlalchemy` returns nothing. Django backend imports do not load SQLAlchemy.
- Sync and async SQLAlchemy integration tests exercise `extensions/sqlalchemy/async_adapter.py` separately from the framework-neutral async DB-API, including cancellation and exception propagation.
- SQLAlchemy compilation and reflection tests pass per dialect (including GoogleSQL backtick quoting and `STRING` types), and `create_engine` resolves all four driver names (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) for every engine to the split per-engine dialect classes, with `pyproject.toml` still registering all `36` entry points.
- Alembic `autogenerate` passes on PostgreSQL and Spanner.
- Django `migrate` passes on PostgreSQL, SQLite, and the Spanner emulator.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Decisions

- **Entry-point driver names** *(user, 2026-11-12)*: Keep every existing `pyproject.toml` `[project.entry-points."sqlalchemy.dialects"]` name (`<engine>+vertebra`, `<engine>+vtb`, `<engine>+vertebra_async`, `<engine>+vtb_async`); t03 and t04 only repoint them to the split dialect modules.
- **Direct DB-API URL** *(user, 2026-11-12)*: A direct DB-API `connect()` call with no URL raises `InterfaceError` and never silently opens SQLite.

## Draft Task Outline

- t01 Harden `db/dbapi/{__init__,_sync,_async,_errors}.py` (`SyncDBAPI*` / `AsyncDBAPI*`, generic registry/driver contracts, canonical error mapping, removal of `_sqlite_conn` and stdlib `sqlite3`, required explicit URL with `InterfaceError` on a missing or empty URL).
- t02 Replace concrete config-class dispatch in the DB-API and Alembic with engine-neutral config metadata set by each owning adapter, and harden `extensions/sqlalchemy/async_adapter.py`.
- t03 Split relational `Sync<Engine>Dialect` / `Async<Engine>Dialect` modules onto SQLAlchemy built-in dialect bases and repoint all four driver names per engine to the new modules.
- t04 Rebase `SyncBigQueryDialect`/`AsyncBigQueryDialect`, `SyncSpannerDialect`/`AsyncSpannerDialect`/`SyncSpangresDialect`/`AsyncSpangresDialect`, and `SyncDuckDBDialect`/`AsyncDuckDBDialect` onto official SQLAlchemy dialect bases.
- t05 Split per-dialect Alembic `DefaultImpl` modules.
- t06 Implement `dogpile.cache` backend (`vertebra.valkey` / `vertebra.redis`), remove the fake `shm://` cache scheme, and delete `sqlalchemy/cache.py`.
- t07 Wire `alchemy/plugin.py::normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes`.
- t08 Rebase Django PostgreSQL, MySQL, Oracle, and SQLite backends onto Django built-ins.
- t09 Rebase the Django Spanner backend onto `django_spanner`, remove `bigquery`/`duckdb`/`mssql` Django backends, and update docs/OKF.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current multi-engine SQLAlchemy dialect and DB-API module slated for extraction and per-engine split.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — PEP 249 connection with a second `sqlite3` connection, a default SQLite URL, and `isinstance` engine dispatch.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Alembic integration resolving migration URLs through `isinstance` checks on concrete configs.
[^entry-points]: `pyproject.toml` — `sqlalchemy.dialects` entry points registering both `vtb` and `vertebra` driver names.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache integration slated for dogpile.cache backend.
[^django-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Django database backend wrapper mixin.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline raising `InterfaceError` on DB-API URL parse failure.
