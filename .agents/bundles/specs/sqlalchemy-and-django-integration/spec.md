---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: SQLAlchemy, Advanced Alchemy, Native Caching, and Django Integration
description: Harden shared vertebra.db.dbapi over native drivers, split all 9 Sync<Engine>Dialect / Async<Engine>Dialect pairs (36 entry points) onto SQLAlchemy built-in and upstream Google Cloud/DuckDB bases, build configurable SyncCacheBackend and AsyncCacheBackend (Rust ShmCache + SHM ring-buffer L1 invalidation, Valkey, Redis, ObjectStore, Memory) for Advanced Alchemy 1.12.0+, dogpile.cache, and SQLAlchemy, and rebase the 5 supported Django 6.0+ database backends plus native Django CacheBackend.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-dbapi-foundation-and-relational-dialects
  - ch02-cloud-dialects-alembic-and-advanced-alchemy
  - ch03-django-backends-and-docs
current_chapter: ch01-dbapi-foundation-and-relational-dialects
current_task: sqlalchemy-and-django-integration:ch01:t01
parent_prd: integration-remediation
depends_on_flows:
  - native-sqlite-driver
  - native-redis-driver
  - native-valkey-driver
plan_revision: 6
plan_commit: 473e1f2e
created_at: '2026-11-03T03:30:00Z'
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
  - by: agent:flow-archive
    at: '2026-11-15T17:48:00Z'
    commit: 246d7d0e
tags:
  - sqlalchemy
  - advanced-alchemy
  - django
  - dialects
  - dbapi
  - cache
  - shm
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: Monolithic SQLAlchemy dialect module (9 Sync/Async pairs, SyncEngineAdapter, attach_serverless_disconnect_guard) slated for per-engine split
  - id: sa-dbapi
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: Standalone PEP 249 connection extracted in broker-pool-sharing still carrying _sqlite_conn, default SQLite URL, raw native exceptions, and a hardcoded autocommit-only dialect name set
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: Monolithic Alembic DefaultImpl integration checking concrete config classes
  - id: entry-points
    resource: pyproject.toml
    title: 36 sqlalchemy.dialects entry points across 9 database engines
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin
  - id: sa-cache
    resource: src/py/vertebra/extensions/sqlalchemy/cache.py
    title: Re-export shim for CacheEntry, CacheStats, and SQLAlchemyCacheBackend (target CompiledCache, ResultCache, and native cache re-exports)
  - id: aa-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: Advanced Alchemy SQLAlchemySyncConfig / SQLAlchemyAsyncConfig and normalize_alchemy_url helpers
  - id: shm-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Rust POSIX shared-memory lease table and ring buffer
  - id: django-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: Generic Django DatabaseWrapperMixin with vendor config dispatch
  - id: django-cache
    resource: src/py/vertebra/extensions/django/cache.py::CacheBackend
    title: Standalone in-memory CacheBackend (no BaseCache base) targeted for SyncCacheBackend and AsyncCacheBackend
  - id: fail-loud-baseline
    resource: src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection
    title: Async PEP 249 connection raising InterfaceError on DB-API URL parse failures with an in-memory sqlite+vtb_async default URL
---

# Specification: SQLAlchemy, Advanced Alchemy, Native Caching, and Django Integration

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` live in extracted `vertebra.db.dbapi` (`broker-pool-sharing`), yet still default `url` to `sqlite+vtb:///:memory:` (`sqlite+vtb_async:///:memory:` on async), open a second stdlib `sqlite3` connection (`_sqlite_conn`, sync only), leak raw `vertebra._vertebra` errors rather than PEP 249 `dbapi.*Error` classes, and select autocommit-only engines through the hardcoded `_AUTOCOMMIT_ONLY_DIALECTS` name set [^sa-dialects] [^sa-dbapi] [^fail-loud-baseline].
- **Target Dialect & Entry-Point Contract**: Split `extensions/sqlalchemy/dialects/<engine>.py` modules expose leading `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes subclassing SQLAlchemy built-ins and `duckdb_engine` / `sqlalchemy_bigquery` / `sqlalchemy_spanner` bases while preserving all `36` `[project.entry-points."sqlalchemy.dialects"]` keys (`<engine>.vertebra`, `.vtb`, `.vertebra_async`, `.vtb_async`; URL form `<engine>+vertebra://`) [^entry-points].
- **Target Native Caching, Advanced Alchemy `1.12.0+` & `dogpile.cache` Contract**: Configurable `SyncCacheBackend` (satisfying `SyncCacheRegionProtocol`: `get`, `get_or_create`, `set`, `delete`, `invalidate`, `configure`) and `AsyncCacheBackend` (`extensions/alchemy/cache.py`, re-exported with `CompiledCache` and `ResultCache` in `extensions/sqlalchemy/cache.py`) run over Rust `ShmCache` (`shm://<segment>` in `src/rs/vertebra-core/src/broker.rs` + `src/rs/vertebra-py/src/broker.rs` with lock-free seqlock + CLOCK eviction and `ShmLeaseTable` ring-buffer L1 invalidation on `b"vtb:cache:inv"`), `memory://`, `valkey://` (`vertebra._valkey`), `redis://` (`vertebra._redis`), and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`vertebra._storage` `VTBSTORE\x01`). `create_cache_config(url="shm://...", l1_url=..., use_dogpile=False)` wires `CacheConfig(region_factory=...)` + `AsyncCacheManager(CacheManager)` directly without requiring `dogpile.cache`, and registers optional `dogpile.cache` backend plugins (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`) when `dogpile.cache` is installed [^aa-cache] [^sa-cache] [^aa-plugin] [^shm-broker].
- **Target Django `6.0+` DB & Cache Contract**: `postgresql`, `mysql`, `oracle`, and `sqlite3` subclass Django built-ins over `vertebra.db.dbapi`, `spanner` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`, and unsupported `bigquery`/`duckdb`/`mssql` Django DB backends are removed. `extensions/django/cache.py::CacheBackend(BaseCache)` delegates sync methods to `SyncCacheBackend` and `*a`-prefixed async methods directly to `AsyncCacheBackend` on the event loop [^django-backends] [^django-cache].
<!-- truth: end -->

## 1. Specification, Context & Provenance

SQLAlchemy and Advanced Alchemy are the primary ORM/query path in Vertebra, alongside native Django `6.0+` database and cache backends. Completed upstream `broker-pool-sharing` (`246d7d0e`) extracted `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py`, `extensions/sqlalchemy/async_adapter.py`, and `extensions/sqlalchemy/pool.py`, while `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver` deliver the Rust-backed SQLite, Redis (`vertebra._redis`), and Valkey (`vertebra._valkey`) wheels.

At `473e1f2e`, three structural defects remain across SQLAlchemy, Advanced Alchemy, and Django [^sa-dbapi] [^sa-alembic] [^django-backends] [^django-cache]:
1. `SyncDBAPIConnection` in `db/dbapi/_sync.py` still opens a separate stdlib `sqlite3.connect` handle (`_sqlite_conn`) and defaults empty URLs to `sqlite+vtb:///:memory:` (`AsyncDBAPIConnection` defaults to `sqlite+vtb_async:///:memory:` and reads `sqlite3.sqlite_version`); both leak raw `vertebra._vertebra` errors before PEP 249 mapping and select autocommit-only engines through the hardcoded `_AUTOCOMMIT_ONLY_DIALECTS` name set.
2. Monolithic `dialects.py` (`756` lines, all 18 `Sync*`/`Async*` dialect classes with hand-rolled `DefaultDialect` subclasses for DuckDB, BigQuery, and Spanner) and `alembic.py` (`418` lines, `isinstance` checks against `PostgreSQLConfig`/`MySQLConfig` in `resolve_migration_url`) await the per-engine split. `alchemy/cache.py::SQLAlchemyCacheBackend` treats `shm://` as a process-local `OrderedDict`, passes the raw URL as `CacheConfig.backend`, and already wires `region_factory` + `AsyncCacheManager` over Valkey/Redis; it lacks Rust `ShmCache`, object-store backends, and separate `SyncCacheBackend`/`AsyncCacheBackend` classes [^aa-cache] [^shm-broker].
3. `extensions/django/db/backends/` routes through a generic vendor-dispatching `DatabaseWrapperMixin` and ships hollow stubs for `bigquery`, `duckdb`, and `mssql`, while `extensions/django/cache.py::CacheBackend` is a base-less in-memory `dict` stub that ignores `LOCATION` and wraps sync methods inside `aget`/`aset` [^django-cache].

**Non-Goals**: SQLSpec adapters (`sqlspec-adapter-parity`) and Django `django.tasks` (`queues-tasks-and-channels`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Shared `vertebra.db.dbapi` Hardening & Purity)**: Consume `vertebra.db.dbapi` (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`) in place; remove `_sqlite_conn`, stdlib `sqlite3`, `url="sqlite+vtb:///:memory:"`, and `database=`; raise `InterfaceError` on empty/missing URLs; forbid imports of SQLAlchemy, Django, framework, or engine-adapter modules inside `db/dbapi/`.
- **R2 (Engine-Neutral Config Metadata & `async_adapter.py`)**: Declare `requires_statement_autocommit: ClassVar[bool] = False` (`True` on `BigQueryConfig` and `SpannerConfig`) and `effective_direct_dsn -> str` on `BaseDatabaseConfig`; isolate SQLAlchemy greenlet `await_only` adaptation in `extensions/sqlalchemy/async_adapter.py` and engine helpers (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`) in `extensions/sqlalchemy/engine.py`.
- **R3 (Custom SQLAlchemy Types)**: `extensions/sqlalchemy/types.py` replaces the multi-dialect `Vector` and `BinaryJSON` types (updating the `alembic.py` comparators) with `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON` (routing strictly through `encode_json` / `decode_json`); the SQLAlchemy `PgVector` type stays import-distinct from the driver value type `vertebra.db.adapters.postgres.PgVector`.
- **R4–R7 (Per-Engine `Sync<Engine>Dialect` / `Async<Engine>Dialect` & 36 Entry Points)**: Move the 18 existing dialect classes out of `dialects.py` into `extensions/sqlalchemy/dialects/{_base,postgresql,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner,__init__}.py` (zero `Vertebra*` or `*VtbDialect` class names), subclassing SQLAlchemy built-in relational bases and `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module` -> `MissingDependencyError`); delete monolithic `dialects.py` and synthetic-only helpers; switch MySQL `paramstyle` from `format` to `qmark`; preserve all `36` `[project.entry-points."sqlalchemy.dialects"]` entries (`<engine>.vertebra`, `.vtb`, `.vertebra_async`, `.vtb_async` across `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`).
- **R8–R9 (Modular Alembic, Native `SyncCacheBackend`/`AsyncCacheBackend`, `ShmCache`, `dogpile.cache` & Advanced Alchemy `1.12.0+`)**:
  - Split `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py` (deleting `alembic.py`).
  - Implement Rust `ShmCache` in `src/rs/vertebra-core/src/broker.rs` + `src/rs/vertebra-py/src/broker.rs` (next to the `LeaseBroker` `publish_channel`/`drain_channel` bindings) + `src/py/vertebra/_vertebra.pyi` (POSIX shared memory with FNV-1a bucket index, seqlock + CLOCK eviction, monotonic `expiry_ns` TTL, atomic `incr`/`decr`, and cross-worker L1 invalidations broadcast over `ShmLeaseTable` ring buffer `publish_channel`/`drain_channel` on `b"vtb:cache:inv"`).
  - Implement configurable `SyncCacheBackend` (satisfying Advanced Alchemy `SyncCacheRegionProtocol`: `get`, `get_or_create`, `set`, `delete`, `invalidate`, `configure`; plus Vertebra extensions `get_multi`, `set_multi`, `delete_multi`, `add`, `incr`, `decr`, `touch`, `has_key`, `invalidate_prefix`, `clear`, `close`) and `AsyncCacheBackend` (`get`, `set`, `delete`, `get_multi`, `set_multi`, `delete_multi`, `add`, `incr`, `decr`, `touch`, `has_key`, `invalidate_prefix`, `clear`, `aclose`) in `extensions/alchemy/cache.py` across `shm://<segment>`, `memory://`, `valkey://` (`vertebra._valkey`), `redis://` (`vertebra._redis`), and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`vertebra._storage` `VTBSTORE\x01`), with optional L1 (`l1_url="shm://..."` or `"memory://"`) invalidated via SHM ring buffer + RESP3 `CLIENT TRACKING`.
  - Wire `create_cache_config(url="shm://...", l1_url=..., use_dogpile=False)` to configure `CacheConfig(region_factory=...)` + `AsyncCacheManager(CacheManager)` by default (zero `dogpile.cache` dependency in `[project.optional-dependencies].alchemy`; a separate optional `dogpile = ["dogpile.cache>=1.3.3"]` extra carries it), register optional `dogpile.cache` backend plugins (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`) when `dogpile.cache` is installed, and expose `SyncCacheBackend`, `AsyncCacheBackend`, `CompiledCache`, and `ResultCache` in `extensions/sqlalchemy/cache.py` [^aa-cache] [^sa-cache] [^aa-plugin].
- **R10–R12 (Django `6.0+` Database & Cache Backends)**: `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django built-ins over `vertebra.db.dbapi`; `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`; delete `django/db/backends/{bigquery,duckdb,mssql}/` and vendor dispatch in `backends/__init__.py`, raising `ImproperConfigurationError` for unsupported engines; upgrade `extensions/django/cache.py::CacheBackend(BaseCache)` to delegate sync methods to `SyncCacheBackend` and `*a`-prefixed async methods (`aget`, `aset`, `aadd`, `adelete`, `aget_many`, `aset_many`, `adelete_many`, `aincr`, `adecr`, `atouch`, `ahas_key`, `aclear`, `aclose`) natively to `AsyncCacheBackend` on the event loop (zero `sync_to_async`) [^django-backends] [^django-cache].
- **R13 (Docs & OKF Sync)**: Synchronize public docs and OKF chapters (`<= 200` lines each).

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`ch03:t04`): `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/reference/extensions.md` (`pymdownx.tabbed` install blocks, 4–6 node conceptual Mermaid diagrams).
- OKF knowledge chapters (`ch03:t04`, each `<= 200` lines): [sqlspec-alchemy-and-dialects.md](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [litestar.md](../../knowledge/domains/extensions/litestar.md), and [database-driver-limitations.md](../../knowledge/domains/data/database-driver-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: DBAPI 2.0 Hardening, Dialect Base, and Relational SQLAlchemy Dialects | `4` tasks | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `ch02` | Chapter 02: Analytical/Cloud SQLAlchemy Dialects, Alembic Operations, Native Caching, and Advanced Alchemy | `4` tasks | `pytest test_alchemy_cache_backend.py test_gcp_adapters_and_sqlalchemy.py` |
| `ch03` | Chapter 03: Django 6.0+ Database & Cache Backends, Unsupported Dialect Cleanup, and Docs/OKF | `4` tasks | `pytest test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [ch01-dbapi-foundation-and-relational-dialects](chapters/ch01-dbapi-foundation-and-relational-dialects/spec.md) — Harden PEP 249 DBAPI and async greenlet adapters, build shared dialect base and native column types, and implement PostgreSQL, SQLite, MySQL, Oracle, and SQL Server SQLAlchemy 2.x dialects.
- [ ] [ch02-cloud-dialects-alembic-and-advanced-alchemy](chapters/ch02-cloud-dialects-alembic-and-advanced-alchemy/spec.md) — Implement DuckDB, BigQuery, and Spanner/Spangres SQLAlchemy dialects and entrypoints, wire Alembic DefaultImpl and migration ops, build Rust `ShmCache` + configurable `SyncCacheBackend`/`AsyncCacheBackend` + optional `dogpile.cache` plugins, and integrate Advanced Alchemy `1.12.0+`.
- [ ] [ch03-django-backends-and-docs](chapters/ch03-django-backends-and-docs/spec.md) — Upgrade Django database backends for PostgreSQL, SQLite3, MySQL, Oracle, and Spanner plus native Django `CacheBackend(BaseCache)`, remove unsupported MSSQL/DuckDB/BigQuery Django stubs, and synchronize public docs and OKF chapters.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-dbapi-foundation-and-relational-dialects` (`sqlalchemy-and-django-integration:ch01:t01`, `0/12` tasks completed across `3` chapters).
- **Upstream Flow Gates**: `broker-pool-sharing` (`246d7d0e`) is complete and archived; awaits `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver`.
- **Full `/flow:finish` Gate**: `make lint-all && make py-test && make docs`.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect module slated for per-engine split.
[^sa-dbapi]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection` — Standalone PEP 249 connection with `_sqlite_conn`, default SQLite URL, unmapped native errors, and a hardcoded autocommit-only dialect name set pending hardening.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic DefaultImpl integration checking concrete config classes.
[^entry-points]: `pyproject.toml` — 36 sqlalchemy.dialects entry points across 9 database engines.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin.
[^sa-cache]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Re-export shim for CacheEntry, CacheStats, and SQLAlchemyCacheBackend (target CompiledCache, ResultCache, and native cache re-exports).
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy SQLAlchemySyncConfig / SQLAlchemyAsyncConfig and normalize_alchemy_url helpers.
[^shm-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Rust POSIX shared-memory lease table and ring buffer.
[^django-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Generic Django DatabaseWrapperMixin with vendor config dispatch.
[^django-cache]: `src/py/vertebra/extensions/django/cache.py::CacheBackend` — Standalone in-memory CacheBackend (no BaseCache base) targeted for SyncCacheBackend and AsyncCacheBackend.
[^fail-loud-baseline]: `src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection` — Async PEP 249 connection raising InterfaceError on DB-API URL parse failures and defaulting to `sqlite+vtb_async:///:memory:`.
