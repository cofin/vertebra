---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: "SQLAlchemy, Advanced Alchemy, and Django DB Integration"
description: "Harden shared vertebra.db.dbapi over native drivers, split all 9 Sync<Engine>Dialect / Async<Engine>Dialect pairs (36 entry points) onto SQLAlchemy built-in and upstream GCP/DuckDB bases, add dogpile.cache>=1.3.3 for Advanced Alchemy 1.12.0+, and rebase the 5 supported Django 6.0+ database backends over vertebra.db.dbapi."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-dbapi-hardening-and-dialect-base
  - ch02-sqlalchemy-core-relational-dialects
  - ch03-sqlalchemy-analytical-cloud-dialects
  - ch04-alembic-dogpile-and-advanced-alchemy
  - ch05-django-relational-backends
  - ch06-django-spanner-and-unsupported-cleanup
  - ch07-sqlalchemy-django-docs-and-okf
current_chapter: ch01-dbapi-hardening-and-dialect-base
current_task: "sqlalchemy-and-django-integration:ch01:t01"
parent_prd: integration-remediation
depends_on_flows:
  - broker-pool-sharing
  - native-sqlite-driver
plan_revision: 5
plan_commit: 18baadcd
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlalchemy
  - advanced-alchemy
  - django
  - dialects
  - dbapi
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Monolithic SQLAlchemy dialect and PEP 249 DB-API module slated for per-engine split"
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: "PEP 249 connection with a second sqlite3 connection, default SQLite URL, and isinstance config dispatch"
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: "Monolithic Alembic DefaultImpl integration checking concrete config classes"
  - id: entry-points
    resource: pyproject.toml
    title: "36 sqlalchemy.dialects entry points across 9 database engines"
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: "Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin"
  - id: aa-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: "Advanced Alchemy SQLAlchemySyncConfig / SQLAlchemyAsyncConfig and normalize_alchemy_url helpers"
  - id: django-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: "Generic Django DatabaseWrapperMixin with vendor config dispatch"
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Fail-loud baseline raising InterfaceError on DB-API URL parse failures"
---

# Specification: SQLAlchemy, Advanced Alchemy, and Django DB Integration

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` reside in monolithic `extensions/sqlalchemy/dialects.py`, default `url` to `sqlite+vtb:///:memory:`, open a second stdlib `sqlite3` connection (`_sqlite_conn`), and dispatch via `isinstance` against concrete adapter configs [^sa-dialects] [^sa-dbapi] [^fail-loud-baseline].
- **Target Dialect & Entry-Point Contract**: Split `extensions/sqlalchemy/dialects/<engine>.py` modules expose leading `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes subclassing SQLAlchemy built-ins and `duckdb_engine` / `sqlalchemy_bigquery` / `sqlalchemy_spanner` bases while preserving all `36` `[project.entry-points."sqlalchemy.dialects"]` names (`<engine>+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) [^entry-points].
- **Target Cache & Advanced Alchemy `1.12.0+` Contract**: `dogpile.cache>=1.3.3` region backends (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`) wire into `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`, and `AlembicImpl` autogenerate/DDL parity, deleting `extensions/sqlalchemy/cache.py` and the fake `shm://` scheme [^aa-cache] [^aa-plugin] [^sa-alembic].
- **Target Django `6.0+` Contract**: `postgresql`, `mysql`, `oracle`, and `sqlite3` subclass Django built-ins over `vertebra.db.dbapi`, `spanner` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`, and unsupported `bigquery`/`duckdb`/`mssql` Django DB backends are removed [^django-backends].
<!-- truth: end -->

## 1. Specification, Context & Provenance

SQLAlchemy and Advanced Alchemy are the primary ORM/query path in Vertebra, alongside native Django `6.0+` database backends. Upstream `broker-pool-sharing:ch07` extracts `src/py/vertebra/db/dbapi/{__init__,_sync,_async,_errors}.py`, `extensions/sqlalchemy/async_adapter.py`, and `extensions/sqlalchemy/pool.py`, while `native-sqlite-driver` delivers the Rust-backed SQLite driver.

At `main @ 18baadcd`, three structural defects remain across SQLAlchemy, Advanced Alchemy, and Django [^sa-dbapi] [^sa-alembic] [^django-backends]:
1. `SyncDBAPIConnection` opens a separate stdlib `sqlite3.connect` handle (`_sqlite_conn`), defaults empty URLs to `sqlite+vtb:///:memory:`, and dispatches autocommit/migration rules via `isinstance` checks on `BigQueryConfig`, `SpannerConfig`, `SQLiteConfig`, `PostgreSQLConfig`, and `MySQLConfig`.
2. Monolithic `dialects.py` (`1,656` lines) and `alembic.py` (`418` lines) mix inline PEP 249 shims with partial hand-rolled cloud/analytical compilers, while `alchemy/cache.py` lacks real `dogpile.cache` backend registration (`dogpile.cache` is absent from `pyproject.toml`) and retains a duplicate `sqlalchemy/cache.py` shim [^aa-cache].
3. `extensions/django/db/backends/` routes through a generic vendor-dispatching `DatabaseWrapperMixin` and ships hollow stubs for `bigquery`, `duckdb`, and `mssql`, which have no built-in Django backends.

**Non-Goals**: SQLSpec adapters (`sqlspec-adapter-parity`), Django `django.tasks` (`queues-tasks-and-channels`), `StatementCache` removal and initial `db/dbapi` extraction (`broker-pool-sharing`), and native SQLite wheel compilation (`native-sqlite-driver`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Shared `vertebra.db.dbapi` Hardening & Purity)**: Consume `vertebra.db.dbapi` (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`) in place; remove `_sqlite_conn`, stdlib `sqlite3`, `url="sqlite+vtb:///:memory:"`, and `database=`; raise `InterfaceError` on empty/missing URLs; forbid imports of SQLAlchemy, Django, framework, or engine-adapter modules inside `db/dbapi/`.
- **R2 (Engine-Neutral Config Metadata & `async_adapter.py`)**: Declare `requires_statement_autocommit: ClassVar[bool] = False` (`True` on `BigQueryConfig` and `SpannerConfig`) and `effective_direct_dsn -> str` on `BaseDatabaseConfig`; isolate SQLAlchemy greenlet `await_only` adaptation in `extensions/sqlalchemy/async_adapter.py` and engine helpers (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`) in `extensions/sqlalchemy/engine.py`.
- **R3 (Custom SQLAlchemy Types)**: `extensions/sqlalchemy/types.py` implements `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON` (routing strictly through `encode_json` / `decode_json`).
- **R4–R7 (Per-Engine `Sync<Engine>Dialect` / `Async<Engine>Dialect` & 36 Entry Points)**: Split `extensions/sqlalchemy/dialects/{_base,postgresql,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner,__init__}.py` (zero `Vertebra*` or `*VtbDialect` class names), subclassing SQLAlchemy built-in relational bases and `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module` -> `MissingDependencyError`); delete monolithic `dialects.py` and synthetic-only helpers; preserve all `36` `[project.entry-points."sqlalchemy.dialects"]` entries (`<engine>+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async` across `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`).
- **R8–R9 (Modular Alembic, `dogpile.cache>=1.3.3` & Advanced Alchemy `1.12.0+`)**: Split `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py` (deleting `alembic.py`); add `dogpile.cache>=1.3.3` and register `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` `CacheBackend`s returning `NO_VALUE` on misses; retain `SQLAlchemyCacheBackend`, `AsyncCacheManager`, and `ZeroCopyServiceMixin`; delete `shm://` and `extensions/sqlalchemy/cache.py`; wire `normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes` alongside `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig` [^aa-plugin].
- **R10–R12 (Django `6.0+` Database Backends)**: `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django built-ins over `vertebra.db.dbapi`; `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`; delete `django/db/backends/{bigquery,duckdb,mssql}/` and vendor dispatch in `backends/__init__.py`, raising `ImproperConfigurationError` for unsupported engines.
- **R13 (Docs & OKF Sync)**: Synchronize public docs and OKF chapters (`<= 200` lines each).

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`ch07:t01`): `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/reference/extensions.md` (`pymdownx.tabbed` install blocks, 4–6 node conceptual Mermaid diagrams).
- OKF knowledge chapters (`ch07:t02`, each `<= 200` lines): [sqlspec-alchemy-and-dialects.md](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [litestar.md](../../knowledge/domains/extensions/litestar.md), and [database-driver-limitations.md](../../knowledge/domains/data/database-driver-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R3` DB-API, metadata, mixins & types | `ch01` (`t01`–`t03`) | `pyproject.toml`, `db/dbapi/{_sync,_async}.py`, `db/driver/_common.py`, `extensions/sqlalchemy/{async_adapter,engine,types,dialects/_base}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R4`–`R5` Relational SQLAlchemy dialects | `ch02` (`t01`–`t03`) | `extensions/sqlalchemy/dialects/{postgresql,sqlite,mysql,oracle,mssql}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R6`–`R7` Cloud/OLAP dialects & 36 entry points | `ch03` (`t01`–`t03`) | `extensions/sqlalchemy/dialects/{duckdb,bigquery,spanner,__init__}.py`, `pyproject.toml` | `pytest test_gcp_adapters_and_sqlalchemy.py` |
| `R8`–`R9` Alembic, `dogpile.cache` & AA `1.12+` | `ch04` (`t01`–`t03`) | `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py`, `extensions/alchemy/{cache,plugin,storage}.py` | `pytest test_alchemy_cache_backend.py` |
| `R10` Django relational DB backends | `ch05` (`t01`–`t02`) | `extensions/django/db/backends/{postgresql,sqlite3,mysql,oracle}/base.py` | `pytest test_django_plugin.py` |
| `R11`–`R12` Django Spanner & stub cleanup | `ch06` (`t01`–`t02`) | `extensions/django/db/backends/{spanner/base,__init__}.py`, `extensions/django/plugin.py` | `pytest test_django_plugin.py` |
| `R13` Docs & OKF knowledge sync | `ch07` (`t01`–`t02`) | `docs/usage/{extensions,databases}.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md` | `review_setup.py validate` |

## 5. Implementation Plan

- [ ] [ch01-dbapi-hardening-and-dialect-base](chapters/ch01-dbapi-hardening-and-dialect-base/spec.md) — Harden `vertebra.db.dbapi` (explicit URL, zero stdlib `sqlite3`), engine-neutral `BaseDatabaseConfig` metadata, `_SyncDialectMixin`/`_AsyncDialectMixin`, and `types.py`.
- [ ] [ch02-sqlalchemy-core-relational-dialects](chapters/ch02-sqlalchemy-core-relational-dialects/spec.md) — Split `postgresql`, `sqlite`, `mysql`, `oracle`, and `mssql` `Sync<Engine>Dialect`/`Async<Engine>Dialect` onto SQLAlchemy built-ins.
- [ ] [ch03-sqlalchemy-analytical-cloud-dialects](chapters/ch03-sqlalchemy-analytical-cloud-dialects/spec.md) — Split `duckdb`, `bigquery`, `spanner`, and `spangres` onto upstream dialect bases, delete `dialects.py`, and repoint all 36 entry points.
- [ ] [ch04-alembic-dogpile-and-advanced-alchemy](chapters/ch04-alembic-dogpile-and-advanced-alchemy/spec.md) — Split `alembic/{impl,ops,hooks}.py`, register `dogpile.cache>=1.3.3` backends, delete `sqlalchemy/cache.py`, and wire Advanced Alchemy `1.12.0+`.
- [ ] [ch05-django-relational-backends](chapters/ch05-django-relational-backends/spec.md) — Rebase Django `6.0+` `postgresql`, `sqlite3`, `mysql`, and `oracle` `DatabaseWrapper` backends onto Django built-ins over `vertebra.db.dbapi`.
- [ ] [ch06-django-spanner-and-unsupported-cleanup](chapters/ch06-django-spanner-and-unsupported-cleanup/spec.md) — Subclass `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi` and delete unsupported `bigquery`, `duckdb`, and `mssql` Django DB backends.
- [ ] [ch07-sqlalchemy-django-docs-and-okf](chapters/ch07-sqlalchemy-django-docs-and-okf/spec.md) — Synchronize `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-dbapi-hardening-and-dialect-base` (`sqlalchemy-and-django-integration:ch01:t01`, `0/18` tasks completed across `7` chapters).
- **Upstream Flow Gates**: Requires `broker-pool-sharing` (`ch07` extracts `vertebra.db.dbapi`, `async_adapter.py`, and `pool.py`) and `native-sqlite-driver` (native Rust SQLite driver replacing stdlib `sqlite3`).
- **Full `/flow:finish` Gate**: `make lint-all && make py-test && make docs`.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect and PEP 249 DB-API module slated for per-engine split.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — PEP 249 connection with a second sqlite3 connection, default SQLite URL, and isinstance config dispatch.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic DefaultImpl integration checking concrete config classes.
[^entry-points]: `pyproject.toml` — 36 sqlalchemy.dialects entry points across 9 database engines.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin.
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy SQLAlchemySyncConfig / SQLAlchemyAsyncConfig and normalize_alchemy_url helpers.
[^django-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Generic Django DatabaseWrapperMixin with vendor config dispatch.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Fail-loud baseline raising InterfaceError on DB-API URL parse failures.
