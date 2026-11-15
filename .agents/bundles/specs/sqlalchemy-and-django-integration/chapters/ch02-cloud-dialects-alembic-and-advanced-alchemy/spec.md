---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: 'Chapter 02: Analytical/Cloud SQLAlchemy Dialects, Alembic Operations, dogpile.cache, and Advanced Alchemy'
description: Implement DuckDB, BigQuery, and Cloud Spanner/Spangres SQLAlchemy dialects and entrypoints, wire Alembic DefaultImpl and migration ops, build dogpile.cache backends, and integrate Advanced Alchemy.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
depends_on_chapters:
- ch01-dbapi-foundation-and-relational-dialects
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlalchemy
- duckdb
- bigquery
- spanner
- spangres
- advanced-alchemy
- alembic
- dogpile-cache
sources:
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Monolithic SQLAlchemy dialect module
- id: entry-points
  resource: pyproject.toml
  title: SQLAlchemy dialect entry points
- id: bq-config
  resource: src/py/vertebra/db/adapters/bigquery/config.py
  title: BigQueryConfig metadata
- id: spn-config
  resource: src/py/vertebra/db/adapters/spanner/config.py
  title: SpannerConfig metadata
- id: sa-alembic
  resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
  title: Monolithic Alembic module
- id: aa-cache
  resource: src/py/vertebra/extensions/alchemy/cache.py
  title: Advanced Alchemy cache module
- id: aa-plugin
  resource: src/py/vertebra/extensions/alchemy/plugin.py
  title: Advanced Alchemy config and URL helpers
- id: sa-cache-shim
  resource: src/py/vertebra/extensions/sqlalchemy/cache.py
  title: Legacy SQLAlchemy cache re-export shim
---

# Chapter 02: Analytical/Cloud SQLAlchemy Dialects, Alembic Operations, dogpile.cache, and Advanced Alchemy

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `DuckDB`, `BigQuery`, `Spanner`, and `Spangres` dialects hand-roll partial `DefaultDialect` stubs inside `dialects.py`, `alembic.py` checks `isinstance(cfg, (PostgreSQLConfig, MySQLConfig))`, `alchemy/cache.py` passes raw URLs as `dogpile` backend names and retains a process-local `shm://` scheme alongside the `sqlalchemy/cache.py` shim, `normalize_alchemy_url` hardcodes only GCP schemes, and `pyproject.toml` points all 36 entry points at `vertebra.extensions.sqlalchemy.dialects` [^sa-dialects] [^entry-points] [^sa-alembic] [^aa-cache] [^aa-plugin] [^sa-cache-shim].
- **Target Architecture**: `dialects/{duckdb,bigquery,spanner}.py` subclass `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` over `vertebra.db.dbapi`, `dialects.py` is deleted, and all 36 entry points target `Sync<Engine>Dialect` / `Async<Engine>Dialect`; modular `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py` implements `DefaultImpl` and autogenerate hooks; `extensions/alchemy/cache.py` registers `dogpile.cache>=1.3.3` backends (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`); `sqlalchemy/cache.py` is deleted; `normalize_alchemy_url` reads `BaseDatabaseConfig.url_schemes` [^bq-config] [^spn-config].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter completes the per-engine SQLAlchemy dialect split across `DuckDB`, `BigQuery`, `Spanner`, and `Spangres`, deletes monolithic `dialects.py`, and repoints all 36 `<engine>+{vertebra,vtb,vertebra_async,vtb_async}` entry points in `pyproject.toml` [^entry-points]. It also splits `alembic.py` into `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py`, integrates `dogpile.cache>=1.3.3` region backends for Advanced Alchemy `1.12.0+` (`SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`, `CacheManager`, `AsyncCacheManager`), removes `sqlalchemy/cache.py`, and generalizes `normalize_alchemy_url` across all registered engines [^aa-cache] [^aa-plugin].

## 2. Requirements, Concrete Contracts & Architecture

- **R6 (Analytical & Cloud Upstream Bases)**: Subclass `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module` -> `MissingDependencyError`).
- **R7 (36 Entry Points & Clean Cutover)**: Keep all 4 driver names per engine (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) across all 9 engines (`36` total) targeting `Sync<Engine>Dialect` / `Async<Engine>Dialect`, and delete `dialects.py`.

```python
class SyncDuckDBDialect(_SyncDialectMixin, DuckDBEngineDialect): ...
class AsyncDuckDBDialect(_AsyncDialectMixin, DuckDBEngineDialect): ...
class SyncBigQueryDialect(_SyncDialectMixin, UpstreamBigQueryDialect): ...
class AsyncBigQueryDialect(_AsyncDialectMixin, UpstreamBigQueryDialect): ...
class SyncSpannerDialect(_SyncDialectMixin, UpstreamSpannerDialect): ...
class AsyncSpannerDialect(_AsyncDialectMixin, UpstreamSpannerDialect): ...
class SyncSpangresDialect(_SyncDialectMixin, UpstreamSpannerDialect): ...
class AsyncSpangresDialect(_AsyncDialectMixin, UpstreamSpannerDialect): ...

def register_sqlalchemy_dialects() -> None: ...
```

- **R8 (Modular Alembic Package)**: Split `alembic/{__init__,impl,ops,hooks}.py`, batch Spanner non-transactional DDL via `update_ddl`, and resolve migration URLs via `BaseDatabaseConfig.effective_direct_dsn`.
- **R9 (`dogpile.cache` & Advanced Alchemy `1.12.0+`)**: Register `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` `dogpile.cache` backends returning `NO_VALUE` on misses, delete `shm://` and `sqlalchemy/cache.py`, and wire `normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes`.

```python
class SpannerAlembicImpl(DefaultImpl):
    __dialect__ = "spanner"
    transactional_ddl = False

class SpangresAlembicImpl(DefaultImpl):
    __dialect__ = "spangres"
    transactional_ddl = False

class ValkeyCacheBackend(CacheBackend):
    def get_serialized(self, key: str) -> bytes | Any: ...
    def set_serialized(self, key: str, value: bytes) -> None: ...
    def delete(self, key: str) -> None: ...

def normalize_alchemy_url(connection_string: str, *, is_async: bool = True) -> str: ...
def create_cache_config(
    url: str | None = None,
    *,
    backend: SQLAlchemyCacheBackend | None = None,
    expiration_time: int = 3600,
) -> CacheConfig: ...
```

## 3. Documentation & OKF Knowledge Plan

Entry-point mappings, optional extra install tables, and Advanced Alchemy `1.12.0+` caching and Alembic migration guides are updated in `ch03-django-backends-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Native DuckDB and BigQuery Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py`, `src/py/vertebra/db/adapters/bigquery/config.py` | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t02` | Implement Spanner/Spangres Dialects, Dialect Barrel, and 36 Entrypoints | `src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py`, `src/py/vertebra/db/adapters/spanner/config.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py` (+2 more) | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Implement Alembic DefaultImpl Subclasses and dogpile.cache Region Backends | `src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py`, `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py`, `src/py/vertebra/extensions/sqlalchemy/alembic/ops.py` (+3 more) | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/extensions/test_alchemy_cache_backend.py` |
| `t04` | Wire Advanced Alchemy Engine Configs, Lifespan Session Hooks, and Repository Tests | `src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py`, `src/py/vertebra/extensions/alchemy/plugin.py`, `src/py/vertebra/extensions/alchemy/storage.py` | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [ ] [t01-duckdb-and-bigquery-dialects.md](tasks/t01-duckdb-and-bigquery-dialects.md) — Build dialects/duckdb.py and dialects/bigquery.py with STRUCT/ARRAY/JSON reflection and named @param binds.
- [ ] [t02-spanner-dialects-and-entrypoints.md](tasks/t02-spanner-dialects-and-entrypoints.md) — Build dialects/spanner.py (GoogleSQL and Spangres), convert dialects/__init__.py barrel, delete dialects.py, and repoint all 36 sqlalchemy.dialects entrypoints in pyproject.toml.
- [ ] [t03-alembic-impl-and-dogpile-cache.md](tasks/t03-alembic-impl-and-dogpile-cache.md) — Register Alembic dialect implementations (including Spanner batch DDL and BigQuery DDL), build native Valkey/Redis/Memory dogpile.cache backends, and delete sqlalchemy/cache.py.
- [ ] [t04-advanced-alchemy-config-and-hooks.md](tasks/t04-advanced-alchemy-config-and-hooks.md) — Integrate Vertebra Sync/Async SQLAlchemy dialects with SQLAlchemySyncConfig, SQLAlchemyAsyncConfig, and SQLAlchemyAsyncRepositoryService.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-cloud-dialects-alembic-and-advanced-alchemy` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisite `native-sqlite-driver` (`broker-pool-sharing` complete and archived at `28415b3f`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect module.
[^entry-points]: `pyproject.toml` — SQLAlchemy dialect entry points.
[^bq-config]: `src/py/vertebra/db/adapters/bigquery/config.py` — BigQueryConfig metadata.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig metadata.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic module.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache module.
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy config and URL helpers.
[^sa-cache-shim]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Legacy SQLAlchemy cache re-export shim.
