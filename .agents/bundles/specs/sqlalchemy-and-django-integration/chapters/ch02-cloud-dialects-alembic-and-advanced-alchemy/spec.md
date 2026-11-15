---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: 'Chapter 02: Analytical/Cloud SQLAlchemy Dialects, Alembic Operations, Native Caching, and Advanced Alchemy'
description: Implement DuckDB, BigQuery, and Spanner/Spangres SQLAlchemy dialects and entrypoints, wire Alembic DefaultImpl and migration ops, build Rust ShmCache + configurable SyncCacheBackend/AsyncCacheBackend + optional dogpile.cache plugins, and integrate Advanced Alchemy 1.12.0+.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
depends_on_chapters:
  - ch01-dbapi-foundation-and-relational-dialects
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
tags:
  - sqlalchemy
  - duckdb
  - bigquery
  - spanner
  - spangres
  - advanced-alchemy
  - alembic
  - cache
  - shm
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
  - id: shm-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Rust POSIX shared-memory lease table and ring buffer
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: Advanced Alchemy cache module
  - id: aa-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: Advanced Alchemy config and URL helpers
  - id: sa-cache
    resource: src/py/vertebra/extensions/sqlalchemy/cache.py
    title: Re-export shim for CacheEntry, CacheStats, and SQLAlchemyCacheBackend (target CompiledCache, ResultCache, and native cache re-exports)
current_task: "sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t01"
---

# Chapter 02: Analytical/Cloud SQLAlchemy Dialects, Alembic Operations, Native Caching, and Advanced Alchemy

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `DuckDB`, `BigQuery`, and `Spanner` dialects hand-roll `DefaultDialect` subclasses inside `dialects.py` (`Spangres` subclasses `PGDialect`), and `alembic.py::resolve_migration_url` checks `isinstance(url, PostgreSQLConfig)` / `isinstance(url, MySQLConfig)`. `alchemy/cache.py::SQLAlchemyCacheBackend` treats `shm://` as a process-local `OrderedDict`, passes the raw URL as `CacheConfig.backend`, and already wires `region_factory` + `AsyncCacheManager` over Valkey/Redis, `normalize_alchemy_url` hardcodes only Google Cloud schemes, and `pyproject.toml` points all 36 entry points at `vertebra.extensions.sqlalchemy.dialects` [^sa-dialects] [^entry-points] [^sa-alembic] [^aa-cache] [^aa-plugin].
- **Target Architecture**: `dialects/{duckdb,bigquery,spanner}.py` subclass `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` over `vertebra.db.dbapi`, `dialects.py` is deleted, and all 36 entry points target `Sync<Engine>Dialect` / `Async<Engine>Dialect`; modular `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py` implements `DefaultImpl` and autogenerate hooks; `vertebra._vertebra.ShmCache` (`src/rs/vertebra-core/src/broker.rs` + `src/rs/vertebra-py/src/broker.rs`) and configurable `SyncCacheBackend` (`SyncCacheRegionProtocol`) / `AsyncCacheBackend` (`extensions/alchemy/cache.py`, re-exported in `extensions/sqlalchemy/cache.py`) support `shm://<segment>` (with `ShmLeaseTable` ring-buffer L1 invalidation), `memory://`, `valkey://` (`vertebra._valkey`), `redis://` (`vertebra._redis`), and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`vertebra._storage` `VTBSTORE\x01`), powering `CacheConfig(region_factory=...)` + `AsyncCacheManager` by default and optional `dogpile.cache` backend plugins (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`); `normalize_alchemy_url` reads `BaseDatabaseConfig.url_schemes` [^bq-config] [^spn-config] [^shm-broker] [^sa-cache].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter completes the per-engine SQLAlchemy dialect split across `DuckDB`, `BigQuery`, `Spanner`, and `Spangres`, deletes monolithic `dialects.py`, and repoints all 36 `<engine>+{vertebra,vtb,vertebra_async,vtb_async}` entry points in `pyproject.toml` [^entry-points]. It also splits `alembic.py` into `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py`, builds Rust `ShmCache` + configurable `SyncCacheBackend` and `AsyncCacheBackend` across `shm://`, `memory://`, `valkey://`, `redis://`, and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`, wires Advanced Alchemy `1.12.0+` `CacheConfig(region_factory=...)` + `AsyncCacheManager` (with optional `dogpile.cache` backend plugins when `dogpile.cache` is installed), updates `extensions/sqlalchemy/cache.py`, and generalizes `normalize_alchemy_url` across all registered engines [^shm-broker] [^aa-cache] [^aa-plugin] [^sa-cache].

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
- **R9 (Rust `ShmCache`, `SyncCacheBackend` / `AsyncCacheBackend`, Advanced Alchemy `1.12.0+` & Optional `dogpile.cache` Plugins)**:
  - Implement `ShmCache` in `src/rs/vertebra-core/src/broker.rs` + `src/rs/vertebra-py/src/broker.rs` (next to the `LeaseBroker` `publish_channel`/`drain_channel` bindings) + `src/py/vertebra/_vertebra.pyi` (POSIX shared memory with FNV-1a bucket index, seqlock + CLOCK eviction, monotonic `expiry_ns` TTL, atomic `incr`/`decr`, and cross-worker L1 invalidations broadcast over `ShmLeaseTable` ring buffer `publish_channel`/`drain_channel` on `b"vtb:cache:inv"`).
  - Implement configurable `SyncCacheBackend` (satisfying Advanced Alchemy `SyncCacheRegionProtocol`: `get`, `get_or_create`, `set`, `delete`, `invalidate`, `configure`; plus Vertebra extensions `get_multi`, `set_multi`, `delete_multi`, `add`, `incr`, `decr`, `touch`, `has_key`, `invalidate_prefix`, `clear`, `close`) and `AsyncCacheBackend` (`get`, `set`, `delete`, `get_multi`, `set_multi`, `delete_multi`, `add`, `incr`, `decr`, `touch`, `has_key`, `invalidate_prefix`, `clear`, `aclose`) in `extensions/alchemy/cache.py` across `shm://`, `memory://`, `valkey://` (`vertebra._valkey`), `redis://` (`vertebra._redis`), and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`vertebra._storage` `VTBSTORE\x01`).
  - Register optional `dogpile.cache` backend plugins (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`) via `register_dogpile_backends()` when `dogpile.cache` is installed.
  - Wire `create_cache_config(url="shm://...", l1_url=..., use_dogpile=False)` to configure `CacheConfig(region_factory=...)` + `AsyncCacheManager(CacheManager)` by default (zero `dogpile.cache` dependency in `[project.optional-dependencies].alchemy`; the optional `dogpile` extra carries it), re-export `SyncCacheBackend`, `AsyncCacheBackend`, `CompiledCache`, and `ResultCache` in `extensions/sqlalchemy/cache.py`, and wire `normalize_alchemy_url` onto `BaseDatabaseConfig.url_schemes`.

```python
class SyncCacheBackend:
    def get(self, key: str, expiration_time: int | None = None) -> Any: ...
    def set(self, key: str, value: Any, expiration_time: int | None = None) -> None: ...
    def delete(self, key: str) -> None: ...
    def get_multi(self, keys: Sequence[str], expiration_time: int | None = None) -> list[Any]: ...
    def set_multi(self, mapping: Mapping[str, Any], expiration_time: int | None = None) -> None: ...
    def delete_multi(self, keys: Sequence[str]) -> None: ...
    def add(self, key: str, value: Any, expiration_time: int | None = None) -> bool: ...
    def incr(self, key: str, delta: int = 1) -> int: ...
    def decr(self, key: str, delta: int = 1) -> int: ...
    def touch(self, key: str, expiration_time: int | None = None) -> bool: ...
    def has_key(self, key: str) -> bool: ...
    def invalidate_prefix(self, prefix: str) -> int: ...
    def clear(self) -> None: ...
    def close(self) -> None: ...

class AsyncCacheBackend:
    async def get(self, key: str, expiration_time: int | None = None) -> Any: ...
    async def set(self, key: str, value: Any, expiration_time: int | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...
    async def get_multi(self, keys: Sequence[str], expiration_time: int | None = None) -> list[Any]: ...
    async def set_multi(self, mapping: Mapping[str, Any], expiration_time: int | None = None) -> None: ...
    async def delete_multi(self, keys: Sequence[str]) -> None: ...
    async def add(self, key: str, value: Any, expiration_time: int | None = None) -> bool: ...
    async def incr(self, key: str, delta: int = 1) -> int: ...
    async def decr(self, key: str, delta: int = 1) -> int: ...
    async def touch(self, key: str, expiration_time: int | None = None) -> bool: ...
    async def has_key(self, key: str) -> bool: ...
    async def invalidate_prefix(self, prefix: str) -> int: ...
    async def clear(self) -> None: ...
    async def aclose(self) -> None: ...

def create_cache_config(
    url: str = "shm://vertebra-alchemy-cache",
    *,
    l1_url: str | None = None,
    backend: SyncCacheBackend | AsyncCacheBackend | None = None,
    expiration_time: int = 3600,
    use_dogpile: bool = False,
) -> CacheConfig: ...
```

## 3. Documentation & OKF Knowledge Plan

Entry-point mappings, optional extra install tables, and Advanced Alchemy `1.12.0+` / SQLAlchemy / Django native caching and Alembic migration guides are updated in `ch03-django-backends-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Native DuckDB and BigQuery Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py`, `src/py/vertebra/db/adapters/bigquery/config.py` | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t02` | Implement Spanner/Spangres Dialects, Dialect Barrel, and 36 Entrypoints | `src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py`, `src/py/vertebra/db/adapters/spanner/config.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py`, `src/py/vertebra/extensions/sqlalchemy/dialects.py` (deleted) (+2 more) | `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Implement Alembic DefaultImpl, Rust ShmCache, and Sync/Async Native Cache Backends | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi`, `src/py/vertebra/extensions/sqlalchemy/alembic/{__init__,impl,ops}.py`, `src/py/vertebra/extensions/sqlalchemy/alembic.py` (deleted), `src/py/vertebra/extensions/alchemy/{cache,__init__}.py` | `src/py/tests/unit/extensions/test_alchemy_cache_backend.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t04` | Wire Advanced Alchemy Cache/Engine Configs, SQLAlchemy Cache Exports, and Alembic Hooks | `src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py`, `src/py/vertebra/extensions/sqlalchemy/{cache,__init__}.py`, `src/py/vertebra/extensions/alchemy/plugin.py` | `src/py/tests/unit/extensions/test_alchemy_cache_backend.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |

## 5. Implementation Plan

- [ ] [t01-duckdb-and-bigquery-dialects.md](tasks/t01-duckdb-and-bigquery-dialects.md) — Build dialects/duckdb.py and dialects/bigquery.py with STRUCT/ARRAY/JSON reflection and named @param binds.
- [ ] [t02-spanner-dialects-and-entrypoints.md](tasks/t02-spanner-dialects-and-entrypoints.md) — Build dialects/spanner.py (GoogleSQL and Spangres), convert dialects/__init__.py barrel, delete dialects.py, and repoint all 36 sqlalchemy.dialects entrypoints in pyproject.toml.
- [ ] [t03-alembic-impl-and-dogpile-cache.md](tasks/t03-alembic-impl-and-dogpile-cache.md) — Split Alembic DefaultImpl and DDL ops into `alembic/{__init__,impl,ops}.py`, implement Rust `ShmCache` + SHM ring-buffer L1 invalidation, and build configurable `SyncCacheBackend` (`SyncCacheRegionProtocol`) and `AsyncCacheBackend` (`shm://`, `memory://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`) plus optional `dogpile.cache` backend plugins.
- [ ] [t04-advanced-alchemy-config-and-hooks.md](tasks/t04-advanced-alchemy-config-and-hooks.md) — Wire `create_cache_config(url="shm://...", l1_url=..., use_dogpile=False)` (`CacheConfig(region_factory=...)` + `AsyncCacheManager`), re-export `SyncCacheBackend`/`AsyncCacheBackend`/`CompiledCache`/`ResultCache` in `extensions/sqlalchemy/cache.py`, implement Alembic `hooks.py`, and integrate `SQLAlchemySyncConfig`/`SQLAlchemyAsyncConfig`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-cloud-dialects-alembic-and-advanced-alchemy` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver`.

Fast Chapter Gate (`< 60s` offline):

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q
```

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect module.
[^entry-points]: `pyproject.toml` — SQLAlchemy dialect entry points.
[^bq-config]: `src/py/vertebra/db/adapters/bigquery/config.py` — BigQueryConfig metadata.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig metadata.
[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic module.
[^shm-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Rust POSIX shared-memory lease table and ring buffer.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache module.
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy config and URL helpers.
[^sa-cache]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Re-export shim for CacheEntry, CacheStats, and SQLAlchemyCacheBackend (target CompiledCache, ResultCache, and native cache re-exports).
