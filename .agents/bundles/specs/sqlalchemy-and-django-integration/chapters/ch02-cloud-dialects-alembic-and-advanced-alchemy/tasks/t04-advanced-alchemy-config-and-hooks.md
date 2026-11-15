---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t04
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Wire Advanced Alchemy Cache/Engine Configs, SQLAlchemy Cache Exports, and Alembic Hooks
description: Wire create_cache_config (CacheConfig(region_factory=...) + AsyncCacheManager), re-export SyncCacheBackend/AsyncCacheBackend/CompiledCache/ResultCache in extensions/sqlalchemy/cache.py, implement Alembic hooks.py, and integrate SQLAlchemySyncConfig/SQLAlchemyAsyncConfig.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t03
files:
  - src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py
  - src/py/vertebra/extensions/sqlalchemy/cache.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/vertebra/extensions/alchemy/plugin.py
tests:
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
  - src/py/vertebra/extensions/sqlalchemy/types.py
  - src/py/vertebra/extensions/sqlalchemy/cache.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/alchemy/plugin.py
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/db/registry.py
estimated_loc: 210
tags:
  - advanced-alchemy
  - sqlalchemy
  - cache
  - alembic
  - autogenerate
sources:
  - id: sa-cache
    resource: src/py/vertebra/extensions/sqlalchemy/cache.py
    title: Re-export shim for CacheEntry, CacheStats, and SQLAlchemyCacheBackend (target CompiledCache, ResultCache, and native cache re-exports)
  - id: aa-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: normalize_alchemy_url and SQLAlchemySyncConfig / SQLAlchemyAsyncConfig factories
  - id: aa-storage
    resource: src/py/vertebra/extensions/alchemy/storage.py
    title: Advanced Alchemy FileObject ObjectStoreBackend
verification_strategy: regression_tdd
---

# Task `t04`: Wire Advanced Alchemy Cache/Engine Configs, SQLAlchemy Cache Exports, and Alembic Hooks

## Objective & Scope Boundary

Implement Alembic autogenerate comparator hooks in `alembic/hooks.py`, wire `create_cache_config(url="shm://...", l1_url=..., use_dogpile=False)` (`CacheConfig(region_factory=...)` + `AsyncCacheManager`) and `SQLAlchemyPlugin` in `extensions/alchemy/plugin.py`, and expose `SyncCacheBackend`, `AsyncCacheBackend`, `CompiledCache`, and `ResultCache` cleanly in `extensions/sqlalchemy/cache.py` and `extensions/sqlalchemy/__init__.py` per [`../spec.md`](../spec.md) [^sa-cache] [^aa-plugin] [^aa-storage].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py` & `types.py` — vector/JSON types and Spanner/BigQuery table options.
- `src/py/vertebra/extensions/sqlalchemy/cache.py` & `__init__.py` — SQLAlchemy `CompiledCache` (`MutableMapping` for `execution_options={"compiled_cache": ...}`) and `ResultCache` (`SyncCacheBackend` / `AsyncCacheBackend` query result caching).
- `src/py/vertebra/extensions/alchemy/cache.py` & `plugin.py` — `create_cache_config`, `normalize_alchemy_url`, `create_sync_alchemy_config`, and `create_async_alchemy_config`.
- `src/py/vertebra/db/registry.py` — registered `BaseDatabaseConfig.url_schemes` lookup.

## Contracts & Edge Cases

- Register `@comparators.dispatch_for` hooks in `alembic/hooks.py` comparing `PgVector`/`OracleVector`/`SpannerVector` dimensions, `MsgspecJSON`, Spanner `INTERLEAVE IN PARENT` / `ROW DELETION POLICY`, and BigQuery `PARTITION BY` / `CLUSTER BY` without false-positive autogenerate diffs.
- Update `create_cache_config(url="shm://vertebra-alchemy-cache", *, l1_url=None, backend=None, expiration_time=3600, use_dogpile=False)` so default `use_dogpile=False` configures `CacheConfig(region_factory=...)` returning `SyncCacheBackend` (`SyncCacheRegionProtocol`) paired with `AsyncCacheManager(CacheManager)` executing all `*_async` methods directly on `AsyncCacheBackend` on the event loop (zero `dogpile.cache` dependency in `[project.optional-dependencies].alchemy`; the optional `dogpile` extra carries it), while `use_dogpile=True` registers and configures the matching `dogpile.cache` backend (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`).
- Keep `src/py/vertebra/extensions/sqlalchemy/cache.py` as the canonical SQLAlchemy caching entry point exporting `SyncCacheBackend`, `AsyncCacheBackend`, `CompiledCache`, and `ResultCache` (zero `Legacy*`/`SQLAlchemyCacheBackend` alias shims).
- Update `normalize_alchemy_url` in `extensions/alchemy/plugin.py` to resolve schemes dynamically from `BaseDatabaseConfig.url_schemes` into `<canonical_engine>+{vtb,vtb_async}://...` across all registered engines.

## Test Matrix

- Alembic `autogenerate` diff tests on PostgreSQL and Spanner schemas (zero false diffs when unchanged; emits `CreateVectorIndexOp` / `InterleaveTableOp` when modified).
- Parametrized `create_cache_config(use_dogpile=False)` (default `region_factory` + `AsyncCacheManager`) and `create_cache_config(use_dogpile=True)` tests across `shm://`, `memory://`, `valkey://`, `redis://`, and `file://`, plus SQLAlchemy `CompiledCache` and `ResultCache` tests.
- Parametrized `normalize_alchemy_url` tests across all registered `BaseDatabaseConfig.url_schemes` and `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig` wiring tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q
```

## Acceptance Criteria

- [ ] `alembic/hooks.py` eliminates false-positive vector/interleave autogenerate diffs.
- [ ] `create_cache_config` defaults to native `CacheConfig(region_factory=...)` + `AsyncCacheManager` (zero `dogpile.cache` required) and supports `use_dogpile=True`.
- [ ] `extensions/sqlalchemy/cache.py` exports `SyncCacheBackend`, `AsyncCacheBackend`, `CompiledCache`, and `ResultCache`.
- [ ] `normalize_alchemy_url` dynamically reads `BaseDatabaseConfig.url_schemes` with zero hardcoded vendor branches.
- [ ] Zero regressions in the fast verification gate.

[^sa-cache]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — SQLAlchemy CompiledCache, ResultCache, and native cache re-exports.
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — normalize_alchemy_url and SQLAlchemySyncConfig / SQLAlchemyAsyncConfig factories.
[^aa-storage]: `src/py/vertebra/extensions/alchemy/storage.py` — Advanced Alchemy FileObject ObjectStoreBackend.
