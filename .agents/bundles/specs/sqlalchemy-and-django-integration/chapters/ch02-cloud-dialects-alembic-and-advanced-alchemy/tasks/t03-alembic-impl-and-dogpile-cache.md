---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t03
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Implement Alembic DefaultImpl Subclasses and dogpile.cache Region Backends
description: Register Alembic dialect implementations (including Spanner batch DDL and BigQuery DDL) and build native Valkey/Redis/SHM dogpile.cache backends.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t02
files:
- src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py
- src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
- src/py/vertebra/extensions/sqlalchemy/alembic/ops.py
- src/py/vertebra/extensions/alchemy/cache.py
- src/py/vertebra/extensions/alchemy/__init__.py
tests:
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/extensions/test_alchemy_cache_backend.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/alembic.py
- src/py/vertebra/db/driver/_common.py
- src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py
- src/py/vertebra/extensions/alchemy/cache.py
- src/py/vertebra/extensions/sqlalchemy/cache.py
- src/py/vertebra/db/adapters/valkey/driver.py
estimated_loc: 235
tags:
- alembic
- sqlalchemy
- ddl
- advanced-alchemy
- dogpile-cache
- valkey
- redis
sources:
- id: sa-alembic
  resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
  title: Monolithic Alembic DefaultImpl and vector/view DDL operations
- id: db-common
  resource: src/py/vertebra/db/driver/_common.py
  title: BaseDatabaseConfig effective_direct_dsn metadata
- id: aa-cache
  resource: src/py/vertebra/extensions/alchemy/cache.py
  title: Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin
- id: sa-cache-shim
  resource: src/py/vertebra/extensions/sqlalchemy/cache.py
  title: Re-export shim slated for deletion
---

# Task `t03`: Implement Alembic DefaultImpl Subclasses and dogpile.cache Region Backends

## Objective & Scope Boundary

Delete monolithic `extensions/sqlalchemy/alembic.py` and split into `alembic/{__init__,impl,ops}.py` per [`../spec.md`](../spec.md) [^sa-alembic] [^db-common]. Out of scope: autogenerate comparator hooks (`t03`).

Implement real `dogpile.cache` `CacheBackend` subclasses in `extensions/alchemy/cache.py` and delete `extensions/sqlalchemy/cache.py` per [`../spec.md`](../spec.md) [^aa-cache] [^sa-cache-shim].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — existing `DefaultImpl` subclasses and DDL ops to refactor and remove.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.effective_direct_dsn`.
- `src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py` — `update_ddl` batching seam.

- `src/py/vertebra/extensions/alchemy/cache.py` — `SQLAlchemyCacheBackend`, `create_cache_config`, `AsyncCacheManager`, and `ZeroCopyServiceMixin`.
- `src/py/vertebra/extensions/sqlalchemy/cache.py` — legacy re-export shim to delete via `git rm`.
- `src/py/vertebra/db/adapters/valkey/driver.py` & `redis/driver.py` — native `SyncValkeyDriver`/`SyncRedisDriver` and `AsyncValkeyDriver`/`AsyncRedisDriver`.

## Contracts & Edge Cases

- Implement `DefaultImpl` subclasses registered via `ImplMeta` (`PostgreSQLAlembicImpl`, `MySQLAlembicImpl`, `SQLiteAlembicImpl`, `OracleAlembicImpl`, `MSSQLAlembicImpl`, `DuckDBAlembicImpl`, `BigQueryAlembicImpl` with `transactional_ddl = False`, `SpannerAlembicImpl` and `SpangresAlembicImpl` with `transactional_ddl = False` and batched `update_ddl`).
- Resolve direct migration URLs from `BaseDatabaseConfig.effective_direct_dsn` with zero `isinstance(..., (PostgreSQLConfig, MySQLConfig))` checks.
- Implement Alembic `MigrateOperation` + `@Operations.implementation_for` directives (`CreateVectorIndexOp`, `DropVectorIndexOp`, `CreateViewOp`, `DropViewOp`, `InterleaveTableOp`, `RowDeletionPolicyOp`).

- Implement `ValkeyCacheBackend(dogpile.cache.api.CacheBackend)`, `RedisCacheBackend(CacheBackend)`, and `MemoryCacheBackend(CacheBackend)` returning `dogpile.cache.api.NO_VALUE` on misses (`get_serialized`, `get_serialized_multi`, `set_serialized`, `set_serialized_multi`, `delete`, `delete_multi`) and register `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` via `dogpile.cache.register_backend`.
- Fix `create_cache_config` so URL strings map to registered `dogpile.cache` backend names + driver `arguments` instead of passing raw URLs as backend names; remove dead `shm_name`, unused `TYPE_CHECKING` blocks, and the process-local fake `shm://` scheme.
- Keep `SQLAlchemyCacheBackend` (`MutableMapping` compiled-cache + prefix-scoped `SCAN`+`UNLINK`), `AsyncCacheManager` (native async L2 caching), and `ZeroCopyServiceMixin`; delete `src/py/vertebra/extensions/sqlalchemy/cache.py`.

## Test Matrix

- Parametrized DDL rendering, `ImplMeta` dialect lookup, non-transactional Spanner `update_ddl` batching, and `effective_direct_dsn` URL resolution tests.

- Parametrized `make_region().configure('vertebra.memory' | 'vertebra.valkey' | 'vertebra.redis')` and Advanced Alchemy stock `CacheManager` hit/miss (`NO_VALUE`), TTL expiration, and `ZeroCopyServiceMixin` tests.
- Negative test confirming `shm://` raises `ImproperConfigurationError` and `vertebra.extensions.sqlalchemy.cache` no longer exists.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] Monolithic `extensions/sqlalchemy/alembic.py` is deleted and replaced by `alembic/{__init__,impl,ops}.py`.
- [ ] Zero concrete `PostgreSQLConfig` or `MySQLConfig` `isinstance` checks remain in Alembic integration.
- [ ] `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` work natively with `dogpile.cache` and Advanced Alchemy `1.12.0+` `CacheManager`.
- [ ] `src/py/vertebra/extensions/sqlalchemy/cache.py` and the fake `shm://` scheme are removed.
- [ ] Zero regressions in the fast verification gate.

[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic DefaultImpl and vector/view DDL operations.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig effective_direct_dsn metadata.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin.
[^sa-cache-shim]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Re-export shim slated for deletion.
