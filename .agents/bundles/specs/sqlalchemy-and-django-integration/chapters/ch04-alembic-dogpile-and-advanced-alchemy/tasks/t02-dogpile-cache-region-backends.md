---
type: Task
id: "sqlalchemy-and-django-integration:ch04-alembic-dogpile-and-advanced-alchemy:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch04-alembic-dogpile-and-advanced-alchemy
title: "Implement dogpile.cache Backends in extensions/alchemy/cache.py and Delete sqlalchemy/cache.py Shim"
description: "Register dogpile.cache backends (vertebra.valkey, vertebra.redis, vertebra.memory), fix create_cache_config, remove fake shm:// scheme, and delete sqlalchemy/cache.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-dbapi-hardening-and-dialect-base:t01"
files:
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/alchemy/__init__.py
tests:
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
read_set:
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/sqlalchemy/cache.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/redis/driver.py
estimated_loc: 235
tags:
  - advanced-alchemy
  - dogpile-cache
  - valkey
  - redis
sources:
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: "Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin"
  - id: sa-cache-shim
    resource: src/py/vertebra/extensions/sqlalchemy/cache.py
    title: "Re-export shim slated for deletion"
---

# Task `t02`: Implement dogpile.cache Backends in extensions/alchemy/cache.py and Delete sqlalchemy/cache.py Shim

## Objective & Scope Boundary

Implement real `dogpile.cache` `CacheBackend` subclasses in `extensions/alchemy/cache.py` and delete `extensions/sqlalchemy/cache.py` per [`../spec.md`](../spec.md) [^aa-cache] [^sa-cache-shim].

## Bounded Read Manifest

- `src/py/vertebra/extensions/alchemy/cache.py` — `SQLAlchemyCacheBackend`, `create_cache_config`, `AsyncCacheManager`, and `ZeroCopyServiceMixin`.
- `src/py/vertebra/extensions/sqlalchemy/cache.py` — legacy re-export shim to delete via `git rm`.
- `src/py/vertebra/db/adapters/valkey/driver.py` & `redis/driver.py` — native `SyncValkeyDriver`/`SyncRedisDriver` and `AsyncValkeyDriver`/`AsyncRedisDriver`.

## Contracts & Edge Cases

- Implement `ValkeyCacheBackend(dogpile.cache.api.CacheBackend)`, `RedisCacheBackend(CacheBackend)`, and `MemoryCacheBackend(CacheBackend)` returning `dogpile.cache.api.NO_VALUE` on misses (`get_serialized`, `get_serialized_multi`, `set_serialized`, `set_serialized_multi`, `delete`, `delete_multi`) and register `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` via `dogpile.cache.register_backend`.
- Fix `create_cache_config` so URL strings map to registered `dogpile.cache` backend names + driver `arguments` instead of passing raw URLs as backend names; remove dead `shm_name`, unused `TYPE_CHECKING` blocks, and the process-local fake `shm://` scheme.
- Keep `SQLAlchemyCacheBackend` (`MutableMapping` compiled-cache + prefix-scoped `SCAN`+`UNLINK`), `AsyncCacheManager` (native async L2 caching), and `ZeroCopyServiceMixin`; delete `src/py/vertebra/extensions/sqlalchemy/cache.py`.

## Test Matrix

- Parametrized `make_region().configure('vertebra.memory' | 'vertebra.valkey' | 'vertebra.redis')` and Advanced Alchemy stock `CacheManager` hit/miss (`NO_VALUE`), TTL expiration, and `ZeroCopyServiceMixin` tests.
- Negative test confirming `shm://` raises `ImproperConfigurationError` and `vertebra.extensions.sqlalchemy.cache` no longer exists.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `vertebra.valkey`, `vertebra.redis`, and `vertebra.memory` work natively with `dogpile.cache` and Advanced Alchemy `1.12.0+` `CacheManager`.
- [ ] `src/py/vertebra/extensions/sqlalchemy/cache.py` and the fake `shm://` scheme are removed.
- [ ] Zero regressions in the chapter fast verification gate.

[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache backend, AsyncCacheManager, and ZeroCopyServiceMixin.
[^sa-cache-shim]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Re-export shim slated for deletion.
