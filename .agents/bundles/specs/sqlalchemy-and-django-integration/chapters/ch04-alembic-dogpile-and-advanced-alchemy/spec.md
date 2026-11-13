---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch04-alembic-dogpile-and-advanced-alchemy
title: "Chapter 04: Modular Alembic DDL, dogpile.cache Region Backends & Advanced Alchemy 1.12+"
description: "Split Alembic DefaultImpl and autogenerate hooks, register dogpile.cache backends, delete the sqlalchemy/cache.py shim, and wire Advanced Alchemy 1.12.0+."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - advanced-alchemy
  - alembic
  - dogpile-cache
  - sqlalchemy
sources:
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: "Monolithic Alembic module"
  - id: aa-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py
    title: "Advanced Alchemy cache module"
  - id: aa-plugin
    resource: src/py/vertebra/extensions/alchemy/plugin.py
    title: "Advanced Alchemy config and URL helpers"
  - id: sa-cache-shim
    resource: src/py/vertebra/extensions/sqlalchemy/cache.py
    title: "Legacy SQLAlchemy cache re-export shim"
---

# Chapter 04: Modular Alembic DDL, dogpile.cache Region Backends & Advanced Alchemy 1.12+

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `alembic.py` checks `isinstance(cfg, (PostgreSQLConfig, MySQLConfig))`, `alchemy/cache.py` passes raw URLs as `dogpile` backend names and exposes a process-local `shm://` scheme, `sqlalchemy/cache.py` is a duplicate re-export shim, and `normalize_alchemy_url` hardcodes only GCP schemes [^sa-alembic] [^aa-cache] [^aa-plugin] [^sa-cache-shim].
- **Target Architecture**: Modular `extensions/sqlalchemy/alembic/{__init__,impl,ops,hooks}.py` provides `DefaultImpl` + autogenerate hooks; `extensions/alchemy/cache.py` registers `dogpile.cache>=1.3.3` backends (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`); `sqlalchemy/cache.py` is deleted; `normalize_alchemy_url` reads `BaseDatabaseConfig.url_schemes`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter splits `alembic.py` into a modular Alembic DDL and autogenerate package, integrates `dogpile.cache>=1.3.3` region backends for Advanced Alchemy `1.12.0+` (`SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`, `CacheManager`, `AsyncCacheManager`), removes `sqlalchemy/cache.py`, and generalizes `normalize_alchemy_url` [^aa-cache] [^aa-plugin].

## 2. Requirements, Concrete Contracts & Architecture

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

Advanced Alchemy `1.12.0+` caching and Alembic migration guides are updated in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R8` Alembic `DefaultImpl` & ops | `t01` | `extensions/sqlalchemy/alembic/{__init__,impl,ops}.py` | `pytest test_gcp_adapters_and_sqlalchemy.py` |
| `R9` `dogpile.cache` backends & shim removal | `t02` | `extensions/alchemy/{cache,__init__}.py` | `pytest test_alchemy_cache_backend.py` |
| `R8`, `R9` Autogenerate hooks & AA `1.12+` | `t03` | `alembic/hooks.py`, `extensions/alchemy/{plugin,storage}.py` | `pytest test_data_storage_integrations.py` |

## 5. Implementation Plan

- [ ] [t01-alembic-impl-and-ops.md](tasks/t01-alembic-impl-and-ops.md) — Split `extensions/sqlalchemy/alembic/{__init__,impl,ops}.py` and delete monolithic `alembic.py`.
- [ ] [t02-dogpile-cache-region-backends.md](tasks/t02-dogpile-cache-region-backends.md) — Implement `dogpile.cache` region backends (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`) and delete `sqlalchemy/cache.py`.
- [ ] [t03-advanced-alchemy-config-and-hooks.md](tasks/t03-advanced-alchemy-config-and-hooks.md) — Implement `alembic/hooks.py` autogenerate comparators and wire `normalize_alchemy_url` + Advanced Alchemy `1.12.0+`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch04-alembic-dogpile-and-advanced-alchemy` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic module.
[^aa-cache]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache module.
[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — Advanced Alchemy config and URL helpers.
[^sa-cache-shim]: `src/py/vertebra/extensions/sqlalchemy/cache.py` — Legacy SQLAlchemy cache re-export shim.
