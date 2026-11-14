---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t04
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Wire Advanced Alchemy Engine Configs, Lifespan Session Hooks, and Repository Tests
description: Integrate Vertebra Sync/Async SQLAlchemy dialects with SQLAlchemySyncConfig, SQLAlchemyAsyncConfig, and SQLAlchemyAsyncRepositoryService.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t03
files:
- src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py
- src/py/vertebra/extensions/alchemy/plugin.py
- src/py/vertebra/extensions/alchemy/storage.py
tests:
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
- src/py/vertebra/extensions/sqlalchemy/types.py
- src/py/vertebra/extensions/alchemy/plugin.py
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/db/registry.py
estimated_loc: 195
tags:
- advanced-alchemy
- alembic
- autogenerate
sources:
- id: aa-plugin
  resource: src/py/vertebra/extensions/alchemy/plugin.py
  title: normalize_alchemy_url and SQLAlchemySyncConfig / SQLAlchemyAsyncConfig factories
- id: aa-storage
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Advanced Alchemy FileObject ObjectStoreBackend
---

# Task `t04`: Wire Advanced Alchemy Engine Configs, Lifespan Session Hooks, and Repository Tests

## Objective & Scope Boundary

Implement Alembic autogenerate comparator hooks in `alembic/hooks.py` and wire `extensions/alchemy/{plugin,storage}.py` onto `BaseDatabaseConfig.url_schemes` and Advanced Alchemy `1.12.0+` per [`../spec.md`](../spec.md) [^aa-plugin] [^aa-storage].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py` & `types.py` — vector/JSON types and Spanner/BigQuery table options.
- `src/py/vertebra/extensions/alchemy/plugin.py` — `normalize_alchemy_url`, `create_sync_alchemy_config`, and `create_async_alchemy_config`.
- `src/py/vertebra/db/registry.py` — registered `BaseDatabaseConfig.url_schemes` lookup.

## Contracts & Edge Cases

- Register `@comparators.dispatch_for` hooks in `alembic/hooks.py` comparing `PgVector`/`OracleVector`/`SpannerVector` dimensions, `MsgspecJSON`, Spanner `INTERLEAVE IN PARENT` / `ROW DELETION POLICY`, and BigQuery `PARTITION BY` / `CLUSTER BY` without false-positive autogenerate diffs.
- Update `normalize_alchemy_url` in `extensions/alchemy/plugin.py` to resolve schemes dynamically from `BaseDatabaseConfig.url_schemes` into `<canonical_engine>+{vtb,vtb_async}://...` across all registered engines.
- Verify `SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`, `CacheManager` / `AsyncCacheManager`, and `ObjectStoreBackend` lifecycle cleanly under Advanced Alchemy `1.12.0+`.

## Test Matrix

- Alembic `autogenerate` diff tests on PostgreSQL and Spanner schemas (zero false diffs when unchanged; emits `CreateVectorIndexOp` / `InterleaveTableOp` when modified).
- Parametrized `normalize_alchemy_url` tests across all registered `BaseDatabaseConfig.url_schemes` and `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig` wiring tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `alembic/hooks.py` eliminates false-positive vector/interleave autogenerate diffs.
- [ ] `normalize_alchemy_url` dynamically reads `BaseDatabaseConfig.url_schemes` with zero hardcoded vendor branches.
- [ ] Zero regressions in the fast verification gate.

[^aa-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py` — normalize_alchemy_url and SQLAlchemySyncConfig / SQLAlchemyAsyncConfig factories.
[^aa-storage]: `src/py/vertebra/extensions/alchemy/storage.py` — Advanced Alchemy FileObject ObjectStoreBackend.
