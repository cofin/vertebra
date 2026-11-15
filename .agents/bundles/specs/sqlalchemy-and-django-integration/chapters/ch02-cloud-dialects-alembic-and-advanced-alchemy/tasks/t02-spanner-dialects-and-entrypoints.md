---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t02
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Implement Spanner/Spangres Dialects, Dialect Barrel, and 36 Entrypoints
description: Build dialects/spanner.py (GoogleSQL and Spangres), convert dialects/__init__.py barrel, delete dialects.py, and repoint all 36 sqlalchemy.dialects entrypoints in pyproject.toml.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t01
files:
- src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py
- src/py/vertebra/db/adapters/spanner/config.py
- src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py
- src/py/vertebra/extensions/sqlalchemy/__init__.py
- pyproject.toml
tests:
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/extensions/sqlalchemy/__init__.py
- src/py/vertebra/db/adapters/spanner/config.py
- src/py/vertebra/utils/module_loader.py
- pyproject.toml
estimated_loc: 235
tags:
- sqlalchemy
- spanner
- spangres
- dialects
- entrypoints
sources:
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Current Spanner and Spangres SQLAlchemy dialect classes
- id: spn-config
  resource: src/py/vertebra/db/adapters/spanner/config.py
  title: SpannerConfig connection configuration
- id: entry-points
  resource: pyproject.toml
  title: 36 sqlalchemy.dialects entry points across 9 engines
- id: sa-init
  resource: src/py/vertebra/extensions/sqlalchemy/__init__.py
  title: Public SQLAlchemy extension barrel
---

# Task `t02`: Implement Spanner/Spangres Dialects, Dialect Barrel, and 36 Entrypoints

## Objective & Scope Boundary

Implement `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` in `dialects/spanner.py`, set `SpannerConfig.requires_statement_autocommit = True`, convert `extensions/sqlalchemy/dialects/__init__.py` into a PEP 562 lazy barrel, update `extensions/sqlalchemy/__init__.py` dialect/type re-exports, delete monolithic `dialects.py`, and repoint all 36 `[project.entry-points."sqlalchemy.dialects"]` entries per [`../spec.md`](../spec.md) [^sa-dialects] [^spn-config] [^entry-points] [^sa-init].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — shared dialect mixins.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — legacy Spanner/Spangres dialect classes to migrate and delete (`git rm`).
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` — public package exports (`Sync<Engine>Dialect` / `Async<Engine>Dialect` and custom vector/JSON/geography types).
- `src/py/vertebra/db/adapters/spanner/config.py` — `SpannerConfig` class attributes.
- `src/py/vertebra/utils/module_loader.py` — `ensure_module` optional dependency guard.
- `pyproject.toml` — `[project.entry-points."sqlalchemy.dialects"]` section (`36` entries).

## Contracts & Edge Cases

- `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` subclass `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module('google.cloud.sqlalchemy_spanner', package='vertebra-core[sqlalchemy-gcp]', extra='sqlalchemy-gcp')`), overriding `import_dbapi`, `get_pool_class`, `create_connect_args`, and Spangres `$1` / double-quote / `TTL INTERVAL` adaptation.
- Wire `INTERLEAVE IN PARENT` table DDL options into the dialect DDL compiler (deleting unwired synthetic helpers `compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, `extract_interleave_options`) and set `requires_statement_autocommit: ClassVar[bool] = True` on `SpannerConfig`.
- Preserve all 36 public entry-point names (`<engine>.vertebra`, `<engine>.vtb`, `<engine>.vertebra_async`, `<engine>.vtb_async` for `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`), repointing each to `vertebra.extensions.sqlalchemy.dialects.<module>:Sync<Engine>Dialect` or `:Async<Engine>Dialect`.
- Use PEP 562 lazy `__getattr__` in `dialects/__init__.py` (and remove stale `dialects.py` references from `extensions/sqlalchemy/__init__.py`) so importing relational dialects never triggers optional `duckdb_engine` / `sqlalchemy_bigquery` / `sqlalchemy_spanner` imports, and delete `src/py/vertebra/extensions/sqlalchemy/dialects.py` completely with zero compatibility aliases.

## Test Matrix

- Parametrized GoogleSQL (`@name`, backticks, `STRING(MAX)`, `INTERLEAVE IN PARENT`) and Spangres (`$1`, double quotes, `TTL INTERVAL`) compilation and `MissingDependencyError` guard tests.
- Parametrized test asserting all 36 `[project.entry-points."sqlalchemy.dialects"]` keys resolve to the exact split `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes and zero `Vertebra*` / `*Vtb*` dialect names exist.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] `SyncSpannerDialect`, `AsyncSpannerDialect`, `SyncSpangresDialect`, and `AsyncSpangresDialect` pass compilation and guard checks.
- [ ] `SpannerConfig.requires_statement_autocommit` is `True`.
- [ ] Monolithic `src/py/vertebra/extensions/sqlalchemy/dialects.py` is deleted.
- [ ] All 36 SQLAlchemy dialect entry points in `pyproject.toml` resolve to `Sync<Engine>Dialect` / `Async<Engine>Dialect`.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current Spanner and Spangres SQLAlchemy dialect classes.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig connection configuration.
[^entry-points]: `pyproject.toml` — 36 sqlalchemy.dialects entry points across 9 engines.
[^sa-init]: `src/py/vertebra/extensions/sqlalchemy/__init__.py` — Public SQLAlchemy extension barrel.
