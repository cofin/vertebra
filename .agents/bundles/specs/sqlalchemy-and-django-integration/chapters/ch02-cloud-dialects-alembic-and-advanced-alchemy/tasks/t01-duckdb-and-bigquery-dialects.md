---
type: Task
id: sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t01
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-cloud-dialects-alembic-and-advanced-alchemy
title: Implement Native DuckDB and BigQuery Sync/Async SQLAlchemy Dialects
description: Build dialects/duckdb.py and dialects/bigquery.py with STRUCT/ARRAY/JSON reflection and named @param binds.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t04
files:
- src/py/vertebra/extensions/sqlalchemy/dialects/duckdb.py
- src/py/vertebra/extensions/sqlalchemy/dialects/bigquery.py
- src/py/vertebra/db/adapters/bigquery/config.py
tests:
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/db/adapters/duckdb/driver.py
- src/py/vertebra/db/adapters/bigquery/config.py
- src/py/vertebra/utils/module_loader.py
estimated_loc: 225
tags:
- sqlalchemy
- duckdb
- bigquery
- dialects
sources:
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Current DuckDB and BigQuery SQLAlchemy dialect implementations
- id: bq-config
  resource: src/py/vertebra/db/adapters/bigquery/config.py
  title: BigQueryConfig connection configuration
---

# Task `t01`: Implement Native DuckDB and BigQuery Sync/Async SQLAlchemy Dialects

## Objective & Scope Boundary

Implement `SyncDuckDBDialect` / `AsyncDuckDBDialect` and `SyncBigQueryDialect` / `AsyncBigQueryDialect` over upstream dialect bases and set `BigQueryConfig.requires_statement_autocommit = True` per [`../spec.md`](../spec.md) [^sa-dialects] [^bq-config].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — shared dialect mixins.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — existing DuckDB and BigQuery dialect classes.
- `src/py/vertebra/db/adapters/bigquery/config.py` — `BigQueryConfig` class attributes.
- `src/py/vertebra/utils/module_loader.py` — `ensure_module` optional dependency guard.

## Contracts & Edge Cases

- `SyncDuckDBDialect` / `AsyncDuckDBDialect` subclass `duckdb_engine.Dialect` (guarded via `ensure_module('duckdb_engine', package='vertebra-core[sqlalchemy-duckdb]', extra='sqlalchemy-duckdb')` -> `MissingDependencyError`), overriding only `import_dbapi`, `get_pool_class`, and `create_connect_args`.
- `SyncBigQueryDialect` / `AsyncBigQueryDialect` subclass `sqlalchemy_bigquery.BigQueryDialect` (guarded via `ensure_module('sqlalchemy_bigquery', package='vertebra-core[sqlalchemy-gcp]', extra='sqlalchemy-gcp')`), preserving backtick quoting, `STRING`/`STRUCT`/`ARRAY`/`GEOGRAPHY` types, and `@name` parameters over `vertebra.db.dbapi`.
- Set `requires_statement_autocommit: ClassVar[bool] = True` on `BigQueryConfig`.

## Test Matrix

- Parametrized tests verifying `MissingDependencyError` when optional upstream dialect packages are absent and verifying backtick/`STRING`/`@name` compilation and `create_connect_args` when present.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] `SyncDuckDBDialect`, `AsyncDuckDBDialect`, `SyncBigQueryDialect`, and `AsyncBigQueryDialect` subclass upstream bases cleanly via `ensure_module`.
- [ ] `BigQueryConfig.requires_statement_autocommit` is `True`.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current DuckDB and BigQuery SQLAlchemy dialect implementations.
[^bq-config]: `src/py/vertebra/db/adapters/bigquery/config.py` — BigQueryConfig connection configuration.
