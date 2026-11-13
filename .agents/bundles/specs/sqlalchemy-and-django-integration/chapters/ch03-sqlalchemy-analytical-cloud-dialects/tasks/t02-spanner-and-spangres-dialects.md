---
type: Task
id: "sqlalchemy-and-django-integration:ch03-sqlalchemy-analytical-cloud-dialects:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-sqlalchemy-analytical-cloud-dialects
title: "Implement SyncSpannerDialect / AsyncSpannerDialect and SyncSpangresDialect / AsyncSpangresDialect"
description: "Subclass google.cloud.sqlalchemy_spanner.SpannerDialect for GoogleSQL and Spangres over vertebra.db.dbapi and mark SpannerConfig statement autocommit."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch02-sqlalchemy-core-relational-dialects:t01"
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py
  - src/py/vertebra/db/adapters/spanner/config.py
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/utils/module_loader.py
estimated_loc: 235
tags:
  - sqlalchemy
  - spanner
  - spangres
  - dialects
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Current Spanner and Spangres SQLAlchemy dialect classes"
  - id: spn-config
    resource: src/py/vertebra/db/adapters/spanner/config.py
    title: "SpannerConfig connection configuration"
---

# Task `t02`: Implement SyncSpannerDialect / AsyncSpannerDialect and SyncSpangresDialect / AsyncSpangresDialect

## Objective & Scope Boundary

Implement `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` in `dialects/spanner.py` and set `SpannerConfig.requires_statement_autocommit = True` per [`../spec.md`](../spec.md) [^sa-dialects] [^spn-config].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — shared dialect mixins.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Spanner and Spangres dialect rules.
- `src/py/vertebra/db/adapters/spanner/config.py` — `SpannerConfig` class attributes.

## Contracts & Edge Cases

- `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect` subclass `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module('google.cloud.sqlalchemy_spanner', package='vertebra-core[sqlalchemy-gcp]', extra='sqlalchemy-gcp')`), overriding `import_dbapi`, `get_pool_class`, `create_connect_args`, and Spangres `$1` / double-quote / `TTL INTERVAL` adaptation.
- Wire `INTERLEAVE IN PARENT` table DDL options into the dialect DDL compiler (deleting unwired synthetic helpers `compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, `extract_interleave_options`).
- Set `requires_statement_autocommit: ClassVar[bool] = True` on `SpannerConfig`.

## Test Matrix

- Parametrized GoogleSQL (`@name`, backticks, `STRING(MAX)`, `INTERLEAVE IN PARENT`) and Spangres (`$1`, double quotes, `TTL INTERVAL`) compilation and `MissingDependencyError` guard tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] `SyncSpannerDialect`, `AsyncSpannerDialect`, `SyncSpangresDialect`, and `AsyncSpangresDialect` pass compilation and guard checks.
- [ ] `SpannerConfig.requires_statement_autocommit` is `True`.
- [ ] Zero regressions in the chapter fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current Spanner and Spangres SQLAlchemy dialect classes.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig connection configuration.
