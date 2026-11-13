---
type: Task
id: "sqlalchemy-and-django-integration:ch04-alembic-dogpile-and-advanced-alchemy:t01"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch04-alembic-dogpile-and-advanced-alchemy
title: "Split extensions/sqlalchemy/alembic/ (impl.py, ops.py, __init__.py) and Delete alembic.py"
description: "Replace monolithic alembic.py with per-dialect DefaultImpl classes, batched Spanner DDL, custom MigrateOperation directives, and engine-neutral URL resolution."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch03-sqlalchemy-analytical-cloud-dialects:t03"
files:
  - src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
  - src/py/vertebra/extensions/sqlalchemy/alembic/ops.py
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py
estimated_loc: 235
tags:
  - alembic
  - sqlalchemy
  - ddl
sources:
  - id: sa-alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: "Monolithic Alembic DefaultImpl and vector/view DDL operations"
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: "BaseDatabaseConfig effective_direct_dsn metadata"
---

# Task `t01`: Split extensions/sqlalchemy/alembic/ (impl.py, ops.py, __init__.py) and Delete alembic.py

## Objective & Scope Boundary

Delete monolithic `extensions/sqlalchemy/alembic.py` and split into `alembic/{__init__,impl,ops}.py` per [`../spec.md`](../spec.md) [^sa-alembic] [^db-common]. Out of scope: autogenerate comparator hooks (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — existing `DefaultImpl` subclasses and DDL ops to refactor and remove.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.effective_direct_dsn`.
- `src/py/vertebra/extensions/sqlalchemy/dialects/spanner.py` — `update_ddl` batching seam.

## Contracts & Edge Cases

- Implement `DefaultImpl` subclasses registered via `ImplMeta` (`PostgreSQLAlembicImpl`, `MySQLAlembicImpl`, `SQLiteAlembicImpl`, `OracleAlembicImpl`, `MSSQLAlembicImpl`, `DuckDBAlembicImpl`, `BigQueryAlembicImpl` with `transactional_ddl = False`, `SpannerAlembicImpl` and `SpangresAlembicImpl` with `transactional_ddl = False` and batched `update_ddl`).
- Resolve direct migration URLs from `BaseDatabaseConfig.effective_direct_dsn` with zero `isinstance(..., (PostgreSQLConfig, MySQLConfig))` checks.
- Implement Alembic `MigrateOperation` + `@Operations.implementation_for` directives (`CreateVectorIndexOp`, `DropVectorIndexOp`, `CreateViewOp`, `DropViewOp`, `InterleaveTableOp`, `RowDeletionPolicyOp`).

## Test Matrix

- Parametrized DDL rendering, `ImplMeta` dialect lookup, non-transactional Spanner `update_ddl` batching, and `effective_direct_dsn` URL resolution tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/alembic src/py/vertebra/extensions/alchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] Monolithic `extensions/sqlalchemy/alembic.py` is deleted and replaced by `alembic/{__init__,impl,ops}.py`.
- [ ] Zero concrete `PostgreSQLConfig` or `MySQLConfig` `isinstance` checks remain in Alembic integration.
- [ ] Zero regressions in the chapter fast verification gate.

[^sa-alembic]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Monolithic Alembic DefaultImpl and vector/view DDL operations.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig effective_direct_dsn metadata.
