---
type: Task
id: "broker-pool-sharing:ch07:t02"
flow_id: broker-pool-sharing
chapter_id: ch07-standalone-dbapi-and-sqlalchemy
title: "SQLAlchemy Async Adapter Isolation and Clean-Break Dialect Rewire"
description: "Add extensions/sqlalchemy/async_adapter.py and remove embedded DB-API classes and adapter isinstance checks from dialects.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch07:t01"
files:
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
estimated_loc: 210
tags:
  - sqlalchemy
  - dbapi
  - dialects
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: "SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes"
  - id: db-config
    resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
    title: "BaseDatabaseConfig with dialect ClassVar and pool_key"
---

# Task `t02`: SQLAlchemy Async Adapter Isolation and Clean-Break Dialect Rewire

## Objective & Scope Boundary

Isolate `AsyncAdapt_dbapi_connection` wrapping in `extensions/sqlalchemy/async_adapter.py` and rewire `dialects.py` and `alembic.py` onto `vertebra.db.dbapi` with zero compatibility re-exports per [`../spec.md` §2](../spec.md) [^sa-dialects] [^db-config].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Dialect `import_dbapi` hooks and embedded DB-API classes to remove.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Alembic import sites.
- `src/py/vertebra/db/dbapi/__init__.py` — Extracted DB-API exports from `t01`.
- `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Dialect and DB-API unit tests.

## Contracts & Edge Cases

- Delete embedded DB-API exceptions, cursors, connections, and modules from `dialects.py` (`PY-07`).
- Keep all four dialect entry points per engine (`<engine>+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) working identically.
- **Edge Case**: `RETURNING` capability checks consult dialect/capability attributes rather than concrete config `isinstance`.

## Test Matrix

- `@pytest.mark.parametrize` verifies sync and async SQLAlchemy `import_dbapi()` across PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and Spanner.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] `dialects.py` contains only SQLAlchemy dialect definitions with zero embedded DB-API classes or re-export shims.
- [ ] Zero regressions in the chapter fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
