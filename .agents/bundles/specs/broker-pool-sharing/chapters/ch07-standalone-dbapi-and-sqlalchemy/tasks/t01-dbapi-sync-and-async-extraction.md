---
type: Task
id: "broker-pool-sharing:ch07:t01"
flow_id: broker-pool-sharing
chapter_id: ch07-standalone-dbapi-and-sqlalchemy
title: "Extract Framework-Neutral Sync and Async PEP 249 Package (vertebra.db.dbapi)"
description: "Create vertebra.db.dbapi with PEP 249 errors, SyncDBAPI*, AsyncDBAPI*, and comment-aware is_row_returning_sql."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/vertebra/db/dbapi/_async.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/registry.py
estimated_loc: 245
tags:
  - dbapi
  - pep249
  - db
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: "SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes"
  - id: db-config
    resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
    title: "BaseDatabaseConfig with dialect ClassVar and pool_key"
  - id: py-db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: "DatabaseRegistry URL parsing and driver resolution"
---

# Task `t01`: Extract Framework-Neutral Sync and Async PEP 249 Package (vertebra.db.dbapi)

## Objective & Scope Boundary

Create `vertebra.db.dbapi` (`__init__.py` with PEP 562 lazy exports, `_sync.py`, `_async.py`) per [`../spec.md` §2](../spec.md) [^sa-dialects] [^db-config] [^py-db-registry]. Out of scope: SQLAlchemy `AsyncAdapt` wrapper (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Embedded PEP 249 errors, cursors, connections, modules, and `_is_row_returning_sql`.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.dialect` and driver contracts.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver lookup.

## Contracts & Edge Cases

- Export PEP 249 exceptions, `apilevel`, `threadsafety`, `paramstyle`, `is_row_returning_sql`, `connect`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule`.
- Restrict imports to stdlib, `vertebra.db.registry`, `vertebra.db.driver`, `vertebra.typing`, and `vertebra.utils`; inspect `type(config).dialect` instead of adapter `isinstance`.
- **Edge Case**: Leading `--` / `/* */` SQL comments and string literals containing `'returning'` classify accurately in `is_row_returning_sql`.
- **Edge Case**: `__init__.py` uses `lazy_exports` (`PY-06`).

## Test Matrix

- Subprocess import-boundary test verifies `import vertebra.db.dbapi` loads zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` modules.
- `@pytest.mark.parametrize` covers `is_row_returning_sql`, sync/async cursor `description`/`rowcount`, and `autocommit` on `bigquery`/`spanner`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q && uv run --no-sync ruff check src/py/vertebra/db/dbapi && uv run --no-sync mypy src/py/vertebra/db/dbapi
```

## Acceptance Criteria

- [ ] `vertebra.db.dbapi` satisfies PEP 249 and async DB-API contracts with zero upward imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
