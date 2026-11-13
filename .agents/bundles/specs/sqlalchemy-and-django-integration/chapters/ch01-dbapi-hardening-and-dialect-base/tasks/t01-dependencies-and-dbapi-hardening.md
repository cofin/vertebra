---
type: Task
id: "sqlalchemy-and-django-integration:ch01-dbapi-hardening-and-dialect-base:t01"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-hardening-and-dialect-base
title: "Declare dogpile.cache & Dialect Extras and Harden Shared vertebra.db.dbapi"
description: "Add dogpile.cache>=1.3.3, django>=6.0, and dialect extras in pyproject.toml and remove stdlib sqlite3 and implicit URL defaults from vertebra.db.dbapi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - pyproject.toml
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/vertebra/db/dbapi/_async.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/registry.py
  - src/py/vertebra/db/driver/_common.py
estimated_loc: 210
tags:
  - sqlalchemy
  - dbapi
  - dependencies
sources:
  - id: pyproject-deps
    resource: pyproject.toml
    title: "Optional alchemy, django, and dialect extras"
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: "Current PEP 249 connection with _sqlite_conn and default URL"
---

# Task `t01`: Declare dogpile.cache & Dialect Extras and Harden Shared vertebra.db.dbapi

## Objective & Scope Boundary

Update `pyproject.toml` extras and harden `vertebra.db.dbapi` (`_sync.py`, `_async.py` extracted by `broker-pool-sharing:ch07`) per [`../spec.md`](../spec.md) [^pyproject-deps] [^sa-dbapi]. Out of scope: recreating `vertebra.db.dbapi` from scratch (owned by `broker-pool-sharing`).

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies]` (`alchemy`, `django`, optional dialect extras) and `[dependency-groups].dev`.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — legacy `_sqlite_conn` and `url='sqlite+vtb:///:memory:'` defects slated for elimination.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver factory seam.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig` and canonical `DatabaseError` hierarchy.

## Contracts & Edge Cases

- Add `dogpile.cache>=1.3.3` to `[project.optional-dependencies].alchemy` and `[dependency-groups].dev`; raise `django>=6.0` on `django`; add optional `sqlalchemy-duckdb`, `sqlalchemy-gcp`, and `django-spanner` extras.
- In `db/dbapi/_sync.py` and `db/dbapi/_async.py`, delete `_sqlite_conn`, stdlib `sqlite3` imports, `url='sqlite+vtb:///:memory:'`, and the `database=` override.
- `SyncDBAPIModule.connect(url)` / `AsyncDBAPIModule.connect(url)` and `SyncDBAPIConnection(url)` / `AsyncDBAPIConnection(url)` raise `InterfaceError` on empty or missing `url`; SQLite URLs execute solely through the registry `SyncSQLiteDriver` / `AsyncSQLiteDriver`.
- `src/py/vertebra/db/dbapi/` imports zero `vertebra.extensions`, `vertebra.framework`, `vertebra.agent`, SQLAlchemy, Django, or engine adapter modules.

## Test Matrix

- Parametrized `@pytest.mark.parametrize` checks verifying missing/empty URLs raise `InterfaceError` on sync and async `connect()` and direct connection constructors.
- SQLite `:memory:` round-trip writes and reads through a single `SyncDBAPIConnection` / `AsyncDBAPIConnection` backed by the native SQLite driver with zero `sqlite3` module attribute.
- Import-graph assertion confirming `vertebra.db.dbapi` loads zero SQLAlchemy, Django, or `vertebra.db.adapters.*` modules.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py
```

## Acceptance Criteria

- [ ] `dogpile.cache>=1.3.3` and `django>=6.0` are declared in `pyproject.toml`.
- [ ] `SyncDBAPIConnection` and `AsyncDBAPIConnection` reject empty/missing URLs with `InterfaceError` and contain zero stdlib `sqlite3` connections.
- [ ] Zero regressions in the chapter fast verification gate.

[^pyproject-deps]: `pyproject.toml` — Optional alchemy, django, and dialect extras.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — Current PEP 249 connection with _sqlite_conn and default URL.
