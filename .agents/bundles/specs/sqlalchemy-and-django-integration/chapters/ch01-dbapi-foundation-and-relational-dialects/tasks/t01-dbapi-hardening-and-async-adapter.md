---
type: Task
id: sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t01
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter
description: Bump SQLAlchemy/Alembic/dogpile/Advanced Alchemy/Django extras, harden PEP 249 cursor/connection semantics, add BaseDatabaseConfig metadata, and extract greenlet async_adapter.py + engine.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on: []
files:
- pyproject.toml
- src/py/vertebra/db/dbapi/_sync.py
- src/py/vertebra/db/dbapi/_async.py
- src/py/vertebra/db/driver/_common.py
- src/py/vertebra/extensions/sqlalchemy/async_adapter.py
- src/py/vertebra/extensions/sqlalchemy/engine.py
tests:
- src/py/tests/unit/db/test_dbapi_standalone.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
- pyproject.toml
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/db/registry.py
- src/py/vertebra/db/driver/_common.py
- src/py/vertebra/extensions/sqlalchemy/alembic.py
estimated_loc: 220
tags:
- sqlalchemy
- dbapi
- dependencies
- async
sources:
- id: pyproject-deps
  resource: pyproject.toml
  title: Optional alchemy, django, and dialect extras
- id: sa-dbapi
  resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
  title: Standalone PEP 249 connection with _sqlite_conn, default URL, and unmapped native errors pending hardening
- id: db-common
  resource: src/py/vertebra/db/driver/_common.py
  title: BaseDatabaseConfig base contract for all database configs
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation
---

# Task `t01`: Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter

## Objective & Scope Boundary

Update `pyproject.toml` extras, harden `vertebra.db.dbapi` (`_sync.py` and `_async.py`, extracted in `broker-pool-sharing:ch04` and archived at `28415b3f`), add engine-neutral transaction/migration metadata on `BaseDatabaseConfig`, harden `extensions/sqlalchemy/async_adapter.py`, and extract `SyncEngineAdapter` + `attach_serverless_disconnect_guard` into `extensions/sqlalchemy/engine.py` per [`../spec.md` §2](../spec.md) [^pyproject-deps] [^sa-dbapi] [^db-common] [^sa-dialects]. Out of scope: per-engine dialect modules (`t03`–`t04`, `ch02`).

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies]` (`alchemy`, `django`, optional dialect extras) and `[dependency-groups].dev`.
- `src/py/vertebra/db/dbapi/{_sync,_async}.py` — `_sqlite_conn`, `url='sqlite+vtb:///:memory:'`, `database=` compatibility parameter, and native exception translation boundary.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncEngineAdapter`, `attach_serverless_disconnect_guard`, and dialect imports.
- `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — greenlet `AsyncAdapt_dbapi_connection` wrapper extracted in `broker-pool-sharing:ch04`.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver factory seam.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig` and canonical `DatabaseError` hierarchy.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — direct migration DSN checks slated to read `effective_direct_dsn`.

## Contracts & Edge Cases

- Add `dogpile.cache>=1.3.3` to `[project.optional-dependencies].alchemy` and `[dependency-groups].dev`; raise `django>=6.0` on `django`; add optional `sqlalchemy-duckdb`, `sqlalchemy-gcp`, and `django-spanner` extras (never adding an `[all]` extra).
- Delete `_sqlite_conn`, stdlib `sqlite3` imports, `url='sqlite+vtb:///:memory:'`, and `database=` from `db/dbapi/{_sync,_async}.py`; raise `InterfaceError` on empty/missing URLs; map native `vertebra._vertebra` exceptions (`DatabaseError`) to PEP 249 `dbapi.*Error` subclasses; execute SQLite solely via `SyncSQLiteDriver`/`AsyncSQLiteDriver`; forbid importing SQLAlchemy, Django, framework, or engine adapter modules inside `db/dbapi/`.
- Declare `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str` on `BaseDatabaseConfig` so `db/dbapi` and Alembic never inspect concrete config `isinstance` checks.
- Harden `extensions/sqlalchemy/async_adapter.py` (`await_only` greenlet boundary, cancellation cleanup, PEP 249 error propagation) and extract `SyncEngineAdapter` and `attach_serverless_disconnect_guard` into `extensions/sqlalchemy/engine.py`.

## Test Matrix

- `@pytest.mark.parametrize` verifies empty/missing URLs raise `InterfaceError`, single-connection SQLite `:memory:` round-trips zero stdlib `sqlite3` handles, native `vertebra._vertebra` exceptions map to PEP 249 `dbapi.*Error`, `requires_statement_autocommit` rejects `autocommit=False` with `NotSupportedError`, and `vertebra.db.dbapi` loads zero upward packages.
- Async greenlet cancellation and `DatabaseError` -> PEP 249 error propagation pass on `async_adapter.py`, plus `test_optional_driver_wheels.py`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

## Acceptance Criteria

- [ ] `dogpile.cache>=1.3.3` and `django>=6.0` are declared in `pyproject.toml`.
- [ ] `SyncDBAPIConnection` and `AsyncDBAPIConnection` reject empty/missing URLs with `InterfaceError` and contain zero stdlib `sqlite3` connections.
- [ ] Zero `isinstance` checks against concrete engine config classes remain in `db/dbapi/` or `extensions/sqlalchemy/`.
- [ ] `async_adapter.py` and `engine.py` pass strict `mypy` and unit cancellation/disconnect tests.
- [ ] Zero regressions in the fast verification gate.

[^pyproject-deps]: `pyproject.toml` — Optional alchemy, django, and dialect extras.
[^sa-dbapi]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection` — Standalone PEP 249 connection with `_sqlite_conn`, default URL, and unmapped native errors pending hardening.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig base contract for all database configs.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation.
