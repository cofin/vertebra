---
type: Task
id: sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t01
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter
description: Bump SQLAlchemy/Alembic/dogpile/Advanced Alchemy extras, harden PEP 249 cursor/connection semantics, and build greenlet-bridged AsyncConnection/AsyncCursor.
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
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
  title: Current PEP 249 connection with _sqlite_conn and default URL
- id: db-common
  resource: src/py/vertebra/db/driver/_common.py
  title: BaseDatabaseConfig base contract for all database configs
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation
---

# Task `t01`: Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter

## Objective & Scope Boundary

Update `pyproject.toml` extras and harden `vertebra.db.dbapi` (`_sync.py`, `_async.py` extracted by `broker-pool-sharing:ch04`) per [`../spec.md`](../spec.md) [^pyproject-deps] [^sa-dbapi]. Out of scope: recreating `vertebra.db.dbapi` from scratch (owned by `broker-pool-sharing`).

Add engine-neutral transaction/migration metadata on `BaseDatabaseConfig` and harden `extensions/sqlalchemy/{async_adapter,engine}.py` per [`../spec.md`](../spec.md) [^db-common] [^sa-dialects]. Out of scope: per-engine dialect modules (`ch02`–`ch03`).

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies]` (`alchemy`, `django`, optional dialect extras) and `[dependency-groups].dev`.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — legacy `_sqlite_conn` and `url='sqlite+vtb:///:memory:'` defects slated for elimination.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver factory seam.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig` and canonical `DatabaseError` hierarchy.

- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig` properties and class attributes.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncEngineAdapter`, `attach_serverless_disconnect_guard`, and `AsyncAdapt_dbapi_connection` wiring.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — direct migration DSN resolution currently checking `PostgreSQLConfig` / `MySQLConfig`.

## Contracts & Edge Cases

- Add `dogpile.cache>=1.3.3` to `[project.optional-dependencies].alchemy` and `[dependency-groups].dev`; raise `django>=6.0` on `django`; add optional `sqlalchemy-duckdb`, `sqlalchemy-gcp`, and `django-spanner` extras.
- In `db/dbapi/_sync.py` and `db/dbapi/_async.py`, delete `_sqlite_conn`, stdlib `sqlite3` imports, `url='sqlite+vtb:///:memory:'`, and the `database=` override.
- `SyncDBAPIModule.connect(url)` / `AsyncDBAPIModule.connect(url)` and `SyncDBAPIConnection(url)` / `AsyncDBAPIConnection(url)` raise `InterfaceError` on empty or missing `url`; SQLite URLs execute solely through the registry `SyncSQLiteDriver` / `AsyncSQLiteDriver`.
- `src/py/vertebra/db/dbapi/` imports zero `vertebra.extensions`, `vertebra.framework`, `vertebra.agent`, SQLAlchemy, Django, or engine adapter modules.

- Declare `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str` on `BaseDatabaseConfig` so `vertebra.db.dbapi` and Alembic never run `isinstance(..., (BigQueryConfig, SpannerConfig, SQLiteConfig, PostgreSQLConfig, MySQLConfig))`.
- Harden `extensions/sqlalchemy/async_adapter.py` (`AdaptedAsyncConnection`, `AdaptedAsyncCursor`, `await_only` greenlet boundary, task cancellation cleanup, PEP 249 exception propagation) without leaking SQLAlchemy imports into `vertebra.db.dbapi`.
- Extract `SyncEngineAdapter` and `attach_serverless_disconnect_guard` into `extensions/sqlalchemy/engine.py`.

## Test Matrix

- Parametrized `@pytest.mark.parametrize` checks verifying missing/empty URLs raise `InterfaceError` on sync and async `connect()` and direct connection constructors.
- SQLite `:memory:` round-trip writes and reads through a single `SyncDBAPIConnection` / `AsyncDBAPIConnection` backed by the native SQLite driver with zero `sqlite3` module attribute.
- Import-graph assertion confirming `vertebra.db.dbapi` loads zero SQLAlchemy, Django, or `vertebra.db.adapters.*` modules.

- Parametrized tests verifying `requires_statement_autocommit` rejects `autocommit=False` with `NotSupportedError` without importing concrete adapter config classes.
- Async greenlet cancellation and `DatabaseError` -> PEP 249 error propagation tests on `AdaptedAsyncConnection` and `AdaptedAsyncCursor`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py
```

## Acceptance Criteria

- [ ] `dogpile.cache>=1.3.3` and `django>=6.0` are declared in `pyproject.toml`.
- [ ] `SyncDBAPIConnection` and `AsyncDBAPIConnection` reject empty/missing URLs with `InterfaceError` and contain zero stdlib `sqlite3` connections.
- [ ] Zero `isinstance` checks against concrete engine config classes remain in `db/dbapi/` or `extensions/sqlalchemy/`.
- [ ] `async_adapter.py` and `engine.py` pass strict `mypy` and unit cancellation/disconnect tests.
- [ ] Zero regressions in the fast verification gate.

[^pyproject-deps]: `pyproject.toml` — Optional alchemy, django, and dialect extras.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — Current PEP 249 connection with _sqlite_conn and default URL.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig base contract for all database configs.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation.
