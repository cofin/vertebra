---
type: Task
id: "sqlalchemy-and-django-integration:ch01-dbapi-hardening-and-dialect-base:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-hardening-and-dialect-base
title: "Add Engine-Neutral Config Metadata and Harden async_adapter.py & engine.py"
description: "Replace concrete config isinstance checks with BaseDatabaseConfig metadata and harden SQLAlchemy greenlet async_adapter.py and engine.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  - src/py/vertebra/extensions/sqlalchemy/engine.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
estimated_loc: 195
tags:
  - sqlalchemy
  - dbapi
  - async
sources:
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: "BaseDatabaseConfig base contract for all database configs"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation"
---

# Task `t02`: Add Engine-Neutral Config Metadata and Harden async_adapter.py & engine.py

## Objective & Scope Boundary

Add engine-neutral transaction/migration metadata on `BaseDatabaseConfig` and harden `extensions/sqlalchemy/{async_adapter,engine}.py` per [`../spec.md`](../spec.md) [^db-common] [^sa-dialects]. Out of scope: per-engine dialect modules (`ch02`–`ch03`).

## Bounded Read Manifest

- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig` properties and class attributes.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncEngineAdapter`, `attach_serverless_disconnect_guard`, and `AsyncAdapt_dbapi_connection` wiring.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — direct migration DSN resolution currently checking `PostgreSQLConfig` / `MySQLConfig`.

## Contracts & Edge Cases

- Declare `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str` on `BaseDatabaseConfig` so `vertebra.db.dbapi` and Alembic never run `isinstance(..., (BigQueryConfig, SpannerConfig, SQLiteConfig, PostgreSQLConfig, MySQLConfig))`.
- Harden `extensions/sqlalchemy/async_adapter.py` (`AdaptedAsyncConnection`, `AdaptedAsyncCursor`, `await_only` greenlet boundary, task cancellation cleanup, PEP 249 exception propagation) without leaking SQLAlchemy imports into `vertebra.db.dbapi`.
- Extract `SyncEngineAdapter` and `attach_serverless_disconnect_guard` into `extensions/sqlalchemy/engine.py`.

## Test Matrix

- Parametrized tests verifying `requires_statement_autocommit` rejects `autocommit=False` with `NotSupportedError` without importing concrete adapter config classes.
- Async greenlet cancellation and `DatabaseError` -> PEP 249 error propagation tests on `AdaptedAsyncConnection` and `AdaptedAsyncCursor`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py
```

## Acceptance Criteria

- [ ] Zero `isinstance` checks against concrete engine config classes remain in `db/dbapi/` or `extensions/sqlalchemy/`.
- [ ] `async_adapter.py` and `engine.py` pass strict `mypy` and unit cancellation/disconnect tests.
- [ ] Zero regressions in the chapter fast verification gate.

[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig base contract for all database configs.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current SyncEngineAdapter, attach_serverless_disconnect_guard, and greenlet adaptation.
