---
type: Task
id: "runtime-perf-remediation:ch04:t02"
chapter_id: ch04-data-benchmarks-and-verification
title: Implement SQLSpec and Advanced Alchemy DB drivers
description: Implement sqlspec and advanced_alchemy WorldDriver backends for SQLite and PostgreSQL supporting both Vertebra extensions (+vtb, BrokerPool) and upstream/vanilla baselines.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch04:t01"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - tools/benchmarks/apps/_db_drivers.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/__init__.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/pool.py
files:
  - tools/benchmarks/apps/_db_orm_drivers.py
tests:
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
estimated_loc: 220
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 specification and adapter matrix
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py::capture_sqlspec_report
    title: Reference SQLSpec and SQLAlchemy BrokerPool wiring patterns
tags:
  - benchmarks
  - sqlspec
  - advanced-alchemy
  - sqlite
  - postgres
---

# Implement SQLSpec and Advanced Alchemy DB Drivers

## Objective & Scope Boundary

Implement `build_orm_world_driver` in `tools/benchmarks/apps/_db_orm_drivers.py` providing `WorldDriver` implementations for `sqlspec` and `advanced_alchemy` across SQLite and PostgreSQL [^src-ch04-spec].

Support both `db_adapter_flavor="vertebra"` (Vertebra SQLSpec adapters and `+vtb` / `+vtb_async` SQLAlchemy dialects) and `db_adapter_flavor="upstream"` (upstream `sqlspec` and vanilla SQLAlchemy engines) for apples-to-apples server baselines [^src-bench-sql].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `tools/benchmarks/apps/_db_drivers.py`
- `src/py/vertebra/extensions/sqlspec/adapters/sqlite/__init__.py`
- `src/py/vertebra/extensions/sqlspec/adapters/postgres/__init__.py`
- `src/py/vertebra/extensions/sqlalchemy/pool.py`

## Contracts & Edge Cases

- Implement `build_orm_world_driver(config: DataBackendConfig, *, pool_size: int) -> WorldDriver` matching the `WorldDriver` protocol from `_db_drivers.py` [^src-ch04-spec].
- `sqlspec` (`vertebra` flavor):
  - SQLite uses `vertebra.extensions.sqlspec.adapters.sqlite.{SQLiteSyncConfig, SQLiteAsyncConfig}`.
  - PostgreSQL uses `vertebra.extensions.sqlspec.adapters.postgres.{PostgresSyncConfig, PostgresAsyncConfig}` honoring `pool_size` and `shm_broker=true` when `broker_mode == "shm_broker"`.
- `sqlspec` (`upstream` flavor):
  - SQLite uses `sqlspec.adapters.sqlite.SqliteConfig` (sync) and `sqlspec.adapters.aiosqlite.AiosqliteConfig` (async, when available).
  - PostgreSQL uses `sqlspec.adapters.psycopg.PsycopgSyncConfig` (sync, when available) and `sqlspec.adapters.asyncpg.AsyncpgConfig` (async) sized to `pool_size`.
- `advanced_alchemy`:
  - Define declarative `BenchWorldModel` (`__tablename__ = "world"`, `id: Mapped[int]`, `randomnumber: Mapped[int]`) paired with `SQLAlchemySyncRepositoryService` and `SQLAlchemyAsyncRepositoryService`.
  - `vertebra` flavor builds `sqlite+vtb` / `sqlite+vtb_async` and `postgresql+vtb` / `postgresql+vtb_async` engines, binding `vertebra.extensions.sqlalchemy.pool.BrokerPool` when `broker_mode == "shm_broker"` and `DatabaseRegistry.default_broker()` is active.
  - `upstream` flavor builds vanilla `sqlite` / `sqlite+aiosqlite` and `postgresql+psycopg` / `postgresql+asyncpg` engines sized to `pool_size`.
- Import optional packages (`sqlspec`, `sqlalchemy`, `advanced_alchemy`) via `vertebra.utils.module_loader.import_optional` and raise actionable `RuntimeError` messages when missing.
- Seed the 10,000-row `world` table idempotently via `_db_drivers` helpers without holding any global Python mutex during async queries.
- Dispose sync engines in `close()` and `await async_engine.dispose()` in `aclose()`.

## Test Matrix

| Case | Setup | Expected Behavior |
| :--- | :--- | :--- |
| SQLSpec SQLite (`vertebra`) | File-backed SQLite via `DataBackend` | Sync and async fetch/update succeed and persist updated `randomNumber` |
| SQLSpec SQLite (`upstream`) | File-backed SQLite with `flavor="upstream"` | Upstream `SqliteConfig` reads seeded `world` rows cleanly |
| Advanced Alchemy SQLite (`vertebra`) | `sqlite+vtb` and `sqlite+vtb_async` | Sync and async repository services fetch and update `BenchWorldModel` |
| Advanced Alchemy SQLite (`upstream`) | Vanilla `sqlite` / `sqlite+aiosqlite` | Sync and async repository services match Vertebra schema and results |
| URL rewriting for Postgres | `postgresql://user:pass@host/db` | Rewrites cleanly to `postgresql+vtb://`, `postgresql+vtb_async://`, `postgresql+psycopg://`, `postgresql+asyncpg://` |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py
uv run --no-sync ruff check tools/benchmarks/apps/_db_orm_drivers.py
uv run --no-sync mypy tools/benchmarks/apps/_db_orm_drivers.py
```

## Acceptance Criteria

- `sqlspec` and `advanced_alchemy` work end-to-end through `DataBackend` for both `vertebra` and `upstream` flavors on SQLite and PostgreSQL.
- `BrokerPool` binds cleanly when `broker_mode == "shm_broker"` on PostgreSQL `+vtb` engines.
- Ruff and Mypy pass cleanly with zero inline `#` comments.

[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 specification and adapter matrix.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_sqlspec_report` — Reference SQLSpec and SQLAlchemy BrokerPool wiring patterns.
