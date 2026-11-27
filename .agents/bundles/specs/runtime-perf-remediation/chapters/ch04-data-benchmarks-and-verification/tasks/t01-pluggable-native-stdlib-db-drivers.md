---
type: Task
id: "runtime-perf-remediation:ch04:t01"
chapter_id: ch04-data-benchmarks-and-verification
title: Implement pluggable native and stdlib DB drivers
description: Extend DataBackendConfig with db_adapter and db_adapter_flavor, decouple async DB paths from _db_lock, and implement native and stdlib SQLite/PostgreSQL WorldDriver backends.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - tools/benchmarks/apps/_data_backend.py
  - src/py/vertebra/db/adapters/sqlite/__init__.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
files:
  - tools/benchmarks/apps/_data_backend.py
  - tools/benchmarks/apps/_db_drivers.py
tests:
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
  - src/py/tests/unit/test_bench_infrastructure.py
estimated_loc: 220
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 specification and WorldDriver contracts
  - id: src-bench-backend
    resource: tools/benchmarks/apps/_data_backend.py::DataBackend
    title: Existing shared benchmark backend and DataBackendConfig
tags:
  - benchmarks
  - sqlite
  - postgres
  - drivers
---

# Implement Pluggable Native and Stdlib DB Drivers

## Objective & Scope Boundary

Add `db_adapter` and `db_adapter_flavor` configuration to `DataBackendConfig`, extract a `WorldDriver` protocol in `_db_drivers.py`, and implement `native` and `stdlib` SQLite/PostgreSQL drivers [^src-ch04-spec].

Route `sqlspec` and `advanced_alchemy` driver construction lazily to `tools.benchmarks.apps._db_orm_drivers.build_orm_world_driver` so `t02` lands without editing `_data_backend.py` or `_db_drivers.py`.

Keep `db_backend="memory"` and `db_adapter="native"` as defaults so all existing offline benchmark apps keep unchanged behavior [^src-bench-backend].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `tools/benchmarks/apps/_data_backend.py`
- `src/py/vertebra/db/adapters/sqlite/__init__.py`
- `src/py/vertebra/db/adapters/postgres/__init__.py`

## Contracts & Edge Cases

- Implement `DataBackendConfig` (`db_adapter`, `db_adapter_flavor`), `WorldDriver`, and `build_world_driver` per chapter `spec.md` Section 2.2 [^src-ch04-spec].
- `load_backend_config` reads `VTB_BENCH_DB_ADAPTER` (default `"native"`) and `VTB_BENCH_DB_ADAPTER_FLAVOR` (default `"vertebra"`), rejecting unknown `db_backend`, `db_adapter`, or `db_adapter_flavor` values with `ValueError`.
- Native SQLite uses `SyncSQLiteDriver` and `AsyncSQLiteDriver`; stdlib SQLite uses `sqlite3` and lazy `aiosqlite`.
- Native PostgreSQL uses `SyncPostgresDriver` and `AsyncPostgresDriver`, honoring `pool_size` and appending `shm_broker=true` when `broker_mode == "shm_broker"`.
- Stdlib PostgreSQL uses `psycopg` (or `psycopg_pool` sized to `pool_size`) for sync and lazy `asyncpg.create_pool(min_size=1, max_size=pool_size)` for async.
- Seed `world` (10,000 rows) idempotently:
  - SQLite: enable `WAL`, `synchronous=NORMAL`, and `busy_timeout=5000` on file-backed DBs, then `INSERT OR IGNORE` missing rows; seed `:memory:` connections on creation.
  - PostgreSQL: `CREATE TABLE IF NOT EXISTS world (id INTEGER PRIMARY KEY, randomnumber INTEGER NOT NULL)` followed by set-based `INSERT INTO world ... SELECT g, ... FROM generate_series(1, 10000) AS g ON CONFLICT (id) DO NOTHING`.
- Never hold `DataBackend._db_lock` across SQLite/Postgres driver I/O; use thread-local `random.Random` for row ID generation and await driver async methods directly in `DataBackend.{get_world,get_worlds,update_worlds,get_cached_worlds}`.
- Fail loud with `ValueError` when `db_backend == "postgres"` and `db_dsn` is empty.
- Strip accidental query strings before passing file paths to SQLite drivers unless using SQLite URI mode.
- Provide `aclose()` on `DataBackend` alongside `close()` to cleanly release async pools and sync connections.

## Test Matrix

| Case | Setup | Expected Behavior |
| :--- | :--- | :--- |
| Config defaults | `load_backend_config({})` | `db_backend="memory"`, `db_adapter="native"`, `db_adapter_flavor="vertebra"` |
| Invalid adapter | `load_backend_config({"VTB_BENCH_DB_ADAPTER": "bogus"})` | Raises `ValueError` listing valid adapters |
| Missing Postgres DSN | `DataBackend(DataBackendConfig(db_backend="postgres"))` | Raises `ValueError` citing `VTB_BENCH_DB_DSN` |
| SQLite `native` sync & async | File-backed `tmp_path / "w.db"` | Seeds 10,000 rows idempotently; sync/async fetch and update persist |
| SQLite `stdlib` sync & async | File-backed `tmp_path / "w.db"` | Sync `sqlite3` and async `aiosqlite` fetch and update without deadlock |
| Postgres SHM broker DSN | `broker_mode="shm_broker"` helper test | `shm_broker=true` appended once to `postgresql://` URL, skipped for SQLite paths |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/test_bench_infrastructure.py
uv run --no-sync ruff check tools/benchmarks/apps/_data_backend.py tools/benchmarks/apps/_db_drivers.py
uv run --no-sync mypy tools/benchmarks/apps/_data_backend.py tools/benchmarks/apps/_db_drivers.py
```

## Acceptance Criteria

- `DataBackendConfig` exposes `db_adapter` and `db_adapter_flavor` while keeping `db_backend="memory"` default.
- `DataBackend` supports `memory`, `sqlite`, and `postgres` with `native` and `stdlib` drivers without serializing async/native Postgres calls behind `_db_lock`.
- Ruff, Mypy, and existing benchmark unit suites pass with zero inline `#` comments in Python.

[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 specification and WorldDriver contracts.
[^src-bench-backend]: `tools/benchmarks/apps/_data_backend.py::DataBackend` — Existing shared benchmark backend and DataBackendConfig.
