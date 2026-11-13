---
type: Task
id: "native-postgres-driver:ch05-atomic-cutover-and-legacy-removal:t02"
flow_id: native-postgres-driver
chapter_id: ch05-atomic-cutover-and-legacy-removal
title: "Replace driver.py with 12-line native barrel and migrate PostgreSQL unit suites"
description: "Cut driver.py, __init__.py, and embeddings.py over to native PostgreSQL drivers and replace psycopg/SQLite test doubles with postgres_ports.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch05:t01"
files: 
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
tests: 
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/db/test_native_db_drivers.py
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/postgres_ports.py
estimated_loc: 245
tags:
  - python
  - postgres
  - cutover
sources:
  - id: pg-driver
    resource: "src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver"
    title: "Public PostgreSQL driver barrel"
---

# Task `t02`: Replace driver.py with 12-line native barrel and migrate PostgreSQL unit suites

## Objective & Scope Boundary

Replace `driver.py` with a clean barrel over `_sync.py`/`_async.py`, finalize `__init__.py` exports, remove `_uses_dollar_binds()` in `embeddings.py`, and migrate unit tests off `_SQLiteAsyncPGPool`/`psycopg` mocks [^pg-driver].

## Bounded Read Manifest

- `driver.py`, `__init__.py`, `embeddings.py`, `test_postgres_driver.py`, and `postgres_ports.py`.

## Contracts & Edge Cases

- Zero `_Legacy*` classes or `psycopg_pool` imports remain in `driver.py`; `embeddings.py` unconditionally emits `$1`/`$2`.

## Test Matrix

- Migrated `test_postgres_driver.py`, `test_pg_dataplane_and_datafusion.py`, `test_google_embedders.py`, and `test_native_db_drivers.py` over `postgres_ports.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py -q
```

## Acceptance Criteria

- [ ] All PostgreSQL adapter and pushdown unit suites pass over native driver classes.
- [ ] Zero regressions in `ch05-atomic-cutover-and-legacy-removal` fast verification gate.

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver barrel.
