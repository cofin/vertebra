---
type: Task
id: "native-postgres-driver:ch04-permanent-native-postgres-drivers:t01"
flow_id: native-postgres-driver
chapter_id: ch04-permanent-native-postgres-drivers
title: "Implement permanent SyncPostgreSQLDriver in _sync.py and postgres_ports.py doubles"
description: "Build SyncPostgreSQLDriver over SyncPostgresPool/SyncPostgresClient with queries, transactions/savepoints, RLS scopes, and binary COPY."
status: stable
state: completed
commit: bc86ed84
last_verified_at: "2026-11-13T05:51:30Z"
last_verified_commit: bc86ed84
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch03:t04"
files: 
  - src/py/vertebra/db/adapters/postgres/_sync.py
tests: 
  - src/py/tests/unit/db/postgres_ports.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
read_set:
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/options.py
  - src/py/vertebra/db/adapters/postgres/binds.py
  - src/py/vertebra/db/adapters/postgres/core.py
estimated_loc: 235
tags:
  - python
  - postgres
  - driver
sources:
  - id: db-driver-base
    resource: src/py/vertebra/db/driver/__init__.py
    title: "SyncDatabaseDriver contract"
---

# Task `t01`: Implement permanent SyncPostgreSQLDriver in _sync.py and postgres_ports.py doubles

## Objective & Scope Boundary

Create `_sync.py::SyncPostgreSQLDriver` and `postgres_ports.py` protocol test doubles per `../spec.md` §2 [^db-driver-base].

## Bounded Read Manifest

- `db/driver/__init__.py`, `_postgres.pyi`, `options.py`, `binds.py`, and `core.py`.

## Contracts & Edge Cases

- Implement `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select*`, `transaction()` / nested savepoints, `enter_tenant_scope`/`exit_tenant_scope`/`evict_connection`, `copy_from`/`copy_to`, `cancel()`, and `close()`.

## Test Matrix

- Parameterized sync driver unit tests over `postgres_ports.py` covering pool lease pin during transactions, `NotFoundError`, savepoint rollback, RLS eviction on reset failure, and `timeout` forwarding.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/_sync.py
```

## Acceptance Criteria

- [x] `SyncPostgreSQLDriver` satisfies `SyncDatabaseDriver` with zero `psycopg` imports.
- [x] Zero regressions in `ch04-permanent-native-postgres-drivers` fast verification gate.

[^db-driver-base]: `src/py/vertebra/db/driver/__init__.py` — SyncDatabaseDriver contract.
