---
type: Task
id: "native-postgres-driver:ch04-permanent-native-postgres-drivers:t02"
flow_id: native-postgres-driver
chapter_id: ch04-permanent-native-postgres-drivers
title: "Implement permanent AsyncPostgreSQLDriver in _async.py"
description: "Build native AsyncPostgreSQLDriver over AsyncPostgresPool/AsyncPostgresClient with shielded rollback and zero asyncio.to_thread."
status: stable
state: completed
commit: 50d7a1ef
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch04:t01"
files: 
  - src/py/vertebra/db/adapters/postgres/_async.py
tests: 
  - src/py/tests/unit/db/test_postgres_native_adapters.py
read_set:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/driver/__init__.py
  - src/py/tests/unit/db/postgres_ports.py
estimated_loc: 240
tags:
  - python
  - postgres
  - driver
  - async
sources:
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: "AsyncPostgresPool and AsyncPostgresClient stubs"
---

# Task `t02`: Implement permanent AsyncPostgreSQLDriver in _async.py

## Objective & Scope Boundary

Create `_async.py::AsyncPostgreSQLDriver` executing natively over `AsyncPostgresPool` / `AsyncPostgresClient` per `../spec.md` §2 [^pg-pyi].

## Bounded Read Manifest

- `_sync.py`, `_postgres.pyi`, `db/driver/__init__.py`, and `postgres_ports.py`.

## Contracts & Edge Cases

- Shield `rollback()` and pool release/eviction inside `anyio.CancelScope(shield=True)` when `BaseException` (including `CancelledError`) exits `transaction()`.

## Test Matrix

- Async unit tests covering shielded cancellation rollback, nested savepoints, RLS reset-or-evict, and per-command `timeout`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/_async.py
```

## Acceptance Criteria

- [ ] `AsyncPostgreSQLDriver` uses native Rust awaitables with zero thread offload.
- [ ] Zero regressions in `ch04-permanent-native-postgres-drivers` fast verification gate.

[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — AsyncPostgresPool and AsyncPostgresClient stubs.
