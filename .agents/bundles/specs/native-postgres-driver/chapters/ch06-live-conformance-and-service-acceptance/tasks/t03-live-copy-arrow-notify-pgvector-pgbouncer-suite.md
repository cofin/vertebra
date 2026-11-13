---
type: Task
id: "native-postgres-driver:ch06-live-conformance-and-service-acceptance:t03"
flow_id: native-postgres-driver
chapter_id: ch06-live-conformance-and-service-acceptance
title: "Create live PGCOPY, VTBARROW, LISTEN/NOTIFY, pgvector, and PgBouncer suite"
description: "Add test_postgres_native_features_live.py covering binary COPY, Arrow SHM, notifications, pgvector, and PgBouncer transaction/statement modes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch06:t01"
files: 
  - src/py/tests/integration/db/test_postgres_native_features_live.py
tests: 
  - src/py/tests/integration/db/test_postgres_native_features_live.py
read_set:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/tests/integration/conftest.py
estimated_loc: 245
tags:
  - python
  - postgres
  - pgvector
  - pgbouncer
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py
    title: "Integration service fixtures"
---

# Task `t03`: Create live PGCOPY, VTBARROW, LISTEN/NOTIFY, pgvector, and PgBouncer suite

## Objective & Scope Boundary

Author `test_postgres_native_features_live.py` with `pgvector:0.8.1-pg18` and `pgbouncer:v1.26.0` fixtures [^int-conftest].

## Bounded Read Manifest

- `_sync.py`, `_async.py`, `notify.py`, and `integration/conftest.py`.

## Contracts & Edge Cases

- Verify `PGCOPY`, `VTBARROW` `ArrowCursor` + `load_from_arrow`, `LISTEN/NOTIFY`, `vector`/`halfvec`/`bit` round-trips, and `PoolerMode::{Transaction, Statement}` unnamed-statement execution + replication refusal.

## Test Matrix

- Live feature matrix + offline skip collection.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_postgres_native_features_live.py -q && uv run --no-sync ruff check src/py/tests/integration/db/test_postgres_native_features_live.py
```

## Acceptance Criteria

- [ ] All native PostgreSQL features and PgBouncer modes are covered with clean offline collection.
- [ ] Zero regressions in `ch06-live-conformance-and-service-acceptance` fast verification gate.

[^int-conftest]: `src/py/tests/integration/conftest.py` — Integration service fixtures.
