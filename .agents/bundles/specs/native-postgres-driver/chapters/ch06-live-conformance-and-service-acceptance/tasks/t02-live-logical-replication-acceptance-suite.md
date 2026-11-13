---
type: Task
id: "native-postgres-driver:ch06-live-conformance-and-service-acceptance:t02"
flow_id: native-postgres-driver
chapter_id: ch06-live-conformance-and-service-acceptance
title: "Create live wal_level=logical replication acceptance suite"
description: "Add test_postgres_replication_live.py covering PostgresChangeSource, unchanged TOAST, non-UTF-8 bytea, reconnect, and slot contention."
status: stable
state: completed
commit: cc5e1533
last_verified_at: "2026-11-13T21:00:57Z"
last_verified_commit: cc5e1533
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch06:t01"
files: 
  - src/py/tests/integration/db/test_postgres_replication_live.py
tests: 
  - src/py/tests/integration/db/test_postgres_replication_live.py
read_set:
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/py/tests/integration/conftest.py
estimated_loc: 230
tags:
  - python
  - postgres
  - replication
  - testing
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py
    title: "Integration service fixtures"
---

# Task `t02`: Create live wal_level=logical replication acceptance suite

## Objective & Scope Boundary

Author `test_postgres_replication_live.py` against a `wal_level=logical` PostgreSQL service [^int-conftest].

## Bounded Read Manifest

- `change_stream.py`, `types.py`, and `integration/conftest.py`.

## Contracts & Edge Cases

- Skip cleanly when `VERTEBRA_CONFORMANCE_DOCKER` is unset; assert live `PG_UNCHANGED_TOAST`, raw `bytea`, CAS checkpoint resume after backend termination, and `55006` slot contention.

## Test Matrix

- Parameterized live replication scenarios + clean offline collection.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication_live.py -q && uv run --no-sync ruff check src/py/tests/integration/db/test_postgres_replication_live.py
```

## Acceptance Criteria

- [x] Suite collects and skips cleanly offline and exercises full `pgoutput` v1 fidelity under Docker.
- [x] Zero regressions in `ch06-live-conformance-and-service-acceptance` fast verification gate.

[^int-conftest]: `src/py/tests/integration/conftest.py` — Integration service fixtures.
