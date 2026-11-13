---
type: Task
id: "native-postgres-driver:ch03-cdc-options-and-bind-compiler:t01"
flow_id: native-postgres-driver
chapter_id: ch03-cdc-options-and-bind-compiler
title: "Add PostgresReplicationDriver, EventSessionRegistry, and PostgresCheckpointStore"
description: "Extend events.py with PostgresReplicationDriver and EventSessionRegistry and create checkpoint structs/stores in change_stream.py."
status: stable
state: completed
commit: bff04dcd
last_verified_commit: bff04dcd
last_verified_at: "2026-11-13T05:14:30Z"
verification_evidence: "ruff + mypy (15 files) + pytest (111 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch02:t03"
files: 
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/adapters/postgres/change_stream.py
tests: 
  - src/py/tests/unit/db/test_postgres_change_stream.py
read_set:
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/cdc.py
estimated_loc: 190
tags:
  - python
  - postgres
  - cdc
sources:
  - id: pg-events
    resource: src/py/vertebra/db/adapters/postgres/events.py
    title: "PostgreSQL notification and event protocols"
---

# Task `t01`: Add PostgresReplicationDriver, EventSessionRegistry, and PostgresCheckpointStore

## Objective & Scope Boundary

Implement `PostgresReplicationDriver`, `EventSessionRegistry`, `PostgresCheckpoint`, `PostgresCheckpointStore`, and `MemoryPostgresCheckpointStore` per `../spec.md` §2 [^pg-events].

## Bounded Read Manifest

- `events.py`, `types.py`, `capabilities.py`, and `cdc.py`.

## Contracts & Edge Cases

- `MemoryPostgresCheckpointStore.save` enforces CAS fencing on `(system_identifier, slot_name, expected_lsn)` and raises `SerializationFailure` on stale writer conflict.
- `EventSessionRegistry.close_all()` drains registered listeners and replication streams cleanly without masking primary errors.

## Test Matrix

- Parameterized unit tests for CAS fencing, regression/gap errors, and `EventSessionRegistry` close cleanup.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_stream.py -q
```

## Acceptance Criteria

- [x] CAS checkpoint fencing and `EventSessionRegistry` cleanup pass all unit assertions.
- [x] Zero regressions in `ch03-cdc-options-and-bind-compiler` fast verification gate.

[^pg-events]: `src/py/vertebra/db/adapters/postgres/events.py` — PostgreSQL notification and event protocols.
