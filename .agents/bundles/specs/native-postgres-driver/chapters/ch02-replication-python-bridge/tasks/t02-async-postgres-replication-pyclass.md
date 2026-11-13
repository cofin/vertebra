---
type: Task
id: "native-postgres-driver:ch02-replication-python-bridge:t02"
flow_id: native-postgres-driver
chapter_id: ch02-replication-python-bridge
title: "Implement frozen AsyncPostgresReplication pyclass and module registration"
description: "Expose AsyncPostgresReplication (#[pyclass(frozen)]) with cancel-safe next, fenced acknowledge, aclose, closed, and async context management."
status: stable
state: completed
commit: 355ff6e2
last_verified_commit: 355ff6e2
last_verified_at: "2026-11-13T04:53:53Z"
verification_evidence: "maturin develop + pytest test_postgres_native_replication.py (110 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch02:t01"
files: 
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests: 
  - src/py/tests/unit/db/test_postgres_native_replication.py
read_set:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/notifications.rs
estimated_loc: 230
tags:
  - rust
  - pyo3
  - postgres
sources:
  - id: pg-lib
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: "vertebra._postgres module registration"
---

# Task `t02`: Implement frozen AsyncPostgresReplication pyclass and module registration

## Objective & Scope Boundary

Implement `AsyncPostgresReplication` and register it in `vertebra._postgres` per `../spec.md` §2 [^pg-lib].

## Bounded Read Manifest

- `replication/mod.rs` — conversion helpers from `t01`.
- `notifications.rs` — `AsyncPostgresListener` pyclass lifecycle pattern.
- `lib.rs` — pymodule exports.

## Contracts & Edge Cases

- Reject concurrent `next()` calls with `InterfaceError`; `StopAsyncIteration` on clean stream end.
- `acknowledge(lsn)` validates `u64` bounds and delegates to `ReplicationSupervisor::acknowledge`; `aclose()` is idempotent.

## Test Matrix

- Parameterized pytest unit tests in `test_postgres_native_replication.py` covering iteration, cancelled `asyncio.wait_for(stream.next(), ...)` non-loss, `acknowledge`, and `aclose`.

## Verification

```bash
UV_NO_SYNC=1 PYO3_PYTHON=.venv/bin/python make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_replication.py -q
```

## Acceptance Criteria

- [x] `AsyncPostgresReplication` passes cancel-safety, concurrent-`next` rejection, and `aclose` tests.
- [x] Zero regressions in `ch02-replication-python-bridge` fast verification gate.

[^pg-lib]: `src/rs/vertebra-postgres/src/lib.rs` — vertebra._postgres module registration.
