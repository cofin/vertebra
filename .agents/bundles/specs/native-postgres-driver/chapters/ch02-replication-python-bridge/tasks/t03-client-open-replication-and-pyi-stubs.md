---
type: Task
id: "native-postgres-driver:ch02-replication-python-bridge:t03"
flow_id: native-postgres-driver
chapter_id: ch02-replication-python-bridge
title: "Wire AsyncPostgresClient.replication_identity/open_replication and 14-class _postgres.pyi"
description: "Add replication_identity and open_replication on AsyncPostgresClient and finalize _postgres.pyi for stubtest parity."
status: stable
state: completed
commit: a37cac43
last_verified_commit: a37cac43
last_verified_at: "2026-11-13T04:53:53Z"
verification_evidence: "stubtest vertebra._postgres + ruff + mypy + pytest (110 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch02:t02"
files: 
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests: 
  - src/py/tests/unit/db/test_postgres_native_replication.py
read_set:
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/types.py
estimated_loc: 205
tags:
  - rust
  - pyo3
  - postgres
  - typing
sources:
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: "Typed PyO3 stub for vertebra._postgres"
---

# Task `t03`: Wire AsyncPostgresClient.replication_identity/open_replication and 14-class _postgres.pyi

## Objective & Scope Boundary

Expose `replication_identity` and `open_replication` on `AsyncPostgresClient` and update `_postgres.pyi` per `../spec.md` §2 [^pg-pyi].

## Bounded Read Manifest

- `async_client.rs` — `open_listener` precedent.
- `_postgres.pyi` — existing 13-class stub.
- `types.py` — `PostgresReplicationIdentity` and `PostgresReplicationRecord`.

## Contracts & Edge Cases

- Refuse `PoolerMode::Transaction` and `PoolerMode::Statement` with `NotSupportedError` before dialing.
- Validate `1 <= queue_capacity <= 1024` (`ValueError`) and verify `mypy.stubtest vertebra._postgres` exits `0`.

## Test Matrix

- Unit tests for pooler-mode refusal, queue-capacity bounds, identity probing, and `mypy.stubtest` verification.

## Verification

```bash
UV_NO_SYNC=1 PYO3_PYTHON=.venv/bin/python make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_replication.py -q
```

## Acceptance Criteria

- [x] `mypy.stubtest vertebra._postgres` passes with all 14 native classes.
- [x] Pooler-mode and queue-bound validations pass unit tests.
- [x] Zero regressions in `ch02-replication-python-bridge` fast verification gate.

[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Typed PyO3 stub for vertebra._postgres.
