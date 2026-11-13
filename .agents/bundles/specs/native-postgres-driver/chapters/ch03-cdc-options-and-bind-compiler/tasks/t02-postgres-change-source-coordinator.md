---
type: Task
id: "native-postgres-driver:ch03-cdc-options-and-bind-compiler:t02"
flow_id: native-postgres-driver
chapter_id: ch03-cdc-options-and-bind-compiler
title: "Implement PostgresChangeSource serialized checkpoint-before-ack coordinator"
description: "Build PostgresChangeSource streaming transaction records into ChangeFeed and persisting CAS checkpoints before wire WAL acks."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch03:t01"
files: 
  - src/py/vertebra/db/adapters/postgres/change_stream.py
tests: 
  - src/py/tests/unit/db/test_postgres_change_stream.py
read_set:
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/cdc.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
estimated_loc: 225
tags:
  - python
  - postgres
  - cdc
sources:
  - id: db-cdc
    resource: "src/py/vertebra/db/cdc.py::ChangeFeed"
    title: "ChangeFeed publish and spill hydration"
---

# Task `t02`: Implement PostgresChangeSource serialized checkpoint-before-ack coordinator

## Objective & Scope Boundary

Complete `PostgresChangeSource` in `change_stream.py` implementing `AsyncChangeSource` per `../spec.md` §2 [^db-cdc].

## Bounded Read Manifest

- `change_stream.py` (`t01`), `events.py`, `cdc.py`, and `contracts.md`.

## Contracts & Edge Cases

- Persist checkpoint inside `anyio.CancelScope(shield=True)` strictly before invoking `stream.acknowledge(commit_lsn)`; never acknowledge uncommitted or failed `ChangeFeed` publications.
- Support `stream()`, `replay_since(last_event_id)`, and idempotent `aclose()`.

## Test Matrix

- Unit tests verifying checkpoint-before-ack ordering, mid-commit cancellation shielding, spill hydration, and checkpoint failure non-acknowledgement.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_stream.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/change_stream.py
```

## Acceptance Criteria

- [ ] Wire `acknowledge(lsn)` never precedes durable checkpoint persistence.
- [ ] `mypy` passes cleanly.
- [ ] Zero regressions in `ch03-cdc-options-and-bind-compiler` fast verification gate.

[^db-cdc]: `src/py/vertebra/db/cdc.py::ChangeFeed` — ChangeFeed publish and spill hydration.
