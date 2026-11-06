---
type: Task
id: native-postgres-driver:t62
title: Send COPY FROM with duplex backpressure and abort recovery
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t61
files:
- src/rs/vertebra-postgres/src/copy.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/py/vertebra/_postgres.pyi
- src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs::copy_in_duplex
- src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs::copy_in_abort_rolls_back
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R19
---

# Send COPY FROM with duplex backpressure and abort recovery

## Objective

Send COPY FROM with duplex backpressure and abort recovery, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 410 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Observe exact COPY header/rows/trailer/CopyDone and server count. Cover producer exception, server constraint failure while writes block, cancellation, repeated close and ambient transaction. A tiny-window peer emits ErrorResponse before reading remaining input; require bounded progress without write-all/read-later deadlock.
2. Add `PostgresConnection::start_copy_in(sql)` and an exclusive COPY input owner, validating CopyInResponse. Reject pipeline/multi-command COPY before side effects. `write_rows(columns,rows)` uses t60 and shared PgValue encoding; split CopyData <=64KiB, retain one bounded row plus two chunks and poll read/write concurrently. Overlapping writes raise InterfaceError before bytes.
3. `finish()` sends one trailer/CopyDone and consumes CommandComplete/ReadyForQuery. `abort(reason)` uses upstream frontend::copy_fail with sanitized bounded reason and drains. COPY-owned transaction commits only successful finish; errors rollback. Ambient transaction remains caller-owned and retains server Failed status. Drop delegates bounded pool cleanup; never silently commit partial input.
4. Expose sync/async `copy_from(sql)` handles with write(rows,column_oids), finish()->int, close/aclose abort and explicit contexts. Update stubs; commit `feat(postgres): stream COPY FROM with abort recovery`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_in
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R19: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
