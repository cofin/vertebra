---
type: Task
id: "native-postgres-driver:t61"
title: "Own COPY TO streaming and recovery"
description: "Own COPY TO streaming and recovery for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t60"
files:
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs
plan_revision: 2
plan_commit: 9af810f5
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - native
  - data-events
---

# Own COPY TO streaming and recovery

## Objective

Own COPY TO streaming and recovery, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 410 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Script binary CopyOutResponse, fragmented rows, CopyData*, CopyDone, CommandComplete(COPY n), ReadyForQuery; assert first row arrives before a peer barrier permits completion. Cover empty output, wrong formats/count, notice/status interleaving, server error before/midstream, malformed/truncated output and consumer close/cancel. Pool-size1 follow-up query verifies reuse only after drain or replacement.
2. Add `PostgresConnection::start_copy_out(sql)` and `CopyOut` owning exclusive PooledConnection/current transaction pin. Caller supplies COPY SQL; reject pipeline COPY before writes. Validate binary overall/per-column formats; feed t60 decoder and expose incremental `next_row()->Option<Vec<Option<Bytes>>>`. Command tag supplies count; full decoded count must agree.
3. Expose `SyncPostgresClient.copy_to(sql)->SyncPostgresCopyOut` and `await AsyncPostgresClient.copy_to(sql)->AsyncPostgresCopyOut`; sync iterator/async iterator return raw nullable field bytes. Explicit close/aclose and context methods own cleanup. Early close uses existing CancelRequest then drains under bounded recovery deadline; failed recovery discards session. Cancellation retains cleanup guard; Drop never returns a copying lease or blocks the GIL.
4. Update exact stubs. Commit `feat(postgres): own COPY TO stream recovery`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_out
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
