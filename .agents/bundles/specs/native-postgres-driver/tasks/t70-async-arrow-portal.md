---
type: Task
id: native-postgres-driver:t70
title: Own asynchronous Arrow portal cancellation
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t69
files:
- src/rs/vertebra-postgres/src/arrow.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/py/vertebra/_postgres.pyi
- src/py/tests/unit/db/test_postgres_native_data.py
tests:
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_async_arrow_batches
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_async_arrow_cancellation_discards_pending_result
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
- R16
- R20
---

# Own asynchronous Arrow portal cancellation

## Objective

Own asynchronous Arrow portal cancellation, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 400 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Mirror t69 async tests plus loop heartbeat during blocked fetch, overlapping anext InterfaceError, aclose before/after first/exhaustion, cancellation during creation/fetch and after completed worker queued publication. Use server/completion-drainer barriers; assert portal Close/rollback or socket EOF and pool-size1 next query progress. Retained batch/views outlive stream closure.
2. Async select_to_arrow returns owned adaptive ArrowOutput through existing `_vertebra_postgres_completion_drainer`, converting t66 PostgresArrowResult inline bytes or SHM capsule only on loop. `AsyncPostgresClient.stream_arrow_batches(...)` returns AsyncPostgresArrowStream directly (not coroutine), with __aiter__/__anext__/aclose and async contexts over t69 owner. States Unopened/Fetching/Open/Closing/Closed prevent overlap.
3. Pending batch completion retains cleanup guard. Publication transfers only batch mapping, never portal lease. Cancelled/undelivered completion drops SHM and closes stream; cannot later publish or return live portal to pool. aclose uses existing protocol cancellation/drain and deadline; no await under std mutex or Python attach on worker.
4. Exact stubs expose stream and sync/async COPY handle methods. t89 wraps native cursor to public ArrowCursor or per-batch PyArrow RecordBatch retaining mapping. Commit `feat(postgres): preserve async Arrow stream ownership`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k async_arrow -q
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R16, R20: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
