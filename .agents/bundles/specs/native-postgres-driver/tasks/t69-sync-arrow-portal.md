---
type: Task
id: native-postgres-driver:t69
title: Expose synchronous Arrow selection and pinned batch streams
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t68
files:
- src/rs/vertebra-postgres/src/arrow.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/py/vertebra/_postgres.pyi
- src/py/tests/unit/db/test_postgres_native_data.py
tests:
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_sync_arrow_schema_and_batches
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_sync_arrow_stream_closes_portal
plan_revision: 1
plan_commit: null
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
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R16
- R20
---

# Expose synchronous Arrow selection and pinned batch streams

## Objective

Expose synchronous Arrow selection and pinned batch streams, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 400 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Test native select_to_arrow empty/inline/threshold boundary/SHM output, correct count/used_shm/buffer values, no negative option SQL side effects, batch boundaries and exact-width renames. Pool-size1 remains pinned until exhaustion/close. Peer PortalSuspended barriers prove incremental fetch and no fetch after close. Cover consumer/server/decode/deadline errors and ambient transaction ownership.
2. `SyncPostgresClient.select_to_arrow(sql,parameters=None,*,threshold_rows=256,max_result_bytes=268435456)` directly uses Rust RowDescription/catalog/builders/IPC and returns t66 PostgresArrowResult with inline bytes for <=threshold rows, SHM core cursor above threshold. Validate threshold>=0, batch_size>0 and max_result_bytes>0 before SQL. select intentionally accumulates IPC under explicit max_result_bytes cap(default256MiB), error on breach; never calls Python query or tuple-row conversion.
3. `stream_arrow_batches(sql,parameters=None,*,batch_size=65536,columns=None)->SyncPostgresArrowStream` yields per-batch core cursors. Iterator/context/close opens t42 portal in owned transaction and pins lease; fetch bounded by rows+bytes. Use t43 `Portal::start_fetch(max_rows)` and `Portal::next_event()->PortalEvent::{Row(Vec<PgValue>),Suspended,Complete{command_tag,affected_rows}}`. Append one owned Row directly to builder, pause when complete row reaches byte cap and retain pending Execute continuation; next batch resumes next_event before another Execute. Only Suspended permits start_fetch again. Never call aggregate Portal.fetch()->QueryResult or its Python conversion. One bounded pending row is permitted for cap handoff; oversized single row errors. columns renames exactly matching width, preserves duplicates.
4. Exhaustion closes portal then commits only stream-owned transaction. Early close/error rolls it back. Ambient transaction uses stream savepoint, rollback/release on early/failure, never commits caller work. Drop uses bounded pool recovery/discard; detach GIL around fetch/close. Commit `feat(postgres): expose synchronous Arrow portal streams`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k sync_arrow -q
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
