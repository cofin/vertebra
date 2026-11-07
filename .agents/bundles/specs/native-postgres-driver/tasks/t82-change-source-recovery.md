---
type: Task
id: "native-postgres-driver:t82"
title: "Preserve change-source replay identity and shutdown semantics"
description: "Preserve change-source replay identity and shutdown semantics for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t81"
files:
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/tests/unit/db/test_postgres_change_source.py
tests:
  - src/py/tests/unit/db/test_postgres_change_source.py
plan_revision: 2
plan_commit: cdb978d3
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

# Preserve change-source replay identity and shutdown semantics

## Objective

Preserve change-source replay identity and shutdown semantics; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 400 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Implement SOURCE-owned reconnect loop around native reconnect=False. On transport error or t81 uncertain acknowledgement outcome, close yield gate, invalidate source generation, dispose/join old native.next and handle, reload store(identity_key) under timeout, reverify identity, reset counter from that EXACT checkpoint and reopen at its commit LSN. No old-generation record may cross the gate. Source config.reconnect permits at most8 recoverable attempts with exponential0.1..5s jitter; false performs close/reload reconciliation then surfaces the original failure without another data generation. Identity/publication/WAL gaps and CAS ownership conflict are terminal adapter PostgresReplicationGapError/CheckpointConflictError(OperationalError); never reset store or adopt a newer server LSN.
2. Deterministic peer/store barriers reproduce A/B/C: A COMMIT's save succeeds, B/C were already yielded, native acknowledgement fails before feedback. Reopen at loaded A end LSN with A.next_event_id, replay B/C with original IDs/dedup keys, never label old records with new IDs or skip them. Separate test save raises/cancels after actual commit; reloaded state, not exception assumption, decides resume. A new source instance after simulated crash loads A and yields the same replay IDs. Assert every source open passed reconnect=False and old peer generation yields nothing after barrier.
3. Keep constructor/driver state typed as adapter-owned events.PostgresReplicationDriver from t80 throughout reopen/reconciliation; never reintroduce concrete legacy driver annotations or a dependency on t90 hook bodies. Real-native NativeReplicationDriverPort implements identity/config/open methods for these earlier source tests. One asyncio-loop owner, short generation/ack state lock never held across native.next, user yield or store/native await. After next returns, validate generation/gate and synchronously assign/yield without another await. During successful ack barrier retain at most one completed next result and publish only if generation unchanged; after reconciliation discard old result and replay. Never discard a result then continue the same generation. Reject concurrent stream; t81 serializes one tracked ack operation.
4. aclose uses one absolute checkpoint_timeout deadline across all join/cancel/reload/native-close steps, gates yields, marks closing and requests no new acknowledgement. Join/shield existing source-owned ack while reserving remaining budget for cancellation/reload. On timeout cancel/join that operation and perform bounded store reload before discarding generation; if durable state cannot be established, remain terminal/failclosed and raise PostgresReplicationGapError(reason=checkpoint_uncertain), with no future yields/reopen or background untracked acknowledgement. Custom store cancellation-cooperation is required. Close old native and pending next under deadline, preserve original error with reconciliation error chained. Nontransactional messages remain commit-only ack/WAL-retention documented. Cross-source production-driver integration is final t97/t90 acceptance. Commit `fix(postgres): reconcile durable checkpoints before source reconnect`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -q
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/tests/unit/db/test_postgres_change_source.py
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k notification_source -q
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22, R23: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
