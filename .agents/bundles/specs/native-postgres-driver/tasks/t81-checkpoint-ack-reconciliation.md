---
type: Task
id: native-postgres-driver:t81
title: Serialize checkpoint acknowledgements and uncertain outcomes
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t80
files:
- src/py/vertebra/db/adapters/postgres/change_stream.py
- src/py/tests/unit/db/test_postgres_change_source.py
tests:
- src/py/tests/unit/db/test_postgres_change_source.py::test_checkpoint_save_precedes_feedback
- src/py/tests/unit/db/test_postgres_change_source.py::test_ack_caller_cancel_keeps_source_owned_task
- src/py/tests/unit/db/test_postgres_change_source.py::test_ack_ambiguous_save_closes_generation
- src/py/tests/unit/db/test_postgres_change_source.py::test_ack_barrier_retains_pending_next
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
- R22
- R23
---

# Serialize checkpoint acknowledgements and uncertain outcomes

## Objective

Keep durable checkpoint state, protocol feedback and event generations consistent when acknowledgement callers fail or cancel.

## Context

Read spec/checkpoint contracts and t78, t79, t80. Execution placement is t80 -> t81 -> t82; numeric task IDs are stable. This pending revision1/null-plan-commit worksheet authorizes no implementation now. One signed conventional local commit on the same complete-driver branch/PR; preserve others. Estimated400 handwritten lines including tests. Source owns the acknowledgement task and yield barrier; native autoreconnect is disabled. No generic AsyncChangeSource ack API is added.

## Steps

1. Add async source.acknowledge(commit_end_lsn). Under short state lock validate emitted contiguous commit boundary and capture its next_event_id, identity and loaded expected checkpoint; reject future/noncommit/nonmonotonic values before I/O. Atomically close the yield gate and install ONE source-owned tracked task before awaiting. A second concurrent acknowledge raises InterfaceError; duplicate fully reconciled ack is idempotent only when checkpoint matches. Do not hold state lock across store/native I/O.
2. Tracked task saves checkpoint with expected CAS first, then awaits current generation native.acknowledge. Caller awaits asyncio.shield(task): cancelling caller raises CancelledError to caller but cannot retract or cancel a possibly durable save. Source retains/observes task and its exception even when caller cancelled; next stream/close sees stored terminal failure and loop exception handler receives no unhandled-task report. No unhandled detached task. On both success update known checkpoint and reopen gate in same generation; retain one pending next result, preserving order. COMMIT counter snapshot is captured at its emission, not current counter after later B/C yields.
3. Any store/native error or internal cancellation is uncertain until durable load resolves. Before exposing error, close gate/invalidate old generation, close old native and run bounded store.load(identity_key); do not assume a failed save did not commit. Install the loaded immutable checkpoint and matching counter together in a KnownClosed state, with no live old generation or event publication. This safe atomic task ends the interrupted stream after reconciliation; t82 adds config-controlled reopen/retry using the same KnownClosed state, so no future helper or placeholder is required for this task to pass. No subsequent event is yielded until loaded LSN and matching next_event_id are installed together. Save-success/native-ack-failure never reuses old native generation at old LSN. Define adapter PostgresReplicationGapError(OperationalError) with reason identity_mismatch/wal_unavailable/checkpoint_uncertain/retries_exhausted and preserve original SQLSTATE/cause where available. CAS conflict also closes/reloads but is terminal ownership conflict: never silently skip locally unseen events by adopting another writer. Preserve original error after reconciliation, with load/cleanup failures chained.
4. Deterministic tests use Events/barriers and scripted native peer: no feedback before successful store CAS; save succeeds then native ack fails/caller cancels; store commits then raises CancelledError; pending native.next finishes during barrier; no old record is yielded while uncertain; caller cancellation does not leave store/protocol half-orchestrated. Basic source close joins or cancels+reloads the tracked task under checkpoint_timeout and ends KnownClosed/Closed without a hidden worker; t82 expands repeated-close/reconnect coverage. Tests assert loaded checkpoint/counter and wire ack, not task scheduling internals. No sleeps. Commit `fix(postgres): serialize durable checkpoint acknowledgement ownership`.

## Verification

Record initial RED for acknowledgement cancellation/save ambiguity, then fresh GREEN:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'ack_ or checkpoint_save' -q
```

Planned commands only; no execution evidence exists during planning. Capture actual exit/assertions during implementation. Rebuild prevents stale native tests. No external writes, Git tag or push authorized.

## Acceptance Criteria

- [ ] R22/R23: save-before-feedback remains true after failure/cancellation, not just the happy path.
- [ ] One shielded source-owned task and generation barrier prevent old-LSN/new-ID delivery; no mutex spans native.next.
- [ ] CAS conflict/ambiguous save is reconciled or fails closed; no untracked acknowledgement remains.
- [ ] One owned local commit and synchronized task/spec state record fresh evidence.

## Notes & Discoveries

None.
