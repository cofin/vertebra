---
type: Task
id: native-postgres-driver:t79
title: Expose cancellation-safe native replication handles
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t78
files:
- src/rs/vertebra-postgres/src/replication/mod.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/py/vertebra/_postgres.pyi
- src/py/vertebra/db/adapters/postgres/types.py
- src/py/tests/unit/db/test_postgres_change_source.py
tests:
- src/py/tests/unit/db/test_postgres_change_source.py::test_native_replication_identity_does_not_start
- src/py/tests/unit/db/test_postgres_change_source.py::test_native_replication_stream_lifecycle
- src/py/tests/unit/db/test_postgres_change_source.py::test_native_replication_cancel_queued_record
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

# Expose cancellation-safe native replication handles

## Objective

Expose cancellation-safe native replication handles; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 400 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Expose AsyncPostgresClient.replication_identity(slot,publication)->Awaitable[PostgresReplicationIdentity(system_identifier,database,slot,publication_fingerprint)]. Reuse existing dedicated IDENTIFY_SYSTEM/slot/publication preflight WITHOUT START_REPLICATION. Validate/close probe session on every result/error/cancel; scripted peer asserts no START and physical EOF. Frozen identity exposes identity_key=(system_identifier,database,slot,publication_fingerprint) for store.load before streaming.
2. AsyncPostgresClient.open_replication(slot,publication,*,start_lsn=None,queue_capacity=1024,reconnect=True)->Awaitable[AsyncPostgresReplication]. next()->Awaitable[PostgresReplicationRecord|None], acknowledge(commit_end_lsn)->Awaitable[None], aclose/async context, readonly closed:bool. closed becomes true on normal terminal end, unrecoverable error or aclose after ownership is terminal, remains false during recoverable raw autoreconnect. Getter is owned-state snapshot with no I/O. Frozen record(kind,payload,commit_end_lsn=None,generation=0) preserves lossless values and TOAST; generation-start control carries verified identity/acknowledged_lsn before any data. Native acknowledge changes protocol feedback only; it promises no durable Python store write.
3. Existing completion bridge has one pending next and publication guards. Cancelled/closed queued completions cannot mark COMMIT emitted or publish after close. aclose cancels read/write/backoff waiters, disposes bounded queue and joins under deadline. No Python attach/callback on workers. Test idle keepalive/full queue, lifecycle/cancel before/after first, queued result, loop heartbeat and EOF/no late ack. Assert closed false while active and true on each normal/error/aclose terminal path, including idempotent close and queued completion cancellation.
4. Update adapter types, native registration and exact pyi. Production driver hooks in t90 are async _replication_identity(slot,publication) (fresh direct client, closefinally) and _open_replication_stream(config:PostgresChangeConfig,*,start_lsn=None,reconnect=False) for source; hook maps config.slot/publication/queue_capacity to native arguments; generic query pool never owns these sessions. Commit `feat(postgres): expose explicit replication identity and handles`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q
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
