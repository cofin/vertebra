---
type: Task
id: native-postgres-driver:t77
title: Acknowledge only completed durable commit boundaries
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t76
files:
- src/rs/vertebra-postgres/src/replication/feedback.rs
- src/rs/vertebra-postgres/src/replication/mod.rs
- src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs::standby_feedback_never_acknowledges_receive_lsn
- src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs::feedback_survives_full_event_queue
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
---

# Acknowledge only completed durable commit boundaries

## Objective

Acknowledge only completed durable commit boundaries; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 390 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Decode CopyData w(XLogData:wal_start,wal_end,server_time,payload) and k(primary keepalive:wal_end,time,reply_requested); reject short/oversize/unknown tags. Golden feedback r contains write/flush/apply u64, PostgreSQL-epoch timestamp i64 and replyRequested0; assert endian/length. Server wal_end may exceed delivered/processed WAL and must never become ack automatically.
2. `ReplicationFeedback` tracks received_complete_lsn, emitted_commit boundaries and acknowledged_commit_lsn separately. Native acknowledge(commit_end_lsn) accepts only completely emitted known contiguous COMMIT boundaries, monotonic <=delivered, never future/noncommit; duplicates idempotent. Flush/apply use acknowledged boundary only. Write may use received_complete_lsn but may not be inferred as durable. Python source persists checkpoint before invoking native ack(t80).
3. Split transport after CopyBoth into bounded read owner and independently polled write owner. Timer default10s (bounded by configured wal_sender_timeout/3 when nonzero) continues when event queue is full/application stalled; received keepalive reply requests wake feedback immediately. use bounded control state/watch, no per-heartbeat task/queue growth. Reply promptly with current ack, not receive WAL. Tokio paused-clock tests advance time; no sleeps.
4. Record emitted COMMIT boundary only once final event has reached receiver publication, not merely queued worker bytes. Bound pending unacked boundaries1024 and stop further WAL reads when bounded event/commit capacity is exhausted; retain at most one pending frame. A keepalive behind unread WAL is not observable until capacity returns; the independent periodic feedback writer keeps server status live without advancing ack. Never claim a paused reader parses past arbitrary queued WAL without buffering. Cancellation/close sends final current feedback then CopyDone under deadline, never advances ack. Commit `feat(postgres): fence durable replication acknowledgements`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests feedback
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
