---
type: Task
id: "native-postgres-driver:t71"
title: "Pump notifications on a dedicated bounded session"
description: "Pump notifications on a dedicated bounded session for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t70"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/notifications.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs
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

# Pump notifications on a dedicated bounded session

## Objective

Pump notifications on a dedicated bounded session; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 380 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Script an idle connection delivering NotificationResponse without queries, interleaved subscription ReadyForQuery/notice/status, bytewise frames, duplicate notifications and empty/UTF8 payload. A query-pool-size1 client remains usable while listener is idle. Assert bounded overflow closes listener with explicit OperationalError rather than dropping oldest/newest silently; peer EOF proves owner cleanup.
2. Implement deep `notifications.rs::PostgresListener` owning its own PostgresConnection and receive task, never a query pool lease or transaction-pooler session. Validate channels as single identifiers and quote LISTEN/UNLISTEN names, no value interpolation. LISTEN becomes active only after successful command ReadyForQuery; notify during command responses routes through the same pump, never a second socket reader.
3. Queue defaults1024 frames plus8MiB bytes, configurable positive bounds. Frames hold pid i32/channel UTF8/payload raw UTF8 bytes. Queue overflow terminates with explicit loss error and discards socket; no transparent recovery claim. Separate bounded control channel handles subscription commands without starvation. Cancellation/drop closes socket, wakes blocked receiver and joins task under existing deadline; no Python callback on worker.
4. Empty channel set owns no subscription and remains closable. Duplicated subscribe/unsubscribe idempotent; invalid channels fail before bytes. Commit `feat(postgres): own dedicated notification receive sessions`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R21: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
