---
type: Task
id: "native-postgres-driver:t72"
title: "Expose async notification handles and reconnect gaps"
description: "Expose async notification handles and reconnect gaps for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t71"
files:
  - src/rs/vertebra-postgres/src/notifications.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_native_notifications.py
tests:
  - src/py/tests/unit/db/test_postgres_native_notifications.py
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

# Expose async notification handles and reconnect gaps

## Objective

Expose async notification handles and reconnect gaps; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 410 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Both native clients expose `open_listener(channels,*,queue_capacity=1024,reconnect=False)->Awaitable[AsyncPostgresListener]`; next()->Awaitable[tuple[int,str,bytes,bool]|None], subscribe/unsubscribe(channels)->Awaitable[None], aclose()->Awaitable[None] and readonly closed:bool. closed is an owned terminal-state snapshot with no I/O: false while active or recoverable reconnect, true after normal terminal end, unrecoverable error or aclose; repeated close remains true. Tuple is pid/channel/payload/reconnected_gap. Exact stubtest covers methods/context; tests use scripted native server, never fake notification implementations.
2. Implement bridge through existing completion queue. Only one next is pending; duplicate next raises InterfaceError. Commands acknowledge server ReadyForQuery before resolving. Close cancels receive/control waits, emits None to pending receiver, clears bounded queue and is idempotent. Cancellation during open/next/queued result publication leaves no orphan session or future. Sync client's listener operations still await without blocking loop/GIL.
3. Optional reconnect retries only recoverable transport failures with bounded exponential0.1..5s jitter and max attempts8; authentication/protocol/overflow failures terminate. Retry uses current subscription snapshot and refreshed IAM password/direct endpoint config; resubscribe all channels and await ReadyForQuery before delivery. Notifications are nondurable/at-most-once; next delivered tuple gap=true after reconnection, no synthetic replay or exactly-once claim. Default reconnect=false surfaces disconnect immediately and sets closed=true after terminal owner cleanup. aclose cancels backoff promptly.
4. Queue count/byte caps persist while reconnecting; successful resubscribe does not hide a delivery gap. Test peer drop during idle and during subscribe, stale-generation response rejection, cancellation in backoff and restored channel set. Add canonical test_native_listener_closed_state covering active false, reconnect false-until-terminal, clean end/error/aclose true and repeated close true; exact stub exposes @property closed without setter. Commit `feat(postgres): expose notification handles and reconnect state`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k native_listener -q
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R21: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
