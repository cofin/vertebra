---
type: Task
id: "native-postgres-driver:ch35:t03"
chapter_id: ch35-replication-supervision
title: Bound retries and cancel credential-refresh reconnects
description: Bound retries and cancel credential-refresh reconnects through the public native replication supervisor seam.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch35:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/session.rs
files:
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 220
plan_revision: 8
plan_commit: d90d4625
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T04:25:15Z"
generated: {by: flow-revise, at: "2026-11-12T05:00:00Z"}
verified: []
sources:
  - id: contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Replication generation, durability, and native handle contracts
  - id: replication
    resource: src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection
    title: Standalone catalog preflight and CopyBoth connection startup
  - id: session
    resource: src/rs/vertebra-postgres/src/session.rs::establish_standalone_connection
    title: Cloud and direct IAM credential refresh on physical connection establishment
tags: [postgres, replication, supervisor]
---

# Bound retries and cancel credential-refresh reconnects

## Objective & Scope Boundary

Complete raw reconnect policy in `supervisor.rs`: bounded seeded jitter, at most eight replacement attempts per disconnect, credential refresh through each existing physical connection establishment, terminal classification, and cancellation during connection/backoff. Keep retry mechanism private except its deterministic delay policy function; exercise policy and wire interactions through supervisor public methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md`
- `src/rs/vertebra-postgres/src/replication/mod.rs`
- `src/rs/vertebra-postgres/src/replication/supervisor.rs`
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`
- `src/rs/vertebra-postgres/src/errors.rs`
- `src/rs/vertebra-postgres/src/session.rs::establish_standalone_connection`

`stream.rs` and `supervisor.rs` are planned modules created by reviewed dependencies/current task, not missing legacy symbols. Parent [spec](../spec.md) defines shared signatures. Apply referenced runtime skill/CDC knowledge; no broader source exploration is needed.

## Symbol & Signature Contracts

- `MAX_RAW_RECONNECT_ATTEMPTS:u32=8`; `raw_reconnect_delay(attempt:u32,jitter_seed:u64)->Duration` accepts zero-based attempt, base=min(100ms*2^min(attempt,6),5000ms), adds deterministic nonnegative jitter up to20percent then clamps5000ms. Seed is stable hash of system_identifier/slot/generation; same input yields same delay. No async tasks or heap allocation per heartbeat/retry delay.
- Initial open has one attempt and preserves errors; retry policy applies only after an established session drops. Each retry awaits the delay with shutdown cancellation, then calls connect_logical_verified using the preserved config; that path calls existing establish_standalone_connection and re-resolves token/cloud material. Eight failed replacement attempts return the last original error, with no ninth dial.
- Retry only Transport errors (EOF/reset/timeout) and server08xxx connection errors excluding `08P01` (protocol_violation). Auth/UnsupportedAuth,28xxx,42501,Protocol,Config,Interface,identity_mismatch,wal_unavailable and all other errors are terminal. Preserve SQLSTATE; do not use broad is_transient policy that includes unrelated query failures.
- is_closed=false during backoff. Cancellation of next alone drops its waiter but not the driver or raw stream. shutdown cancellation wakes delay or cancels in-flight candidate startup, drops its sockets and joins under connect_timeout. Reset attempt counter only after a replacement completes verified START and activates the new session.

Caller -> seam -> collaborator: canonical scripted TCP peer -> public supervisor -> session delivery/verified startup -> PostgreSQL protocol and credential establishment [^replication] [^session].

## Edge Cases & Error Semantics

Generation/closed checks and acknowledgement updates share one state lock; no mutex spans await. No late guard can publish or restore into a new session. Error matching preserves `PostgresError` category and SQLSTATE [^contracts]. Shutdown is idempotent, queued delivery cannot advance feedback, and no slot creation/drop occurs. Queue limits remain ch34's record/byte bounds; only one outstanding delivery is retained. Normal CopyDone with a complete transaction returns None. A frame-boundary TCP EOF/reset is Transport and permits replay of an interrupted transaction from its acknowledged boundary when reconnect=true. A truncated backend/pgoutput frame, or normal CopyDone inside an incomplete transaction, is terminal Protocol; it never triggers retry. Queue capacity must be in 1..=1024; invalid capacity returns Interface before any metadata or replication dial.

## Public-Seam Test Blueprint

Canonical test: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned protocol peer is a real local TCP listener, never a mocked supervisor/session. Reuse existing catalog/auth/startup frame helpers, count metadata and replication connections separately, and use Tokio paused time/advance with explicit peer barriers (no real sleeps).

| Case | Input | Expected output/error |
| --- | --- | --- |
| Bounded delay | attempts0..9 and seeds0/1/99 | Always100..5000ms; same input identical; cap5000ms |
| Exhaustion | Eight reset replacement peers | Eight attempts, original last Transport, no ninth dial |
| Auth terminal | Replacement responds28P01 | Exact SQLSTATE, no subsequent attempt |
| Malformed WAL | Unknown pgoutput tag | Protocol terminal, zero replacement accepts |
| Server protocol violation | Peer sends ErrorResponse SQLSTATE08P01 | Exact08P01 preserved, terminal, zero replacement attempts |
| Credential refresh | Fake IAM token endpoint expires tokenA before disconnect | Existing refresh path obtains tokenB; replacement startup uses B |
| Backoff cancel | Paused clock, shutdown before delay completes | Closed and joined without advancing5seconds |
| No-reconnect | reconnect=false with reset | Zero replacement accepts regardless retry budget |

## Verification

Red: add one scenario at the public seam then run `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests reconnect_policy_` and record the expected failure before production edits.

Green: same focused command exits0, then full `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests`, `PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`, and `git diff --check` exit0. Record focused test count so a zero-selected filter cannot pass unnoticed.

## Acceptance Criteria

- [ ] Parent R22.5 behavior owned by this task matches the concrete signatures and matrix.
- [ ] Real peer assertions cover the positive behavior and refusal/cancellation paths without mocking the SUT.
- [ ] Verification exits0 and task/checklist state is synchronized in one signed local commit.

## Notes & Discoveries

- Publication ownership crosses the async completion boundary via a lease; raw reconnect never accesses Python checkpoint persistence.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Replication delivery, generation, and durable checkpoint contracts.
[^replication]: `src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection` — Catalog preflight and dedicated replication startup.
[^session]: `src/rs/vertebra-postgres/src/session.rs::establish_standalone_connection` — Existing credential-refresh and connection deadline seam.
