---
type: Task
id: "native-postgres-driver:ch35:t01"
chapter_id: ch35-replication-supervision
title: Own a generation-fenced single-session stream
description: Own a generation-fenced single-session stream through the public native replication supervisor seam.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/stream.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 235
plan_revision: 8
plan_commit: 5eb75b2a
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
updated_at: "2026-11-12T05:00:00Z"
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

# Own a generation-fenced single-session stream

## Objective & Scope Boundary

Implement `ReplicationSupervisor`, resume/event/delivery types, `open`, `next_event`, `resume`, `acknowledge`, `cancel`, `shutdown`, and `is_closed` from the parent contract. Add `supervisor.rs` and reexport in `mod.rs`. Use a shared driver state that observes ch34 terminal notifications independently of consumer polling; retain the waiter reservation in a lease across queued completion. This task supports `reconnect=false`; for `reconnect=true`, transport loss returns its original terminal error until t02 installs resume. The final chapter contract is exercised after all three tasks; no replacement connection is attempted in t01.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md`
- `src/rs/vertebra-postgres/src/replication/mod.rs`
- `src/rs/vertebra-postgres/src/replication/stream.rs`
- `src/rs/vertebra-postgres/src/replication/supervisor.rs`
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`

`stream.rs` and `supervisor.rs` are planned modules created by reviewed dependencies/current task, not missing legacy symbols. Parent [spec](../spec.md) defines shared signatures. Apply referenced runtime skill/CDC knowledge; no broader source exploration is needed.

## Symbol & Signature Contracts

- `open` first rejects queue capacity outside 1..=1024 with Interface before any metadata or replication dial, then uses the existing `connect_logical`, then `into_stream(queue_capacity, status_interval, config.connect_timeout)` with the configured queue capacity and parameter-derived interval. The connection retains its validated `effective_start_lsn` from successful START; the session initializes feedback exclusively from that stored fence, without an independently supplied initial LSN.
- `ReplicationEventDelivery::event` borrows an owned Control or Record. It retains the inner session lease and shared next reservation; `publish` validates generation while holding state lock, invokes inner publish, and returns the event. Drop restores exactly once or discards after close/fence.
- `cancel(&self)` synchronously fences leases, calls the active `ReplicationSession::cancel`, and cancels/aborts owned supervisor tasks, with no runtime lookup or network I/O. The public `wait_closed` ch34 notification wakes the driver without a consumer poll. `shutdown` cancels the supervisor driver and active session, wakes blocked next waiters, invalidates outstanding leases and joins; Drop only cancels/aborts.
- Public symbol signatures are exactly the parent section2 contract; no public manual front-buffer mutation method is added.

Caller -> seam -> collaborator: canonical scripted TCP peer -> public supervisor -> session delivery/verified startup -> PostgreSQL protocol and credential establishment [^replication] [^session].

## Edge Cases & Error Semantics

Generation/closed checks and acknowledgement updates share one state lock; no mutex spans await. No late guard can publish or restore into a new session. Error matching preserves `PostgresError` category and SQLSTATE [^contracts]. Shutdown is idempotent, queued delivery cannot advance feedback, and no slot creation/drop occurs. Queue limits remain ch34's record/byte bounds; only one outstanding delivery is retained. Normal CopyDone with a complete transaction returns None. A frame-boundary TCP EOF/reset is Transport and permits replay of an interrupted transaction from its acknowledged boundary when reconnect=true. A truncated backend/pgoutput frame, or normal CopyDone inside an incomplete transaction, is terminal Protocol; it never triggers retry. Queue capacity must be in 1..=1024; invalid capacity returns Interface before any metadata or replication dial.

## Public-Seam Test Blueprint

Canonical test: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned protocol peer is a real local TCP listener, never a mocked supervisor/session. Reuse existing catalog/auth/startup frame helpers, count metadata and replication connections separately, and use Tokio paused time/advance with explicit peer barriers (no real sleeps).

| Case | Input | Expected output/error |
| --- | --- | --- |
| Control ordering | Open slot `cdc_slot`, receive Begin immediately | Control generation0 precedes Begin |
| Cancelled lease | Drop first Record lease, call next_event again | Same record coordinate appears once, ack still fails until publish |
| Overlap | Hold one lease, call next_event | Interface, first lease still publishable |
| Pooler refusal | Transaction/Statement/Auto | Interface with zero accepts |
| Single connection lifetime | reconnect=false, peer reset | Transport error, closed=true, no replacement accepts |
| Close pending lease | Obtain lease then shutdown then publish | Interface, no feedback advancement |

## Verification

Red: add one scenario at the public seam then run `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests supervisor_` and record the expected failure before production edits.

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
