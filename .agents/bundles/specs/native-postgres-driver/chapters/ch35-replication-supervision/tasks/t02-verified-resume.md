---
type: Task
id: "native-postgres-driver:ch35:t02"
chapter_id: ch35-replication-supervision
title: Verify identity and WAL bounds before resume
description: Verify identity and WAL bounds before resume through the public native replication supervisor seam.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch35:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
  - src/rs/vertebra-postgres/src/session.rs
files:
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 245
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

# Verify identity and WAL bounds before resume

## Objective & Scope Boundary

Add `PostgresReplicationConnection::connect_logical_verified` in `mod.rs` and reconnect composition in `supervisor.rs`. Before START on the replacement connection compare identity and WAL bounds, then create a fresh single-generation session at the durable acknowledgement. t02 attempts one immediate replacement on transient transport failure when reconnect=true; t03 replaces that scheduling with bounded cancellable retry. Preserve t01 reconnect=false behavior.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md`
- `src/rs/vertebra-postgres/src/replication/mod.rs`
- `src/rs/vertebra-postgres/src/replication/supervisor.rs`
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`
- `src/rs/vertebra-postgres/src/session.rs`

`stream.rs` and `supervisor.rs` are planned modules created by reviewed dependencies/current task, not missing legacy symbols. Parent [spec](../spec.md) defines shared signatures. Apply referenced runtime skill/CDC knowledge; no broader source exploration is needed.

## Symbol & Signature Contracts

- `connect_logical_verified(config:&PostgresConfig,slot:&str,publication:&str,start_lsn:u64,expected:&VerifiedReplicationIdentity)->async Result<Self,PostgresError>` shares connect preflight, IDENTIFY_SYSTEM, type preload, and START code with `connect_logical`; identity comparison runs after IDENTIFY_SYSTEM and before START.
- On disconnect advance generation under state lock before detaching the dead session, invalidate all old leases, snapshot acknowledged LSN, clear control frontslot, close old session, establish replacement with exact snapshot. Checked generation overflow fails terminal Operational rather than wrapping.
- Match system_identifier/database/slot/publication; allow only same/increased timeline. Require start>=restart and confirmed_flush; never use wal_end. Candidate rejection closes socket and issues no START. Preserve errors from catalog validation.
- Fresh replacement becomes active atomically and queues Control before any Record; downstream acknowledgements require current generation. Old pending record guards only hold the retired session and cannot restore into replacement.

Caller -> seam -> collaborator: canonical scripted TCP peer -> public supervisor -> session delivery/verified startup -> PostgreSQL protocol and credential establishment [^replication] [^session].

## Edge Cases & Error Semantics

Generation/closed checks and acknowledgement updates share one state lock; no mutex spans await. No late guard can publish or restore into a new session. Error matching preserves `PostgresError` category and SQLSTATE [^contracts]. Shutdown is idempotent, queued delivery cannot advance feedback, and no slot creation/drop occurs. Queue limits remain ch34's record/byte bounds; only one outstanding delivery is retained. Normal CopyDone with a complete transaction returns None. A frame-boundary TCP EOF/reset is Transport and permits replay of an interrupted transaction from its acknowledged boundary when reconnect=true. A truncated backend/pgoutput frame, or normal CopyDone inside an incomplete transaction, is terminal Protocol; it never triggers retry. Queue capacity must be in 1..=1024; invalid capacity returns Interface before any metadata or replication dial.

## Public-Seam Test Blueprint

Canonical test: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned protocol peer is a real local TCP listener, never a mocked supervisor/session. Reuse existing catalog/auth/startup frame helpers, count metadata and replication connections separately, and use Tokio paused time/advance with explicit peer barriers (no real sleeps).

| Case | Input | Expected output/error |
| --- | --- | --- |
| Durable resume | Ack1/200; publish1/300 without ack; drop socket | generation1 Control(1/200), START contains `1/200` |
| Cross-generation lease | Hold gen0 lease during reset, publish after gen1 | Interface and gen1 feedback unchanged |
| Identity mismatch | system identifier `other` on replacement | Operational identity_mismatch, no START |
| WAL gap | restart1/500 vs ack1/200 | Operational wal_unavailable, no START |
| Advanced slot | confirmed_flush1/250 vs ack1/200 | Operational wal_unavailable, no skipping |
| Timeline | matching identity timeline2 after1 | Resume succeeds; timeline0 is refused |
| Partial transaction | Disconnect after Begin/Insert | New relation cache, replay from ack, no old uncommitted records |

## Verification

Red: add one scenario at the public seam then run `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests reconnect_verified_` and record the expected failure before production edits.

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
