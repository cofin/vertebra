---
type: Task
id: "native-postgres-driver:ch34:t02"
chapter_id: ch34-replication-session-pump
title: "Drain coalesced WAL through bounded single-generation reader"
description: "Wire real protocol reads to pgoutput decoding and bounded FIFO, retaining startup bytes and independent writer liveness. Introduce next returning a delivery lease with complete successful-path record/publish semantics. Cancellation retention, the lease-duration waiter reservation and unacked-capacity gating belong to t03."
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch34:t01"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/stream.rs
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/stream.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 245
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
claimed_by: agent:executor
claimed_at: "2026-11-12T19:56:42.010392+00:00"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T19:56:42.010392+00:00"
generated: {by: flow-revise, at: "2026-11-12T05:00:00Z"}
verified: []
sources:
  - id: contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Commit-fenced replication and cancellation contracts
  - id: wire
    resource: src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
    title: Canonical real-wire scripted PostgreSQL peer tests
tags: [postgres, replication, bounded, tdd]
---

# Drain coalesced WAL through bounded single-generation reader

## Objective & Scope Boundary

Wire real protocol reads to pgoutput decoding and bounded FIFO, retaining startup bytes and independent writer liveness. Introduce next returning a delivery lease with complete successful-path record/publish semantics. Cancellation retention, the lease-duration waiter reservation and unacked-capacity gating belong to t03. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `src/rs/vertebra-postgres/src/replication/stream.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/protocol.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/feedback.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — owned seam or direct collaborator.

The owning chapter spec and referenced domain skills/knowledge are contextual reads. The source read set has 6 files; request a plan revision if additional source exploration is needed.

## Symbol & Signature Contracts

In `replication/mod.rs`, `pub mod stream; pub use stream::{ReplicationSession, ReplicationDelivery};`.

```rust
impl PostgresReplicationConnection {
    pub fn into_stream(self, queue_capacity: usize,
        status_interval: Duration, shutdown_timeout: Duration) -> Result<ReplicationSession, PostgresError>;
}
impl ReplicationSession {
    pub fn identity(&self) -> &VerifiedReplicationIdentity;
    pub async fn next(&self) -> Result<Option<ReplicationDelivery>, PostgresError>;
    pub fn acknowledge(&self, commit_end_lsn: u64) -> Result<u64, PostgresError>;
    pub fn status_positions(&self) -> (u64, u64, u64);
    pub fn is_closed(&self) -> bool;
    pub fn cancel(&self);
    pub async fn shutdown(&self) -> Result<(), PostgresError>;
    pub async fn wait_closed(&self) -> Result<(), PostgresError>;
}
impl ReplicationDelivery {
    pub fn record(&self) -> &ReplicationRecord;
    pub fn publish(self) -> Result<ReplicationRecord, PostgresError>;
}
```

The single-generation owner keeps immutable identity, shared bounded queue/feedback, one receiver lease, cancellation notification and join handles. Only `publish` marks a Commit published; a popped or leased Commit remains unacknowledgeable. Delivery Drop restores a one-slot front record and clears the waiter reservation. The reservation lasts until publish/Drop, including time queued on a Python completion drainer. `shutdown` fences publication and clears session-owned FIFO/front/pending records. A caller-owned delivery lease keeps its one bounded record alive so record() borrows remain valid; that record is released only when the caller drops the lease or attempts publication after closure. No per-record worker tasks or unbounded channel is allowed. Supervisor generations belong to ch35, Python handle construction to ch36.

Reader owns readhalf, startup read_buf, TypeCatalog, CodecContext, type identities and PgOutputDecoder. Always call read_replication_message with the same read_buf before awaiting more socket bytes. Decode w payload incrementally with PgOutputDecoder::push/next_record; metadata updates do not consume queue slots. On decoded Commit call feedback.record_received_commit(endLSN), never use wal_end. Queue uses BoundedReplicationQueue(queue_capacity,8MiB,16MiB). Maintain at most one pending decoded record and at most one bounded input frame/decoder accumulator; drain decoder before reading another frame. Pause when queue full or published-unacked count1024; capacity notifications resume without lost wakeups. A single empty queue admits one <=16MiBrecord even beyond8MiBaggregate. This task implements ReplicationDelivery::record and publish completely for the successful single-caller path: consume the owned record; for Commit call feedback.record_published_commit before returning it; for other records return unchanged. It rejects publication after session closure. Coalesced wire tests call publish on each returned lease before next. No later-task method or placeholder is needed. t03 owns the lease Drop restoration, waiter reservation extending across caller-owned leases, and complete admission coordination; those are not duplicated here.

Parsed keepalive reply_requested signals writer watch; keepalive behind unread WAL during backpressure waits for resume while periodic writer continues. Never parse ahead or promise prompt replies to unread bytes. `next` consumes the next FIFO record into a delivery lease; successful callers publish it before requesting another. t03 adds front restoration and rejects overlapping calls/live leases. On no data await notification without holding synchronous locks; terminal error wakes waiters even when queue full.

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

Protocol corruption and malformedCopyData discard unread pending data and set terminal error; do not acknowledge. TransportEOF while partial transaction becomes terminal transport error (supervisor classifies/reconnects), not graceful success. Backend CopyDone plus clean decoderEOF yields None once queued records drain. Only normal backend CopyDone invokes decoder.finish_clean_eof; socket EOF/reset at a frame boundary maps to Transport even with partial transaction and is retryable by the supervisor. Truncated backend frame EOF and normal CopyDone with partial transaction map to Protocol and are terminal.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Startup coalescence | one write W+k(reply1)+w(Begin+Relation+Insert+Commit) | immediate status with initialflush; next leases yield Begin,Insert,Commit in exact order |
| Full FIFO | capacity4;peer sends many transactions;pause consumer;advance30s | reader retains bounded pendingdata;writer emits3statuses without flush advancement |
| Oversized valid record | single9MiB record then another | first admitted empty;second waits for capacity;max individual16MiB enforced |
| Malformed envelope | k length12 or unknownx | terminal Protocol; awaitingnext wakes; flush unchanged |
| EOF transaction | Begin+Insert then socketEOF | error; no published Commit or gracefulNone |

## Verification

Red: add one canonical `session_reader` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests session_reader
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [ ] R22.4 bounded coalesced frame ingestion and real pgoutput ordering pass.
- [ ] Blocked producer, malformed frame, and EOF all wake correct owners without starving writer.

- [ ] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
