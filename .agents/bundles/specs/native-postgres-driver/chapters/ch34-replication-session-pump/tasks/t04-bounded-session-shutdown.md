---
type: Task
id: "native-postgres-driver:ch34:t04"
chapter_id: ch34-replication-session-pump
title: "Join replication owners and drain CopyDone under one deadline"
description: "Finish graceful and terminal session lifecycle: bounded final feedback/CopyDone plus backend drain, receiver wakeup, and cancellation-safe cleanup of split socket owners and delivery leases."
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch34:t03"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/stream.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/stream.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 235
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
claimed_by: agent:executor
claimed_at: "2026-11-12T20:05:41.205447+00:00"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T20:05:41.205447+00:00"
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

# Join replication owners and drain CopyDone under one deadline

## Objective & Scope Boundary

Finish graceful and terminal session lifecycle: bounded final feedback/CopyDone plus backend drain, receiver wakeup, and cancellation-safe cleanup of split socket owners and delivery leases. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `src/rs/vertebra-postgres/src/replication/stream.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/mod.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/protocol.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — owned seam or direct collaborator.

The owning chapter spec and referenced domain skills/knowledge are contextual reads. The source read set has 5 files; request a plan revision if additional source exploration is needed.

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

`cancel` is synchronous and idempotent: under the session state lock it sets closed/closing, invalidates held delivery leases, clears session-owned FIFO/front/pending records and wakes all waiters, then aborts both worker owners without waiting. It performs no graceful status write or new acknowledgement. A held lease cannot delay cancellation and its later Drop discards; publish returns Interface. The supervisor uses cancel to fence a generation before replacing its session.

`wait_closed` is a cancel-safe, multiple-waiter terminal notification independent of next/lease ownership. It returns Ok after clean completion and the mapped original terminal error after failure; callers clone error fields into a fresh same-category error if PostgresError is not Clone. The supervisor uses it to fence held leases immediately on socket loss.

`shutdown` atomically marks closing and wakes read/write/next/capacity waiters. Use a single absolute deadline now+shutdown_timeout for final status, frontendCopyDone, backend CopyDone+CommandComplete+ReadyForQuery drain, transport shutdown and join; never restart budget between phases. Transfer readhalf/read_buf from decoder loop to shutdown drain only after cancelling reader wait; buffered frames remain available. Writer sends exactly one finalstatus, never promoting decoded/published-unacked Commit. Concurrent shutdown callers observe same terminal outcome; repeated completed shutdown is Ok. Worker errors record firstterminalerror outside FIFO, cancel companion owner and wake all waiters. Read/next after terminal error returns originalerror once then None; is_closed true. Explicit close fences publication on every outstanding lease. Externally owned leases retain their one bounded record until dropped or consumed by a failed publish attempt, preserving any record() borrow; shutdown does not claim to reclaim caller-owned bytes. Drop aborts/cancels owners synchronously; no detached task indefinitely owns a socket.

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

If peer omits CopyDone/ReadyForQuery, timeout returns Operational, discards socket and joins/aborts workers within same budget. Backend ErrorResponse preserves mapped SQLSTATE and closes. Alreadyclosed aclose is idempotent. A client cancellation during shutdown does not strand owned tasks: cancellation signals are set before first await and Drop guards own abort handles.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Graceful fence | publishCommit500 without ack;shutdown;peer acceptsCopyDone | finalr reportspriorack;thenCopyDone;backenddrain cleanEOF;closedtrue |
| Unresponsive peer | peer never repliesCopyDone;shutdown_timeout100ms;advance100ms | Operational timeout; socketEOF; no active task/socket remains |
| Blocked producer and lease | fullFIFO+blockedreader+heldlease;shutdown | allwake;leasepublishfails;noqueue/frontretention;held lease retains one bounded record until consumed |
| Synchronous cancellation with held lease | holdCommit500 lease;cancel;publish;callcancelagain | cancel returns immediately,closedtrue,publish Interface,session-owned front/FIFO/pending cleared,held record borrow stays valid until lease consumed,workers aborted,flush never500;wait_closed resolves |
| Concurrent close | two shutdown callers;then third | one wireCopyDone sequence; all converge terminal |
| Backend error | ErrorResponse42501 duringdrain | SQLSTATE42501 preserved;no feedbackadvance;closedtrue |

## Verification

Red: add one canonical `session_shutdown` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests session_shutdown
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [ ] R22.4 close uses one bounded deadline and never advances unacknowledged Commit.
- [ ] Idle, backpressured, cancelled and failed owners relinquish every socket and join handle.

- [ ] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
