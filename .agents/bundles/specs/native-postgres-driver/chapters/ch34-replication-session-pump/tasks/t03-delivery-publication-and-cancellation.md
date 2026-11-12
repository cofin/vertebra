---
type: Task
id: "native-postgres-driver:ch34:t03"
chapter_id: ch34-replication-session-pump
title: "Publish Commit leases exactly at consumer delivery"
description: "Harden successful ReplicationDelivery publication with lease-duration single-next ownership and cancellation retention; publication capacity notifications coordinate reader and ack writer. No reconnect or Python completion code belongs here."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch34:t02"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/stream.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/stream.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 195
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
claimed_by: agent:executor
claimed_at: "2026-11-12T19:57:19.194997+00:00"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T20:05:41.175938+00:00"
last_verified_commit: 9d4c4258
verification_evidence: "Red exit101 held lease overlap; focused4/full39 pass; Clippy/packagefmt/diffcheck exit0. Logs /tmp/ch34-t03-{red,green,module,clippy}.log"
commit: 9d4c4258
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T20:05:41.175938+00:00"
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

# Publish Commit leases exactly at consumer delivery

## Objective & Scope Boundary

Harden successful ReplicationDelivery publication with lease-duration single-next ownership and cancellation retention; publication capacity notifications coordinate reader and ack writer. No reconnect or Python completion code belongs here. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `src/rs/vertebra-postgres/src/replication/stream.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/feedback.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs` — owned seam or direct collaborator.
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

The normal record/publish path is implemented and tested in t02. This task adds the lease-duration reservation, cancellation restoration, and unacked-capacity coordination without adding an unimplemented public method.

`next` reserves its single waiter before awaiting and transfers that reservation to a returned lease. Cancellation before dequeue releases it; dropping a returned lease stores the exact record in one front slot under shared state lock, before releasing reservation. No subsequent next can overtake. `publish(self)` locks session state, rejects closed owner, marks a Commit with record_published_commit then consumes the lease and returns owned record; nonCommit publication only consumes lease. Unknown or stale publication cannot modify feedback. A Commit is not leased while unacked_count1024; `acknowledge` signals capacity and writer once successful. `record()` only borrows. Session never makes checkpoint durability claims. Close cannot reclaim a record currently owned by an external lease: it invalidates publication, while the lease retains its at-most16MiB record until Drop or failed publication consumes it.

No user callback runs under shared lock. Publication failure restores lease through Drop if active. Duplicate publication is prevented by consuming self. Pending lease lifetime remains bounded to one; retainedfront+FIFO+onependingrecord remain separate explicit bounds.

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

`next` while activewaiter or lease returns Interface; acknowledgement of leased/dropped/unpublished Commit fails. Closed-session publish returns Interface and discards rather than restoring. Mutex/notification lock scope must preclude missed wakeups and deadlocks.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Cancel pending waiter | next awaits on empty;cancel;sendBegin | newnext gets Begin; no stuck waiter |
| Cancel leased Commit | leaseCommit0x200000300;drop;peer sendsBeginxid9;ack300 | ackfails;next returns sameCommit then Begin9;publishCommit then ack300 succeeds |
| Concurrent lease | holdCommit lease;call next | Interface; no next-record theft |
| Published cap | 1024published commits unacked;nextCommit waits;acklast | ackfreescapacity;next resumes;writer uses acknowledgedend |
| Close with lease | holdCommit;shutdown;publish | Interface; zero final ack advancement, no retained record |

## Verification

Red: add one canonical `session_delivery` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests session_delivery
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [x] R22.4 publication requires consuming a live delivery lease.
- [x] Cancellation returns exact event once in FIFO order without pre-publication acknowledgement.

- [x] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
