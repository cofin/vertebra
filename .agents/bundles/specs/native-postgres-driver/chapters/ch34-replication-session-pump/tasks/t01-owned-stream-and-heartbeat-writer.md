---
type: Task
id: "native-postgres-driver:ch34:t01"
chapter_id: ch34-replication-session-pump
title: "Transfer CopyBoth ownership and run independent status writer"
description: "Introduce the public ReplicationSession ownership shell, consuming into_stream, identity/status/ack/closed access, timer writer and bounded cancellation shutdown. No record-consumption or placeholder next/publish methods are added in this task; those are owned by t02/t03."
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch23:t02"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/stream.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 230
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 2
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
    title: Commit-fenced replication and cancellation contracts
  - id: wire
    resource: src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
    title: Canonical real-wire scripted PostgreSQL peer tests
tags: [postgres, replication, bounded, tdd]
---

# Transfer CopyBoth ownership and run independent status writer

## Objective & Scope Boundary

Introduce the public ReplicationSession ownership shell, consuming into_stream, identity/status/ack/closed access, timer writer and bounded cancellation shutdown. No record-consumption or placeholder next/publish methods are added in this task; those are owned by t02/t03. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `src/rs/vertebra-postgres/src/replication/mod.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/session.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/feedback.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/protocol.rs` — owned seam or direct collaborator.
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

Add private `effective_start_lsn: u64` to PostgresReplicationConnection. On each successful connect_logical startup, initialize it from the validated requested start_lsn or slot confirmed_flush_lsn when absent, exactly matching the START_REPLICATION request semantics. The session initializes ReplicationFeedback::new from that stored value; into_stream accepts no independent LSN and exposes no extra accessor. Explicit START_LSN must match the stored initial feedback fence; do not accept a second caller-controlled fence that can silently skip processing.

Remove `impl Drop for PostgresReplicationConnection` whose only action is marking its private connection closed. `PostgresConnection` has no Drop; consuming field destructuring safely moves `conn.stream` and `conn.read_buf` with catalog, codec context and type identities into the stream owner without unsafe, replacement/dummy sockets, or prematurely shutting down the moved stream. Directly dropping an unconsumed connection still drops its actual socket. Validate positive queue capacity<=1024 and nonzero intervals/timeouts before split/spawn. Hold read half and exact startup read_buf for t02.

Split via tokio::io::split(ConnectionStream). The writer owns write half, watch channel for coalesced status/reply changes, interval and cancel branch. Use fixed34-byte status payload plus5-byte frontend header; no per-heartbeat allocation/spawn. First periodic tick is interval after creation, not immediate. Shutdown sends final current status then CopyDone under shutdown_timeout and joins writer; t04 adds backend response drain.

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

Invalid queue>1024/zero or zero timeout/interval: Interface before tasks spawn. Drop of session cancels/aborts workers, never blocks. Writer error closes session and wakes receiver; session cannot report healthy after transport failure.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Bound initial fence | connect_logical with explicit startLSN0x200000100, then into_stream with no LSN parameter | initial status positions exactly0x200000100; no caller parameter can advance the fence |
| Default initial fence | connect_logical with start_lsnNone and slot confirmed_flush_lsn0x200000080 | initial status positions exactly0x200000080, matching effective START semantics |
| Idle writer | open real CopyBoth connection,startLSN0x200000100,interval10s;advance30s | 3periodic status packets,flush/apply initial |
| Ownership transfer | transfer connection with coalesced W+k bytes in read_buf | no Terminate/CopyDone before explicit shutdown; immutable identity retained |
| Ack refusal | acknowledge0x200000500 before publication | Interface; wire flush/apply unchanged |
| Basic close | shutdown twice | finalstatus then CopyDone on firstcall; both calls finish within timeout; closedtrue |

## Verification

Red: add one canonical `session_writer` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests session_writer
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [ ] R22.4 independent periodic writer works without reader/consumer tasks.
- [ ] Safe consuming transfer preserves all buffered bytes and ownership on every error path.

- [ ] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
