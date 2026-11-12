---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch35-replication-supervision
title: Verified replication supervision and generation fencing
description: Own raw stream lifetime, cancellation-safe delivery leases, identity-verified resume, and bounded credential-refresh retries.
status: draft
state: planned
planning_status: complete
depends_on_chapters: [ch34-replication-session-pump]
replan_trigger: null
current_task: "native-postgres-driver:ch35:t01"
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
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
tags: [postgres, replication, reconnect, cancellation]
---

# Verified Replication Supervision and Generation Fencing

<!-- truth: start -->
- The supervisor wraps the reviewed single-generation `ReplicationSession` from ch34; publication remains a delivery-lease operation, and cancelled Python completions cannot publish a commit [^contracts].
- Resume compares server/database/slot/publication identity and WAL availability before emitting `START_REPLICATION`; it uses only the acknowledged commit boundary [^replication].
- `reconnect=false` opens no replacement connection; raw reconnect uses at most eight replacement attempts with fresh established connection credentials and cancellable backoff [^session].
<!-- truth: end -->

## 1. Specification, Context & Provenance

A raw logical stream needs one owner for generation changes and retries. A consumer can hold a queued delivery while the old socket fails; that delivery must become stale before replacement WAL can be acknowledged. The supervisor hides that race behind a lease and an explicit generation on acknowledgements. Python durable checkpoint storage remains in ch24; native Python wrapping remains in ch36. The existing connection starts replication before returning identity, so reconnect adds an identity-checked startup variant [^replication].

Read [contracts](../../contracts.md), [ch34](../ch34-replication-session-pump/spec.md), and the [data knowledge chapter](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md). Apply [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

- R22.5a: Open one dedicated session, reject unsafe pooler modes before dial, return initial control before records, and protect publication/acknowledgement with a generation lease.
- R22.5b: On transport failure resume from acknowledged WAL only after same-connection identity verification; clear old decoder/cache and unacknowledged deliveries.
- R22.5c: Refresh credentials on every dial, use bounded retry, preserve terminal errors, and cancel retries on shutdown.

```rust
pub struct PostgresReplicationResume {
    pub identity: VerifiedReplicationIdentity,
    pub acknowledged_lsn: u64,
    pub generation: u64,
}
pub enum ReplicationStreamEvent {
    Control { identity: VerifiedReplicationIdentity, acknowledged_lsn: u64, generation: u64 },
    Record { record: ReplicationRecord, generation: u64 },
}
pub struct ReplicationSupervisor { /* shared state, active session, reconnect driver */ }
pub struct ReplicationEventDelivery { /* event, session lease, next reservation */ }
impl ReplicationSupervisor {
    pub async fn open(config: PostgresConfig, slot: String, publication: String,
        start_lsn: Option<u64>, queue_capacity: usize, reconnect: bool) -> Result<Self, PostgresError>;
    pub async fn next_event(&self) -> Result<Option<ReplicationEventDelivery>, PostgresError>;
    pub fn resume(&self) -> PostgresReplicationResume;
    pub fn acknowledge(&self, generation: u64, commit_end_lsn: u64) -> Result<u64, PostgresError>;
    pub fn cancel(&self);
    pub async fn shutdown(&self) -> Result<(), PostgresError>;
    pub fn is_closed(&self) -> bool;
}
impl ReplicationEventDelivery {
    pub fn event(&self) -> &ReplicationStreamEvent;
    pub fn publish(self) -> Result<ReplicationStreamEvent, PostgresError>;
}
impl PostgresReplicationConnection {
    pub async fn connect_logical_verified(config: &PostgresConfig, slot: &str,
        publication: &str, start_lsn: u64, expected: &VerifiedReplicationIdentity)
        -> Result<Self, PostgresError>;
}
pub const MAX_RAW_RECONNECT_ATTEMPTS: u32 = 8;
pub fn raw_reconnect_delay(attempt: u32, jitter_seed: u64) -> Duration;
```

`supervisor.rs` owns the public supervisor and lease; `mod.rs` reexports them and owns verified connection startup. `open` validates queue capacity in 1..=1024 (otherwise Interface) and accepts only Direct/Session modes before metadata or replication dial. It initializes acknowledgement to explicit `start_lsn` or verified `confirmed_flush_lsn`, starts the ch34 session, and returns generation0 Control first. `next_event` rejects a second outstanding waiter/lease with Interface, retaining its reservation until delivery publication/drop. `event` borrows owned data for Python conversion. `publish` atomically checks active generation and closed state, then publishes the inner session lease; no fallible conversion follows publication. Dropping a Control lease restores the one front slot; dropping a Record lease restores its inner ch34 delivery. Stale or closed leases discard data and cannot update feedback. This lease owns restoration; no manual `push_front_unconsumed` API is exposed.

`acknowledge(generation,lsn)` holds the supervisor state lock across generation validation and the session acknowledgement. Regressions, unknown boundaries, and stale generations return Interface without mutation. `resume` snapshots owned identity, acknowledgement, and generation under the same lock. No state mutex is held over socket I/O or await. Normal CopyDone after a complete transaction makes `next_event` return None. Frame-boundary TCP EOF/reset is Transport, allowing interrupted-transaction replay from the acknowledged boundary when reconnect=true. Truncated backend/pgoutput frames and normal CopyDone inside an incomplete transaction remain terminal Protocol. Server08xxx connection errors may retry except08P01 protocol_violation, which preserves its SQLSTATE and terminates without replacement attempts. A transport loss signalled by either read or write pump fences the generation immediately, including during a paused consumer; the supervisor driver awaits `ReplicationSession::wait_closed()->Result<(),PostgresError>` independently of `next_event`. `is_closed` stays false during recoverable backoff and becomes true for EOF, terminal errors, exhaustion, or shutdown. `cancel` synchronously fences outstanding leases, sets closed, calls the active `ReplicationSession::cancel`, signals the driver cancellation token, aborts owned supervisor task handles, and releases owned sockets; it needs no current Tokio runtime and performs no network I/O. Final Drop invokes cancel; shutdown additionally joins ownership under connect_timeout and sends bounded final feedback when possible.

`connect_logical_verified` reuses metadata preflight and IDENTIFY_SYSTEM on the actual replacement connection and validates before START: equal system_identifier, database, slot, publication fingerprint; timeline must not decrease (equal or greater accepted); start_lsn >= restart_lsn and confirmed_flush_lsn; existing plugin/database/active-slot checks stay enforced. A mismatch closes the candidate socket and returns Operational with `identity_mismatch` or `wal_unavailable`. It never recreates/changes a slot or skips ahead. A new session receives a fresh decoder and type catalog; no old queued bytes or unacknowledged boundaries survive.

Caller -> seam -> collaborator: Python bridge -> supervisor delivery -> ch34 session lease -> ch23 feedback; reconnect driver -> verified connection startup -> existing standalone IAM/cloud establishment.

| Condition | Outcome |
| --- | --- |
| Transaction/Statement/Auto mode | Interface, zero metadata/replication accepts |
| Generation0 delivery held when socket fails | Generation1 fence invalidates publication and acknowledgement |
| Duplicate current acknowledged LSN | Idempotent success |
| Identity changes or restart exceeds checkpoint | Operational refusal before START, no further retries |
| Queue capacity0 or1025 | Interface before any metadata/replication accepts |
| Server08P01 | Exact SQLSTATE, terminal, zero replacement attempts |
| Reconnect disabled | First transport error closes supervisor, zero replacements |
| Shutdown during backoff | Wake/join immediately without sleeping to next retry |

Alternative: a delivery lease concentrates reservation and restoration in one owner. A separate public pop/publish/push-front protocol leaves cancellation paths responsible for restoring ownership and can release the next reservation too early. Reconnect checks inside startup avoid the race inherent in a separate identity probe followed by an unchecked dial.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/2 files documented]`. Chapter closeout sync updates `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` with raw reconnect ownership and `.agents/bundles/knowledge/domains/data/postgres-limitations.md` with the raw-native versus public-driver boundary. Full user guides remain owned by ch30; ch36 documents Python handles. Sync runs before code/doc review and is not claimed as runtime acceptance.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical test |
| --- | --- | --- | --- |
| R22.5a | t01 | behavior_tdd | postgres_replication_wire_tests.rs supervisor_ |
| R22.5b | t02 | behavior_tdd | postgres_replication_wire_tests.rs reconnect_verified_ |
| R22.5c | t03 | behavior_tdd | postgres_replication_wire_tests.rs reconnect_policy_ |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch35:t01 — Own a generation-fenced single-session stream (tasks/t01-generation-fenced-supervisor.md)
- [ ] native-postgres-driver:ch35:t02 — Verify identity and WAL bounds before resume (tasks/t02-verified-resume.md)
- [ ] native-postgres-driver:ch35:t03 — Bound retries and cancel credential-refresh reconnects (tasks/t03-bounded-reconnect-policy.md)

**Chapter Recovery Checkpoint:** `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests && PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings && git diff --check` exits0. Present-tense knowledge sync and correctness then fresh quality review precede review stamping.

## 6. Current State & Remaining Plan Snapshot

- Plan revision8 at5eb75b2a; three pending tasks await approval and reviewed ch34.
- No production or test implementation is present for this chapter.
- Dependency order is ch23 -> ch34 -> ch35 -> ch36 -> ch24; existing ch25–ch33 retain their scopes.
- Consumer drivers remain on legacy PostgreSQL dependencies until ch27; live/wheel/benchmark acceptance remains ch28/ch29/ch33.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Replication delivery, generation, and durable checkpoint contracts.
[^replication]: `src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection` — Catalog preflight and dedicated replication startup.
[^session]: `src/rs/vertebra-postgres/src/session.rs::establish_standalone_connection` — Existing credential-refresh and connection deadline seam.
