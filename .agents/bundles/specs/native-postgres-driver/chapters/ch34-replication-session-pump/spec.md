---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch34-replication-session-pump
title: "Chapter 34: Single-Generation Replication Session"
description: "Own a split CopyBoth stream with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown."
status: stable
state: active
planning_status: complete
depends_on_chapters: ["ch23-replication-feedback-resume-and-bridge"]
replan_trigger: null
current_task: "native-postgres-driver:ch34:t04"
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
chapter_base_commit: f58aa695
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T20:05:41.205447+00:00"
generated: {by: flow-revise, at: "2026-11-12T05:00:00Z"}
verified: []
sources:
  - id: contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Replication ownership and acknowledgement contracts
  - id: connection
    resource: src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection
    title: Dedicated CopyBoth session ownership
  - id: protocol
    resource: src/rs/vertebra-postgres/src/protocol.rs::read_replication_message
    title: Buffered bounded PostgreSQL protocol frames
  - id: decoder
    resource: src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder
    title: Incremental pgoutput transaction records
tags: [postgres, replication, bounded]
---

# Chapter 34: Single-Generation Replication Session

<!-- truth: start -->
- Own a split CopyBoth stream with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown. [^contracts]
- Feedback never promotes server wal_end or payload byte lengths into acknowledged durability. [^contracts]
- Every transport task has one owner and bounded cancellation; no Python objects or checkpoint-store calls belong to this chapter. [^connection]
<!-- truth: end -->

## 1. Specification, Context & Provenance

Own a split CopyBoth stream with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown.

The dedicated `PostgresReplicationConnection` validates slots/publications and owns the CopyBoth socket plus coalesced `read_buf`. `PgOutputDecoder` emits incrementally bounded transaction records and `BoundedReplicationQueue` supplies record/byte limits. The plan uses those concrete seams; it defines all new APIs before their isolated executor tasks. [^connection] [^protocol] [^decoder]

Parent: [full roadmap](../../spec.md) and [contracts](../../contracts.md). Domain context: [CDC](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [FFI discipline](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [runtime skill](../../../../../skills/runtime-ipc/SKILL.md), [PyO3 skill](../../../../../skills/pyo3-bridge/SKILL.md). Source anchors are current symbols; new modules in worksheets are intentional creation targets.

Non-goals: verified reconnect/generation transitions (ch35), Python handles (ch36), durable checkpoint CAS (ch24), driver activation (ch27), and live/wheel/release acceptance (ch28–ch33).

## 2. Requirements, Concrete Contracts & Architecture

- R22.4c (`t01`): consuming safe connection transfer and independent timer/status writer.
- R22.4d (`t02`): bounded buffered read/decode pump preserving coalesced startup data, queue backpressure and complete normal delivery publication.
- R22.4e (`t03`): lease-duration single-waiter reservation, capacity gating and cancellation retention around the existing normal publication path.
- R22.4f (`t04`): one-deadline graceful/terminal shutdown and all-task/socket cleanup.

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

### Internal coordination contract

`ReplicationSession` uses an `Arc<SessionShared>` state owner and `Arc<VerifiedReplicationIdentity>`. `SessionShared` privately owns one synchronized state containing `BoundedReplicationQueue`, `ReplicationFeedback`, front-slot `Option<ReplicationRecord>`, single waiter/lease reservation, closing/closed flags and first terminal error. It also owns `Notify` signals for queue availability/capacity and cancellation, plus a `watch` status revision channel. Never hold its synchronous lock across await. Reader, writer, lease and caller use the same state to make capacity/publication/close transitions atomic. Register Notify wait before rechecking predicate. Reader owns TypeCatalog/codec/decoder state exclusively; caller never touches those mutable structures.

The connection stores private effective_start_lsn at successful START from the validated explicit requested start_lsn, or confirmed_flush_lsn when absent. into_stream initializes feedback exclusively from this stored fence and accepts no independent LSN.

The immutable identity is captured before consuming `PostgresReplicationConnection`. Remove its redundant mark-only Drop before safe destructuring; PostgresConnection has no Drop. Ordinary abandoned unsplit connections still close by dropping their socket. Private writer/read owners use `tokio::io::{ReadHalf, WriteHalf}<ConnectionStream>`, where ConnectionStream is the existing session transport type. Preserve exact read_buf alongside readhalf and own both join/abort handles. The writer accepts a shared status revision and cancellation signal; it uses received Commit ends and the acknowledged fence, never wal_end.

Admission bounds are queue_capacity1..1024,8MiB aggregate queue,16MiB individual record,4096relations, at most one inputframe, one pending decoded record, one front-slot and one live delivery. Empty queue may admit one individually valid record beyond aggregate bytecap. Queue memory does not become transaction-sized. Received and published are independent; no pending unacked1025thCommit is leased. Every blocked wait selects cancellation and terminal state.

Status interval comes from `compute_status_interval`; caller supplies the computed value explicitly because wal_sender_timeout is not guaranteed to be a ParameterStatus. Default10s applies when unavailable. A parsed reply-requested keepalive wakes writer immediately; an unread keepalive behind backpressured WAL is serviced on resume while periodic statuses continue.


Caller → public feedback/session API → strict frame codec, PgOutputDecoder and real dedicated socket; no pool/query socket reuse. The generation supervisor composes this chapter's API after completion, so tests never depend on a downstream supervisor.

| Boundary / failure | Required result |
| --- | --- |
| W+k+w coalesced in startup readbuf | Drain preserved buffered bytes before another socket read; exact record order. |
| Slow/no consumer | Read pauses at record or bytecapacity; writer timer still fires with unchanged ack. |
| Cancelled pending/leased next | Single reservation released; leased record restored ahead of FIFO without publication. |
| Malformed data or writerfailure while FIFO full | First terminalerror stored outside FIFO; wake/cancel all owners. |
| Close with unacknowledged Commit | Final status uses currentack then CopyDone; one deadline bounds drain+join. |
| Socket EOF midtransaction | Terminal transporterror; supervisor owns replay, session never returns clean EOF for partial transaction. |


| Interface decision | Chosen design | Alternative and tradeoff |
| --- | --- | --- |
| Feedback boundary | Explicit received, published, acknowledged stages | Echoing server WAL overstates processing and loses replay safety. |
| Public test surface | Small independently callable feedback/session API | Testing only through a later supervisor inverts task dependencies and hides lifecycle bugs. |
| Socket ownership | Consuming transfer and split-owned loops | Unsafe extraction or dummy socket replacement adds ownership failure paths. |
| Cancellation | Owned single-receiver delivery lease | A bare popped record plus external publication lets cancelled Python completions lose or prematurely publish records. |

Acceptance scenarios are the literal Worked Test Matrices in the task blueprints; each has its own public seam, Red command, Green module suite and Clippy gate.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/2 files documented]` at chapter sync. The isolated sync subagent updates these living files with implementation-backed truth before code and fresh quality review:

- [ ] `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Describe single-generation transport support and remaining reconnect/Python/cutover limitations.
- [ ] `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Document native event leases, feedback fences, queue bounds and shutdown ownership.


The sync owns these docs collectively, independent of task production limits. Full user API guides, packaging and release documentation retain ch30–ch32 ownership; interim knowledge explicitly describes the native/legacy cutover boundary.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical verification |
| --- | --- | --- | --- |
| R22.4 | [native-postgres-driver:ch34:t01](tasks/t01-owned-stream-and-heartbeat-writer.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |
| R22.4 | [native-postgres-driver:ch34:t02](tasks/t02-bounded-read-pump.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |
| R22.4 | [native-postgres-driver:ch34:t03](tasks/t03-delivery-publication-and-cancellation.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |
| R22.4 | [native-postgres-driver:ch34:t04](tasks/t04-bounded-session-shutdown.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |

| Living knowledge accuracy | Chapter sync before review | documentation_validation | source links, Markdown/frontmatter, review-check |

## 5. Implementation Plan

- [x] [f520aa6d] native-postgres-driver:ch34:t01 — Transfer CopyBoth ownership and run independent status writer (tasks/t01-owned-stream-and-heartbeat-writer.md)
- [x] [d99caddf] native-postgres-driver:ch34:t02 — Drain coalesced WAL through bounded single-generation reader (tasks/t02-bounded-read-pump.md)
- [x] [9d4c4258] native-postgres-driver:ch34:t03 — Publish Commit leases exactly at consumer delivery (tasks/t03-delivery-publication-and-cancellation.md)
- [~] native-postgres-driver:ch34:t04 — Join replication owners and drain CopyDone under one deadline (tasks/t04-bounded-session-shutdown.md)


One executor per worksheet, in dependency order. No shared-file task executors run in parallel.

**Chapter Recovery Checkpoint:**

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
UV_NO_SYNC=1 make review-check
git diff --check
```

All commands exit0 before present-tense documentation sync, zero-context correctness review, fresh quality review and `last_reviewed_commit` closeout. Chapter completion does not imply live PostgreSQL or wheel acceptance.

## 6. Current State & Remaining Plan Snapshot

- Plan revision 8 is approved at reviewed plan commit `d90d4625`; execution requires the declared chapter dependencies and task preflight.
- All 4 worksheets are pending, signature-complete and individually bounded; no production changes are present.
- Root order continues ch35 supervision → ch36 Python bridge → ch24 checkpoints, followed by ch25–ch33. This chapter introduces no reconnect behavior or Python public API.
- Doc/OKF manifest is open until chapter implementation sync; native public Python driver cutover remains ch27.
- Stop after this chapter's recovery checkpoint, living documentation sync and independent code/quality review.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Replication ownership and acknowledgement contracts.
[^connection]: `src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection` — Dedicated CopyBoth session ownership.
[^protocol]: `src/rs/vertebra-postgres/src/protocol.rs::read_replication_message` — Buffered bounded protocol frame parsing.
[^decoder]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental bounded pgoutput record decoding.
