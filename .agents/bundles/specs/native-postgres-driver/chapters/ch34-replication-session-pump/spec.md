---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch34-replication-session-pump
title: "Chapter 34: Single-Generation Replication Session"
description: "Own a split CopyBoth stream with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown."
status: stable
state: completed
planning_status: complete
depends_on_chapters: ["ch23-replication-feedback-resume-and-bridge"]
replan_trigger: null
current_task: null
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
chapter_base_commit: f58aa695
last_verified_checkpoint: 6e4e6bfc
last_reviewed_commit: 6e4e6bfc
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T21:09:21.836151+00:00"
generated: {by: flow-revise, at: "2026-11-12T05:00:00Z"}
verified: []
sources:
  - id: stream
    resource: src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession
    title: Bounded session transport and publication leases
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
- `ReplicationSession` owns split CopyBoth transport with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown. [^stream]
- Feedback never promotes server wal_end or payload byte lengths into acknowledged durability. [^contracts]
- Every transport task has one owner and bounded cancellation; no Python objects or checkpoint-store calls belong to this chapter. [^connection]
<!-- truth: end -->

## 1. Specification, Context & Provenance

Own a split CopyBoth stream with bounded decoding, independent heartbeats, cancellation-safe delivery leases and bounded shutdown.

The dedicated `PostgresReplicationConnection` validates slots/publications and owns the CopyBoth socket plus coalesced `read_buf`. `PgOutputDecoder` emits incrementally bounded transaction records and `BoundedReplicationQueue` supplies record/byte limits. `ReplicationSession` composes those seams and owns both transport halves through one abortable worker. [^connection] [^protocol] [^decoder] [^stream]

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

`ReplicationSession` retains immutable `VerifiedReplicationIdentity` and an `Arc<SessionShared>`. The shared mutex contains `BoundedReplicationQueue`, `ReplicationFeedback`, one front-slot record, the single waiter/lease reservation, closing/EOF flags, the absolute shutdown deadline and first terminal result. Separate coalesced `watch` revisions wake record/capacity waiters and the status writer. Subscribe before checking state, and never hold the synchronous lock across await. Reader, writer, lease and caller make publication, capacity and close transitions under the same lock. The reader exclusively owns catalog/codec/decoder state. [^stream]

The connection stores private effective_start_lsn at successful START from the validated explicit requested start_lsn, or confirmed_flush_lsn when absent. into_stream initializes feedback exclusively from this stored fence and accepts no independent LSN.

The immutable identity is captured before consuming `PostgresReplicationConnection`. Remove its redundant mark-only Drop before safe destructuring; PostgresConnection has no Drop. Ordinary abandoned unsplit connections still close by dropping their socket. Private writer/read owners use `tokio::io::{ReadHalf, WriteHalf}<ConnectionStream>`, where ConnectionStream is the existing session transport type. Preserve exact read_buf alongside readhalf. One abort-owned task polls reader and writer futures concurrently; either failure drops both halves, and one absolute shutdown deadline covers final status, CopyDone, backend drain and transport closure. The writer accepts a shared status revision and cancellation signal; it uses received Commit ends and the acknowledged fence, never wal_end.

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

Progress: `[2/2 files documented]`. Both living knowledge chapters describe the verified implementation and remaining integration boundaries before correctness and fresh quality review:

- [x] `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Describe single-generation transport support and remaining reconnect/Python/cutover limitations.
- [x] `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Document native event leases, feedback fences, queue bounds and shutdown ownership.


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
- [x] [a1340034] native-postgres-driver:ch34:t04 — Join replication owners and drain CopyDone under one deadline (tasks/t04-bounded-session-shutdown.md)


One executor per worksheet, in dependency order. No shared-file task executors run in parallel.

**Chapter Recovery Checkpoint:**

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-postgres --check
UV_NO_SYNC=1 make review-check
git diff --check
```

All commands exit0 before present-tense documentation sync, zero-context correctness review, fresh quality review and `last_reviewed_commit` closeout. Chapter completion does not imply live PostgreSQL or wheel acceptance.

## 6. Current State & Remaining Plan Snapshot

- All four tasks are complete at `f520aa6d`, `d99caddf`, `9d4c4258` and `a1340034`; correctness and fresh quality review approve `f58aa695..6e4e6bfc` with no open findings.
- The recovery checkpoint passes 49 canonical wire tests, Clippy and package formatting. The tests cover start fences, bounded/coalesced decoding, Commit publication, cancellation retention, heartbeat liveness and graceful/terminal shutdown.
- `cancel` and explicit `shutdown` fence external leases and discard session-owned queued records. Natural clean CopyDone closes transport while allowing already decoded records to drain and publish. [^stream]
- Documentation coverage is `[2/2]`; native public Python driver cutover remains ch27. Reconnect supervision (ch35), Python handles (ch36), checkpoints (ch24), and ch25–ch33 remain open: 41 tasks in 12 subsequent chapters.
- Next: approve the chapter through independent correctness and fresh quality review, stamp the reviewed commit, then execute ch35 in dependency order.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Replication ownership and acknowledgement contracts.
[^connection]: `src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection` — Dedicated CopyBoth session ownership.
[^protocol]: `src/rs/vertebra-postgres/src/protocol.rs::read_replication_message` — Buffered bounded protocol frame parsing.
[^decoder]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental bounded pgoutput record decoding.

[^stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded session transport and publication leases.
