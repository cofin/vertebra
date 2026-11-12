---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch23-replication-feedback-resume-and-bridge
title: "Chapter 23: Commit-Fenced Replication Feedback"
description: "Implement strict CopyData payload codecs, status intervals, and separate received/published/acknowledged Commit positions."
status: draft
state: planned
planning_status: complete
depends_on_chapters: ["ch22-copyboth-and-pgoutput-decoding"]
replan_trigger: null
current_task: "native-postgres-driver:ch23:t01"
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 2
chapter_base_commit: null
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T05:00:00Z"
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

# Chapter 23: Commit-Fenced Replication Feedback

<!-- truth: start -->
- Implement strict CopyData payload codecs, status intervals, and separate received/published/acknowledged Commit positions. [^contracts]
- Feedback never promotes server wal_end or payload byte lengths into acknowledged durability. [^contracts]
- Every transport task has one owner and bounded cancellation; no Python objects or checkpoint-store calls belong to this chapter. [^connection]
<!-- truth: end -->

## 1. Specification, Context & Provenance

Implement strict CopyData payload codecs, status intervals, and separate received/published/acknowledged Commit positions.

The dedicated `PostgresReplicationConnection` validates slots/publications and owns the CopyBoth socket plus coalesced `read_buf`. `PgOutputDecoder` emits incrementally bounded transaction records and `BoundedReplicationQueue` supplies record/byte limits. The plan uses those concrete seams; it defines all new APIs before their isolated executor tasks. [^connection] [^protocol] [^decoder]

Parent: [full roadmap](../../spec.md) and [contracts](../../contracts.md). Domain context: [CDC](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [FFI discipline](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [runtime skill](../../../../../skills/runtime-ipc/SKILL.md), [PyO3 skill](../../../../../skills/pyo3-bridge/SKILL.md). Source anchors are current symbols; new modules in worksheets are intentional creation targets.

Non-goals: verified reconnect/generation transitions (ch35), Python handles (ch36), durable checkpoint CAS (ch24), driver activation (ch27), and live/wheel/release acceptance (ch28–ch33).

## 2. Requirements, Concrete Contracts & Architecture

- R22.4a (`t01`): strict XLogData/PrimaryKeepalive payload decode, exact StandbyStatusUpdate payload encode and interval computation.
- R22.4b (`t02`): initialized, monotonic Commit state; cumulative exact published-boundary acknowledgement capped at1024 unacked commits.
- Only stream delivery code in ch34 invokes publication; no I/O pump, reconnect or Python handle implementation belongs to ch23.

In `replication/feedback.rs`, exported through `pub mod feedback` in `replication/mod.rs`:

```rust
pub const MAX_UNACKED_COMMITS: usize = 1024;
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);
pub enum ReplicationCopyData<'a> {
    XLogData { wal_start: u64, wal_end: u64, server_time_pg_us: i64, payload: &'a [u8] },
    PrimaryKeepalive { wal_end: u64, server_time_pg_us: i64, reply_requested: bool },
}
pub fn decode_replication_copy_data(frame: &[u8]) -> Result<ReplicationCopyData<'_>, PostgresError>;
pub fn encode_standby_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64, client_time_pg_us: i64, reply_requested: bool) -> [u8; 34];
pub fn compute_status_interval(wal_sender_timeout: Option<Duration>) -> Duration;
```

`w` requires at least25bytes; `k` exactly18bytes and final flag0or1. Encode network endian `r`+threeu64+i64+bool. The function emits payload only; the stream writer adds frontend `CopyData` framing. Interval: None/zero→10s, otherwise min(10s,max(100ms,timeout/3)).

In `replication/feedback.rs`:

```rust
pub struct ReplicationFeedback { /* private state */ }
impl ReplicationFeedback {
    pub fn new(initial_lsn: u64) -> Self;
    pub fn record_received_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError>;
    pub fn record_published_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError>;
    pub fn acknowledge(&mut self, commit_end_lsn: u64) -> Result<u64, PostgresError>;
    pub fn status_positions(&self) -> (u64, u64, u64);
    pub fn unacked_count(&self) -> usize;
}
```

Initialize received/acknowledged to `initial_lsn`, empty published deque. Received Commit ends advance monotonically (equal is idempotent); reject regression. Publication requires strictly increasing endLSN above acknowledged and <=received; caller owns proof that this is a decoded Commit. Reject duplicates and cap1024. `acknowledge` accepts initial/current acknowledged idempotently or an exact published boundary, cumulatively drains earlier published boundaries, and otherwise leaves state unchanged. `status_positions` is (max(received,acknowledged),acknowledged,acknowledged). No received advance comes from server wal_end, wal_start, payload byte count, or keepalive. Stream admission enforces one publication per consumed Commit.

Caller → public feedback/session API → strict frame codec, PgOutputDecoder and real dedicated socket; no pool/query socket reuse. The generation supervisor composes this chapter's API after completion, so tests never depend on a downstream supervisor.

| Boundary / failure | Required result |
| --- | --- |
| Server WAL ahead of received Commit | Only received complete Commit can move write; only acknowledged published Commit moves flush/apply. |
| Future/noncommit/regressed acknowledgement | Interface with unchanged state. |
| Malformed frame | Protocol with no partial output/state mutation. |
| 1025th published Commit | Interface; state count remains1024 until cumulative acknowledgement drains. |


| Interface decision | Chosen design | Alternative and tradeoff |
| --- | --- | --- |
| Feedback boundary | Explicit received, published, acknowledged stages | Echoing server WAL overstates processing and loses replay safety. |
| Public test surface | Small independently callable feedback/session API | Testing only through a later supervisor inverts task dependencies and hides lifecycle bugs. |
| Socket ownership | Consuming transfer and split-owned loops | Unsafe extraction or dummy socket replacement adds ownership failure paths. |
| Cancellation | Owned single-receiver delivery lease | A bare popped record plus external publication lets cancelled Python completions lose or prematurely publish records. |

Acceptance scenarios are the literal Worked Test Matrices in the task blueprints; each has its own public seam, Red command, Green module suite and Clippy gate.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]` at chapter sync. The isolated sync subagent updates these living files with implementation-backed truth before code and fresh quality review:

- [ ] `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Describe available native feedback APIs and unimplemented stream/reconnect/bridge boundary.


The sync owns these docs collectively, independent of task production limits. Full user API guides, packaging and release documentation retain ch30–ch32 ownership; interim knowledge explicitly describes the native/legacy cutover boundary.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical verification |
| --- | --- | --- | --- |
| R22.4 | [native-postgres-driver:ch23:t01](tasks/t01-feedback-wire-codecs.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |
| R22.4 | [native-postgres-driver:ch23:t02](tasks/t02-commit-fenced-feedback.md) | behavior_tdd | postgres_replication_wire_tests + Clippy |

| Living knowledge accuracy | Chapter sync before review | documentation_validation | source links, Markdown/frontmatter, review-check |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch23:t01 — Decode replication CopyData and encode bounded status payloads (tasks/t01-feedback-wire-codecs.md)
- [ ] native-postgres-driver:ch23:t02 — Fence acknowledgement to published Commit boundaries (tasks/t02-commit-fenced-feedback.md)


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

- Plan revision8 is draft against verified repository baseline `5eb75b2a`; execution requires the planning approval gate.
- All 2 worksheets are pending, signature-complete and individually bounded; no production changes are present.
- Root dependency order continues ch34 session → ch35 supervision → ch36 Python bridge → ch24 checkpoints, then ch25–ch33. Full flow contains36chapters; completed22chapter history remains intact.
- Doc/OKF manifest is open until chapter implementation sync; native public Python driver cutover remains ch27.
- Stop after this chapter's recovery checkpoint, living documentation sync and independent code/quality review.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Replication ownership and acknowledgement contracts.
[^connection]: `src/rs/vertebra-postgres/src/replication/mod.rs::PostgresReplicationConnection` — Dedicated CopyBoth session ownership.
[^protocol]: `src/rs/vertebra-postgres/src/protocol.rs::read_replication_message` — Buffered bounded protocol frame parsing.
[^decoder]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental bounded pgoutput record decoding.
