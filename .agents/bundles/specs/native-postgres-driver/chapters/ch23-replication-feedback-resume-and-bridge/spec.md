---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch23-replication-feedback-resume-and-bridge
title: "Chapter 23: Commit-Fenced LSN Feedback, Verified Replication Resume, and PyO3 Bridge"
description: Implement commit-boundary StandbyStatusUpdate feedback, identity-verified stream reconnect, and cancellation-safe AsyncPostgresReplication PyO3 handles.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch22-copyboth-and-pgoutput-decoding
replan_trigger: null
current_task: "native-postgres-driver:ch23:t01"
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL replication handle, closed snapshot, and feedback invariants
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: XLogData, PrimaryKeepalive, StandbyStatusUpdate, and commit-fenced LSN rules
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Per-extension AsyncCompletionQueue drainer and cancellation-safe task completion
  - id: ffi-discipline
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: PyO3 gil_used=false and worker-thread GIL detachment rules
tags:
  - postgres
  - replication
  - feedback
  - pyo3
  - cdc
---

# Chapter 23: Commit-Fenced LSN Feedback, Verified Replication Resume, and PyO3 Bridge

<!-- truth: start -->
- **Monotonic Commit-Boundary Feedback**: `ReplicationFeedback` separates `received_complete_lsn` from `acknowledged_commit_lsn`; `StandbyStatusUpdate` (`'r'`) reports `flush_lsn = apply_lsn = acknowledged_commit_lsn` and accepts only completely published contiguous `COMMIT` boundaries, never advancing on server `wal_end`, keepalives, or cancelled completions [^npd-contracts] [^npd-proto-research].
- **Independent Heartbeat Writer**: Split read/write socket ownership answers `PrimaryKeepalive` (`'k'`) `reply_requested` flags immediately and emits periodic `10s` (`min(10s, wal_sender_timeout / 3)`) status updates even while the bounded reader pauses on full event/unacked-commit queues [^npd-proto-research].
- **Verified Generation Resume & `AsyncPostgresReplication`**: Raw `reconnect=True` re-runs `IDENTIFY_SYSTEM` and slot/publication preflight, resumes strictly from `acknowledged_commit_lsn` with fenced generation IDs, and `reconnect=False` terminates after a single socket drop so `PostgresChangeSource` owns durable checkpoint retries [^npd-contracts] [^py-async-rs] [^ffi-discipline].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

A logical replication client that echoes server `wal_end` in `StandbyStatusUpdate` (`'r'`) or starves heartbeats when consumer queues fill either loses uncommitted/unprocessed WAL on crash or triggers `wal_sender_timeout` disconnects [^npd-proto-research]. Chapter 23 implements `XLogData` (`'w'`), `PrimaryKeepalive` (`'k'`), and `StandbyStatusUpdate` (`'r'`) framing with an independent feedback writer fenced strictly to published `Commit` boundaries, adds verified `PostgresReplicationResume` reconnect semantics, and exposes `AsyncPostgresClient.{replication_identity, open_replication}` and `AsyncPostgresReplication` across `vertebra._postgres` [^npd-contracts] [^py-async-rs].

### 1.2 Non-Goals

- Python `PostgresCheckpointStore` compare-and-swap persistence and `PostgresChangeSource` replay orchestration (owned by Chapter 24).
- Driver-level `_open_replication_stream` / `_replication_identity` handle registry and pooler `derive_direct_config` enforcement (owned by Chapters 25 and 26).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) and [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R22.4 (`ch23:t01`)**: Decode `CopyData` `'w'` (`XLogData`) and `'k'` (`PrimaryKeepalive`), encode `'r'` (`StandbyStatusUpdate`), enforce `ReplicationFeedback` monotonic published-commit acknowledgement (`<= 1024` pending unacked commits), and split `CopyBoth` into a bounded reader and independent timer/keepalive feedback writer.
- **R22.5 (`ch23:t02`)**: Implement `PostgresReplicationResume` generation fencing, `IDENTIFY_SYSTEM`/slot/publication re-verification, IAM credential refresh before reconnect, `0.1s..5.0s` jittered backoff (`max 8` attempts) when `reconnect=True`, and strict single-socket termination when `reconnect=False`.
- **R22.6 & R23.1 (`ch23:t03`)**: Expose `AsyncPostgresClient.replication_identity`, `AsyncPostgresClient.open_replication`, and `AsyncPostgresReplication` (`next`, `acknowledge`, `aclose`, `__aenter__`, `__aexit__`, `closed`) in `async_client.rs`, `replication/mod.rs`, and `src/py/vertebra/_postgres.pyi` with cancellation-safe completion publication and zero worker-thread `Python::attach`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-postgres/src/replication/feedback.rs` & `mod.rs`):
  - `pub enum ReplicationCopyData<'a> { XLogData { wal_start: u64, wal_end: u64, server_time_pg_us: i64, payload: &'a [u8] }, PrimaryKeepalive { wal_end: u64, server_time_pg_us: i64, reply_requested: bool } }`
  - `pub fn encode_standby_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64, now_pg_us: i64, reply_requested: bool) -> [u8; 34]`
  - `impl ReplicationFeedback { pub fn record_published_commit(&mut self, commit_end_lsn: u64) -> Result<(), crate::errors::PostgresError>; pub fn acknowledge(&mut self, commit_end_lsn: u64) -> Result<u64, crate::errors::PostgresError>; }`
  - `pub struct PostgresReplicationResume { pub identity: VerifiedReplicationIdentity, pub acknowledged_lsn: u64, pub generation: u64 }`
  - `ReplicationSupervisor::open(config: crate::config::PostgresConfig, slot: String, publication: String, start_lsn: Option<u64>, queue_capacity: usize, reconnect: bool) -> Result<Self, crate::errors::PostgresError>` (dials dedicated non-pooled `replication=database` connections via `PostgresConfig`, which carries no `pooler_mode`; pooled `SessionHandle` / `AsyncPostgresClient` `crate::session::statements::PoolerMode` and Python `PostgreSQLConfig.derive_direct_config()` reject non-direct/session pooler modes before opening replication).
- Python Stubs (`src/py/vertebra/_postgres.pyi`):
  - `class AsyncPostgresClient:`
    - `async def replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity: ...`
    - `async def open_replication(self, slot: str, publication: str, *, start_lsn: int | None = None, queue_capacity: int = 1024, reconnect: bool = True) -> AsyncPostgresReplication: ...`
  - `class AsyncPostgresReplication:`
    - `@property def closed(self) -> bool: ...`
    - `async def next(self) -> PostgresReplicationRecord | None: ...`
    - `async def acknowledge(self, commit_end_lsn: int) -> None: ...`
    - `async def aclose(self) -> None: ...`
    - `async def __aenter__(self) -> AsyncPostgresReplication: ...`
    - `async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None: ...`

### 2.3 Caller -> Seam -> Collaborator Map

- Python caller / `PostgresChangeSource` (`ch24:t01`) -> `AsyncPostgresClient.{replication_identity, open_replication}` -> `PostgresReplicationConnection` + `AsyncPostgresReplication`.
- `AsyncPostgresReplication.{next, acknowledge, aclose}` -> `spawn_rust_db_task_with_attr` completion queue -> split `ReplicationReadLoop` (`PgOutputDecoder` + `BoundedReplicationQueue`) & `ReplicationWriteLoop` (`ReplicationFeedback` + `StandbyStatusUpdate` watch channel).

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| `acknowledge(lsn)` called with future LSN, non-commit DML LSN, or `< acknowledged_commit_lsn` | Reject with `InterfaceError` / `ValueError` without mutating feedback state; equal `acknowledged_commit_lsn` is an idempotent no-op. |
| Event queue (`1024` / `8 MiB`) or unacked-commit deque (`1024`) reaches capacity | Pause socket reads holding at most `1` pending frame; independent `ReplicationWriteLoop` continues `10s` periodic `StandbyStatusUpdate` frames with unchanged `acknowledged_commit_lsn`. |
| Queued `AsyncPostgresReplication.next()` `Future` cancelled before Python loop delivery | Publication guard prevents marking `Commit` boundary as published and prevents dropping or acknowledging unseen events; `aclose()` drains cleanly. |
| Reconnect encounters changed `system_identifier`, database, publication, or `acknowledged_lsn < restart_lsn` | Fail closed immediately without retrying or adopting newer server WAL; `closed` transitions to `True`. |
| `open_replication(..., reconnect=False)` loses TCP connection | Emit terminal `ConnectionFailure` / `OperationalError`, transition `closed` to `True`, and open zero replacement sockets. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Feedback writer scheduling | Independent write loop over `tokio::sync::watch` + interval timer | Coupled read/write `select!` inside a single reader loop that stalls heartbeats whenever downstream event queues exert backpressure. |
| Raw vs source reconnect | Optional `reconnect` kwarg (`False` for `PostgresChangeSource`) | Mandatory raw auto-reconnect that would race Python CAS checkpoint reloads on ambiguous save failures. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch23]` (public documentation and OKF chapters update in `ch30:t01` and `ch30:t02`; `src/py/vertebra/_postgres.pyi` type stubs and docstrings for `AsyncPostgresReplication` and `AsyncPostgresClient` replication methods land in `ch23:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R22 (R22.4) | [t01](tasks/t01-durable-commit-boundary-feedback.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` |
| R22 (R22.5) | [t02](tasks/t02-checkpoint-verified-replication-resume.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` |
| R22, R23 (R22.6, R23.1) | [t03](tasks/t03-cancellation-safe-replication-bridge.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_change_source.py`, `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch23:t01 — Acknowledge only completed durable commit boundaries (tasks/t01-durable-commit-boundary-feedback.md)
- [ ] native-postgres-driver:ch23:t02 — Reconnect logical streams from verified checkpoints (tasks/t02-checkpoint-verified-replication-resume.md)
- [ ] native-postgres-driver:ch23:t03 — Expose cancellation-safe native replication handles (tasks/t03-cancellation-safe-replication-bridge.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests && make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch24-change-source-and-checkpoints` and `ch26-compiler-pipeline-arrow-and-event-hooks`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL replication handle, closed snapshot, and feedback invariants.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — XLogData, PrimaryKeepalive, StandbyStatusUpdate, and commit-fenced LSN rules.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Per-extension AsyncCompletionQueue drainer and cancellation-safe task completion.
[^ffi-discipline]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 gil_used=false and worker-thread GIL detachment rules.
