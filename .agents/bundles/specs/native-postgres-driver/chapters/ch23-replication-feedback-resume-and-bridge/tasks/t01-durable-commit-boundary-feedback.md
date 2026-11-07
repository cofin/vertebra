---
type: Task
id: "native-postgres-driver:ch23:t01"
chapter_id: ch23-replication-feedback-resume-and-bridge
title: Acknowledge only completed durable commit boundaries
description: Implement XLogData and PrimaryKeepalive decoding, StandbyStatusUpdate encoding, monotonic commit-fenced ReplicationFeedback, and split read/write heartbeat ownership.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch22:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 235
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Contiguous emitted commit-boundary acknowledgement contract
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: XLogData, PrimaryKeepalive, and StandbyStatusUpdate wire specifications
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Split async read/write transport ownership primitives
tags:
  - postgres
  - replication
  - feedback
  - lsn
---

# Task t01: Acknowledge Only Completed Durable Commit Boundaries

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/replication/feedback.rs` and wire split read/write stream ownership in `src/rs/vertebra-postgres/src/replication/mod.rs` to decode `CopyData` `'w'` (`XLogData`) and `'k'` (`PrimaryKeepalive`), emit `'r'` (`StandbyStatusUpdate`) heartbeats independently of consumer backpressure, and fence `flush_lsn`/`apply_lsn` strictly to monotonic published `Commit` boundaries [^npd-contracts] [^npd-proto-research] [^pg-conn-rs]. Stream reconnect (`ch23:t02`) and PyO3 bindings (`ch23:t03`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Commit-boundary acknowledgement and durable-before-feedback invariants [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `XLogData` (`'w'`), `PrimaryKeepalive` (`'k'`), and `StandbyStatusUpdate` (`'r'`) wire field layouts [^npd-proto-research].
- `src/rs/vertebra-postgres/src/replication/mod.rs` — `PostgresReplicationConnection` from Chapter 22.
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs` — `PgOutputDecoder`, `ReplicationRecord::Commit`, and `BoundedReplicationQueue`.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Replication wire integration test suite.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/feedback.rs`:
  - `pub const MAX_UNACKED_COMMITS: usize = 1024;` and `pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);`
  - `pub enum ReplicationCopyData<'a> { XLogData { wal_start: u64, wal_end: u64, server_time_pg_us: i64, payload: &'a [u8] }, PrimaryKeepalive { wal_end: u64, server_time_pg_us: i64, reply_requested: bool } }`
  - `pub fn decode_replication_copy_data(frame: &[u8]) -> Result<ReplicationCopyData<'_>, crate::errors::PostgresError>` — validates `'w'` (`>= 25` bytes: `1` tag + `8` `wal_start` + `8` `wal_end` + `8` `server_time_pg_us` + `payload`) and `'k'` (exact `18` bytes: `1` tag + `8` `wal_end` + `8` `server_time_pg_us` + `1` `reply_requested` in `{0, 1}`).
  - `pub fn encode_standby_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64, client_time_pg_us: i64, reply_requested: bool) -> [u8; 34]` — network-endian `b'r'` + `write_lsn` + `flush_lsn` + `apply_lsn` + `client_time_pg_us` + `0u8`/`1u8` (`reply_requested = false` on client status frames), framed inside frontend `CopyData` (`b'd'`).
  - `pub fn compute_status_interval(wal_sender_timeout: Option<Duration>) -> Duration` — returns `DEFAULT_STATUS_INTERVAL` when `None` or `Zero`, else `min(DEFAULT_STATUS_INTERVAL, max(Duration::from_millis(100), timeout / 3))`.
  - `pub struct ReplicationFeedback`:
    - Tracks `received_complete_lsn: u64`, `published_commits: VecDeque<u64>` (capped at `MAX_UNACKED_COMMITS = 1024`), and `acknowledged_commit_lsn: u64`.
    - `pub fn record_published_commit(&mut self, commit_end_lsn: u64) -> Result<(), crate::errors::PostgresError>` — invoked ONLY when a `ReplicationRecord::Commit` is actually delivered to the consumer receiver (never when merely decoded or queued).
    - `pub fn acknowledge(&mut self, commit_end_lsn: u64) -> Result<u64, crate::errors::PostgresError>` — returns `Ok(acknowledged_commit_lsn)` idempotently when `commit_end_lsn == acknowledged_commit_lsn`; drains contiguous entries `<= commit_end_lsn` up to an exact match in `published_commits`, updating `acknowledged_commit_lsn = commit_end_lsn`; rejects `< acknowledged_commit_lsn`, unknown non-commit LSNs, and future/unemitted LSNs.
    - `pub fn status_positions(&self) -> (u64, u64, u64)` — returns `(write_lsn = max(received_complete_lsn, acknowledged_commit_lsn), flush_lsn = acknowledged_commit_lsn, apply_lsn = acknowledged_commit_lsn)`.
- `src/rs/vertebra-postgres/src/replication/mod.rs`:
  - Split `TransportStream` via `tokio::io::split` after `CopyBothResponse` into `ReplicationReadTask` and `ReplicationWriteTask` coordinated via a bounded `tokio::sync::watch::channel` (zero per-heartbeat allocations or spawned tasks):
    - When `BoundedReplicationQueue` (`1024` records / `8 MiB`) or `published_commits.len() >= 1024` is full, `ReplicationReadTask` stops reading further socket WAL frames (retaining at most `1` decoded pending record), while `ReplicationWriteTask` continues firing `StandbyStatusUpdate` on `compute_status_interval` with `flush_lsn = apply_lsn = acknowledged_commit_lsn`.
    - On `'k'` with `reply_requested == true`, `ReplicationReadTask` signals the watch channel so `ReplicationWriteTask` flushes a `StandbyStatusUpdate` immediately using current `acknowledged_commit_lsn` (never server `wal_end`).
    - On close/cancellation, `ReplicationWriteTask` sends one final `StandbyStatusUpdate` with current `acknowledged_commit_lsn` then `CopyDone` under deadline without advancing `acknowledged_commit_lsn`.

## Edge Cases & Error Semantics

- Server `wal_end` in `'w'` or `'k'` frequently leads delivered transactions (e.g. `wal_end = 0x1_0000_9000` while last acknowledged commit is `0x1_0000_0440`); `flush_lsn` and `apply_lsn` MUST remain `0x1_0000_0440`.
- Keepalive frames sitting behind unread TCP WAL while the bounded reader is paused on consumer backpressure are processed once consumer capacity drains; meanwhile the independent periodic timer in `ReplicationWriteTask` keeps the PostgreSQL WAL sender alive without parsing ahead or buffering unbounded WAL.
- Short `'w'` (`< 25` bytes), wrong-length `'k'` (`!= 18` bytes), `reply_requested` outside `{0, 1}`, or unknown `CopyData` tag returns `ProtocolError` and terminates the session without advancing feedback.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` (using `tokio::time::pause()` and `tokio::time::advance(...)` — zero real sleeps).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Golden `'w'`, `'k'`, & `'r'` wire layout | Peer sends `'k'` (`wal_end = 0x2_0000_1000`, `reply_requested = 1`) before any acknowledged commit (`initial_lsn = 0x2_0000_0100`) | Client immediately replies with `34`-byte `'r'` payload inside `CopyData`: `write_lsn = 0x2_0000_0100`, `flush_lsn = 0x2_0000_0100`, `apply_lsn = 0x2_0000_0100`, valid PG-epoch timestamp, `reply_requested = 0` (never `0x2_0000_1000`). |
| Monotonic contiguous commit fencing | Publish `Commit(0x2_0000_0200)` and `Commit(0x2_0000_0350)`; call `acknowledge(0x2_0000_0250)` (non-commit), `acknowledge(0x2_0000_0999)` (future), `acknowledge(0x2_0000_0350)` (valid cumulative), `acknowledge(0x2_0000_0350)` (idempotent), `acknowledge(0x2_0000_0200)` (regression) | Non-commit, future, and regression calls fail without mutating state; `0x2_0000_0350` advances `flush_lsn = apply_lsn = 0x2_0000_0350`; duplicate `0x2_0000_0350` succeeds idempotently. |
| Heartbeat survival under stalled consumer backpressure | Fill `BoundedReplicationQueue` (`capacity = 4` in test), pause consumer, advance Tokio clock by `30s` (`3 * 10s` intervals) | Reader pauses socket WAL consumption with `<= 1` pending frame; writer still emits `3` periodic `'r'` status frames carrying unchanged `flush_lsn = apply_lsn`. |
| Shutdown feedback fence | Deliver `Begin` + `Insert` + `Commit(0x2_0000_0500)` without calling `acknowledge`, then `shutdown()` | Shutdown emits final `'r'` with prior `acknowledged_commit_lsn` (NOT `0x2_0000_0500`) followed by `CopyDone`. |
| Malformed `CopyData` tag / truncated `'w'` or `'k'` | Peer sends `CopyData(b"x...")` or `12`-byte `'k'` | Fails immediately with `ProtocolError`; zero feedback advancement. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests feedback`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests feedback
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [ ] `feedback.rs` decodes `'w'` / `'k'`, encodes `34`-byte `'r'` `StandbyStatusUpdate` frames, and enforces monotonic published-commit acknowledgement (`<= 1024` unacked commits) without echoing server `wal_end` into `flush_lsn`/`apply_lsn` (R22).
- [ ] Split read/write ownership answers `reply_requested` keepalives promptly and sustains periodic `StandbyStatusUpdate` heartbeats under paused Tokio clock while the reader is backpressured (R22).
- [ ] Commit message follows `feat(postgres): fence durable replication acknowledgements` once verified.

## Notes & Discoveries

- Marking a commit boundary as acknowledgeable only upon consumer receiver publication (rather than worker decode) prevents acknowledging a transaction whose queued Python future is cancelled before delivery in `ch23:t03` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Contiguous emitted commit-boundary acknowledgement contract.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — XLogData, PrimaryKeepalive, and StandbyStatusUpdate wire specifications.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Split async read/write transport ownership primitives.
