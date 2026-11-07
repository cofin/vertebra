---
type: Task
id: "native-postgres-driver:ch18:t02"
chapter_id: ch18-binary-copy-streaming
title: Implement exclusive COPY TO STDOUT streaming and CancelRequest recovery
description: Add PostgresConnection::start_copy_out, SessionHandle::copy_to, and CopyOut streaming in vertebra-postgres with binary format validation, row-count verification, and CancelRequest drain/retirement on early close.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch18:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch18-binary-copy-streaming/spec.md
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/cancel.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs
estimated_loc: 220
plan_revision: 4
plan_commit: 45ff9563
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
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Exclusive session lease, cancellation retirement, and COPY contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL COPY TO STDOUT protocol flow and CancelRequest drain recovery
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Underlying TransportStream socket lifecycle
tags:
  - postgres
  - rust
  - copy
  - streaming
  - cancellation
---

# Task t02: Implement Exclusive COPY TO STDOUT Streaming and CancelRequest Recovery

## Objective & Scope Boundary

Implement `PostgresConnection::start_copy_out`, `SessionHandle::copy_to`, and `CopyOut` in `src/rs/vertebra-postgres/src/{copy,session}.rs` so `COPY ... TO STDOUT (FORMAT binary)` streams rows incrementally under exclusive session/transaction ownership and recovers early consumer closes via `CancelRequest` + bounded drain or physical socket retirement [^pg-contracts] [^pg-protocol-research] [^pg-conn-rs]. `COPY FROM STDIN` belongs to `ch18:t03`, and PyO3 `SyncPostgresCopyOut` / `AsyncPostgresCopyOut` client wrappers belong to `ch20:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 18 `COPY TO STDOUT` streaming and `CancelRequest` recovery contracts.
- `src/rs/vertebra-postgres/src/copy.rs` — `BinaryCopyDecoder` from `ch18:t01`.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, `SessionHandle`, operation generation tokens, and pipeline scope guards.
- `src/rs/vertebra-postgres/src/cancel.rs` — Generation-fenced `CancelRequest` sender and physical connection retirement accessed via `crate::session::cancel` (`#[path = "cancel.rs"] pub mod cancel;` inside `session.rs` from `ch16:t03`; `lib.rs` leaves `cancel` unexported).
- [contracts.md](../../../contracts.md) — Pool lease pinning, pipeline `COPY` refusal, and cancellation retirement rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `COPY TO` wire framing (`CopyOutResponse`, `CopyData`, `CopyDone`, `CommandComplete`, `ReadyForQuery`) and why `CopyFail` is invalid for `COPY TO` [^pg-protocol-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/session.rs`:
  - `pub async fn start_copy_out(&mut self, sql: &str) -> Result<usize, PostgresError>` on `PostgresConnection`:
    - Rejects active pipeline scope (`PostgresError::NotSupported` / `Interface`) and multi-statement SQL before writing bytes.
    - Sends simple `Query` (or unnamed extended `Parse`/`Bind`/`Execute`/`Sync`), processes `NoticeResponse` / `ParameterStatus`, handles pre-stream `ErrorResponse` by draining through `ReadyForQuery`, and validates `CopyOutResponse` (`overall_format == 1` and every per-column format `== 1`), returning `column_count`.
  - `pub async fn copy_to(self: &Arc<Self>, sql: &str) -> Result<CopyOut, PostgresError>` on `SessionHandle`:
    - Acquires exclusive operation reservation (pinning the underlying `PooledConnection` or direct socket and current `TransactionScope` until `CopyOut` finishes or retires).
- `src/rs/vertebra-postgres/src/copy.rs`:
  - `pub struct CopyOut`:
    - `pub fn column_count(&self) -> usize`
    - `pub fn rows_delivered(&self) -> u64`
    - `pub async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError>` (returns buffered decoded row immediately when available; otherwise reads backend frames, routing `CopyData` into `BinaryCopyDecoder`, handling interleaved `NoticeResponse` / `ParameterStatus`, and on `CopyDone` calling `decoder.finish()`, consuming `CommandComplete("COPY <n>")` + `ReadyForQuery`, verifying `decoded_rows == tag_rows`, and releasing the exclusive operation guard before returning `Ok(None)`)
    - `pub async fn close(&mut self) -> Result<(), PostgresError>` (idempotent: no-op when already exhausted; when closed midstream, triggers generation-fenced `CancelRequest` via `crate::session::cancel`, drains `CopyData` / `CopyDone` / `ErrorResponse(57014)` / `ReadyForQuery` within `connect_timeout`, and marks the physical connection retired so pooled leases spawn a fresh backend on next borrow)
    - `impl Drop for CopyOut` (when dropped unexhausted without `close().await`, spawns bounded background `CancelRequest` + socket retirement on `db_runtime_handle()` before releasing the pool permit; never returns an active `COPY` socket to the pool or blocks caller threads)

## Edge Cases & Error Semantics

- Non-binary `CopyOutResponse` (`overall_format == 0` or any column format `!= 1`) triggers immediate `CancelRequest` + bounded drain/retirement and returns `PostgresError::Data("COPY TO requires FORMAT binary")`.
- Server `ErrorResponse` before `CopyOutResponse` (e.g., `COPY (SELECT 1/0) TO STDOUT (FORMAT binary)` -> `22012`) drains `ReadyForQuery`, preserves session usability when no `CancelRequest` was sent, and surfaces canonical `DatabaseError` with `sqlstate = Some("22012")`.
- Midstream `ErrorResponse` (between `CopyData` frames) drains `ReadyForQuery`, releases/retires according to session state, and surfaces the exact server `SQLSTATE` rather than a generic truncation error.
- Malformed `PGCOPY` bytes or `CommandComplete("COPY <n>")` count mismatch against `BinaryCopyDecoder::finish()` marks the session broken/retired after draining `ReadyForQuery` (if reachable within recovery deadline) and returns `PostgresError::Data` / `Protocol`.
- Concurrent operations on the same `SessionHandle` while `CopyOut` is live fail immediately with `PostgresError::Interface` without touching the socket.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs` (`copy_out_*` suite over real loopback TCP scripted PostgreSQL peers and `PhysicalPool` with `max_size = 1`)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Incremental row arrival before `CopyDone` | Peer sends `CopyOutResponse(binary, 2 cols)`, first row split across 3 `CopyData` frames, then blocks on a `tokio::sync::Notify` barrier before sending row 2 + `CopyDone` + `CommandComplete("COPY 2")` + `ReadyForQuery(I)` | First `copy_out.next_row().await` resolves immediately to `Some([Some(b"\x00\x00\x00\x07"), None])` while barrier is still held; releasing barrier yields row 2 then `None`, and pool-size-1 follow-up query reuses the same backend PID. |
| Empty `COPY 0` & interleaved `NoticeResponse` / `ParameterStatus` | Peer emits `CopyOutResponse`, `NoticeResponse`, `CopyData(header)`, `ParameterStatus("TimeZone", "UTC")`, `CopyData(trailer -1i16)`, `CopyDone`, `CommandComplete("COPY 0")`, `ReadyForQuery(T)` inside ambient transaction | `next_row().await` returns `Ok(None)`, `rows_delivered() == 0`, session parameter `TimeZone == "UTC"` is updated, and ambient transaction remains `T` on the same pinned session. |
| Reject text `CopyOutResponse` & pipeline `COPY` | (a) Peer sends `CopyOutResponse(overall_format=0)` or per-column `[1, 0]`; (b) caller invokes `copy_to` while pipeline scope is active | (a) Returns `Err(PostgresError::Data(_))`, sends `CancelRequest`, and retires socket; (b) returns `Err` before emitting wire bytes. |
| Pre-stream & midstream server `ErrorResponse` | (a) `ErrorResponse(22012)` + `ReadyForQuery(I)` before `CopyOutResponse`; (b) `CopyData(header)` + `ErrorResponse(57014)` + `ReadyForQuery(I)` midstream | Both surface exact server `SQLSTATE` (`22012` / `57014`) and leave pool-size-1 usable for subsequent `SELECT 1`. |
| Early `close()` & `Drop` recovery on `max_size = 1` pool | Consumer reads 1 row from infinite `CopyData` peer then calls `copy_out.close().await` (and in second test drops `CopyOut` directly) | Separate `CancelRequest(pid, key)` arrives on cancel listener; old socket drains or closes within `connect_timeout`; next `pool.acquire()` succeeds on a fresh backend PID (`pid_2 != pid_1`). |
| Count mismatch & truncated `CopyDone` | Peer sends 1 valid row + `-1i16` trailer + `CopyDone` + `CommandComplete("COPY 2")` + `ReadyForQuery(I)` | `next_row().await` returns `Err(PostgresError::Protocol(_))` (decoded `1 != tag 2`). |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_out`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_out
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): own COPY TO stream recovery`

## Acceptance Criteria

- [ ] `SessionHandle::copy_to` and `CopyOut` pin the physical session and active transaction, reject pipeline `COPY` before writes, validate binary `CopyOutResponse` formats, and yield rows incrementally before `CopyDone` (R19).
- [ ] `CopyOut` verifies `CommandComplete("COPY <n>")` count parity and processes interleaved `NoticeResponse` / `ParameterStatus` and pre/midstream `ErrorResponse` frames cleanly (R19).
- [ ] Early `close()` and `Drop` issue generation-fenced `CancelRequest`, drain or discard within `connect_timeout`, and retire any cancelled physical socket before pool permit release (R18, R19).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exclusive session lease, cancellation retirement, and COPY contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL COPY TO STDOUT protocol flow and CancelRequest drain recovery.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Underlying TransportStream socket lifecycle.
