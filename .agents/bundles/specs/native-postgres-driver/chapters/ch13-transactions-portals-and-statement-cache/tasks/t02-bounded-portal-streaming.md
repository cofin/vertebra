---
type: Task
id: "native-postgres-driver:ch13:t02"
chapter_id: ch13-transactions-portals-and-statement-cache
title: "Stream bounded results through session-retaining portals"
description: "Expose a concrete Rust Portal with row-limited Execute, incremental PortalEvent resumption, pinned TransactionScope ownership, and nonblocking Drop cleanup."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch13:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch13-transactions-portals-and-statement-cache/spec.md
  - src/rs/vertebra-postgres/src/transaction.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/extended.rs
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/portal_protocol.rs
estimated_loc: 240
plan_revision: 7
plan_commit: 1c75af65
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
    title: Frozen native PostgreSQL portal streaming and pinned-lease contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL portal Execute(max_rows), PortalSuspended, and transaction survival evidence
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::db_runtime_handle
    title: Background Tokio runtime handle for nonblocking Drop cleanup
tags:
  - postgres
  - portals
  - streaming
  - rust
---

# Task t02: Stream Bounded Results Through Session-Retaining Portals

## Objective & Scope Boundary

Expose a concrete Rust `Portal` in `vertebra-postgres` with row-limited `Execute(max_rows)`, suspension/resumption across `PortalSuspended`, row-level `PortalEvent` iteration for Arrow batch builders, and deterministic `TransactionScope` + `SessionHandle` ownership [^pg-contracts] [^pg-protocol-research]. Python `SyncPostgresPortal` / `AsyncPostgresPortal` wrappers (`ch17:t03`) and Arrow portal streaming (`ch20:t03`–`t04`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 13 `Portal` and `PortalEvent` requirements and edge-case matrix.
- [contracts.md](../../../contracts.md) — Portal lease pinning and column/result invariants [^pg-contracts].
- `src/rs/vertebra-postgres/src/transaction.rs` — `TransactionOwner` and `TransactionScope` (`ExplicitTransaction` vs `NestedSavepoint`) from `ch13:t01`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle` `pub(crate)` `SessionOperationGuard` / `with_connection` reservation helpers and `PostgresConnection` transport reader/writer.
- `src/rs/vertebra-postgres/src/extended.rs` — `Parameter`, `encode_bind`, `PreparedDescription`, and column format negotiation from `ch12:t03`.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Verified PG18.3 behavior showing `Execute(max_rows)` + `Sync` outside `BEGIN` destroys the portal on `ReadyForQuery('I')` while `BEGIN` preserves `ReadyForQuery('T')` [^pg-protocol-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/portal.rs`:
  - `pub enum PortalEvent { Row(Vec<PgValue>), Suspended, Complete { command_tag: String, affected_rows: u64 } }`
  - `pub struct Portal` retaining `Arc<SessionHandle>`, unique portal name (`format!("vtb_p_{id}")`), owner/connection generation, `TransactionScope`, ordered `Vec<Column>` metadata, `CodecContext` snapshot, and pending `Execute` frame-stream state (`exhausted: bool`, `closed: bool`, `in_flight_execute: bool`).
  - `impl Portal`:
    - `pub fn columns(&self) -> &[Column]`
    - `pub fn is_exhausted(&self) -> bool` and `pub fn is_closed(&self) -> bool`
    - `pub async fn start_fetch(&mut self, max_rows: i32) -> Result<(), PostgresError>` — validates `max_rows >= 1`, refuses a second `Execute` while `in_flight_execute` is `true`, and sends `Execute(portal_name, max_rows)` + `Sync` on the pinned session.
    - `pub async fn next_event(&mut self) -> Result<PortalEvent, PostgresError>` — reads one `DataRow` (`PortalEvent::Row`), `PortalSuspended` (drains trailing `ReadyForQuery('T')`, clears `in_flight_execute`, yields `PortalEvent::Suspended`), or `CommandComplete` / `EmptyQueryResponse` (closes portal, drains `ReadyForQuery`, finishes owned read `TransactionScope` cleanly, yields `PortalEvent::Complete`). Arrow batch builders can pause after any single `Row` and resume the same in-flight `Execute` response later without buffering the full chunk or issuing a premature `Execute`.
    - `pub async fn fetch(&mut self, max_rows: i32) -> Result<QueryResult, PostgresError>` — convenience aggregator over `start_fetch` + `next_event` enforcing both `max_rows` and configured `max_result_bytes`.
    - `pub async fn close(&mut self) -> Result<(), PostgresError>` — idempotent explicit cleanup: drains any in-flight `Execute`, sends `Close(b'P', portal_name)` + `Sync` (`CloseComplete` + `ReadyForQuery`), rolls back early-abandoned owned streaming transactions (or rolls back/releases only the portal savepoint in ambient `T` without committing caller work), and releases the pinned session reservation.
    - `impl Drop for Portal` — if neither exhausted nor closed, schedules bounded native cleanup on `vertebra_db::py_async::db_runtime_handle()` without blocking or awaiting under the GIL; any failed or uncertain cleanup marks the physical socket retired before releasing session ownership [^py-async-rs].
- `src/rs/vertebra-postgres/src/session.rs` and `src/rs/vertebra-postgres/src/lib.rs`:
  - Add `SessionHandle::open_portal(self: &Arc<Self>, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>, batch_size: i32) -> impl Future<Output = Result<Portal, PostgresError>> + Send`:
    - Validates `batch_size >= 1` (`PostgresError::Interface` on `<= 0`), resolves required custom parameter metadata via `metadata::resolve_parameter_metadata` before `BEGIN`/savepoint or inside safe savepoint scope, opens `TransactionScope` (`BEGIN` when `Idle`, `SAVEPOINT vtb_sp_<n>` when `InTransaction`, rejected when `Failed`), and executes `Parse` + `DescribeStatement` + named `Bind(portal_name)` + `DescribePortal(portal_name)` + `Sync` to materialize column descriptions while keeping `ReadyForQuery('T')`.
  - Export `Portal` and `PortalEvent` at crate root in `src/rs/vertebra-postgres/src/lib.rs`.

## Edge Cases & Error Semantics

- **Invalid row limits**: `batch_size <= 0` or `max_rows <= 0` returns `Err(PostgresError::Interface)` before sending any wire frame.
- **Overlapping operations on pinned session**: While `Portal` is open and unclosed, any ordinary `query`, `simple_query`, `begin`, `commit`, `rollback`, `open_portal`, or pool lease return on `SessionHandle` fails immediately with `PostgresError::Interface`.
- **Pause and resume mid-`Execute`**: Calling `start_fetch` while a previous `Execute` has unread `DataRow` / `PortalSuspended` frames returns `Err(PostgresError::Interface)` with zero wire frames; calling `next_event()` resumes the existing response stream.
- **Decode or server `ErrorResponse` mid-stream**: Drains boundedly to `ReadyForQuery`, rolls back only the portal's `TransactionScope` (owned transaction -> `ROLLBACK`; ambient `T` -> `ROLLBACK TO SAVEPOINT` + `RELEASE SAVEPOINT`), marks the portal closed, and surfaces the canonical `PostgresError` (retiring the socket on framing/cleanup failure).
- **Interleaved asynchronous frames**: `NoticeResponse`, `ParameterStatus`, and `NotificationResponse` arriving between `DataRow`, `PortalSuspended`, and `CloseComplete` are handled through the shared session dispatcher without desynchronizing portal state.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/portal_protocol.rs` using a stateful loopback wire peer that destroys unnamed/unscoped portals if `ReadyForQuery('I')` ever occurs before exhaustion and counts exact `Execute` / `Close` / `Sync` frames.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Bounded `fetch` across `PortalSuspended` | Idle session opens portal over 5 rows; calls `fetch(2)`, `fetch(2)`, `fetch(2)` | Auto-emits `"BEGIN"` (`'T'`); chunk 1 returns 2 rows (`Suspended` -> `'T'`), chunk 2 returns 2 rows (`Suspended` -> `'T'`), chunk 3 returns 1 row + `CommandComplete("SELECT 5")`, commits owned read scope (`'I'`), `is_exhausted() == true`. |
| Exact boundary with empty final chunk | 4 rows total; `fetch(2)` -> 2 rows (`Suspended`), `fetch(2)` -> 2 rows (`Suspended`), `fetch(2)` -> 0 rows (`CommandComplete`) | Final `fetch(2)` returns 0 rows with preserved `columns` metadata, marks `is_exhausted() == true`, and restores `'I'`. |
| Row-by-row pause & resume (`next_event`) | `start_fetch(2)`, read 1 `PortalEvent::Row` via `next_event()`, call `start_fetch(2)` (rejected), then call `next_event()` twice | Second `start_fetch` returns `Err(PostgresError::Interface)` with no wire `Execute`; subsequent `next_event()` yields row 2 then `PortalEvent::Suspended` from the original `Execute`. |
| Ambient transaction savepoint isolation | Caller runs `begin()` + `INSERT`, opens portal, reads 1 batch, calls `portal.close()` early | Portal opens `SAVEPOINT vtb_sp_<n>`, closes `Close(b'P')`, rolls back/releases only `vtb_sp_<n>`, and leaves caller transaction alive in `'T'` with the `INSERT` intact. |
| Concurrent query & premature release blocked | Open active `Portal`; attempt `session.query("SELECT 1", ...)` on same `SessionHandle` | Fails immediately with `Err(PostgresError::Interface)` before writing to the socket; succeeds after `portal.close().await`. |
| Server error & `Drop` without `close()` | Drop unexhausted `Portal` (and separately inject `ErrorResponse("22012")` on row 2) | `ErrorResponse` drains to `'E'`, rolls back portal scope, and surfaces SQLSTATE `"22012"`; dropping an active `Portal` schedules background cleanup/retirement without panicking or leaking busy state. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test portal_protocol
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test portal_protocol
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `Portal` bounds per-fetch memory via `Execute(max_rows)` and exposes both `start_fetch` / `next_event` (`PortalEvent`) and `fetch` -> `QueryResult` without buffering complete result sets (R16).
- [ ] Portals opened at `'I'` own a streaming transaction and portals opened at `'T'` own a nested savepoint so intermediate `Sync` frames remain in `'T'` without committing ambient caller work (R16).
- [ ] Concurrent session operations are rejected while a portal is active, and early `close()`, error recovery, or `Drop` never leaks a transaction or returns a dirty session (R16).
- [ ] `portal_protocol` integration tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen portal streaming and pinned-lease contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL portal `Execute(max_rows)`, `PortalSuspended`, and transaction-pinning protocol rules.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::db_runtime_handle` — Shared multi-threaded Tokio database runtime handle.
