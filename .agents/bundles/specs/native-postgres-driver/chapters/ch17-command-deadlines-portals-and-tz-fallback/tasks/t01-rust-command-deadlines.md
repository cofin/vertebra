---
type: Task
id: "native-postgres-driver:ch17:t01"
chapter_id: ch17-command-deadlines-portals-and-tz-fallback
title: "Implement absolute Rust command I/O deadlines and bounded cancel retirement"
description: "Bound SessionHandle and PostgresConnection timed queries via an RAII active_deadline on PostgresConnection with OperationalError precedence, automatic AfterCompletion preservation, and connect_timeout-capped cancel cleanup."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch16:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/cancel.rs
files:
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/extended.rs
tests:
  - src/rs/vertebra-postgres/tests/command_deadlines.rs
estimated_loc: 245
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
    title: Frozen single-Instant command deadline, OperationalError precedence, and enrichment fallback contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL protocol cancellation, ReadyForQuery drain, and post-completion metadata boundaries
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical OperationalError and QueryCancelledError distinction
tags:
  - postgres
  - deadlines
  - cancellation
  - query
  - rust
---

# Task t01: Implement Absolute Rust Command I/O Deadlines and Bounded Cancel Retirement

## Objective & Scope Boundary

Implement `CommandBudget`, RAII-scoped `active_deadline: Option<tokio::time::Instant>` on `PostgresConnection`, and `SessionHandle::{query_with_deadline, simple_query_with_deadline}` in `vertebra-postgres` (`session.rs`, `query.rs`, `extended.rs`) so an explicitly timed query bounds all command I/O — including `TypeCatalog::discover`, `metadata::resolve_parameter_metadata`, primary query execution, and `metadata::normalize_materialized_result` — under **one** non-renewing `tokio::time::Instant` without modifying `metadata.rs` or `types.rs`, surfaces canonical `OperationalError` (`sqlstate = None`) on client expiry, skips optional post-`ReadyForQuery` enrichment when the budget is exhausted, preserves `AfterCompletion.completed_result` automatically on fatal active enrichment, and runs `connect_timeout`-capped protocol cancellation + socket retirement via `crate::session::cancel` without SQL replay [^pg-contracts] [^pg-protocol-research]. Python `SyncPostgresClient` / `AsyncPostgresClient` `timeout=` forwarding and live `postgres_service` tests land immediately next in `ch17:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 17 `CommandBudget`, RAII `active_deadline`, deadline precedence, and enrichment fallback contracts.
- [contracts.md](../../../contracts.md) — Single-`Instant` reservation budget, `OperationalError` (`sqlstate=None`) vs server `57014` `QueryCancelledError`, and `connect_timeout`-capped cleanup invariants [^pg-contracts].
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, `SessionHandle`, `SessionAccess::Ordinary`, and `crate::session::cancel` integration.
- `src/rs/vertebra-postgres/src/query.rs` — `PostgresConnection::simple_query` and sole backend frame reader used by simple queries and `TypeCatalog::discover`.
- `src/rs/vertebra-postgres/src/extended.rs` — `PostgresConnection::query`, `ExtendedQueryCollector` / `query_with_row_sink`, pre-`Parse` `metadata::resolve_parameter_metadata`, and post-`ReadyForQuery` `metadata::normalize_materialized_result` (`AfterCompletion` wrapper).
- `src/rs/vertebra-postgres/src/cancel.rs` (`crate::session::cancel`) — Existing `PostgresCancelHandle`, `OperationGeneration`, and `retire_on_finish` cleanup from `ch16:t03` [^db-errors-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/session.rs`:
  - `#[derive(Clone, Copy, Debug, Default)] pub struct CommandBudget(Option<tokio::time::Instant>);` (`crate::session::CommandBudget`):
    - `pub fn untimed() -> Self` (`CommandBudget(None)`)
    - `pub fn validate_optional_seconds(timeout: Option<f64>) -> Result<Option<Duration>, PostgresError>` — `None` -> `Ok(None)`; rejects `<= 0.0`, `NaN`, `+-infinity`, or `Duration` / `Instant::checked_add` overflow with `PostgresError::Interface` (`InterfaceError`) before any operation/lease reservation.
    - `pub fn start_from_duration(duration: Option<Duration>) -> Result<Self, PostgresError>` — computes the single absolute `tokio::time::Instant` once when operation reservation succeeds.
    - `pub fn deadline(&self) -> Option<tokio::time::Instant>` and `pub fn is_expired(&self) -> bool`.
  - Store `active_deadline: Option<tokio::time::Instant>` on `PostgresConnection` with `pub fn active_deadline(&self) -> Option<tokio::time::Instant>` and RAII `PostgresConnection::with_command_deadline` (`CommandDeadlineGuard` restoring `active_deadline = None` on drop so subsequent untimed calls and cancel/drain cleanup never inherit a stale deadline) plus `PostgresConnection::{query_with_deadline, simple_query_with_deadline}` convenience wrappers.
  - Add `SessionHandle::query_with_deadline(&self, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>, budget: CommandBudget) -> impl Future<Output = Result<QueryResult, PostgresError>> + Send` and `SessionHandle::simple_query_with_deadline(&self, sql: &str, budget: CommandBudget) -> impl Future<Output = Result<Vec<QueryResult>, PostgresError>> + Send` (with `SessionHandle::query` and `SessionHandle::simple_query` delegating with `CommandBudget::untimed()`):
    - When in-flight command I/O expires at `active_deadline`: clears `active_deadline`, claims only the active `OperationGeneration` (if no wire I/O ever started, sends **zero** `CancelRequest` packets), invokes `crate::session::cancel`'s existing cancel dialer + original-reader drain/retirement from `ch16:t03` under a **single** separate absolute cleanup cap of `Instant::now() + config.connect_timeout` (covering cancel transport dial/write and original socket drain/close together, closing immediately on partial-frame uncertainty or silent peer timeout), marks any ambient transaction failed until `rollback()`, and returns `PostgresError::Operational("command I/O deadline exceeded".into())` (`sqlstate = None`, `vendor_code = None`) — preserving `AfterCompletion` if wrapped during post-`ReadyForQuery` enrichment [^pg-contracts] [^db-errors-rs].
- `src/rs/vertebra-postgres/src/query.rs`:
  - Enforce `self.active_deadline()` inside `PostgresConnection::simple_query(&mut self, sql: &str)` across outbound `Query` write/flush and **all** result sets through `ReadyForQuery` using the sole existing protocol parser (never starting a fresh per-frame or per-result timer). Because `TypeCatalog::discover` and metadata catalog queries execute on `&mut PostgresConnection`, they automatically obey `active_deadline` without modifying `metadata.rs` or `types.rs`.
- `src/rs/vertebra-postgres/src/extended.rs`:
  - Enforce `self.active_deadline()` inside `ExtendedQueryCollector` / `query_with_row_sink` and `PostgresConnection::query`:
    1. Required pre-`Parse` custom parameter catalog discovery (`metadata::resolve_parameter_metadata` runs on `&mut PostgresConnection` under `active_deadline`; expiry before original query emission has **no** `completed_result`).
    2. Outbound extended group write/flush and inbound response read through original `ReadyForQuery` under `active_deadline`.
    3. Optional post-`ReadyForQuery` cold text result metadata enrichment: if `self.active_deadline()` is already exhausted (`Instant::now() >= d`) once original `ReadyForQuery` is recorded, **skip** calling `metadata::normalize_materialized_result` I/O and return the completed `QueryResult` immediately using exact owned text + column OID fallback and captured `CodecContext`. If `active_deadline` expires *during* active `metadata::normalize_materialized_result` catalog I/O, `normalize_materialized_result`'s existing post-`ReadyForQuery` error wrapper automatically wraps the `PostgresError::Operational` deadline failure inside `PostgresError::AfterCompletion { source, completed_result }` preserving the **original** `completed_result` without touching `metadata.rs` [^pg-contracts].

## Edge Cases & Error Semantics

- **Explicit error precedence**: Once client `deadline` expiry claims the active `OperationGeneration`, a subsequent server `ErrorResponse("57014")` received while draining the cancelled command MUST NOT overwrite the primary `PostgresError::Operational` (`sqlstate = None`) with `QueryCancelledError` or attach a fake `"57014"` SQLSTATE. Conversely, an actual server `ErrorResponse("57014")` received *before* `deadline` expires remains canonical `ServerError` (`QueryCancelledError`, `sqlstate = "57014"`).
- **Zero SQL replay**: Never replay any timed command, even when expiry races `CommandComplete`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/command_deadlines.rs` using loopback TCP silent/dribbling peers, barriers, and `PostgresPool` (`max_size = 1`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Silent peer & ignored `CancelRequest` | `query_with_deadline("SELECT pg_sleep(10)", ..., Some(now + 40ms))` against peer that accepts `CancelRequest(pid, key)` and stays silent past `connect_timeout = 80ms` | Sends exact `CancelRequest(pid, key)`, closes/retires old socket within `40ms + 80ms` bound, returns `PostgresError::Operational` (`sqlstate() == None`), and next pool `acquire()` connects with a fresh backend PID. |
| Dribbled frames never renew budget | `simple_query_with_deadline("SELECT 1; SELECT 2", Some(now + 60ms))` where peer sends `DataRow` every `25ms` for `120ms` | Expires at ~`60ms` (does not reset per frame or per result set), cancels/retires once, zero replay. |
| Cleanup `57014` vs earlier server `57014` | (a) Client deadline expires first, then peer replies `ErrorResponse("57014")` on `CancelRequest`; (b) peer replies `ErrorResponse("57014")` at `10ms` before `500ms` deadline | (a) Returns `PostgresError::Operational` (`sqlstate() == None`); (b) returns `PostgresError::Server` with `sqlstate() == Some("57014")`. |
| Pre-`Parse` discovery vs post-`ReadyForQuery` enrichment | (a) Expire during pre-`Parse` `PgVector` OID lookup; (b) budget exhausted right at `ReadyForQuery` before optional custom result enrichment; (c) expire mid-enrichment with EOF | (a) `Operational` error with `0` original `Parse`/`Execute` and no `AfterCompletion`; (b) returns `Ok(QueryResult)` with exact text/OID fallback and `0` catalog queries; (c) returns `AfterCompletion` wrapping `Operational` + original `completed_result`. |
| `deadline = None` & invalid seconds | `query_with_deadline(..., None)` held behind barrier; `CommandBudget::validate_optional_seconds(Some(0.0 | -1.0 | f64::NAN | f64::INFINITY))` | `None` stays pending until barrier releases and succeeds; invalid floats fail with `PostgresError::Interface` before reservation. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test command_deadlines
  ```
- Final Green, regression, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test command_deadlines
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test cancellation_protocol
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SessionHandle::{query_with_deadline, simple_query_with_deadline}` and RAII `PostgresConnection::with_command_deadline` (`active_deadline`) bound parameter discovery, writes/flush, all result frames, and metadata enrichment under one non-renewing `Option<tokio::time::Instant>` while `query` / `simple_query` delegate with `CommandBudget::untimed()` (R10, R12).
- [ ] Client deadline expiry claims `OperationGeneration`, preserves `Operational` (`sqlstate = None`) over cleanup `57014` (while keeping earlier server `57014` intact), bounds `crate::session::cancel` cleanup + retirement by `connect_timeout`, and never replays SQL (R10, R18).
- [ ] Post-`ReadyForQuery` optional enrichment skips I/O on exhausted budgets using exact owned text/OID fallback and automatically preserves `AfterCompletion.completed_result` through `normalize_materialized_result` on fatal active enrichment (R12).
- [ ] `command_deadlines` and `cancellation_protocol` tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen command deadline, `OperationalError` precedence, and enrichment fallback contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL protocol cancellation and post-`ReadyForQuery` metadata rules.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `OperationalError` and `QueryCancelledError` classes.
