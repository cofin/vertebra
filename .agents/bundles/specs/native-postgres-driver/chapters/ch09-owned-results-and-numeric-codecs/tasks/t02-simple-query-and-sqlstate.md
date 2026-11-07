---
type: Task
id: "native-postgres-driver:ch09:t02"
chapter_id: ch09-owned-results-and-numeric-codecs
title: Implement multi-result simple query execution and canonical SQLSTATE error draining
description: Mount pub mod query in lib.rs and execute PostgreSQL simple Query exchanges via SimpleQueryCollector and PostgresConnection::simple_query, parsing command-tag row counts and draining ServerError exchanges through ReadyForQuery.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch09:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch09-owned-results-and-numeric-codecs/spec.md
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/errors.rs
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/query_protocol.rs
estimated_loc: 235
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
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Simple query multi-result, command tag, and SQLSTATE error contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 simple-query framing, unnamed statement invalidation, and error draining
  - id: py-db-errors
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical shared DatabaseError subclass hierarchy in vertebra._vertebra
tags:
  - postgres
  - rust
  - protocol
  - sqlstate
---

# Task t02: Implement Multi-Result Simple Query Execution and Canonical SQLSTATE Error Draining

## Objective & Scope Boundary

Mount `pub mod query;` directly in `src/rs/vertebra-postgres/src/lib.rs` and implement real PostgreSQL Protocol 3.0 simple-query execution (`SimpleQueryCollector` in `query.rs` and `PostgresConnection::simple_query` in `session.rs`), retaining every ordered result set, parsing checked `u64` affected row counts from documented command tags, constructing structured `ServerError` diagnostics over the upfront `errors.rs` contract (`ch01:t04` / `ch05:t02`), and draining recoverable `ErrorResponse` exchanges cleanly through `ReadyForQuery` [^pg-contracts] [^pg-research] [^py-db-errors]. Typed scalar/numeric cell decoding (`ch09:t03`–`ch09:t04`) and extended `Parse`/`Bind`/`Execute` queries (`ch12:t03`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Multi-result `query_multi` / `simple_query` ordering, command-tag counts, and SQLSTATE exception identity [^pg-contracts].
- [spec.md](../spec.md) — Chapter 09 `SimpleQueryCollector`, `PostgresConnection::simple_query`, and `ServerError` contracts.
- `src/rs/vertebra-postgres/src/lib.rs` — Crate root module declarations where `pub mod query;` and `pub use query::SimpleQueryCollector;` are mounted.
- `src/rs/vertebra-postgres/src/result.rs` — `QueryResult`, `Column`, `Format`, `TransactionStatus`, and `PgValue`.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, upfront `TransactionStatus::{Idle, InTransaction, Failed}` (`ch02:t02`), `protocol::read_message` integration, `ParameterStatus` map, and socket retirement.
- `src/rs/vertebra-postgres/src/errors.rs` — Upfront `ServerError { severity, sqlstate, message, diagnostics }`, `PostgresError::Server(ServerError)`, `PostgresError::sqlstate(&self) -> Option<&str>`, and `postgres_error_to_py` mapping onto `vertebra._vertebra` exception classes [^py-db-errors].

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/lib.rs`**:
  - Declare `pub mod query;` directly at crate root and re-export `pub use query::SimpleQueryCollector;`.
- **`src/rs/vertebra-postgres/src/query.rs`**:
  - `pub struct SimpleQueryCollector` correlating `RowDescription`, `DataRow`, `CommandComplete`, and `EmptyQueryResponse` frames into owned `Vec<QueryResult>`.
  - `pub(crate) fn parse_error_response(body: &postgres_protocol::message::backend::ErrorResponseBody<'_>) -> Result<ServerError, PostgresError>` extracting structured `severity`, 5-byte ASCII `sqlstate` (`C`), primary `message` (`M`), and diagnostic tag/value pairs into the upfront `crate::errors::ServerError` model (`PostgresError::Server(ServerError)`).
  - `pub(crate) fn parse_command_tag_affected_rows(tag: &str) -> Result<u64, PostgresError>` parsing checked `u64` trailing counts ONLY for documented row-bearing tags (`INSERT <oid> <rows>`, `DELETE <rows>`, `UPDATE <rows>`, `SELECT <rows>`, `MOVE <rows>`, `FETCH <rows>`, `COPY <rows>`, `MERGE <rows>`) and returning `Ok(0)` for DDL/utility/empty tags (`CREATE TABLE`, `ALTER`, `BEGIN`, `COMMIT`, `VACUUM`, `""`).
  - `pub(crate) fn parse_row_description(body: &postgres_protocol::message::backend::RowDescriptionBody<'_>) -> Result<Vec<Column>, PostgresError>` validating positive column OIDs (`>= 1`) and `Format::from_code`.
  - Until `ch09:t03` installs `codecs::decode_value`, non-null cells validate UTF-8 and construct `PgValue::Text(String)`, while SQL `NULL` (`len == -1`) constructs `PgValue::Null`.
- **`src/rs/vertebra-postgres/src/session.rs`**:
  - Track `unnamed_statement_valid: bool` on `PostgresConnection` and set it to `false` on every `Query` frame emission (because PostgreSQL simple `Query` destroys the unnamed prepared statement).
  - Expose central `pub(crate) fn dispatch_notification(&mut self, pid: i32, channel: String, payload: String)` hook and live `ParameterStatus` update handling during query reads, updating `PostgresConnection`'s upfront `TransactionStatus` (`Idle`, `InTransaction`, `Failed`) solely from `ReadyForQuery`.
  - Implement `pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PostgresError>` encoding `postgres_protocol::message::frontend::query(sql, &mut buf)`, flushing once, and reading via `protocol::read_message` through `ReadyForQuery`.

## Edge Cases & Error Semantics

- **Mid-batch `ErrorResponse` draining**: When a multi-statement `Query` (`"SELECT 1; SELECT 1/0; SELECT 2"`) emits `ErrorResponse`, retain the first `ServerError`, discard any partial in-progress result set, continue consuming interleaved `NoticeResponse` / `ParameterStatus` / `NotificationResponse` frames until `ReadyForQuery(status)` arrives, record the actual `TransactionStatus` (`Idle`, `InTransaction`, or `Failed`) on `PostgresConnection`, and return `Err(PostgresError::Server(err))`.
- **Post-error session reuse vs failed ambient transaction**: After a recoverable error drained to `ReadyForQuery(b'I')`, the next `conn.simple_query("SELECT 9")` succeeds on the same socket. Inside an ambient transaction, `ReadyForQuery(b'E')` leaves `conn.transaction_status() == TransactionStatus::Failed` until explicit `ROLLBACK`.
- **Fatal framing & ordering violations**: Unexpected `DataRow` without preceding `RowDescription`, column count mismatch between `DataRow` and `RowDescription`, negative field length `< -1`, invalid UTF-8 in text cell, unexpected extended-only frame (`BindComplete`, `PortalSuspended`), or socket EOF before `ReadyForQuery` permanently marks `PostgresConnection` broken/closed and returns `PostgresError::Protocol` or `PostgresError::Transport`.
- **Embedded NUL in SQL**: `frontend::query` NUL-byte rejection surfaces as `PostgresError::Interface` / `PostgresError::Data` before writing any frame to the socket.
- **No false commit inference**: `CommandComplete` never implies transaction commit; only `ReadyForQuery` sets `TransactionStatus`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/query_protocol.rs` (loopback scripted-peer socket fixture exercising public `PostgresConnection::simple_query`, `SimpleQueryCollector`, and `PostgresConnection::transaction_status`, zero mocks).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Multi-statement heterogeneous schemas & DML/DDL tags | `"CREATE TEMP TABLE t(a int); INSERT INTO t VALUES (1),(2); UPDATE t SET a=3; MERGE INTO t ...; SELECT a, 'ok' FROM t"` | Emits exact `'Q'` frame; returns 5 ordered `QueryResult` entries with `affected_rows` `[0, 2, 2, 1, 2]`, exact `command_tag` strings, `PgValue::Text` / `PgValue::Null` rows, and `TransactionStatus::Idle`. |
| Empty query string | `conn.simple_query("")` against peer returning `EmptyQueryResponse` + `ReadyForQuery(b'I')` | Returns 1 `QueryResult` with `columns == []`, `rows == []`, `affected_rows == 0`, `command_tag == ""`, `TransactionStatus::Idle`. |
| Interleaved `NoticeResponse`, `ParameterStatus`, & `NotificationResponse` | Peer interleaves `NoticeResponse`, `ParameterStatus("TimeZone", "UTC")`, and `NotificationResponse(42, "ch", "p")` mid-`DataRow` stream | `simple_query` ignores notice without failing, updates `conn.parameters()["TimeZone"] == "UTC"`, records notification via session hook, and returns intact rows. |
| Mid-statement `ErrorResponse` & reuse at `I` vs `E` | `"SELECT 1; SELECT 1/0; SELECT 2"` returning `ErrorResponse(C="22012", S="ERROR", M="division by zero")` + `ReadyForQuery(b'I')`, followed by `conn.simple_query("SELECT 9")` | First call returns `Err(PostgresError::Server)` with `err.sqlstate() == Some("22012")`, `conn.transaction_status() == Idle`; second call returns `"9"`. Under `ReadyForQuery(b'E')`, `transaction_status() == Failed`. |
| Structured `ServerError` diagnostics & SQLSTATE classes | `ErrorResponse` frames carrying `"22012"`, `"23505"`, `"42601"`, `"08006"`, `"40001"`, `"40P01"`, `"57014"`, `"XX000"` with diagnostic fields | Preserves exact `sqlstate`, `severity`, primary `message`, and diagnostic tags in `PostgresError::Server(ServerError)`, matching canonical `vertebra._vertebra` exception categories. |
| Protocol violation retires socket | Peer sends `DataRow` with 3 fields after a 2-column `RowDescription`, or closes before `ReadyForQuery` | Returns `Err(PostgresError::Protocol)` / `Err(PostgresError::Transport)` and marks connection broken so subsequent `simple_query` fails immediately. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test query_protocol`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test query_protocol && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `src/rs/vertebra-postgres/src/lib.rs` mounts `pub mod query;` directly and `PostgresConnection::simple_query` executes multi-statement `Query` exchanges preserving ordered schemas, rows, command tags, `u64` affected counts, and `Idle`/`InTransaction`/`Failed` status (R10, R12).
- [ ] Recoverable `ErrorResponse` frames drain cleanly through `ReadyForQuery` while malformed framing or premature EOF permanently retires the socket (R10, R12).
- [ ] `ServerError` preserves SQLSTATE, severity, sanitized message, and diagnostic fields over the upfront `errors.rs` contract without duplicate exception hierarchies (R10).
- [ ] Every `simple_query` emission invalidates local unnamed-prepared-statement state and routes asynchronous notifications through one central session hook (R12).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Simple query multi-result, command tag, and SQLSTATE error contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 simple-query framing, unnamed statement invalidation, and error draining.
[^py-db-errors]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical shared `DatabaseError` subclass hierarchy in `vertebra._vertebra`.
