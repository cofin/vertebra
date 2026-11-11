---
type: Task
id: "native-postgres-driver:ch16:t03"
chapter_id: ch16-pipeline-scope-and-cancellation
title: "Implement protocol CancelRequest engine, connection retirement, and non-aborting async cancel callback"
description: "Implement generation-fenced protocol CancelRequest dialing and mandatory cancel-sent connection retirement in vertebra-postgres plus spawn_rust_db_task_with_attr_and_cancel in vertebra-db."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch16:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch16-pipeline-scope-and-cancellation/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-db/src/py_async.rs
files:
  - src/rs/vertebra-postgres/src/cancel.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-db/src/py_async.rs
tests:
  - src/rs/vertebra-postgres/tests/cancellation_protocol.rs
  - src/rs/vertebra-db/src/py_async.rs
estimated_loc: 250
plan_revision: 5
plan_commit: 0147d80c
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T19:32:00Z"
last_verified_commit: 1ae9ab3d
verification_evidence: "cargo test -p vertebra-db --features pyo3 (49 passed) + cargo test -p vertebra-postgres --test cancellation_protocol (6 passed)"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: e8b397b4
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen generation-fenced cancellation, delayed-cancel retirement, and ambient transaction failure contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL protocol 3.0 32-bit and protocol 3.2 variable-length 256-bit CancelRequest framing and PgBouncer forwarding
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::CancelHandle
    title: Shared Rust Connection::cancel_handle and CancelHandle trait
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared PyO3 Tokio completion bridge and AbortHandle setup guard
tags:
  - postgres
  - cancellation
  - protocol
  - retirement
  - rust
---

# Task t03: Implement Protocol `CancelRequest` Engine, Connection Retirement, and Non-Aborting Async Cancel Callback

## Objective & Scope Boundary

Implement generation-fenced protocol `CancelRequest` cancellation and mandatory cancel-sent connection retirement in `vertebra-postgres` (`cancel.rs`, `session.rs`), plus `spawn_rust_db_task_with_attr_and_cancel` in `vertebra-db` (`py_async.rs`) so Python `Future` cancellation can trigger protocol cancellation without aborting the cleanup-owning Tokio worker [^pg-contracts] [^pg-protocol-research] [^py-async-rs]. Wiring `SyncPostgresClient.cancel()` and `AsyncPostgresClient.cancel()` onto PyO3 clients and `_postgres.pyi` lands immediately next in `ch16:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 16 `PostgresCancelHandle`, `OperationGeneration`, mandatory `retire_on_finish`, and `spawn_rust_db_task_with_attr_and_cancel` contracts.
- [contracts.md](../../../contracts.md) — Generation fencing, delayed-cancel retirement, ambient transaction failure persistence, and cleanup budget rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Protocol 3.0 (`32`-bit PID/secret via `frontend::cancel_request(pid, key)`) vs PostgreSQL 18 Protocol 3.2 variable-length (`256`-bit) cancellation keys, PgBouncer synthetic keys, and zero-response cancel socket semantics [^pg-protocol-research].
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `PostgresConnection`, `BackendKeyData`, endpoint/TLS/cloud dialer config, and `PooledConnection::mark_retire`.
- `src/rs/vertebra-db/src/connection.rs::{Connection, CancelHandle, TransportStream}` — Shared `CancelHandle` trait and direct/TLS/Cloud SQL/AlloyDB transport dialers [^pg-conn-rs].
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr`, `DbTaskCanceller`, and the `ch07:t01` abort-on-setup-failure worker guard [^py-async-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/cancel.rs` and `src/rs/vertebra-postgres/src/session.rs` (mounted as canonical `crate::session::cancel` via `#[path = "cancel.rs"] pub mod cancel; pub use cancel::{CancelDisposition, OperationGeneration, PostgresCancelHandle};` inside `src/rs/vertebra-postgres/src/session.rs` — `rustc` 2021/2024 resolves `#[path = "cancel.rs"]` relative to `session.rs`'s parent `src/` directory, whereas unannotated `pub mod cancel;` looks in `src/session/` and `"../cancel.rs"` escapes `src/`; never declare a duplicate `mod cancel;` in `src/lib.rs`):
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct OperationGeneration(pub u64);`
  - `pub enum CancelSecret { V30(i32), Variable(Box<[u8]>) }` (Rust-private `BackendKeyData` storing `process_id: i32` + `CancelSecret`: encodes canonical Protocol 3.0 `32`-bit `postgres_protocol::message::frontend::cancel_request(pid, key)` across PostgreSQL 14–18, and frames `16 + secret.len()` variable-length up-to-`256`-bit `80877102` `CancelRequest` if a variable-length Protocol 3.2 key is ever held, while never exposing `process_id` or secret bytes to Python) [^pg-protocol-research].
  - `pub enum CancelDisposition { IdleNoOp, MissingBackendKey, Sent, TransportError }`
  - `pub struct PostgresCancelHandle` implementing `vertebra_db::connection::CancelHandle` (`Connection::cancel_handle` on `PostgresConnection` / `SessionHandle`):
    - Claims cancellation ONLY for an active `OperationGeneration` (idle session or already-finished generation is an immediate `IdleNoOp` with **zero** network sockets opened).
    - Dials a **fresh** transport stream to the exact validated original endpoint/dialer/pooler (`SSLRequest` + verified TLS for direct TLS, outer TLS 1.3 mTLS for Cloud SQL, outer TLS 1.3 mTLS + `MetadataExchange` for AlloyDB, or Unix socket — never reusing the busy query socket, never sending `StartupMessage` or passwords, and never bypassing a PgBouncer endpoint), writes the `CancelRequest`, flushes, and closes under `connect_timeout`.
    - **Mandatory retirement rule**: the moment `CancelRequest` may have been sent (`dial` started/succeeded for the claimed generation), `SessionHandle` irreversibly marks the physical connection `retire_on_finish` (`PooledConnection::mark_retire`) and serializes claim/send with operation completion so **no** subsequent query or next pool borrower can ever run on that physical socket and get hit by a delayed backend `SIGINT` [^pg-contracts] [^pg-protocol-research].
    - Retains the sole original reader to drain boundedly to `ReadyForQuery` under the cleanup deadline (`57014` `QueryCancelledError` when cancel wins, normal completion when the query already finished; immediate socket close/retire on EOF, partial framing, or drain timeout). If an ambient transaction was active, marks it logically failed until explicit `rollback()` clears it.
- `src/rs/vertebra-db/src/py_async.rs`:
  - Add `pub fn spawn_rust_db_task_with_attr_and_cancel<F, T, E, MapOk, MapErr, OnCancel>(py: Python<'_>, fut: F, map_ok: MapOk, map_err: MapErr, on_cancel: OnCancel, attr_name: &'static str) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` where `OnCancel: FnOnce() + Send + 'static`:
    - Preserves `spawn_rust_db_task_with_attr` unchanged (default `AbortHandle::abort()` behavior for other database crates).
    - Protects callback allocation and `future.add_done_callback(...)` registration with the armed setup-failure guard from `ch07:t01` (if registration fails before monitor handoff, aborts and drops the spawned worker immediately).
    - Once registered, cancelling the Python `asyncio.Future` invokes `on_cancel()` at most once **without** calling `AbortHandle::abort()` on the Tokio worker, allowing native PostgreSQL drain + connection retirement to complete independently after the Python `Future` resolves to `CancelledError` [^py-async-rs].

## Edge Cases & Error Semantics

- **Repeated `cancel()` calls**: Idempotent; at most one `CancelRequest` dial claims a given `OperationGeneration`, and post-completion `cancel()` calls are `IdleNoOp`.
- **Missing `BackendKeyData` (e.g. server omitted `BackendKeyData`)**: Marks active session `retire_on_finish` / closes cleanly under cleanup deadline without panicking or fabricating wire bytes.
- **Cancel socket failure / timeout**: Original query drain/retirement still proceeds boundedly under `connect_timeout` without hanging the caller.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/cancellation_protocol.rs` and `src/rs/vertebra-db/src/py_async.rs` (`py_async::tests`, including exact named tests `cancel_callback_runs_without_aborting_cleanup`, `legacy_abort_cancels_worker`, and `registration_failure_drops_worker`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Idle cancel (`IdleNoOp`) | Call `session.cancel()` when no operation is active | `0` cancel TCP connections opened; session remains clean and reusable without retirement. |
| Active query `CancelRequest` (`57014`) | Start slow query (`BackendKeyData(pid=4242, key=0x12345678)`), trigger `PostgresCancelHandle` | Fresh second connection sends exact 16-byte `80877102` `CancelRequest(4242, 0x12345678)` with zero `StartupMessage`; query drains `ErrorResponse("57014")` + `'I'` and retires socket. |
| Delayed cancel loses race to completion | Hold cancel dial behind `Barrier`, complete query `SELECT 1`, release barrier, then acquire from `max_size=1` pool | Query returns `Ok(1)`, physical socket `pid=4242` retires on finish, and next pool `acquire()` dials fresh `pid=4243` (delayed cancel never hits borrower 2). |
| Ambient transaction failure on cancel | `begin()`, cancel in-flight query (`57014` -> `'E'`), attempt next `query("SELECT 1")` before `rollback()` | Next `query` fails until explicit `rollback()`, which closes/retires the cancelled socket cleanly. |
| `py_async` custom cancel vs legacy abort & setup failure | Run `cancel_callback_runs_without_aborting_cleanup`, `legacy_abort_cancels_worker`, and `registration_failure_drops_worker` | Custom callback runs once while worker finishes cleanup; `spawn_rust_db_task_with_attr` still aborts worker on `Future.cancel()`; failing `add_done_callback` aborts/drops worker immediately. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test cancellation_protocol
  ```
- Final Green, named-test listing check, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests -- --list
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test cancellation_protocol
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --all-targets -- -D warnings
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `PostgresCancelHandle` sends exact `BackendKeyData` `CancelRequest` packets over a fresh endpoint/TLS/cloud connection with generation fencing and zero Python secret exposure (R18).
- [ ] Every session for which `CancelRequest` may have been sent retires before any subsequent operation or pool borrower, and ambient transactions stay failed until explicit `rollback()` (R18).
- [ ] `py_async::tests` lists and executes `cancel_callback_runs_without_aborting_cleanup`, `legacy_abort_cancels_worker`, and `registration_failure_drops_worker` under `--features pyo3` (R7, R18).
- [ ] `cancellation_protocol`, `py_async::tests`, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen generation-fenced cancellation and mandatory session retirement contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Protocol 3.0 `32`-bit and Protocol 3.2 `256`-bit `CancelRequest` framing and PgBouncer cancellation constraints.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::CancelHandle` — Shared Rust `CancelHandle` trait and transport dialers.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared PyO3 Tokio completion bridge.
