---
type: Task
id: "native-postgres-driver:ch13:t01"
chapter_id: ch13-transactions-portals-and-statement-cache
title: "Enforce pinned transaction and numeric savepoint command state"
description: "Track actual server I/T/E transaction state, numeric savepoint identifiers, command-tag transitions, and lock-free logical owner pinning in vertebra-postgres."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch12:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch13-transactions-portals-and-statement-cache/spec.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/rs/vertebra-postgres/src/transaction.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/transaction_state.rs
estimated_loc: 215
plan_revision: 6
plan_commit: 8833b064
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
    title: Frozen native PostgreSQL transaction and pinned-session contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL ReadyForQuery I/T/E and command-tag semantics
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::Connection
    title: Shared Rust database connection lifecycle trait
tags:
  - postgres
  - transactions
  - savepoints
  - rust
---

# Task t01: Enforce Pinned Transaction and Numeric Savepoint Command State

## Objective & Scope Boundary

Track actual server `ReadyForQuery` (`I` / `T` / `E`) status, command tags, and logical ownership for explicit transactions and nested savepoints in `vertebra-postgres` [^pg-contracts] [^pg-protocol-research]. Bounded portals (`ch13:t02`), prepared statement caching (`ch13:t03`), and `DISCARD ALL` pool reset (`ch14:t02`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 13 transaction/savepoint contracts and recovery checkpoint.
- [contracts.md](../../../contracts.md) — Pinned transaction lease rules, savepoint nesting, and `I`/`T`/`E` status invariants [^pg-contracts].
- `src/rs/vertebra-postgres/src/session.rs` — Existing `PostgresConnection`, `SessionHandle`, and `TransactionStatus` state machine.
- `src/rs/vertebra-postgres/src/query.rs` — Existing simple-query execution and `CommandComplete` / `ReadyForQuery` parsing [^pg-protocol-research].
- [query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — Transaction scoping and lease-pinning invariants [^pg-conn-rs].
- [pyo3-bridge/SKILL.md](../../../../../../skills/pyo3-bridge/SKILL.md) — Lock-free operation reservation rules across Python conversion and GIL reacquisition.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/transaction.rs`:
  - `pub struct TransactionOwner { pub id: u64, pub generation: u64 }`
  - `pub enum TransactionDisposition { Begun, Committed, RolledBack, SavepointCreated(u64), SavepointReleased(u64), RolledBackToSavepoint(u64) }`
  - `pub enum TransactionScopeKind { ExplicitTransaction, NestedSavepoint { token: u64 } }`
  - `pub struct TransactionScope` tracking `owner: TransactionOwner`, `kind: TransactionScopeKind`, and `failed: bool`, used by nested library scopes (portals and pipelines) so nested scopes take a savepoint inside an ambient `T` transaction and never commit caller work.
  - Format internal savepoint SQL identifiers strictly from bounded `u64` numeric tokens (`format!("vtb_sp_{token}")`) — never interpolate caller-supplied strings into `SAVEPOINT`, `RELEASE SAVEPOINT`, or `ROLLBACK TO SAVEPOINT`.
- `src/rs/vertebra-postgres/src/session.rs` and `src/rs/vertebra-postgres/src/lib.rs`:
  - Add `PostgresConnection::begin(&mut self) -> impl Future<Output = Result<TransactionDisposition, PostgresError>> + Send`.
  - Add `PostgresConnection::commit(&mut self) -> impl Future<Output = Result<TransactionDisposition, PostgresError>> + Send`.
  - Add `PostgresConnection::rollback(&mut self) -> impl Future<Output = Result<TransactionDisposition, PostgresError>> + Send`.
  - Add `PostgresConnection::{savepoint, release_savepoint, rollback_to_savepoint}(&mut self, token: u64) -> impl Future<Output = Result<TransactionDisposition, PostgresError>> + Send`.
  - Extend `SessionHandle` with atomic/short-lock reservation (`pub(crate) struct SessionOperationGuard<'a>`, `pub(crate) async fn acquire_operation(&self) -> Result<SessionOperationGuard<'_>, PostgresError>`, and `pub(crate) async fn with_connection<F, Fut, T>(&self, f: F) -> Result<T, PostgresError>`) plus `TransactionOwner` / generation pinning so exclusive session execution holds no Rust `Mutex` across `.await`, Python object conversion, or GIL reacquisition, and sibling crate modules (`portal.rs`, `copy.rs`, `arrow.rs`, `notify.rs`, `replication/mod.rs` in `ch13`–`ch23`) can drive `&mut PostgresConnection` cleanly without re-editing `session.rs`.
  - Re-export `TransactionDisposition`, `TransactionOwner`, and `TransactionScope` from `src/rs/vertebra-postgres/src/lib.rs`.

## Edge Cases & Error Semantics

- **Aborted transaction `COMMIT` (`CommandComplete("ROLLBACK")`)**: In PostgreSQL, sending `COMMIT` while in failed state `E` returns `CommandComplete("ROLLBACK")` and `ReadyForQuery('I')`. `PostgresConnection::commit` inspects both the actual command tag and prior/final `TransactionStatus`, updates session state to `I`, and returns an explicit transaction rollback error (`PostgresError::Operational` / transaction-aborted diagnostic) rather than reporting committed success [^pg-protocol-research].
- **Operations while in `E`**: When `TransactionStatus::Failed` (`'E'`) is active, ordinary queries, `begin`, `savepoint`, and `release_savepoint` fail before or via server state without silently switching physical connections; only `rollback()` (restoring `I`) or `rollback_to_savepoint(token)` for an active savepoint (restoring `T`) clears the logical failed state after confirmed server recovery or closed-socket disposition [^pg-contracts].
- **Cross-owner interference**: If `SessionHandle` has an active pinned `TransactionOwner`, any `begin`, `commit`, `rollback`, `savepoint`, query, or close/release attempt by a different logical owner or concurrent caller fails with `PostgresError::Interface` before touching the wire.
- **Savepoint stack ordering**: Releasing or rolling back to a nonexistent or already-released numeric token fails cleanly without corrupting outer savepoint tracking; rolling back to `t1` invalidates inner savepoints (`t2 > t1`) created after `t1`.
- **Disconnect during `COMMIT` / `ROLLBACK`**: EOF or transport failure before `ReadyForQuery` marks the physical `PostgresConnection` broken/closed and surfaces `ConnectionFailure` without claiming commit or clean rollback.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/transaction_state.rs` using a scripted wire loopback peer that asserts exact frontend SQL frames and controls `CommandComplete` + `ReadyForQuery('I' | 'T' | 'E')` transitions.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Clean explicit transaction | `conn.begin()`, DML query returning `'T'`, `conn.commit()` | Emits `"BEGIN"` -> `'T'`, DML -> `'T'`, `"COMMIT"` -> `CommandComplete("COMMIT")` + `'I'`; returns `TransactionDisposition::Committed` and `conn.transaction_status() == Idle`. |
| `COMMIT` in failed transaction `'E'` | `conn.begin()`, failing query returning `ErrorResponse("23505")` + `'E'`, then `conn.commit()` | Server sends `CommandComplete("ROLLBACK")` + `'I'`; `commit()` returns `Err(PostgresError)` (explicit rollback failure) while updating `transaction_status()` to `Idle`. |
| Recovery gated in `'E'` | Enter `'E'`, attempt `savepoint(1)` then `rollback()` | `savepoint(1)` is rejected while in `'E'`; `rollback()` emits `"ROLLBACK"` -> `'I'` and restores `Idle` usability. |
| Nested numeric savepoints | `begin()`, `savepoint(1)`, `savepoint(2)`, error -> `'E'`, `rollback_to_savepoint(2)`, `release_savepoint(2)`, `commit()` | Emits exact `"SAVEPOINT vtb_sp_1"`, `"SAVEPOINT vtb_sp_2"`, `"ROLLBACK TO SAVEPOINT vtb_sp_2"` (`'E'` -> `'T'`), `"RELEASE SAVEPOINT vtb_sp_2"` (`'T'`), `"COMMIT"` (`'I'`). |
| Outer rollback invalidates inner token | `begin()`, `savepoint(10)`, `savepoint(11)`, `rollback_to_savepoint(10)`, then `release_savepoint(11)` | `release_savepoint(11)` returns `Err(PostgresError::Interface)` because token `11` was popped by rolling back to `10`; ambient `'T'` remains valid. |
| Concurrent / foreign owner conflict | `owner_a` pins `SessionHandle` via `begin()`; `owner_b` attempts `commit()` or `query(...)` | `owner_b` receives `Err(PostgresError::Interface)` with zero frames emitted on `owner_a`'s socket. |
| Disconnect mid-`COMMIT` | Peer drops TCP stream after `"COMMIT"` write before `ReadyForQuery` | `commit()` returns `Err(PostgresError::ConnectionClosed)` and `conn.is_broken()` is `true`. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test transaction_state
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test transaction_state
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `PostgresConnection` and `SessionHandle` track server `I`/`T`/`E` status and command tags accurately, failing explicitly when `COMMIT` yields `ROLLBACK` (R13).
- [ ] Internal savepoint names derive exclusively from bounded numeric `u64` tokens (`vtb_sp_<u64>`), and nested library scopes use savepoints inside ambient `'T'` without committing caller transactions (R13).
- [ ] `SessionHandle` exposes `pub(crate)` exclusive operation-reservation helpers (`SessionOperationGuard` / `with_connection`) for sibling modules without holding a Rust mutex across `.await` or GIL transitions, and never swaps sessions to hide `'E'` (R13).
- [ ] `transaction_state` integration tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen transaction, savepoint, and session-pinning contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL protocol 3.0 `ReadyForQuery` and transaction state transitions.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::Connection` — Shared Rust connection lifecycle interface.
