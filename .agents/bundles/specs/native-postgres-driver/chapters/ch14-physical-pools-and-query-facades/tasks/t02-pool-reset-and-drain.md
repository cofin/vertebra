---
type: Task
id: "native-postgres-driver:ch14:t02"
chapter_id: ch14-physical-pools-and-query-facades
title: "Implement rollback/DISCARD ALL pool reset and draining shutdown"
description: "Release pooled sessions only after bounded drain, rollback, DISCARD ALL, GUC restoration, and cache invalidation, retiring uncertain sockets and draining pool shutdown under a deadline."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch14:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch14-physical-pools-and-query-facades/spec.md
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/statements.rs
  - src/rs/vertebra-db/src/py_async.rs
files:
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/statements.rs
tests:
  - src/rs/vertebra-postgres/tests/pool_reset.rs
estimated_loc: 245
plan_revision: 4
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
updated_at: "2026-11-07T18:00:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL pooled lease reset, pinned ownership, and close drain contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL session state cleanup, statement invalidation, and transaction pooler semantics
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::db_runtime_handle
    title: Tokio database runtime handle for nonblocking Drop reset and drain tasks
tags:
  - postgres
  - pool
  - reset
  - shutdown
  - rust
---

# Task t02: Implement Rollback/`DISCARD ALL` Pool Reset and Draining Shutdown

## Objective & Scope Boundary

Release `PooledConnection` leases back to idle availability only after deterministic drain, `ROLLBACK`, portal closure, `DISCARD ALL`, baseline `session_gucs` restoration, and `StatementCache` / `TypeCatalog` generation invalidation, retiring any uncertain socket and draining `PostgresPool::close` within a bounded deadline [^pg-contracts] [^pg-protocol-research]. Python pool PyO3 bindings (`ch15:t01`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 14 conservative reset, single-permit retirement, and `PoolStats` invariants.
- [contracts.md](../../../contracts.md) — Pooled `client.close()` vs direct `client.close()` semantics and `reset_timeout` / `close_timeout` contracts [^pg-contracts].
- `src/rs/vertebra-postgres/src/pool.rs` — `PostgresPool`, `PooledConnection`, and `PoolStats` from `ch14:t01`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `PostgresConnection`, `TypeCatalog`, and pinned transaction/portal state.
- `src/rs/vertebra-postgres/src/statements.rs` (`crate::session::statements`) — `StatementCache::invalidate_all` and `PoolerMode` from `ch13:t03`.
- `src/rs/vertebra-db/src/py_async.rs::db_runtime_handle` — Background Tokio runtime for nonblocking `Drop` return scheduling [^py-async-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/pool.rs`:
  - `PooledConnection::release(self) -> impl Future<Output = Result<(), PostgresError>> + Send` — executes the bounded reset sequence under `options.reset_timeout` before publishing the connection to `idle` (or retiring it on failure/retirement flag).
  - `PooledConnection::mark_retire(&mut self)` — irreversibly marks the physical connection to close and release its permit on return instead of requeuing.
  - `impl Drop for PooledConnection` — when dropped without explicit `release().await`, transitions slot state to `resetting` and spawns bounded async reset/retirement onto `db_runtime_handle()` without blocking caller threads or holding the GIL [^py-async-rs].
  - `PostgresPool::close(&self, deadline: Duration) -> impl Future<Output = Result<(), PostgresError>> + Send`:
    - Immediately rejects new `acquire()` calls and wakes queued waiters with `PostgresError::Interface` / closed-pool error.
    - Closes all currently `idle` sockets and awaits active `borrowed` / `resetting` leases up to `deadline`.
    - On deadline expiry, marks remaining active leases `retire_on_return` so their permits/sockets clean up when dropped without leaking capacity, and returns `Err(PostgresError::Operational)` (drain timeout).
    - `PoolStats` accurately distinguishes `connecting`, `idle`, `borrowed`, `resetting`, and cumulative `retired`.
- `src/rs/vertebra-postgres/src/session.rs` and `src/rs/vertebra-postgres/src/statements.rs`:
  - Extend `SessionHandle` backing storage to hold either direct `PostgresConnection` ownership or concrete `PooledConnection` ownership (`SessionHandle::from_pooled(PooledConnection)`):
    - `SessionHandle::close(&self)` terminates a direct socket and calls `PooledConnection::release` on a pooled lease once no active portal/pipeline scope pins it; subsequent calls on that `SessionHandle` fail with `PostgresError::Interface`.
    - `commit()` / `rollback()` end a transaction on a retained `SessionHandle` without prematurely returning its `PooledConnection` while the caller holds the client.
  - Exact `Direct` / `Session` reset sequence under `reset_timeout`:
    1. Finish/drain any bounded in-flight response; if unread COPY/replication frames, partial framing, or uncertain cancellation exist, retire immediately.
    2. If `transaction_status()` is `'T'` or `'E'`, execute `ROLLBACK` and verify `ReadyForQuery('I')`.
    3. Close any remaining owned portals (`Close(b'P')`) before `DISCARD ALL`.
    4. Execute `DISCARD ALL` ONLY after confirmed `'I'` (never inside `'T'`/`'E'`, where PostgreSQL rejects `DISCARD ALL` with `25001`), then re-apply configured baseline startup/`session_gucs` (`SET ...`) and bump `StatementCache` + `TypeCatalog` generations (`invalidate_all()`).
    5. In `Transaction` / `Statement` pooler modes, drain and `ROLLBACK` `'T'`/`'E'` + invalidate local statement/type caches without claiming cleanup of unpinned backend session state via `DISCARD ALL` [^pg-protocol-research].
    6. Never fast-path return arbitrary `SELECT` sessions without reset, because SQL functions (`set_config`, `pg_advisory_lock`, temp objects) mutate backend state.

## Edge Cases & Error Semantics

- **Single-permit retirement invariant**: Whether a lease fails during `ROLLBACK`, `DISCARD ALL`, baseline GUC restoration, `reset_timeout`, EOF, or post-deadline `close()`, its pool permit is released **exactly once** and the socket is never exposed in `idle` before reset completes.
- **Pinned scopes during `PostgresPool::close`**: `close(deadline)` never steals a `PooledConnection` out from under an active `TransactionScope` or `Portal`; it waits up to `deadline` and marks unreturned leases `retire_on_return`.
- **Expired lifetime on return**: Even if reset would succeed, a connection exceeding `max_lifetime` closes cleanly on return instead of running redundant `DISCARD ALL`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/pool_reset.rs` using deterministic `tokio::sync::Barrier` / `Notify` coordination (no `sleep`-based races) against a stateful wire server tracking session GUCs, temp tables, prepared statements, `LISTEN` channels, advisory locks, and transaction status per backend PID.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full session reset on reuse | `max_size = 1`; borrow `c1`, create temp state, mutate GUC, prepare `vtb_s_1`, register `LISTEN`, take advisory lock, leave `'T'` open; `c1.release().await`; reborrow `c2` | Emits `"ROLLBACK"` -> `'I'`, `"DISCARD ALL"` -> `'I'`, restores configured `session_gucs`, invalidates `StatementCache`; `c2` reuses same backend PID with clean server state. |
| Dirty `'E'` transaction & portal reset | Leave session in `'E'` (and separately with suspended portal), drop `PooledConnection` without `release()`, then `acquire()` | Background `Drop` task drains/closes portal, runs `"ROLLBACK"` + `"DISCARD ALL"`, and publishes clean `'I'` lease only after reset finishes. |
| No `SELECT` fast-return shortcut | Borrow `c1`, execute `"SELECT set_config('search_path', 'dirty', false)"`, `release()`, reborrow `c2` | Reset still executes `"DISCARD ALL"` + baseline GUC restore; `c2` never observes `'dirty'`. |
| Reset failure / timeout retires socket | Inject EOF or hang exceeding `reset_timeout` during `"DISCARD ALL"` on `max_size = 1` pool, then `acquire()` | Broken socket retires (`stats().retired == 1`), permit releases exactly once, and next `acquire()` dials a fresh backend PID. |
| Concurrent `close(deadline)` with barriers | Hold 1 borrowed lease behind a `Barrier`, call `pool.close(deadline)`: (a) release before deadline, (b) hold past deadline then drop | New `acquire()` fails immediately; (a) `close` returns `Ok(())` with `borrowed == 0, idle == 0`; (b) `close` returns timeout `Err`, lease remains un-stolen until dropped, then retires with zero leaked permits. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pool_reset
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pool_reset
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] No dirty, failed (`'T'`/`'E'`), or partially read session is ever returned to `idle` borrowers; `Direct`/`Session` reset executes `ROLLBACK` -> `DISCARD ALL` -> baseline `session_gucs` restoration and invalidates `StatementCache` / `TypeCatalog` generations (R14).
- [ ] Failed/timed-out resets and uncertain protocol states retire the physical socket and release its capacity permit exactly once (R14).
- [ ] `SessionHandle::from_pooled` and `PostgresPool::close(deadline)` preserve pinned scope ownership, bound shutdown duration, and track `connecting`/`idle`/`borrowed`/`resetting`/`retired` accurately (R14).
- [ ] `pool_reset` and `physical_pool` integration tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pooled lease reset, `SessionHandle` close semantics, and drain deadlines.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `DISCARD ALL` state preconditions and transaction-pooler session boundaries.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::db_runtime_handle` — Shared multi-threaded Tokio database runtime handle.
