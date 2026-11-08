---
type: Task
id: "native-postgres-driver:ch14:t01"
chapter_id: ch14-physical-pools-and-query-facades
title: "Implement concrete Rust physical connection pool and bounded leases"
description: "Own authenticated physical PostgreSQL sockets in a reusable Rust rlib PostgresPool with bounded leases, lazy min_size initialization, and canonical configuration comparison."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch13:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch14-physical-pools-and-query-facades/spec.md
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/statements.rs
  - src/rs/vertebra-db/src/postgres/mod.rs
files:
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/physical_pool.rs
estimated_loc: 235
plan_revision: 9
plan_commit: b0de4dcb
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
    title: Frozen native PostgreSQL pool bounds, timeouts, and configuration identity contracts
  - id: pg-shm-pool
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: Existing SHM admission counter kept distinct from physical socket pooling
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared transport stream and deadline primitives
tags:
  - postgres
  - pool
  - leases
  - rust
---

# Task t01: Implement Concrete Rust Physical Connection Pool and Bounded Leases

## Objective & Scope Boundary

Own real authenticated `PostgresConnection` sockets inside a concrete Rust `PostgresPool` (`vertebra-postgres` `rlib`) with bounded lease permits, lazy `min_size` warmup on first acquisition, and canonical full-configuration comparison [^pg-contracts] [^pg-shm-pool]. Full rollback/`DISCARD ALL` reset and draining shutdown (`ch14:t02`) and Python `SyncPostgresPool` / `AsyncPostgresPool` classes (`ch15:t01`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 14 `PostgresPool`, `PostgresPoolOptions`, `PooledConnection`, and `PoolStats` contracts.
- [contracts.md](../../../contracts.md) — Pool option defaults (`min_size`, `max_size`, `acquire_timeout`, `reset_timeout`, `close_timeout`, `pooler_mode`, `protocol_level_pooler_cache`, `statement_cache_size`) and `validate_configuration` identity rules [^pg-contracts].
- `src/rs/vertebra-postgres/src/config.rs` — Validated `PostgresConfig` (credentials, Unix/TCP, TLS/PEM, channel binding, GUCs, keepalive, Cloud SQL/AlloyDB, Admin/IAM identity).
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection` connect/startup lifecycle and `BackendKeyData` PID tracking.
- `src/rs/vertebra-postgres/src/statements.rs` (`crate::session::statements`) — `PoolerMode`, `PoolerCapabilities`, and `StatementCache` from `ch13:t03`.
- `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Existing SHM-only admission helper; live sockets remain strictly inside `vertebra_postgres::PostgresPool` [^pg-shm-pool] [^pg-conn-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/config.rs`:
  - Derive or implement canonical equality on `PostgresConfig` covering every connection/authentication/TLS/PEM/channel-binding/keepalive/GUC/cloud/Admin/IAM field, and carry `pooler_mode: PoolerMode`, `protocol_level_pooler_cache: bool`, and `statement_cache_size: usize` so session startup configures `StatementCache` and `PoolerCapabilities` consistently.
- `src/rs/vertebra-postgres/src/pool.rs`:
  - `pub struct PostgresPoolOptions`:
    - `pub pooler_mode: PoolerMode` (default `PoolerMode::Direct`)
    - `pub protocol_level_pooler_cache: bool` (default `false`)
    - `pub statement_cache_size: usize` (default `256`)
    - `pub min_size: usize` (default `1`)
    - `pub max_size: usize` (default `10`)
    - `pub acquire_timeout: Duration` (default `30s`)
    - `pub reset_timeout: Duration` (default `10s`)
    - `pub close_timeout: Duration` (default `30s`)
    - Optional `max_idle_lifetime: Option<Duration>` and `max_lifetime: Option<Duration>` for idle-age and max-lifetime eviction.
    - Validate `max_size >= 1`, `min_size <= max_size`, and positive finite `acquire_timeout`, `reset_timeout`, and `close_timeout` (`PostgresError::InvalidConfig` on violation).
  - `pub struct PoolStats { pub connecting: usize, pub idle: usize, pub borrowed: usize, pub resetting: usize, pub retired: u64 }`
  - `pub struct PostgresPool`:
    - `pub fn new(config: PostgresConfig, options: PostgresPoolOptions) -> Result<Arc<Self>, PostgresError>` — validates and copies configuration with zero network or credential-refresh I/O.
    - `pub async fn acquire(self: &Arc<Self>) -> Result<PooledConnection, PostgresError>` — triggers bounded lazy `min_size` background warmup on first call, reserves capacity (`connecting + idle + borrowed + resetting <= max_size`) before dialing, evicts broken/expired idle sessions, wakes queued waiters fairly without holding pool locks across network I/O, and enforces `acquire_timeout` across waiting + connection setup without extending `config.connect_timeout`.
    - `pub fn validate_configuration(&self, config: &PostgresConfig, options: &PostgresPoolOptions) -> Result<(), PostgresError>` — compares complete canonical `PostgresConfig` + preparation policy (`pooler_mode`, `protocol_level_pooler_cache`, `statement_cache_size`) without I/O and returns `Err(PostgresError::Interface)` on any mismatch; no caller-supplied pool key can override this check [^pg-contracts].
    - `pub fn stats(&self) -> PoolStats`
  - `pub struct PooledConnection`:
    - Owns one concrete `PostgresConnection`, pool permit/generation, and RAII return hook (`Deref<Target = PostgresConnection>` + `DerefMut`); until `ch14:t02` installs full rollback/`DISCARD ALL` reset, only known-idle/unchanged sessions requeue while modified/broken sessions retire and release their permit cleanly.
- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export concrete `PostgresPool`, `PostgresPoolOptions`, `PooledConnection`, `PoolStats` (`pub mod pool;`), and re-export `pub use crate::session::statements::{PoolerCapabilities, PoolerMode, StatementCache, StatementKey};` at crate root (never declaring a duplicate `mod statements;` in `lib.rs`) with zero cross-`cdylib` trait-object pointers.

## Edge Cases & Error Semantics

- **Saturation timeout**: When `borrowed == max_size` and no lease returns before `acquire_timeout`, `acquire()` returns `Err(PostgresError::PoolExhausted)` and leaves capacity accounting exact.
- **Cancelled waiter or failed dial**: Dropping an `acquire()` future mid-wait or mid-dial releases the reserved slot and drops any partially opened stream without leaking `connecting` or starving the next waiter.
- **Connect deadline vs acquire deadline**: Each physical dial uses `min(remaining_acquire_budget, config.connect_timeout)` so a large `acquire_timeout` (`30s`) never stretches an explicit `connect_timeout` (`2s`) on a dead host.
- **Closed pool acquisition**: Calling `acquire()` on a closed pool fails immediately with `Err(PostgresError::Interface)` / `PoolClosed` before dialing.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/physical_pool.rs` using a multi-connection scripted PostgreSQL server that assigns monotonic `BackendKeyData` PIDs and executes real wire queries on every borrowed `PooledConnection`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| No-I/O construction & lazy `min_size` | `PostgresPool::new(cfg, opts { min_size: 2, max_size: 4 })`, inspect server accepts before and after first `acquire()` | `0` TCP accepts at construction; first `acquire()` returns a live socket executing `SELECT 1` and warms total pool sessions to `min_size = 2`. |
| Unique concurrent backend PIDs & `max_size` bound | `max_size = 2`; borrow `c1` and `c2` simultaneously, run queries on both, attempt 3rd `acquire()` with `acquire_timeout = 50ms` | `c1.backend_pid() != c2.backend_pid()`; 3rd `acquire()` fails with `PostgresError::PoolExhausted`; `stats().borrowed == 2`. |
| Cancelled waiter & dial failure recovery | `max_size = 1`; cancel waiter `w1` while `c1` is held, then drop `c1`; also fail 1st dial and retry `acquire()` | Cancelled waiter and failed dial release their slot cleanly; subsequent `acquire()` succeeds with `connecting == 0, borrowed == 1`. |
| Expired / broken idle eviction | Idle connection exceeds `max_idle_lifetime` or peer closes idle socket before next `acquire()` | Pool evicts stale/EOF socket (`stats().retired >= 1`), dials a fresh backend PID, and returns a working `PooledConnection`. |
| `validate_configuration` mismatch | Compare pool (`sslmode = VerifyFull`, `pooler_mode = Direct`, `statement_cache_size = 256`) against altered password, GUC, `pooler_mode = Transaction`, or `statement_cache_size = 0` | Returns `Err(PostgresError::Interface)` with zero network I/O; identical config + preparation policy returns `Ok(())`. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `PostgresPool::new` performs zero I/O, validates bounds/timeouts, and `acquire()` yields concrete `PooledConnection` leases backed by real authenticated sockets with distinct server PIDs (R14).
- [ ] Capacity accounting across `connecting + idle + borrowed` stays leak-free under saturation timeouts, cancelled waiters, dial failures, and idle eviction without holding locks across I/O (R14).
- [ ] `validate_configuration` compares full canonical connection + preparation policy without caller keys or network access, and `lib.rs` exports concrete pool types (R14).
- [ ] `physical_pool` integration tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pool bounds, timeouts, and configuration identity invariants.
[^pg-shm-pool]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Shared-memory slot admission counter distinct from physical socket pooling.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Underlying TCP, Unix, and TLS stream primitives.
