---
type: Task
id: "native-postgres-driver:ch15:t01"
chapter_id: ch15-pool-facades-and-single-sync-pipeline
title: "Expose SyncPostgresPool and AsyncPostgresPool carrying real session leases"
description: "Implement immutable Python SyncPostgresPool and AsyncPostgresPool classes transferring concrete PooledConnection leases into SessionHandle with cancelled-acquire recovery and validate_configuration."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch14:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
files:
  - src/rs/vertebra-postgres/src/python_pool.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_pools.py
estimated_loc: 245
plan_revision: 4
plan_commit: e8901de4
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen SyncPostgresPool and AsyncPostgresPool constructor, acquire, close, and validate_configuration contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared Tokio completion ring and cancelled-future cleanup pattern
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL pooler mode and connection option alignment
tags:
  - postgres
  - pool
  - pyo3
  - leases
---

# Task t01: Expose `SyncPostgresPool` and `AsyncPostgresPool` Carrying Real Session Leases

## Objective & Scope Boundary

Expose immutable `SyncPostgresPool` and `AsyncPostgresPool` PyO3 classes in `vertebra._postgres` that acquire real `PooledConnection` socket leases, transfer them into `SessionHandle::from_pooled`, recover leases safely on cancelled `Future`s, and validate canonical configuration without network I/O [^pg-contracts] [^py-async-rs]. High-level Python `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` pool registry integration belongs to Chapter 25 (`ch25:t03`–`t04`).

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 15 pool facade and cancelled-acquire lease recovery contracts.
- [contracts.md](../../../contracts.md) — Complete 26 connection kwargs + 8 pool kwargs, `acquire()`, `close()`, and `validate_configuration` invariants [^pg-contracts].
- `src/rs/vertebra-postgres/src/pool.rs` — `PostgresPool`, `PostgresPoolOptions`, `PooledConnection`, and `validate_configuration` from `ch14:t01`–`t02`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle::from_pooled` and pooled vs direct `close()` ownership from `ch14:t02`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient::from_session` and constructor option validator.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient::from_session` and `_vertebra_postgres_completion_drainer` [^py-async-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/python_pool.rs` and `src/rs/vertebra-postgres/src/lib.rs`:
  - `#[pyclass(name = "SyncPostgresPool", module = "vertebra._postgres", frozen, immutable_type)] pub struct PySyncPostgresPool`
  - `#[pyclass(name = "AsyncPostgresPool", module = "vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresPool`
  - Both `#[new]` constructors accept the exact keyword-only connection signature from `contracts.md` (`user`, `database`, `host="localhost"`, `port=5432`, `password=None`, `unix_socket=None`, `sslmode="verify-full"`, `ca_pem=None`, `client_cert_pem=None`, `client_key_pem=None`, `server_name=None`, `channel_binding="prefer"`, `connect_timeout=10.0`, `application_name="vertebra"`, `session_gucs=None`, `keepalives=True`, `keepalives_idle=None`, `keepalives_interval=None`, `keepalives_count=None`, `cloud_sql_instance=None`, `cloud_sql_ip_type="PRIVATE"`, `alloydb_instance_uri=None`, `alloydb_ip_type="PRIVATE"`, `enable_iam_auth=False`, `cloud_auth_options=None`, `iam_auth_options=None`) PLUS:
    - `pooler_mode: &str = "direct"`
    - `protocol_level_pooler_cache: bool = false`
    - `statement_cache_size: isize = 256` (reject `bool` and `< 0`)
    - `min_size: isize = 1`
    - `max_size: isize = 10`
    - `acquire_timeout: f64 = 30.0`
    - `reset_timeout: f64 = 10.0`
    - `close_timeout: f64 = 30.0`
    Reusing the single shared option validator with zero network I/O at construction [^pg-contracts] [^pg-config].
  - Methods on `SyncPostgresPool` / `AsyncPostgresPool`:
    - `acquire(&self, py: Python<'_>) -> PyResult<PySyncPostgresClient>` / `Awaitable[AsyncPostgresClient]`: acquires a `PooledConnection` with GIL detached, wraps it in `SessionHandle::from_pooled`, and returns the corresponding client. In `AsyncPostgresPool.acquire`, wrap the acquired `PooledConnection` in an armed return-on-drop guard inside the `DbOutcomeProducer` closure so if the Python `Future` was cancelled before `DbCompletionDrainer` runs (when `produce` is dropped without executing) or `Py::new` fails, the lease schedules immediate background return to `PostgresPool`.
    - `close(&self, py: Python<'_>) -> PyResult<()>` / `Awaitable[None]`: drains the pool under configured `close_timeout`.
    - `validate_configuration(&self, ...same keyword-only options...) -> PyResult<()>`: validates candidate options and runs `PostgresPool::validate_configuration` (covering credentials, Unix/TCP, TLS/PEM, channel binding, GUCs, keepalive, Cloud SQL/AlloyDB/Admin/IAM identity, `pooler_mode`, `protocol_level_pooler_cache`, and `statement_cache_size`), raising `vertebra._vertebra.InterfaceError` on mismatch with zero network I/O and no caller-supplied pool key.
  - Register `python_pool::register(m)` alongside `sync_client::register(m)` and `async_client::register(m)` in `src/rs/vertebra-postgres/src/lib.rs`.
- `src/py/vertebra/_postgres.pyi`:
  - Add complete typed declarations for `SyncPostgresPool` and `AsyncPostgresPool`.

## Edge Cases & Error Semantics

- **`client.close()` vs `commit()`/`rollback()`**: `commit()` and `rollback()` end a transaction while keeping a caller-retained pooled client pinned; `client.close()` resets and releases the `PooledConnection` back to the pool and marks that client handle permanently closed (`InterfaceError` on further calls).
- **Bool/range validation**: Passing `bool` for integer bounds (`min_size=True`, `statement_cache_size=False`), `min_size > max_size`, `max_size < 1`, non-positive/NaN/infinite timeouts, or unknown `pooler_mode` raises `TypeError` / `ValueError` / `InterfaceError` synchronously at construction without I/O.
- **Pool exhaustion & drain timeout**: `acquire()` timeout maps to `vertebra._vertebra.PoolExhaustedError`; `close()` drain timeout maps to `vertebra._vertebra.OperationalError` while marking remaining leases `retire_on_return`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_pools.py` exercising `SyncPostgresPool` and `AsyncPostgresPool` (`max_size=1`) against scripted loopback PostgreSQL servers reporting real `pg_backend_pid()` / `BackendKeyData` PIDs.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Real PID reuse & transaction pinning | `max_size=1`; `c1 = pool.acquire()`, `c1.begin()`, query PID `p1`, `c1.commit()`, query PID `p1`, `c1.close()`; `c2 = pool.acquire()`, query PID | `c1` retains `p1` across `begin()`/`commit()` until `c1.close()`; `c2` observes clean reset `p1`; calling `c1.query(...)` after `c1.close()` raises `InterfaceError`. |
| Saturation & waiter exclusion | `max_size=1, acquire_timeout=0.05`; hold `c1`, call second `pool.acquire()` | Second `acquire()` raises `vertebra._vertebra.PoolExhaustedError`; releasing `c1` unblocks subsequent `acquire()`. |
| Cancelled `AsyncPostgresPool.acquire` race | `max_size=1`; cancel second `acquire()` `Future` both while waiting and right as `c1.close()` hands off the lease | Zero permit leak; a third `await pool.acquire()` succeeds immediately and executes `SELECT 1`. |
| Reset failure replacement | Inject server EOF during `c1.close()` reset on `max_size=1` pool, then `c2 = pool.acquire()` | Broken `p1` socket retires and `c2` connects with fresh backend PID `p2 != p1`. |
| Constructor & `validate_configuration` | Enumerate all 3 preparation kwargs + 5 bounds/timeouts; call `pool.validate_configuration(...)` with matching vs mismatched `session_gucs`, `sslmode`, `pooler_mode`, or `statement_cache_size` | Matching options return `None` with `0` TCP accepts; any mismatch raises `vertebra._vertebra.InterfaceError` without I/O. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py -q
  ```
- Final Green, stubtest, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_pools.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SyncPostgresPool` and `AsyncPostgresPool` expose the exact constructor, `acquire`, `close`, and `validate_configuration` contracts from `contracts.md` and `_postgres.pyi` (R6, R7, R14).
- [ ] Acquired clients own real `PooledConnection` leases verified by backend PID, and cancelled async acquisitions or dropped clients never leak pool capacity (R7, R14).
- [ ] `validate_configuration` detects credential, TLS, GUC, cloud/IAM, and preparation-policy mismatches synchronously without caller keys or network I/O (R14).
- [ ] `test_postgres_native_pools.py`, `mypy.stubtest vertebra._postgres`, Ruff, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen `SyncPostgresPool` and `AsyncPostgresPool` public contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio completion bridge.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Pooler mode and statement cache configuration semantics.
