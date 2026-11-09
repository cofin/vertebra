---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch04-pools-drcp-and-session-piggyback
id: "native-oracle-driver:ch04:t01"
title: "Implement native Rust SyncOraclePool and AsyncOraclePool"
description: "Implement SyncOraclePool and AsyncOraclePool in src/rs/vertebra-oracle/src/pool.rs over crate::config::NativeOracleConfig and crate::errors::OracleError with min_size, max_size, acquire_timeout, reset_timeout, close_timeout, max_lifetime_seconds, idle_timeout_seconds, health_check_interval_seconds, stmtcachesize, rollback-on-release, and broken-session eviction."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch03:t03"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/errors.rs
files:
  - src/rs/vertebra-oracle/src/pool.rs
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 240
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
commit_sha: pending
completed_at: "2026-11-09T16:01:00Z"
created_at: "2026-11-09T00:58:00Z"
updated_at: "2026-11-09T16:01:00Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public interface, FFI, type, pooling, tenancy, and ownership contracts
  - id: ora-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: OracleConfig and pool lifecycle parameters
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: AsyncCompletionQueue task bridge for non-blocking PyO3 futures
  - id: py-errors
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Cached Python database exception hierarchy including PoolExhaustedError
tags:
  - oracle
  - pool
  - rust
  - pyo3
---

# Task `native-oracle-driver:ch04:t01`: Implement native Rust SyncOraclePool and AsyncOraclePool

<!-- truth: start -->
- **Bounded Native Rust Pools (`SyncOraclePool` & `AsyncOraclePool`)**: `src/rs/vertebra-oracle/src/pool.rs` implements `SyncOraclePool` and `AsyncOraclePool` over `crate::config::NativeOracleConfig` and `crate::errors::OracleError`, accepting all connection kwargs from `contracts.md` plus `min_size`, `max_size`, `acquire_timeout`, `reset_timeout`, `close_timeout`, `max_lifetime_seconds`, `idle_timeout_seconds`, and `health_check_interval_seconds`, enforcing `opening + idle + borrowed <= max_size` and raising `PoolExhaustedError` (`sqlstate="53300"`) when `acquire_timeout` expires [^ora-contracts] [^ora-config] [^py-errors].
- **Rollback-on-Release & Poisoned-Session Eviction (`evict`)**: Returning a connection via `release(client)` rolls back uncommitted state within `reset_timeout` and permanently evicts (`evict(client)`) any closed (`client.closed`), broken, expired (`max_lifetime_seconds` / `idle_timeout_seconds`), or failed-reset physical session [^ora-contracts].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Implement `SyncOraclePool` and `AsyncOraclePool` in `src/rs/vertebra-oracle/src/pool.rs`, register both classes on `vertebra._oracle` in `src/rs/vertebra-oracle/src/lib.rs`, and declare their type signatures in `src/py/vertebra/_oracle.pyi` [^ora-contracts] [^ora-config].

- **In Scope**:
  - `OraclePoolOptions` wrapping `crate::config::NativeOracleConfig` and thread-safe pool state machine (`Mutex<PoolState>` + `Condvar` for sync waiters and `AsyncCompletionQueue` / oneshot waiter queue for async callers) using `crate::errors::OracleError` in `src/rs/vertebra-oracle/src/pool.rs` [^py-async].
  - Zero-I/O constructor validation (`min_size`, `max_size`, timeouts, `stmtcachesize`, `purity`).
  - Bounded `opening + idle + borrowed <= max_size` slot reservation so concurrent `acquire()` calls never exceed `max_size` while dialing new sessions outside the pool lock.
  - Lifecycle management: `max_lifetime_seconds`, `idle_timeout_seconds`, `health_check_interval_seconds` ping validation, rollback-on-release within `reset_timeout`, permanent eviction (`evict()`) of broken or failed-reset sessions, `stats()`, `closed` property, and `close()` within `close_timeout`.
  - Registration of `SyncOraclePool` and `AsyncOraclePool` in `src/rs/vertebra-oracle/src/lib.rs` and type stub declarations in `src/py/vertebra/_oracle.pyi`.
- **Out of Scope**:
  - DRCP `cclass`, `purity` (`SELF` vs `NEW`), `connection_tag` matching (`acquire_from_pool`, `release_to_pool`), and TTC `OracleSessionAttrs` piggyback (`ch04:t02`).
  - Python `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` wrapper classes and `ContextVar` transaction pinning (`ch04:t03`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | Canonical `SyncOraclePool` and `AsyncOraclePool` kwargs and lifecycle invariants [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md` | Chapter 04 pool state machine and eviction requirements |
| `src/py/vertebra/db/adapters/oracle/config.py` | `OracleConfig` pool parameters and defaults [^ora-config] |
| `src/rs/vertebra-db/src/py_async.rs` | `spawn_rust_db_task_with_attr` async task bridge [^py-async] |
| `src/rs/vertebra-py/src/errors.rs` | `PoolExhaustedError` (`sqlstate="53300"`) and `InterfaceError` mapping [^py-errors] |

## 3. Symbol & Signature Contracts

### 3.1 `src/rs/vertebra-oracle/src/pool.rs` & `src/rs/vertebra-oracle/src/lib.rs`

```rust
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use pyo3::prelude::*;
use crate::client::{AsyncOracleClient, SyncOracleClient};
use crate::config::NativeOracleConfig;
use crate::errors::OracleError;

#[derive(Clone, Debug)]
pub struct OraclePoolOptions {
    pub config: NativeOracleConfig,
    pub min_size: usize,
    pub max_size: usize,
    pub acquire_timeout: Duration,
    pub reset_timeout: Duration,
    pub close_timeout: Duration,
    pub max_lifetime: Duration,
    pub idle_timeout: Duration,
    pub health_check_interval: Duration,
}

#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
pub struct SyncOraclePool {
    inner: Arc<OraclePoolCore>,
}

#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
pub struct AsyncOraclePool {
    inner: Arc<OraclePoolCore>,
}
```

### 3.2 `src/py/vertebra/_oracle.pyi`

```python
@final
class SyncOraclePool:
    @property
    def min_size(self) -> int: ...
    @property
    def max_size(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def idle_size(self) -> int: ...
    @property
    def borrowed_size(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    def __init__(
        self,
        *,
        user: str | None = None,
        password: str | None = None,
        dsn: str | None = None,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "FREEPDB1",
        sid: str | None = None,
        tns_admin: str | None = None,
        connect_descriptor: str | None = None,
        protocol: str = "tcp",
        ssl_server_dn_match: bool = True,
        ssl_server_cert_dn: str | None = None,
        wallet_location: str | None = None,
        wallet_password: str | None = None,
        pem_cert_data: bytes | None = None,
        cwallet_sso_data: bytes | None = None,
        auth_mode: str = "password",
        access_token: str | None = None,
        token_auth_options: Mapping[str, str] | None = None,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        stmtcachesize: int = 64,
        prefetchrows: int = 256,
        arraysize: int = 256,
        call_timeout_ms: int | None = None,
        connect_timeout: float = 10.0,
        enable_lowercase_column_names: bool = True,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
        max_lifetime_seconds: float = 3600.0,
        idle_timeout_seconds: float = 600.0,
        health_check_interval_seconds: float = 30.0,
    ) -> None: ...
    def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> SyncOracleClient: ...
    def release(
        self,
        client: SyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    def evict(self, client: SyncOracleClient) -> None: ...
    def stats(self) -> dict[str, int]: ...
    def close(self) -> None: ...

@final
class AsyncOraclePool:
    @property
    def min_size(self) -> int: ...
    @property
    def max_size(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def idle_size(self) -> int: ...
    @property
    def borrowed_size(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    def __init__(self, **kwargs: Any) -> None: ...
    async def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> AsyncOracleClient: ...
    async def release(
        self,
        client: AsyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    async def evict(self, client: AsyncOracleClient) -> None: ...
    def stats(self) -> dict[str, int]: ...
    async def close(self) -> None: ...
```

## 4. Edge Cases & Error Semantics

- **Invalid Pool Bounds at Construction**: `max_size < 1`, `min_size < 0`, `min_size > max_size`, `acquire_timeout <= 0.0`, `reset_timeout <= 0.0`, or `close_timeout <= 0.0` returns `OracleError` mapped to `InterfaceError` / `ValueError` immediately in `__init__` with zero network I/O.
- **Acquire Timeout & `PoolExhaustedError`**: When all `max_size` slots are borrowed (or opening) and no session is returned before `acquire_timeout` (or per-call `timeout`), `acquire()` raises `PoolExhaustedError` with `sqlstate="53300"` and `is_transient=True` [^py-errors].
- **Acquire on Closed Pool**: Calling `acquire()` after `pool.close()` (`pool.closed is True`) raises `InterfaceError` (`"Oracle pool is closed"`).
- **Eviction on Failed Reset or Broken Session**: If `release()` is called with `discard=True`, with a closed/broken `client` (`client.closed is True`), or if the rollback-on-release operation fails or exceeds `reset_timeout`, `evict()` permanently destroys the session (`size` decrements) and notifies any waiting borrower to dial a fresh session.

## 5. Public-Seam Test Blueprint

Add behavioral pool unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_oracle_pool_zero_io_init_and_bounds_validation` | Construct `SyncOraclePool(min_size=1, max_size=4)` and invalid `SyncOraclePool(min_size=5, max_size=2)` / `max_size=0` | Inspect pool properties (`size`, `idle_size`, `borrowed_size`, `closed`, `stats()`) and invalid constructors | Valid pool constructs in `< 5ms` with `size == 0`, `idle_size == 0`, `borrowed_size == 0`, `closed is False`; invalid bounds raise `InterfaceError` / `ValueError` |
| `test_oracle_pool_acquire_release_and_exhaustion_timeout` | Pool with `max_size=1, acquire_timeout=0.05` (sync and async) | Acquire slot 1, attempt second concurrent `acquire()`, then release slot 1 | Second `acquire()` while slot 1 is held raises `PoolExhaustedError` (`sqlstate == "53300"`, `is_transient is True`); after `release()`, `idle_size == 1`, `borrowed_size == 0` |
| `test_oracle_pool_rollback_on_release_and_evict_on_broken_session` | Borrowed session with dirty transaction state vs session marked broken (`client.closed`) / `discard=True` / `pool.evict(client)` | Call `pool.release(client)` and `pool.release(broken_client, discard=True)` | Clean session executes rollback and returns to `idle_size == 1`; broken or discarded session is permanently evicted (`size == 0`, `idle_size == 0`) |
| `test_oracle_pool_max_lifetime_and_idle_timeout_eviction` | Pool with `idle_timeout_seconds=0.02` | Acquire and release session, wait `0.03s`, then acquire again | Expired idle session is closed and replaced by a newly dialed session |

## 6. Verification

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "oracle_pool" -q
```

## 7. Acceptance Criteria

- [x] `SyncOraclePool` and `AsyncOraclePool` are exported from `vertebra._oracle` over `NativeOracleConfig` and `OracleError` and match `contracts.md` constructor and method signatures (`acquire`, `release`, `evict`, `stats`, `close`, `closed`).
- [x] Pool slot reservation enforces `opening + idle + borrowed <= max_size` and raises `PoolExhaustedError` (`sqlstate="53300"`) on acquire timeout.
- [x] `release()` rolls back uncommitted work within `reset_timeout` and permanently evicts broken, expired, or failed-reset connections.
- [x] `uv run --no-sync python -m mypy.stubtest vertebra._oracle` and all pool tests in `src/py/tests/unit/db/test_oracle_adapter.py` pass with zero allowlist overrides.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — `OracleConfig` and pool lifecycle parameters.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — `AsyncCompletionQueue` task bridge for non-blocking PyO3 futures.
[^py-errors]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Cached Python database exception hierarchy including `PoolExhaustedError`.
