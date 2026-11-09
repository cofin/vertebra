---
type: Spec
parent_flow: native-oracle-driver
chapter_id: ch04-pools-drcp-and-session-piggyback
title: "Chapter 04: Native Sync/Async Pools, DRCP Purity/Tagging, and TTC Session Piggyback"
description: "Implement native Rust SyncOraclePool and AsyncOraclePool, DRCP cclass/purity/connection_tag state tracking, zero-extra-round-trip TTC session attribute piggyback, and Python pool and transaction-pinned driver lease facades."
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch03-types-bind-and-arrow-shm
replan_trigger: null
current_task: "native-oracle-driver:ch04:t01"
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-09T00:58:00Z"
updated_at: "2026-11-09T01:28:30Z"
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
    title: OracleConfig, OracleDrcpConfig, and pool lifecycle parameters
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver, AsyncOracleDriver, SyncOraclePool, and AsyncOraclePool surface
  - id: ora-init
    resource: src/py/vertebra/db/adapters/oracle/__init__.py
    title: Public vertebra.db.adapters.oracle lazy export map
  - id: ora-rs-codecs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Shared Rust OracleSessionAttrs TTC piggyback encoder ([0x11, 0x6e, count, ...])
  - id: py-errors
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Cached Python database exception hierarchy including PoolExhaustedError
tags:
  - oracle
  - pool
  - drcp
  - piggyback
  - transactions
---

# Chapter 04: Native Sync/Async Pools, DRCP Purity/Tagging, and TTC Session Piggyback

<!-- truth: start -->
- **Native Rust Physical Connection Pools (`R8` — `ch04:t01`)**: `src/rs/vertebra-oracle/src/pool.rs` implements `SyncOraclePool` and `AsyncOraclePool` (`min_size`, `max_size`, `acquire_timeout`, `reset_timeout`, `close_timeout`, `max_lifetime_seconds`, `idle_timeout_seconds`, `health_check_interval_seconds`, `stmtcachesize`) over `crate::config::NativeOracleConfig` and `crate::errors::OracleError` with bounded slot reservation (`opening + idle + borrowed <= max_size`), rollback-on-release, `evict()`, `stats()`, `closed`, and permanent eviction of broken or failed-reset sessions [^ora-contracts] [^ora-config] [^py-errors].
- **DRCP Purity, Connection Tagging & Zero-Extra-Round-Trip TTC Piggyback (`R8` — `ch04:t02`)**: Pooled and direct worker sessions track DRCP `cclass`, `purity` (`"SELF"` vs `"NEW"`), `connection_tag`, and `tag_matched`, and stage `OracleSessionAttrs` (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`) on connection lease checkout (`acquire_from_pool` / `set_session_attrs`) so TTC piggyback frames (`[0x11, 0x6e, count, ...]`) flush on the first outbound statement without an extra network round-trip [^ora-contracts] [^ora-rs-codecs].
- **Transaction-Pinned Python Pool & Driver Lease Facades (`R8` — `ch04:t03`)**: `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` and `src/py/vertebra/db/adapters/oracle/__init__.py` acquire/release native pool leases (`acquire_from_pool` / `release_to_pool`), re-introduce `set_session_attrs` and `last_piggyback_frame` on the drivers backed by `_oracle`, and pin leases across explicit transactions (`ContextVar` transaction pinning with thread/task identity guards, `begin`/`commit`/`rollback`/`savepoint`) [^ora-driver] [^ora-init].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Enterprise Oracle 26ai workloads rely on bounded physical session pools, Database Resident Connection Pooling (DRCP) broker classes (`cclass`, `purity`, `connection_tag`), statement handle caching (`stmtcachesize`), and end-to-end tracing attributes (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`) [^ora-contracts] [^ora-config].

This chapter implements Requirement `R8` from the [parent specification](../../spec.md) and [public contracts](../../contracts.md) across three vertical slices:
- `src/rs/vertebra-oracle/src/pool.rs`, `src/rs/vertebra-oracle/src/lib.rs`, and `src/py/vertebra/_oracle.pyi` implement native Rust `SyncOraclePool` and `AsyncOraclePool` using `crate::config::NativeOracleConfig` and `crate::errors::OracleError` with zero-I/O construction, bounded capacity reservation (`PoolExhaustedError` on `acquire_timeout`), `max_lifetime_seconds`, `idle_timeout_seconds`, `health_check_interval_seconds`, per-connection `stmtcachesize`, rollback-on-release within `reset_timeout`, and permanent eviction (`evict()`) of broken or failed-reset sessions ([t01](tasks/t01-rust-physical-connection-pool.md)) [^ora-contracts] [^py-errors].
- `src/rs/vertebra-oracle/src/pool.rs`, `src/rs/vertebra-oracle/src/client.rs`, and `src/py/vertebra/_oracle.pyi` implement DRCP `cclass`, `purity` (`"SELF"` vs `"NEW"`), and `connection_tag` state tracking (`acquire_from_pool`, `release_to_pool`, `tag_matched`) plus zero-extra-round-trip TTC piggyback (`OracleSessionAttrs` `[0x11, 0x6e, count, ...]` for `CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`) staged on checkout and flushed on the first outbound statement ([t02](tasks/t02-drcp-purity-tagging-and-ttc-piggyback.md)) [^ora-contracts] [^ora-rs-codecs].
- `src/py/vertebra/db/adapters/oracle/driver.py` and `src/py/vertebra/db/adapters/oracle/__init__.py` wire Python `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` to acquire and release native pool leases, re-introduce `set_session_attrs` and `last_piggyback_frame` on `SyncOracleDriver` and `AsyncOracleDriver` backed by `_oracle`, guard transaction pinning with thread/task identity tokens, and implement `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint` ([t03](tasks/t03-python-pool-and-driver-lease-facades.md)) [^ora-driver] [^ora-init].

### 1.2 Non-Goals

- Sequential delayed-commit pipelines (`SyncOraclePipeline` / `AsyncOraclePipeline`) and VPD trusted-package / `SET_IDENTIFIER` tenancy scope reset (owned by [ch05-pipelines-tenancy-and-apex](../ch05-pipelines-tenancy-and-apex/spec.md)).
- TxEventQ / `DBMS_AQ` notification streaming and `OracleEmbeddingPushdown` (owned by [ch06-notifications-and-embeddings](../ch06-notifications-and-embeddings/spec.md)).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Concrete Pool, DRCP, TTC Piggyback, & Lease Contracts (`R8`)

#### Native Rust `SyncOraclePool` & `AsyncOraclePool` (`R8` — [t01](tasks/t01-rust-physical-connection-pool.md))

`src/rs/vertebra-oracle/src/pool.rs` defines `OraclePoolOptions` wrapping `crate::config::NativeOracleConfig` and exports `SyncOraclePool` and `AsyncOraclePool` registered in `src/rs/vertebra-oracle/src/lib.rs` and typed in `src/py/vertebra/_oracle.pyi` [^ora-contracts]:

```python
class SyncOraclePool:
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
```

`AsyncOraclePool` mirrors `SyncOraclePool` with `async def acquire(...) -> AsyncOracleClient`, `async def release(...) -> None`, `async def evict(...) -> None`, and `async def close() -> None`.

Pool lifecycle invariants:
1. **Zero-I/O Constructor & Parameter Validation**: `__init__` validates `NativeOracleConfig`, `min_size >= 0`, `max_size >= 1`, `min_size <= max_size`, `purity in ("SELF", "NEW")`, and positive timeouts without opening sockets or reading the filesystem [^ora-contracts].
2. **Bounded Slot Reservation (`opening + idle + borrowed <= max_size`)**: When no idle connection is available and total slots are below `max_size`, the pool atomically increments `opening` before releasing the state lock to dial a new worker session; if dialing fails, `opening` decrements and waiters are notified. If `opening + idle + borrowed == max_size`, callers wait up to `acquire_timeout` (`timeout` override) and raise `PoolExhaustedError` (`sqlstate="53300"`, `is_transient=True`) on expiry [^py-errors].
3. **Expiry, Health Check & Rollback-on-Release**:
   - Idle sessions exceeding `max_lifetime_seconds` or `idle_timeout_seconds` are retired and closed on checkout.
   - Idle sessions whose time since last activity exceeds `health_check_interval_seconds` run a lightweight `connection.ping()` health check before checkout; if the ping fails, the session is evicted and replaced.
   - On `release(client, discard=False)`, the pool executes an uncommitted-transaction `rollback()` within `reset_timeout` before returning the session to the idle deque. If `discard=True`, `client.closed`, a fatal transport `ORA-03113` / `ORA-03114` / `ORA-03135` occurred, or the reset/rollback fails or times out, the pool permanently evicts (`evict(client)`) and closes the physical connection so poisoned state never re-enters the idle queue [^ora-contracts].

#### DRCP Purity, Tagging & Zero-Extra-Round-Trip TTC Piggyback (`R8` — [t02](tasks/t02-drcp-purity-tagging-and-ttc-piggyback.md))

`src/rs/vertebra-oracle/src/pool.rs`, `src/rs/vertebra-oracle/src/client.rs`, and `src/py/vertebra/_oracle.pyi` implement DRCP session state and TTC attribute piggybacking [^ora-contracts] [^ora-rs-codecs]:
1. **DRCP `cclass`, `purity`, and `connection_tag`**:
   - `OracleDrcpConfig` / pool kwargs configure `cclass: str | None`, `purity: Literal["SELF", "NEW"] = "SELF"`, and default `connection_tag: str | None`.
   - When `purity == "NEW"`, `acquire()` / `acquire_from_pool()` forces a clean untagged session state. When `purity == "SELF"` and a `connection_tag` is requested (e.g., `"NLS_DATE_FORMAT=YYYY-MM-DD;TIME_ZONE=UTC"`), `acquire(connection_tag=...)` preferentially selects an idle session with a matching `connection_tag` and records `client.tag_matched: bool`.
   - On `release(client, connection_tag=...)` / `release_to_pool()`, the session's active `connection_tag` is updated in pool metadata (unless evicted).
2. **Zero-Extra-Round-Trip TTC Session Piggyback (`OracleSessionAttrs` `[0x11, 0x6e, count, ...]`)**:
   - `SyncOracleClient.set_session_attrs` and `AsyncOracleClient.set_session_attrs` (and `acquire(...)` kwargs `client_identifier`, `module`, `action`, `ecid`) stage an `OracleSessionAttrs` struct (`vertebra_db::oracle::OracleSessionAttrs`) on the worker session without sending a network packet [^ora-rs-codecs].
   - On the next outbound `query`, `query_one`, `execute`, `execute_many`, or `query_arrow` call, the worker thread encodes the staged `client_identifier`, `module`, `action`, and `ecid` via `attrs.encode_ttc_piggyback_session_attrs()` (which starts with the 2-byte TTC `KEYVAL` piggyback function header `[0x11, 0x6e]`) and applies them onto the `oracledb::Connection` immediately before statement execution so Oracle TTC piggybacks the metadata on the SQL RPC frame in zero extra round-trips, clearing `pending_session_attrs` and recording `last_piggyback_frame` (`frame.startswith(b"\x11\x6e")`) [^ora-contracts] [^ora-rs-codecs].

#### Python Pool & Driver Lease Facades (`R8` — [t03](tasks/t03-python-pool-and-driver-lease-facades.md))

`src/py/vertebra/db/adapters/oracle/driver.py` and `src/py/vertebra/db/adapters/oracle/__init__.py` expose Python `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` [^ora-driver] [^ora-init]:
- **Python `SyncOraclePool` & `AsyncOraclePool`**:
  - Wrap the native `vertebra._oracle.SyncOraclePool` / `AsyncOraclePool` while accepting `OracleConfig` or kwargs, exposing `acquire_from_pool` / `acquire`, `release_to_pool` / `release`, `evict`, `lease()`, `close()`, and pool metrics (`min_size`, `max_size`, `size`, `idle_size`, `borrowed_size`, `closed` / `is_closed`).
- **Native Session Attribute Piggyback & Transaction Pinning on `SyncOracleDriver` & `AsyncOracleDriver`**:
  - Re-introduce `set_session_attrs(client_identifier=..., module=..., action=..., ecid=...)` and `last_piggyback_frame: bytes | None` on `SyncOracleDriver` and `AsyncOracleDriver` backed by the native `_oracle` client (replacing the SQLite stand-in helpers removed in `driver-fail-loud`).
  - Standalone `execute*` and `select*` calls on a pool-backed driver check out a lease via `acquire_from_pool`, stage any pending `OracleSessionAttrs`, run the operation, and release the lease via `release_to_pool` in a `finally:` block.
  - Calling `begin()` (or entering `with driver.transaction():` / `async with driver.transaction():`) acquires and pins a dedicated connection lease in a `ContextVar` tagged with `(threading.get_ident(), asyncio_current_task_id)` so concurrent threads or spawned child tasks never inherit a parent's pinned transaction connection.
  - `savepoint(name="sp")`, `release_savepoint(name="sp")`, and `rollback_to_savepoint(name="sp")` validate the savepoint identifier via `validate_identifier(name, "savepoint")` and emit `SAVEPOINT <name>` and `ROLLBACK TO SAVEPOINT <name>` (with `release_savepoint` acting as a validated no-op because Oracle SQL has no `RELEASE SAVEPOINT` statement) [^ora-driver].

### 2.2 Caller -> Seam -> Collaborator Integration Map

```text
SyncOracleDriver / AsyncOracleDriver (src/py/vertebra/db/adapters/oracle/driver.py)
  -> SyncOraclePool / AsyncOraclePool.{acquire_from_pool,release_to_pool,evict,lease} (driver.py)
    -> vertebra._oracle.{SyncOraclePool,AsyncOraclePool} (src/rs/vertebra-oracle/src/pool.rs)
      -> crate::config::NativeOracleConfig & crate::errors::OracleError
      -> SyncOracleClient / AsyncOracleClient.{set_session_attrs,last_piggyback_frame} (src/rs/vertebra-oracle/src/client.rs)
        -> vertebra_db::oracle::OracleSessionAttrs::encode_ttc_piggyback_session_attrs ([0x11, 0x6e, count, ...])
```

### 2.3 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| `max_size < 1`, `min_size > max_size`, non-positive timeouts, or `purity not in ("SELF", "NEW")` | Rejected synchronously in `SyncOraclePool.__init__` / `AsyncOraclePool.__init__` with `InterfaceError` / `ValueError` and zero socket I/O. |
| All `max_size` slots borrowed when `acquire()` / `acquire_from_pool()` is called | Blocks (inside `py.detach` or `AsyncCompletionQueue`) up to `acquire_timeout` and raises `PoolExhaustedError` (`sqlstate="53300"`, `is_transient=True`). |
| Uncommitted transaction or broken socket when returning a lease via `release()` | Pool runs `rollback()` within `reset_timeout`; if `discard=True`, `client.closed` is `True`, or rollback fails/times out, the physical session is permanently evicted (`evict()`). |
| `set_session_attrs` called multiple times before first SQL statement | Coalesces non-`None` attributes into `pending_session_attrs` and flushes a single `[0x11, 0x6e, count, ...]` TTC piggyback frame on the next outbound statement. |
| Child `asyncio.create_task(...)` spawned inside `async with driver.transaction():` | Task-identity guard `(threading.get_ident(), id(asyncio.current_task()))` detects mismatch so the child task checks out its own pool lease instead of sharing the parent's pinned transaction connection. |

### 2.4 Alternatives Considered

| Seam | Chosen Design | Rejected Alternative | Rationale |
| :--- | :--- | :--- | :--- |
| Physical connection pool ownership | Native Rust `SyncOraclePool` and `AsyncOraclePool` in `src/rs/vertebra-oracle/src/pool.rs` with thin Python lease facades | Pure-Python `queue.Queue` / `asyncio.Queue` pool | Keeps slot reservation, `reset_timeout` rollback-on-release, DRCP tag matching, and health-check pings in Rust without GIL contention under `3.14t`/`3.15t`. |
| Session attribute propagation (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`) | Stage `OracleSessionAttrs` in memory and flush `[0x11, 0x6e, ...]` TTC piggyback on first outbound statement | Executing separate `DBMS_APPLICATION_INFO.SET_MODULE` PL/SQL round-trips on every checkout | Saves a full network round-trip per pool lease checkout. |
| `_oracle.pyi` synchronization across `ch04:t01` and `ch04:t02` | Update `_oracle.pyi` in both `ch04:t01` (pool classes) and `ch04:t02` (DRCP and piggyback client/pool methods) | Deferring `_oracle.pyi` updates to `ch04:t03` | Ensures `uv run --no-sync python -m mypy.stubtest vertebra._oracle` passes at every individual task commit while keeping `len(files) == 3`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (all user documentation and OKF v0.2 knowledge updates, including `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` and `.agents/bundles/knowledge/domains/data/connection-pools-and-brokers.md`, are consolidated in [ch07-conformance-and-okf-synthesis](../ch07-conformance-and-okf-synthesis/spec.md) tasks `ch07:t03` and `ch07:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement ID | Requirement Summary | Owning Task(s) | Verification Artifact |
| :--- | :--- | :--- | :--- |
| `R8` (Pool Core) | Native Rust `SyncOraclePool` & `AsyncOraclePool` over `NativeOracleConfig` and `OracleError` with `min_size`, `max_size`, timeouts, `stmtcachesize`, rollback-on-release, `evict`, `stats`, and `closed` | [t01](tasks/t01-rust-physical-connection-pool.md) (`native-oracle-driver:ch04:t01`) | `src/py/tests/unit/db/test_oracle_adapter.py`, `mypy.stubtest vertebra._oracle` |
| `R8` (DRCP & Piggyback) | DRCP `cclass`, `purity` (`SELF` vs `NEW`), `connection_tag` matching (`acquire_from_pool`, `release_to_pool`), and zero-extra-round-trip TTC piggyback (`[0x11, 0x6e, count, ...]`) | [t02](tasks/t02-drcp-purity-tagging-and-ttc-piggyback.md) (`native-oracle-driver:ch04:t02`) | `src/py/tests/unit/db/test_oracle_adapter.py`, `mypy.stubtest vertebra._oracle` |
| `R8` (Python Facades) | Python `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` lease checkout, re-introduced native `set_session_attrs` / `last_piggyback_frame`, `ContextVar` transaction pinning, and savepoints | [t03](tasks/t03-python-pool-and-driver-lease-facades.md) (`native-oracle-driver:ch04:t03`) | `src/py/tests/unit/db/test_oracle_adapter.py` |

## 5. Implementation Plan

- [ ] native-oracle-driver:ch04:t01 — Implement native Rust SyncOraclePool and AsyncOraclePool in src/rs/vertebra-oracle/src/pool.rs, register them in lib.rs, and update _oracle.pyi (tasks/t01-rust-physical-connection-pool.md)
- [ ] native-oracle-driver:ch04:t02 — Implement DRCP cclass, purity (SELF vs NEW), and connection_tag tracking plus zero-extra-round-trip TTC piggyback (OracleSessionAttrs) in pool.rs, client.rs, and _oracle.pyi (tasks/t02-drcp-purity-tagging-and-ttc-piggyback.md)
- [ ] native-oracle-driver:ch04:t03 — Wire Python SyncOraclePool, AsyncOraclePool, SyncOracleDriver, and AsyncOracleDriver lease management, native set_session_attrs/last_piggyback_frame, ContextVar transaction pinning, and savepoint methods (tasks/t03-python-pool-and-driver-lease-facades.md)

**Chapter Recovery Checkpoint:**
```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "pool or drcp or piggyback or transaction or savepoint" -q
```

## 6. Current State & Remaining Plan Snapshot

- **Completed Tasks**: None (`0/3`).
- **Active Frontier**: [t01](tasks/t01-rust-physical-connection-pool.md) (`native-oracle-driver:ch04:t01`, waiting on `ch03-types-bind-and-arrow-shm`).
- **Remaining Tasks**: [t01](tasks/t01-rust-physical-connection-pool.md), [t02](tasks/t02-drcp-purity-tagging-and-ttc-piggyback.md), and [t03](tasks/t03-python-pool-and-driver-lease-facades.md).
- **Documentation & OKF Status**: `[0/6 files documented]`, scheduled in [ch07-conformance-and-okf-synthesis](../ch07-conformance-and-okf-synthesis/spec.md) (`ch07:t03`–`t04`).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — `OracleConfig`, `OracleDrcpConfig`, and pool lifecycle parameters.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver`, `AsyncOracleDriver`, `SyncOraclePool`, and `AsyncOraclePool` surface.
[^ora-init]: `src/py/vertebra/db/adapters/oracle/__init__.py` — Public `vertebra.db.adapters.oracle` lazy export map.
[^ora-rs-codecs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Shared Rust `OracleSessionAttrs` TTC piggyback encoder (`[0x11, 0x6e, count, ...]`).
[^py-errors]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Cached Python database exception hierarchy including `PoolExhaustedError`.
