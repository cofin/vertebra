---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch04-pools-drcp-and-session-piggyback
id: "native-oracle-driver:ch04:t03"
title: "Wire Python SyncOraclePool, AsyncOraclePool, SyncOracleDriver, and AsyncOracleDriver lease and transaction facades"
description: "Wire Python SyncOraclePool, AsyncOraclePool, SyncOracleDriver, and AsyncOracleDriver in src/py/vertebra/db/adapters/oracle/driver.py and __init__.py to acquire/release native pool leases (acquire_from_pool / release_to_pool), re-introduce native set_session_attrs and last_piggyback_frame on the drivers, and guard ContextVar transaction pinning and savepoints."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch04:t02"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/db/driver/_common.py
files:
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 235
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
commit_sha: pending
completed_at: "2026-11-09T16:17:00Z"
created_at: "2026-11-09T00:58:00Z"
updated_at: "2026-11-09T16:17:00Z"
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
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver and AsyncOracleDriver adapter surface
  - id: ora-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: OracleConfig pool and driver factory configuration
  - id: ora-init
    resource: src/py/vertebra/db/adapters/oracle/__init__.py
    title: Public vertebra.db.adapters.oracle lazy export map
  - id: driver-common
    resource: src/py/vertebra/db/driver/_common.py::validate_identifier
    title: SQL identifier validation helper for savepoints and tables
tags:
  - oracle
  - pool
  - driver
  - transactions
  - savepoint
---

# Task `native-oracle-driver:ch04:t03`: Wire Python SyncOraclePool, AsyncOraclePool, SyncOracleDriver, and AsyncOracleDriver lease and transaction facades

<!-- truth: start -->
- **Python Pool & Driver Lease Orchestration (`acquire_from_pool` / `release_to_pool`)**: `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` and `src/py/vertebra/db/adapters/oracle/__init__.py` acquire and release native `vertebra._oracle` pool leases per operation, re-introduce `set_session_attrs` and `last_piggyback_frame` on `SyncOracleDriver` and `AsyncOracleDriver` backed by the native `_oracle` client, and expose broker-ready pool traits [^ora-contracts] [^ora-driver] [^ora-init].
- **Identity-Guarded `ContextVar` Transaction Pinning & Savepoints**: Explicit transactions (`begin`, `commit`, `rollback`, `transaction()`, `savepoint`, `release_savepoint`, `rollback_to_savepoint`) pin a dedicated connection lease in a `ContextVar` guarded by `(threading.get_ident(), asyncio_current_task_id)` so spawned child tasks or threads never inherit or corrupt a parent transaction's pinned connection [^ora-contracts] [^ora-driver] [^driver-common].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Implement the Python `SyncOraclePool` and `AsyncOraclePool` facades and wire `SyncOracleDriver` and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` and `src/py/vertebra/db/adapters/oracle/__init__.py` to acquire/release native pool leases (`acquire_from_pool` / `release_to_pool`), re-introduce `set_session_attrs` and `last_piggyback_frame` on `SyncOracleDriver` and `AsyncOracleDriver` backed by the native `_oracle` client, guard transaction pinning with thread/task identity tokens, and support `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint` [^ora-contracts] [^ora-driver] [^ora-init].

- **In Scope**:
  - `SyncOraclePool` and `AsyncOraclePool` in `src/py/vertebra/db/adapters/oracle/driver.py` wrapping `vertebra._oracle.SyncOraclePool` and `vertebra._oracle.AsyncOraclePool` (with support for `OracleConfig` construction, `acquire()` / `acquire_from_pool()`, `release()` / `release_to_pool()`, `evict()`, context-managed `lease()`, `close()`, and `min_size`, `max_size`, `size`, `idle_size`, `borrowed_size`, `closed` / `is_closed` properties).
  - Re-introducing `set_session_attrs(self, *, client_identifier=None, module=None, action=None, ecid=None) -> None` and `last_piggyback_frame: bytes | None` on `SyncOracleDriver` and `AsyncOracleDriver` backed by the native `_oracle` client (replacing the SQLite stand-in helpers removed in `driver-fail-loud`).
  - Pool-backed lease checkout on `SyncOracleDriver` and `AsyncOracleDriver`: standalone operations check out a client lease via `acquire_from_pool`, stage any configured `OracleSessionAttrs` (`client_identifier`, `module`, `action`, `ecid`), execute the operation, and return the lease via `release_to_pool` (marking `discard=True` on fatal transport errors).
  - `ContextVar` transaction pinning with `(threading.get_ident(), id(asyncio.current_task()))` identity validation so `begin()`, `commit()`, `rollback()`, `transaction()`, `savepoint()`, `release_savepoint()`, and `rollback_to_savepoint()` operate on the pinned lease and release it cleanly on `commit()` or `rollback()` [^ora-driver] [^driver-common].
  - Exporting `SyncOraclePool` and `AsyncOraclePool` in `src/py/vertebra/db/adapters/oracle/__init__.py` (`__all__` and `_EXPORT_MAP`).
- **Out of Scope**:
  - Sequential delayed-commit `SyncOraclePipeline` / `AsyncOraclePipeline` (`ch05:t01`).
  - VPD trusted-package and `DBMS_SESSION.SET_IDENTIFIER` tenancy scope reset (`ch05:t02`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | Pool, driver, transaction, and tenancy contracts [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md` | Chapter 04 Python pool and transaction-pinning specifications |
| `src/py/vertebra/db/adapters/oracle/driver.py` | Existing `SyncOracleDriver` and `AsyncOracleDriver` implementation [^ora-driver] |
| `src/py/vertebra/db/adapters/oracle/config.py` | `OracleConfig` and `OracleDrcpConfig` fields [^ora-config] |
| `src/py/vertebra/db/adapters/oracle/__init__.py` | Lazy module export map for `vertebra.db.adapters.oracle` [^ora-init] |
| `src/py/vertebra/db/driver/_common.py` | `validate_identifier` helper [^driver-common] |

## 3. Symbol & Signature Contracts

### 3.1 `src/py/vertebra/db/adapters/oracle/driver.py` & `src/py/vertebra/db/adapters/oracle/__init__.py`

```python
class SyncOraclePool:
    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        native_pool: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def config(self) -> OracleConfig: ...
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
    @property
    def is_closed(self) -> bool: ...
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
    ) -> Any: ...
    def acquire_from_pool(self, **kwargs: Any) -> Any: ...
    def release(
        self,
        client: Any,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    def release_to_pool(self, client: Any, **kwargs: Any) -> None: ...
    def evict(self, client: Any) -> None: ...
    @contextmanager
    def lease(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Iterator[SyncOracleDriver]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...

class AsyncOraclePool:
    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        native_pool: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def config(self) -> OracleConfig: ...
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
    @property
    def is_closed(self) -> bool: ...
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
    ) -> Any: ...
    async def acquire_from_pool(self, **kwargs: Any) -> Any: ...
    async def release(
        self,
        client: Any,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
    async def release_to_pool(self, client: Any, **kwargs: Any) -> None: ...
    async def evict(self, client: Any) -> None: ...
    @asynccontextmanager
    async def lease(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> AsyncIterator[AsyncOracleDriver]: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...
```

On `SyncOracleDriver` and `AsyncOracleDriver`:
- `set_session_attrs(self, *, client_identifier: str | None = None, module: str | None = None, action: str | None = None, ecid: str | None = None) -> None` stages session attributes on the underlying native `_oracle` client (or stages them on the driver to be applied on the next `acquire_from_pool` checkout).
- `last_piggyback_frame: bytes | None` property reads the most recent `[0x11, 0x6e, count, ...]` frame flushed by the native `_oracle` client.
- `in_transaction: bool` property reflects whether the current thread/task has an active pinned transaction on this driver.
- `begin()`, `commit()`, `rollback()`, `savepoint(name="sp")`, `release_savepoint(name="sp")`, and `rollback_to_savepoint(name="sp")` (plus `transaction()` context manager).

## 4. Edge Cases & Error Semantics

- **Cross-Task / Cross-Thread `ContextVar` Inheritance Guard**: In Python `asyncio`, `asyncio.create_task()` copies the parent's `ContextVar` context. Storing `(driver_id, thread_id, task_id, lease)` in the `ContextVar` ensures that if a child task is spawned inside an active `async with driver.transaction():` block, the child task sees `task_id != current_task_id` and acquires its own independent pool lease via `acquire_from_pool` rather than sharing the parent's in-flight transaction connection.
- **Savepoint Identifier Validation & Oracle SQL Semantics**: `savepoint(name)`, `release_savepoint(name)`, and `rollback_to_savepoint(name)` validate `name` via `validate_identifier(name, "savepoint")` (raising `ValueError` / `ProgrammingError` on invalid identifiers). Because Oracle SQL does not support `RELEASE SAVEPOINT <name>`, `release_savepoint(name)` validates the identifier without sending invalid SQL to Oracle, while `savepoint(name)` executes `SAVEPOINT <name>` and `rollback_to_savepoint(name)` executes `ROLLBACK TO SAVEPOINT <name>` [^ora-driver].
- **Lease Release on Transaction Exception**: If an exception escapes `with driver.transaction():` or `async with driver.transaction():`, the driver rolls back the transaction and releases the pinned pool lease via `release_to_pool` in `finally:` (setting `discard=True` / `evict` if the rollback itself fails).

## 5. Public-Seam Test Blueprint

Add behavioral pool and transaction lease tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_python_oracle_pool_lease_and_driver_piggyback` | Import `SyncOraclePool`, `AsyncOraclePool` from `vertebra.db.adapters.oracle`; use `with pool.lease() as drv:` and call `drv.set_session_attrs(client_identifier="t-1", module="api")` | Execute `drv.select_value("SELECT 1 FROM DUAL")`, inspect `drv.last_piggyback_frame`, and exit context | Lease checks out `1` connection (`borrowed_size == 1`), `drv.last_piggyback_frame.startswith(b"\x11\x6e")` is `True`, and lease releases on exit (`borrowed_size == 0`, `idle_size == 1`) |
| `test_oracle_driver_transaction_pins_lease_and_guards_child_tasks` | Pool-backed `AsyncOracleDriver(config, pool=async_pool)` with `max_size=2` | Enter `async with driver.transaction():`, run query in parent task, and concurrently `await asyncio.create_task(driver.select("SELECT 1 FROM DUAL"))` | Parent task stays pinned to lease 1 across multiple queries and commits on exit; spawned child task detects task-identity mismatch and checks out lease 2 via `acquire_from_pool` instead of sharing lease 1 |
| `test_oracle_driver_savepoint_and_rollback_to_savepoint` | `SyncOracleDriver` and `AsyncOracleDriver` inside an active transaction | Call `savepoint("sp_1")`, `rollback_to_savepoint("sp_1")`, `release_savepoint("sp_1")`, and `savepoint("bad;name")` | Emits `SAVEPOINT sp_1` and `ROLLBACK TO SAVEPOINT sp_1` on the pinned connection; `release_savepoint("sp_1")` succeeds without emitting invalid SQL; `"bad;name"` raises `ValueError` |

## 6. Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "pool or transaction or savepoint" -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/ src/py/tests/unit/db/test_oracle_adapter.py
uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/
```

## 7. Acceptance Criteria

- [x] `SyncOraclePool` and `AsyncOraclePool` are implemented in `src/py/vertebra/db/adapters/oracle/driver.py` and exported in `src/py/vertebra/db/adapters/oracle/__init__.py`.
- [x] `SyncOracleDriver` and `AsyncOracleDriver` re-introduce `set_session_attrs` and `last_piggyback_frame` backed by the native `_oracle` client, acquire/release pool leases (`acquire_from_pool` / `release_to_pool`) for standalone queries, and pin leases across explicit transactions with `(thread_id, task_id)` identity guards.
- [x] `savepoint`, `release_savepoint`, and `rollback_to_savepoint` validate identifiers and execute on the pinned transaction lease.
- [x] All unit tests, `ruff`, and `mypy` checks pass.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — `OracleConfig` pool and driver factory configuration.
[^ora-init]: `src/py/vertebra/db/adapters/oracle/__init__.py` — Public `vertebra.db.adapters.oracle` lazy export map.
[^driver-common]: `src/py/vertebra/db/driver/_common.py::validate_identifier` — SQL identifier validation helper for savepoints and tables.
