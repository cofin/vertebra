---
type: Task
id: "native-postgres-driver:ch25:t04"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Implement the permanent asynchronous native PostgreSQL driver before activation
description: Implement AsyncPostgreSQLDriver in _async.py directly over native AsyncPostgresPool leases with task-pinned transactions, shielded cleanup, static JSONB helpers, command timeouts, Arrow forwarders, and cancel().
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch25:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/driver/_async.py
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/py/vertebra/db/adapters/postgres/_async.py
tests:
  - src/py/tests/unit/db/postgres_ports.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
estimated_loc: 245
plan_revision: 7
plan_commit: c8722c76
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Permanent _async.py AsyncPostgreSQLDriver, task-pinned scopes, and async cancellation contracts
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: Existing public AsyncPostgreSQLDriver properties and surface preserved until Chapter 27
  - id: async-base-py
    resource: src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver
    title: Base AsyncDatabaseDriver lifecycle, Arrow Liskov signatures, and shielded transaction rollback
tags:
  - postgres
  - driver
  - async
  - cancellation
---

# Task t04: Implement the Permanent Asynchronous Native PostgreSQL Driver Before Activation

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/postgres/_async.py` defining the permanent `AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig])` directly over `vertebra._postgres.AsyncPostgresPool` with zero `SyncPostgreSQLDriver` coupling or thread-offload fallback, preserving `dialer`/`pool` properties and `strip_jsonb_header`/`encode_jsonb` static helpers, rejecting `memory://` and invalid external `pool=` synchronously, and implementing `ContextVar` task-pinned transactions and savepoints, shielded cancellation/rollback cleanup, command-only `timeout` forwarding, thin async Arrow/COPY forwarders, and a generation-fenced active-operation `cancel()` registry [^npd-contracts] [^pg-driver-py] [^async-base-py]. Public `driver.py` exports remain untouched until `ch27:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — `AsyncPostgresPool` ownership, `ensure_module` metadata, `ContextVar` task/thread isolation, command `timeout` rules, and explicit `driver.cancel()` vs `Task.cancel()` semantics [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Shared projection, static JSONB helpers, and active-operation registry conventions from `ch25:t03`.
- `src/py/vertebra/db/adapters/postgres/driver.py` — Existing public `AsyncPostgreSQLDriver` properties (`dialer`, `pool`), static helpers, and method signatures [^pg-driver-py].
- `src/py/vertebra/db/adapters/postgres/config.py` — `PostgreSQLConfig.resolve_async_options` from `ch25:t01`.
- `src/py/vertebra/db/driver/_async.py` — `AsyncDatabaseDriver` base contract, Arrow signatures, and `CancelScope(shield=True)` rollback discipline [^async-base-py].
- [SKILL.md](../../../../../../skills/pyo3-bridge/SKILL.md) — Event-loop non-blocking completion discipline.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_async.py`:
  - `class AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig]):`
    - `def __init__(self, config: PostgreSQLConfig | None = None, *, pool: Any = None, dialer: Any = None, broker: Any = None) -> None`:
      - Constructs zero `SyncPostgreSQLDriver` instances, rejects `executor=` (`TypeError`), performs zero `asyncio.to_thread` query fallback, and synchronously rejects case-insensitive `memory://` `config.dsn` with `ImproperConfigurationError` matching `"memory://"`.
      - Stores `self._dialer = dialer` and exposes read-only `@property def dialer(self) -> Any` and `@property def pool(self) -> Any` for `DatabaseRegistry` attachment assertions.
      - When `pool is not None`, synchronously rejects any object missing callable `validate_configuration` or `acquire` with `ImproperConfigurationError`, then calls `pool.validate_configuration(**conn_kwargs)` (`InterfaceError` on mismatch).
    - Preserved `@staticmethod` JSONB helpers: `strip_jsonb_header(payload: bytes | bytearray | memoryview) -> bytes` and `encode_jsonb(value: Any) -> bytes`.
    - Pool & transaction lifecycle (`connect() -> Awaitable[Self]`, `close() -> Awaitable[None]`, `begin() -> Awaitable[None]`, `commit() -> Awaitable[None]`, `rollback() -> Awaitable[None]`, `transaction() -> AsyncIterator[Self]`):
      - Lazily creates `vertebra._postgres.AsyncPostgresPool` via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` and `config.resolve_async_options()`, so `MissingDependencyError` sets `exc.package == "vertebra-core[postgres]"` and includes `"vertebra-core[postgres]"` in `str(exc)`; closes private pools via `await pool.close()` on `close()`.
      - Pins one acquired `AsyncPostgresClient` in an instance `ContextVar` recording `(threading.get_ident(), asyncio.current_task(), client, savepoint_depth, failed)` only after `BEGIN` succeeds; spawned child `asyncio.Task`s inheriting the `ContextVar` raise `InterfaceError` so two tasks never share one physical socket concurrently.
      - Nested `async with driver.transaction():` uses native savepoints and shields savepoint/transaction rollback via `CancelScope(shield=True)` so `asyncio.CancelledError` rolls back cleanly and preserves the original exception.
    - Query execution & `timeout` forwarding:
      - `async def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> int`
      - `async def execute_many(self, sql: str, parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]], *, timeout: float | None = None, **kwargs: Any) -> int` (per-command `timeout`)
      - `async def execute_with_description(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`
      - `async def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[Any] | None = None, timeout: float | None = None, **kwargs: Any) -> list[Any]`
      - `async def query_multi(self, sql: str, *, timeout: float | None = None) -> list[PostgresQueryResult]`
    - Staged async Arrow & COPY forwarders matching `AsyncDatabaseDriver` Liskov signatures (`async def select_to_arrow(..., *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor`, `async def stream_arrow_batches(..., *, batch_size: int = 65_536, columns: Sequence[str] | None = None, **kwargs: Any) -> AsyncIterator[ArrowCursor]`, and `async def load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None, **kwargs: Any) -> int` delegating to `postgres/arrow.py::load_arrow_async`), completed in `ch26:t03`.
    - Explicit `async def cancel(self) -> None`:
      - Driver-private active-operation registry guarded by a short lock never held across `await`.
      - Prefers the caller `asyncio.Task`'s active operation; otherwise selects the sole active child/worker operation belonging to this driver; returns `None` when idle; raises `InterfaceError` before sending any `CancelRequest` when `>= 2` operations are active on this driver; ignores other drivers sharing the pool.
      - Claims the operation/lease until shielded generation-fenced native cancel completes, retires any physical session that may have received `CancelRequest`, and leaves any cancelled ambient transaction marked failed until `await driver.rollback()`.

## Edge Cases & Error Semantics

- `AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://test"))` and `AsyncPostgreSQLDriver(pool=object())` fail synchronously in `__init__` with `ImproperConfigurationError`; missing `vertebra._postgres` raises `MissingDependencyError` carrying `package="vertebra-core[postgres]"`.
- `Task.cancel()` on an in-flight query cancels only that operation while shielding native cleanup/retirement (`await client.close()` only on dedicated clients, pool retirement on pooled clients), whereas `await driver.cancel()` targets this driver's active-operation registry.
- A cancelled or failed ambient transaction rejects follow-up queries with `InterfaceError` / `OperationalError` until `await rollback()`, after which the next operation borrows a fresh session; event loop ticks continue unobstructed during slow native queries.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/postgres_ports.py` and `src/py/tests/unit/db/test_postgres_native_adapters.py` (importing `AsyncPostgreSQLDriver` from `vertebra.db.adapters.postgres._async`, using `@pytest.mark.parametrize` across rejection/projection cases).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `__init__` guards, `dialer`/`pool`, static JSONB helpers, & missing wheel | `dsn="MEMORY://db"`, `pool=object()`, mismatched `pool`, `dialer=sentinel`, absent `vertebra._postgres`, `strip_jsonb_header`/`encode_jsonb` | `MEMORY://` and bad `pool=` raise `ImproperConfigurationError` synchronously; `driver.dialer is sentinel`; missing wheel raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` in `str(exc)`; static JSONB helpers round-trip cleanly. |
| Async DDL/DML counts, JSON-null projection, & two-task isolation | Concurrent `asyncio.gather` queries across `2` tasks; `CREATE TABLE` (`0`), `execute_many` count, `select()` vs `query_multi()` `PgJson(None)` | Both tasks lease distinct sockets and return isolated rows; counts and `PgJson(None)` projection match `_sync.py`; all leases returned (`in_use == 0`). |
| Spawned child-task `ContextVar` guard & cancelled-scope rollback | Inside `async with driver.transaction():`, `asyncio.create_task(driver.execute(...))`; separately cancel task inside `transaction()` | Child task raises `InterfaceError` without touching parent's pinned socket; cancelled transaction runs shielded `ROLLBACK` and releases/retires lease cleanly. |
| Async `timeout` forwarding & client expiry vs `57014` | `await driver.select(..., timeout=0.2)`, `await driver.query_multi(..., timeout=0.2)`, `await driver.execute_many(..., [p1, p2], timeout=0.2)` | Forwards exact per-command `timeout=0.2`; client expiry raises `OperationalError(sqlstate=None)` and retires session with zero SQL replay; server `57014` raises `QueryCancelledError`. |
| Parent `await driver.cancel()` on child task, multi-op refusal, & loop responsiveness | (a) Parent awaits `driver.cancel()` while `1` child task runs slow query; (b) `2` active tasks on `driver`; (c) sibling `driver2` on shared pool; (d) ticker coroutine alongside slow query | (a) Sends `1` `CancelRequest` and retires socket; (b) raises `InterfaceError` with `0` packets; (c) `driver2` unaffected; (d) ticker advances unobstructed. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q`
- Final Green + Legacy Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/postgres_ports.py src/py/tests/unit/db/test_postgres_native_adapters.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `_async.py::AsyncPostgreSQLDriver` operates directly over `AsyncPostgresPool` with zero `SyncPostgreSQLDriver` coupling, enforcing `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, synchronous `memory://` and invalid `pool=` checks, `dialer`/`pool` properties, static JSONB helpers, task-pinned `ContextVar` transactions, shielded rollback/cancel cleanup, `query_multi`, per-command `timeout`, Arrow forwarders, and active-operation `cancel()` (R7, R13, R18, R24).
- [ ] Legacy `driver.py` and `test_postgres_driver.py` remain untouched and green before `ch27:t01` (R24).
- [ ] Commit message follows `feat(postgres): implement permanent asynchronous native driver` once verified.

## Notes & Discoveries

- Keeping `_sync.py` and `_async.py` independent eliminates the legacy coupling where `AsyncPostgreSQLDriver.__init__` instantiated `SyncPostgreSQLDriver` internally [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Permanent _async.py AsyncPostgreSQLDriver, task-pinned scopes, and async cancellation contracts.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — Existing public AsyncPostgreSQLDriver properties and surface preserved until Chapter 27.
[^async-base-py]: `src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver` — Base AsyncDatabaseDriver lifecycle, Arrow Liskov signatures, and shielded transaction rollback.
