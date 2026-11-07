---
type: Task
id: "native-postgres-driver:ch25:t03"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Implement the permanent synchronous native PostgreSQL driver before activation
description: Implement SyncPostgreSQLDriver in _sync.py over native SyncPostgresPool leases with ContextVar thread-pinned transactions, command timeouts, Arrow forwarders, and active-operation cancel().
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch25:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/driver/_sync.py
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
tests:
  - src/py/tests/unit/db/postgres_ports.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
estimated_loc: 245
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
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Permanent _sync.py SyncPostgreSQLDriver, ContextVar scope, timeout forwarding, and cancel registry contracts
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Existing public SyncPostgreSQLDriver surface and row/hydration conventions
  - id: sync-base-py
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: Base SyncDatabaseDriver lifecycle and query methods
tags:
  - postgres
  - driver
  - sync
  - cancellation
---

# Task t03: Implement the Permanent Synchronous Native PostgreSQL Driver Before Activation

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/postgres/_sync.py` defining the permanent `SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig])` backed by `vertebra._postgres.SyncPostgresPool`, `ContextVar` thread-pinned transactions and savepoints, command-only `timeout` forwarding, thin Arrow/COPY forwarders, and a generation-fenced active-operation `cancel()` registry, alongside native-shaped test ports in `src/py/tests/unit/db/postgres_ports.py` [^npd-contracts] [^pg-driver-py] [^sync-base-py]. Do NOT edit `driver.py`, `__init__.py`, SQLSpec, or consumer helpers in this task: the legacy public pair remains untouched until atomic activation in `ch27:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Pool `validate_configuration`, `ContextVar` scope affinity, `_project_public_row` JSON-null projection, command `timeout` forwarding, and active-operation `cancel()` registry rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/driver.py` — Existing `SyncPostgreSQLDriver` public methods (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, `decode_pgcopy_binary_rows`, `execute`, `select`, etc.) [^pg-driver-py].
- `src/py/vertebra/db/adapters/postgres/config.py` — `PostgreSQLConfig.resolve_sync_options` from `ch25:t01`.
- `src/py/vertebra/db/adapters/postgres/core.py` — `PostgreSQLCore.prepare_native_query` from `ch25:t02`.
- `src/py/vertebra/db/driver/_sync.py` — `SyncDatabaseDriver` base contract [^sync-base-py].
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — Module layout, PEP 604 typing, and zero inline `#` comments.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py`:
  - `class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):`
    - `def __init__(self, config: PostgreSQLConfig | None = None, *, pool: Any = None, dialer: Any = None, broker: Any = None) -> None` — rejects legacy `executor=` or `memory://` fallback modes; lazily loads `vertebra._postgres` via `ensure_module("vertebra._postgres", install_package="vertebra-core[postgres]", extra="postgres")` on pool creation; calls `pool.validate_configuration(**conn_kwargs)` when an external/shared pool is supplied (`share_pool=False` creates private driver-owned pool closed once on `close()`; broker SHM admission leases stay distinct from physical socket leases).
    - Transaction & scope management: `connect() -> Self`, `close() -> None`, `begin() -> None`, `commit() -> None`, `rollback() -> None`, `transaction() -> Iterator[Self]`:
      - Tracks pinned lease in a driver-instance `ContextVar` carrying `(owner_thread_id, owner_task_id, client, savepoint_depth, failed)`.
      - Publishes pinned scope ONLY after `BEGIN` succeeds; nested `transaction()` / `begin()` uses native savepoints and restores parent state on exit; accessing an inherited `ContextVar` from a different thread or task raises `InterfaceError`.
      - Ordinary calls outside an explicit transaction acquire and release one transient `SyncPostgresClient` lease in `finally`.
    - Query execution & `timeout` forwarding:
      - `execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> int` (returns `result.affected_rows`, e.g. `0` for DDL, exact server count for `INSERT`/`UPDATE`/`DELETE`/`RETURNING`).
      - `execute_many(self, sql: str, parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]], *, timeout: float | None = None, **kwargs: Any) -> int` (applies `timeout` separately per command, never as a whole-batch deadline).
      - `execute_with_description(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` (projects rows via `types._project_public_row`, mapping top-level `PgJson(None)` -> `None` while preserving inner JSON nulls and empty-row column names).
      - `select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[Any] | None = None, timeout: float | None = None, **kwargs: Any) -> list[Any]`
      - `query_multi(self, sql: str, *, timeout: float | None = None) -> list[PostgresQueryResult]` (returns rich lossless `PostgresQueryResult` records including `PgJson(None)`; accepts zero parameters).
    - Arrow & COPY forwarders: `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` delegating directly to native `SyncPostgresClient` Arrow/COPY methods and `vertebra.db.arrow` helpers while retaining lease ownership until cursor/generator closure (extended in `ch26:t03`).
    - Explicit `cancel(self) -> None`:
      - Uses a driver-private active-operation registry guarded by a short `threading.Lock` (released BEFORE native cancel I/O).
      - Registers `(thread_id, task_id, operation_guard, client)` before wire submission and unregisters after drain/cleanup once any active cancel claim finishes (preventing `finally` from returning a mid-cancel client to the pool).
      - Prefers an active operation matching the caller's `(thread_id, task_id)`; otherwise selects the sole active operation in THIS driver (e.g. one worker thread). Zero active records -> no-op; `>= 2` active candidates -> raises `InterfaceError` with ZERO `CancelRequest` packets; other drivers sharing the pool are never candidates. Any possibly-sent `CancelRequest` retires the physical socket and marks any ambient transaction failed until `rollback()`.

## Edge Cases & Error Semantics

- Missing `vertebra._postgres` wheel raises `MissingDependencyError` naming `"vertebra-core[postgres]"`.
- Supplying a shared pool whose credentials, TLS, GUCs, cloud target, IAM options, or pooler mode differ from `config` raises `InterfaceError` via `validate_configuration` without network I/O.
- Client-side command `timeout` expiry raises `OperationalError` (`sqlstate=None`), retires the timed-out session under bounded cleanup without replaying SQL, whereas an earlier server `57014` remains `QueryCancelledError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/postgres_ports.py` (observable native-shaped client/pool support with real lease accounting and zero DBAPI cursor mocks) and `src/py/tests/unit/db/test_postgres_native_adapters.py` (importing `SyncPostgreSQLDriver` from `vertebra.db.adapters.postgres._sync`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Counts, empty metadata, & JSON-null projection | Empty `SELECT id, payload FROM items WHERE 1=0`, `CREATE TABLE ...`, `UPDATE` (`0` and `3` rows), `INSERT ... RETURNING`, top-level `PgJson(None)` vs SQL `NULL` in `select()` vs `query_multi()` | Empty `SELECT` returns `([], ("id", "payload"))`; DDL returns `0`; `UPDATE` returns `0` / `3`; `select()` maps both SQL `NULL` and top-level `PgJson(None)` to `None` while `query_multi()` preserves `PgJson(None)` vs `None`. |
| Nested savepoints, failed `BEGIN`, & cross-thread `ContextVar` guard | Nested `with driver.transaction():`, failed inner block rolls back savepoint while outer commits; spawn child thread reading inherited scope | Outer commit persists outer row without inner row; failed `BEGIN` publishes zero scope and releases lease; child thread using inherited scope raises `InterfaceError`; balanced lease count `0` in-use afterward. |
| Per-command `timeout` forwarding & expiry vs `57014` | `execute(..., timeout=0.25)`, `query_multi(..., timeout=0.25)`, `execute_many(..., [p1, p2], timeout=0.25)`; simulate client expiry vs server `57014` | Native client receives exact `timeout=0.25` on each call (`2` separate budgets in `execute_many`); client expiry raises `OperationalError` (`sqlstate is None`) and retires socket; server `57014` raises `QueryCancelledError`. |
| Active-operation `cancel()` routing & multi-op refusal | (a) Idle `driver.cancel()`; (b) parent thread calls `driver.cancel()` while `1` worker thread runs query; (c) `2` concurrent worker queries on `driver`; (d) sibling `driver2` sharing pool | (a) No-op; (b) sends `1` `CancelRequest`, retires that socket, next borrower gets fresh session; (c) raises `InterfaceError` with `0` `CancelRequest`s; (d) `driver2` query untouched. |
| Missing optional wheel error | Simulate absent `vertebra._postgres` on connect | Raises `MissingDependencyError` referencing `vertebra-core[postgres]`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q -k 'sync_native or native_missing_extra'`
- Final Green + Legacy Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q -k 'sync_native or native_missing_extra'
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/tests/unit/db/postgres_ports.py src/py/tests/unit/db/test_postgres_native_adapters.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `_sync.py::SyncPostgreSQLDriver` implements native `SyncPostgresPool` lease management, thread-pinned `ContextVar` transactions/savepoints, `query_multi`, per-command `timeout` forwarding, Arrow forwarders, and active-operation `cancel()` selection with zero legacy `executor`/`memory://` code paths (R6, R13, R18, R24).
- [ ] Existing `test_postgres_driver.py` continues passing unchanged against legacy `driver.py` before `ch27:t01` (R24).
- [ ] Commit message follows `feat(postgres): implement permanent synchronous native driver` once verified.

## Notes & Discoveries

- `pipeline()` is added in `ch26:t02`, Arrow edge cases in `ch26:t03`, and dedicated event hooks in `ch26:t04` before `ch27:t01` switches `driver.py` to re-export `_sync.SyncPostgreSQLDriver` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Permanent _sync.py SyncPostgreSQLDriver, ContextVar scope, timeout forwarding, and cancel registry contracts.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Existing public SyncPostgreSQLDriver surface and row/hydration conventions.
[^sync-base-py]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` — Base SyncDatabaseDriver lifecycle and query methods.
