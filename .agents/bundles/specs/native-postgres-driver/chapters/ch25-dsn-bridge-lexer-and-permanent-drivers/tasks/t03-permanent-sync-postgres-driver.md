---
type: Task
id: "native-postgres-driver:ch25:t03"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Implement the permanent synchronous native PostgreSQL driver before activation
description: Implement SyncPostgreSQLDriver in _sync.py over native SyncPostgresPool leases with ContextVar thread-pinned transactions, static binary helpers, command timeouts, Arrow forwarders, and active-operation cancel().
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
plan_revision: 6
plan_commit: 043d8444
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Permanent _sync.py SyncPostgreSQLDriver, ContextVar scope, timeout forwarding, and cancel registry contracts
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Existing public SyncPostgreSQLDriver static COPY/JSONB helpers, properties, and hydration conventions
  - id: sync-base-py
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: Base SyncDatabaseDriver lifecycle, Arrow Liskov signatures, and query methods
tags:
  - postgres
  - driver
  - sync
  - cancellation
---

# Task t03: Implement the Permanent Synchronous Native PostgreSQL Driver Before Activation

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/postgres/_sync.py` defining the permanent `SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig])` backed by `vertebra._postgres.SyncPostgresPool`, preserving `dialer`/`pool` properties and static COPY/JSONB helpers, rejecting `memory://` and invalid external `pool=` synchronously, and implementing `ContextVar` thread-pinned transactions and savepoints, command-only `timeout` forwarding, thin Arrow/COPY forwarders, and a generation-fenced active-operation `cancel()` registry alongside native-shaped test ports in `src/py/tests/unit/db/postgres_ports.py` [^npd-contracts] [^pg-driver-py] [^sync-base-py]. Leave `driver.py`, `__init__.py`, SQLSpec, and consumer helpers untouched until atomic activation in `ch27:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Pool `validate_configuration`, `ensure_module` metadata, `ContextVar` scope affinity, `_project_public_row` JSON-null projection, command `timeout` forwarding, and active-operation `cancel()` registry rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/driver.py` — Existing `SyncPostgreSQLDriver` public properties (`dialer`, `pool`), static methods (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, `parse_pgcopy_binary_rows`), and query contracts [^pg-driver-py].
- `src/py/vertebra/db/adapters/postgres/config.py` — `PostgreSQLConfig.resolve_sync_options` from `ch25:t01`.
- `src/py/vertebra/db/adapters/postgres/core.py` — `PostgreSQLCore.prepare_native_query` from `ch25:t02`.
- `src/py/vertebra/db/driver/_sync.py` — `SyncDatabaseDriver` base lifecycle and Arrow signatures [^sync-base-py].
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — Structured class layout, PEP 604 typing, and zero inline `#` comments.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py`:
  - `class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):`
    - `def __init__(self, config: PostgreSQLConfig | None = None, *, pool: Any = None, dialer: Any = None, broker: Any = None) -> None`:
      - Rejects legacy `executor=` (`TypeError`) and rejects case-insensitive `memory://` `config.dsn` synchronously in `__init__` with `ImproperConfigurationError` matching `"memory://"`.
      - Stores `self._dialer = dialer` and exposes read-only `@property def dialer(self) -> Any` and `@property def pool(self) -> Any` for `DatabaseRegistry` attachment assertions.
      - When `pool is not None`, synchronously rejects any object missing callable `validate_configuration` or `acquire` with `ImproperConfigurationError`, then runs `pool.validate_configuration(**conn_kwargs)` (`InterfaceError` on mismatch).
      - When constructing a native `SyncPostgresPool` (`share_pool=False` closes once on `close()`; broker SHM admission leases remain distinct from physical socket leases), always loads `vertebra._postgres` via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` so `MissingDependencyError` sets `exc.package == "vertebra-core[postgres]"` and includes `"vertebra-core[postgres]"` in `str(exc)`.
    - Preserved `@staticmethod` binary/JSONB helpers matching `driver.py` 1:1:
      - `strip_jsonb_header(payload: bytes | bytearray | memoryview) -> bytes`
      - `encode_jsonb(value: Any) -> bytes`
      - `encode_pgcopy_binary_rows(rows: Sequence[Sequence[Any]]) -> bytes`
      - `parse_pgcopy_binary_rows(payload: bytes | bytearray | memoryview) -> list[tuple[Any, ...]]`
    - Transaction & scope management (`connect() -> Self`, `close() -> None`, `begin() -> None`, `commit() -> None`, `rollback() -> None`, `transaction() -> Iterator[Self]`):
      - Tracks pinned lease in a driver-instance `ContextVar` carrying `(owner_thread_id, owner_task_id, client, savepoint_depth, failed)`.
      - Publishes pinned scope only after `BEGIN` succeeds; nested `transaction()` / `begin()` uses native savepoints and restores parent state on exit; accessing an inherited `ContextVar` from a different thread or task raises `InterfaceError`.
      - Ordinary calls outside an explicit transaction acquire and release one transient `SyncPostgresClient` lease in `finally`.
    - Query execution & `timeout` forwarding:
      - `execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> int`
      - `execute_many(self, sql: str, parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]], *, timeout: float | None = None, **kwargs: Any) -> int` (applies `timeout` separately per command, never as a whole-batch deadline)
      - `execute_with_description(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, timeout: float | None = None, **kwargs: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` (projects rows via `types._project_public_row`, mapping top-level `PgJson(None)` -> `None` while preserving inner JSON nulls and empty-row column names)
      - `select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[Any] | None = None, timeout: float | None = None, **kwargs: Any) -> list[Any]`
      - `query_multi(self, sql: str, *, timeout: float | None = None) -> list[PostgresQueryResult]` (lossless `PostgresQueryResult` records including `PgJson(None)`; zero parameters)
    - Staged Arrow & COPY forwarders matching `SyncDatabaseDriver` Liskov names (`select_to_arrow(..., *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor`, `stream_arrow_batches(..., *, batch_size: int = 65_536, columns: Sequence[str] | None = None, **kwargs: Any) -> Iterator[ArrowCursor]`, `load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None, **kwargs: Any) -> int` delegating to `postgres/arrow.py::load_arrow_sync`), completed with edge-case tests in `ch26:t03`.
    - Explicit `cancel(self) -> None`:
      - Driver-private active-operation registry guarded by a short `threading.Lock` released before native cancel I/O.
      - Prefers an active operation matching caller `(thread_id, task_id)`; otherwise selects the sole active operation in this driver; `0` active -> no-op; `>= 2` candidates -> raises `InterfaceError` with zero `CancelRequest` packets; ignores other drivers sharing the pool; retires any targeted physical socket and marks any ambient transaction failed until `rollback()`.

## Edge Cases & Error Semantics

- `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="MEMORY://unit"))` raises `ImproperConfigurationError(match="memory://")` immediately in `__init__`; passing `pool=object()` raises `ImproperConfigurationError`; passing a pool failing `validate_configuration` raises `InterfaceError` without network I/O.
- Absent `vertebra._postgres` raises `MissingDependencyError` with `exc.package == "vertebra-core[postgres]"` and `"vertebra-core[postgres]"` in `str(exc)`.
- Client-side command `timeout` expiry raises `OperationalError` (`sqlstate=None`) and retires the session under bounded cleanup without replaying SQL; server `57014` raises `QueryCancelledError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/postgres_ports.py` (observable native-shaped client/pool test ports with real lease accounting and zero DBAPI cursor mocks) and `src/py/tests/unit/db/test_postgres_native_adapters.py` (importing `SyncPostgreSQLDriver` from `vertebra.db.adapters.postgres._sync`, using `@pytest.mark.parametrize` across rejection/projection cases).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `__init__` guards, `dialer`/`pool`, static helpers, & missing wheel | `dsn="MEMORY://db"`, `pool=object()`, mismatched `pool`, `dialer=sentinel`, absent `vertebra._postgres`, and static `strip_jsonb_header`/`encode_jsonb`/`encode_pgcopy_binary_rows`/`parse_pgcopy_binary_rows` round-trip | `MEMORY://` and bad `pool=` raise `ImproperConfigurationError` synchronously; mismatched pool raises `InterfaceError`; `driver.dialer is sentinel`; missing wheel raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` in `str(exc)`; static COPY/JSONB round-trips match `driver.py`. |
| Counts, empty metadata, & JSON-null projection | Empty `SELECT id, payload FROM items WHERE 1=0`, `CREATE TABLE ...`, `UPDATE` (`0` and `3` rows), `INSERT ... RETURNING`, top-level `PgJson(None)` vs SQL `NULL` in `select()` vs `query_multi()` | Empty `SELECT` returns `([], ("id", "payload"))`; DDL returns `0`; `UPDATE` returns `0` / `3`; `select()` maps both SQL `NULL` and top-level `PgJson(None)` to `None` while `query_multi()` preserves `PgJson(None)` vs `None`. |
| Nested savepoints, failed `BEGIN`, & cross-thread `ContextVar` guard | Nested `with driver.transaction():`, failed inner block rolls back savepoint while outer commits; spawn child thread reading inherited scope | Outer commit persists outer row without inner row; failed `BEGIN` publishes zero scope and releases lease; child thread using inherited scope raises `InterfaceError`; lease `in_use == 0` afterward. |
| Per-command `timeout` forwarding & expiry vs `57014` | `execute(..., timeout=0.25)`, `query_multi(..., timeout=0.25)`, `execute_many(..., [p1, p2], timeout=0.25)`; client expiry vs server `57014` | Native client receives exact `timeout=0.25` per command (`2` separate budgets in `execute_many`); client expiry raises `OperationalError` (`sqlstate is None`) and retires socket; `57014` raises `QueryCancelledError`. |
| Active-operation `cancel()` routing & multi-op refusal | (a) Idle `driver.cancel()`; (b) parent thread calls `driver.cancel()` while `1` worker thread runs query; (c) `2` concurrent worker queries on `driver`; (d) sibling `driver2` sharing pool | (a) No-op; (b) sends `1` `CancelRequest`, retires socket, next borrower gets fresh session; (c) raises `InterfaceError` with `0` packets; (d) `driver2` untouched. |

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

- [ ] `_sync.py::SyncPostgreSQLDriver` enforces `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, synchronous `memory://` and invalid `pool=` `ImproperConfigurationError`, `dialer`/`pool` properties, exact static COPY/JSONB helpers (`parse_pgcopy_binary_rows`), `ContextVar` transactions/savepoints, `query_multi`, per-command `timeout`, Arrow forwarders, and active-operation `cancel()` (R6, R13, R18, R24).
- [ ] Existing `test_postgres_driver.py` continues passing unchanged against legacy `driver.py` before `ch27:t01` (R24).
- [ ] Commit message follows `feat(postgres): implement permanent synchronous native driver` once verified.

## Notes & Discoveries

- `pipeline()` is added in `ch26:t02`, Arrow edge cases in `ch26:t03`, and dedicated event hooks in `ch26:t04` before `ch27:t01` switches `driver.py` to re-export `_sync.SyncPostgreSQLDriver` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Permanent _sync.py SyncPostgreSQLDriver, ContextVar scope, timeout forwarding, and cancel registry contracts.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Existing public SyncPostgreSQLDriver static COPY/JSONB helpers, properties, and hydration conventions.
[^sync-base-py]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` — Base SyncDatabaseDriver lifecycle, Arrow Liskov signatures, and query methods.
