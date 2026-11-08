---
type: Task
id: "driver-fail-loud:ch03:t01"
chapter_id: ch03-remove-memory-fakes
title: PostgreSQL and MySQL drivers reject memory:// and lose in-memory rows
description: Delete _in_memory_rows, _tx_snapshot, and every memory branch from the sync and async PostgreSQL and MySQL drivers, raise ImproperConfigurationError for memory:// DSNs and for PostgreSQL pools with no execution method, and retarget tests to injected pool ports.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 165
plan_revision: 2
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: PostgreSQL sync and async drivers
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL sync and async drivers
  - id: improper-config
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error exported from vertebra.typing
tags:
  - database
  - postgres
  - mysql
  - fail-loud
  - tdd
---

# Task t01: PostgreSQL and MySQL Drivers Reject `memory://`

## Objective & Scope Boundary

Make the four PostgreSQL and MySQL driver classes raise `ImproperConfigurationError` when constructed with a `memory://` DSN, delete the `_in_memory_rows` and `_tx_snapshot` state and every `memory://` branch, and replace the PostgreSQL empty-row fallthrough with an `ImproperConfigurationError`.[^pg-driver][^mysql-driver][^improper-config] Retarget the three tests that build a PostgreSQL driver from `memory://` to a `postgresql://` DSN with an injected `pool=` port.

Out of scope: `postgres/config.py` and `mysql/config.py` (configs stay constructible with any DSN); `_ensure_async_pool` returning the `asyncpg` module without assigning it; install-hint changes in `_ensure_pool` / `_ensure_async_pool` (owned by `ch08-install-hints-and-extras-gate`); `from __future__ import annotations` removal.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/postgres/driver.py` — `SyncPostgreSQLDriver.__init__`, `begin`, `rollback`, `stream_arrow_batches`, `_release_tx_conn`, `_run_sync_query`, `_ensure_pool`; `AsyncPostgreSQLDriver.__init__`, `_run_async_query`, `_ensure_async_pool`.[^pg-driver]
- `src/py/vertebra/db/adapters/mysql/driver.py` — `SyncMySQLDriver.__init__`, `begin`, `commit`, `rollback`, `close`, `_run_sync_query`; `AsyncMySQLDriver.__init__`, `begin`, `rollback`, `_release_tx_conn`, `_run_async_query`. `SyncMySQLDriver` has no `_release_tx_conn`; its `commit`, `rollback`, and `close` clear `_tx_snapshot` directly.[^mysql-driver]
- `src/py/tests/unit/db/test_native_db_drivers.py` — imports `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, `SyncMySQLDriver`, `AsyncMySQLDriver`, `PostgreSQLConfig`, `MySQLConfig`; `test_postgres_pooler_modes_and_jsonb_strip` is the neighbouring anchor.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py::test_postgres_vtb_dialect_dbapi_and_compiler_features`.
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_postgresql_tenant_session_prepares_psycopg_binds`, `::test_postgresql_tenant_session_prepares_asyncpg_binds`.

`ImproperConfigurationError` is defined in `src/py/vertebra/_typing.py` as `class ImproperConfigurationError(VertebraError, ValueError)` and imported as `from vertebra.typing import ImproperConfigurationError`.[^improper-config] Neither `postgres/driver.py` nor `mysql/driver.py` imports it today.

## Symbol & Signature Contracts

`src/py/vertebra/db/adapters/postgres/driver.py`:

```python
def _reject_memory_dsn(dsn: str) -> None:
    """Reject in-process memory:// DSNs.

    Args:
        dsn: Configured PostgreSQL DSN.

    Raises:
        ImproperConfigurationError: When ``dsn`` starts with ``memory://`` (case-insensitive).
    """
    if dsn.strip().lower().startswith("memory://"):
        msg = "PostgreSQL drivers do not support memory:// DSNs; pass a postgresql:// DSN or inject pool="
        raise ImproperConfigurationError(msg)
```

- Add `from vertebra.typing import ImproperConfigurationError` to the module imports (after `from vertebra.db.driver import (...)`, keeping isort order).
- Place `_reject_memory_dsn` after both driver classes (private helpers follow the public API that calls them).
- `SyncPostgreSQLDriver.__init__`: `resolved = config or PostgreSQLConfig()`, then `_reject_memory_dsn(resolved.dsn)`, then `super().__init__(...)`. Delete the `_in_memory_rows` and `_tx_snapshot` slots and assignments.
- `AsyncPostgreSQLDriver.__init__`: same guard immediately after resolving the config.
- `begin`: delete `self._tx_snapshot = ...`; condition becomes `if pool is None and self._executor is None:`.
- `rollback`: delete the snapshot-restore lines.
- `stream_arrow_batches`: condition becomes `if pool is None and self._executor is None:`.
- `_release_tx_conn`: delete `self._tx_snapshot = None`.
- `_run_sync_query`: `if pool is None: pool = self._ensure_pool()`; after the three `hasattr` branches, replace the `_in_memory_rows` return with `msg = "PostgreSQL pool must provide connection(), execute_with_description(), or execute()"` followed by `raise ImproperConfigurationError(msg)`, and delete the `_ = (sql, params_arg)` line. The `if pool is not None:` wrapper goes away because `pool` is always set at that point.
- `_ensure_pool` and `AsyncPostgreSQLDriver._ensure_async_pool`: delete the `memory://` early return.
- `AsyncPostgreSQLDriver._run_async_query`: condition becomes `if self._sync._executor is None and self._pool is None:`.

`src/py/vertebra/db/adapters/mysql/driver.py`:

- Add the module-private `_reject_memory_dsn(dsn: str) -> None` with message `MySQL drivers do not support memory:// DSNs; pass a mysql:// DSN or inject connect_factory= or pool=`; place it after both driver classes and import `ImproperConfigurationError` from `vertebra.typing`.
- `SyncMySQLDriver.__init__` and `AsyncMySQLDriver.__init__` call the guard right after resolving the config.
- Delete `_in_memory_rows` and `_tx_snapshot` from both `__slots__` and both `__init__`, and every assignment or read of them in: `SyncMySQLDriver.begin` (snapshot capture), `SyncMySQLDriver.commit` (`self._tx_snapshot = None`), `SyncMySQLDriver.rollback` (snapshot restore and `self._tx_snapshot = None`), `SyncMySQLDriver.close` (`self._tx_snapshot = None`), `AsyncMySQLDriver.begin` (snapshot capture), `AsyncMySQLDriver.rollback` (snapshot restore), and `AsyncMySQLDriver._release_tx_conn` (`self._tx_snapshot = None`).
- `SyncMySQLDriver.begin`: `if self._tx_conn is None:` then acquire.
- `AsyncMySQLDriver.begin`: `if self._tx_conn is not None: return`.
- Both `_run_sync_query` / `_run_async_query`: delete the `memory://` early return.

Call-site wiring: `DatabaseRegistry.get_shared_driver(PostgreSQLConfig(dsn="memory://"))` → `PostgreSQLConfig.build_async_driver` → `AsyncPostgreSQLDriver.__init__` → `_reject_memory_dsn` → `ImproperConfigurationError`.

## Edge Cases & Error Semantics

- `dsn="memory://"`, `"MEMORY://"`, and `"  memory://x"` raise `ImproperConfigurationError` from every driver constructor before any pool, dialer, or `CloudSQL` dialer object is built.
- A PostgreSQL key-value DSN (`"host=db dbname=app"`) passes the guard.
- `pool=object()` with no `connection`, `execute_with_description`, or `execute` raises `ImproperConfigurationError` on the first query for the sync driver and, through `asyncio.to_thread`, for the async driver.
- `executor=` callables still short-circuit `_run_sync_query` before the pool branch.
- No pool and no `psycopg_pool` installed raises `MissingDependencyError` from `_ensure_pool` (unchanged).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_native_db_drivers.py` (new tests), `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` and `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (retargets). Dependency category: injected in-process ports at the pool boundary.
- New `test_postgres_and_mysql_drivers_reject_memory_dsn(factory: Callable[[], object])`, parametrized over zero-argument callables so each driver constructs inside `pytest.raises(ImproperConfigurationError, match="memory://")`. Each case is one `pytest.param` line whose config literal is written `dsn="memory://"` (or `dsn="MEMORY://"`), so the `driver-fail-loud:ch04:t01` residue gate filter `rg -v 'dsn="memory://"|reject_memory_dsn'` drops every row:

```python
@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda: SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://")), id="sync-postgres"),
        pytest.param(lambda: AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="MEMORY://")), id="async-postgres"),
        pytest.param(lambda: SyncMySQLDriver(MySQLConfig(dsn="memory://")), id="sync-mysql"),
        pytest.param(lambda: AsyncMySQLDriver(MySQLConfig(dsn="memory://")), id="async-mysql"),
    ],
)
def test_postgres_and_mysql_drivers_reject_memory_dsn(factory: Callable[[], object]) -> None:
    with pytest.raises(ImproperConfigurationError, match="memory://"):
        factory()
```

| Case id | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `sync-postgres` | `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://"))` | `ImproperConfigurationError` matching `memory://` |
| `async-postgres` | `AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="MEMORY://"))` (uppercase scheme) | `ImproperConfigurationError` matching `memory://` |
| `sync-mysql` | `SyncMySQLDriver(MySQLConfig(dsn="memory://"))` | `ImproperConfigurationError` matching `memory://` |
| `async-mysql` | `AsyncMySQLDriver(MySQLConfig(dsn="memory://"))` | `ImproperConfigurationError` matching `memory://` |

- `Callable` joins the existing `from collections.abc import ...` line; `ImproperConfigurationError` is imported from `vertebra.typing`. Both guard messages contain the literal `memory://`, so the `match` holds for the uppercase input.

- New `test_postgres_pool_without_execution_surface_raises` (sync) and `test_async_postgres_pool_without_execution_surface_raises` (`@pytest.mark.anyio`):

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Sync opaque pool | `SyncPostgreSQLDriver(PostgreSQLConfig(), pool=object()).execute_with_description("SELECT 1")` | `ImproperConfigurationError` matching `pool must provide` |
| Async opaque pool | `await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=object()).execute_with_description("SELECT 1")` | `ImproperConfigurationError` matching `pool must provide` |

- Retargets:

| Test | Change | Assertion After Change |
| --- | --- | --- |
| `test_postgres_vtb_dialect_dbapi_and_compiler_features` | Replace the `memory://` driver and `_in_memory_rows` assignment with a local `Port` class whose `execute_with_description(sql, parameters)` returns `([(1, "item1"), (2, "item2")], (("id",), ("name",)))`; build `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://localhost:5432/app"), pool=Port())` | `cur.fetchall() == [(1, "item1"), (2, "item2")]`; `[d[0] for d in cur.description] == ["id", "name"]` |
| `test_postgresql_tenant_session_prepares_psycopg_binds` | `PostgreSQLConfig(dsn="postgresql://localhost:5432/app")` | Existing `port.calls[0][1] == ("app.tenant_id", "acme", True)` |
| `test_postgresql_tenant_session_prepares_asyncpg_binds` | `PostgreSQLConfig(dsn="postgresql://localhost:5432/app")` | Existing assertions unchanged |

## Verification

- Red: add the two new test functions first, then `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py -k "reject_memory_dsn or execution_surface" -q` fails (drivers construct; opaque pool returns `([], ())`).
- Green + lint + typecheck: `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_enterprise_olap_drivers.py && uv run --no-sync ruff format --check src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py`
- Residue gate: `rg -n "memory://|_in_memory_rows|_tx_snapshot" src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py` prints only lines inside the two `_reject_memory_dsn` helpers (per module: the docstring summary, the `Raises:` docstring line, the `startswith("memory://")` condition, and the message).

## Acceptance Criteria

- [ ] All four PostgreSQL/MySQL driver constructors raise `ImproperConfigurationError` for `memory://` DSNs, case-insensitively (R6).
- [ ] `_in_memory_rows`, `_tx_snapshot`, and every `memory://` branch are gone from both modules (R6).
- [ ] An opaque PostgreSQL pool raises `ImproperConfigurationError` instead of returning empty rows (R6).
- [ ] Retargeted SQLAlchemy and tenant-session tests pass against injected ports.
- [ ] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] `ch01` edits Oracle cases in `test_enterprise_olap_drivers.py` and `ch02` edits `test_sqlalchemy_and_sqlspec_vtb.py`; rebase onto those commits before editing.

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — PostgreSQL sync and async drivers.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL sync and async drivers.
[^improper-config]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error exported from vertebra.typing.
