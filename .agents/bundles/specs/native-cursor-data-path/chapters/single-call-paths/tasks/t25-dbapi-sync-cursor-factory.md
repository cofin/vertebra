---
type: Task
id: "native-cursor-data-path:single:t25"
chapter_id: single-call-paths
title: "Sync DB-API cursor factory bound to the native factory"
description: "Replace the SyncDBAPIConnection.cursor method with an instance attribute holding a callable bound to the driver's cursor factory, rebuilt when autocommit changes and replaced on close. PostgreSQL keeps a callable that decides per call."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t06"
  - "native-cursor-data-path:cleanup:t18"
read_set:
  - "src/py/vertebra/db/dbapi/_sync.py:7-22"
  - "src/py/vertebra/db/dbapi/_sync.py:640-773"
  - "src/py/vertebra/db/dbapi/_sync.py:830-868"
  - "src/py/vertebra/db/dbapi/_sync.py:936-942"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:348-361"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:260-270"
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:116-118"
  - "src/py/vertebra/db/adapters/mysql/_sync.py:117-121"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:1-60"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:196-232"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:536-560"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:925-937"
files:
  - src/py/vertebra/db/dbapi/_sync.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 170
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: dbapi-sync-conn
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: "Sync DB-API connection"
tags: [dbapi, sqlalchemy, performance]
---

# t25 — Sync DB-API cursor factory bound to the native factory

## Objective & Scope Boundary

After this task, `connection.cursor()` on a sync DB-API connection runs no Python frame of the DB-API module on every engine except PostgreSQL. The connection holds a callable bound to the driver's cursor factory, and SQLAlchemy calls that callable directly. PostgreSQL keeps the per-call decision it has today, with no change in behavior.[^dbapi-sync-conn]

In scope, all in `src/py/vertebra/db/dbapi/_sync.py`:

- `SyncDBAPIConnection.__slots__`, `__init__`, the `autocommit` setter, and `close`.
- The `cursor` method: deleted. `cursor` becomes an instance attribute.
- Five new private methods on `SyncDBAPIConnection` and one module constant.
- Tests in `test_dbapi_standalone.py`. `test_sqlalchemy_vtb_dialects_and_dbapi.py` runs as the regression check and gains one test.

Out of scope:

- `src/py/vertebra/db/dbapi/_async.py` (t26).
- Every native driver's `cursor` method.
- `SyncDBAPIConnection.execute`. It keeps calling `self.cursor()`.
- `begin`, `commit`, `rollback`, `_transaction_open`, and `_require_open`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `_sync.py:7-22` | Imports |
| `_sync.py:640-773` | `SyncDBAPIConnection` from `__slots__` through `execute` |
| `_sync.py:830-868` | `close`, `_transaction_open`, `_require_open` |
| `_sync.py:936-942` | `_config_dialect` |
| `adapters/postgres/_sync.py:348-361` | `cursor` routes to the pinned session, the tenant session, or the pool, and rejects `lazy_begin=True` on the pool |
| `adapters/oracle/_sync.py:260-270` | `cursor` uses the pinned client or the single client, and raises on a pool with no open transaction |
| `adapters/sqlite/_sync.py:116-118`, `adapters/mysql/_sync.py:117-121` | `cursor` on single-client drivers |
| `test_dbapi_standalone.py:1-60`, `196-232`, `536-560`, `925-937` | Imports, the closed-connection test, the stub drivers, and the missing-cursor test |

## Symbol & Signature Contracts

Imports in `_sync.py`: add `from functools import partial`.

Module constant, placed directly above `class SyncDBAPIConnection`:

```python
_POOL_ROUTED_DIALECTS: Final[frozenset[str]] = frozenset({"postgres"})
```

`SyncDBAPIConnection.__slots__`: remove `"_native_cursor"`. Add `"_driver_cursor"` and `"cursor"` in sorted position. The class has `__slots__`, so `cursor` is declared there and the method of the same name is deleted. A method and a `__slots__` entry cannot share a name.

`__init__`: the block from `self._native_cursor: Callable[..., Any] | None = None` to the end of the method becomes:

```python
self._driver_cursor: Callable[..., Any] | None = None
if self._driver is not None:
    factory = getattr(self._driver, "cursor", None)
    if not callable(factory):
        msg = f"{type(self._driver).__name__} does not provide cursor()"
        raise InterfaceError(msg)
    self._driver_cursor = factory
self.cursor: Callable[..., Any] = self._bound_cursor()

if autocommit is not None:
    self.autocommit = bool(autocommit)
```

The `autocommit` setter keeps its checks and its commit. After `self._autocommit = value` it adds:

```python
if not self.closed:
    self.cursor = self._bound_cursor()
```

`close`: directly after `self.closed = True`, add `self.cursor = self._closed_cursor`.

New private methods, placed before `_transaction_open`:

```python
def _bound_cursor(self) -> Callable[..., Any]:
    """Return the callable stored in `cursor` for the current autocommit state.

    The driver's `cursor` method is stored unchanged when autocommit is on. PostgreSQL, whose driver
    accepts lazy begin only while a session is pinned, gets `_session_routed_cursor`, which decides on
    each call. Every other engine gets the driver method with `lazy_begin=True` bound.
    """
    factory = self._driver_cursor
    if factory is None:
        return self._no_driver_cursor
    if self._autocommit:
        return factory
    if self._engine_key() in _POOL_ROUTED_DIALECTS:
        return self._session_routed_cursor
    return partial(factory, lazy_begin=True)

def _session_routed_cursor(self) -> Any:
    """Return a lazy-begin cursor when the driver has a pinned session, else a plain cursor.

    Raises:
        InterfaceError: When the driver cannot provide a cursor at all.
    """
    factory = cast("Callable[..., Any]", self._driver_cursor)
    try:
        return factory(lazy_begin=True)
    except InterfaceError:
        return factory(lazy_begin=False)

def _engine_key(self) -> str:
    """Return the lowercase engine name from the config, or from the driver when no config resolved."""
    if self._config is not None:
        return _config_dialect(self._config)
    return str(getattr(self._driver, "dialect", "")).lower()

def _no_driver_cursor(self) -> Any:
    """Raise `InterfaceError` for a connection built without a driver."""
    msg = "SyncDBAPIConnection has no underlying driver cursor factory"
    raise InterfaceError(msg)

def _closed_cursor(self) -> Any:
    """Raise `InterfaceError` for a cursor request on a closed connection."""
    raise InterfaceError("Cannot operate on a closed connection.")
```

How the callable is formed:

| State | `connection.cursor` |
| --- | --- |
| Autocommit on, any engine | The driver's bound `cursor` method. Its `lazy_begin` default is `False` on every engine |
| Autocommit off, PostgreSQL | `self._session_routed_cursor`, which tries `lazy_begin=True` and uses `lazy_begin=False` when the driver raises `InterfaceError` |
| Autocommit off, every other engine | `functools.partial(driver.cursor, lazy_begin=True)` |
| No driver | `self._no_driver_cursor` |
| Closed | `self._closed_cursor` |

Why routing stays correct. The callable is bound to the driver's `cursor` method, not to a client. Each driver decides the target inside that method on every call:

| Engine | What `driver.cursor` resolves per call |
| --- | --- |
| PostgreSQL | The pinned transaction session, then the pinned tenant session, then the pool |
| Oracle | The pinned transaction client, then the single client. A pool with no open transaction raises `InterfaceError` |
| SQLite, DuckDB | The driver's one client, or `InterfaceError` when the driver is closed |
| MySQL, SQL Server | The driver's one client, connected on demand |
| Spanner, BigQuery | The active transaction or the client. Their connections require autocommit, so lazy begin is never bound |

Engines whose routing changes after the connection is built are PostgreSQL and Oracle. Both keep the driver's `cursor` method as the function that is called, so a transaction opened later is found. No engine is bound to a client method.

Which engine decides per call, read from the driver code:

- PostgreSQL decides per call. `SyncPostgreSQLDriver.cursor` accepts `lazy_begin=True` on a pinned transaction session or a pinned tenant session and raises `InterfaceError` on the pool. Pinning changes with each transaction, so no single bound value is right. `_session_routed_cursor` keeps the two-step call the `cursor` method has now.
- Oracle is decided once. A single-client driver accepts lazy begin at any time. A pooled driver raises `InterfaceError` from `cursor()` outside a transaction whatever the flag is, and accepts lazy begin inside one. Binding `lazy_begin=True` gives the result the two-step call gives in every case.
- SQLite, DuckDB, MySQL, and SQL Server accept lazy begin at any time. Spanner and BigQuery never receive it.

No other driver rejects `lazy_begin=True` depending on transaction state. After this task the DB-API module has no per-call test of `self._autocommit`, and its only per-call `try` is in `_session_routed_cursor`.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| `connection.cursor()` on an open SQLite connection, autocommit off | `partial` calls `driver.cursor(lazy_begin=True)`. No frame of `_sync.py` runs |
| `connection.autocommit = True` | The setter commits an open transaction, then stores the driver's `cursor` method. A cursor created earlier keeps lazy begin |
| `connection.autocommit = False` on Spanner or BigQuery | `NotSupportedError` from the setter, as today. `cursor` is unchanged |
| `connection.cursor()` after `close()` | `InterfaceError` with `Cannot operate on a closed connection.` |
| `connection.autocommit = True` after `close()` | The flag changes. `cursor` still raises `InterfaceError` |
| A driver with no `cursor` attribute | `InterfaceError` from `__init__`, as today |
| A driver whose `cursor` does not accept `lazy_begin` | `TypeError` on the first call with autocommit off, as today |
| `connection.cursor(1)` | `TypeError` from the driver method. The callable takes no positional argument |
| PostgreSQL, autocommit off, no pinned session | `driver.cursor(lazy_begin=True)` raises `InterfaceError`, and the cursor comes from `driver.cursor(lazy_begin=False)` on the pool. Unchanged |
| PostgreSQL, after `connection.begin()` | The cursor is created on the pinned session with `lazy_begin=True`. Kept across `commit()`, it opens a new transaction on its next statement. Unchanged |
| PostgreSQL with a pinned tenant session and no explicit `begin()` | The cursor is created with `lazy_begin=True`, so its first statement opens the implicit transaction. Unchanged |
| PostgreSQL, autocommit on | The driver's bound `cursor` method. No frame of `_sync.py` runs |
| Oracle pooled driver, outside a transaction | `InterfaceError` from the driver, as today |
| A shared driver closed by another holder | The driver's `cursor` raises its own closed-driver error |
| `SyncDBAPIConnection.execute(sql)` | Calls the attribute and then `cur.execute`, as today |


## Public-Seam Test Blueprint

New tests in `src/py/tests/unit/db/test_dbapi_standalone.py`:

1. `test_sync_dbapi_cursor_is_instance_attribute`: `"cursor"` is in `SyncDBAPIConnection.__slots__`, `"_native_cursor"` is not, and `type(vars(SyncDBAPIConnection)["cursor"]).__name__ == "member_descriptor"`.
2. `test_sync_dbapi_cursor_callable_follows_autocommit`, `@pytest.mark.parametrize("autocommit", [False, True])`, on `SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False, autocommit=autocommit)`: with autocommit off, `conn.cursor` is a `functools.partial` whose `func == conn.driver.cursor` and whose `keywords == {"lazy_begin": True}`. With autocommit on, `conn.cursor == conn.driver.cursor`. In both cases `conn.cursor()` returns a `SyncSQLiteCursor`.
3. `test_sync_dbapi_cursor_callable_rebuilt_on_autocommit_change`: start with autocommit off, set it on, then off. The callable is a `partial`, then the bound method, then a new `partial`.
4. `test_sync_dbapi_cursor_lazy_begin_by_engine`, parametrized over the dialects `oracle`, `mysql`, `mssql`, `sqlite`, and `duckdb`.
   - The driver double has a `dialect` attribute and `in_transaction = False`.
   - Its `cursor(self, *, lazy_begin=False)` records the flag.
   - The connection is built with `driver=` and no URL.
   - `conn.cursor` is a `functools.partial`, and one `conn.cursor()` call records `[True]`.
4a. `test_sync_dbapi_postgres_cursor_decides_per_call`, parametrized over `("pinned", "expected_flags")` with the rows `(False, [True, False])` and `(True, [True])`.
   - The driver double has `dialect = "postgres"` and a `pinned` attribute.
   - Its `cursor` records the flag. It raises `InterfaceError` for `lazy_begin=True` when `pinned` is false.
   - `conn.cursor == conn._session_routed_cursor`.
   - One `conn.cursor()` call records `expected_flags`.
4b. `test_sync_dbapi_postgres_cursor_follows_pinning_changes`, with the same double.
   - Call `conn.cursor()` unpinned. Set `pinned = True` and call again. Set `pinned = False` and call again.
   - The cursors are created with `lazy_begin` `False`, `True`, `False`.
   - This pins both behaviors. A cursor created while a transaction session or a tenant session is pinned has lazy begin.
   - Such a cursor opens a new transaction after `commit()`, and opens the implicit transaction on a tenant session.
4c. `test_sync_dbapi_postgres_autocommit_binds_driver_cursor`: the same double with `autocommit=True`. `conn.cursor == driver.cursor`, and a call records `[False]`.
5. `test_sync_dbapi_cursor_runs_no_dbapi_module_frame`: with `sys.setprofile` collecting `frame.f_code.co_filename` for `call` events during one `conn.cursor()` on a SQLite connection, no collected name ends with `db/dbapi/_sync.py`. The profile hook is removed in a `finally` block.
6. `test_sync_dbapi_cursor_after_close_raises_interface_error`: after `conn.close()`, `conn.cursor()` raises `InterfaceError`. Setting `conn.autocommit = True` afterwards leaves `conn.cursor()` raising `InterfaceError`.
7. `test_sync_dbapi_lazy_begin_still_opens_transaction`: on a `tmp_path` SQLite database with autocommit off, create a table and commit, insert through `conn.cursor()`, and `rollback()`. A new cursor counts zero rows.

New test in `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py`:

8. `test_sqlite_vtb_engine_cursor_is_native_cursor`: with `create_engine("sqlite+vtb:///:memory:")`, `engine.raw_connection().cursor()` returns a `SyncSQLiteCursor`, and `connection.execute(text("select 1")).scalar() == 1` inside `engine.connect()`.

Existing tests:

- `test_sync_dbapi_native_engine_rejects_driver_without_cursor` keeps passing unchanged.
- The closed-connection assertions at `test_dbapi_standalone.py:196-232` keep passing unchanged.
- Any test in the two files that reads `conn._native_cursor` reads `conn._driver_cursor`.

Test doubles without `cursor()`:

- Both test files pass in full before this task, so neither holds a double that fails for lack of `cursor()`. None is edited here.
- `_FakeMySQLSyncProtocolDriver` in `src/py/tests/unit/db/test_mysql_driver.py` fails today with `does not provide cursor()`. It belongs to the test tree repair in `cleanup-and-checks`.


## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_dbapi_standalone.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/db/dbapi/_sync.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
uv run --no-sync --frozen mypy src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlalchemy
test -z "$(git grep -n '_native_cursor' -- src/py/vertebra/db/dbapi/_sync.py)"
```

The async tests in both files still pass after this task, because `_async.py` is not edited.

## Acceptance Criteria

- [ ] `SyncDBAPIConnection` has no `cursor` method. `cursor` is a `__slots__` entry assigned in `__init__`.
- [ ] `connection.cursor()` runs no frame of `src/py/vertebra/db/dbapi/_sync.py` on an open connection with a driver, except on PostgreSQL with autocommit off.
- [ ] The callable is rebuilt by the `autocommit` setter and replaced by `close()`.
- [ ] The `InterfaceError` fallback exists only in `_session_routed_cursor`, and only PostgreSQL connections with autocommit off use it.
- [ ] A driver without `cursor()` raises `InterfaceError` when the connection is built.
- [ ] PostgreSQL cursor behavior is unchanged: lazy begin on a pinned session, a plain cursor on the pool.
- [ ] The eleven new tests pass and both test files pass in full.

## Notes & Discoveries

- The callable is bound to the driver's `cursor` method on every engine. Binding the native client's `cursor` directly would remove one more interpreted frame on SQLite and DuckDB, but a shared driver can replace its client when it reconnects, and the drivers expose no public client accessor.

[^dbapi-sync-conn]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection`
