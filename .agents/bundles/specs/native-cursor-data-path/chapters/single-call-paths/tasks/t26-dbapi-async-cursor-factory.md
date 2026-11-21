---
type: Task
id: "native-cursor-data-path:single:t26"
chapter_id: single-call-paths
title: "Async DB-API cursor factory bound to the native factory"
description: "Replace the AsyncDBAPIConnection.cursor method with an instance attribute holding a callable bound to the async driver's cursor factory, and confirm the SQLAlchemy asyncio adapter still receives a native async cursor."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t25"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t25-dbapi-sync-cursor-factory.md"
  - "src/py/vertebra/db/dbapi/_async.py:1-48"
  - "src/py/vertebra/db/dbapi/_async.py:118-240"
  - "src/py/vertebra/db/dbapi/_async.py:271-296"
  - "src/py/vertebra/db/dbapi/_sync.py::_config_dialect"
  - "src/py/vertebra/db/adapters/postgres/_async.py:353-366"
  - "src/py/vertebra/db/adapters/oracle/_async.py:262-272"
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py:60-70"
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py:156-168"
  - "src/py/tests/unit/db/test_dbapi_standalone.py::test_async_dbapi_native_engine_rejects_driver_without_cursor"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:160-216"
files:
  - src/py/vertebra/db/dbapi/_async.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 160
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: dbapi-async-conn
    resource: src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection
    title: "Async DB-API connection"
  - id: sa-async-adapter
    resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py::AsyncAdapt_vertebra_cursor
    title: "SQLAlchemy asyncio cursor adapter"
tags: [dbapi, sqlalchemy, performance]
---

# t26 — Async DB-API cursor factory bound to the native factory

## Objective & Scope Boundary

After this task, `connection.cursor()` on an async DB-API connection runs no Python frame of the DB-API module on every engine except PostgreSQL, and the SQLAlchemy asyncio adapter receives the native async cursor from that call. PostgreSQL keeps the per-call decision it has today, with no change in behavior.[^dbapi-async-conn][^sa-async-adapter]

This task is the async form of t25. It shares both test files with t25, which is why it runs after it.

In scope, all in `src/py/vertebra/db/dbapi/_async.py`:

- `AsyncDBAPIConnection.__slots__`, `__init__`, the `autocommit` setter, and `close`.
- The `cursor` method: deleted. `cursor` becomes an instance attribute.
- Five new private methods on `AsyncDBAPIConnection`.
- Tests in `test_dbapi_standalone.py` and `test_sqlalchemy_vtb_dialects_and_dbapi.py`.

Out of scope:

- `src/py/vertebra/db/dbapi/_sync.py`. `_POOL_ROUTED_DIALECTS` and `_config_dialect` are imported from it, not edited.
- `src/py/vertebra/extensions/sqlalchemy/async_adapter.py`. It is read to confirm the call it makes.
- `AsyncDBAPIConnection.__getattr__`, `begin`, `commit`, `rollback`, `_transaction_open`, and `_require_open`.

## Bounded Read Manifest

| Entry | Why |
| --- | --- |
| The t25 worksheet | The contract this task mirrors: how the callable is formed and why routing stays correct |
| `_async.py:1-48` | Imports, including the names taken from `_sync.py` |
| `_async.py:118-240` | `AsyncDBAPIConnection` from `__slots__` through `cursor` |
| `_async.py:271-296` | `close`, `_transaction_open`, `_require_open` |
| `_sync.py`, `_config_dialect` and the constant `_POOL_ROUTED_DIALECTS` that t25 adds above `SyncDBAPIConnection` | Names this task imports |
| `adapters/postgres/_async.py:353-366`, `adapters/oracle/_async.py:262-272` | The async `cursor` methods are plain methods with the same routing as the sync drivers |
| `async_adapter.py:60-70`, `156-168` | `_aenter_cursor` returns the cursor unchanged; the connection adapter names its cursor classes |
| Test file symbols and `test_sqlalchemy_vtb_dialects_and_dbapi.py:160-216` | The missing-cursor test and the asyncio adapter setup the new test reuses |

Confirmed from the installed `sqlalchemy` package, `sqlalchemy/connectors/asyncio.py`: `AsyncAdapt_dbapi_cursor.__init__` calls `self._make_new_cursor(self._connection)`, which returns `connection.cursor()` with no argument. The adapter never passes a positional or keyword argument to `cursor`.

## Symbol & Signature Contracts

Imports in `_async.py`:

- Add `from functools import partial`.
- Add `_POOL_ROUTED_DIALECTS` and `_config_dialect` to the existing `from vertebra.db.dbapi._sync import (...)` list.

`AsyncDBAPIConnection.__slots__`: remove `"_native_cursor"`. Add `"_driver_cursor"` and `"cursor"` in sorted position. The `cursor` method is deleted.

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

The `autocommit` setter keeps its check. After `self._autocommit = value` it adds:

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
    msg = "AsyncDBAPIConnection has no underlying driver cursor factory"
    raise InterfaceError(msg)

def _closed_cursor(self) -> Any:
    """Raise `InterfaceError` for a cursor request on a closed connection."""
    raise InterfaceError("Cannot operate on a closed connection.")
```

- All five are plain methods. `cursor` on an async driver is a plain method that returns a native async cursor, so the stored callable is not awaited.
- The table of states, the per-engine routing, and the rule that only PostgreSQL decides per call are the ones in the t25 worksheet. `AsyncPostgreSQLDriver.cursor` has the same three routes and the same `InterfaceError` as the sync driver.
- `__init__` assigns `cursor` on every path. `__getattr__` forwards only names the instance lacks, so it never forwards `cursor`.

## Edge Cases & Error Semantics

The table in the t25 worksheet applies with these differences:

| Case | Behavior |
| --- | --- |
| `connection.cursor(1)` or `connection.cursor(server_side=True)` | `TypeError` from the driver method. Today the method accepts and ignores any argument |
| `connection.autocommit = True` | The callable is rebuilt. The async setter does not commit, as today |
| `connection.cursor()` after `await connection.close()` | `InterfaceError` with `Cannot operate on a closed connection.` |
| A standalone caller with autocommit off | The cursor has lazy begin bound, so the caller does not await `begin()` |
| SQLAlchemy asyncio adapter creating a cursor | `connection.cursor()` returns the native async cursor, and `_aenter_cursor` returns it unchanged |
| SQLAlchemy server-side cursor | The adapter calls the same `connection.cursor()` with no argument |

## Public-Seam Test Blueprint

New tests in `src/py/tests/unit/db/test_dbapi_standalone.py`, each the async form of the t25 test with the same assertions:

1. `test_async_dbapi_cursor_is_instance_attribute`: `"cursor"` is in `AsyncDBAPIConnection.__slots__`, `"_native_cursor"` is not, and the class attribute is a `member_descriptor`.
2. `test_async_dbapi_cursor_callable_follows_autocommit`, `@pytest.mark.parametrize("autocommit", [False, True])`, on `AsyncDBAPIConnection(driver=AsyncSQLiteDriver(), share_pool=False, autocommit=autocommit)`: a `functools.partial` with `keywords == {"lazy_begin": True}` when off, the driver's bound `cursor` when on. `conn.cursor()` returns an `AsyncSQLiteCursor` without `await`.
3. `test_async_dbapi_cursor_callable_rebuilt_on_autocommit_change`.
4. `test_async_dbapi_cursor_lazy_begin_by_engine`, `test_async_dbapi_postgres_cursor_decides_per_call`, `test_async_dbapi_postgres_cursor_follows_pinning_changes`, and `test_async_dbapi_postgres_autocommit_binds_driver_cursor`: the async forms of t25 tests 4, 4a, 4b, and 4c, with the same parameters and an async driver double whose `cursor` is a plain method.
5. `test_async_dbapi_cursor_runs_no_dbapi_module_frame`: no collected file name ends with `db/dbapi/_async.py` or `db/dbapi/_sync.py`.
6. `test_async_dbapi_cursor_after_close_raises_interface_error`.
7. `test_async_dbapi_lazy_begin_still_opens_transaction`: on a `tmp_path` SQLite database with autocommit off, create a table and commit, insert through `conn.cursor()` without awaiting `begin()`, and `await conn.rollback()`. A new cursor counts zero rows.
8. `test_async_dbapi_getattr_does_not_shadow_cursor`: `conn.cursor` is the stored callable and not `conn.driver.cursor` when autocommit is off.

New test in `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py`:

9. `test_asyncio_adapter_receives_native_async_cursor`: using the setup of the existing test that calls `greenlet_spawn(module.connect, driver=driver, share_pool=False)` with an `AsyncSQLiteDriver`, create an adapted cursor inside `greenlet_spawn`. Its `_cursor` attribute has the exact type `AsyncSQLiteCursor`. `execute("select 1")` followed by `fetchall()` returns `[(1,)]`.

Existing tests:

- `test_async_dbapi_native_engine_rejects_driver_without_cursor` keeps passing unchanged.
- The async closed-connection assertions in the test around `test_dbapi_standalone.py:232-250` keep passing unchanged.
- A call in either test file that passes an argument to an async connection's `cursor` drops the argument.

Test doubles without `cursor()`: both files pass in full before this task. The async twin of `_FakeMySQLSyncProtocolDriver` in `src/py/tests/unit/db/test_mysql_driver.py` belongs to the test tree repair in `cleanup-and-checks`.


## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_dbapi_standalone.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/db/dbapi/_async.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
uv run --no-sync --frozen mypy src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlalchemy
test -z "$(git grep -n '_native_cursor' -- src/py/vertebra/db/dbapi)"
```

## Acceptance Criteria

- [x] `AsyncDBAPIConnection` has no `cursor` method. `cursor` is a `__slots__` entry assigned in `__init__`.
- [x] `connection.cursor()` runs no frame of the DB-API module on an open connection with a driver, except on PostgreSQL with autocommit off.
- [x] The callable is rebuilt by the `autocommit` setter and replaced by `close()`.
- [x] The `InterfaceError` fallback exists only in `_session_routed_cursor`, and only PostgreSQL connections with autocommit off use it.
- [x] PostgreSQL cursor behavior is unchanged: lazy begin on a pinned session, a plain cursor on the pool.
- [x] A driver without `cursor()` raises `InterfaceError` when the connection is built.
- [x] The SQLAlchemy asyncio adapter holds a native async cursor.
- [x] `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` has no diff.
- [x] The twelve new tests pass and both test files pass in full.

## Notes & Discoveries

- `test_async_adapter_never_calls_cursor_aenter` in `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py` previously monkeypatched `AsyncDBAPIConnection.cursor`. With `cursor` moved to a `__slots__` instance attribute, class-level monkeypatching overwrote the slot descriptor and raised `AttributeError` in `__init__`; updated the test to monkeypatch `AsyncSQLiteDriver.cursor` instead, preserving the exact `_NoAenterCursorProxy` check.
- `make dev` and repo-wide `make lint` were not run because this task made zero Rust changes and repo-wide `make lint` runs mutating auto-fixers across all workspace files; targeted `ruff check`, `ruff format --check`, `mypy`, `pyright`, `_native_cursor` grep, and both pytest suites all passed with `UV_CACHE_DIR=/tmp/uv-cache`.

[^dbapi-async-conn]: `src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection`
[^sa-async-adapter]: `src/py/vertebra/extensions/sqlalchemy/async_adapter.py::AsyncAdapt_vertebra_cursor`
