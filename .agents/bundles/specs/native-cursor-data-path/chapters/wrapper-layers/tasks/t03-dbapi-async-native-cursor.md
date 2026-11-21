---
type: Task
id: "native-cursor-data-path:wrapper:t03"
chapter_id: wrapper-layers
title: "Async DB-API connection returns the native cursor"
description: "Make AsyncDBAPIConnection.cursor() return the SQLite or DuckDB native async cursor, and prove the SQLAlchemy sync and asyncio engines run on native cursors."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:wrapper:t02"
read_set:
  - "src/py/vertebra/db/dbapi/_async.py:1-60"
  - "src/py/vertebra/db/dbapi/_async.py:124-200"
  - "src/py/vertebra/db/dbapi/_async.py:427-583"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection.cursor"
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py:60-135"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:196-320"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:447-668"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:148-340"
files:
  - src/py/vertebra/db/dbapi/_async.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 210
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: dbapi-async-conn
    resource: src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection
    title: "Async DB-API connection"
  - id: sa-async-adapter
    resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py::AsyncAdapt_vertebra_cursor
    title: "SQLAlchemy asyncio cursor adapter"
tags: [dbapi, sqlalchemy, sqlite, duckdb, async]
---

# t03 — Async DB-API connection returns the native cursor

## Objective & Scope Boundary

After this task, `AsyncDBAPIConnection.cursor()` returns the native async cursor for SQLite and DuckDB. Together with t01 and t02, the SQLAlchemy `sqlite+vtb` and `duckdb+vtb` engines, sync and asyncio, run every statement on a native cursor.

In scope:

- `AsyncDBAPIConnection.__init__`, `__slots__`, `cursor()`, `close()`, and the `autocommit` setter.
- Async tests in `test_dbapi_standalone.py`.
- SQLAlchemy tests for the sync and asyncio engines on SQLite and DuckDB.

Out of scope:

- `AsyncDBAPICursor` and `AsyncSQLDriverProtocol`. They serve the six network engines and are not edited.
- Any file under `src/py/vertebra/extensions/sqlalchemy/`. The adapter change landed in t02.
- Every file under `src/py/vertebra/db/adapters/`. `AsyncSQLiteDriver.cursor()` and `AsyncDuckDBDriver.cursor()` come from `embedded-cursors`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/db/dbapi/_async.py:1-60` | Imports from `_sync` and `__all__` |
| `src/py/vertebra/db/dbapi/_async.py:124-200` | `_build_async_driver` and the start of `AsyncDBAPICursor` |
| `src/py/vertebra/db/dbapi/_async.py:427-583` | The whole connection class |
| `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection.cursor` | The sync form from t01 |
| `src/py/vertebra/extensions/sqlalchemy/async_adapter.py:60-135` | How the adapter calls the cursor |
| `src/py/tests/unit/db/test_dbapi_standalone.py:196-320`, `447-668` | Async tests that change |
| `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:148-340` | SQLite engine tests |

## Symbol & Signature Contracts

`src/py/vertebra/db/dbapi/_async.py`:

- Import `NATIVE_CURSOR_ENGINES` from `vertebra.db.driver._common` and `_config_dialect` from `vertebra.db.dbapi._sync`.
- `AsyncDBAPIConnection.__slots__` gains `"_native_cursor"` in sorted position.
- At the end of `__init__`, before the `autocommit` argument is applied:

```python
self._native_cursor: Callable[[], Any] | None = None
if self._config is not None and self._driver is not None and _config_dialect(self._config) in NATIVE_CURSOR_ENGINES:
    factory = getattr(self._driver, "cursor", None)
    if not callable(factory):
        msg = f"{type(self._driver).__name__} does not provide cursor() for a native-cursor engine"
        raise InterfaceError(msg)
    self._native_cursor = factory
```

- `cursor` becomes:

```python
def cursor(self, *args: Any, **kwargs: Any) -> Any:
    """Return the native async cursor for native-cursor engines, else an AsyncDBAPICursor."""
    self._require_open()
    _ = (args, kwargs)
    factory = self._native_cursor
    if factory is None:
        return AsyncDBAPICursor(self)
    return factory(lazy_begin=not self._autocommit)
```

- Add `_transaction_open(self) -> bool` with the same body as the sync connection in t01. `close()` and the `autocommit` setter test it where they test `self._in_transaction` today.

- `begin()`, `commit()`, `rollback()`, and `__getattr__` are not changed.

`cursor()` stays a plain method because SQLAlchemy calls it without awaiting. It opens no transaction. The extension opens it on the first awaited statement.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| Standalone caller, autocommit off, awaits `begin()`, executes, awaits `rollback()` | The writes are undone |
| Standalone caller, autocommit off, executes without `begin()` | The extension opens the transaction on that statement. `await conn.rollback()` undoes it |
| Cursor reused after `await conn.commit()` | Its next statement opens a new transaction. `rollback()` undoes that statement |
| SQLAlchemy asyncio engine, default isolation level | The first statement opens the transaction in the extension. `rollback()` undoes the writes |
| `cursor()` on a closed connection | `InterfaceError`, as today |
| `await cursor.execute(...)` with a constraint violation | `IntegrityError` raised by the extension. `dbapi.IntegrityError` is that class. SQLAlchemy raises `sqlalchemy.exc.IntegrityError` |
| `await cursor.fetchall()` before `execute` | `ProgrammingError` from the native cursor |
| Injected driver that resolves to SQLite or DuckDB and has no `cursor` method | `InterfaceError` at construction |
| Engine outside the set, or a driver with no resolvable config | Existing path, `AsyncDBAPICursor` |

## Public-Seam Test Blueprint

Tests use real in-memory or `tmp_path` databases.

New tests in `src/py/tests/unit/db/test_dbapi_standalone.py`:

1. `test_async_dbapi_native_engine_cursor_is_native_cursor`, parametrized over SQLite and DuckDB: the cursor is not an `AsyncDBAPICursor`, `await cur.execute("select 1")` then `await cur.fetchall() == [(1,)]`, and `cur.description` and `cur.rowcount` are plain attributes.
2. `test_async_dbapi_native_cursor_explicit_begin_rollback_and_commit`, same parameters: create a table, `await conn.begin()`, insert, `await conn.rollback()`, count is zero. Then `await conn.begin()`, insert, `await conn.commit()`, count is one.
3. `test_async_dbapi_native_cursor_dml_rowcount_lastrowid_and_description`: after an awaited `INSERT`, `description is None`, `rowcount == 1`, and `lastrowid` is set on SQLite. `INSERT ... RETURNING id` sets `description`.
4. `test_async_dbapi_native_cursor_execute_without_begin_is_rolled_back`, same parameters: create a table and commit. Insert through a cursor with no `begin()` call, then `await conn.rollback()`. The count is zero. Then insert, `await conn.commit()`, insert again through the same cursor, `await conn.rollback()`. The count is one.
5. `test_async_dbapi_native_cursor_raises_dbapi_exception_classes`: a unique-key violation raises `dbapi.IntegrityError`; `fetchall()` before `execute` raises `dbapi.ProgrammingError`.

New tests in `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py`:

6. `test_sqlalchemy_sync_engines_run_on_native_cursor`, parametrized over `sqlite+vtb` and `duckdb+vtb`: `engine.raw_connection().cursor()` is not a `SyncDBAPICursor`; an ORM-free round trip (create table, insert two rows, select) returns the rows; `conn.rollback()` undoes an insert.
7. `test_sqlalchemy_async_engines_run_on_native_cursor`, parametrized over the asyncio forms: the driver connection's `cursor()` is not an `AsyncDBAPICursor`; the same round trip and rollback hold.
8. `test_sqlalchemy_native_cursor_integrity_error_and_lastrowid`: on `sqlite+vtb`, a duplicate primary key raises `sqlalchemy.exc.IntegrityError`, and `result.inserted_primary_key` after an autoincrement insert equals the new rowid.

Existing tests to change:

- An async SQLite or DuckDB standalone test that relies on an implicit transaction passes unchanged.
- An async SQLite or DuckDB standalone test that uses `async with conn.cursor()`, `var`, `nextset`, `setinputsizes`, or `setoutputsize` is moved onto an injected driver double with no URL, so it keeps covering `AsyncDBAPICursor`.
- `test_async_dbapi_non_sqlite_and_async_sqlite_driver_dml_lastrowid` reads `lastrowid` from the native cursor in its SQLite half.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py -q
uv run --no-sync ruff check src/py/vertebra/db/dbapi/_async.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
uv run --no-sync mypy src/py/vertebra/db/dbapi/_async.py
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q
```

Run test 1 first. If `AsyncSQLiteDriver.cursor()` or `AsyncDuckDBDriver.cursor()` does not exist, does not accept `lazy_begin`, or does not return the native async cursor, set the task to `blocked`. This task does not add either method.

## Acceptance Criteria

- [x] `AsyncDBAPIConnection.cursor()` returns the native async cursor for SQLite and DuckDB and `AsyncDBAPICursor` for every other engine.
- [x] The constant is read only in `AsyncDBAPIConnection.__init__` within `_async.py`.
- [x] The eight new tests pass.
- [x] The three listed test commands pass in full.
- [x] No file under `src/py/vertebra/extensions/sqlalchemy/` has a diff from this task.

## Notes & Discoveries

- `AsyncDBAPIConnection._native_cursor` is typed as `Callable[..., Any] | None` (matching `SyncDBAPIConnection._native_cursor`) so mypy accepts `factory(lazy_begin=not self._autocommit)`.
- `AsyncDBAPIConnection.autocommit` setter is synchronous and does not check `self._in_transaction`; `close()` now checks `self._transaction_open()` so transactions opened lazily by native cursors are rolled back or committed on close.
- On SQLite with `autocommit=False`, a `SELECT` on a `lazy_begin=True` native cursor opens a transaction in the extension while `AsyncDBAPIConnection.begin()` checks `self._in_transaction` (unchanged by contract); callers mixing implicit `lazy_begin` queries with explicit `await conn.begin()` must close the read transaction with `commit()` or `rollback()` first.
- Full workspace `make lint` was not run because the sandbox blocks Rust/uv cache writes outside `/tmp`; targeted `ruff format --check`, `ruff check`, `mypy`, and `pyright` all exited 0 on all modified files.
