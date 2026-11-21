---
type: Task
id: "native-cursor-data-path:wrapper:t01"
chapter_id: wrapper-layers
title: "Sync DB-API connection returns the native cursor"
description: "Add NATIVE_CURSOR_ENGINES and make SyncDBAPIConnection.cursor() return the SQLite or DuckDB native cursor, created with lazy begin when autocommit is off."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/py/vertebra/db/driver/_common.py:1-54"
  - "src/py/vertebra/db/dbapi/_sync.py:1-70"
  - "src/py/vertebra/db/dbapi/_sync.py:302-364"
  - "src/py/vertebra/db/dbapi/_sync.py::_build_driver"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection"
  - "src/py/vertebra/db/dbapi/_sync.py::_config_dialect"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:1-56"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:321-446"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:683-768"
files:
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/dbapi/_sync.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
estimated_loc: 180
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: dbapi-sync-conn
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: "Sync DB-API connection"
  - id: driver-common
    resource: src/py/vertebra/db/driver/_common.py
    title: "Driver common module"
tags: [dbapi, sqlite, duckdb]
---

# t01 — Sync DB-API connection returns the native cursor

## Objective & Scope Boundary

After this task, `vertebra.db.dbapi.connect("sqlite://...").cursor()` and the DuckDB equivalent return the native cursor object. The Python `SyncDBAPICursor` class is no longer built for those two engines.

In scope:

- The `NATIVE_CURSOR_ENGINES` constant.
- `SyncDBAPIConnection.__init__`, `__slots__`, `cursor()`, `close()`, and the `autocommit` setter.
- Tests in `test_dbapi_standalone.py` for the sync path.

Out of scope:

- `SyncDBAPICursor`, the classifiers, `coerce_dbapi_rows_and_desc`, `_ErrorTranslator`, and the `execute_*` helpers on the connection. They serve the six network engines and are not edited.
- `src/py/vertebra/db/dbapi/_async.py` (t02).
- `src/py/vertebra/db/dbapi/__init__.py`. The constant is not re-exported from the DB-API package.
- Every file under `src/py/vertebra/db/adapters/`. `SyncSQLiteDriver.cursor()` and `SyncDuckDBDriver.cursor()` come from `embedded-cursors`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/db/driver/_common.py:1-54` | Imports, `__all__`, and the dialect tag block the constant follows |
| `src/py/vertebra/db/dbapi/_sync.py:1-70` | Imports and `__all__` |
| `src/py/vertebra/db/dbapi/_sync.py:302-364` | Driver protocol. The exception name bindings follow it |
| `src/py/vertebra/db/dbapi/_sync.py`, function `_build_driver` | How the driver is built |
| `src/py/vertebra/db/dbapi/_sync.py`, class `SyncDBAPIConnection` | Slots, `__init__`, `autocommit`, `cursor`, `execute`, `begin`, `commit`, `rollback`, `close` |
| `src/py/vertebra/db/dbapi/_sync.py`, function `_config_dialect` | The engine key |
| `src/py/tests/unit/db/test_dbapi_standalone.py:1-56`, `321-446`, `683-768` | Imports, fake-driver tests, and the SQLite tests that change |

Ranges past line 364 of `src/py/vertebra/db/dbapi/_sync.py` are located by symbol, because `embedded-cursors` t01 shortens the file.

## Symbol & Signature Contracts

`src/py/vertebra/db/driver/_common.py`:

```python
NATIVE_CURSOR_ENGINES: Final[frozenset[str]] = frozenset({"sqlite", "duckdb"})
```

- Placed directly after `DATAFUSION_DIALECT_TAG`.
- Added to `__all__` in sorted position.
- The module docstring gains one sentence: the constant names the engines whose wrappers use the native cursor, and it is removed when every engine has one.

`src/py/vertebra/db/dbapi/_sync.py`:

- Import `NATIVE_CURSOR_ENGINES` next to `BaseDatabaseConfig` from `vertebra.db.driver._common`.
- `SyncDBAPIConnection.__slots__` gains `"_native_cursor"` in sorted position.
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
def cursor(self) -> Any:
    """Return the native cursor for native-cursor engines, else a plain-tuple DBAPI cursor."""
    self._require_open()
    factory = self._native_cursor
    if factory is None:
        return SyncDBAPICursor(self)
    return factory(lazy_begin=not self._autocommit)
```

- `cursor()` opens no transaction. With `lazy_begin=True` the extension opens it on the first statement.
- New private method, placed before `_require_open`:

```python
def _transaction_open(self) -> bool:
    """Return whether a transaction is open, asking the driver on native-cursor engines."""
    if self._native_cursor is not None and self._driver is not None:
        return bool(self._driver.in_transaction)
    return self._in_transaction
```

- `close()` and the `autocommit` setter test `self._transaction_open()` where they test `self._in_transaction` today. `begin()`, `commit()`, and `rollback()` keep their bodies.
- `SyncDBAPIConnection.execute` keeps its body and changes its return annotation to `Any`.
- No other method changes. An explicit `begin()` still works: the driver opens the transaction and the cursor's lazy begin finds it open.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| SQLite or DuckDB URL, autocommit off, `cursor()` | No `begin`. `driver.in_transaction` is `False` until the first statement |
| First statement on that cursor | The extension opens the transaction. A later `rollback()` undoes the writes |
| Second cursor in the same transaction | Its statements join the open transaction |
| Cursor reused after `commit()` | Its next statement opens a new transaction. `rollback()` undoes that statement |
| `close()` with a transaction the extension opened | The connection rolls it back, because `_transaction_open()` asks the driver |
| `autocommit=True` | `driver.begin()` is never called. A write is visible to a second connection on the same file without `commit()` |
| `autocommit` switched from off to on with a transaction open | The setter commits, using `_transaction_open()`. The next `cursor()` is created with `lazy_begin=False`. A cursor created earlier keeps lazy begin |
| `cursor()` on a closed connection | `InterfaceError` from `_require_open`, as today |
| Constraint violation in `cursor.execute` | `IntegrityError` raised by the extension. `dbapi.IntegrityError` is that class. No translator runs |
| SQL syntax error | `ProgrammingError` or another `DatabaseError` subclass, as the extension raises it. The test asserts `isinstance(exc, dbapi.DatabaseError)` |
| Cancelled statement | `QueryCancelledError`, which `except dbapi.OperationalError` catches |
| `fetchall()` before any `execute` | `ProgrammingError` from the native cursor |
| Injected `driver=` whose URL resolves to SQLite or DuckDB and that has no `cursor` method | `InterfaceError` at construction |
| Injected `driver=` with no resolvable config | Existing path, `SyncDBAPICursor` |
| Engine outside the set | Existing path, unchanged |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/db/test_dbapi_standalone.py` and use real in-memory or `tmp_path` databases.

New tests:

1. `test_native_cursor_engines_constant_lists_embedded_engines`: `NATIVE_CURSOR_ENGINES == frozenset({"sqlite", "duckdb"})` and the name is in `vertebra.db.driver._common.__all__`.
2. `test_sync_dbapi_native_engine_cursor_is_native_cursor`, parametrized over a SQLite and a DuckDB in-memory URL: `type(conn.cursor()).__module__` starts with `vertebra._` and the object is not a `SyncDBAPICursor`. Two calls to `conn.driver.cursor()` return two distinct objects. `execute("select 1")` then `fetchall() == [(1,)]`, `description[0][0]` is the column name, and rows are tuples.
3. `test_sync_dbapi_native_cursor_rollback_and_commit_follow_connection_state`, same parameters: create a table and commit, insert through a cursor, `rollback()`, a new cursor counts zero rows. Insert again, `commit()`, a new cursor counts one row.
4. `test_sync_dbapi_native_cursor_autocommit_persists_without_commit`: file-backed SQLite under `tmp_path`, `autocommit=True`, insert, then a second connection reads the row. `conn.driver.in_transaction` is `False` after the insert.
5. `test_sync_dbapi_native_cursor_dml_rowcount_lastrowid_and_description`: after an `INSERT`, `description is None`, `rowcount == 1`, and `lastrowid` is the new rowid on SQLite. After `INSERT ... RETURNING id`, `description` is not `None` and `fetchall()` returns the id.
6. `test_sync_dbapi_native_cursor_reused_after_commit_starts_new_transaction`, same parameters: create a table and commit. Insert through one cursor and `commit()`. Insert again through the same cursor, then `rollback()`. A new cursor counts one row. `conn.driver.in_transaction` is `True` after the second insert and `False` after the rollback.
7. `test_sync_dbapi_native_cursor_raises_dbapi_exception_classes`: a unique-key violation raises `dbapi.IntegrityError`, and the raised object's type is `vertebra._vertebra.IntegrityError`; a statement against a missing table raises a `dbapi.DatabaseError` subclass; `fetchall()` before `execute` raises `dbapi.ProgrammingError`.
8. `test_sync_dbapi_native_engine_rejects_driver_without_cursor`: a small driver double with `url = "sqlite://"` and no `cursor` attribute raises `InterfaceError` at construction.

Existing tests to change:

- A test that opens SQLite or DuckDB and calls `var`, `nextset`, `setinputsizes`, `setoutputsize`, uses the cursor as a context manager, or iterates it is moved onto an injected driver double with no URL, so it keeps covering `SyncDBAPICursor`.
- `test_sync_dbapi_sqlite_comment_prefixed_insert_captures_and_clears_lastrowid` keeps its assertions on `lastrowid` and reads them from the native cursor.
- `test_sync_dbapi_sqlite_uses_native_driver_and_has_no_sqlite_conn` additionally asserts the cursor is not a `SyncDBAPICursor`.
- Tests of `is_row_returning_sql` and of the translator stay as they are.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q
uv run --no-sync ruff check src/py/vertebra/db/driver/_common.py src/py/vertebra/db/dbapi/_sync.py src/py/tests/unit/db/test_dbapi_standalone.py
uv run --no-sync mypy src/py/vertebra/db/dbapi/_sync.py src/py/vertebra/db/driver/_common.py
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q -k "sqlite and not async"
```

Run test 2 first. If `SyncSQLiteDriver.cursor()` or `SyncDuckDBDriver.cursor()` does not exist, does not accept `lazy_begin`, or does not return the native cursor, set the task to `blocked`. This task does not add either method.

## Acceptance Criteria

- [x] `NATIVE_CURSOR_ENGINES` is defined and exported from `src/py/vertebra/db/driver/_common.py`.
- [x] `SyncDBAPIConnection.cursor()` returns the native cursor for SQLite and DuckDB and `SyncDBAPICursor` for every other engine.
- [x] The constant is read only in `SyncDBAPIConnection.__init__` within `_sync.py`.
- [x] No SQL classifier, translator, or `hasattr` probe runs between `cursor()` and the native `execute` for SQLite and DuckDB.
- [x] `cursor()` calls no `begin`. A cursor reused after `commit()` starts a new transaction that `rollback()` undoes.
- [x] The eight new tests pass and the whole test file passes.
- [x] The sync SQLite cases in the SQLAlchemy test file still pass.

## Notes & Discoveries

- `get_shared_sync_driver` can hand the same native driver to several DB-API connections. `driver.cursor()` returns a new cursor per call, so connections do not share cursor state. Transaction sharing on that driver is unchanged from today.
- `SyncSQLiteCursor` and `SyncDuckDBCursor` are exposed from `vertebra._sqlite` and `vertebra._duckdb` with PyO3's default `type(cur).__module__ == "builtins"`. `test_sync_dbapi_native_engine_cursor_is_native_cursor` therefore asserts exact class identity (`type(cur) is SyncSQLiteCursor` / `SyncDuckDBCursor`) and `not isinstance(cur, SyncDBAPICursor)` instead of checking `__module__.startswith("vertebra._")`.
- `SyncSQLiteCursor.executemany` does not populate `description` for `RETURNING` statements, so `test_dbapi_executemany_returning_and_negative_fetchmany` was moved onto `SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False)` alongside the `SyncDBAPICursor` `var`/`nextset`/`setinputsizes`/`setoutputsize` tests.
- `make lint` was not run end-to-end because the sandbox mounts `~/.cache/uv` read-only; targeted `ruff check`, `ruff format --check`, `mypy`, and `pyright` were all run with `UV_CACHE_DIR=/tmp/uv-cache` and exited `0`.
