---
type: Task
id: "native-cursor-data-path:wrapper:t05"
chapter_id: wrapper-layers
title: "SQLSpec sync base on the native cursor"
description: "Make BaseSyncDriver yield a native cursor from with_cursor, read results from cursor attributes, reuse one exception handler, and drop the sync dispatch_execute_many overrides in the SQLite and DuckDB adapters."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:wrapper:t01"
  - "native-cursor-data-path:wrapper:t04"
read_set:
  - "src/py/vertebra/extensions/sqlspec/_driver.py:1-100"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:117-137"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:237-298"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:355-466"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:518-617"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:882-894"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:1-92"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:1-112"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:1-60"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:222-312"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:740-838"
files:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 250
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec sync adapter base"
tags: [sqlspec, sqlite, duckdb]
---

# t05 — SQLSpec sync base on the native cursor

## Objective & Scope Boundary

After this task, a sync SQLSpec session on SQLite or DuckDB runs each statement as one `cursor.execute` on a native cursor. A cache hit takes upstream SQLSpec's direct cursor path. Rows reach SQLSpec as tuples with column names from `cursor.description`.

In scope:

- In `src/py/vertebra/extensions/sqlspec/_driver.py`: a per-instance flag read from `NATIVE_CURSOR_ENGINES` in `BaseSyncDriver.__init__`; `with_cursor`, `handle_database_exceptions`, `dispatch_execute`, `dispatch_execute_many`, `dispatch_select_stream`, `collect_rows`, `resolve_rowcount`; a new private class `_NativeSyncCursorContext`.
- In the SQLite and DuckDB adapter `driver.py` files: delete `dispatch_execute_many` from `SQLiteSyncDriver` and `DuckDBSyncDriver`. Those overrides call `execute_many` on the object from `with_cursor` and convert its return value with `int()`. A native cursor has `executemany` and returns itself, so the overrides cannot stay once the base yields a native cursor.

Out of scope:

- `BaseAsyncDriver` and the async adapter classes (t06).
- `_map_native_database_exception`. The native cursor raises `vertebra._vertebra` classes, which its entry check and class-specific branches already test. It is not edited.
- `_unwrap_typed_parameters`, `_normalize_bound_params`, `_prepare_native_parameters`, `_compile_for_native`, and `_fetch_rows_with_columns`. They stay for the six network adapters.
- The `_prepare_native_parameters` overrides and the `core.py` normalizers in the SQLite and DuckDB adapters (t07, t08). The native path does not call them after this task.
- `select_to_arrow`, `select_to_storage`, `stream_arrow_batches`, and `execute_stack`. They are not cursor paths.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlspec/_driver.py:1-100` | Imports and `__all__` |
| `src/py/vertebra/extensions/sqlspec/_driver.py:117-137` | `_DeferredSyncExceptionHandler` |
| `src/py/vertebra/extensions/sqlspec/_driver.py:237-298` | `BaseSyncDriver` slots, `__init__`, parameter helpers |
| `src/py/vertebra/extensions/sqlspec/_driver.py:355-466` | `_map_native_database_exception`, read only, for the classes its branches test |
| `src/py/vertebra/extensions/sqlspec/_driver.py:518-617` | Handler factory, `with_cursor`, dispatch methods, stream |
| `src/py/vertebra/extensions/sqlspec/_driver.py:882-894` | `collect_rows`, `resolve_rowcount` |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:1-92` | Imports and `SQLiteSyncDriver` |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:1-112` | Imports and `DuckDBSyncDriver` up to its `dispatch_execute_many` |
| `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:1-60`, `222-312`, `740-838` | Imports, base driver tests, `__slots__` surface test |

Upstream reference, read in the installed `sqlspec` package: `SyncDriverAdapterBase._execute_cache_hit_with_cursor` in `sqlspec/driver/_sync.py`. It takes the direct path only when the object from `with_cursor` is not `self.connection` and has `execute`.

## Symbol & Signature Contracts

Imports in `_driver.py`:

- `from contextlib import nullcontext`. Drop `contextmanager` from the existing `contextlib` import.
- `from vertebra.db.driver._common import NATIVE_CURSOR_ENGINES`.
- No import from `vertebra.db.dbapi` is added.

New module-level class, placed before `BaseSyncDriver`:

```python
class _NativeSyncCursorContext:
    """Context manager opening one native cursor on a driver and closing it on exit."""

    __slots__ = ("_cursor", "_driver")

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self._cursor: Any = None

    def __enter__(self) -> Any:
        self._cursor = self._driver.cursor()
        return self._cursor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        cursor = self._cursor
        if cursor is not None:
            self._cursor = None
            cursor.close()
```

`BaseSyncDriver`:

- `__slots__ = ("_driver_closed", "_exception_handler", "_native_cursor", "_shared_pool", "native_driver")`.
- `__init__` adds, after the existing assignments:

```python
self._native_cursor: bool = (
    str(getattr(getattr(connection, "config", None), "dialect", "")).lower() in NATIVE_CURSOR_ENGINES
)
self._exception_handler: _DeferredSyncExceptionHandler = _DeferredSyncExceptionHandler(self._map_exception)
```

- `handle_database_exceptions(self) -> _DeferredSyncExceptionHandler`: when `self._native_cursor`, set `self._exception_handler.pending_exception = None` and return that object. Otherwise return `_DeferredSyncExceptionHandler(self._map_exception)` as today.
- `with_cursor(self, connection: Any) -> AbstractContextManager[Any]`: a plain method with no decorator. Returns `_NativeSyncCursorContext(connection)` when `self._native_cursor`, else `nullcontext(connection)`.
- `dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult`: when `self._native_cursor`:

```python
effective_config = getattr(statement, "statement_config", None) or self.statement_config
sql, params = self._compiled_sql(statement, effective_config)
cursor.execute(sql, params or ())
description = cursor.description
if description is not None:
    rows = cursor.fetchall()
    return self.create_execution_result(
        cursor,
        selected_data=rows,
        column_names=[column[0] for column in description],
        data_row_count=len(rows),
        is_select_result=True,
        row_format="tuple",
        last_inserted_id=cursor.lastrowid,
    )
rowcount = cursor.rowcount
return self.create_execution_result(
    cursor,
    rowcount_override=rowcount if rowcount > 0 else 0,
    last_inserted_id=cursor.lastrowid,
)
```

  Otherwise the existing body runs unchanged.
- `dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult`: when `self._native_cursor`:

```python
effective_config = getattr(statement, "statement_config", None) or self.statement_config
sql, prepared = self._compiled_sql(statement, effective_config)
if not prepared:
    return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
cursor.executemany(sql, prepared)
rowcount = cursor.rowcount
return self.create_execution_result(cursor, rowcount_override=rowcount if rowcount > 0 else 0, is_many_result=True)
```

  Otherwise the existing body runs unchanged.
- `collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]`: when `self._native_cursor`, read `description = cursor.description`; return `([], [], 0)` when it is `None`, else `(fetched, [column[0] for column in description], len(fetched))`. Otherwise the existing body.
- `resolve_rowcount(self, cursor: Any) -> int`: when `self._native_cursor`, `rowcount = cursor.rowcount` and return it when positive, else `0`. Otherwise the existing body.
- `dispatch_select_stream`: when `self._native_cursor`, compile with `self._compiled_sql(statement, effective_config)` and pass `params or None` to `_VertebraSyncRowSource`. Otherwise the existing `_compile_for_native` call.

Adapter `driver.py` files:

- Delete `SQLiteSyncDriver.dispatch_execute_many` and `DuckDBSyncDriver.dispatch_execute_many`.
- Keep every import the async classes in the same file still use.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| Driver whose `connection.config.dialect` is `sqlite` or `duckdb` | Native path |
| Connection object without `config`, or with another dialect | Existing path. `with_cursor` yields the connection itself |
| `INSERT ... RETURNING` | `description` is set, so the result is a row result. `statement.returns_rows()` is not consulted |
| `SELECT` with zero rows | `selected_data == []` and `column_names` come from `description` |
| Statement with no result set and no row count | `rowcount_override == 0` |
| Compiled parameters are `None`, `[]`, `()`, or `{}` | `cursor.execute(sql, ())` |
| `execute_many` with no parameter sets | No native call. `rowcount_override == 0` |
| DuckDB `execute_many` where a parameter set is a bare scalar | Passed through unchanged. The adapter no longer wraps it in a one-item tuple. Callers pass tuples |
| `connection.cursor()` raises inside `with_cursor` | The error leaves `__enter__` and the active exception handler maps it |
| Statement raises a `vertebra._vertebra` error | The engine `_map_exception` runs first, then `_map_native_database_exception`. The caller receives a `sqlspec.exceptions` type with the native error as `__cause__` |
| Next statement after a mapped error on a native driver | `handle_database_exceptions()` cleared `pending_exception`, so no stale error is raised |
| Two handler uses nested on one native driver | The inner use finishes and is checked before the outer one exits, so the shared object carries one pending error at a time |
| Exit of `_NativeSyncCursorContext` after an error | The cursor is closed. `close()` is idempotent per the cursor contract |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py`. They build a real in-memory `SyncSQLiteDriver` and the `SQLiteSyncDriver` adapter over it, and a real `SyncDuckDBDriver` with `DuckDBSyncDriver` where a row says so.

Test helper, defined in the test file: `_CountingSyncDriver` wraps a real native driver. It exposes `config`, counts calls to `cursor()`, returns a `_CountingSyncCursor` proxy around the real native cursor, and forwards every other attribute through `__getattr__` while recording the requested names. `_CountingSyncCursor` defines `execute`, `executemany`, `fetchall`, `fetchone`, `fetchmany`, `close`, and the properties `description`, `rowcount`, `lastrowid`, each forwarding to the real cursor and counting calls. The helper wraps the dependency under the adapter. It does not replace the adapter.

New tests:

1. `test_native_sync_cache_hit_performs_one_native_execute`: run `select ? as a` twice with different values through the adapter over `_CountingSyncDriver`. The second call adds exactly one `execute` and one `cursor()` call, and the recorded forwarded names for the second call contain none of `query_with_columns`, `execute_with_description`, `execute_tuples`, `select`, `execute`. Both results return the bound value under key `a`.
2. `test_native_sync_with_cursor_yields_closed_on_exit_native_cursor`: the object from `with driver.with_cursor(driver.connection)` is not the connection, and after the block a call to `fetchall()` on it raises `InterfaceError`. `vertebra.db.dbapi.InterfaceError` and `vertebra._vertebra.InterfaceError` are the same class.
3. `test_native_sync_dispatch_reads_description_rowcount_and_lastrowid`: an `INSERT` reports `rows_affected == 1`; `INSERT ... RETURNING id` returns the id as data; a `SELECT` with zero rows returns `[]` and keeps `column_names`.
4. `test_native_sync_execute_many_reports_engine_rowcount`, parametrized over SQLite and DuckDB: three parameter tuples report `rows_affected == 3`; an empty list reports `0` and adds no `executemany` call.
5. `test_native_sync_cursor_errors_map_to_sqlspec_exceptions`: a duplicate key raises `sqlspec.exceptions.UniqueViolationError` whose `__cause__` has the type `vertebra._vertebra.IntegrityError`; a statement on a missing table raises `SQLParsingError`.
6. `test_native_sync_driver_reuses_one_exception_handler`: `driver.handle_database_exceptions() is driver.handle_database_exceptions()` on the SQLite adapter; after a mapped failure a following `select 1` succeeds. A `BaseSyncDriver` over a connection double with no `config` returns two distinct handler objects.
7. `test_map_native_database_exception_matches_cursor_error_classes`, parametrized, with inputs built from the names in `vertebra.db.dbapi`: `IntegrityError("UNIQUE constraint failed")` maps to `UniqueViolationError`, `DataError` to `DataError`, `NotSupportedError` to `DialectNotSupportedError`, `OperationalError("database is locked")` to `OperationalError`, `ProgrammingError("syntax error")` to `SQLParsingError`, and `InterfaceError("closed")` to `OperationalError`. One more row passes `vertebra._vertebra.QueryCancelledError("stopped")` and expects `OperationCancelledError`. Outputs are `sqlspec.exceptions` types. If a row fails because a DB-API name is not the native class, set the task to `blocked`.

Existing tests to change:

- `test_base_driver_module_surface_slots_and_lazy_store_attr`: the expected `BaseSyncDriver.__slots__` tuple gains `_exception_handler` and `_native_cursor`.
- `test_base_drivers_inherit_template_dispatch_and_with_cursor_cache_contract`: its sync half keeps asserting that a connection double with no `config` is yielded unchanged by `with_cursor`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
```

Run test 2 first. If `SyncSQLiteDriver.cursor()` or `SyncDuckDBDriver.cursor()` does not return the native cursor, set the task to `blocked`.

## Acceptance Criteria

- [ ] `NATIVE_CURSOR_ENGINES` is read only in `BaseSyncDriver.__init__` within the sync half of `_driver.py`.
- [ ] On SQLite and DuckDB, `dispatch_execute`, `dispatch_execute_many`, `collect_rows`, and `resolve_rowcount` call none of `_compile_for_native`, `_unwrap_typed_parameters`, `_normalize_bound_params`, `_fetch_rows_with_columns`, or `statement.returns_rows()`.
- [ ] A native-cursor driver returns the same handler object from every `handle_database_exceptions()` call.
- [ ] `SQLiteSyncDriver` and `DuckDBSyncDriver` define no `dispatch_execute_many`.
- [ ] The seven new tests pass.
- [ ] `_map_native_database_exception` has no diff.
- [ ] The listed test files pass in full, including the MySQL, SQL Server, and stack cases on the existing path and the async SQLite and DuckDB cases.

## Notes & Discoveries

- Implementation of `_NativeSyncCursorContext`, `BaseSyncDriver` native-cursor paths, removal of sync `dispatch_execute_many` overrides in SQLite/DuckDB adapters, and all 7 new tests (15 parametrized cases) pass `pytest`, `ruff check`, and `mypy`.
- Blocked by two contract gaps outside `t05` owned files (562/572 tests in the verification suite pass; 10 fail):
  1. `SyncSQLiteCursor.fetchall()` (`src/rs/crates/vertebra-sqlite/`, outside `t05` scope) raises `vertebra._vertebra.ProgrammingError: no result set; call execute() with a row-returning statement first` when upstream `sqlspec`'s `SyncDriverAdapterBase._execute_cache_hit_with_cursor` calls `cursor.fetchall()` on a cached `PRAGMA` setter (`returns_rows=True`, `cursor.description is None`), breaking 8 SQLite Litestar/ADK sync tests. Python `sqlite3.Cursor.fetchall()` returns `[]` when `description is None`, allowing `BaseSyncDriver.collect_rows` to handle `description is None`.
  2. `SQLiteSyncDriver.statement_config` (`src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py`, `t04`, outside `t05` scope) compiles `is_many=True` `datetime` values into `TypedParameter(datetime(...), original_type=datetime)`. Because `t05` forbids `_unwrap_typed_parameters` in `BaseSyncDriver.dispatch_execute_many` on the native path, `SyncSQLiteCursor.executemany()` raises `vertebra._vertebra.InterfaceError: unsupported Python parameter type 'TypedParameter' for SQLite binding`, breaking 2 ChangeFeed sync `publish_many` tests.
