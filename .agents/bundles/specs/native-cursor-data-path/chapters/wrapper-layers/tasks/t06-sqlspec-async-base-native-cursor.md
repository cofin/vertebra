---
type: Task
id: "native-cursor-data-path:wrapper:t06"
chapter_id: wrapper-layers
title: "SQLSpec async base on the native cursor"
description: "Give BaseAsyncDriver the native cursor path with awaitable calls and drop the async dispatch_execute_many overrides in the SQLite and DuckDB adapters."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:wrapper:t05"
read_set:
  - "src/py/vertebra/extensions/sqlspec/_driver.py:139-158"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:916-1063"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:1336-1349"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:95-158"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:156-276"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:237-250"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:740-838"
files:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 230
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-async-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver
    title: "SQLSpec async adapter base"
tags: [sqlspec, sqlite, duckdb, async]
---

# t06 — SQLSpec async base on the native cursor

## Objective & Scope Boundary

After this task, an async SQLSpec session on SQLite or DuckDB runs each statement as one awaited `cursor.execute` on a native async cursor, with the same result rules as the sync base from t05.

In scope:

- In `src/py/vertebra/extensions/sqlspec/_driver.py`: `BaseAsyncDriver.__slots__`, `__init__`, `with_cursor`, `handle_database_exceptions`, `dispatch_execute`, `dispatch_execute_many`, `dispatch_select_stream`, `collect_rows`, `resolve_rowcount`; a new private class `_NativeAsyncCursorContext`.
- In the SQLite and DuckDB adapter `driver.py` files: delete `dispatch_execute_many` from `SQLiteAsyncDriver` and `DuckDBAsyncDriver`, and remove the imports that only those overrides and the ones deleted in t05 used.

Out of scope:

- `_map_native_database_exception`. It already tests the native classes the cursor raises, and the async handler calls the same function.
- `BaseAsyncDriver._unwrap_typed_parameters`, `_normalize_bound_params`, `_prepare_native_parameters`, `_compile_for_native`, and `_fetch_rows_with_columns`. They stay for the six network adapters.
- The `_prepare_native_parameters` overrides and `core.py` normalizers (t07, t08).

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlspec/_driver.py:139-158` | `_DeferredAsyncExceptionHandler` |
| `src/py/vertebra/extensions/sqlspec/_driver.py`, class `BaseSyncDriver` and the sync cursor context class t05 adds before it | The sync forms from t05 to mirror |
| `src/py/vertebra/extensions/sqlspec/_driver.py:916-1063` | `BaseAsyncDriver` through `dispatch_select_stream` (line numbers shift after t05; locate by symbol) |
| `src/py/vertebra/extensions/sqlspec/_driver.py:1336-1349` | Async `collect_rows`, `resolve_rowcount` |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:95-158` | `SQLiteAsyncDriver` |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:156-276` | `DuckDBAsyncDriver` |
| `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:237-250`, `740-838` | Async base test and `__slots__` surface test |

Upstream reference, read in the installed `sqlspec` package: `AsyncDriverAdapterBase._execute_cache_hit_with_cursor` in `sqlspec/driver/_async.py`. It enters `with_cursor` with `async with` and awaits `execute` and `fetchall`.

## Symbol & Signature Contracts

Imports in `_driver.py`: drop `asynccontextmanager` from the `contextlib` import. `nullcontext` is already imported and works with `async with`.

New module-level class, placed after `_NativeSyncCursorContext`:

```python
class _NativeAsyncCursorContext:
    """Async context manager opening one native async cursor on a driver and closing it on exit."""

    __slots__ = ("_cursor", "_driver")

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self._cursor: Any = None

    async def __aenter__(self) -> Any:
        self._cursor = self._driver.cursor()
        return self._cursor

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        cursor = self._cursor
        if cursor is not None:
            self._cursor = None
            await cursor.close()
```

`BaseAsyncDriver`:

- `__slots__ = ("_driver_closed", "_exception_handler", "_native_cursor", "_shared_pool", "native_driver")`.
- `__init__` adds the same two assignments as `BaseSyncDriver`, with `_DeferredAsyncExceptionHandler`.
- `handle_database_exceptions(self) -> _DeferredAsyncExceptionHandler`: when `self._native_cursor`, clear `pending_exception` on the stored handler and return it. Otherwise build a new handler as today.
- `with_cursor(self, connection: Any) -> AbstractAsyncContextManager[Any]`: a plain method with no decorator. Returns `_NativeAsyncCursorContext(connection)` when `self._native_cursor`, else `nullcontext(connection)`.
- `async dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult`: when `self._native_cursor`, the body from t05 with `await cursor.execute(sql, params or ())` and `rows = await cursor.fetchall()`. `description`, `rowcount`, and `lastrowid` are plain attribute reads. Otherwise the existing body.
- `async dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult`: when `self._native_cursor`, the body from t05 with `await cursor.executemany(sql, prepared)`. Otherwise the existing body.
- `collect_rows` and `resolve_rowcount`: the same native branches as t05. Both stay plain methods.
- `dispatch_select_stream`: when `self._native_cursor`, compile with `self._compiled_sql(statement, effective_config)` and pass `params or None` to `_VertebraAsyncRowSource`. Otherwise the existing call.

Adapter `driver.py` files:

- Delete `SQLiteAsyncDriver.dispatch_execute_many` and `DuckDBAsyncDriver.dispatch_execute_many`.
- In `sqlite/driver.py`, remove the imports of `unwrap_typed_parameters`, `normalize_sqlite_many_parameters`, and `ExecutionResult`. `normalize_sqlite_parameters` and `SQL` stay imported for the `_prepare_native_parameters` overrides.
- In `duckdb/driver.py`, remove the import of `normalize_duckdb_many_parameters`, and `ExecutionResult` when no use remains. `normalize_duckdb_parameters` stays imported.

## Edge Cases & Error Semantics

The table in t05 applies with awaited calls. Additional rows:

| Case | Behavior |
| --- | --- |
| `async with driver.with_cursor(conn)` on an engine outside the set | `nullcontext(conn)` yields the connection itself, as the generator did |
| Task cancelled while `cursor.execute` is awaited | The cancellation leaves the `async with`. `__aexit__` awaits `cursor.close()` |
| Two coroutines use one native-cursor driver at the same time | Not supported, as today. The shared handler object assumes one statement at a time per driver |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py`. They build a real in-memory `AsyncSQLiteDriver` with `SQLiteAsyncDriver`, and `AsyncDuckDBDriver` with `DuckDBAsyncDriver` where a row says so.

Test helper: `_CountingAsyncDriver` and `_CountingAsyncCursor`, the async forms of the helpers from t05. Cursor methods are `async def` and await the real cursor.

New tests:

1. `test_native_async_cache_hit_performs_one_native_execute`: the async form of t05 test 1.
2. `test_native_async_with_cursor_yields_closed_on_exit_native_cursor`: the object from `async with driver.with_cursor(driver.connection)` is not the connection, and after the block `await cursor.fetchall()` raises `InterfaceError`.
3. `test_native_async_dispatch_reads_description_rowcount_and_lastrowid`: the async form of t05 test 3.
4. `test_native_async_execute_many_reports_engine_rowcount`, parametrized over SQLite and DuckDB: the async form of t05 test 4.
5. `test_native_async_cursor_errors_map_to_sqlspec_exceptions`: the async form of t05 test 5.
6. `test_native_async_driver_reuses_one_exception_handler`: the async form of t05 test 6.

Existing tests to change:

- `test_base_driver_module_surface_slots_and_lazy_store_attr`: the expected `BaseAsyncDriver.__slots__` tuple gains `_exception_handler` and `_native_cursor`.
- `test_base_async_driver_preserves_zero_row_columns_and_cursor_contract`: keeps asserting that a connection double with no `config` is yielded unchanged by `with_cursor`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
```

Run test 2 first. If `AsyncSQLiteDriver.cursor()` or `AsyncDuckDBDriver.cursor()` does not return the native async cursor, set the task to `blocked`.

## Acceptance Criteria

- [x] `NATIVE_CURSOR_ENGINES` is read in `_driver.py` only in `BaseSyncDriver.__init__` and `BaseAsyncDriver.__init__`.
- [x] `_driver.py` imports neither `contextmanager` nor `asynccontextmanager`.
- [x] `SQLiteAsyncDriver` and `DuckDBAsyncDriver` define no `dispatch_execute_many`.
- [x] The six new tests pass.
- [x] The listed test files pass in full.

## Notes & Discoveries

- Updated `test_sqlite_driver_imports_unwrap_typed_parameters_from_sqlite_core` (renamed to `test_sqlite_driver_imports_helpers_from_sqlite_core_not_driver_base`) in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py` because deleting `SQLiteAsyncDriver.dispatch_execute_many` removed the last import of `unwrap_typed_parameters`, `normalize_sqlite_many_parameters`, and `ExecutionResult` from `sqlite/driver.py`.
- Full repo `make lint` was not run because the sandbox has a read-only `~/.cache/uv` and other toolchains (Rust/TS) were untouched; targeted Python checks (`ruff check`, `ruff format --check`, `mypy`, `pyright`) all passed with 0 errors on all touched files.
