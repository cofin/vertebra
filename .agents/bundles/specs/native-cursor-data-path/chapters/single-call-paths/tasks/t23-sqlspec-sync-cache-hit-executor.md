---
type: Task
id: "native-cursor-data-path:single:t23"
chapter_id: single-call-paths
title: "SQLSpec sync cache-hit executor and execute_dml on the driver bases"
description: "Register a cache-hit executor on BaseSyncDriver that runs a cached statement with one driver call, and add execute_dml to SyncDatabaseDriver for engines without a native single-call method."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t06"
  - "native-cursor-data-path:single:t09"
  - "native-cursor-data-path:single:t13"
  - "native-cursor-data-path:single:t17"
read_set:
  - "src/py/vertebra/extensions/sqlspec/_driver.py:1-100"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:117-137"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:237-260"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:287-318"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:380-390"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:538-578"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:874-885"
  - "src/py/vertebra/db/driver/_sync.py:1-25"
  - "src/py/vertebra/db/driver/_sync.py:69-108"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:1-66"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:1-72"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:268-272"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:415-423"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:256-264"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:1-126"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:176-239"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:306-314"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:1879-1973"
files:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 290
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec sync adapter base"
  - id: driver-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: "Native sync driver base"
tags: [sqlspec, performance, database]
---

# t23 — SQLSpec sync cache-hit executor and execute_dml on the driver bases

## Objective & Scope Boundary

After this task, a cached statement run through a sync SQLSpec session on SQLite, DuckDB, PostgreSQL, MySQL, or SQL Server makes one driver call and builds its result once. Today it passes through ten interpreted calls in the adapter base.[^sqlspec-base]

In scope:

- `src/py/vertebra/db/driver/_sync.py`: `execute_dml` on `SyncDatabaseDriver`, with a type-only `cursor` declaration.[^driver-base]
- `src/py/vertebra/extensions/sqlspec/_driver.py`: the state type, `BaseSyncDriver.__init__`, and three new methods on `BaseSyncDriver`.
- `adapters/sqlite/driver.py` and `adapters/duckdb/driver.py`: one override on `SQLiteSyncDriver` and on `DuckDBSyncDriver`.
- Tests in `test_sqlspec_driver_dispatch.py`.

Out of scope:

- `BaseAsyncDriver`, `SQLiteAsyncDriver`, `DuckDBAsyncDriver`, and `src/py/vertebra/db/driver/_async.py` (t24).
- The Oracle, Spanner, and BigQuery adapters. They register their executors in t28, t29, and t30.
- `with_cursor`, `collect_rows`, `resolve_rowcount`, `handle_database_exceptions`, `dispatch_execute`, and `dispatch_execute_many`. They stay unchanged for every path upstream does not send through the executor.
- `_map_native_database_exception` and every adapter `_map_exception`.
- Every file under `src/py/vertebra/db/adapters/`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `_driver.py:1-100` | Imports and the `ExceptionMapper` alias |
| `_driver.py:117-137` | `_DeferredSyncExceptionHandler`, the mapping order the executor repeats |
| `_driver.py:237-260` | `_NativeSyncCursorContext`, the cursor calls the method path makes |
| `_driver.py:287-318` | `BaseSyncDriver` `__slots__` and `__init__` |
| `_driver.py:380-390` | Entry checks of `_map_native_database_exception` |
| `_driver.py:538-578` | `_map_exception`, `handle_database_exceptions`, `with_cursor`, `dispatch_execute` |
| `_driver.py:874-885` | `collect_rows`, `resolve_rowcount` |
| `db/driver/_sync.py:1-25`, `69-108` | Imports, `execute`, `execute_with_description` |
| `adapters/sqlite/driver.py:1-66` | Imports and `SQLiteSyncDriver` |
| `adapters/duckdb/driver.py:1-72` | Imports and `DuckDBSyncDriver` |
| `adapters/oracle/driver.py:268-272`, `adapters/spanner/driver.py:415-423`, `adapters/bigquery/driver.py:256-264` | The three `with_cursor` overrides, read only |
| `test_sqlspec_driver_dispatch.py:1-126`, `176-239`, `306-314`, `1879-1973` | Imports, the recording doubles, `_SampleSyncAdapter`, and the counting test that changes |

Upstream reference, read in the installed `sqlspec` package next to its compiled modules:

- `sqlspec/driver/_common.py`: `run_cached_execution`, `cache_hit_hooks_unchanged`, `_CACHE_HIT_HOOKS`, `_has_active_observability`, and the `_cache_hit_executor` and `_cache_hit_state` attributes in `CommonDriverAttributesMixin.__init__`.
- `sqlspec/driver/_sync.py`: `SyncDriverAdapterBase.execute`, `_execute_cache_hit`, and `_execute_cache_hit_with_cursor`.
- `execute_cache_hit` in `sqlspec/adapters/sqlite/core.py` and in `sqlspec/adapters/psycopg/core.py`.
- `sqlspec/core/result/_base.py`: `SQLResult.create_cached_select` and `DMLResult.__init__`.
- `__exit__` of `BaseSyncExceptionHandler` in `sqlspec/driver/_exception_handler.py`.

What the upstream source establishes:

- `run_cached_execution` is the only caller of the executor, and `SyncDriverAdapterBase.execute` is its only sync caller. It calls `executor(driver.connection, statement, params, cached, config, driver._cache_hit_state)` at two places.
- Both places first test `_has_active_observability(driver)` and send the statement to `_execute_cache_hit_observed` when it is true. Registering an executor does not skip hooks, observers, or spans.
- An executor that returns `None` declines, and the statement runs through `_execute_cache_hit`.
- No transaction check sits in front of the executor. `execute` calls `_ensure_transaction_for_cached_sql` before `run_cached_execution` when `_auto_begin_on_write` is set, so the executor also runs inside a transaction and on a pinned session.
- The `statement` argument is the caller's raw SQL. The SQL to run is `cached.compiled_sql`.
- The executor path has no `handle_database_exceptions` around it. The executor maps errors itself.
- `_CACHE_HIT_HOOKS` is `_execute_cache_hit`, `_execute_cache_hit_with_cursor`, `build_cached_select_result`, `collect_rows`, `handle_database_exceptions`, `resolve_rowcount`, `with_cursor`.
- `_cache_hit_executor` and `_cache_hit_state` are upstream `__slots__` entries. `BaseSyncDriver.__slots__` does not change.

## Symbol & Signature Contracts

### `src/py/vertebra/db/driver/_sync.py`

A type-only declaration of `cursor` and one method on `SyncDatabaseDriver`, placed directly after `execute`. Add `TYPE_CHECKING` to the `typing` import:

```python
if TYPE_CHECKING:

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Return a native cursor on this driver."""

def execute_dml(
    self,
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> tuple[int, Any]:
    """Run a statement without reading rows and return `(rowcount, lastrowid)`.

    Args:
        sql: Statement text in the engine's parameter style.
        parameters: Positional or named parameters.
        **kwargs: Ignored by the base implementation.

    Returns:
        The affected row count, `0` when the engine reports none, and the cursor's `lastrowid`.
    """
    _ = kwargs
    cur = self.cursor()
    try:
        cur.execute(sql, parameters or ())
        rowcount = cur.rowcount
        return (rowcount if rowcount > 0 else 0), cur.lastrowid
    finally:
        cur.close()
```

- Every engine driver already defines `cursor` with a compatible keyword-only signature. The declaration sits under `TYPE_CHECKING` so `execute_dml` type-checks and the base class gains no `cursor` attribute at run time. Code that tests `callable(getattr(driver, "cursor", None))` sees no change.
- An engine driver that has a native single-call method overrides `execute_dml`. `SyncSQLiteDriver` has one today; the DuckDB, PostgreSQL, and Oracle drivers gain theirs in the tasks this one depends on.
- `lastrowid` from the base implementation is whatever the engine's cursor reports:

| Engine | `lastrowid` |
| --- | --- |
| MySQL | The `AUTO_INCREMENT` id of the statement, `None` when it is `0` |
| SQL Server, Spanner, BigQuery | `None` |
| SQLite, DuckDB, PostgreSQL, Oracle | From the driver's own override, not from this method |

### `src/py/vertebra/extensions/sqlspec/_driver.py`

Imports:

- Add `DMLResult` to the existing `from sqlspec.core.result._base import ...` line.
- Add `cache_hit_hooks_unchanged` to the existing `from sqlspec.driver._common import ...` line.
- Add `from sqlspec.core.cache import CachedQuery` inside the `if TYPE_CHECKING:` block.

Type alias, placed directly after `ExceptionMapper`:

```python
type _CacheHitState = tuple[Callable[..., Any], Callable[..., Any], ExceptionMapper]
```

The three items are the row fetch, the statement run, and the adapter's error mapper. The result constructors need nothing else.

`BaseSyncDriver.__init__` adds, after the `_exception_handler` assignment:

```python
callables = self._cache_hit_callables()
if callables is not None:
    self._cache_hit_executor = BaseSyncDriver._run_cached_statement
    self._cache_hit_state = (callables[0], callables[1], self._map_exception)
```

New methods on `BaseSyncDriver`, placed directly after `_map_exception`:

```python
@staticmethod
def _single_call_pair(
    target: Any, fetch_name: str, dml_name: str
) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the two bound single-call methods of `target`, or `None` when either is missing."""
    fetch_rows = getattr(target, fetch_name, None)
    run_dml = getattr(target, dml_name, None)
    if not callable(fetch_rows) or not callable(run_dml):
        return None
    return fetch_rows, run_dml

def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the row fetch and statement run the cache-hit executor calls.

    Returns:
        The native driver's `execute_with_description` and `execute_dml`, or `None` to register no executor.
    """
    if not cache_hit_hooks_unchanged(type(self), BaseSyncDriver):
        return None
    return self._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")

@staticmethod
def _run_cached_statement(
    connection: Any,
    sql: str,
    params: Any,
    cached: CachedQuery,
    statement_config: StatementConfig,
    state: _CacheHitState,
) -> SQLResult:
    """Run a cached statement with one driver call and build its result.

    Args:
        connection: The native driver. Unused; the callables in `state` are bound to it.
        sql: Raw SQL text the caller passed.
        params: Parameters the caller passed, already valid for the compiled SQL.
        cached: Cached compilation of `sql`.
        statement_config: Statement configuration the result uses to rebuild its statement.
        state: Row fetch, statement run, and error mapper chosen at construction.

    Returns:
        The statement result.

    Raises:
        SQLSpecError: Mapped from a native error, with the native error as its cause.
    """
    _ = connection
    compiled_sql = cached.compiled_sql
    try:
        if cached.operation_profile.returns_rows:
            rows, names = state[0](compiled_sql, params or ())
            return SQLResult.create_cached_select(
                sql,
                statement_config,
                cached.processed_state,
                compiled_sql,
                params,
                rows,
                list(names),
                len(rows),
                "tuple",
                None,
            )
        rowcount, lastrowid = state[1](compiled_sql, params or ())
    except Exception as exc:
        if isinstance(exc, SQLSpecError):
            raise
        mapped = state[2](exc)
        if mapped is None:
            mapped = BaseSyncDriver._map_native_database_exception(exc)
        if mapped is None:
            raise
        raise mapped from exc
    operation_type = cached.operation_type
    if operation_type != "INSERT" or rowcount == 0:
        return DMLResult(operation_type, rowcount, None)
    return DMLResult(operation_type, rowcount, lastrowid)
```

What the executor reads and calls:

- From `cached`: `compiled_sql`, `operation_profile.returns_rows`, `processed_state`, and `operation_type`.
- One call: `state[0]` for a row-returning statement, `state[1]` for any other.
- `SQLResult.create_cached_select` takes its ten arguments in the upstream order: raw SQL, config, cached state, compiled SQL, parameters, data, column names, affected rows, row format, last inserted id.
- The row format is the literal `"tuple"`. Native rows are always tuples.
- `DMLResult(operation_type, rows_affected, last_inserted_id)`. `rows_affected` is the first item `execute_dml` returns, which is already `0` when the engine reports none. `last_inserted_id` is set only for an `INSERT` that affected a row, the rule the upstream SQLite executor uses.
- A row-returning statement reports `last_inserted_id=None`, as the method path does on a cache hit.
- The executor never returns `None`. Every decline is decided at construction, by `_cache_hit_callables` returning `None`.

### Which adapters register

The executor is registered when `_cache_hit_callables()` returns a pair at construction. Nothing is probed per statement.

| Condition | Result when false |
| --- | --- |
| `_cache_hit_callables()` returns a pair | No executor |

The base `_cache_hit_callables` returns `None` when the class overrides a name in `_CACHE_HIT_HOOKS`, or when the connection object lacks one of the two methods.

| Adapter | Registered | Callables |
| --- | --- | --- |
| `SQLiteSyncDriver`, `DuckDBSyncDriver` | Yes | The native client's `fetch_rows` and `execute_dml` |
| `PostgresSyncDriver` | Yes | The native driver's `execute_with_description` and `execute_dml`, which route to the pinned session or the pool |
| `MySQLSyncDriver`, `MSSQLSyncDriver` | Yes | The native driver's `execute_with_description` and the base `execute_dml` |
| `OracleSyncDriver`, `SpannerSyncDriver`, `BigQuerySyncDriver` | Not in this task | Each overrides `with_cursor`, so the base method returns `None`. Each gains its own `_cache_hit_callables` in t28, t29, and t30 |

Until those tasks land, a cached statement on the three adapters runs as it does today.

### SQLite and DuckDB adapter overrides

One method on `SQLiteSyncDriver` and the same method on `DuckDBSyncDriver`, placed directly before `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the client-bound callables the native driver holds, or `None` to register no executor."""
    if not cache_hit_hooks_unchanged(type(self), SQLiteSyncDriver):
        return None
    return self._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")
```

- The DuckDB method tests against `DuckDBSyncDriver`.
- Each `_cache_hit_callables` tests the hook names against its own class. An adapter that overrides a hook on purpose supplies its own callables and its own test; the Oracle, Spanner, and BigQuery adapters do that in t28, t29, and t30.
- Add `from collections.abc import Callable` to each file when it is not imported, and `from sqlspec.driver._common import cache_hit_hooks_unchanged`.
- `_fetch_rows` and `_run_dml` are the attributes the SQLite and DuckDB native drivers bind to the client's `fetch_rows` and `execute_dml` when the client is set. They come from the driver tasks this one depends on.
- The two attributes are read once, at construction. The executor calls the client's bound methods with no driver frame in between.
- A native driver that is already closed holds its closed-driver function in both attributes. The executor is registered, and every cached statement raises the driver's `InterfaceError`.
- `fetch_rows(sql, params)` returns `(rows, names)` and `execute_dml(sql, params)` returns `(rowcount, lastrowid)`, the signatures the native driver methods have.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| First execution of a statement | Cache miss. `run_cached_execution` returns `None` and the statement runs through `dispatch_execute` |
| Hooks, observers, or spans are active | Upstream sends the cached statement to `_execute_cache_hit_observed`. The executor is not called |
| Parameters need rebinding, the count differs from the cached count, or the config has an output transformer | Upstream does not reach the executor. Unchanged |
| A statement inside `begin()` ... `commit()` | The executor runs. The PostgreSQL driver methods use the pinned session. SQLite, DuckDB, MySQL, and SQL Server hold one client, so the transaction is the client's |
| `SELECT` with zero rows | `data == []`, column names kept, `rows_affected == 0` |
| A row-returning statement the engine answers with no result set | `fetch_rows` returns `([], ())`. The result has no rows and no column names |
| `INSERT ... RETURNING` | Row result. `rows_affected` is the number of rows returned |
| `UPDATE` that matches no row | `DMLResult` with `rows_affected == 0` and `last_inserted_id is None` |
| `params` is `()`, `[]`, or `{}` | The call receives `()` |
| A native error from the call | The adapter's `_map_exception` runs first, then `_map_native_database_exception`. The mapped SQLSpec error is raised with the native error as `__cause__` |
| An error neither mapper recognizes | Raised unchanged |
| A `SQLSpecError` from the call | Raised unchanged |
| The native driver is closed, PostgreSQL, MySQL, SQL Server | The driver method raises its closed-driver error, which is mapped like any native error |
| The native driver is closed after the adapter was built, SQLite and DuckDB | The adapter still holds the closed client's methods. They raise `InterfaceError`, the class `driver.cursor()` raises on the method path, so both paths map to the same SQLSpec class |
| A SQLite or DuckDB native driver closed and then reconnected under a live adapter | The adapter stays bound to the client it was built with and its cached statements fail. `SyncAdapterConfig` builds a new adapter for each session, which binds the new client |
| An adapter built over an object with no `execute_dml` | No executor. Method path |
| A SQLite or DuckDB adapter built over an object with no `_fetch_rows` or `_run_dml` | No executor. Method path |
| A subclass that overrides one of the names in `_CACHE_HIT_HOOKS` | No executor. Method path |
| A driver class that defines no `cursor` | The base `execute_dml` raises `AttributeError` |

Why both paths raise the same class. On the method path `_DeferredSyncExceptionHandler._handle_exception` calls the adapter's mapper, then `BaseSyncDriver._map_native_database_exception`, and returns without handling when both give `None`. Upstream then sets `__cause__` to the native error and raises the mapped error. The executor runs the same two mappers in the same order on the same native error and raises with `from exc`.

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py`.

Test doubles, defined in the test file after `_RecordingSyncNativeDriver`:

- `_SingleCallSyncCursor(_RecordingSyncCursor)`: `execute` raises the driver's `fail_with` when set, calls the parent, then copies the driver's `lastrowid` and, for a statement without rows, its `rowcount`.
- `_SingleCallSyncClient`: holds the driver double. `fetch_rows(sql, params=())` appends `("fetch_rows", sql, params)` to the driver's `calls`, raises `fail_with` when set, and returns `(tuple_rows, tuple(columns))`. `execute_dml(sql, params=())` appends `("client_execute_dml", sql, params)`, raises `fail_with` when set, and returns `(rowcount, lastrowid)`.
- `_SingleCallSyncNative(_RecordingSyncNativeDriver)`: constructor arguments `rows`, `columns`, and keyword-only `rowcount: int = 1`, `lastrowid: int | None = 7`, `with_client: bool = False`, `fail_with: BaseException | None = None`. When `with_client` is true, the double holds a `_SingleCallSyncClient` and sets `_fetch_rows` and `_run_dml` to that client's `fetch_rows` and `execute_dml`. Otherwise it has neither attribute. `execute_with_description` raises `fail_with` when set, else calls the parent. `execute_dml` appends `("execute_dml", sql, parameters)`, raises `fail_with` when set, and returns `(rowcount, lastrowid)`. `cursor` appends `("cursor", "", None)` and returns a `_SingleCallSyncCursor`.

Module-level parameter table `_CACHE_HIT_SYNC_ADAPTERS`, one `pytest.param` per engine with `id` equal to the engine name: `(adapter class, registered, with_client, fetch call name, dml call name)`.

| Engine | Adapter | Registered | `with_client` | Fetch call | DML call |
| --- | --- | --- | --- | --- | --- |
| sqlite | `SQLiteSyncDriver` | yes | yes | `fetch_rows` | `client_execute_dml` |
| duckdb | `DuckDBSyncDriver` | yes | yes | `fetch_rows` | `client_execute_dml` |
| postgres | `PostgresSyncDriver` | yes | no | `execute_with_description` | `execute_dml` |
| mysql | `MySQLSyncDriver` | yes | no | `execute_with_description` | `execute_dml` |
| mssql | `MSSQLSyncDriver` | yes | no | `execute_with_description` | `execute_dml` |
| oracle | `OracleSyncDriver` | no | no | | |
| spanner | `SpannerSyncDriver` | no | no | | |
| bigquery | `BigQuerySyncDriver` | no | no | | |

New tests:

1. `test_sync_cache_hit_executor_registration_follows_hook_overrides`, parametrized over the eight rows: `cache_hit_hooks_unchanged(adapter_cls, BaseSyncDriver)` equals the `registered` column. For a registered row, an adapter built over `_SingleCallSyncNative(with_client=...)` has `_cache_hit_executor is BaseSyncDriver._run_cached_statement` and a three-item `_cache_hit_state`.
2. `test_sync_cache_hit_makes_one_driver_call`, parametrized over the five registered rows and over `("SELECT 1 AS a", fetch)` and `("UPDATE t SET a = 2", dml)`: run the statement once, clear `calls`, run it again. `calls` holds exactly one entry, its name is the row's fetch or DML call name, and its SQL is the compiled statement. No `cursor` entry is present.
3. `test_sync_cache_miss_runs_through_cursor`, parametrized over the five registered rows: the first execution of `SELECT 1 AS a` records a `cursor` entry and an `execute` entry, and returns the double's rows.
4. `test_sync_cache_hit_result_matches_method_path`, parametrized over the five registered rows and over five statement cases. It builds two adapters over two doubles with the same data, sets `_cache_hit_executor = None` on the second, and runs the statement twice on each. For the second run of each it compares `all()`, `column_names`, and `rows_affected`.

   | Case | Double | Extra assertion on the executor result |
   | --- | --- | --- |
   | select | `rows=[{"a": 1}]`, `columns=["a"]`, `SELECT 1 AS a` | `all() == [{"a": 1}]` |
   | empty select | `rows=[]`, `columns=["a"]`, `SELECT 1 AS a` | `all() == []`, `column_names == ["a"]` |
   | insert | `INSERT INTO t (a) VALUES (1)`, `rowcount=1`, `lastrowid=7` | `rows_affected == 1`, `last_inserted_id == 7` |
   | update | `UPDATE t SET a = 2`, `rowcount=3`, `lastrowid=7` | `rows_affected == 3`, `last_inserted_id is None` |
   | update, no match | `UPDATE t SET a = 2`, `rowcount=0` | `rows_affected == 0` |

5. `test_sync_cache_hit_returning_clause_builds_row_result`, parametrized over sqlite, duckdb, and postgres: `INSERT INTO t (a) VALUES (1) RETURNING a` with `rows=[{"a": 1}]`. The second run makes the row's fetch call, returns `[{"a": 1}]`, and equals the method-path adapter's second run.
6. `test_sync_cache_hit_errors_match_method_path`, parametrized over the five registered rows and over the native errors below. Run `SELECT 1 AS a` once, set `fail_with` on the double, and run it again on an executor adapter and on a method-path adapter. Both raise the same exception class, and the executor error's `__cause__` is the native error.

   | `fail_with` | Class on `_SampleSyncAdapter` |
   | --- | --- |
   | `NativeIntegrityError("UNIQUE constraint failed: t.id")` | `UniqueViolationError` |
   | `ProgrammingError("syntax error")` | `SQLParsingError` |
   | `NativeOperationalError("database is locked")` | `OperationalError` |
   | `InterfaceError("closed")` | `OperationalError` |
   | `QueryCancelledError("stopped")` | `OperationCancelledError` |

   A sixth row passes `ValueError("boom")` and expects `ValueError` on both paths with no mapping. A separate test `test_sync_cache_hit_error_classes_on_base_adapter`, parametrized over the five mapped rows, asserts the class in the second column for `_SampleSyncAdapter`.
7. `test_sync_cache_hit_with_active_observability_uses_observed_path`: build `_SampleSyncAdapter` with `observability=ObservabilityRuntime(ObservabilityConfig(statement_observers=(events.append,)))`, both names imported from `sqlspec.observability`. Run `SELECT 1 AS a` twice. The second run records a `cursor` entry and no `execute_with_description` entry, and `events` holds two items.
8. `test_sync_cache_hit_on_real_embedded_engines`, parametrized over an in-memory `SyncSQLiteDriver` with `SQLiteSyncDriver` and an in-memory `SyncDuckDBDriver` with `DuckDBSyncDriver`: create a table, run the same `INSERT` twice and the same `SELECT` twice. The second insert reports `rows_affected == 1`; on SQLite its `last_inserted_id` is `2`. The second select returns both rows. A repeated insert of a duplicate key raises `UniqueViolationError` whose `__cause__` has the type `vertebra._vertebra.IntegrityError`. After `native.close()`, a cached select on this adapter and on a second adapter with `_cache_hit_executor = None` raise the same class.
9. `test_sync_base_execute_dml_runs_one_cursor_statement`: `_RecordingSyncNativeDriver().execute_dml("UPDATE t SET a = 2")` returns `(1, None)` and records one `execute` entry with `()` as parameters. A second case uses `_SingleCallSyncNative`-style capture of the cursor returned by `cursor()` and asserts its `closed` flag is true after the call.

Existing test to change:

- `test_native_sync_cache_hit_performs_one_native_execute`: rename to `test_native_sync_cache_hit_opens_no_cursor`. The second run adds no `cursor()` call and no cursor `execute`, returns `[{"a": 25}]`, and `counting.forwarded_names` for the second run is empty. `_fetch_rows` and `_run_dml` are read once, at construction.

Stop rules:

- Each test in items 2 to 6 first asserts `adapter._stmt_cache_enabled`. If the second run of a parameterless statement does not reach the executor for one of the five registered adapters, set the task to `blocked` and record the adapter and the `CachedQuery` flags.
- If a closed SQLite or DuckDB client raises a class other than `InterfaceError`, set the task to `blocked`.

## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_sync.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync --frozen mypy src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_sync.py src/py/vertebra/db/adapters src/py/vertebra/extensions/sqlspec/adapters
uv run --no-sync --frozen pyright src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_sync.py
```

Run the second command before the change and again after it. A test that passes before and fails after blocks the task; record its name and do not edit those three files. The `mypy` run covers every engine driver, because each one defines the `cursor` the base now declares.

## Acceptance Criteria

- [x] `SyncDatabaseDriver` has `execute_dml` with the body above, and `cursor` only as a type-only declaration.
- [x] `BaseSyncDriver.__init__` sets `_cache_hit_executor` and `_cache_hit_state` for SQLite, DuckDB, PostgreSQL, MySQL, and SQL Server, and leaves both `None` for Oracle, Spanner, and BigQuery until t28, t29, and t30.
- [x] `_run_cached_statement` is a `@staticmethod` with the upstream six-argument signature and makes exactly one call per statement.
- [x] No statement path reads an engine name, probes the driver with `getattr`, or builds an exception handler.
- [x] A native error raises the same SQLSpec class on the executor path and the method path, with the native error as `__cause__`.
- [x] `with_cursor`, `collect_rows`, `resolve_rowcount`, `handle_database_exceptions`, and `_map_native_database_exception` have no diff.
- [x] `BaseSyncDriver.__slots__` has no diff.
- [x] The new tests pass, the changed test passes, and the three other extension test files pass as they did before.

## Notes & Discoveries

- The upstream method path on a cache hit builds `DMLResult(operation_type, rows_affected)` with no `last_inserted_id`. The executor reports the engine's id for an `INSERT` that affected a row, as the upstream SQLite executor does. The equality test therefore compares rows, column names, and `rows_affected` between the two paths, and asserts `last_inserted_id` on the executor result alone.
- Both `vertebra-sqlite` (`ensure_open`) and `vertebra-duckdb` (`ensure_open`) return `InvalidInput` (raising `vertebra._vertebra.InterfaceError`) when closed, matching `SyncSQLiteDriver.cursor()` and `SyncDuckDBDriver.cursor()` so closed-client errors map identically on the cache-hit executor and method paths.
- `SQLiteSyncDriver` and `DuckDBSyncDriver` bind `_fetch_rows` and `_run_dml` once at construction; closing and reconnecting a native driver under an existing adapter instance leaves cached statements bound to the closed client until a new adapter is constructed per session.

[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver`
[^driver-base]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver`
