---
type: Task
id: "native-cursor-data-path:single:t24"
chapter_id: single-call-paths
title: "SQLSpec async cache-hit executor"
description: "Register an async cache-hit executor on BaseAsyncDriver that awaits one awaitable per cached statement, and add execute_dml to AsyncDatabaseDriver for engines without a native single-call method."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t23"
  - "native-cursor-data-path:single:t14"
  - "native-cursor-data-path:single:t18"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t23-sqlspec-sync-cache-hit-executor.md"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver._run_cached_statement"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver._single_call_pair"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::_DeferredAsyncExceptionHandler"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver.__init__"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver.dispatch_execute"
  - "src/py/vertebra/db/driver/_async.py:1-33"
  - "src/py/vertebra/db/driver/_async.py:77-116"
  - "src/py/vertebra/db/driver/_async.py:239-292"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py::SQLiteAsyncDriver"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py::DuckDBAsyncDriver"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py::_RecordingAsyncCursor"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py::_RecordingAsyncNativeDriver"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py::test_native_async_cache_hit_performs_one_native_execute"
files:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/driver/_async.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 280
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: sqlspec-async-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver
    title: "SQLSpec async adapter base"
  - id: async-driver-base
    resource: src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver
    title: "Native async driver base"
tags: [sqlspec, performance, database]
---

# t24 — SQLSpec async cache-hit executor

## Objective & Scope Boundary

After this task, a cached statement run through an async SQLSpec session on SQLite, DuckDB, PostgreSQL, MySQL, or SQL Server awaits one awaitable and builds its result once.[^sqlspec-async-base]

This task is the async form of t23. It shares `_driver.py`, the two adapter files, and the test file with t23, which is why it runs after it.

In scope:

- `src/py/vertebra/db/driver/_async.py`: `execute_dml` on `AsyncDatabaseDriver` with a type-only `cursor` declaration, and `execute_dml` on `ThreadOffloadAsyncDatabaseDriver`.[^async-driver-base]
- `src/py/vertebra/extensions/sqlspec/_driver.py`: `BaseAsyncDriver.__init__` and two new methods on `BaseAsyncDriver`.
- `adapters/sqlite/driver.py` and `adapters/duckdb/driver.py`: one override on `SQLiteAsyncDriver` and on `DuckDBAsyncDriver`.
- Tests in `test_sqlspec_driver_dispatch.py`.

Out of scope:

- Everything t23 delivers: the `_CacheHitState` alias, `BaseSyncDriver._single_call_pair`, the sync executor, and the sync tests. They are used, not edited.
- The Oracle, Spanner, and BigQuery async adapters. They register their executors in t28, t29, and t30.
- The async hook methods `with_cursor`, `collect_rows`, `resolve_rowcount`, `handle_database_exceptions`, and `dispatch_execute`.
- Every file under `src/py/vertebra/db/adapters/`.

## Bounded Read Manifest

Line numbers in `_driver.py`, the adapter files, and the test file move when t23 lands, so those entries are located by symbol.

| Entry | Why |
| --- | --- |
| The t23 worksheet | The executor contract this task mirrors, including what the upstream source establishes |
| `_driver.py`, `BaseSyncDriver._run_cached_statement` and `_single_call_pair` | The sync executor body and the helper this task reuses |
| `_driver.py`, `_DeferredAsyncExceptionHandler` | The mapping order on the async method path |
| `_driver.py`, `BaseAsyncDriver.__init__` and `dispatch_execute` | Where the registration goes, and the awaits the method path makes |
| `db/driver/_async.py:1-33`, `77-116` | Imports, `execute`, `execute_with_description` |
| `db/driver/_async.py:239-292` | `ThreadOffloadAsyncDatabaseDriver` and its `execute` |
| Adapter files, `SQLiteAsyncDriver` and `DuckDBAsyncDriver` | Where the override goes |
| Test file, `_RecordingAsyncCursor`, `_RecordingAsyncNativeDriver`, and the async counting test | Doubles to extend and the test that changes |

Upstream reference, read in the installed `sqlspec` package: `AsyncDriverAdapterBase.execute` in `sqlspec/driver/_async.py`, and `execute_cache_hit_async` in `sqlspec/adapters/psycopg/core.py`.

What the upstream source establishes for the async form:

- `AsyncDriverAdapterBase.execute` is the only async caller of `run_cached_execution`. It returns the outcome when it is an `SQLResult`, and otherwise awaits it.
- When the awaited outcome is `None`, it runs `_execute_cache_hit`. An async executor can therefore decline after it is awaited.
- The observability test in `run_cached_execution` runs before the executor is called, as in the sync form.

## Symbol & Signature Contracts

### `src/py/vertebra/db/driver/_async.py`

A type-only declaration of `cursor` and one method on `AsyncDatabaseDriver`, placed directly after `execute`. Add `TYPE_CHECKING` to the `typing` import:

```python
if TYPE_CHECKING:

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Return a native async cursor on this driver."""

async def execute_dml(
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
        await cur.execute(sql, parameters or ())
        rowcount = cur.rowcount
        return (rowcount if rowcount > 0 else 0), cur.lastrowid
    finally:
        await cur.close()
```

One method on `ThreadOffloadAsyncDatabaseDriver`, placed directly after its `execute`:

```python
async def execute_dml(
    self,
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> tuple[int, Any]:
    """Run a statement via the paired synchronous driver and return `(rowcount, lastrowid)`."""
    return await asyncio.to_thread(self._sync.execute_dml, sql, parameters, **kwargs)
```

- `cursor` is a plain method, not a coroutine. Every async engine driver already defines it that way. The declaration is type-only, so `ThreadOffloadAsyncDatabaseDriver` still has no `cursor` attribute at run time and the Spanner async adapter's `callable(cursor_fn)` test keeps its result.
- `cur.close()` returns a completed awaitable after t19 to t22 and the tasks that cover the other four engines, so the base `execute_dml` re-enters the event loop once, for `execute`.
- `lastrowid` per engine is the table in the t23 worksheet.

### `src/py/vertebra/extensions/sqlspec/_driver.py`

`BaseAsyncDriver.__init__` adds, after the `_exception_handler` assignment:

```python
callables = self._cache_hit_callables()
if callables is not None:
    self._cache_hit_executor = BaseAsyncDriver._run_cached_statement
    self._cache_hit_state = (callables[0], callables[1], self._map_exception)
```

New methods on `BaseAsyncDriver`, placed directly after `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the row fetch and statement run the cache-hit executor awaits.

    Returns:
        The native driver's `execute_with_description` and `execute_dml`, or `None` to register no executor.
    """
    if not cache_hit_hooks_unchanged(type(self), BaseAsyncDriver):
        return None
    return BaseSyncDriver._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")

@staticmethod
async def _run_cached_statement(
    connection: Any,
    sql: str,
    params: Any,
    cached: CachedQuery,
    statement_config: StatementConfig,
    state: _CacheHitState,
) -> SQLResult:
    """Run a cached statement by awaiting one driver call and build its result.

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
            rows, names = await state[0](compiled_sql, params or ())
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
        rowcount, lastrowid = await state[1](compiled_sql, params or ())
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

- The body has exactly two `await` expressions, and one statement reaches exactly one of them.
- The constructor calls, the row format, and the `last_inserted_id` rule are the ones in the t23 worksheet.
- The executor never resolves to `None`. Every decline is decided at construction, by `_cache_hit_callables` returning `None`.

### Which adapters register

The rule is the one in the t23 worksheet, with the hook names tested against `BaseAsyncDriver`.

| Adapter | Registered | Callables |
| --- | --- | --- |
| `SQLiteAsyncDriver`, `DuckDBAsyncDriver` | Yes | The native async client's `fetch_rows` and `execute_dml`, each returning one awaitable |
| `PostgresAsyncDriver` | Yes | The native driver's `execute_with_description` and `execute_dml`, which route to the pinned session or the pool |
| `MySQLAsyncDriver`, `MSSQLAsyncDriver` | Yes | The native driver's `execute_with_description` and the base `execute_dml` |
| `OracleAsyncDriver`, `SpannerAsyncDriver`, `BigQueryAsyncDriver` | Not in this task | Each overrides `with_cursor`. Each gains its own `_cache_hit_callables` in t28, t29, and t30 |

### SQLite and DuckDB adapter overrides

One method on `SQLiteAsyncDriver` and the same method on `DuckDBAsyncDriver`, placed directly before `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the client-bound callables the native async driver holds, or `None` to register no executor."""
    if not cache_hit_hooks_unchanged(type(self), SQLiteAsyncDriver):
        return None
    return BaseSyncDriver._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")
```

The DuckDB method tests against `DuckDBAsyncDriver`. Import `BaseSyncDriver` from `vertebra.extensions.sqlspec._driver` in each file when it is not already imported there.

## Edge Cases & Error Semantics

The table in the t23 worksheet applies with these async differences:

| Case | Behavior |
| --- | --- |
| A cached statement on a registered adapter | Upstream awaits the executor coroutine, and the coroutine awaits one driver awaitable |
| Task cancelled while the driver awaitable is pending | `CancelledError` is a `BaseException`. It passes through the `except Exception` clause unmapped |
| A native error raised when the awaitable is awaited | Mapped by the adapter's `_map_exception`, then `_map_native_database_exception`, and raised with the native error as `__cause__` |
| A native error raised by the call before any awaitable exists | Mapped the same way. The call sits inside the same `try` |
| The native driver is closed, SQLite and DuckDB | The client's method raises `InterfaceError`, at the call or on `await`. Both are mapped |
| MySQL and SQL Server `execute_dml` | The base method awaits `cursor.execute` and then a completed `cursor.close()`. The executor still awaits one awaitable |
| `ThreadOffloadAsyncDatabaseDriver` under an adapter | Its `execute_dml` runs the sync driver's method on a worker thread |

Why both paths raise the same class. `_DeferredAsyncExceptionHandler._handle_exception` calls the adapter's mapper and then `BaseSyncDriver._map_native_database_exception`, the same two mappers in the same order as the executor.

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py` and carry `@pytest.mark.anyio`.

Test doubles, defined after `_RecordingAsyncNativeDriver` and mirroring the sync doubles from t23:

- `_SingleCallAsyncCursor(_RecordingAsyncCursor)`: `execute` raises the driver's `fail_with` when set, awaits the parent, then copies the driver's `lastrowid` and, for a statement without rows, its `rowcount`.
- `_SingleCallAsyncNative` with `with_client` true sets `_fetch_rows` and `_run_dml` to its client's methods, as the sync double does.
- `_SingleCallAsyncClient`: `async def fetch_rows(sql, params=())` and `async def execute_dml(sql, params=())`, recording `("fetch_rows", sql, params)` and `("client_execute_dml", sql, params)`.
- `_SingleCallAsyncNative(_RecordingAsyncNativeDriver)`: the same constructor arguments as the sync double. `execute_with_description` and `execute_dml` are `async def`. `cursor` is a plain method that appends `("cursor", "", None)`.
- Every `async def` on the three doubles adds one to `driver.awaited` when its body starts. That counter is the number of driver awaitables a statement awaited.

Module-level parameter table `_CACHE_HIT_ASYNC_ADAPTERS` with the columns of the sync table and these classes: `SQLiteAsyncDriver`, `DuckDBAsyncDriver`, `PostgresAsyncDriver`, `MySQLAsyncDriver`, `MSSQLAsyncDriver` registered; `OracleAsyncDriver`, `SpannerAsyncDriver`, `BigQueryAsyncDriver` not registered.

New tests, each the async form of the t23 test with the same parameters and assertions:

1. `test_async_cache_hit_executor_registration_follows_hook_overrides`: eight rows. `cache_hit_hooks_unchanged(adapter_cls, BaseAsyncDriver)` equals the `registered` column. A registered adapter has `_cache_hit_executor is BaseAsyncDriver._run_cached_statement`.
2. `test_async_cache_hit_awaits_one_driver_awaitable`: five rows and the two statements. After the first run, clear `calls` and set `awaited = 0`. After the second run, `calls` holds one entry with the row's call name, `awaited == 1`, and no `cursor` entry is present.
3. `test_async_cache_miss_runs_through_cursor`: five rows.
4. `test_async_cache_hit_result_matches_method_path`: five rows and the five statement cases from the t23 table.
5. `test_async_cache_hit_returning_clause_builds_row_result`: sqlite, duckdb, postgres.
6. `test_async_cache_hit_errors_match_method_path`: five rows and the six error rows from the t23 table, and `test_async_cache_hit_error_classes_on_base_adapter` for `_SampleAsyncAdapter`.
7. `test_async_cache_hit_with_active_observability_uses_observed_path`.
8. `test_async_cache_hit_on_real_embedded_engines`: an in-memory `AsyncSQLiteDriver` with `SQLiteAsyncDriver` and an in-memory `AsyncDuckDBDriver` with `DuckDBAsyncDriver`, with the assertions of the sync test.
9. `test_async_base_execute_dml_awaits_one_cursor_statement`: `await _RecordingAsyncNativeDriver().execute_dml("UPDATE t SET a = 2")` returns `(1, None)` and records one `execute` entry with `()` as parameters.
10. `test_thread_offload_execute_dml_delegates_to_sync_driver`: a `ThreadOffloadAsyncDatabaseDriver` over `_RecordingSyncNativeDriver` returns `(1, None)` from `await driver.execute_dml("UPDATE t SET a = 2")`.

Existing test to change:

- `test_native_async_cache_hit_performs_one_native_execute`: rename to `test_native_async_cache_hit_opens_no_cursor`. The second run adds no `cursor()` call and no cursor `execute`, and returns `[{"a": 25}]`.

Stop rules: the two from the t23 worksheet apply to the async adapters and clients.

## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_async.py src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync --frozen mypy src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_async.py src/py/vertebra/db/adapters src/py/vertebra/extensions/sqlspec/adapters
uv run --no-sync --frozen pyright src/py/vertebra/extensions/sqlspec/_driver.py src/py/vertebra/db/driver/_async.py
```

Run the second command before the change and again after it. A test that passes before and fails after blocks the task; record its name and do not edit those three files.

## Acceptance Criteria

- [x] `AsyncDatabaseDriver` has `execute_dml` and a type-only `cursor` declaration, and `ThreadOffloadAsyncDatabaseDriver` has `execute_dml`, with the bodies above.
- [x] `BaseAsyncDriver.__init__` sets `_cache_hit_executor` and `_cache_hit_state` for SQLite, DuckDB, PostgreSQL, MySQL, and SQL Server, and leaves both `None` for Oracle, Spanner, and BigQuery until t28, t29, and t30.
- [x] `_run_cached_statement` is an `async def` `@staticmethod` with the upstream six-argument signature, and one cached statement awaits one driver awaitable.
- [x] A native error raises the same SQLSpec class on the executor path and the method path, with the native error as `__cause__`.
- [x] The async hook methods and `_map_native_database_exception` have no diff.
- [x] `BaseAsyncDriver.__slots__` has no diff.
- [x] The new tests pass, the changed test passes, and the three other extension test files pass as they did before.

## Notes & Discoveries

- `SQLiteAsyncDriver` and `DuckDBAsyncDriver` bind `_fetch_rows` and `_run_dml` once at construction; closing and reconnecting a native async driver under an existing adapter leaves cached statements bound to the closed client until a new session adapter is created.
- Both real `AsyncSQLiteDriver` and `AsyncDuckDBDriver` raise `vertebra._vertebra.InterfaceError` when closed on both the cache-hit executor path and the method path, mapping to the same SQLSpec exception class.
- `make dev` was skipped because no Rust source files were modified in this task and all 13 native extensions are already built and verified in `.venv`. `make lint` was not run globally with `--fix` because unrelated tasks in this spec hold uncommitted edits in the working tree; targeted `ruff check`, `ruff format --check`, `mypy` (across `_driver.py`, `_async.py`, `db/adapters`, and `sqlspec/adapters`), and `pyright` all passed with zero errors.
- Inside the Linux sandbox, `uv run` requires `UV_CACHE_DIR=/tmp/uv-cache` because `~/.cache/uv` is read-only.

[^sqlspec-async-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver`
[^async-driver-base]: `src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver`
