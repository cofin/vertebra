---
type: Task
id: "native-cursor-data-path:single:t30"
chapter_id: single-call-paths
title: "BigQuery SQLSpec cache-hit executor"
description: "Register the cache-hit executor on the sync and async BigQuery SQLSpec adapters with callables that run a cached statement on one cursor and record its query metadata on the connection."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t23"
  - "native-cursor-data-path:single:t24"
  - "native-cursor-data-path:single:t22"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t23-sqlspec-sync-cache-hit-executor.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t24-sqlspec-async-cache-hit-executor.md"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:1-70"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:198-282"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:555-640"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:273-297"
  - "src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py:915-1118"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py
estimated_loc: 210
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T17:00:00Z
sources:
  - id: bigquery-adapter
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py::BigQuerySyncDriver
    title: "BigQuery SQLSpec adapter: cursor path and query metadata recording"
tags: [sqlspec, bigquery, performance]
---

# t30 — BigQuery SQLSpec cache-hit executor

## Objective & Scope Boundary

After this task, a cached statement on a sync or async BigQuery SQLSpec session runs through the cache-hit executor from t23 and t24. Its job metadata reaches the native driver as it does for a first execution.[^bigquery-adapter]

In scope, all in `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py`:

- Three new methods on `BigQuerySyncDriver` and three on `BigQueryAsyncDriver`.
- Tests in `test_sqlspec_bigquery_adapter.py`.

Out of scope:

- `src/py/vertebra/extensions/sqlspec/_driver.py`.
- `with_cursor`, `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `prepare_statement`, and `_record_cursor_metadata`.
- Every file under `src/py/vertebra/db/adapters/bigquery/`.

## Bounded Read Manifest

| Entry | Why |
| --- | --- |
| The t23 and t24 worksheets | The executor contract, the state tuple, and how the base `__init__` calls `_cache_hit_callables` |
| `bigquery/driver.py:1-70` | Imports and `_record_cursor_metadata` |
| `bigquery/driver.py:198-282` | `BigQuerySyncDriver`: `__init__`, `_map_exception`, `with_cursor`, `dispatch_execute` |
| `bigquery/driver.py:555-640` | The async forms |
| `db/adapters/bigquery/_sync.py:273-297` | The native `cursor`: it attaches the session, the job options, and the parameter encoder |
| `test_sqlspec_bigquery_adapter.py:915-1118` | `_CountingBigQueryCursor`, its async twin, `_CountingBigQueryConnection`, and the existing cache-hit test with cost metadata |

What the adapter code establishes:

- `with_cursor` creates the cursor with `connection.cursor()` and closes it on exit.
- `dispatch_execute` calls `_record_cursor_metadata(self.connection, cursor)` after `execute`. That function reads `cursor.query_metadata` and passes it to `connection._after_query` when both exist.
- The native driver's `execute_with_description` closes its own cursor and does not return the metadata, so the adapter cannot record it from that call.
- The native cursor encodes Python parameters with the driver's encoder. A cached statement passes its parameters to the cursor as they are.

## Symbol & Signature Contracts

Imports in `bigquery/driver.py`: add `Callable` to the `collections.abc` import, and add `from sqlspec.driver._common import cache_hit_hooks_unchanged`.

The base `__init__` calls `self._cache_hit_callables()` and registers the executor when it returns a pair. The callables are bound methods and read the connection when a statement runs.

Methods on `BigQuerySyncDriver`, placed directly before `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the metadata-recording callables the cache-hit executor calls, or `None` to register no executor."""
    if not cache_hit_hooks_unchanged(type(self), BigQuerySyncDriver):
        return None
    if not callable(getattr(self.native_driver, "cursor", None)):
        return None
    return self._cached_fetch_rows, self._cached_run_dml

def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Run a cached row-returning statement on one cursor, record its metadata, and return `(rows, names)`."""
    cursor = self.connection.cursor()
    try:
        cursor.execute(sql, params)
        _record_cursor_metadata(self.connection, cursor)
        description = cursor.description
        if description is None:
            return [], ()
        return cursor.fetchall(), tuple(str(column[0]) for column in description)
    finally:
        cursor.close()

def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
    """Run a cached statement without rows on one cursor, record its metadata, and return `(rowcount, None)`."""
    cursor = self.connection.cursor()
    try:
        cursor.execute(sql, params)
        _record_cursor_metadata(self.connection, cursor)
        return max(cursor.rowcount, 0), None
    finally:
        cursor.close()
```

Methods on `BigQueryAsyncDriver`, placed directly before its `_map_exception`:

- `_cache_hit_callables`: the same body, testing against `BigQueryAsyncDriver`.
- `async def _cached_fetch_rows` and `async def _cached_run_dml`: the same bodies with `await cursor.execute(sql, params)`, `await cursor.fetchall()`, and `await cursor.close()`. `_record_cursor_metadata` stays a plain call.

Why the callables use a cursor and not the native driver's row methods. The metadata is an attribute of the cursor. A cursor created with `connection.cursor()` is the route `with_cursor` uses, so the session, job options, and encoder are the same on both paths.

Cases that register no executor, decided at construction:

| Case | Reason |
| --- | --- |
| The connection has no callable `cursor` | The callables cannot run |
| A subclass overrides a name in `_CACHE_HIT_HOOKS` | The subclass keeps its override on the method path |

No statement is declined at call time.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| Cached `SELECT` | One `cursor()`, one `execute`, one metadata record, one `fetchall`, one `close` |
| The cursor has no `query_metadata`, or it is `None` | Nothing is recorded, as in `_record_cursor_metadata` |
| The connection has no `_after_query` | Nothing is recorded |
| Statement inside a BigQuery session | The native `cursor` attaches the session id, as on the method path |
| DML result | `rows_affected` is `max(cursor.rowcount, 0)`. `last_inserted_id` is `None` |
| A statement with a result set and zero rows | `([], names)` |
| A native error from `execute` | No metadata is recorded. The cursor is closed, then the executor maps the error with `map_bigquery_exception` and the base mapper |
| `execute(..., tenant_id=...)` | Rejected by `prepare_statement` on the path that reaches it. This task does not change which calls reach the cached path |
| Async: task cancelled during `execute` | The `finally` closes the cursor. `CancelledError` is not mapped |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py`, after the existing `test_bigquery_native_cursor_*` tests. They use `_CountingBigQueryConnection` and are parametrized over `mode` in `["sync", "async"]` with the pattern the existing tests use.

To see the executor run, a test replaces `adapter._cache_hit_state` with a tuple whose first two items wrap the originals and count calls.

1. `test_bigquery_cache_hit_executor_is_registered`: `adapter._cache_hit_executor` is the base executor for the mode, and `_cache_hit_state` has three items.
2. `test_bigquery_cache_hit_makes_one_adapter_call_and_one_cursor`, parametrized over a select and an update: run once and reset the counters. The second run adds one to the matching callable counter, creates one cursor on the connection double, and calls `execute` once on it. The cursor is closed.
3. `test_bigquery_cache_hit_result_matches_method_path`, parametrized over a select, an empty select, and an update with `rowcount=5`: a second adapter over an identical connection has `_cache_hit_executor = None`. The second run on each returns equal `all()`, `column_names`, and `rows_affected`.
4. `test_bigquery_cache_hit_records_query_metadata`: a connection built with `query_metadata={"total_bytes_billed": 10, "cache_hit": False}`. Clear `conn.last_query_metadata` after the first run. After the cached run it equals that dictionary again. The keys are the ones the existing cost-metadata test uses.
5. `test_bigquery_cache_hit_error_records_no_metadata_and_matches_method_path`: a connection built with `error=BigQueryError("HTTP 400: Syntax error: Unexpected identifier SELEC")`, the error the existing invalid-query test uses. The executor adapter and the method-path adapter raise the same class on the second run, the executor error's `__cause__` is the native error, and `conn.last_query_metadata` is unchanged. Build the cache entry on a connection without the error first.
6. `test_bigquery_cache_hit_not_registered_without_cursor`: an adapter over a connection double with no `cursor` has `_cache_hit_executor is None`.

Existing tests: `test_bigquery_native_cursor_cache_hit_rows_and_cost_metadata` and the other `test_bigquery_native_cursor_*` tests keep passing. If one asserts a count that the executor changes, update that assertion and state the new count in Notes.

## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py
uv run --no-sync --frozen mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery
```

Run the second command before the change and again after it. A test that passes before and fails after blocks the task; record its name and do not edit those two files.

## Acceptance Criteria

- [x] Both BigQuery adapters register the base executor when the connection has a callable `cursor`.
- [x] A cached statement makes one call into the adapter, creates one cursor, and records that cursor's metadata on the connection.
- [x] A failed statement records no metadata and closes its cursor.
- [x] `with_cursor`, `dispatch_execute_many`, and `_record_cursor_metadata` have no diff, and `dispatch_execute` adds only `self._cache_statement(statement)` so `_stmt_cache` is populated on first execution.
- [x] The six new tests pass in both modes, and the file's existing tests plus `test_sqlspec_driver_dispatch.py` pass.

## Notes & Discoveries

- The upstream method path calls `cursor.execute` directly on a cache hit and skips `dispatch_execute`, which holds the adapter's `_record_cursor_metadata` call. The callables make that call on every cached statement.
- `BigQuerySyncDriver.dispatch_execute` and `BigQueryAsyncDriver.dispatch_execute` now call `self._cache_statement(statement)` after `compile_bigquery_statement` so SQLSpec populates `self._stmt_cache` on first execution.
- Updated `test_sqlspec_driver_dispatch.py` registration matrix assertions to expect BigQuery's own `_cache_hit_callables` registration.
- Verified with `pytest` (`866 passed`), `ruff check` (`All checks passed`), and `mypy` (`0 issues`).

[^bigquery-adapter]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py::BigQuerySyncDriver`
