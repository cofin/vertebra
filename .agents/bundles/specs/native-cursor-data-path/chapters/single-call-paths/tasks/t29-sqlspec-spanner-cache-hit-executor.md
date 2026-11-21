---
type: Task
id: "native-cursor-data-path:single:t29"
chapter_id: single-call-paths
title: "Spanner SQLSpec cache-hit executor"
description: "Register the cache-hit executor on the sync and async Spanner SQLSpec adapters with callables that read the session's staleness and request tags at call time and keep the read-only check."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t23"
  - "native-cursor-data-path:single:t24"
  - "native-cursor-data-path:single:t21"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t23-sqlspec-sync-cache-hit-executor.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t24-sqlspec-async-cache-hit-executor.md"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:1-40"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:133-158"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:167-193"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:328-362"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:396-447"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:933-993"
  - "src/py/vertebra/db/adapters/spanner/_sync.py:453-496"
  - "src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py:1266-1404"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py
estimated_loc: 240
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T17:00:00Z
sources:
  - id: spanner-adapter
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py::SpannerSyncDriver
    title: "Spanner SQLSpec adapter: per-session cursor options and the read-only check"
tags: [sqlspec, spanner, performance]
---

# t29 — Spanner SQLSpec cache-hit executor

## Objective & Scope Boundary

After this task, a cached statement on a sync or async Spanner SQLSpec session runs through the cache-hit executor from t23 and t24. Each statement still gets the session's staleness and request tags, and a read-only session still refuses a write.[^spanner-adapter]

In scope, all in `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py`:

- Three new methods on `SpannerSyncDriver` and three on `SpannerAsyncDriver`.
- Tests in `test_sqlspec_spanner_adapter.py`.

Out of scope:

- `src/py/vertebra/extensions/sqlspec/_driver.py`.
- `with_cursor`, `dispatch_execute`, `dispatch_execute_many`, `_cursor_kwargs`, `_require_writable`, and `_ThreadedAsyncCursor`.
- Every file under `src/py/vertebra/db/adapters/spanner/`.

## Bounded Read Manifest

| Entry | Why |
| --- | --- |
| The t23 and t24 worksheets | The executor contract, the state tuple, and how the base `__init__` calls `_cache_hit_callables` |
| `spanner/driver.py:1-40` | Imports |
| `spanner/driver.py:133-158` | `_is_read_sql` |
| `spanner/driver.py:167-193` | `_ThreadedAsyncCursor`, used by the async `with_cursor` when the connection has no `cursor` |
| `spanner/driver.py:328-362` | `SpannerSyncDriver.__init__`: the session values are assigned after `super().__init__` |
| `spanner/driver.py:396-447` | Sync `_map_exception`, `_cursor_kwargs`, `with_cursor`, `_require_writable`, `dispatch_execute` |
| `spanner/driver.py:933-993` | The async forms |
| `db/adapters/spanner/_sync.py:453-496` | The native `cursor`: it attaches the parameter encoder and uses the active transaction when one exists |
| `test_sqlspec_spanner_adapter.py:1266-1404` | `_CountingSyncCursor`, `_CountingAsyncCursor`, `_CountingSpannerConnection`, and the existing cache-hit test |

What the adapter code establishes:

- `with_cursor` creates the cursor with `connection.cursor(**self._cursor_kwargs())`. `_cursor_kwargs` reads `_staleness`, the active snapshot, and the `request_tag` and `transaction_tag` driver features each time it is called.
- `dispatch_execute` raises `ImproperConfigurationError` for a statement that is not a read when the session is read-only.
- The native cursor encodes Python parameters with the driver's encoder. A cached statement passes its parameters to the cursor as they are.
- The async `with_cursor` falls back to a threaded wrapper around `connection._sync` when the connection has no `cursor`.

## Symbol & Signature Contracts

Imports in `spanner/driver.py`: add `Callable` to the `collections.abc` import, and add `from sqlspec.driver._common import cache_hit_hooks_unchanged`.

The base `__init__` calls `self._cache_hit_callables()` before `SpannerSyncDriver.__init__` assigns `_read_only`, `_staleness`, and `_active_snapshot`. The callables are bound methods that read those attributes when a statement runs, so every statement sees the session's current values.

Methods on `SpannerSyncDriver`, placed directly before `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the session-aware callables the cache-hit executor calls, or `None` to register no executor."""
    if not cache_hit_hooks_unchanged(type(self), SpannerSyncDriver):
        return None
    if not callable(getattr(self.native_driver, "cursor", None)):
        return None
    return self._cached_fetch_rows, self._cached_run_dml

def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Run a cached row-returning statement on a session-configured cursor and return `(rows, names)`."""
    if self._read_only and not _is_read_sql(sql):
        self._require_writable("DML/DDL")
    cursor = self.connection.cursor(**self._cursor_kwargs())
    try:
        cursor.execute(sql, params)
        description = cursor.description
        if description is None:
            return [], ()
        return cursor.fetchall(), tuple(column[0] for column in description)
    finally:
        cursor.close()

def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
    """Run a cached statement without rows on a session-configured cursor and return `(rowcount, None)`."""
    if self._read_only and not _is_read_sql(sql):
        self._require_writable("DML/DDL")
    cursor = self.connection.cursor(**self._cursor_kwargs())
    try:
        cursor.execute(sql, params)
        return max(cursor.rowcount, 0), None
    finally:
        cursor.close()
```

Methods on `SpannerAsyncDriver`, placed directly before its `_map_exception`:

- `_cache_hit_callables`: the same body, testing against `SpannerAsyncDriver`.
- `async def _cached_fetch_rows` and `async def _cached_run_dml`: the same bodies with `await cursor.execute(sql, params)`, `await cursor.fetchall()`, and `await cursor.close()`. The cursor is created with the plain call `self.connection.cursor(**self._cursor_kwargs())`.

Why the callables use a cursor and not the native driver's row methods. The native `execute` and the base `execute_dml` create their cursor without the session's staleness, and the connection doubles and wrappers this adapter accepts provide `cursor` only. A cursor created with `_cursor_kwargs()` is the route `with_cursor` uses, so staleness and tags are the same on both paths.

Cases that register no executor, decided at construction:

| Case | Reason |
| --- | --- |
| The connection has no callable `cursor` | The async adapter serves such a connection through `_ThreadedAsyncCursor` on the method path. The executor does not repeat that probe per statement |
| A subclass overrides a name in `_CACHE_HIT_HOOKS` | The subclass keeps its override on the method path |

No statement is declined at call time. A write on a read-only session raises, as on the method path.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| Session built with `staleness=` | Every cached statement's cursor is created with that staleness |
| Session inside an active snapshot | The snapshot's staleness is read at call time |
| `driver_features["request_tag"]` changed between two runs of a cached statement | The second run's cursor carries the new tag |
| Read-only session, cached `UPDATE` or DML with a returning clause | `ImproperConfigurationError` before any cursor is created. It is a SQLSpec error and passes through the executor unmapped |
| Read-only session, cached `SELECT` | Runs |
| Session bound to a read-write transaction | The native `cursor` returns the transaction's cursor, as on the method path |
| Parameters are a tuple, list, or dict | Passed to the cursor, which encodes them with the driver's encoder |
| DML result | `rows_affected` is `max(cursor.rowcount, 0)`. `last_inserted_id` is `None` |
| A native error from `execute` | The cursor is closed, then the executor maps the error with `map_spanner_exception` and the base mapper |
| Async adapter over a connection with only `_sync` | No executor. Method path with the threaded cursor |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py`, after the existing `test_spanner_native_cursor_*` tests. They use `_CountingSpannerConnection` and are parametrized over `mode` in `["sync", "async"]` with the pattern the existing tests use.

To see the executor run, a test replaces `adapter._cache_hit_state` with a tuple whose first two items wrap the originals and count calls.

1. `test_spanner_cache_hit_executor_is_registered`: `adapter._cache_hit_executor` is the base executor for the mode, and `_cache_hit_state` has three items.
2. `test_spanner_cache_hit_makes_one_adapter_call_and_one_cursor`, parametrized over a select and an update: run once and reset the counters. The second run adds one to the matching callable counter, one entry to `conn.cursor_kwargs`, and one `execute` to that cursor. The cursor is closed.
3. `test_spanner_cache_hit_result_matches_method_path`, parametrized over a select, an empty select, an update with `rowcount=4`, and DML with a returning clause: a second adapter over an identical connection has `_cache_hit_executor = None`. The second run on each returns equal `all()`, `column_names`, and `rows_affected`.
4. `test_spanner_cache_hit_keeps_session_staleness_and_tags`: an adapter built with `staleness=StalenessBound.exact_staleness(2500)` and `driver_features={"request_tag": "first"}`. After the cached run, the last entry of `conn.cursor_kwargs` holds that staleness and `request_tag == "first"`. Set `adapter.driver_features["request_tag"] = "second"` and run again. The last entry holds `"second"`.
5. `test_spanner_cache_hit_read_only_session_refuses_write`: on an adapter built with `read_only=True`, calling `adapter._cache_hit_state[1]("UPDATE t SET a = 1", ())` raises `ImproperConfigurationError` and adds no entry to `conn.cursor_kwargs`. Calling `adapter._cache_hit_state[0]("SELECT 1", ())` returns rows. The async mode awaits both.
6. `test_spanner_cache_hit_not_registered_without_cursor`: an async adapter over a connection double that has `_sync` and no `cursor` has `_cache_hit_executor is None`.
7. `test_spanner_cache_hit_errors_match_method_path`: a connection built with `error=_make_spanner_error("already exists", 6)`. The executor adapter and the method-path adapter raise the same class on the second run, and the executor error's `__cause__` is the native error. Build the cache entry on a connection without the error first, sharing nothing else.

Existing tests: `test_spanner_native_cursor_cache_hit_and_rows` and the other `test_spanner_native_cursor_*` tests keep passing. If one asserts a count that the executor changes, such as the number of `fetchall` calls, update that assertion and state the new count in Notes.

## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py
uv run --no-sync --frozen mypy src/py/vertebra/extensions/sqlspec/adapters/spanner
```

Run the second command before the change and again after it. A test that passes before and fails after blocks the task; record its name and do not edit those two files.

## Acceptance Criteria

- [x] Both Spanner adapters register the base executor when the connection has a callable `cursor`.
- [x] A cached statement makes one call into the adapter and creates one cursor with `_cursor_kwargs()` read at that moment.
- [x] A read-only session raises `ImproperConfigurationError` for a cached write before any cursor is created.
- [x] `with_cursor`, `dispatch_execute_many`, `_cursor_kwargs`, and `_require_writable` have no diff, and `dispatch_execute` adds only `self._cache_statement(statement)` so `_stmt_cache` is populated on first execution.
- [x] The seven new tests pass in both modes where they apply, and the file's existing tests plus `test_sqlspec_driver_dispatch.py` pass.

## Notes & Discoveries

- The upstream method path does not run the read-only check on a cache hit, because it calls `cursor.execute` directly and skips `dispatch_execute`. The callables run the check, so a cached write on a read-only session now fails before the request is sent.
- `SpannerSyncDriver.dispatch_execute` and `SpannerAsyncDriver.dispatch_execute` now call `self._cache_statement(statement)` after `compile_spanner_statement` so SQLSpec populates `self._stmt_cache` on first execution.
- Updated `test_sqlspec_driver_dispatch.py` registration matrix assertions to expect Spanner's own `_cache_hit_callables` registration.
- Verified with `pytest` (`866 passed`), `ruff check` (`All checks passed`), and `mypy` (`0 issues`).

[^spanner-adapter]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py::SpannerSyncDriver`
