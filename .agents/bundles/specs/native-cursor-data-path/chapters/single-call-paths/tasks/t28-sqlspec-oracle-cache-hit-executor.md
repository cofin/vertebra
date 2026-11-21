---
type: Task
id: "native-cursor-data-path:single:t28"
chapter_id: single-call-paths
title: "Oracle SQLSpec cache-hit executor"
description: "Register the cache-hit executor on the sync and async Oracle SQLSpec adapters with callables that run a cached statement through checkout_cursor and record out-binds on the connection."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t23"
  - "native-cursor-data-path:single:t24"
  - "native-cursor-data-path:single:t17"
  - "native-cursor-data-path:single:t18"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t23-sqlspec-sync-cache-hit-executor.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t24-sqlspec-async-cache-hit-executor.md"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:1-40"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:76-104"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:238-325"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:598-685"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:260-282"
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py::_CountingOracleSyncDriverProxy"
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py:1376-1420"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py
estimated_loc: 230
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T17:00:00Z
sources:
  - id: oracle-adapter
    resource: src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py::OracleSyncDriver
    title: "Oracle SQLSpec adapter: with_cursor through checkout_cursor and out-bind recording"
tags: [sqlspec, oracle, performance]
---

# t28 — Oracle SQLSpec cache-hit executor

## Objective & Scope Boundary

After this task, a cached statement on a sync or async Oracle SQLSpec session runs through the cache-hit executor from t23 and t24. It makes one call into the adapter, checks one cursor out, and records out-binds on the connection as the method path does.[^oracle-adapter]

In scope, all in `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`:

- Four new methods on `OracleSyncDriver` and four on `OracleAsyncDriver`.
- Tests in `test_sqlspec_vtb_stacks.py`.

Out of scope:

- `src/py/vertebra/extensions/sqlspec/_driver.py`. The executor function and the registration in the base `__init__` are used, not edited.
- `execute`, `with_cursor`, `dispatch_execute`, and `dispatch_execute_many` on both Oracle adapters. They stay for the method path.
- Every file under `src/py/vertebra/db/adapters/oracle/`.

## Bounded Read Manifest

| Entry | Why |
| --- | --- |
| The t23 and t24 worksheets | The executor contract, the state tuple, and how the base `__init__` calls `_cache_hit_callables` |
| `oracle/driver.py:1-40` | Imports |
| `oracle/driver.py:76-104` | `OracleSyncDriver.__init__` |
| `oracle/driver.py:238-325` | Sync `execute`, `with_cursor`, `dispatch_execute`, `_map_exception` |
| `oracle/driver.py:598-685` | The async forms |
| `db/adapters/oracle/_sync.py:260-282` | `cursor` and `checkout_cursor` on the native driver |
| Test file, `_CountingOracleSyncDriverProxy` and its async twin directly below it | Connection doubles that count `checkout_cursor` calls and hold `_last_out_binds` |
| `test_sqlspec_vtb_stacks.py:1376-1420` | `_FakeOracleCursor`, the cursor double shape |

What the adapter code establishes:

- `with_cursor` enters `connection.checkout_cursor()`. On a pooled driver that is the only way to get a cursor outside a transaction.
- `dispatch_execute` writes `cursor.out_binds` to `connection._last_out_binds` after every statement, when the connection has that attribute.
- Out-bind declarations and the sessionless suspend flag reach `dispatch_execute` only through `statement_config.execution_args`.
- `execute` handles `suspend_on_success=True` itself and never reaches the cached path with it.

## Symbol & Signature Contracts

Imports in `oracle/driver.py`: add `Callable` to the `collections.abc` import, and add `from sqlspec.driver._common import cache_hit_hooks_unchanged`.

The base `__init__` calls `self._cache_hit_callables()` and registers the executor when it returns a pair. The callables are bound methods of the adapter. They read the connection at call time, so nothing they need has to exist before the adapter's own `__init__` finishes.

Methods on `OracleSyncDriver`, placed directly before `_map_exception`:

```python
def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
    """Return the cursor-checkout callables the cache-hit executor calls, or `None` to register no executor."""
    if not cache_hit_hooks_unchanged(type(self), OracleSyncDriver):
        return None
    if self.statement_config.execution_args:
        return None
    if not callable(getattr(self.native_driver, "checkout_cursor", None)):
        return None
    return self._cached_fetch_rows, self._cached_run_dml

def _record_out_binds(self, cursor: Any) -> None:
    """Copy the cursor's out-bind values to the connection when it tracks them."""
    if hasattr(self.connection, "_last_out_binds"):
        self.connection._last_out_binds = cursor.out_binds

def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Run a cached row-returning statement on one checked-out cursor and return `(rows, names)`."""
    with self.connection.checkout_cursor() as cursor:
        cursor.execute(sql, params)
        self._record_out_binds(cursor)
        description = cursor.description
        if description is None:
            return [], ()
        return cursor.fetchall(), tuple(column[0] for column in description)

def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
    """Run a cached statement without rows on one checked-out cursor and return `(rowcount, lastrowid)`."""
    with self.connection.checkout_cursor() as cursor:
        cursor.execute(sql, params)
        self._record_out_binds(cursor)
        rowcount = cursor.rowcount
        return (rowcount if rowcount > 0 else 0), cursor.lastrowid
```

Methods on `OracleAsyncDriver`, placed directly before its `_map_exception`:

- `_cache_hit_callables`: the same body, testing against `OracleAsyncDriver`.
- `_record_out_binds`: the same body.
- `async def _cached_fetch_rows` and `async def _cached_run_dml`: the same bodies with `async with self.connection.checkout_cursor() as cursor`, `await cursor.execute(sql, params)`, and `await cursor.fetchall()`.

Why the callables use `checkout_cursor` and not the native single-call methods. The native driver's row methods set its out-bind record to `None`, and only its `execute_dml` returns out values. The method path records `cursor.out_binds` after every statement, including a row-returning one with out-bind variables in its parameters. A cursor carries both, so the result and the recorded out-binds equal the method path in every case, and no statement has to be declined at call time.

Cases that register no executor, all decided at construction:

| Case | Reason |
| --- | --- |
| `statement_config.execution_args` is not empty | `out_binds` and the sessionless suspend flag are read from it by `dispatch_execute`. The method path keeps them |
| The connection has no `checkout_cursor` | The callables cannot run |
| A subclass overrides a name in `_CACHE_HIT_HOOKS` | The subclass keeps its override on the method path |

No statement is declined at call time.

## Edge Cases & Error Semantics

| Case | Behavior |
| --- | --- |
| Cached `SELECT` on a pooled driver outside a transaction | One `checkout_cursor()`, one `execute`, one `fetchall`. The client goes back to the pool when the block exits |
| Cached statement inside a transaction | `checkout_cursor` uses the pinned client, as on the method path |
| Parameters hold an out-bind variable | The variable is filled in place, and `connection._last_out_binds` is the cursor's `out_binds` |
| A plain statement after one with out-binds | `connection._last_out_binds` becomes the new cursor's `out_binds`, which is `None` |
| A connection object without `_last_out_binds` | Nothing is recorded, as in `dispatch_execute` |
| `execute(..., suspend_on_success=True)` | Handled by the adapter's `execute`. The executor is not called |
| `INSERT` that affects a row | `last_inserted_id` is the cursor's `lastrowid`, by the rule in the t23 executor |
| A native error from `execute` | Mapped by `map_oracle_exception`, then by the base mapper, in the executor. The cursor block exits first and returns the client |
| Async: task cancelled inside the block | The `async with` exits and returns the client. `CancelledError` is not mapped |

## Public-Seam Test Blueprint

All tests go in `src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py`. They use `_CountingOracleSyncDriverProxy` and its async twin over the stub clients the existing Oracle tests in the file use. Each test is parametrized over `mode` in `["sync", "async"]` with the file's existing pattern for running both forms.

To see the executor run, a test replaces `adapter._cache_hit_state` with a tuple whose first two items wrap the originals and count calls.

1. `test_oracle_cache_hit_executor_is_registered`: `adapter._cache_hit_executor` is `BaseSyncDriver._run_cached_statement` or `BaseAsyncDriver._run_cached_statement`, and `_cache_hit_state` has three items.
2. `test_oracle_cache_hit_makes_one_adapter_call_and_one_checkout`, parametrized over `SELECT 1 AS a FROM dual` and `UPDATE t SET a = 2`: run once, then note `checkout_calls` and reset the counters. The second run adds one to the matching callable counter, zero to the other, and one to `checkout_calls`.
3. `test_oracle_cache_hit_result_matches_method_path`, parametrized over a select, an empty select, an update, and an insert: a second adapter over an identical double has `_cache_hit_executor = None`. The second run on each returns equal `all()`, `column_names`, and `rows_affected`.
4. `test_oracle_cache_hit_records_out_binds`: the cursor double reports `out_binds == {"out": 99}` for one statement and `None` for the next. After the cached run of the first, `connection._last_out_binds == {"out": 99}`. After the cached run of the second, it is `None`.
5. `test_oracle_cache_hit_not_registered_with_execution_args`: an adapter built with `statement_config=build_oracle_statement_config().replace(execution_args={"out_binds": {"out": "NUMBER"}})` has `_cache_hit_executor is None`, and a repeated statement still returns its result through the method path.
6. `test_oracle_cache_hit_not_registered_without_checkout_cursor`: an adapter over a connection double with no `checkout_cursor` has `_cache_hit_executor is None`.
7. `test_oracle_cache_hit_errors_match_method_path`: the cursor double raises `NativeIntegrityError("ORA-00001: unique constraint violated")` on the second run. The executor adapter and the method-path adapter raise the same class, and the executor error's `__cause__` is the native error. `checkout_calls` shows the cursor block was entered once.
8. `test_oracle_suspend_on_success_skips_executor`: with the sessionless client double the file already uses, `execute(sql, suspend_on_success=True)` run twice leaves both callable counters at zero.

Existing Oracle tests in the file keep passing unchanged.

## Verification

```bash
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_context_notify_and_queues.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py
uv run --no-sync --frozen mypy src/py/vertebra/extensions/sqlspec/adapters/oracle
```

Run the second command before the change and again after it. A test that passes before and fails after blocks the task; record its name. In `test_sqlspec_driver_dispatch.py`, the registration tests from t23 and t24 assert only `cache_hit_hooks_unchanged` against the base classes for Oracle, so they keep passing.

## Acceptance Criteria

- [x] Both Oracle adapters register the base executor when the three construction checks pass.
- [x] A cached statement makes one call into the adapter and one `checkout_cursor()`.
- [x] `connection._last_out_binds` after a cached statement equals what `dispatch_execute` records for the same statement.
- [x] An adapter whose statement config carries `execution_args` registers no executor.
- [x] `execute`, `with_cursor`, `dispatch_execute`, and `dispatch_execute_many` have no diff.
- [x] The eight new tests pass in both modes, and the file's existing tests pass.

## Notes & Discoveries

- The Oracle callables keep the generator-based `checkout_cursor`. They remove the upstream hook calls around it, not the checkout itself. The native single-call methods cannot be used here without losing the out-bind record for row-returning statements.
- Verified with `UV_CACHE_DIR=/tmp/uv-cache` because `~/.cache/uv` is read-only inside the sandbox; `make dev` and repo-wide `make lint` were not run directly inside the shared workspace to avoid recompiling untouched Rust crates or mutating files owned by concurrent workers. All targeted Oracle checks (`ruff format --check`, `ruff check`, `mypy`, `pyright`, `test_sqlspec_vtb_stacks.py`, `test_oracle_context_notify_and_queues.py`, and Oracle cases in `test_sqlspec_driver_dispatch.py`) passed.
- In `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py`, the 4 unrelated `spanner` and `bigquery` registration tests failed because concurrent tasks (`t29`/`t30`) registered cache-hit executors on `Spanner*Driver` and `BigQuery*Driver` while `test_sqlspec_driver_dispatch.py` still expected `registered=False`; both Oracle registration tests passed.

[^oracle-adapter]: `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py::OracleSyncDriver`
