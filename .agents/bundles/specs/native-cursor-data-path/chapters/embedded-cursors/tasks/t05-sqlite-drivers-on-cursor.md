---
type: Task
id: "native-cursor-data-path:embedded:t05"
chapter_id: embedded-cursors
title: "SQLite native drivers call the cursor and expose cursor()"
description: "Move the SQLite sync and async native drivers onto the native cursor, add cursor() to both, remove SQL classification, statement-cache lookups, and leases from their query path, and record the SQLite benchmark sample."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t04"
read_set:
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:1-48"
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:100-125"
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:154-274"
  - "src/py/vertebra/db/adapters/sqlite/_async.py:1-32"
  - "src/py/vertebra/db/adapters/sqlite/_async.py:100-125"
  - "src/py/vertebra/db/adapters/sqlite/_async.py:151-262"
  - "src/py/vertebra/db/driver/_sync.py:91-180"
  - "src/py/tests/unit/db/test_sqlite_driver.py:1-18"
  - "src/py/tests/unit/db/test_sqlite_driver.py:160-228"
  - "tools/benchmarks/adapters/sql.py:198-350"
  - "tools/benchmarks/adapters/sql.py:1312-1341"
files:
  - src/py/vertebra/db/adapters/sqlite/_sync.py
  - src/py/vertebra/db/adapters/sqlite/_async.py
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 250
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver
    title: "SQLite native driver query methods"
tags: [sqlite, native-driver, cleanup, benchmark]
---

# SQLite native drivers call the cursor and expose cursor()

## Objective & Scope Boundary

A query through `SyncSQLiteDriver` or `AsyncSQLiteDriver` makes one native `execute` call on a native cursor. The driver no longer classifies SQL text, looks up the shared-memory statement cache, or takes a broker lease.

Both drivers gain a `cursor()` method that returns a new native cursor. This task is the only one that adds it. The DB-API module and the SQLSpec base in `wrapper-layers` obtain the SQLite native cursor through it.

Existing public method names, signatures, and return types of both drivers stay the same. `pipeline.py`, `config.py`, `core.py`, and `driver.py` in the SQLite adapter are not edited. The native client methods `execute` and `fetch_all` stay in the extension.

## Bounded Read Manifest

- `sqlite/_sync.py:1-48` — imports, the `TYPE_CHECKING` block, and `_sqlite_param_count`.
- `sqlite/_sync.py:100-125`, `sqlite/_async.py:100-125` — `_require_client` and `connect`, which the new `cursor` method follows.
- `sqlite/_sync.py:154-274` — `execute`, `execute_dml`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_to_arrow`.
- `sqlite/_async.py:1-32`, `151-262` — the async twins and the import of `_sqlite_param_count`.
- `db/driver/_sync.py:91-180` — base `execute_tuples`, `select`, `select_one`, `select_value`; these keep calling `execute_with_description`.
- `test_sqlite_driver.py:1-18`, `160-228` — imports and the two broker tests this task replaces.
- `tools/benchmarks/adapters/sql.py:198-350`, `1312-1341` — SQLite capture and its CLI flags.

## Symbol & Signature Contracts

New method on `SyncSQLiteDriver`, placed directly after `connect`:

```python
def cursor(self, *, lazy_begin: bool = False) -> SyncSQLiteCursor:
    """Return a new native cursor on this driver's client."""
    return self._require_client().cursor(lazy_begin=lazy_begin)
```

New method on `AsyncSQLiteDriver`, placed directly after `connect`. It is a plain `def`:

```python
def cursor(self, *, lazy_begin: bool = False) -> AsyncSQLiteCursor:
    """Return a new native async cursor on this driver's client."""
    return self._require_client().cursor(lazy_begin=lazy_begin)
```

The driver's own query methods create their cursors with the default, so they never open a transaction.

Each cursor class is imported from `vertebra._sqlite` next to the client class in the file's existing `TYPE_CHECKING` block.

Query methods of both drivers, with `await` added in the async driver:

| Method | New body |
| --- | --- |
| `execute_with_description(sql, parameters=None, **kwargs)` | `cur = self._require_client().cursor()`; `cur.execute(sql, parameters)`; `desc = cur.description`; set `self._lastrowid = cur.lastrowid`; when `desc is None` return `([], ())`; otherwise return `(cur.fetchall(), tuple([col[0] for col in desc]))` |
| `execute_dml(sql, parameters=None, **kwargs)` | one cursor; `cur.execute`; `rowcount = cur.rowcount`; `rowid = cur.lastrowid`; set `self._lastrowid = rowid`; return `(rowcount if rowcount > 0 else 0, rowid)` |
| `execute(sql, parameters=None, **kwargs)` | unchanged: returns the first item of `execute_dml` |
| `execute_many(sql, parameter_sets, **kwargs)` | return `0` for an empty sequence; otherwise `cur.executemany(sql, parameter_sets)` and return `cur.rowcount` |
| `select_to_arrow(...)` | keeps its native `select_to_arrow` call; the `_lookup_or_cache_statement`, `_acquire_broker_lease`, and `_release_broker_lease` lines and the `try/finally` around them are removed |
| `execute_tuples`, `select` | deleted from both SQLite drivers; the base class versions at `db/driver/_sync.py:91-120` and their async twins do the same work |

Deleted:

- `_sqlite_param_count` in `_sync.py` and its import in `_async.py`.
- `from vertebra._vertebra import classify_sql_statement` in both files.
- The `if self._broker is None:` branch in `execute_with_description`.
- Every `_lookup_or_cache_statement`, `_acquire_broker_lease`, and `_release_broker_lease` call in both files.
- Imports left unused by the above (`Mapping` stays if still used in annotations).

The `broker` constructor argument and the `broker` property stay. `last_insert_rowid()` and the `lastrowid` property stay.

In the async driver, `cur.execute(...)`, `cur.executemany(...)`, and `cur.fetchall()` are awaited. `cur.description`, `cur.rowcount`, and `cur.lastrowid` are plain reads.

## Edge Cases & Error Semantics

- A statement with no result set passed to `execute_with_description`, `execute_tuples`, or `select` returns no rows and no columns. It does not call `fetchall`.
- `execute` on a read-only statement returns `0`.
- `self._lastrowid` is `None` after any statement that is not a successful insert, as before.
- Errors from cursor-backed methods are the `vertebra._vertebra` classes the drivers raise today, with the same specific subclasses. Existing error assertions pass unchanged.
- `cursor()` on a closed driver raises the error `_require_client` raises today.
- A closed driver raises `InterfaceError` from `_require_client` before any cursor is created.
- Concurrent calls from several threads each use their own cursor.
- An explicit transaction still routes reads to the writer. That decision is made in the pool.

## Public-Seam Test Blueprint

Delete `test_sync_sqlite_driver_uses_common_mixin_broker_and_releases_on_error` and its async twin. Their assertions describe the removed statement-cache and lease path. Replace them with the two broker rows below. Remove the `SQLITE_DIALECT_TAG` import if it has no other use in the file.

| Case | Steps | Expected |
| --- | --- | --- |
| No lease, sync | driver built with `LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)`; create, two inserts, one select, one failing select | `broker.stats().total_acquired == 0`; `broker.statement_cache().stats()` shows zero inserts and zero hits; the failing select raises `vertebra._vertebra.DatabaseError` |
| No lease, async | same through `AsyncSQLiteDriver` | same |
| Insert metadata | `execute_dml` insert, then update | `(1, 1)` then `(n, None)`; `driver.lastrowid` follows |
| Returning rows | `execute_tuples("INSERT ... RETURNING id")` | rows returned; existing returning tests at lines 20-116 and 231-340 pass unchanged |
| No result set | `execute_with_description("CREATE TABLE ...")` | `([], ())` |
| `select_one` | `select_one("SELECT 1 AS a")` | `{"a": 1}` |
| Native cursor, sync | `driver.cursor()` twice | two distinct objects whose type name is `SyncSQLiteCursor`; `execute("SELECT 1")` then `fetchall() == [(1,)]` |
| Native cursor, async | `driver.cursor()` twice on `AsyncSQLiteDriver` | two distinct objects whose type name is `AsyncSQLiteCursor`; the call is not awaited |
| Cursor on closed driver | `close()` then `cursor()` | the error `_require_client` raises |
| Lazy begin through the driver | `driver.cursor(lazy_begin=True)`; insert; `driver.in_transaction`; `driver.rollback()` | `True`; the row is gone |
| `execute_many` | three sets, then empty | `3`, then `0` |
| Removed names | `hasattr(sqlite_sync_mod, "_sqlite_param_count")`, `hasattr(sqlite_sync_mod, "classify_sql_statement")`, same for the async module | all `False` |
| Threads | existing `test_sync_sqlite_driver_serializes_concurrent_multithreaded_operations` | passes unchanged |

## Verification

- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q`
- Engine suite: `make sqlite-test`
- Removal check, each must print nothing:
  - `git grep -n "classify_sql_statement\|_sqlite_param_count\|_lookup_or_cache_statement\|_acquire_broker_lease\|_release_broker_lease" -- src/py/vertebra/db/adapters/sqlite`
- Lint and types: `uv run --no-sync ruff check src/py/vertebra/db/adapters/sqlite src/py/tests/unit/db/test_sqlite_driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/sqlite && uv run --no-sync pyright src/py/vertebra/db/adapters/sqlite`
- Benchmark sample, on release builds:
  - `make build`
  - `PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn ":memory:" --iterations 2000 --warmup 200 --output "$(mktemp -d)/embedded-sqlite.json"`
  - Run the capture three times and keep the median per case. Each report goes to a temporary directory outside the repository. Nothing is written under `tools/benchmarks/results/`.

## Acceptance Criteria

- [ ] Both drivers pass every row of the blueprint and `make sqlite-test` passes.
- [ ] `SyncSQLiteDriver.cursor()` and `AsyncSQLiteDriver.cursor()` each return a new native cursor per call.
- [ ] The removal check prints nothing.
- [ ] No `broker` lease or statement-cache entry is created by any SQLite driver query method.
- [ ] The benchmark sample is recorded under Notes & Discoveries with the `vertebra-sqlite-sync` and `sqlite3` medians for the single-row select case.
- [ ] Native SQLite `select_one` is at or below the `sqlite3` comparator. If it is not, stop after recording the numbers and report the miss before t06 starts.

## Notes & Discoveries

Benchmark sample, 2026-11-20, three captures, median of the per-capture `median_ms`, `--iterations 2000 --warmup 200`, `:memory:`, CPython 3.12 with the GIL:

| Case | `vertebra-sqlite-sync` | `vertebra-sqlite-async` | `sqlite3` | `aiosqlite` |
| --- | --- | --- | --- | --- |
| `point_select_1` | 0.003695 ms | 0.081061 ms | 0.001574 ms | 0.089035 ms |
| `multi_row_select_10` | 0.009016 ms | 0.086944 ms | 0.012860 ms | 0.103569 ms |

- **Miss on the last acceptance criterion.** Sync `select_one` is 2.35x the `sqlite3` comparator on the single-row case. The task is left `in_progress` and t06 should not start until this is decided.
- Where the time goes, measured with `timeit` on the same build: reused native cursor `execute` plus `fetchall` 1.42 us; `client.fetch_all` 1.39 us; `sqlite3` `execute().fetchone()` 1.33 us; `client.cursor()` 0.11 us; `driver.execute_with_description` 2.11 us; `driver.select_one` 3.58 us. The native cursor is within 7% of `sqlite3`. About 0.6 us is the Python body of `execute_with_description` and about 1.5 us is the base `select` -> `hydrate_rows` -> `select_one` chain in `db/driver/_sync.py`, which this task does not own. The comparator returns a bare tuple; `select_one` returns a dict.
- No before-change sample was taken, so the size of the gain from removing classification, cache lookup, and lease is not measured.
- `make build` failed in the sandbox: `uv run maturin develop` tried to fetch `maturin` from PyPI and got HTTP 403. The sample uses release builds of `vertebra-core` and `vertebra-sqlite` only, installed with `uv run --no-sync maturin develop --uv -r` and `... -m src/rs/vertebra-sqlite/Cargo.toml`. Those two release extensions are still installed in `.venv`; the other extensions are unchanged.
- `make lint` stops at pyright on `src/py/vertebra/_typing.py:360` (`numpy` import not resolved), a file outside this task. Ruff, cargo fmt, clippy, and mypy passed before it, and it changed no files. `check_extras` and the 13-extension stubtest did not run from `make lint`; `_sqlite` stubtest passed inside `make sqlite-test`.
- `lastrowid` now comes from the cursor. `INSERT ... RETURNING` through `execute_with_description` sets it, as before. `execute_many` no longer touches `self._lastrowid`.
- Commands in this run used `UV_CACHE_DIR` pointed at a scratch directory because `~/.cache/uv` is read-only in the sandbox.
