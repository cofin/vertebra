---
type: Task
id: "native-cursor-data-path:embedded:t10"
chapter_id: embedded-cursors
title: "DuckDB async driver calls the cursor and unused helpers are deleted"
description: "Move AsyncDuckDBDriver onto the native cursor, delete the Python _AsyncDuckDBCursor and the DuckDBCore helpers with no caller left, and record the DuckDB benchmark sample."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t09"
read_set:
  - "src/py/vertebra/db/adapters/duckdb/_async.py:1-36"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:109-118"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:195-298"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:299-460"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:475-592"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver"
  - "src/py/vertebra/db/adapters/duckdb/core.py:1-135"
  - "src/py/vertebra/db/adapters/duckdb/driver.py"
  - "src/py/tests/unit/db/test_duckdb_driver.py:190-226"
  - "src/py/tests/unit/db/test_duckdb_driver.py:391-496"
  - "src/py/tests/unit/db/test_duckdb_driver.py:498-550"
  - "src/py/tests/unit/db/test_duckdb_driver.py:672-765"
  - "src/py/tests/unit/db/test_duckdb_driver.py:796-832"
  - "tools/benchmarks/adapters/sql.py:353-440"
files:
  - src/py/vertebra/db/adapters/duckdb/_async.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/core.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 250
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/_async.py::AsyncDuckDBDriver
    title: "DuckDB async native driver query methods, secret refresh, and Python cursor"
tags: [duckdb, native-driver, cleanup, benchmark]
---

# DuckDB async driver calls the cursor and unused helpers are deleted

## Objective & Scope Boundary

`AsyncDuckDBDriver` gets the same change `SyncDuckDBDriver` got in t09. After both drivers have moved, the `DuckDBCore` helpers that only the old query path called are deleted with their tests. The chapter's closing DuckDB benchmark sample is recorded here, on the jemalloc build from t11.

Public method names, signatures, and return types stay the same, except that `cursor()` returns the native `AsyncDuckDBCursor`. `_sync.py`, `pipeline.py`, and `config.py` are not edited. This task is the only one that edits `core.py`, and in `driver.py` it removes the `_AsyncDuckDBCursor` import and export that t09 left.

## Bounded Read Manifest

- `duckdb/_async.py:1-36` — imports and `__all__`.
- `duckdb/_async.py:109-118` — `connect` and `cursor`.
- `duckdb/_async.py:195-298` — `refresh_secrets`, `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select_with_total`.
- `duckdb/_async.py:299-460` — `select_to_arrow`, `stream_arrow`, `stream_arrow_batches`, `load_from_arrow`.
- `duckdb/_async.py:475-592` — `_refresh_dynamic_secrets_async` and `_AsyncDuckDBCursor`.
- `src/py/vertebra/db/adapters/duckdb/_sync.py` — the `_on_native_error` method t09 adds to `SyncDuckDBDriver` is the handler to mirror.
- `duckdb/core.py:1-135` — module constants, `is_write_statement`, `normalize_param`, `normalize_parameters`, `param_len`, `invalidate_rejected_credentials`, `_scan_sql_keywords_outside_literals`.
- `duckdb/driver.py` — re-exports.
- `test_duckdb_driver.py:190-226`, `391-496`, `498-550`, `672-765`, `796-832` — the async halves of the tests t09 changed, and the `is_write_statement` assertions.
- `tools/benchmarks/adapters/sql.py:353-440` — DuckDB capture.

## Symbol & Signature Contracts

New private method on `AsyncDuckDBDriver`:

```python
async def _on_native_error(self, exc: BaseException) -> None:
    """Invalidate rejected dynamic secrets and refresh them before the error propagates."""
```

Same rules as the sync handler in t09, with `await self._refresh_dynamic_secrets_async(force=True)`.

Query methods:

| Method | New body |
| --- | --- |
| `cursor(*, lazy_begin=False)` | `return self._require_client().cursor(lazy_begin=lazy_begin)`; return annotation `AsyncDuckDBCursor`, imported under `TYPE_CHECKING` from `vertebra._duckdb` |
| `execute_with_description` | `cur = self._require_client().cursor()`; inside `try`: `await cur.execute(sql, parameters)`; `desc = cur.description`; return `([], ())` when `desc is None`, else `(await cur.fetchall(), tuple([col[0] for col in desc]))`; `except Exception as exc`: `await self._on_native_error(exc)` then `raise` |
| `execute` | same shape; return `cur.rowcount` when positive, else `0` |
| `execute_many` | return `0` for an empty sequence; otherwise `await cur.executemany(sql, parameter_sets)` and return `cur.rowcount` |
| `execute_tuples` | deleted from this driver; the base class version does the same work |
| `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow` | keep their native calls and pass `parameters` through unchanged; remove the per-query secret refresh, `DuckDBCore.normalize_parameters`, `DuckDBCore.param_len`, `_lookup_or_cache_statement`, and both lease calls; each existing `except Exception as exc` block awaits `self._on_native_error(exc)` |

Deleted from `_async.py` and `driver.py`: the `_AsyncDuckDBCursor` class, its `__all__` entries, its import, every per-query secret refresh call, `finally` blocks that only released a lease, and imports left unused.

Deleted from `core.py`, after confirming with the removal check below that no caller remains: `DuckDBCore.is_write_statement`, `DuckDBCore.normalize_param`, `DuckDBCore.normalize_parameters`, `DuckDBCore.param_len`, `DuckDBCore._scan_sql_keywords_outside_literals`, the `_MUTATING_SQL_VERBS` constant, and imports left unused. `invalidate_rejected_credentials`, `probe_provider_generation`, `probe_provider_needs_refresh`, and `_CREDENTIAL_REJECTION_PATTERN` stay.

## Edge Cases & Error Semantics

- All edge cases of t09 apply, with awaited calls.
- The secret refresh at open stays where it runs today, in `connect` or first use, with `force=True`.
- A synchronous provider and an awaitable provider are both handled by `_refresh_dynamic_secrets_async`, as today.
- A cancelled task during `cur.execute` propagates `asyncio.CancelledError`. It is a `BaseException`, so the error handler does not run.
- If the removal check finds a caller of a `core.py` helper outside the two driver files, keep that helper, list the caller under Notes & Discoveries, and delete the rest.

## Public-Seam Test Blueprint

Changes to existing tests:

- Lines 190-226: replace `async with async_driver.cursor() as acur:` with `acur = async_driver.cursor()` followed by `try/finally: await acur.close()`.
- Lines 391-496: after the generation change, `await async_driver.refresh_secrets(force=False)` before asserting the applied generation.
- Lines 498-550 and 827-832: delete the `DuckDBCore.is_write_statement` assertions. Rename the test functions if their names mention write detection.
- Lines 672-765: for the async driver, assert the provider was fetched again before the next query runs.
- Lines 796-826: remove the `_AsyncDuckDBCursor` row from the export parity table.

New tests, `pytest.mark.anyio`:

| Case | Steps | Expected |
| --- | --- | --- |
| Native cursor returned | `type(async_driver.cursor()).__name__` | `"AsyncDuckDBCursor"` |
| No lease | driver built with a `LeaseBroker`; create, insert, select | `broker.stats().total_acquired == 0` |
| No secret check per query | counting provider; ten selects | provider called once |
| Rejection refreshes | one native call raises an error containing `HTTP 401` | provider invalidated and fetched again; original error raised |
| Other errors do not refresh | syntax error | provider call count unchanged |
| No result set | `execute_with_description("CREATE TABLE ...")` | `([], ())` |
| Removed names | `hasattr(DuckDBCore, name)` for the five deleted helpers; `hasattr(duckdb_async_mod, "_AsyncDuckDBCursor")` | all `False` |

## Verification

- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q`
- Engine suite: `make duckdb-test`
- Removal check, must print nothing: `git grep -n "_SyncDuckDBCursor\|_AsyncDuckDBCursor\|is_write_statement\|normalize_parameters\|normalize_param\b\|param_len\|_scan_sql_keywords_outside_literals\|_MUTATING_SQL_VERBS" -- src/py/vertebra/db/adapters/duckdb src/py/vertebra/extensions tools docs`
- Lease check, must print nothing: `git grep -n "_lookup_or_cache_statement\|_acquire_broker_lease\|_release_broker_lease" -- src/py/vertebra/db/adapters/duckdb`
- Lint and types: `uv run --no-sync ruff check src/py/vertebra/db/adapters/duckdb src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/duckdb && uv run --no-sync pyright src/py/vertebra/db/adapters/duckdb`
- Benchmark sample, on release builds:
  - `make build`
  - `PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine duckdb --dsn ":memory:" --iterations 2000 --warmup 200 --output "$(mktemp -d)/embedded-duckdb.json"`
  - Run the capture three times and keep the median per case. Each report goes to a temporary directory outside the repository.
- Chapter check: run the command list in chapter spec section 5.

## Acceptance Criteria

- [ ] Every blueprint row passes and `make duckdb-test` passes.
- [ ] Both removal checks print nothing, or the kept helper and its caller are listed under Notes & Discoveries.
- [ ] `AsyncDuckDBDriver` query methods make no provider call, no lease call, and no statement-cache call on the success path.
- [ ] The DuckDB benchmark medians are recorded under Notes & Discoveries, next to the figures t11 recorded. The target is at or below the PyPI `duckdb` comparator on Linux. A miss is recorded with both medians and does not block the task; `cleanup-and-checks` enforces the row.
- [ ] The chapter check passes.

## Notes & Discoveries

### Benchmark Medians (Release Build, `bundled` Allocator — t11 Deferred)

Three captures of `tools.benchmarks.adapters.sql capture --engine duckdb --dsn ":memory:" --iterations 2000 --warmup 200` on the release `vertebra._duckdb` build (t11 was deferred by user decision on 2026-11-20, so `vertebra._duckdb` uses the glibc system allocator while PyPI `duckdb` links jemalloc):

| Case | `vertebra-duckdb-sync` | `vertebra-duckdb-async` | `duckdb` (PyPI comparator) | Starting baseline (t11 spec) |
| --- | --- | --- | --- | --- |
| `point_select_1` | `377.71 us` (runs: `380.78`, `376.96`, `377.71`) | `501.33 us` (runs: `484.0`, `501.33`, `503.62`) | `255.00 us` (runs: `253.31`, `255.72`, `255.0`) | sync `426 us` vs comparator `245 us` |
| `analytical_scan_1000` | `540.90 us` (runs: `538.83`, `540.9`, `547.63`) | `661.02 us` (runs: `661.02`, `651.99`, `666.8`) | `451.53 us` (runs: `434.9`, `452.12`, `451.53`) | — |

Miss against the PyPI comparator is expected with t11 deferred (allocator difference) and is left for `cleanup-and-checks` allow-listing.

### Kept Helper from Removal Check

- `DuckDBCore.normalize_param` was kept in `src/py/vertebra/db/adapters/duckdb/core.py` because `load_from_arrow` still calls it for row-sequence ingestion in both `src/py/vertebra/db/adapters/duckdb/_sync.py:385` (not owned by t10) and `src/py/vertebra/db/adapters/duckdb/_async.py:401`.
- All other targets (`_SyncDuckDBCursor`, `_AsyncDuckDBCursor`, `is_write_statement`, `normalize_parameters`, `param_len`, `_scan_sql_keywords_outside_literals`, `_MUTATING_SQL_VERBS`) and all lease/statement-cache calls (`_lookup_or_cache_statement`, `_acquire_broker_lease`, `_release_broker_lease`) were removed with zero remaining matches.

### Integration Test Seed Update & Lint Caveat

- `src/py/tests/integration/db/test_duckdb_native.py` (executed by `make duckdb-test` and `make db-conformance ENGINE=duckdb`, not listed in t09/t10 `tests:`) had fault-injecting client doubles without `cursor()` and post-401/403 assertions expecting deferred next-query secret refresh instead of immediate refresh in `_on_native_error` / explicit `refresh_secrets(force=False)`. Updated those test doubles and assertions so `make duckdb-test` (198 passed, 1 skipped) and `make db-conformance ENGINE=duckdb` (43 passed) pass.
- `make lint` passed Biome, `tsc`, Ruff, Rust fmt/Clippy, and Mypy (850 files), and reported 1 pre-existing environment Pyright error in unrelated `src/py/vertebra/_typing.py:360` (`Import "numpy" could not be resolved`). Scoped `pyright src/py/vertebra/db/adapters/duckdb` passed with `0 errors, 0 warnings`.
