---
type: Task
id: "native-cursor-data-path:embedded:t09"
chapter_id: embedded-cursors
title: "DuckDB sync driver calls the cursor"
description: "Move SyncDuckDBDriver onto the native cursor, delete the Python _SyncDuckDBCursor, and move secret refresh off the query path."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t08"
read_set:
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:1-36"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:113-118"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:192-294"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:295-455"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:463-579"
  - "src/py/vertebra/db/adapters/duckdb/core.py:91-113"
  - "src/py/vertebra/db/adapters/duckdb/driver.py"
  - "src/py/vertebra/db/driver/_sync.py:91-120"
  - "src/py/tests/unit/db/test_duckdb_driver.py:190-226"
  - "src/py/tests/unit/db/test_duckdb_driver.py:391-496"
  - "src/py/tests/unit/db/test_duckdb_driver.py:672-765"
  - "src/py/tests/unit/db/test_duckdb_driver.py:796-826"
files:
  - src/py/vertebra/db/adapters/duckdb/_sync.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 230
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver
    title: "DuckDB native driver query methods, secret refresh, and Python cursor"
tags: [duckdb, native-driver, cleanup, secrets]
---

# DuckDB sync driver calls the cursor

## Objective & Scope Boundary

A query through `SyncDuckDBDriver` makes one native `execute` call on a native cursor. The driver no longer normalizes parameters, checks secrets, looks up the statement cache, or takes a lease per query. `driver.cursor()` returns the native `SyncDuckDBCursor`.

Public method names, signatures, and return types stay the same, except that `cursor()` returns the native class. `core.py` and `_async.py` are not edited here. t10 handles them. In `driver.py` this task removes the `_SyncDuckDBCursor` import and export only. t10 removes the async ones. `pipeline.py` and `config.py` are not edited.

## Bounded Read Manifest

- `duckdb/_sync.py:1-36` — imports and `__all__`.
- `duckdb/_sync.py:113-118` — `cursor`.
- `duckdb/_sync.py:192-294` — `refresh_secrets`, `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select_with_total`.
- `duckdb/_sync.py:295-455` — `select_to_arrow`, `stream_arrow`, `stream_arrow_batches`, `load_from_arrow`.
- `duckdb/_sync.py:463-579` — `_refresh_dynamic_secrets` and `_SyncDuckDBCursor`.
- `duckdb/core.py:91-113` — `invalidate_rejected_credentials`.
- `duckdb/driver.py` — re-exports.
- `db/driver/_sync.py:91-120` — base `execute_tuples` and `select`.
- `test_duckdb_driver.py:190-226`, `391-496`, `672-765`, `796-826` — the cursor usage, the two secret tests, and the export parity table.

## Symbol & Signature Contracts

New private method:

```python
def _on_native_error(self, exc: BaseException) -> None:
    """Invalidate rejected dynamic secrets and refresh them before the error propagates."""
```

- Returns at once when `self._dynamic_secrets` is empty.
- Reads `len(self._secret_generations)`, calls `DuckDBCore.invalidate_rejected_credentials(exc, self._dynamic_secrets, self._secret_generations)`, and returns when the length did not shrink.
- Otherwise calls `self._refresh_dynamic_secrets(force=True)` inside `try/except Exception as refresh_exc` and, on failure, calls `exc.add_note(f"DuckDB secret refresh after credential rejection failed: {refresh_exc}")`.
- Never raises.

Query methods:

| Method | New body |
| --- | --- |
| `cursor(*, lazy_begin=False)` | `return self._require_client().cursor(lazy_begin=lazy_begin)`; return annotation `SyncDuckDBCursor`, imported under `TYPE_CHECKING` from `vertebra._duckdb` |
| `execute_with_description(sql, parameters=None, **kwargs)` | `cur = self._require_client().cursor()`; inside `try`: `cur.execute(sql, parameters)`; `desc = cur.description`; return `([], ())` when `desc is None`, else `(cur.fetchall(), tuple([col[0] for col in desc]))`; `except Exception as exc`: `self._on_native_error(exc)` then `raise` |
| `execute(sql, parameters=None, **kwargs)` | same `try/except` shape; `cur.execute`; return `cur.rowcount` when positive, else `0` |
| `execute_many(sql, parameter_sets, **kwargs)` | return `0` for an empty sequence; otherwise `cur.executemany(sql, parameter_sets)` and return `cur.rowcount` |
| `execute_tuples` | deleted from this driver; the base class version does the same work |
| `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow` | keep their native calls and pass `parameters` through unchanged; remove `_refresh_dynamic_secrets()`, `DuckDBCore.normalize_parameters`, `DuckDBCore.param_len`, `_lookup_or_cache_statement`, `_acquire_broker_lease`, and `_release_broker_lease`; each existing `except Exception as exc` block calls `self._on_native_error(exc)` in place of `DuckDBCore.invalidate_rejected_credentials(...)` |

Deleted:

- The `_SyncDuckDBCursor` class, its `__all__` entry in `_sync.py`, and its import and `__all__` entry in `driver.py`.
- Every per-query `self._refresh_dynamic_secrets()` call. The constructor call with `force=True` and `refresh_secrets` stay.
- `finally` blocks that only released a lease.
- Imports left unused by the above.

## Edge Cases & Error Semantics

- Parameters reach the cursor exactly as the caller passed them. `Decimal`, date and time types, `UUID`, `dict`, `list`, and `tuple` values are converted in Rust.
- A statement with no result set passed to `execute_with_description` or `select` returns no rows and no columns.
- A credential rejection (HTTP 401 or 403 text in the error) invalidates the tracked generation, refreshes all dynamic secrets once, and re-raises the original error. The next query uses the new secret.
- An error that is not a credential rejection triggers no refresh and no provider call.
- A refresh failure on the error path adds a note to the original error and does not replace it.
- A provider whose generation changes without an error is picked up by `refresh_secrets(force=False)`, not by the next query.
- Errors from cursor-backed methods are the `vertebra._vertebra` classes the driver raises today, with the same specific subclasses, and carry `sqlstate`, `vendor_code`, and `is_transient`.
- The `broker` constructor argument stays accepted. No query method uses it.

## Public-Seam Test Blueprint

Changes to existing tests:

- Lines 190-226: replace `with sync_driver.cursor() as cur:` with `cur = sync_driver.cursor()` followed by `try/finally: cur.close()`. The async half of that test is changed in t10.
- Lines 391-496: after `token_state["generation"] = 2`, call `sync_driver.refresh_secrets(force=False)` before asserting the applied generation. Leave the async half for t10.
- Lines 672-765: keep the assertions on `invalidated` and `generation`. For the sync driver, assert the provider was fetched again before the next query runs.
- Lines 796-826: remove the `_SyncDuckDBCursor` row from the export parity table.

New tests:

| Case | Steps | Expected |
| --- | --- | --- |
| Native cursor returned | `type(sync_driver.cursor()).__name__` | `"SyncDuckDBCursor"` |
| No lease | driver built with a `LeaseBroker`; create, insert, select | `broker.stats().total_acquired == 0`; statement cache shows zero inserts |
| No secret check per query | dynamic secret provider with a call counter; ten selects | provider called once, at open |
| Rejection refreshes | patch the client so one call raises an error containing `HTTP 403` | provider `invalidate` called; provider fetched a second time; the original error is raised |
| Other errors do not refresh | failing select with a syntax error | provider call count unchanged |
| Refresh failure noted | rejection while the provider raises | original error raised with a note that names the refresh failure |
| Rich binds | `execute` with `Decimal`, `datetime`, `UUID`, `dict` | stored values equal to the pre-change results asserted elsewhere in the file |
| `execute` on a select | `execute("SELECT 1")` | `0` |
| Removed name | `hasattr(duckdb_sync_mod, "_SyncDuckDBCursor")` | `False` |

## Verification

- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q`
- Removal check, must print nothing: `git grep -n "_SyncDuckDBCursor\|_lookup_or_cache_statement\|_acquire_broker_lease\|_release_broker_lease\|normalize_parameters\|param_len\|is_write_statement" -- src/py/vertebra/db/adapters/duckdb/_sync.py src/py/vertebra/db/adapters/duckdb/driver.py`
- Lint and types: `uv run --no-sync ruff check src/py/vertebra/db/adapters/duckdb src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/duckdb && uv run --no-sync pyright src/py/vertebra/db/adapters/duckdb`

## Acceptance Criteria

- [ ] Every blueprint row passes and the rest of `test_duckdb_driver.py` passes.
- [ ] The removal check prints nothing.
- [ ] `SyncDuckDBDriver` query methods make no provider call, no lease call, and no statement-cache call on the success path.
- [ ] `git diff --stat` shows only the three files above.

## Notes & Discoveries

- The declared checks pass: `test_duckdb_driver.py` (127 passed), the removal check, ruff, mypy, and pyright.
- The test proxy `_FaultInjectingSyncClient` gained a `cursor()` that returns a fault-injecting cursor wrapper, because the driver no longer calls `client.execute_with_description`. Its unused `execute_with_description` override is removed.
- The rotation test asserts the applied generation stays `1` after a plain query and becomes `2` after `refresh_secrets(force=False)`.
- Six tests outside the write set fail because they assert per-query leases, statement caching, per-query secret rotation, or use a client proxy with no `cursor()`: three DuckDB cases in `src/py/tests/unit/storage/test_gcs_token_refresh.py`, two in `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (`test_sqlalchemy_dbapi_broker_forwarding[duckdb+vtb:///:memory:]`, `test_sync_duckdb_driver_uses_real_lease_broker_and_releases_on_error`), and `src/py/tests/integration/db/test_duckdb_native.py::test_duckdb_loopback_gcs_bearer_secret_rotation_on_auth_rejection[asyncio-sync-401]`. No task in this chapter lists those files. They need a follow-up task or a refine.
- Whether those six passed before this change was not measured; the failure messages point at the removed lease, cache, and per-query refresh calls.
- No red run was captured before the driver edit.
- `make lint` exits 2 at the Pyright step on `src/py/vertebra/_typing.py:360` (`Import "numpy" could not be resolved`), a file this task does not touch. Biome, Ruff, and Mypy (850 files) passed before it; the steps after Pyright did not run. `make lint` changed no files.
- `uv` needed `UV_CACHE_DIR` pointed at a writable directory in the worker sandbox.
- Recorded in `knowledge/domains/data/database-driver-limitations.md` under "DuckDB Sync Driver on the Native Cursor".
