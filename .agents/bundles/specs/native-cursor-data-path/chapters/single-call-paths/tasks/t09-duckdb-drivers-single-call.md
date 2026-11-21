---
type: Task
id: "native-cursor-data-path:single:t09"
chapter_id: single-call-paths
title: "DuckDB native drivers on the single-call methods"
description: "Move the query methods of SyncDuckDBDriver and AsyncDuckDBDriver onto the client's single-call methods through callables bound when the client is set, keep the error-path secret refresh around each call, and record the DuckDB adapter matrix sample."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t08"
  - "native-cursor-data-path:cleanup:t16"
read_set:
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:1-116"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:181-257"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py:399-417"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:1-115"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:184-266"
  - "src/py/vertebra/db/adapters/duckdb/_async.py:414-442"
  - "src/py/vertebra/db/adapters/duckdb/core.py:20-60"
  - "src/py/vertebra/db/driver/_sync.py:101-167"
  - "src/py/vertebra/db/arrow.py:651-680"
  - "src/py/tests/unit/db/test_duckdb_driver.py:631-711"
  - "src/py/tests/unit/db/test_duckdb_driver.py:1553-1655"
  - "src/py/tests/unit/db/test_duckdb_driver.py:1746-1850"
  - "tools/benchmarks/adapters/sql.py:60-92"
  - "tools/benchmarks/adapters/sql.py:1459-1491"
files:
  - src/py/vertebra/db/adapters/duckdb/_sync.py
  - src/py/vertebra/db/adapters/duckdb/_async.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/integration/db/test_duckdb_native.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 290
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver
    title: "DuckDB native driver query methods and secret refresh"
  - id: driver-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: "Native driver select family built on execute_with_description"
  - id: bench-adapters
    resource: tools/benchmarks/adapters/sql.py
    title: "Adapter benchmark matrix with capture and compare commands"
tags: [duckdb, native-driver, single-call, benchmark]
---

# DuckDB native drivers on the single-call methods

## Objective & Scope Boundary

Each query method of `SyncDuckDBDriver` and `AsyncDuckDBDriver` makes one native call, or awaits one awaitable, in one Python frame.

The secret refresh rules in the `embedded-cursors` chapter spec ("DuckDB secret refresh") keep holding. The error-path refresh wraps each single-call method exactly as it wraps the cursor-backed methods today.

Method names, signatures, and return values stay the same. Both drivers gain `execute_dml`. `cursor()`, `execute_many`, `select_with_total`, the Arrow methods, the pipeline, the transaction methods, and `refresh_secrets` are not changed. `_require_client()` stays for them.

The task ends with the DuckDB adapter matrix sample.

## Bounded Read Manifest

- `duckdb/_sync.py:1-116` — imports, slots, constructor, `cursor`.
- `duckdb/_sync.py:181-257` — `close`, `execute`, `execute_many`, `execute_with_description`, `select_with_total`.
- `duckdb/_sync.py:399-417` — `_require_client` and `_on_native_error`.
- `duckdb/_async.py:1-115`, `184-266`, `414-442` — the async twins, the `_initial_secrets_applied` check, the awaited `_on_native_error`, and `_cleanup_failed_startup_client`.
- `duckdb/core.py:20-60` — `invalidate_rejected_credentials`: it acts only on a message that matches an HTTP 401 or 403 rejection.
- `db/driver/_sync.py:101-167` — the base `select` family that the overrides replace for DuckDB.
- `db/arrow.py:651-680` — `_hydrate_rows_with_schema`.
- `test_duckdb_driver.py:631-711` — the four fault-injecting doubles.
- `test_duckdb_driver.py:1553-1655`, `1746-1850` — the sync and async secret tests.
- `tools/benchmarks/adapters/sql.py:60-92`, `1459-1491` — driver labels, the allow-list entry for DuckDB, and the capture flags.

## Symbol & Signature Contracts

### Module function, in `duckdb/_sync.py`

```python
def _raise_closed(*args: Any, **kwargs: Any) -> NoReturn:
    """Raise the closed-driver error in place of a native call."""
    msg = "DuckDB driver is closed"
    raise InterfaceError(msg)
```

`duckdb/_async.py` imports it from `vertebra.db.adapters.duckdb._sync`.

### Slots and binding, both drivers

Add `"_fetch_dicts"`, `"_fetch_rows"`, `"_fetch_value"`, and `"_run_dml"` to `__slots__`, in sorted position. The four slots are annotated `Callable[..., Any]` in the class body. `Callable` joins the file's existing `collections.abc` import.

```python
def _bind_client(self, client: SyncDuckDBClient | None) -> None:
    """Set the client and bind its single-call methods, or the closed-driver error when there is none."""
    self._client = client
    if client is None:
        self._fetch_dicts = self._fetch_rows = self._fetch_value = self._run_dml = _raise_closed
        return
    self._fetch_dicts = client.fetch_dicts
    self._fetch_rows = client.fetch_rows
    self._fetch_value = client.fetch_value
    self._run_dml = client.execute_dml
```

Every assignment to `self._client` becomes a `_bind_client` call:

| Site | Call |
| --- | --- |
| Sync `__init__`, both branches | `self._bind_client(_client)` or `self._bind_client(native_mod.SyncDuckDBClient(...))` |
| Sync `__init__`, failed initial refresh | `client = self._client`; `self._bind_client(None)`; `self._closed = True`; close `client` |
| Sync `close` | `self._closed = True`; `client = self._client`; `self._bind_client(None)`; close `client` |
| Async `__init__`, both branches | as sync, with `AsyncDuckDBClient` |
| Async `close` | as sync, awaited |
| Async `_cleanup_failed_startup_client` | `client = self._client`; `self._bind_client(None)`; `self._closed = True`; await `client.close()` |

### Query methods, sync driver

Each body starts with `_ = kwargs`. Each native call sits inside the existing wrapper:

```python
try:
    ...
except Exception as exc:
    self._on_native_error(exc)
    raise
```

```python
def select(self, sql, parameters=None, *, schema_type=None, **kwargs) -> list[Any]:
    try:
        if schema_type is None or schema_type is dict:
            return self._fetch_dicts(sql, parameters)
        rows, cols = self._fetch_rows(sql, parameters)
    except Exception as exc:
        self._on_native_error(exc)
        raise
    return self.hydrate_rows(rows, cols, schema_type)

def select_one(self, sql, parameters=None, *, schema_type=None, **kwargs) -> Any:
    (the row fetch of select, inline, inside the same wrapper)
    if not rows:
        msg = "Query returned no rows"
        raise NotFoundError(msg)
    return rows[0]

def select_one_or_none(self, sql, parameters=None, *, schema_type=None, **kwargs) -> Any | None:
    (the same row fetch)
    return rows[0] if rows else None

def select_value(self, sql, parameters=None, *, value_type=None, **kwargs) -> Any:
    try:
        val = self._fetch_value(sql, parameters)
    except Exception as exc:
        self._on_native_error(exc)
        raise
    if value_type is not None and val is not None:
        return to_value_type(val, value_type)
    return val

def execute_with_description(self, sql, parameters=None, **kwargs) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    (wrapper around) return self._fetch_rows(sql, parameters)

def execute_tuples(self, sql, parameters=None, **kwargs) -> list[tuple[Any, ...]]:
    (wrapper around) return self._fetch_rows(sql, parameters)[0]

def execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, int | None]:
    (wrapper around) return self._run_dml(sql, parameters)

def execute(self, sql, parameters=None, **kwargs) -> int:
    (wrapper around) return self._run_dml(sql, parameters)[0]
```

Write each signature in full with the annotations the base class uses, and a one-line Google-style docstring. `execute_dml` has the signature of `SyncSQLiteDriver.execute_dml`. The hydration call and the `NotFoundError` raised by `select_one` stay outside the wrapper, so only native errors reach `_on_native_error`.

### Query methods, async driver

The same eight methods as `async def`. Each keeps the existing first step, then awaits the bound call inside the awaited wrapper:

```python
async def select(self, sql, parameters=None, *, schema_type=None, **kwargs) -> list[Any]:
    if not self._initial_secrets_applied:
        await self._refresh_dynamic_secrets_async(force=True)
    try:
        if schema_type is None or schema_type is dict:
            return await self._fetch_dicts(sql, parameters)
        rows, cols = await self._fetch_rows(sql, parameters)
    except Exception as exc:
        await self._on_native_error(exc)
        raise
    return self.hydrate_rows(rows, cols, schema_type)
```

New top-level imports in both files: `NotFoundError` from `vertebra._vertebra`, `to_value_type` from `vertebra.utils.schema`, `Callable` from `collections.abc`, and `NoReturn` from `typing` in `_sync.py`.

## Edge Cases & Error Semantics

- Secret refresh at open, on request, and after a credential rejection is unchanged. No query method checks the clock.
- A native error whose message matches an HTTP 401 or 403 rejection invalidates the tracked generations, refreshes once, adds a note when the refresh fails, and re-raises the original error object. This holds for every single-call driver method.
- Any other native error is re-raised without touching a secret provider.
- A closed driver: `_raise_closed` raises `InterfaceError` with `DuckDB driver is closed` from inside the wrapper. `_on_native_error` receives it and does nothing, because the message is not a credential rejection. The same `InterfaceError` object is re-raised. Today the closed check runs before the wrapper; the result for the caller is the same.
- `connect()` on a closed DuckDB driver does not reopen it, as today. The bound callables stay `_raise_closed`.
- The async driver whose initial required secret refresh fails ends with `_client` `None`, the client closed once, and every query method raising. `_cleanup_failed_startup_client` binds the closed callables.
- A driver built with `_client=` binds that object's methods. A test double must expose `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`.
- A statement with no result set: `select` returns `[]`, `execute_with_description` returns `([], ())`, `select_one` and `select_value` raise `NotFoundError`, and `select_one_or_none` returns `None`.
- `execute` on a read returns `0`. `execute_dml` always returns `None` as its second item.
- `select_with_total` calls `select` and so makes one native call.
- No lease is taken and no statement-cache entry is written, as today.

## Public-Seam Test Blueprint

Update the doubles in the test file first. `_FaultInjectingSyncClient` and `_FaultInjectingAsyncClient` each gain `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`. Each raises `self._query_error` when it is set and forwards to the inner client otherwise. The async forms are `async def` and await the inner call. The existing `execute`, `cursor`, and `close` members stay.

Add the `_CountingClient` double with the same body as the one in `test_sqlite_driver.py`: it forwards every attribute and records the name of each method call.

| Case | Steps | Expected |
| --- | --- | --- |
| One native call, sync | driver built with `_client=_CountingClient(SyncDuckDBClient(":memory:"))`; clear `calls` before each of `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, `execute_dml` | `["fetch_dicts"]` three times, `["fetch_value"]`, `["fetch_rows"]` twice, `["execute_dml"]` twice |
| One native call with a schema | `select`, `select_one`, `select_one_or_none` with a `msgspec.Struct` `schema_type` | `["fetch_rows"]` each; results are instances of the struct |
| One native call, async | the same through `AsyncDuckDBDriver` and `AsyncDuckDBClient` | the same call lists |
| Secrets checked once at open | existing `test_duckdb_sync_driver_checks_dynamic_secrets_once_at_open` and its async twin | pass unchanged |
| Other errors do not refresh | existing `test_duckdb_sync_driver_other_errors_do_not_refresh_secrets` and its async twin | pass unchanged |
| Credential rejection refreshes | existing `test_duckdb_sync_driver_credential_rejection_refreshes_and_reraises_original_error` and its async twin, now reaching the fault through the single-call doubles | pass with no change to their assertions |
| Rejection through each method | sync and async: a 403 injected, then each of `select`, `select_one`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, `execute_dml` | the original error object is raised each time; the provider is invalidated and refreshed once per rejection |
| 401, 403, 404 | existing `test_duckdb_driver_invalidates_only_401_and_403_errors_and_rotates_next_query` | passes unchanged |
| Failed initial refresh | existing `test_duckdb_driver_closes_client_when_initial_required_secret_refresh_fails` | passes unchanged |
| Closed, sync | `close()`, then each of the eight methods, with one dynamic secret configured | `InterfaceError` matching `DuckDB driver is closed`; the provider call count does not change |
| Closed, async | the same, awaited | the same |
| Results | `execute("CREATE TABLE ...")`, `execute_dml` insert of two rows, `execute` on a select, `select_value`, `select(schema_type=dict)` | `0`; `(2, None)`; `0`; the count; plain dictionaries |
| Rich binds | existing `test_duckdb_sync_driver_rich_binds_and_execute_on_select` | passes unchanged |
| No lease | existing `test_duckdb_sync_driver_queries_take_no_lease_and_cache_no_statement` | passes unchanged |

## Verification

- Rebuild: `make dev`
- Red then green: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_duckdb_driver.py -q`
- Engine suite: `make duckdb-test`
- Lint and types: `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/duckdb src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/duckdb && uv run --no-sync --frozen pyright src/py/vertebra/db/adapters/duckdb`
- Per-call closed check gone from the query methods: `git grep -n "_require_client().cursor()" -- src/py/vertebra/db/adapters/duckdb` lists `execute_many` only, in each file.
- Benchmark sample, on release builds:
  - `make build`
  - `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine duckdb --dsn ":memory:" --iterations 2000 --warmup 200 --output "$(mktemp -d)/single-call-duckdb.json"`
  - Run the capture three times and keep the median per case. Each report goes to a temporary directory outside the repository. Nothing is written under `tools/benchmarks/results/`.

## Acceptance Criteria

- [x] Every row of the blueprint passes, and `make duckdb-test` passes.
- [x] Each of the eight query methods on both drivers makes one native call inside the `_on_native_error` wrapper.
- [x] No existing secret test changes its assertions.
- [x] `ruff`, `mypy`, and `pyright` report nothing for the DuckDB adapter package.
- [x] The benchmark sample is recorded under Notes & Discoveries: the medians of `vertebra-duckdb-sync` against `duckdb` for each captured case. The row is on the comparison allow-list, so a miss is recorded and does not block.

## Notes & Discoveries

- Benchmark sample on release builds (`maturin develop --uv -r` for `vertebra-core` and `vertebra-duckdb`; 3 captures of `tools.benchmarks.adapters.sql capture --engine duckdb --dsn ":memory:" --iterations 2000 --warmup 200`, median per case across the 3 runs; outputs written to a temporary directory outside the repo and deleted):
  - `point_select_1`: `vertebra-duckdb-sync` median `0.3712 ms` (runs `0.3728`, `0.3712`, `0.3668`) vs `duckdb` median `0.2584 ms` (runs `0.2706`, `0.2559`, `0.2584`), ratio `1.437x` (allow-listed in `COMPARATOR_ALLOW_LIST`).
  - `analytical_scan_1000`: `vertebra-duckdb-sync` median `0.5400 ms` (runs `0.5520`, `0.5371`, `0.5400`) vs `duckdb` median `0.4572 ms` (runs `0.4572`, `0.4638`, `0.4536`), ratio `1.181x` (allow-listed in `COMPARATOR_ALLOW_LIST`).
- Test-double parity resolved across all three declared test files:
  - Added `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml` to `_SyncFaultInjectingDuckDBClient` and `_AsyncFaultInjectingDuckDBClient` in `src/py/tests/integration/db/test_duckdb_native.py` and to `_SyncClientFaultProxy` and `_AsyncClientFaultProxy` in `src/py/tests/unit/storage/test_gcs_token_refresh.py` without changing any test assertions.
  - `make duckdb-test` passed completely (14 Rust tests, `mypy.stubtest vertebra._duckdb`, and 265 passed / 1 skipped Python tests), `pytest` on `test_duckdb_driver.py` + `test_duckdb_native.py` + `test_gcs_token_refresh.py` passed (253 passed, 1 skipped), and `ruff check`, `ruff format --check`, `mypy`, and `pyright` reported 0 issues across `src/py/vertebra/db/adapters/duckdb` and all three test files.
