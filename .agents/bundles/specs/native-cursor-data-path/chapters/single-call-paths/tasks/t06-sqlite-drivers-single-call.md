---
type: Task
id: "native-cursor-data-path:single:t06"
chapter_id: single-call-paths
title: "SQLite native drivers on the single-call methods"
description: "Move the query methods of SyncSQLiteDriver and AsyncSQLiteDriver onto the client's single-call methods through callables bound at connect, read lastrowid from the client, and record the SQLite adapter matrix sample."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t05"
read_set:
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:1-57"
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:79-202"
  - "src/py/vertebra/db/adapters/sqlite/_async.py:1-57"
  - "src/py/vertebra/db/adapters/sqlite/_async.py:83-207"
  - "src/py/vertebra/db/driver/_sync.py:101-167"
  - "src/py/vertebra/db/driver/_common.py:141-148"
  - "src/py/vertebra/db/arrow.py:651-680"
  - "src/py/tests/unit/db/test_sqlite_driver.py:1-21"
  - "src/py/tests/unit/db/test_sqlite_driver.py:249-335"
  - "tools/benchmarks/adapters/sql.py:60-70"
  - "tools/benchmarks/adapters/sql.py:1459-1491"
files:
  - src/py/vertebra/db/adapters/sqlite/_sync.py
  - src/py/vertebra/db/adapters/sqlite/_async.py
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 280
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver
    title: "SQLite native driver query methods"
  - id: driver-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: "Native driver select family built on execute_with_description"
  - id: bench-adapters
    resource: tools/benchmarks/adapters/sql.py
    title: "Adapter benchmark matrix with capture and compare commands"
tags: [sqlite, native-driver, single-call, benchmark]
---

# SQLite native drivers on the single-call methods

## Objective & Scope Boundary

Each query method of `SyncSQLiteDriver` and `AsyncSQLiteDriver` makes one native call, or awaits one awaitable. The method body is one Python frame: it calls a callable bound to the client and returns the result.

Method names, signatures, and return values stay the same. `cursor()`, `execute_many`, the Arrow methods, the pipeline, and the transaction methods are not changed. `_require_client()` stays for them.

`driver.lastrowid` keeps its values. It reads the client's `last_statement_rowid` in place of a driver attribute.

The task ends with the SQLite adapter matrix sample.

## Bounded Read Manifest

- `sqlite/_sync.py:1-57` — imports, slots, constructor.
- `sqlite/_sync.py:79-202` — `_require_client`, `lastrowid`, `connect`, `cursor`, `close`, and the query methods.
- `sqlite/_async.py:1-57`, `83-207` — the async twins.
- `db/driver/_sync.py:101-167` — the base `select` family that the overrides replace for SQLite.
- `db/driver/_common.py:141-148` — `hydrate_rows`.
- `db/arrow.py:651-680` — `_hydrate_rows_with_schema`: `None` and `dict` both mean plain dictionaries.
- `test_sqlite_driver.py:1-21`, `249-335` — imports, the inheritance test this task deletes, and the two driver result tests.
- `tools/benchmarks/adapters/sql.py:60-70`, `1459-1491` — driver labels with their comparators, and the capture flags.

## Symbol & Signature Contracts

### Module function, in `sqlite/_sync.py`

```python
def _raise_closed(*args: Any, **kwargs: Any) -> NoReturn:
    """Raise the closed-driver error in place of a native call."""
    msg = "SQLite driver is closed"
    raise InterfaceError(msg)
```

`sqlite/_async.py` imports it from `vertebra.db.adapters.sqlite._sync`, next to its existing import of `SyncSQLiteDriver`.

### Slots and binding, both drivers

`__slots__ = ("_client", "_fetch_dicts", "_fetch_rows", "_fetch_value", "_run_dml")`. The `_lastrowid` slot and every assignment to it are deleted.

```python
def _bind_client(self, client: SyncSQLiteClient | None) -> None:
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

The async driver has the same method typed with `AsyncSQLiteClient`. The four slots are annotated `Callable[..., Any]` in the class body. `Callable` joins the file's existing `collections.abc` import.

| Site | New body |
| --- | --- |
| `__init__` | after `super().__init__`, `self._bind_client(_client if _client is not None else self._create_client())` |
| `connect` | when `self._client is None or self._closed`: `self._bind_client(self._create_client())`, then `self._closed = False` |
| `close` | `self._closed = True`; `client = self._client`; `self._bind_client(None)`; close `client` when it is not `None` (awaited in the async driver) |
| `lastrowid` property | `client = self._client`; return `client.last_statement_rowid` when `client` is not `None`, otherwise `None` |

### Query methods, sync driver

Signatures are the ones in the driver base. Each body starts with `_ = kwargs`.

```python
def select(self, sql, parameters=None, *, schema_type=None, **kwargs) -> list[Any]:
    if schema_type is None or schema_type is dict:
        return self._fetch_dicts(sql, parameters)
    rows, cols = self._fetch_rows(sql, parameters)
    return self.hydrate_rows(rows, cols, schema_type)

def select_one(self, sql, parameters=None, *, schema_type=None, **kwargs) -> Any:
    if schema_type is None or schema_type is dict:
        rows = self._fetch_dicts(sql, parameters)
    else:
        raw, cols = self._fetch_rows(sql, parameters)
        rows = self.hydrate_rows(raw, cols, schema_type)
    if not rows:
        msg = "Query returned no rows"
        raise NotFoundError(msg)
    return rows[0]

def select_one_or_none(self, sql, parameters=None, *, schema_type=None, **kwargs) -> Any | None:
    (same row fetch as select_one)
    return rows[0] if rows else None

def select_value(self, sql, parameters=None, *, value_type=None, **kwargs) -> Any:
    val = self._fetch_value(sql, parameters)
    if value_type is not None and val is not None:
        return to_value_type(val, value_type)
    return val

def execute_with_description(self, sql, parameters=None, **kwargs) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    return self._fetch_rows(sql, parameters)

def execute_tuples(self, sql, parameters=None, **kwargs) -> list[tuple[Any, ...]]:
    return self._fetch_rows(sql, parameters)[0]

def execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, int | None]:
    return self._run_dml(sql, parameters)

def execute(self, sql, parameters=None, **kwargs) -> int:
    return self._run_dml(sql, parameters)[0]
```

Write each signature in full with the annotations the base class uses, and a one-line Google-style docstring. `select_one` and `select_one_or_none` repeat the row fetch inline; they do not call `select`.

The async driver has the same eight methods as `async def`, with `await` on each bound call:

```python
async def select(self, sql, parameters=None, *, schema_type=None, **kwargs) -> list[Any]:
    if schema_type is None or schema_type is dict:
        return await self._fetch_dicts(sql, parameters)
    rows, cols = await self._fetch_rows(sql, parameters)
    return self.hydrate_rows(rows, cols, schema_type)
```

New top-level imports in both files: `NotFoundError` from `vertebra._vertebra`, `to_value_type` from `vertebra.utils.schema`, `Callable` from `collections.abc`, and `NoReturn` from `typing` in `_sync.py`.

## Edge Cases & Error Semantics

- A closed driver: `close()` binds `_raise_closed`, so every query method raises `InterfaceError` with `SQLite driver is closed`. No query method reads `_closed` or calls `_require_client()`. In the async driver the error is raised when the method is awaited.
- `connect()` after `close()` binds the new client. Query methods work again, and `lastrowid` is `None`.
- A driver built with `_client=` binds that object's methods. A test double must expose `fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`, and `last_statement_rowid`.
- `lastrowid`: the rowid after an insert that changed rows, through `execute`, `execute_dml`, or a row method with a returning clause. `None` after any other successful statement, after `close()`, and on a new driver. `execute_many` and `cursor()` statements do not change it. A failed statement leaves it as it was.
- Thread rule for `lastrowid`: it is the last statement run through the driver's client by any thread, the rule the driver attribute had.
- A statement with no result set: `select` returns `[]`, `execute_with_description` returns `([], ())`, `select_one` and `select_value` raise `NotFoundError`, and `select_one_or_none` returns `None`.
- `execute` on a read returns `0`.
- `select_value` on an empty result raises the `NotFoundError` the extension builds. It is the `vertebra._vertebra` class with the message `Query returned no rows`.
- `schema_type=dict` gives the same plain dictionaries as `None`.
- Errors are the classes the cursor path raised. Existing error assertions pass unchanged.

## Public-Seam Test Blueprint

Delete `test_sqlite_driver_inherits_base_read_helpers`. It asserts that the drivers do not define `select` and `execute_tuples`, and they now do.

Add a counting double to the test file:

```python
class _CountingClient:
    """Forward to a real client and record the name of each method call."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)
        if not callable(attr):
            return attr

        def counted(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return attr(*args, **kwargs)

        return counted
```

| Case | Steps | Expected |
| --- | --- | --- |
| One native call, sync | driver built with `_client=_CountingClient(SyncSQLiteClient(":memory:"))`; clear `calls` before each of `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, `execute_dml` | `calls` is `["fetch_dicts"]`, `["fetch_dicts"]`, `["fetch_dicts"]`, `["fetch_value"]`, `["fetch_rows"]`, `["fetch_rows"]`, `["execute_dml"]`, `["execute_dml"]` |
| One native call with a schema, sync | `select`, `select_one`, `select_one_or_none` with a `msgspec.Struct` `schema_type` | `["fetch_rows"]` each; results are instances of the struct |
| One native call, async | the same two rows through `AsyncSQLiteDriver` and `AsyncSQLiteClient` | the same call lists |
| Results | existing `test_sync_sqlite_driver_cursor_backed_query_results` and its async twin | pass unchanged, including the `lastrowid` assertions |
| `schema_type=dict` | `select("SELECT 1 AS a", schema_type=dict)` | `[{"a": 1}]` |
| `select_value` type | `select_value("SELECT 1", value_type=str)` | `"1"` |
| Not found | `select_one` and `select_value` with a false predicate; `select_one_or_none` | `NotFoundError` twice; `None` |
| Closed, sync | `close()`, then each of the eight methods | `InterfaceError` matching `SQLite driver is closed` |
| Closed, async | the same, awaited | the same error on await |
| Reconnect | `close()`, `connect()`, `select_value("SELECT 1")`, `lastrowid` | `1`; `None` |
| `lastrowid` through the client | insert through `execute`; read `driver.lastrowid`; `execute_many` of two rows; read again | the rowid; the same rowid |
| Slots | `hasattr(driver, "_lastrowid")` on both drivers | `False` |
| Threads | existing `test_sync_sqlite_driver_serializes_concurrent_multithreaded_operations` | passes unchanged |
| No lease | existing `test_sync_sqlite_driver_queries_take_no_lease_or_statement_cache_entry` and its async twin | pass unchanged |

## Verification

- Rebuild: `make dev`
- Red then green: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q`
- Engine suite: `make sqlite-test`
- Neighbors that use the SQLite driver: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_dbapi_standalone.py -q -k sqlite`
- Lint and types: `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/sqlite src/py/tests/unit/db/test_sqlite_driver.py && uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/sqlite && uv run --no-sync --frozen pyright src/py/vertebra/db/adapters/sqlite`
- Removed attribute, must print nothing: `git grep -n "_lastrowid" -- src/py/vertebra/db/adapters/sqlite`
- Benchmark sample, on release builds:
  - `make build`
  - `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn ":memory:" --iterations 2000 --warmup 200 --output "$(mktemp -d)/single-call-sqlite.json"`
  - Run the capture three times and keep the median per case. Each report goes to a temporary directory outside the repository. Nothing is written under `tools/benchmarks/results/`.

## Acceptance Criteria

- [x] Every row of the blueprint passes, and `make sqlite-test` passes.
- [x] Each of the eight query methods on both drivers makes one native call.
- [x] No query method calls `_require_client()` or reads `_closed`.
- [x] The removed-attribute check prints nothing.
- [x] `ruff`, `mypy`, and `pyright` report nothing for the SQLite adapter package.
- [x] The benchmark sample is recorded under Notes & Discoveries: the medians of `vertebra-sqlite-sync` against `sqlite3`, and `vertebra-sqlite-async` against `aiosqlite`, for each captured case.
- [x] When `vertebra-sqlite-sync` is above `sqlite3` on the single-row select case, the numbers are recorded and the miss is reported to the coordinator. The task still completes; t27 and the comparison check decide the row.

## Notes & Discoveries

- SQLite adapter benchmark sample (2026-11-21, release builds of `vertebra-core` and `vertebra-sqlite`, 3 captures x 2000 iterations, 200 warmup, `--dsn ":memory:"`, JSON reports written to `mktemp -d`):
  - `point_select_1`: `vertebra-sqlite-sync` median `0.000934 ms` (runs: `0.000927`, `0.000934`, `0.000959`) vs `sqlite3` median `0.001558 ms` (`0.001541`, `0.001602`, `0.001558`) — ratio `0.599x` (`vertebra-sqlite-sync` is faster than `sqlite3`).
  - `point_select_1`: `vertebra-sqlite-async` median `0.042792 ms` (runs: `0.047536`, `0.041400`, `0.042792`) vs `aiosqlite` median `0.084696 ms` (`0.088201`, `0.083896`, `0.084696`) — ratio `0.505x`.
  - `multi_row_select_10`: `vertebra-sqlite-sync` median `0.004812 ms` (runs: `0.004793`, `0.004901`, `0.004812`) vs `sqlite3` median `0.013220 ms` (`0.012819`, `0.013381`, `0.013220`) — ratio `0.364x`.
  - `multi_row_select_10`: `vertebra-sqlite-async` median `0.047608 ms` (runs: `0.053082`, `0.047160`, `0.047608`) vs `aiosqlite` median `0.100213 ms` (`0.100213`, `0.102531`, `0.098201`) — ratio `0.475x`.
  - `cursor_point_select_1`: `vertebra-sqlite-sync` median `0.000736 ms` (runs: `0.000721`, `0.000736`, `0.000750`) vs `sqlite3` median `0.001331 ms` (`0.001345`, `0.001331`, `0.001309`) — ratio `0.553x`.
- Typing deviation from `Callable[..., Any]` slot annotation: annotated `_fetch_dicts`, `_fetch_rows`, and `_run_dml` with their concrete return types (and `Awaitable[...]` in `_async.py`) instead of `Callable[..., Any]` so `mypy` (`warn_return_any`) passes with zero errors without adding runtime `cast(...)` frames on the hot path. `_raise_closed -> NoReturn` remains assignable to all four slots.
- Release build scope: built release `_vertebra` and `_sqlite` via `maturin develop --uv -r` rather than full `make build` across all 13 extensions and TS packages; `UV_CACHE_DIR=/tmp/uv-cache` was required because `~/.cache/uv` is read-only inside the sandbox.
