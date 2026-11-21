---
type: Task
id: "native-cursor-data-path:single:t17"
chapter_id: single-call-paths
title: "Oracle sync driver on the single-call methods"
description: "SyncOracleDriver query methods make one native single-call per statement, without the generator-based checkout_cursor context manager."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t15"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/research/native-cursor-hot-paths_20261121.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t15-oracle-bind-and-sync-single-call.md"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:1-42"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:212-326"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:556-629"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:745-859"
  - "src/py/vertebra/db/adapters/oracle/core.py:717-723"
  - "src/py/vertebra/db/adapters/oracle/core.py:847-870"
  - "src/py/vertebra/db/driver/_sync.py:122-167"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:266-274"
  - "src/py/tests/unit/db/test_oracle_adapter.py:2336-2450"
files:
  - "src/py/vertebra/db/adapters/oracle/_sync.py"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 130
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
created_at: 2026-11-21T16:23:02Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, requirement R16 and the lease contract"
  - id: oracle-sync-driver
    resource: "src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver"
    title: "Sync Oracle driver query path"
  - id: hot-path-research
    resource: .agents/bundles/research/native-cursor-hot-paths_20261121.md
    title: "Section 6: generator context manager and five native calls per statement"
tags: [oracle, python, driver, performance]
---

# Oracle sync driver on the single-call methods

## Objective & Scope Boundary

`select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, and the new `execute_dml` on `SyncOracleDriver` each make one native statement call. No statement enters the generator-based `checkout_cursor` context manager.

`estimated_loc` counts about 95 lines added and 35 removed in the driver. Tests add or change about 160 lines.

Out of scope:

- `execute_many`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, notifications, queues, and the pipeline.
- `cursor()` and `checkout_cursor()`. Both keep their code and their contracts.
- The `SyncOraclePool` wrapper class in the same file. It gains no method.
- `src/py/vertebra/db/driver/_sync.py`. This task overrides methods on the Oracle driver only.

## Bounded Read Manifest

- Root spec, sections "Single-call methods (R16)" and "Lease contract (R6)". The `network-engines` chapter spec for where the Oracle driver attaches and releases a lease.
- The t15 worksheet for the native signatures, the four-item result of `execute_dml`, and the pool method's checkout and discard rule.
- `_sync.py:556-629` are `_execute_cursor` and the query methods. `:810-838` is the generator `_checkout_client`. `:781-808` are `_acquire_pool_client` and `_release_pool_client`. `:840-859` are the session-attribute and piggyback helpers.
- `core.py:847-870` is `track_session_context_sql`.
- `test_oracle_adapter.py:2336-2450` are the driver tests for out-binds, the cursor contracts, and session-context tracking.

## Contracts & Edge Cases

### Who still calls `checkout_cursor`

After this task and t18, `checkout_cursor` has these callers outside the query methods: `execute_many` in both drivers, the SQLSpec Oracle adapter (`src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`, sync and async `with_cursor`), and the driver tests. It stays a public method, unchanged. The generator `_checkout_client` also stays: `checkout_cursor`, `ping`, `select_to_arrow`, `publish_notification`, and `pipeline.py` call it.

### Routing

| Route | When | Native object called |
| --- | --- | --- |
| Pinned transaction client | `self._get_pinned_lease()` returns a client | that client |
| Single client | no pinned client and `self._pool is None` | `self._client` |
| Pool | a pool, no broker, no pending session attributes | the native pool |
| Checked-out client | a pool with a broker attached, or pending session attributes on any route | a client checked out for this statement, or the pinned or single client after its attributes are applied |

On the first three routes a query method makes exactly one native call. On the last route it applies attributes or checks a client out, makes one native statement call, and returns the client. The lease contract ties the broker lease to a checkout that Python can see, and session attributes travel with the acquire call. Lease counts do not change.

### New state in `__init__`

```python
self._native_pool: Any = pool.native_pool if isinstance(pool, SyncOraclePool) else pool
self._pool_target: Any = None if self._lease_at_checkout else self._native_pool
```

Both lines go after `self._lease_at_checkout` is set. `_pool_target` is `None` for a driver without a pool.

### Helpers

Module level, with `from operator import attrgetter` and `from vertebra.utils.schema import to_value_type` added to the top-level imports:

```python
_FETCH_ROWS = attrgetter("fetch_rows")
_FETCH_DICTS = attrgetter("fetch_dicts")
_FETCH_VALUE = attrgetter("fetch_value")
_EXECUTE_DML = attrgetter("execute_dml")
```

```python
def _single_call(
    self,
    pick: Callable[[Any], Callable[..., Any]],
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    out_binds: Any,
) -> Any:
    """Run one statement through one native single-call method and return its result."""
    if self._pending_session_attrs is None:
        target = self._get_pinned_lease()
        if target is None:
            target = self._client if self._pool is None else self._pool_target
        if target is not None:
            return pick(target)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)
    return self._single_call_checked(pick, sql, parameters, out_binds)
```

`Callable` is added to the `collections.abc` import. `self._cursor_opts` holds the four option keywords the native methods take, with the names `cursor()` uses.

```python
def _single_call_checked(
    self,
    pick: Callable[[Any], Callable[..., Any]],
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    out_binds: Any,
) -> Any:
    """Run one statement on a client after applying session attributes or checking it out of the pool."""
```

Its body, in order:

1. `active = self._get_pinned_lease()`; when that is `None` and `self._pool is None`, `active = self._client`.
2. When `active` is not `None`: `self._apply_pending_session_attrs(active)`, then return `pick(active)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)`, with `self._capture_piggyback(active)` in a `finally`.
3. Otherwise: `lease = self._acquire_pool_client()`; return `pick(lease)(...)`. On `BaseException`, set `discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)` and re-raise. In a `finally`, `self._release_pool_client(lease, discard=discard)`.

This is the body of `_checkout_client` written as plain `try` and `finally` blocks.

`_release_pool_client` gains one line at its start, after the `self._pool is None` return: `self._capture_piggyback(lease)`. Every pooled client then has its piggyback frame recorded when it leaves the driver, including a transaction client whose statements took the direct route.

Piggyback on the direct routes: the pinned and single-client routes do not call `_capture_piggyback`, because `last_piggyback_frame` reads the client directly for them. The direct pool route sends no session attributes, so the recorded frame keeps its value.

### Query methods

`out_binds` is read with `kwargs.get("out_binds")`, as `_execute_cursor` reads it today. Other keyword arguments are ignored, as today.

| Method | Body |
| --- | --- |
| `execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, Any]` | `rowcount, lastrowid, out_values, touches = self._single_call(_EXECUTE_DML, sql, parameters, kwargs.get("out_binds"))`; `self._last_out_binds = out_values`; when `touches`, `OracleCore.track_session_context_sql(self._session_context, sql, parameters)`; `return rowcount, lastrowid` |
| `execute` | `return self.execute_dml(sql, parameters, **kwargs)[0]` |
| `execute_with_description` | `self._last_out_binds = None`; `return self._single_call(_FETCH_ROWS, sql, parameters, kwargs.get("out_binds"))` |
| `execute_tuples` | `self._last_out_binds = None`; `return self._single_call(_FETCH_ROWS, sql, parameters, kwargs.get("out_binds"))[0]` |
| `select` | `self._last_out_binds = None`. With `schema_type is None`: `return self._single_call(_FETCH_DICTS, ...)`. Otherwise `rows, names = self._single_call(_FETCH_ROWS, ...)`; `return _hydrate_rows_with_schema(rows, names, schema_type) if rows else []` |
| `select_value(self, sql, parameters=None, *, value_type=None, **kwargs)` | `self._last_out_binds = None`; `value = self._single_call(_FETCH_VALUE, ...)`; return `to_value_type(value, value_type)` when `value_type is not None and value is not None`, else `value` |
| `select_one`, `select_one_or_none` | Not overridden. The base methods call `self.select` |

- `execute_dml` is new on the driver. Its docstring is `Execute a statement and return its row count and last row id.` It returns two items; the native method returns four.
- Session-context tracking and `out_binds` follow `execute` and `execute_dml`. The four-item native result is the only place the flag and the out-bind values come back from a single call (t15).
- A fetch-route method sets `driver.out_binds` to `None`. Out-bind variables passed in the parameters are still filled in place, and a statement with out-bind values and no rows returns those values as its one row.
- This is a recorded limitation: `driver.out_binds` and session-context tracking follow `execute` and `execute_dml` only. The task result states it so that the `cleanup-and-checks` chapter adds it to the driver limitations knowledge page.
- Column casing and value options reach the native method through `self._cursor_opts` on every call.
- `select_value` on an empty result raises the native `NotFoundError`.
- `driver.cursor()` outside a transaction on a pooled driver still raises `InterfaceError` with the existing message.

### Deleted in this task

- `_execute_cursor`. Its only callers are `execute` and `execute_with_description`.

## Test Matrix

Tests use the doubles from `oracle_test_doubles.py`, which t15 extends with the single-call methods and a `single_calls` list on the pool doubles.

| Case | Input | Expected |
| --- | --- | --- |
| One call, single client | Each of the eight methods once on `SyncOracleDriver(client=stub)` | One entry per method in `stub.calls` with the matching method name; `stub.cursor_calls` is empty |
| One call, pooled | The same on a pooled driver without a broker | One entry per method in the pool double's `single_calls`; no `acquire` from Python |
| Pinned transaction | `begin()`, three statements, `rollback()` | The three run on the pinned client; no pool single-call |
| No generator | Replace `checkout_cursor` and `_checkout_client` with spies; run each query method | Neither spy is called |
| Select | `select("SELECT 1 AS a FROM dual")` | `[{"a": 1}]` |
| Upper-case names | Config with `enable_lowercase_column_names=False` | `[{"A": 1}]`; the stub receives the option as `False` |
| Typed select | `select(sql, schema_type=_EmployeeRow)` | One `fetch_rows`; hydrated instances |
| Tuples and names | `execute_with_description("SELECT 1 AS a, 2 AS b FROM dual")` | `([(1, 2)], ("a", "b"))` |
| DDL through description | `execute_with_description("CREATE TABLE ...")` | `([], ())` |
| DML | `execute` of an `UPDATE` on two rows | `2` |
| `execute_dml` | The same `UPDATE` | `(2, None)` |
| Out-bind | `execute("BEGIN :out := 99; END;", {"out": var})` | `1`; the variable holds 99; `driver.out_binds == {"out": 99}` |
| Out-binds reset | A `select` after the out-bind statement | `driver.out_binds is None` |
| Session context | `execute` of a `DBMS_SESSION.SET_IDENTIFIER` block with a `value` bind | The local session map is updated |
| Plain statement | `execute` of an `UPDATE` | `track_session_context_sql` is not called (spy) |
| Pending session attributes | Driver built with a `client_identifier`; first statement | Attributes applied once; later statements take the direct route |
| Lease per statement, pooled with broker | Three autocommit statements with a recording broker | Three acquires and three releases |
| Lease per transaction | Transaction with five statements and a recording broker | One acquire, one release |
| Fatal transport error | Pooled driver with a broker; the stub raises vendor code 3113 | The client is released with `discard=True` |
| `cursor()` on a pooled driver | No transaction | `InterfaceError` with the existing message |
| `checkout_cursor` | The existing contract test | Passes unchanged |
| Not found | `select_value` with no row | `NotFoundError` |
| Error class | ORA-00001 through `execute` | `IntegrityError` |

## Verification

- `make dev`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/oracle/_sync.py src/py/tests/unit/db/test_oracle_adapter.py`
- `uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/oracle/_sync.py`
- `test -z "$(git grep -n '_execute_cursor' -- src/py/vertebra/db/adapters/oracle/_sync.py)"`
- `make build`
- `OUT="$(mktemp -d)"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine oracle --dsn "$ORACLE_DSN" --output "$OUT/oracle.json"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine oracle --oracle-reset-relay off --dsn "$ORACLE_DSN" --output "$OUT/oracle-relay-off.json"`
- `git status --porcelain -- tools/benchmarks`

`ORACLE_DSN` is the URL of the `oracle` container fixture in `tools/benchmarks/fixtures.py::FIXTURE_SPECS`. The first capture uses the default relay setting and the second turns the relay off. Both reports stay in the temporary directory. The last command prints nothing. When the fixture cannot start on the host, record the captures as not measured with the error text.

## Acceptance Criteria

- [x] On the pinned, single-client, and direct pool routes each of the eight methods makes exactly one native call, and the counting tests assert it.
- [x] No query method enters `checkout_cursor` or `_checkout_client`, and a test asserts it.
- [x] `_execute_cursor` is gone from `_sync.py`. `checkout_cursor` is still public and unchanged.
- [x] Session-context tracking, `out_binds`, column casing, and the pooled `cursor()` rule behave as the matrix states.
- [x] The task result lists the Oracle sync figures with the default relay setting and with the relay off, next to their comparators, or states why a capture was not measured.

## Notes & Discoveries

- `SyncOracleDriver` routes `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, and `execute_dml` through `_single_call` / `_single_call_checked` onto `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`, removing `_execute_cursor` while leaving `cursor()`, `checkout_cursor()`, and `_checkout_client()` intact.
- Recorded limitation (for `cleanup-and-checks` to add to `database-driver-limitations.md`): `driver.out_binds` and session-context tracking follow `execute` and `execute_dml` only; fetch-route methods (`select`, `select_one`, `select_one_or_none`, `select_value`, `execute_tuples`, `execute_with_description`) reset `driver.out_binds` to `None`, while out-bind variables passed in parameters are still populated in place.
- Oracle sync benchmark captures (default relay setting and `--oracle-reset-relay off`) were not measured on this host because Docker is unavailable in the sandbox (`failed to connect to the docker API at unix:///var/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /var/run/docker.sock: connect: no such file or directory`).
- Full workspace `make dev`, `make build`, and `make lint` were not run to avoid rebuilding unmodified Rust crates or auto-fixing unowned files in parallel; `vertebra._oracle` was verified to expose all sync single-call methods on `SyncOracleClient` and `SyncOraclePool` (`mypy.stubtest vertebra._oracle` passed), and scoped `ruff check`, `ruff format --check`, `mypy`, `pyright`, and `pytest` (234 passed) all succeeded on the owned files.
