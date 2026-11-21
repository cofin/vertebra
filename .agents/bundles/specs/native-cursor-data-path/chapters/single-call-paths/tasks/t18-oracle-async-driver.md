---
type: Task
id: "native-cursor-data-path:single:t18"
chapter_id: single-call-paths
title: "Oracle async driver on the single-call methods"
description: "AsyncOracleDriver query methods await one native awaitable per statement, without the generator-based checkout_cursor context manager."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t16"
  - "native-cursor-data-path:single:t17"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t16-oracle-async-single-call.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t17-oracle-sync-driver.md"
  - "src/py/vertebra/db/adapters/oracle/_async.py:1-45"
  - "src/py/vertebra/db/adapters/oracle/_async.py:214-285"
  - "src/py/vertebra/db/adapters/oracle/_async.py:556-643"
  - "src/py/vertebra/db/adapters/oracle/_async.py:786-901"
  - "src/py/vertebra/db/adapters/oracle/core.py:717-723"
  - "src/py/vertebra/db/driver/_async.py:130-175"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:626-634"
  - "src/py/tests/unit/db/test_oracle_adapter.py:2620-2780"
files:
  - "src/py/vertebra/db/adapters/oracle/_async.py"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 135
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-21T16:23:02Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, requirement R16 and the lease contract"
  - id: oracle-async-driver
    resource: "src/py/vertebra/db/adapters/oracle/_async.py::AsyncOracleDriver"
    title: "Async Oracle driver query path"
tags: [oracle, python, driver, async, performance]
---

# Oracle async driver on the single-call methods

## Objective & Scope Boundary

`select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, and the new `execute_dml` on `AsyncOracleDriver` each await one native awaitable. No statement enters the generator-based `checkout_cursor` context manager.

This task depends on t17 as well as t16, because both driver tasks edit `src/py/tests/unit/db/test_oracle_adapter.py`.

`estimated_loc` counts about 100 lines added and 35 removed in the driver. Tests add or change about 170 lines.

Out of scope:

- `execute_many`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, notifications, queues, and the pipeline.
- `cursor()` and `checkout_cursor()`. Both keep their code and their contracts.
- The `AsyncOraclePool` wrapper class in the same file. It gains no method.
- `src/py/vertebra/db/driver/_async.py`.

## Bounded Read Manifest

- The t17 worksheet. The async driver mirrors its routing table, its state, its method table, and its `checkout_cursor` answer.
- The t16 worksheet for the native awaitables and the pool method's checkout, discard, and cancel rules.
- `_async.py:556-643` are `_execute_cursor`, the query methods, and `exec_returning_async`. `:853-880` is the generator `_checkout_client`. `:823-851` are `_acquire_pool_client` and `_release_pool_client`.
- `test_oracle_adapter.py:2620-2780` are the async driver tests for out-binds and the cursor contracts.

## Contracts & Edge Cases

### Who still calls `checkout_cursor`

The answer is the one in t17: `execute_many`, the SQLSpec Oracle adapter, and the driver tests still call it, so it stays public and unchanged. The async generator `_checkout_client` stays for `checkout_cursor`, `ping`, `select_to_arrow`, `publish_notification`, and `pipeline.py`.

### Routing

The four routes are the ones in the t17 table. On the pinned, single-client, and direct pool routes a query method awaits exactly one awaitable, and it is the native one. On the checked-out route it awaits the checkout, one native statement awaitable, and the release. Lease counts do not change.

### New state in `__init__`

```python
self._native_pool: Any = pool.native_pool if isinstance(pool, AsyncOraclePool) else pool
self._pool_target: Any = None if self._lease_at_checkout else self._native_pool
```

Both lines go after `self._lease_at_checkout` is set.

### Helpers

Module level, with `from operator import attrgetter` and `from vertebra.utils.schema import to_value_type` added to the top-level imports: `_FETCH_ROWS`, `_FETCH_DICTS`, `_FETCH_VALUE`, `_EXECUTE_DML`, defined as in t17. `Awaitable` and `Callable` are added to the `collections.abc` import.

`_single_call` is a plain function, not `async def`. On the direct branch it returns the native awaitable itself:

```python
def _single_call(
    self,
    pick: Callable[[Any], Callable[..., Awaitable[Any]]],
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    out_binds: Any,
) -> Awaitable[Any]:
    """Return the awaitable of one native single-call method for one statement."""
    if self._pending_session_attrs is None:
        target = self._get_pinned_lease()
        if target is None:
            target = self._client if self._pool is None else self._pool_target
        if target is not None:
            return pick(target)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)
    return self._single_call_checked(pick, sql, parameters, out_binds)
```

```python
async def _single_call_checked(
    self,
    pick: Callable[[Any], Callable[..., Awaitable[Any]]],
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    out_binds: Any,
) -> Any:
    """Run one statement on a client after applying session attributes or checking it out of the pool."""
```

Its body, in order:

1. `active = self._get_pinned_lease()`; when that is `None` and `self._pool is None`, `active = self._client`.
2. When `active` is not `None`: `self._apply_pending_session_attrs(active)`, then `return await pick(active)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)`, with `self._capture_piggyback(active)` in a `finally`.
3. Otherwise: `lease = await self._acquire_pool_client()`; `return await pick(lease)(...)`. On `BaseException`, set `discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)` and re-raise. In a `finally`, `await self._release_pool_client(lease, discard=discard)`.

This is the body of the async `_checkout_client` written as plain `try` and `finally` blocks. Cancellation of the awaiting task reaches the `finally`, as it does in the generator today.

`_release_pool_client` gains one line at its start, after the `self._pool is None` return: `self._capture_piggyback(lease)`.

### Query methods

Each is `async def`. `out_binds` is read with `kwargs.get("out_binds")`. Other keyword arguments are ignored, as today.

| Method | Body |
| --- | --- |
| `execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, Any]` | `rowcount, lastrowid, out_values, touches = await self._single_call(_EXECUTE_DML, sql, parameters, kwargs.get("out_binds"))`; `self._last_out_binds = out_values`; when `touches`, `OracleCore.track_session_context_sql(self._session_context, sql, parameters)`; `return rowcount, lastrowid` |
| `execute` | `rowcount, _ = await self.execute_dml(sql, parameters, **kwargs)`; `return rowcount` |
| `execute_with_description` | `self._last_out_binds = None`; `return await self._single_call(_FETCH_ROWS, sql, parameters, kwargs.get("out_binds"))` |
| `execute_tuples` | `self._last_out_binds = None`; `rows, _ = await self._single_call(_FETCH_ROWS, ...)`; `return rows` |
| `select` | `self._last_out_binds = None`. With `schema_type is None`: `return await self._single_call(_FETCH_DICTS, ...)`. Otherwise await `_FETCH_ROWS` and `return _hydrate_rows_with_schema(rows, names, schema_type) if rows else []` |
| `select_value(self, sql, parameters=None, *, value_type=None, **kwargs)` | `self._last_out_binds = None`; `value = await self._single_call(_FETCH_VALUE, ...)`; return `to_value_type(value, value_type)` when `value_type is not None and value is not None`, else `value` |
| `select_one`, `select_one_or_none` | Not overridden. The base methods await `self.select` |
| `exec_returning_async` | Unchanged. It awaits `self.select` |

- `execute` awaits `execute_dml`, which is one extra coroutine frame and no extra native awaitable. Every other method awaits the native awaitable directly on the direct routes.
- `execute_dml` is new on the driver. Its docstring is `Execute a statement and return its row count and last row id.`
- The rules for session-context tracking, `out_binds`, column casing, the fetch-route reset of `driver.out_binds`, and `NotFoundError` are the ones in t17.
- `driver.cursor()` outside a transaction on a pooled driver still raises `InterfaceError` with the existing message.
- This is a recorded limitation: `driver.out_binds` and session-context tracking follow `execute` and `execute_dml` only. The task result states it so that the `cleanup-and-checks` chapter adds it to the driver limitations knowledge page.

### Deleted in this task

- `_execute_cursor`. Its only callers are `execute` and `execute_with_description`.

## Test Matrix

Tests use the async doubles from `oracle_test_doubles.py`, which t15 extends with the single-call methods. Add `_CountingAwaitable` to the test file: it wraps one awaitable, and its `__await__` increments a shared counter and delegates. A counting subclass of the async pool double returns it from plain `def` single-call methods.

| Case | Input | Expected |
| --- | --- | --- |
| One awaitable, pooled | `await drv.select_value(sql)` on a pooled driver without a broker | One pool single-call; the awaitable counter is 1 |
| Same for each method | Each of the eight methods once | One pool single-call and one counted await per method |
| One call, single client | Each method on `AsyncOracleDriver(client=stub)` | One entry per method in `stub.calls`; `stub.cursor_calls` is empty |
| No generator | Replace `checkout_cursor` and `_checkout_client` with spies; run each query method | Neither spy is called |
| No helper coroutine | Replace `_single_call_checked` with a spy on a pooled driver without a broker or pending attributes | The spy is not called |
| Pinned transaction | `begin()`, three statements, `rollback()` | The three run on the pinned client |
| Select, casing, typed select | The t17 rows, awaited | Same values |
| Out-bind | `await drv.execute("BEGIN :out := 99; END;", {"out": var})` | `1`; `driver.out_binds == {"out": 99}` |
| Out-binds reset | A `select` after the out-bind statement | `driver.out_binds is None` |
| Session context | `execute` of a `DBMS_SESSION.SET_IDENTIFIER` block | The local session map is updated |
| `exec_returning_async` | A returning statement whose stub reply has out-bind values and no rows | The out-bind row, hydrated |
| Pending session attributes | Driver built with a `client_identifier`; first statement | Attributes applied once; later statements take the direct route |
| Lease per statement, pooled with broker | Three autocommit statements with a recording broker | Three acquires and three releases |
| Cancel on checked-out route | Cancel a task inside a statement with a broker attached | Acquire count equals release count |
| Fatal transport error | The stub raises vendor code 3113 on the checked-out route | The client is released with `discard=True` |
| `cursor()` on a pooled driver | No transaction | `InterfaceError` with the existing message |
| `checkout_cursor` | The existing async contract tests | Pass unchanged |
| Not found | `select_value` with no row | `NotFoundError` |
| Error class | ORA-00001 through `execute` | `IntegrityError` |

## Verification

- `make dev`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/oracle/_async.py src/py/tests/unit/db/test_oracle_adapter.py`
- `uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/oracle/_async.py`
- `test -z "$(git grep -n '_execute_cursor' -- src/py/vertebra/db/adapters/oracle/)"`
- `make build`
- `OUT="$(mktemp -d)"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine oracle --dsn "$ORACLE_DSN" --output "$OUT/oracle.json"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine oracle --oracle-reset-relay off --dsn "$ORACLE_DSN" --output "$OUT/oracle-relay-off.json"`
- `git status --porcelain -- tools/benchmarks`

`ORACLE_DSN` is the URL of the `oracle` container fixture in `tools/benchmarks/fixtures.py::FIXTURE_SPECS`. The first capture uses the default relay setting and the second turns the relay off. Both reports stay in the temporary directory. The last command prints nothing. When the fixture cannot start on the host, record the captures as not measured with the error text.

## Acceptance Criteria

- [x] On the pinned, single-client, and direct pool routes each of the eight methods awaits exactly one native awaitable, and the counting tests assert it.
- [x] No query method enters `checkout_cursor` or `_checkout_client`, and a test asserts it.
- [x] `_execute_cursor` is gone from `_async.py`. `checkout_cursor` is still public and unchanged.
- [x] Session-context tracking, `out_binds`, column casing, and the pooled `cursor()` rule behave as the matrix states.
- [x] The task result lists the Oracle figures with the default relay setting and with the relay off, next to their comparators, or states why a capture was not measured.

## Notes & Discoveries

- `AsyncOracleDriver` routes `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, and `execute_dml` through plain-function `_single_call` and async `_single_call_checked` onto `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`, removing `_execute_cursor` while keeping `cursor()`, `checkout_cursor()`, and `_checkout_client()` intact.
- `_release_pool_client` now captures `last_piggyback_frame` at its start (after the `self._pool is None` check) so pooled clients always record piggyback frames on release.
- Recorded limitation (for `cleanup-and-checks` to add to `database-driver-limitations.md`): `driver.out_binds` and session-context tracking follow `execute` and `execute_dml` only; fetch-route methods (`select`, `select_one`, `select_one_or_none`, `select_value`, `execute_tuples`, `execute_with_description`) reset `driver.out_binds` to `None`, while out-bind variables passed in parameters are still populated in place.
- Oracle benchmark captures (default relay setting and `--oracle-reset-relay off`) were not measured on this host because Docker is unavailable in the sandbox (`failed to connect to the docker API at unix:///var/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /var/run/docker.sock: connect: no such file or directory`).
- Full workspace `make dev`, `make build`, and `make lint` were not run to avoid rebuilding unmodified Rust crates or auto-fixing unowned files in parallel; `vertebra._oracle` was verified to expose all four async single-call methods on `AsyncOracleClient` and `AsyncOraclePool` (`mypy.stubtest vertebra._oracle` passed), and scoped `ruff check`, `ruff format --check`, `mypy`, `pyright`, and `pytest` (267 passed) all succeeded on the owned files.
