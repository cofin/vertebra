---
type: Task
id: "native-cursor-data-path:single:t14"
chapter_id: single-call-paths
title: "PostgreSQL async driver on the single-call methods"
description: "AsyncPostgreSQLDriver query methods await one native awaitable per statement, with no helper coroutine on the autocommit path."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t12"
  - "native-cursor-data-path:single:t13"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/research/native-cursor-hot-paths_20261121.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t13-postgres-sync-driver.md"
  - "src/py/vertebra/db/adapters/postgres/_async.py:1-103"
  - "src/py/vertebra/db/adapters/postgres/_async.py:143-148"
  - "src/py/vertebra/db/adapters/postgres/_async.py:311-389"
  - "src/py/vertebra/db/adapters/postgres/_async.py:694-739"
  - "src/py/vertebra/db/adapters/postgres/_async.py:768-850"
  - "src/py/vertebra/db/adapters/postgres/_async.py:873-937"
  - "src/py/vertebra/db/driver/_async.py:77-175"
  - "src/py/tests/unit/db/test_postgres_native_adapters.py:520-760"
files:
  - "src/py/vertebra/db/adapters/postgres/_async.py"
tests:
  - "src/py/tests/unit/db/test_postgres_native_adapters.py"
estimated_loc: 180
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
  - id: pg-async-driver
    resource: "src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver"
    title: "Async PostgreSQL driver query path"
  - id: hot-path-research
    resource: .agents/bundles/research/native-cursor-hot-paths_20261121.md
    title: "Section 5: three coroutine frames and two awaitables per query"
tags: [postgres, python, driver, async, performance]
---

# PostgreSQL async driver on the single-call methods

## Objective & Scope Boundary

An autocommit query through `AsyncPostgreSQLDriver` awaits one native awaitable. The coroutines `_execute_cursor`, `_ensure_pool_async`, and `_execute_on_pool` are no longer created per query when the pool exists and no token provider is configured.

This task depends on t13 as well as t12, because both driver tasks edit `src/py/tests/unit/db/test_postgres_native_adapters.py`.

`estimated_loc` counts about 95 lines added and 85 removed in the driver. Tests add or change about 170 lines.

Out of scope:

- `execute_many`, `cursor()`, `pipeline`, Arrow, COPY, listeners, and replication.
- `src/py/vertebra/db/driver/_async.py`. This task overrides methods on the PostgreSQL driver only.

## Bounded Read Manifest

- Root spec, sections "Single-call methods (R16)" and "Lease contract (R6)".
- Research brief, section 5, for the frames this task removes.
- The t13 worksheet. The async driver mirrors its routing, state, and method table.
- `_async.py:368-389` are `execute` and `execute_with_description`. `:873-937` is `_execute_cursor`. `:768-794` is `_execute_on_pool`. `:694-739` are `_ensure_pool` and `_ensure_pool_async`. `:320-351` is `close`.
- `driver/_async.py:130-155`: the base `select_one` and `select_one_or_none` await `self.select`.
- `test_postgres_native_adapters.py:543-590` are the async pool doubles for the token refresh test. `:670-730` is the async lease-count test.

## Contracts & Edge Cases

### Routing

The four routes and their order are the ones in the t13 table. On the pinned, tenant, and pool routes a query method awaits exactly one awaitable, and it is the native one. On the leased route it awaits the checkout, one native statement awaitable, and the release, because the lease contract ties the lease to a checkout that Python can see. Lease counts do not change.

### New state

- Add `"_fast_pool"` to `__slots__` and `self._fast_pool: Any = None` to `__init__`.
- Add `_publish_fast_pool(self) -> None` with the body and docstring given in t13.
- `_ensure_pool` and `_ensure_pool_async` call `self._publish_fast_pool()` before each `return self._pool`. Where `_ensure_pool_async` sets `self._pool = None` during token rotation it also sets `self._fast_pool = None`.
- `close` sets `self._fast_pool = None` on the line after it marks the driver closed.
- `connect()` already awaits `_ensure_pool_async()`, so a connected driver uses the direct pool route from its first statement.

### Helpers

Module level, with `from operator import attrgetter` and `from vertebra.utils.schema import to_value_type` added to the top-level imports: `_FETCH_ROWS`, `_FETCH_DICTS`, `_FETCH_VALUE`, `_EXECUTE_DML`, defined as in t13.

`_single_call` is a plain function. It is not `async def`. On the direct branch it returns the native awaitable itself, so the query method that awaits it creates no second coroutine:

```python
def _single_call(
    self,
    pick: Callable[[Any], Callable[..., Awaitable[Any]]],
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    kwargs: dict[str, Any],
) -> Awaitable[Any]:
    """Return the awaitable of one native single-call method for one statement."""
    timeout = kwargs.pop("timeout", None)
    parameter_oids = kwargs.pop("parameter_oids", None)
    annotated_sql = self._annotate_sql(sql, **kwargs)
    params: Sequence[Any] | Mapping[str, Any] = parameters or ()
    target = self._tx_conn or self._tenant_conn or self._fast_pool
    if target is not None:
        return pick(target)(annotated_sql, params, parameter_oids=parameter_oids, timeout=timeout)
    return self._single_call_checked(pick, annotated_sql, params, parameter_oids, timeout)
```

`Awaitable` is added to the `collections.abc` import.

The fast path is the `if target is not None` branch. It is taken when a session is pinned, or when the pool exists, no token provider is configured, no client factory is set, and no broker is attached. It calls no `async def` of the driver.

```python
async def _single_call_checked(
    self,
    pick: Callable[[Any], Callable[..., Awaitable[Any]]],
    sql: str,
    params: Sequence[Any] | Mapping[str, Any],
    parameter_oids: Any,
    timeout: Any,
) -> Any:
    """Run one statement on a leased client, or on the pool with one token refresh on authentication rejection."""
```

The slow path. Its body, in order:

1. `self._ensure_open()`.
2. Leased route, when `self._client_factory is not None or self._lease_at_checkout`: `client, owns_client = await self._acquire_session_client()`; set `self._active_client` and restore it in a `finally`; `return await pick(client)(sql, params, parameter_oids=parameter_oids, timeout=timeout)`. On exit, when `owns_client`, release inside `CancelScope(shield=True)`: `evict=True` under `contextlib.suppress(Exception)` if the call raised, `evict=False` otherwise. This is `_execute_cursor:900-929` with the cursor lines replaced by the one await.
3. Pool route: `pool = await self._ensure_pool_async()`, then `return await pick(pool)(...)`. On an exception: re-raise unless `self.config.token_provider is not None` and `is_postgres_auth_rejection(exc)`. Otherwise call the provider's `invalidate` when it is callable; when `self._owns_pool`, `pool = await self._ensure_pool_async(force_refresh=True)`; otherwise `await self.config.resolve_iam_password_async(force_refresh=True)` and `pool = await self._ensure_pool_async()`. Then await the call once more and let its error propagate. Token refresh on authentication rejection therefore works as today.

A driver with a token provider always takes the slow path on the pool route, because the token is compared with the pool's token on every statement, as today.

### Query methods

Signatures are the base class signatures. Each is `async def`.

| Method | Body |
| --- | --- |
| `execute` | `rowcount, _ = await self._single_call(_EXECUTE_DML, sql, parameters, kwargs)`; `return rowcount` |
| `execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, Any]` | `return await self._single_call(_EXECUTE_DML, sql, parameters, kwargs)` |
| `execute_with_description` | `return await self._single_call(_FETCH_ROWS, sql, parameters, kwargs)` |
| `execute_tuples` | `rows, _ = await self._single_call(_FETCH_ROWS, sql, parameters, kwargs)`; `return rows` |
| `select` | With `schema_type is None`: `return await self._single_call(_FETCH_DICTS, sql, parameters, kwargs)`. Otherwise await `_FETCH_ROWS` and `return self.hydrate_rows(rows, names, schema_type)` |
| `select_value` | `value = await self._single_call(_FETCH_VALUE, sql, parameters, kwargs)`; return `to_value_type(value, value_type)` when `value_type is not None and value is not None`, else `value` |
| `select_one`, `select_one_or_none` | Not overridden. The base methods await `self.select` |

- `execute_dml` is new on the driver. Its docstring is `Execute a statement and return its row count and last row id.`
- A cancelled task cancels the native awaitable. On the leased route the release still runs inside the shielded scope.
- `select_value` on an empty result raises the native `NotFoundError`.

### Deleted in this task

- `_execute_cursor` and `_execute_on_pool`. `git grep` finds no caller outside the methods above.
- Imports that only they used.

`_ensure_pool`, `_ensure_pool_async`, `_acquire_pool_client`, `_acquire_session_client`, and `_release_pooled_client` stay. `connect`, `cursor`, `execute_many`, `begin`, the pipeline, Arrow, and COPY still call them.

### Test doubles in the test file

- `_AsyncAuthRejectingOncePool` rejects through the four single-call methods in place of `cursor()`. `_AsyncAuthRejectingOnceCursor` is deleted with its last user. Rename `test_async_pool_cursor_auth_rejection_retries_once` to `test_async_pool_auth_rejection_retries_once`; its assertions stay.
- Add `_CountingAwaitable`: it wraps one awaitable, and its `__await__` increments a shared counter and delegates.
- Add `_CountingAsyncPool(FakeAsyncPostgresPool)`: each of the four single-call methods is a plain `def` that appends its name to `self.calls` and returns a `_CountingAwaitable` around the parent's awaitable. `cursor()` and `acquire()` append `"cursor"` and `"acquire"`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One awaitable per `select_value` | Connected driver on `_CountingAsyncPool`; `await drv.select_value("SELECT 1")` | `calls == ["fetch_value"]`; the awaitable counter is 1 |
| Same for each method | Each of the eight methods once | One pool call and one counted await per method; no `cursor`, no `acquire` |
| No helper coroutine | After `connect()`, replace `_ensure_pool_async` and `_single_call_checked` with spies; run `select_value` | Neither spy is called |
| Returned object | `drv._single_call(_FETCH_VALUE, "SELECT 1", None, {})` on the direct route | The object the pool double returned |
| Pinned transaction | `begin()`, three statements, `commit()` | One `acquire`; no pool single-call |
| Tenant session | Inside a tenant scope | Statements run on the tenant client |
| Lease counts | `test_async_broker_checkout_lease_counts`, all modes | Unchanged counts |
| Token refresh | `_AsyncAuthRejectingOncePool` with a token provider | Two attempts; one `invalidate`; one forced resolve |
| No token provider | Auth rejection with no provider | The error propagates after one attempt |
| Not found | `select_value` with no row | `NotFoundError` |
| Options | `select_value(sql, timeout=1.5, parameter_oids=(23,))` | The pool double receives both keywords |
| Cancel on leased route | Cancel a task inside a statement with a broker attached | Acquire count equals release count; the client is evicted, as the existing cancellation test expects |
| Closed driver | `close()` then `select` | `InterfaceError` |
| Concurrent selects | `test_async_concurrent_select_tasks` | Passes unchanged |
| `execute_dml` | `UPDATE` of two rows | `(2, None)` |

## Verification

- `make dev`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapters.py`
- `uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/postgres/_async.py`
- `test -z "$(git grep -n -e '_execute_cursor' -e '_execute_on_pool' -- src/py/vertebra/db/adapters/postgres/)"`
- `make build`
- `OUT="$(mktemp -d)"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine postgres --dsn "$PG_DSN" --output "$OUT/postgres.json"`
- `git status --porcelain -- tools/benchmarks`

`PG_DSN` is the URL of the `postgres` container fixture in `tools/benchmarks/fixtures.py::FIXTURE_SPECS`. The report stays in the temporary directory. The last command prints nothing. When the fixture cannot start on the host, record the capture as not measured with the error text.

## Acceptance Criteria

- [x] On the pinned, tenant, and direct pool routes each of the eight methods awaits exactly one awaitable, and the counting test asserts it.
- [x] With a pool and no token provider, a query creates no `_ensure_pool_async` coroutine and no slow-path coroutine, and a test asserts it.
- [x] Token refresh on authentication rejection still retries once.
- [x] `_execute_cursor` and `_execute_on_pool` are gone from `_async.py`.
- [x] The task result lists the PostgreSQL async pooled figure from the capture next to its comparator, or states why the capture was not measured.

## Notes & Discoveries

- Verified `src/py/vertebra/db/adapters/postgres/_async.py` and `src/py/tests/unit/db/test_postgres_native_adapters.py`:
  - `pytest` on `test_postgres_native_adapters.py`, `test_postgres_driver.py`, `test_postgres_native_adapter_pipeline.py`, and `test_postgres_native_adapter_arrow.py` (`334 passed`), plus `test_postgres_native_queries.py`, `test_postgres_native_pools.py`, and `test_postgres_native_cancel.py` (`103 passed`).
  - `ruff check`, `ruff format --check`, `mypy`, `pyright`, and `mypy.stubtest vertebra._postgres` all passed with 0 errors on the owned files.
  - `git grep -n -e '_execute_cursor' -e '_execute_on_pool' -- src/py/vertebra/db/adapters/postgres/` returned empty.
- Full `make lint` passed Biome, `tsc`, Ruff (853 files), Rust `fmt`/`clippy`, and Mypy (851 files), and stopped only on a pre-existing unrelated environment pyright error in `src/py/vertebra/_typing.py:360` (`Import "numpy" could not be resolved`).
- `make dev` / `make build` were not re-run because no Rust files were modified and the installed `vertebra._postgres` extension already exposes `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml` on `AsyncPostgresClient` and `AsyncPostgresPool` (verified via Python import check and `mypy.stubtest`).
- Live PostgreSQL benchmark capture (`tools.benchmarks.adapters.sql capture --engine postgres`) was not measured because the Docker daemon is unavailable in this environment (`failed to connect to the docker API at unix:///var/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /var/run/docker.sock: connect: no such file or directory`).
