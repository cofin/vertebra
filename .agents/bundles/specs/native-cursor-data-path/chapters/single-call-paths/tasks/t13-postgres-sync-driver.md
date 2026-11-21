---
type: Task
id: "native-cursor-data-path:single:t13"
chapter_id: single-call-paths
title: "PostgreSQL sync driver on the single-call methods"
description: "SyncPostgreSQLDriver query methods make one native single-call per statement on the pinned session, the pool, or a leased client."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t11"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t11-postgres-sync-single-call.md"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:1-105"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:146-150"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:307-384"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:671-767"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:816-879"
  - "src/py/vertebra/db/driver/_sync.py:69-167"
  - "src/py/vertebra/db/driver/_common.py:141-148"
  - "src/py/vertebra/db/driver/_common.py:220-228"
  - "src/py/tests/unit/db/test_postgres_native_adapters.py:285-520"
files:
  - "src/py/vertebra/db/adapters/postgres/_sync.py"
tests:
  - "src/py/tests/unit/db/test_postgres_native_adapters.py"
estimated_loc: 170
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
  - id: pg-sync-driver
    resource: "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver"
    title: "Sync PostgreSQL driver query path"
tags: [postgres, python, driver, performance]
---

# PostgreSQL sync driver on the single-call methods

## Objective & Scope Boundary

`select`, `select_one`, `select_one_or_none`, `select_value`, `execute_with_description`, `execute_tuples`, `execute`, and the new `execute_dml` on `SyncPostgreSQLDriver` each make one native statement call. The driver creates no cursor and reads no cursor attribute for them.

`estimated_loc` counts about 90 lines added and 80 removed in the driver. Tests add or change about 150 lines.

Out of scope:

- `execute_many`, `cursor()`, `pipeline`, Arrow, COPY, listeners, and replication. They keep their code.
- `src/py/vertebra/db/driver/_sync.py`. Another task owns the base class. This task overrides methods on the PostgreSQL driver only.
- The async driver (t14).

## Bounded Read Manifest

- Root spec, section "Single-call methods (R16)" for the return shapes, and section "Lease contract (R6)".
- `network-engines` chapter spec, section "Lease contract (R6)", for where PostgreSQL attaches and releases a lease.
- `_sync.py:363-384` are `execute` and `execute_with_description`. `:816-879` is `_execute_cursor`. `:701-726` is `_execute_on_pool`. `:671-699` is `_ensure_pool`. `:316-346` is `close`.
- `driver/_sync.py:122-147`: the base `select_one` and `select_one_or_none` call `self.select`.
- `test_postgres_native_adapters.py:334-381` are the pool doubles for the token refresh test. `:458-502` is the lease-count test.

## Contracts & Edge Cases

### Routing (unchanged rules, new mechanics)

| Route | When | Native object called |
| --- | --- | --- |
| Pinned transaction session | `self._tx_conn` is set | that client |
| Tenant session | `self._tenant_conn` is set | that client |
| Leased client | `self._client_factory` is set or `self._lease_at_checkout` is true | a client checked out for this statement |
| Pool | otherwise | the pool |

On the pinned, tenant, and pool routes a query method makes exactly one native call. On the leased route it checks a client out, makes one native statement call, and returns the client, because the lease contract ties the lease to a checkout that Python can see. The lease counts of the `network-engines` chapter do not change.

### New state

- Add `"_fast_pool"` to `__slots__` and `self._fast_pool: Any = None` to `__init__`.
- `_fast_pool` is the pool object when the pool route needs no per-statement check, and `None` otherwise. Add:

```python
def _publish_fast_pool(self) -> None:
    """Record the pool as the direct single-call target when no token check or lease is needed."""
    direct = self.config.token_provider is None and self._client_factory is None and not self._lease_at_checkout
    self._fast_pool = self._pool if direct else None
```

- `_ensure_pool` calls `self._publish_fast_pool()` before each `return self._pool`. Where it sets `self._pool = None` during token rotation it also sets `self._fast_pool = None`.
- `close` sets `self._fast_pool = None` on the line after `self._closed = True`.
- `connect()` already calls `_ensure_pool()`, so a connected driver uses the direct pool route from its first statement. A driver that was never connected takes the checked route once.

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
    kwargs: dict[str, Any],
) -> Any:
    """Run one statement through one native single-call method and return its result."""
    timeout = kwargs.pop("timeout", None)
    parameter_oids = kwargs.pop("parameter_oids", None)
    annotated_sql = self._annotate_sql(sql, **kwargs)
    params: Sequence[Any] | Mapping[str, Any] = parameters or ()
    target = self._tx_conn or self._tenant_conn or self._fast_pool
    if target is not None:
        return pick(target)(annotated_sql, params, parameter_oids=parameter_oids, timeout=timeout)
    return self._single_call_checked(pick, annotated_sql, params, parameter_oids, timeout)
```

- `_annotate_sql`, `timeout`, and `parameter_oids` are handled as `_execute_cursor` handles them today.
- The direct branch does not call `_ensure_open`. A closed driver has no pinned session and no `_fast_pool`, so it reaches `_single_call_checked`, which calls `_ensure_open` first and raises `InterfaceError`.
- The direct branch does not set `_active_client`. `cancel()` already falls back to `_tx_conn` and `_tenant_conn`.

```python
def _single_call_checked(
    self,
    pick: Callable[[Any], Callable[..., Any]],
    sql: str,
    params: Sequence[Any] | Mapping[str, Any],
    parameter_oids: Any,
    timeout: Any,
) -> Any:
    """Run one statement on a leased client, or on the pool with one token refresh on authentication rejection."""
```

Its body, in order:

1. `self._ensure_open()`.
2. Leased route, when `self._client_factory is not None or self._lease_at_checkout`: `client, owns_client = self._acquire_session_client()`; set `self._active_client` to the client and restore the previous value in a `finally`; return `pick(client)(sql, params, parameter_oids=parameter_oids, timeout=timeout)`. On exit, when `owns_client`, call `self._release_pooled_client(client, evict=True)` under `contextlib.suppress(Exception)` if the call raised, and `self._release_pooled_client(client, evict=False)` otherwise. This is the structure of `_execute_cursor:843-868` with the cursor lines replaced by the one call.
3. Pool route: `pool = self._ensure_pool()`, then `pick(pool)(...)`. On an exception: re-raise unless `self.config.token_provider is not None` and `is_postgres_auth_rejection(exc)`. Otherwise call the provider's `invalidate` when it is callable; when `self._owns_pool`, `pool = self._ensure_pool(force_refresh=True)`; otherwise `self.config.resolve_iam_password_sync(force_refresh=True)` and `pool = self._ensure_pool()`. Then make the call once more and let its error propagate. This is `_execute_on_pool` with the cursor replaced by the pool method.

### Query methods

Signatures are the base class signatures. Each body is as follows.

| Method | Body |
| --- | --- |
| `execute` | `return self._single_call(_EXECUTE_DML, sql, parameters, kwargs)[0]` |
| `execute_dml(self, sql, parameters=None, **kwargs) -> tuple[int, Any]` | `return self._single_call(_EXECUTE_DML, sql, parameters, kwargs)` |
| `execute_with_description` | `return self._single_call(_FETCH_ROWS, sql, parameters, kwargs)` |
| `execute_tuples` | `return self._single_call(_FETCH_ROWS, sql, parameters, kwargs)[0]` |
| `select` | With `schema_type is None`: `return self._single_call(_FETCH_DICTS, sql, parameters, kwargs)`. Otherwise `rows, names = self._single_call(_FETCH_ROWS, sql, parameters, kwargs)` and `return self.hydrate_rows(rows, names, schema_type)` |
| `select_value` | `value = self._single_call(_FETCH_VALUE, sql, parameters, kwargs)`; return `to_value_type(value, value_type)` when `value_type is not None and value is not None`, else `value` |
| `select_one`, `select_one_or_none` | Not overridden. The base methods call `self.select`, which makes the one native call |

- `execute` returns the native row count, which is never negative.
- `execute_dml` is new on the driver. Its docstring is `Execute a statement and return its row count and last row id.`
- `select_value` on an empty result raises the native `NotFoundError` with the message `Query returned no rows`. The driver adds no check.
- `execute_with_description` on a statement without a result set returns `([], ())`.

### Deleted in this task

- `_execute_cursor` and `_execute_on_pool`. `git grep` finds no caller outside the methods above.
- Imports that only they used.

`cursor()`, `_ensure_pool`, `_acquire_pool_client`, `_acquire_session_client`, and `_release_pooled_client` stay. `execute_many`, `begin`, the pipeline, Arrow, and COPY still call them.

### Test doubles in the test file

- `_AuthRejectingOncePool` rejects through the four single-call methods in place of `cursor()`: each override counts an attempt, raises the `28P01` `OperationalError` while `auth_failures_remaining` is positive, and otherwise calls the parent method. `_AuthRejectingOnceCursor` is deleted with its last user. Rename `test_sync_pool_cursor_auth_rejection_retries_once` to `test_sync_pool_auth_rejection_retries_once`; its assertions stay.
- Add `_CountingSyncPool(FakeSyncPostgresPool)`: each of the four single-call methods appends its name to `self.calls` and calls the parent; `cursor()` and `acquire()` append `"cursor"` and `"acquire"`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One call per method, pool route | Connected driver on `_CountingSyncPool`; each of the eight methods once | `calls` gains exactly one name per method: `fetch_dicts` for `select`, `select_one`, `select_one_or_none`; `fetch_value`; `fetch_rows` twice; `execute_dml` twice. No `cursor`, no `acquire` |
| Typed select | `select(sql, schema_type=Row)` | One `fetch_rows`; hydrated instances |
| Values | The existing query matrix test | Unchanged results |
| Pinned transaction | `begin()`, three statements, `commit()` | One `acquire`; no pool single-call; the three run on the pinned client |
| Tenant session | Inside a tenant scope | Statements run on the tenant client |
| Lease counts | `test_sync_broker_checkout_lease_counts`, all modes | Unchanged counts |
| Token refresh | `_AuthRejectingOncePool` with a token provider | Two attempts; one `invalidate`; `resolve_calls == [True]` |
| No token provider | Auth rejection with no provider | The error propagates after one attempt |
| Not found | `select_value` with no row | `NotFoundError` |
| Value type | `select_value("SELECT '5'", value_type=int)` | `5` |
| Options | `select_value(sql, timeout=1.5, parameter_oids=(23,))` | The pool double receives both keywords |
| Trace comment | Active trace context | The SQL the double receives ends with the `traceparent` comment |
| Closed driver | `close()` then `select` | `InterfaceError` |
| Reopen | `close()`, `connect()`, `select` | One pool call |
| `execute_dml` | `UPDATE` of two rows | `(2, None)` |
| Error class | Unique violation through `execute` | `IntegrityError` |

## Verification

- `make dev`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/tests/unit/db/test_postgres_native_adapters.py`
- `uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/postgres/_sync.py`
- `test -z "$(git grep -n -e '_execute_cursor' -e '_execute_on_pool' -- src/py/vertebra/db/adapters/postgres/_sync.py)"`
- `make build`
- `OUT="$(mktemp -d)"`
- `uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine postgres --dsn "$PG_DSN" --output "$OUT/postgres.json"`
- `git status --porcelain -- tools/benchmarks`

`PG_DSN` is the URL of the `postgres` container fixture in `tools/benchmarks/fixtures.py::FIXTURE_SPECS`. The capture needs release extensions, which `make build` produces. The report stays in the temporary directory. The last command prints nothing. When the fixture cannot start on the host, record the capture as not measured with the error text.

## Acceptance Criteria

- [ ] On the pinned, tenant, and pool routes each of the eight methods makes exactly one native call, and the counting test asserts it.
- [ ] `_execute_cursor` and `_execute_on_pool` are gone from `_sync.py`.
- [ ] The lease-count test passes with its assertions unchanged.
- [ ] The driver file has no inline comment, and every new function has a docstring.
- [ ] The task result lists the sync PostgreSQL figures from the capture next to their comparators, or states why the capture was not measured.
