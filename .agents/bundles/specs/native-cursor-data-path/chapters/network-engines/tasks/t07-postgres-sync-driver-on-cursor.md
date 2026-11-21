---
type: Task
id: "native-cursor-data-path:network:t07"
chapter_id: network-engines
title: "Synchronous PostgreSQL driver on the native cursor with leases at checkout"
description: "SyncPostgreSQLDriver query methods call the native cursor and the broker lease moves to client checkout."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t02"
  - "native-cursor-data-path:network:t06"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:52-116"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:346-412"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:703-771"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:804-864"
  - "src/py/vertebra/db/adapters/postgres/core.py:232-246"
  - "src/py/vertebra/db/driver/_common.py:148-199"
  - "src/py/vertebra/db/driver/_sync.py:69-186"
  - "src/py/vertebra/db/adapters/postgres/options.py:243-254"
  - "src/py/tests/unit/db/test_postgres_native_adapters.py"
files:
  - "src/py/vertebra/db/adapters/postgres/_sync.py"
tests:
  - "src/py/tests/unit/db/test_postgres_native_adapters.py"
estimated_loc: 240
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: pg-sync-driver
    resource: "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver"
    title: "Native driver query path"
tags: [postgres, python, driver, lease]
---

# Synchronous PostgreSQL driver on the native cursor with leases at checkout

## Objective & Scope Boundary

`execute`, `execute_with_description`, `execute_many`, and the inherited `select` family on `SyncPostgreSQLDriver` run through one native cursor call. The per-statement statement-cache lookup and broker lease are gone. A broker lease is taken when a pooled client is checked out and returned when it is released.

Out of scope:

- `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, COPY, pipelines, listeners, and replication. They keep `prepare_query` and their native calls.
- The async driver (t08).
- `core.py`, `binds.py`, `types.py`, and `pipeline.py`.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/postgres/_sync.py:804-864` is `_run_query`, which this task deletes.
- `src/py/vertebra/db/adapters/postgres/_sync.py:346-412` are the three public methods to rewrite.
- `src/py/vertebra/db/adapters/postgres/_sync.py:703-771` holds `_query_on_pool`, `_acquire_pool_client`, `_acquire_session_client`, and `_release_pooled_client`.
- `src/py/vertebra/db/driver/_common.py:148-199` holds `_acquire_broker_lease` and `_release_broker_lease`.

## Contracts & Edge Cases

- Add `def cursor(self, *, lazy_begin: bool = False) -> SyncPostgresCursor`. It returns `self._tx_conn.cursor(lazy_begin=lazy_begin)` when a transaction is pinned, else `self._tenant_conn.cursor(lazy_begin=lazy_begin)` when a tenant scope is pinned. Otherwise, with `lazy_begin` false it returns `self._ensure_pool().cursor()`, and with `lazy_begin` true it returns a cursor on the driver's single session client when the driver has no pool, or raises `InterfaceError` with the message `lazy_begin needs a session; this driver is pooled`. It takes no lease.
- `__init__` sets `self._lease_at_checkout: bool = self._broker is not None` once. No query method reads `self._broker`.
- Add `_execute_cursor(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None, **kwargs: Any) -> SyncPostgresCursor`. It pops `timeout` and `parameter_oids`, applies `_annotate_sql(sql, **kwargs)`, and runs `cursor.execute(annotated, parameters or (), parameter_oids=..., timeout=...)`.
- `_execute_cursor` picks the cursor in this order: pinned transaction or tenant connection; else, when `_client_factory` is set or `_lease_at_checkout` is true, a client from `_acquire_session_client()` that is released in a `finally` block, evicted on error; else the pool cursor.
- On the pool-cursor branch, an authentication rejection is retried once with the logic `_query_on_pool` has today. Rename `_query_on_pool` to `_execute_on_pool` and change it to take and return a cursor.
- `_active_client` is set and restored around the call on the session branches, as `_run_query` does today, so `cancel()` keeps working.
- `execute` returns `max(cursor.rowcount, 0)`.
- `execute_with_description` returns `(cursor.fetchall(), tuple(col[0] for col in cursor.description))` when `cursor.description` is not `None`, and `([], ())` otherwise.
- `execute_many` keeps its `_ensure_open` and empty-input checks, acquires one session client, and calls `client.cursor().executemany(annotated_sql, parameter_sets)`. It returns `max(cursor.rowcount, 0)`. `PostgreSQLCore.prepare_query` is no longer called here.
- `_acquire_pool_client` calls `self._attach_checkout_lease(client)` after a successful `pool.acquire()` when `_lease_at_checkout` is true. The driver stores no lease. A refused acquire attaches nothing and the checkout proceeds. If the call raises, the client is released before the error propagates.
- `_release_pooled_client` calls `client.release_lease()` before the client is returned or evicted.
- `_evict_pinned_connections` and `close` call `release_lease()` on each client they still hold. A client dropped any other way releases through its guard.
- Delete `_run_query`, the `_lookup_or_cache_statement` call, and every per-statement `_acquire_broker_lease` and `_release_broker_lease` call. `PostgreSQLCore.project_query_rows` loses its sync caller here; task t08 deletes the function after the async caller is gone.
- Errors from query methods are the `vertebra._vertebra` classes the driver raises today, with the same specific subclasses. Existing error-class assertions stay unchanged.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Tuples | `execute_with_description('SELECT 1 AS a, 2 AS b')` | `([(1, 2)], ('a', 'b'))` |
| Command | `execute_with_description('SET search_path TO public')` | `([], ())` |
| Row count | `execute` of an `UPDATE` touching two rows | `2` |
| Named parameters | `select` with `%(x)s` and a mapping | Correct row |
| execute_many | Three sets | Returns `3` |
| No broker | Ten statements with `broker=None` | A recording broker double is never called |
| Lease per checkout | Transaction with five statements and a recording broker | One acquire and one release |
| Lease per autocommit call | Three autocommit statements with a recording broker | Three acquires, three releases, each around one checkout |
| Lease released on error | Failing statement inside a checkout | Acquire count equals release count |
| Acquire refused at checkout | Broker double whose `acquire` raises `RuntimeError` | The statement succeeds; zero releases |
| Auth retry | Token provider and a first `28P01` rejection | One retry, then success |
| Error class | Duplicate key through `execute` | `IntegrityError` |
| Closed driver | `close()` then `select` | `InterfaceError` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/_sync.py`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/postgres/_sync.py)"`
- `test -z "$(git grep -n '_run_query' -- src/py/vertebra/db/adapters/postgres/_sync.py)"`

## Acceptance Criteria

- [ ] Lease tests use a recording broker double and assert call counts.
- [ ] `_sync.py` contains no per-statement lease or statement-cache call.
- [ ] Arrow, COPY, and pipeline tests pass unchanged.
