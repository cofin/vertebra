---
type: Task
id: "native-cursor-data-path:network:t26"
chapter_id: network-engines
title: "Synchronous Oracle driver on the native cursor with leases at checkout"
description: "SyncOracleDriver query methods call the native cursor and the broker lease moves to client checkout."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t25"
  - "native-cursor-data-path:network:t21"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:211-335"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:447-460"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:514-672"
  - "src/py/vertebra/db/adapters/oracle/_sync.py:759-862"
  - "src/py/vertebra/db/adapters/oracle/core.py:969-993"
  - "src/py/vertebra/db/driver/_common.py:148-199"
  - "src/py/tests/unit/db/test_oracle_adapter.py"
files:
  - "src/py/vertebra/db/adapters/oracle/_sync.py"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 250
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
  - id: oracle-sync-driver
    resource: "src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver"
    title: "Native driver query path"
tags: [oracle, python, driver, lease]
---

# Synchronous Oracle driver on the native cursor with leases at checkout

## Objective & Scope Boundary

`execute`, `execute_many`, `execute_with_description`, and the `select` family on `SyncOracleDriver` run one native cursor call. Python no longer splits out-binds, coerces values, reads the SQL text, or takes a lease per statement. A broker lease is taken when a client is checked out and returned when it is released.

Out of scope:

- `core.py`. Task t27 deletes the helpers once the async driver stops calling them.
- `stream_arrow_batches`, `load_from_arrow`, notifications, queues, and pipelines.
- `select_to_arrow` beyond removing its per-statement lease and statement-cache calls.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/oracle/_sync.py:514-672` holds `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, and `select_to_arrow`.
- `src/py/vertebra/db/adapters/oracle/_sync.py:795-842` holds `_acquire_pool_client`, `_release_pool_client`, and `_checkout_client`.
- `src/py/vertebra/db/adapters/oracle/core.py:970-993` is `track_session_context_sql`, which stays and is called only when the cursor reports the flag.

## Contracts & Edge Cases

- Add `_cursor_options(self) -> dict[str, Any]` returning the four keyword arguments for `client.cursor` from `self.config.driver_features`: `enable_uuid_binary`, `fetch_decimals`, `vector_return_format`, `enable_lowercase_column_names`. The dict is built once in `__init__` and stored; the method returns the stored object.
- Add `cursor(self) -> SyncOracleCursor`. It returns a cursor on the pinned transaction client when one exists, else on `self._client` when the driver has no pool. With a pool and no pinned client it raises `InterfaceError` with the message `cursor() needs an open transaction on a pooled Oracle driver; use checkout_cursor()`.
- Add the public context manager `checkout_cursor(self) -> Iterator[SyncOracleCursor]`. It enters `_checkout_client()`, which applies pending session attributes, captures the piggyback, and discards a broken client as today. It yields `active_client.cursor(**self._cursor_options())` and closes the cursor on exit. The SQLSpec adapter uses it in t28.
- Add `_execute_cursor(self, sql, parameters, **kwargs) -> SyncOracleCursor` that runs inside `checkout_cursor()`, calls `execute(sql, parameters or (), out_binds=kwargs.get('out_binds'))`, stores `cursor.out_binds` on `self._last_out_binds`, and calls `OracleCore.track_session_context_sql(self._session_context, sql, parameters)` only when `cursor.touches_session_context` is true. Rows are fetched before the context exits, because exit closes the cursor: the helper returns `(rows, description, rowcount)` as a tuple in place of the cursor.
- `_execute_cursor` returns `tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]`. Rows are `cursor.fetchall()` when the description is not `None` and `[]` otherwise.
- `execute` returns `max(rowcount, 0)`.
- `execute_with_description` returns `(rows, names)` with names taken from the description, or `([], ())` when it is `None`.
- `execute_tuples` and `select` keep their return types. `select` returns `[]` for no rows and otherwise hydrates with `_hydrate_rows_with_schema`.
- `execute_many` runs inside `checkout_cursor()` and calls `executemany(sql, parameter_sets)`. It returns `max(cursor.rowcount, 0)`. An empty input returns `0` without a checkout.
- Lease with a pool: `_acquire_pool_client` calls `self._attach_checkout_lease(client)` after the pool returns a client. The driver stores no lease. If the call raises, the client is released to the pool first. `_release_pool_client` calls `client.release_lease()` before the client goes back to the pool. A discarded client releases through its guard.
- Lease without a pool: `connect` calls `self._attach_checkout_lease(client)` once after the client is ready; `close` calls `client.release_lease()`. The lease is held for the life of the client, so an idle open connection occupies a broker slot.
- A refused acquire attaches nothing and the driver proceeds. A driver dropped without `close()` releases through the client's guard.
- `cursor` and `checkout_cursor` take keyword-only `lazy_begin: bool = False` and pass it to the client cursor. On a pooled driver with no pinned client, `cursor(lazy_begin=True)` raises the same `InterfaceError` as `cursor()`.
- `__init__` sets `self._lease_at_checkout: bool = self._broker is not None`. When it is false, no lease call runs.
- Delete every `_lookup_or_cache_statement`, `_acquire_broker_lease`, and `_release_broker_lease` call inside `execute`, `execute_many`, `execute_with_description`, and `select_to_arrow`, and the `try` and `finally` blocks that existed only for them.
- `select_to_arrow` keeps calling `OracleCore.coerce_parameters` and its native Arrow method.
- Tests that assert a `vertebra._vertebra` error class from a query method stay unchanged. Tests that asserted statement-cache registration are deleted.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `driver.select('SELECT 1 AS a FROM dual')` | `[{'a': 1}]` |
| Upper-case names | Config with `enable_lowercase_column_names=False` | `[{'A': 1}]` |
| Tuples and names | `execute_with_description('SELECT 1 AS a, 2 AS b FROM dual')` | `([(1, 2)], ('a', 'b'))` |
| DML | `execute` of an `UPDATE` on two rows | `2` |
| DDL through description | `execute_with_description('CREATE TABLE ...')` | `([], ())` |
| Out-bind | `execute` of a block with a variable | Variable filled; `driver._last_out_binds` set |
| checkout_cursor | `with driver.checkout_cursor() as cur: cur.execute('SELECT 1 FROM dual')` on a pooled driver | Rows readable inside the block; client back in the pool after it |
| cursor on pooled driver | `driver.cursor()` with a pool and no transaction | `InterfaceError` |
| cursor in transaction | `driver.begin()` then `driver.cursor()` | Cursor on the pinned client |
| Session context | `DBMS_SESSION.SET_IDENTIFIER` block with a `value` bind | Local session map updated |
| Plain statement | A select | `track_session_context_sql` not called (assert with a spy) |
| execute_many | Three sets | `3` |
| No broker | Ten statements with `broker=None` | Recording broker double never called |
| Lease per checkout, pooled | Transaction with five statements | One acquire, one release |
| Lease released on error | Failing statement on a pooled checkout | Acquire count equals release count; client discarded per the existing fatal-error rule |
| Lease without pool | Ten statements on a single-client driver | One acquire at connect; one release at close |
| Acquire refused at connect | Broker double whose `acquire` raises `RuntimeError`; connect, then run a statement | The statement succeeds; no `attach_lease` call; zero releases at `close()` |
| Driver dropped without close | Recording broker; connect, run a statement, `del driver`, `gc.collect()` | One release recorded |
| Error class | ORA-00001 through `execute` | `IntegrityError` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/_sync.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/_sync.py`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/oracle/_sync.py)"`
- `test -z "$(git grep -n 'split_out_bind_parameters' -- src/py/vertebra/db/adapters/oracle/_sync.py)"`
- `test -z "$(git grep -n 'is_dml_or_plsql' -- src/py/vertebra/db/adapters/oracle/_sync.py)"`

## Acceptance Criteria

- [ ] `_sync.py` contains no per-statement lease or statement-cache call and no call to the out-bind, row-coercion, or statement-kind helpers.
- [ ] Lease tests use a recording broker double and assert call counts.
- [ ] Notification, queue, and Arrow tests pass unchanged.
