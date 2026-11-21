---
type: Task
id: "native-cursor-data-path:cleanup:t15"
chapter_id: cleanup-and-checks
title: "PostgreSQL pool double and broker assertions on the cursor contract"
description: "Give the embedding pushdown pool double a pool cursor and rewrite the PostgreSQL broker test to assert one lease per checkout and no shared-memory statement cache entry."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::_NativeEmbeddingQueryPort"
  - "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::test_postgres_sync_embedding_pushdown_binds_with_native_dollar_pool"
  - "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::test_postgres_sync_similarity_search_binds_with_native_dollar_pool"
  - "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py:40-48"
  - "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._execute_cursor"
  - "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._execute_on_pool"
  - "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._ensure_pool"
  - "src/py/tests/unit/db/postgres_ports.py::FakeSyncPostgresPool.cursor"
  - "src/py/tests/unit/db/test_postgres_driver.py::test_postgres_drivers_discard_statement_plan_and_release_real_broker_leases"
  - "src/py/tests/unit/db/test_postgres_native_adapters.py::test_sync_broker_checkout_lease_counts"
files: []
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_postgres_driver.py
estimated_loc: 80
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: pool-double
    resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::_NativeEmbeddingQueryPort
    title: "Pool double for the embedding pushdown tests"
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._execute_cursor
    title: "Driver path that runs an autocommit statement on a pool cursor"
  - id: broker-test
    resource: src/py/tests/unit/db/test_postgres_driver.py::test_postgres_drivers_discard_statement_plan_and_release_real_broker_leases
    title: "Broker test that still asserts a shared-memory statement cache hit"
tags: [testing, database, postgres, lease]
---

# t15 — PostgreSQL pool double and broker assertions on the cursor contract

## Objective & Scope Boundary

Three tests pass again: the two embedding pushdown tests and the PostgreSQL broker test.[^pool-double][^broker-test]

Neither failure is a production defect:

- With no broker and no client factory, `SyncPostgreSQLDriver` runs an autocommit statement on `pool.cursor()`. The pool double only has the removed `query` method.[^pg-driver]
- With a broker, the driver takes one lease per checkout, which the test already counts correctly. It no longer writes to the shared-memory statement cache, which the test still expects.

In scope: the two test files. Out of scope: every production file, `postgres_ports.py`, and every other test.

## Bounded Read Manifest

- `_NativeEmbeddingQueryPort`, its two tests, and lines `40-48` of the file for the import this task removes.
- `_execute_cursor`, `_execute_on_pool`, and `_ensure_pool` in the sync PostgreSQL driver for the calls the double must answer. `_ensure_pool` requires `acquire` on an injected pool.
- `FakeSyncPostgresPool.cursor` as the reference for the pool cursor factory signature.
- The broker test, and `test_sync_broker_checkout_lease_counts` for the lease contract it already covers with a recording double.

## Contracts & Edge Cases

### Pool double in `test_pg_dataplane_and_datafusion.py`

Add one module-level class above `_NativeEmbeddingQueryPort`:

```python
class _NativeEmbeddingQueryCursor:
    """Pool cursor double that records one statement on its owning pool double."""

    description: tuple[tuple[Any, ...], ...] | None
    rowcount: int
    lastrowid: Any | None
    arraysize: int

    def __init__(self, port: _NativeEmbeddingQueryPort) -> None: ...

    def execute(
        self,
        sql: str,
        params: Any = (),
        *,
        parameter_oids: Any = None,
        timeout: float | None = None,
    ) -> _NativeEmbeddingQueryCursor: ...

    def executemany(self, sql: str, seq_of_params: Any) -> _NativeEmbeddingQueryCursor: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...
```

- `__init__` sets `description = None`, `rowcount = -1`, `lastrowid = None`, `arraysize = 1`, and an empty row buffer.
- `execute` discards `parameter_oids` and `timeout`, appends `(sql, tuple(params))` to `port.calls`, sets `description` to one tuple `(name, 25, None, None, None, None, None)` per column name, buffers a copy of the port's rows, sets `rowcount` to the row count, and returns `self`.
- `executemany` calls `execute` once per parameter set, then sets `description = None` and clears the buffer.
- `fetchone` pops the first buffered row or returns `None`. `fetchmany` returns up to `size` rows, or `arraysize` rows when `size` is `None`. `fetchall` returns and clears the buffer. `close` clears the buffer.

Change `_NativeEmbeddingQueryPort`:

- `__slots__` becomes `("calls", "columns", "rows")`.
- `__init__` stores `self.columns = columns` (the tuple of names) and `self.rows = tuple(tuple(r) for r in rows)`. It no longer builds `PostgresColumn` objects.
- Delete `query`. Nothing calls it.
- Add `def cursor(self, *, lazy_begin: bool = False) -> _NativeEmbeddingQueryCursor`. It discards `lazy_begin` and returns `_NativeEmbeddingQueryCursor(self)`.
- Keep `acquire` and `close` as they are. `_ensure_pool` rejects an injected pool without `acquire`.
- Class docstring: `Native pool port handing out a recording pool cursor for embedding pushdown tests.`

Delete the import line `from vertebra.db.adapters.postgres.types import PostgresColumn, PostgresQueryResult`. Neither name has another use in the file.

The two test bodies and their assertions do not change. `port.calls[0]` is still `(sql, parameters)` with the parameters as a tuple.

### Broker test in `test_postgres_driver.py`

`test_postgres_drivers_discard_statement_plan_and_release_real_broker_leases` becomes `test_postgres_drivers_take_one_lease_per_checkout_and_cache_no_statement_plan`.

- Docstring: `Sync and async PostgreSQL drivers take one broker lease per checkout and write no shared-memory statement plan.`
- Sync half: replace `assert sc.stats().hits == 1` with `assert sc.stats().hits == 0` and `assert sc.stats().inserts == 0`. Keep `active_leases == 0`, `total_acquired == 3`, and `total_released == 3`.
- Async half: replace `assert async_sc.stats().hits == 1` with `assert async_sc.stats().hits == 0` and `assert async_sc.stats().inserts == 0`. Keep `active_leases == 0`, `total_acquired == 2`, and `total_released == 2`.
- Nothing else in the test changes. `LeaseBroker` stays imported.

Edge cases:

- This task runs before the `single-call-paths` driver tasks. Those tasks own adding the single-call methods to this double when they route the driver through them.
- The lease counts hold because each autocommit statement is one checkout. A different count after this edit means the driver changed; stop and report the observed numbers instead of editing the expected ones.
- No inline comment. Each new class and public method has a one-line docstring.

## Test Matrix

| Test | Expected |
| --- | --- |
| `test_postgres_sync_embedding_pushdown_binds_with_native_dollar_pool` | Passes; one recorded call with parameters `("gemini-embedding-001", ["document"])` and SQL holding `$1` and `$2` |
| `test_postgres_sync_similarity_search_binds_with_native_dollar_pool` | Passes; one recorded call with parameters `([3.0, 4.0], 2)` |
| `test_postgres_drivers_take_one_lease_per_checkout_and_cache_no_statement_plan` | Passes; statement cache `hits` and `inserts` are `0`; leases `3` and `3` sync, `2` and `2` async |
| The old broker test name | Not collected |
| Every other test in the two files | Unchanged result |
| `mypy` and `ruff check` | No error or finding in the two files |

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_driver.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_adapters.py -p no:xdist -q -k "broker_checkout_lease_counts"
uv run --no-sync --frozen ruff check src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_driver.py
uv run --no-sync --frozen mypy | grep "test_pg_dataplane_and_datafusion.py\|test_postgres_driver.py"
git diff --stat -- src/py/vertebra src/rs
```

The first two commands report no failure. The `grep` and the last command print nothing.

## Acceptance Criteria

- [ ] The three tests pass.
- [ ] The pool double has `cursor(*, lazy_begin=False)` and `acquire`, and no `query`.
- [ ] The broker test asserts zero statement cache hits and inserts, and the lease counts it asserted before.
- [ ] No production file has a diff from this task.

[^pool-double]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::_NativeEmbeddingQueryPort`
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._execute_cursor`
[^broker-test]: `src/py/tests/unit/db/test_postgres_driver.py::test_postgres_drivers_discard_statement_plan_and_release_real_broker_leases`
