---
type: Task
id: "native-cursor-data-path:network:t10"
chapter_id: network-engines
title: "PostgreSQL SQLSpec adapter on the native cursor and engine switch"
description: "Remove the PostgreSQL SQLSpec adapter's parameter re-walk hooks and add postgres to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t09"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:30-92"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:179-196"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:203-265"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:352-369"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py:125-145"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py:300-360"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:523-535"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py"
  - "src/py/tests/unit/extensions/test_postgres_compiler_transport.py"
  - "src/py/tests/unit/db/test_dbapi_standalone.py"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
estimated_loc: 200
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: pg-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py::PostgresSyncDriver"
    title: "SQLSpec PostgreSQL adapter"
tags: [postgres, sqlspec, python, cleanup]
---

# PostgreSQL SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on PostgreSQL passes SQLSpec's compiled SQL and parameters to the native cursor unchanged. A cache hit performs one native `execute`. This is the task that the acceptance row "SQLSpec PostgreSQL async at or above 1.0x" depends on.

Adding `postgres` to `NATIVE_CURSOR_ENGINES` also moves the DB-API connection and the SQLAlchemy dialect for PostgreSQL onto the native cursor.

Out of scope:

- Any other edit to `_common.py`. Task t40 removes the constant and the unused helpers.
- The SQLSpec base class and the DB-API module. Tasks t37 to t39 remove their old paths.
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:73-75` and `:246-248` are the `_prepare_native_parameters` overrides.
- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py:179-196` and `:352-369` are the `dispatch_execute_many` overrides.
- `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py:125-145` and `:300-335` hold `unwrap_typed_parameters`, `_normalize_bound_params`, `normalize_pg_parameters`, and `normalize_pg_many_parameters`.
- The wrapper-layers chapter spec describes what `with_cursor`, `collect_rows`, and `resolve_rowcount` do for engines in `NATIVE_CURSOR_ENGINES`.

## Contracts & Edge Cases

- Delete both `_prepare_native_parameters` overrides.
- `dispatch_execute_many` on both drivers becomes: compile with `self._compiled_sql(statement, effective_config)`, return a zero-row result for an empty set, otherwise call `cursor.executemany(sql, prepared)` (awaited on the async driver) and build the result with `rowcount_override=max(cursor.rowcount, 0)` and `is_many_result=True`. No `getattr` probe remains.
- Delete `unwrap_typed_parameters`, `_normalize_bound_params`, `_unwrap_scalar`, `normalize_pg_parameters`, and `normalize_pg_many_parameters` from `core.py` and from its `__all__`.
- `compile_postgres_numeric_transport` returns `prepared.compile()` output directly as `(prepared, compiled_sql, bound)`.
- Any value conversion those helpers did beyond unwrapping `TypedParameter` is registered in the type-coercion map built by `build_postgres_statement_config`, so SQLSpec applies and caches it. Unwrapping `TypedParameter` itself is done by SQLSpec's parameter processing when the statement config sets the execution style; confirm with the cache-hit test below.
- `_map_exception` keeps mapping to SQLSpec exception classes. `map_postgres_exception` reads `sqlstate` from the error the cursor raises. The cursor raises the same `vertebra._vertebra` classes as before, so its class tests are not changed.
- Add the string `postgres` to `NATIVE_CURSOR_ENGINES` in `_common.py`. Use the engine key the wrapper-layers chapter uses for SQLite and DuckDB entries: the lowercase `dialect` class attribute of the engine's config. Make no other change to that file.
- After the switch, a DB-API connection for a PostgreSQL URL returns the native cursor from `connection.cursor()`, per the wrapper-layers contract.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice on one session, counting `execute` calls on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Row count | `session.execute` of an `UPDATE` | `rows_affected` equals rows touched |
| execute_many | Three parameter sets | `rows_affected` 3 |
| Typed parameter | A `TypedParameter` bound to a `jsonb` column | Stored value round-trips |
| Integrity error | Duplicate key | SQLSpec `UniqueViolationError` or the class the adapter maps today |
| Async session | Same cases on `PostgresAsyncDriver` | Same results |
| DB-API cursor type | `connect(postgres_url).cursor()` | Instance of `SyncPostgresCursor` |
| SQLAlchemy dialect | Existing PostgreSQL dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -q -k "postgres"`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q -k "postgres"`
- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py`
- `test -z "$(git grep -n 'unwrap_typed_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/postgres)"`
- `test -z "$(git grep -n '_prepare_native_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/postgres)"`

## Acceptance Criteria

- [ ] The cache-hit test counts exactly one native `execute`.
- [ ] No removed helper is referenced under the PostgreSQL SQLSpec adapter package.
- [ ] `NATIVE_CURSOR_ENGINES` contains `postgres` and the DB-API and SQLAlchemy PostgreSQL tests pass.

## Notes & Discoveries

- `SyncDBAPIConnection.cursor()` and `AsyncDBAPIConnection.cursor()` now fall back to `lazy_begin=False` when a pooled driver raises `InterfaceError` for `lazy_begin=True`.
- Register `TypedParameter` in `ParameterStyleConfig.type_coercion_map` inside `build_postgres_statement_config()` so compiled parameters are unwrapped without a Python loop.
- Update the fake drivers in `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` and `src/py/tests/unit/db/test_dbapi_standalone.py` to expose `cursor()`, update the `NATIVE_CURSOR_ENGINES` assertion in `test_dbapi_standalone.py`, and drop `postgres` from `test_isolated_adapter_unwrap_typed_parameters` in `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py`.
