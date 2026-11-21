---
type: Task
id: "native-cursor-data-path:network:t18"
chapter_id: network-engines
title: "SQL Server SQLSpec adapter on the native cursor and engine switch"
description: "Remove the SQL Server SQLSpec adapter's parameter re-walk hooks and add mssql to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t17"
  - "native-cursor-data-path:network:t14"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py:28-205"
  - "src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py:154-230"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:523-535"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
estimated_loc: 180
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
  - id: mssql-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py::MSSQLSyncDriver"
    title: "SQLSpec SQL Server adapter"
tags: [mssql, sqlspec, python, cleanup]
---

# SQL Server SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on SQL Server passes SQLSpec's compiled SQL and parameters to the native cursor unchanged, and the DB-API connection and SQLAlchemy dialect for SQL Server use the native cursor.

Out of scope:

- Any other edit to `_common.py`.
- The SQLSpec base class and the DB-API module (t37 to t39).
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py:63-117` and `:118-205` hold the sync and async overrides of `_prepare_native_parameters` and `dispatch_execute_many`.
- `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py:178-230` holds `unwrap_typed_parameters`, `normalize_mssql_parameters`, and `normalize_mssql_many_parameters`.

## Contracts & Edge Cases

- Delete both `_prepare_native_parameters` overrides.
- `dispatch_execute_many` on both drivers: compile with `self._compiled_sql(statement, effective_config)`; return a zero-row result for an empty set; otherwise call `cursor.executemany(sql, prepared)` (awaited on the async driver) and build the result with `rowcount_override=max(cursor.rowcount, 0)` and `is_many_result=True`.
- Any other override in `driver.py` that exists only to call a deleted helper is deleted with it. An override that changes behavior (for example identity retrieval) stays and reads `cursor.description`, `cursor.rowcount`, and `cursor.fetchall()`.
- Delete `unwrap_typed_parameters`, `normalize_mssql_parameters`, and `normalize_mssql_many_parameters` from `core.py` and its `__all__`. Conversions beyond unwrapping `TypedParameter` move into the type-coercion map built by `build_statement_config`.
- `_map_exception` receives the same `vertebra._vertebra` error classes as before. Its class tests are not changed.
- Add `mssql` to `NATIVE_CURSOR_ENGINES`. Make no other change to `_common.py`.
- The DB-API module's SQL Server autocommit handling (`requires_statement_autocommit`) is owned by the wrapper-layers contract and is not changed here. If a SQL Server DB-API test fails after the switch because of it, mark the task blocked with the failing test name.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice, counting `execute` on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Row count | `UPDATE` through `session.execute` | `rows_affected` equals rows touched |
| execute_many | Three sets | `rows_affected` 3 |
| OUTPUT rows | `INSERT ... OUTPUT inserted.id` through `session.select` | Rows returned |
| Integrity error | Key violation | The SQLSpec class the adapter maps today |
| DB-API cursor type | `connect(mssql_url).cursor()` | Instance of `SyncMSSQLCursor` |
| SQLAlchemy dialect | Existing SQL Server dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py -q -k "mssql"`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q -k "mssql"`
- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py`
- `test -z "$(git grep -n 'unwrap_typed_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/mssql)"`
- `test -z "$(git grep -n '_prepare_native_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/mssql)"`

## Acceptance Criteria

- [x] The cache-hit test counts exactly one native `execute`.
- [x] `NATIVE_CURSOR_ENGINES` contains `mssql`.
