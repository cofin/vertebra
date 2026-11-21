---
type: Task
id: "native-cursor-data-path:network:t14"
chapter_id: network-engines
title: "MySQL SQLSpec adapter on the native cursor and engine switch"
description: "Remove the MySQL SQLSpec adapter's parameter re-walk hooks and add mysql to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t13"
  - "native-cursor-data-path:network:t10"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py:28-155"
  - "src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py:172-240"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:523-535"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py"
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
  - id: mysql-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py::MySQLSyncDriver"
    title: "SQLSpec MySQL adapter"
tags: [mysql, sqlspec, python, cleanup]
---

# MySQL SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on MySQL passes SQLSpec's compiled SQL and parameters to the native cursor unchanged, and the DB-API connection and SQLAlchemy dialect for MySQL use the native cursor.

Out of scope:

- Any other edit to `_common.py`.
- The SQLSpec base class and the DB-API module (t37 to t39).
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py:70-91` and `:135-155` are the `_prepare_native_parameters` and `dispatch_execute_many` overrides.
- `src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py:193-240` holds `unwrap_typed_parameters`, `normalize_mysql_parameters`, and `normalize_mysql_many_parameters`.

## Contracts & Edge Cases

- Delete both `_prepare_native_parameters` overrides.
- `dispatch_execute_many` on both drivers: compile with `self._compiled_sql(statement, effective_config)`; return a zero-row result for an empty set; otherwise call `cursor.executemany(sql, prepared)` (awaited on the async driver) and build the result with `rowcount_override=max(cursor.rowcount, 0)` and `is_many_result=True`.
- Delete `unwrap_typed_parameters`, `normalize_mysql_parameters`, and `normalize_mysql_many_parameters` from `core.py` and its `__all__`.
- Conversions those helpers did beyond unwrapping `TypedParameter` move into the type-coercion map built by `build_statement_config`. JSON, UUID, and buffer values need no entry: the cursor coerces them (t11).
- If t13 kept `MySQLCore.compile_statement` because this adapter calls it, replace that call with SQLSpec's own compiled output and leave the function in place; the cleanup chapter audits it.
- `_map_exception` receives the same `vertebra._vertebra` error classes as before. Its class tests are not changed.
- Add `mysql` to `NATIVE_CURSOR_ENGINES`, plus `mariadb` if the MySQL config class serves that dialect key in the same set the wrapper-layers chapter keyed for SQLite and DuckDB. Make no other change to `_common.py`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice, counting `execute` on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Row count | `UPDATE` through `session.execute` | `rows_affected` equals rows touched |
| execute_many | Three sets | `rows_affected` 3 |
| JSON parameter | A `dict` bound to a JSON column | Round-trips |
| Integrity error | Duplicate key | The SQLSpec class the adapter maps today |
| DB-API cursor type | `connect(mysql_url).cursor()` | Instance of `SyncMySQLCursor` |
| SQLAlchemy dialect | Existing MySQL dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py -q -k "mysql"`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q -k "mysql"`
- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py`
- `test -z "$(git grep -n 'unwrap_typed_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/mysql)"`
- `test -z "$(git grep -n '_prepare_native_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/mysql)"`

## Acceptance Criteria

- [ ] The cache-hit test counts exactly one native `execute`.
- [ ] `NATIVE_CURSOR_ENGINES` contains `mysql`.
