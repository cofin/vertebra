---
type: Task
id: "native-cursor-data-path:network:t28"
chapter_id: network-engines
title: "Oracle SQLSpec adapter on the native cursor and engine switch"
description: "Remove the Oracle SQLSpec adapter's parameter re-walk hooks and add oracle to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t27"
  - "native-cursor-data-path:network:t18"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:83-200"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:416-530"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py:196-235"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py:336-430"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:523-535"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
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
  - id: oracle-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py::OracleSyncDriver"
    title: "SQLSpec Oracle adapter"
tags: [oracle, sqlspec, python, cleanup]
---

# Oracle SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on Oracle passes SQLSpec's compiled SQL and parameters to the native cursor unchanged, and the DB-API connection and SQLAlchemy dialect for Oracle use the native cursor, including out-binds through `cursor.var`.

Out of scope:

- Any other edit to `_common.py`.
- The SQLSpec base class and the DB-API module (t37 to t39).
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py:83-200` and `:416-530` hold the sync and async overrides of the base hooks.
- `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py:196-216` holds `unwrap_typed_parameters` and `_normalize_bound_params`; `:370-430` holds `_unwrap_oracle_scalar`, `normalize_oracle_parameters`, and `normalize_oracle_many_parameters`.

## Contracts & Edge Cases

- Delete both `_prepare_native_parameters` overrides and any override of `_compile_for_native` or `_fetch_rows_with_columns`.
- An override of `dispatch_execute` or `dispatch_execute_many` that exists to pass Oracle options stays, and is rewritten to call `cursor.execute(sql, params)` or `cursor.executemany(sql, param_sets)` and to read `cursor.description`, `cursor.rowcount`, `cursor.fetchall()`, and `cursor.out_binds`. No `getattr` probe for method shapes remains.
- Delete `unwrap_typed_parameters`, `_normalize_bound_params`, `_unwrap_oracle_scalar`, `normalize_oracle_parameters`, and `normalize_oracle_many_parameters` from `core.py` and its `__all__`.
- Conversions those helpers did beyond unwrapping `TypedParameter` move into the type-coercion map built by `build_oracle_statement_config`. UUID, JSON, and vector values need no entry: the cursor coerces them (t22).
- Both adapter drivers override `with_cursor(self, connection)` to enter `connection.checkout_cursor()` and yield its cursor. This keeps one client checkout per statement on a pooled driver, with the lease, session attributes, and broken-client handling of t26, and the cursor carries the driver feature options. The async driver uses `async with`.
- `_map_exception` receives the same `vertebra._vertebra` error classes as before. Its class tests are not changed.
- Add `oracle` to `NATIVE_CURSOR_ENGINES`. Make no other change to `_common.py`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice, counting `execute` on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a FROM dual')` | `[{'a': 1}]` |
| Pooled session | Two statements on a pooled config outside a transaction | Each checks out and returns one client |
| Row count | `UPDATE` through `session.execute` | `rows_affected` equals rows touched |
| execute_many | Three sets | `rows_affected` 3 |
| Returning | `INSERT ... RETURNING id INTO :id` through the session | Returned id available as today |
| Integrity error | ORA-00001 | The SQLSpec class the adapter maps today |
| DB-API cursor type | `connect(oracle_url).cursor()` | Instance of `SyncOracleCursor` |
| DB-API out-bind | `cur.var(NUMBER)` used in a block | The object is a `vertebra._oracle.OracleOutBindVar`; `getvalue()` returns the value |
| SQLAlchemy dialect | Existing Oracle dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -q -k "oracle"`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q -k "oracle"`
- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py`
- `test -z "$(git grep -n 'unwrap_typed_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/oracle)"`
- `test -z "$(git grep -n '_prepare_native_parameters' -- src/py/vertebra/extensions/sqlspec/adapters/oracle)"`

## Acceptance Criteria

- [ ] The cache-hit test counts exactly one native `execute`.
- [ ] `NATIVE_CURSOR_ENGINES` contains `oracle`.
- [ ] Out-binds work through the DB-API connection and the SQLAlchemy dialect.
