---
type: Task
id: "native-cursor-data-path:network:t36"
chapter_id: network-engines
title: "BigQuery SQLSpec adapter on the native cursor and engine switch"
description: "Move the BigQuery SQLSpec adapter off the base compile and row-probe hooks and add bigquery to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t35"
  - "native-cursor-data-path:network:t32"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:185-200"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:249-385"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:405-420"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:540-560"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:617-750"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py:132-170"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:536-547"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py"
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
  - id: bigquery-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py::BigQuerySyncDriver"
    title: "SQLSpec BigQuery adapter"
tags: [bigquery, sqlspec, python, cleanup]
---

# BigQuery SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on BigQuery runs statements through the native cursor. The adapter keeps one compile step of its own for typed parameter JSON and stops using the base class hooks that task t39 deletes.

Out of scope:

- Any other edit to `_common.py`.
- The SQLSpec base class and the DB-API module (t37 to t39).
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:307-310` and `:675-678` are the `_compile_for_native` overrides, which call the base implementation. Callers are at `:197`, `:313`, `:413`, `:554`, `:681`, `:781`, and `:935`.
- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py:311-382` and `:679-750` hold the three dispatch overrides for each driver.

## Contracts & Edge Cases

- Add `compile_bigquery_statement(statement: SQL) -> tuple[str, Any]` to `core.py`. It calls `statement.compile()` once, unwraps `TypedParameter` values in one pass, and applies whatever the two overrides apply today after the base call.
- Both `_compile_for_native` overrides are deleted. Every caller listed in the manifest calls `compile_bigquery_statement(statement)`.
- `dispatch_execute` on both drivers calls `cursor.execute(sql, params)`. With a description it builds a select result from `cursor.fetchall()` and the description names; otherwise a row-count result from `max(cursor.rowcount, 0)`. `statement.returns_rows()` is not consulted.
- `dispatch_execute_many` calls `cursor.executemany(sql, param_sets)` and reads `cursor.rowcount`. `dispatch_execute_script` keeps its statement splitting and runs each statement through `cursor.execute`.
- Both drivers override `with_cursor(self, connection)` to yield `connection.cursor()` and close it on exit. The native driver's `cursor()` supplies the parameter encoder and job options (t35).
- After each cursor call the adapter calls the native driver's `_after_query(cursor.query_metadata)` so session ids and cost metrics keep being recorded.
- `_map_exception` receives the same `vertebra._vertebra` error classes as before. Its class tests are not changed, and the `code` and `reason` attribute checks stay.
- Add `bigquery` to `NATIVE_CURSOR_ENGINES`. Make no other change to `_common.py`. After this task the set holds all eight engines.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice, counting `execute` on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| DML | `UPDATE` through `session.execute` | `rows_affected` equals rows touched |
| Script | Two statements through `execute_script` | Both run |
| Typed parameter | A `TypedParameter` holding a date | Encoded with its BigQuery type |
| Cost metadata | After a select | Native driver `last_query_metadata` updated |
| Invalid query | `SELEC 1` | The SQLSpec class the adapter maps today |
| DB-API cursor type | `connect(bigquery_url).cursor()` | Instance of `SyncBigQueryCursor` |
| SQLAlchemy dialect | Existing BigQuery dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery_adapter.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q -k "bigquery"`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py`
- `test -z "$(git grep -n '_compile_for_native' -- src/py/vertebra/extensions/sqlspec/adapters/bigquery)"`
- `test -z "$(git grep -n '_fetch_rows_with_columns' -- src/py/vertebra/extensions/sqlspec/adapters/bigquery)"`

## Acceptance Criteria

- [ ] The cache-hit test counts exactly one native `execute`.
- [ ] The BigQuery adapter package has no reference to `_compile_for_native`.
- [ ] `NATIVE_CURSOR_ENGINES` holds eight engine names.
