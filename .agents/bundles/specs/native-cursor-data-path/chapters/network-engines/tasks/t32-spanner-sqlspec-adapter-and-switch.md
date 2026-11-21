---
type: Task
id: "native-cursor-data-path:network:t32"
chapter_id: network-engines
title: "Spanner SQLSpec adapter on the native cursor and engine switch"
description: "Move the Spanner SQLSpec adapter off the base compile and row-probe hooks and add spanner to NATIVE_CURSOR_ENGINES."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t31"
  - "native-cursor-data-path:network:t28"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:232-360"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:455-520"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:625-645"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:761-890"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py:115-240"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:536-547"
  - "src/py/vertebra/db/driver/_common.py:25-55"
files:
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py"
estimated_loc: 260
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
  - id: spanner-sqlspec
    resource: "src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py::SpannerSyncDriver"
    title: "SQLSpec Spanner adapter"
tags: [spanner, sqlspec, python, cleanup]
---

# Spanner SQLSpec adapter on the native cursor and engine switch

## Objective & Scope Boundary

A SQLSpec session on Spanner runs statements through the native cursor. The adapter keeps one compile step of its own, because Spanner needs typed parameter JSON that SQLSpec does not produce, and stops using the base class hooks that task t39 deletes.

Out of scope:

- Any other edit to `_common.py`.
- The SQLSpec base class and the DB-API module (t37 to t39).
- Every other module in the adapter directory.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py:332-345` and `:861-875` are the `_compile_for_native` overrides and their first callers. Other callers are at `:463`, `:487`, `:507`, `:634`, `:992`, `:1016`, `:1036`, and `:1163`.
- `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py:115-240` holds the statement config builders and key normalization.

## Contracts & Edge Cases

- Add `compile_spanner_statement(statement: SQL, *, dialect: str) -> tuple[str, str | None]` to `core.py`. It calls `statement.compile()` once and returns the SQL and the parameter JSON from `SpannerCore.encode_parameters_json`. It unwraps `TypedParameter` values while encoding, in the same pass.
- Both `_compile_for_native` overrides are deleted. Every caller listed in the manifest calls `compile_spanner_statement(statement, dialect=self.dialect)`.
- `dispatch_execute` on both drivers calls `cursor.execute(sql, params_json)`. When `cursor.description` is not `None` it builds a select result from `cursor.fetchall()` and the description names; otherwise it builds a row-count result from `max(cursor.rowcount, 0)`. `statement.returns_rows()` is not consulted.
- `dispatch_execute_many` calls `cursor.executemany(sql, [json for each set])` and reads `cursor.rowcount`.
- Both drivers override `with_cursor(self, connection)` to yield `connection.cursor()` and close it on exit, passing the session's staleness and tags when the driver holds them.
- Methods that call native driver features with no cursor form (mutations, partitioned DML, Arrow, DDL batches) keep their calls.
- `_map_exception` receives the same `vertebra._vertebra` error classes as before. Its class tests are not changed, and the `code` attribute checks stay.
- Add `spanner` to `NATIVE_CURSOR_ENGINES`. If the Spanner config class reports a second dialect key for the PostgreSQL-compatible dialect, add that key too. Make no other change to `_common.py`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Same `select` twice, counting `execute` on a wrapped cursor | Second call performs one `execute` and one `fetchall` |
| Rows | `session.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| DML | `UPDATE` through `session.execute` | `rows_affected` equals rows touched |
| DML returning | `INSERT ... THEN RETURN id` through `session.select` | Rows returned |
| execute_many | Three sets | `rows_affected` 3 |
| Typed parameter | A `TypedParameter` holding a timestamp | Encoded with its Spanner type |
| Already exists | Duplicate key | The SQLSpec class the adapter maps today |
| DB-API cursor type | `connect(spanner_url).cursor()` | Instance of `SyncSpannerCursor` |
| SQLAlchemy dialect | Existing Spanner dialect tests | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner_adapter.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q -k "spanner"`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py`
- `test -z "$(git grep -n '_compile_for_native' -- src/py/vertebra/extensions/sqlspec/adapters/spanner)"`
- `test -z "$(git grep -n '_fetch_rows_with_columns' -- src/py/vertebra/extensions/sqlspec/adapters/spanner)"`

## Acceptance Criteria

- [ ] The cache-hit test counts exactly one native `execute`.
- [ ] The Spanner adapter package has no reference to `_compile_for_native`.
- [ ] `NATIVE_CURSOR_ENGINES` contains `spanner`.
