---
type: Task
id: "native-cursor-data-path:cleanup:t17"
chapter_id: cleanup-and-checks
title: "Typed Oracle test doubles"
description: "Clear the fifteen mypy errors that come from the Oracle stub doubles: the context-manager return type, the untyped keyword dict, stub cursor attributes read through the native cursor type, and two overrides with a narrower argument type."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleCursor"
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient.cursor"
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient.query"
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient.execute"
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubAsyncOracleCursor"
  - "src/py/tests/unit/db/test_oracle_adapter.py:14-32"
  - "src/py/tests/unit/db/test_oracle_adapter.py:2380-2412"
  - "src/py/tests/unit/db/test_oracle_adapter.py:2680-2708"
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py::_OracleMatrixStubClient"
files: []
tests:
  - src/py/tests/unit/db/oracle_test_doubles.py
  - src/py/tests/unit/db/test_oracle_adapter.py
  - src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py
estimated_loc: 45
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: oracle-doubles
    resource: src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient
    title: "Shared Oracle stub client and cursor doubles"
  - id: oracle-adapter-tests
    resource: src/py/tests/unit/db/test_oracle_adapter.py
    title: "Oracle adapter tests that read stub cursor attributes"
  - id: stacks-tests
    resource: src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py::_OracleMatrixStubClient
    title: "Stub client subclass with narrower overrides"
tags: [testing, typing, oracle]
---

# t17 — Typed Oracle test doubles

## Objective & Scope Boundary

`mypy` reports no error in `oracle_test_doubles.py`, `test_oracle_adapter.py`, and `test_sqlspec_vtb_stacks.py`. It reports fifteen there today.[^oracle-doubles][^oracle-adapter-tests][^stacks-tests]

Every fix is an annotation or a narrowing in a test file. No test changes what it asserts. No production file changes, and no stub entry in `src/py/vertebra/_oracle.pyi` changes.

Out of scope: the same `override` error in `test_sqlalchemy_vtb_dialects_and_dbapi.py`, which t18 fixes in its own file.

## Bounded Read Manifest

- `_StubSyncOracleCursor`, `_StubAsyncOracleCursor`, and `_StubSyncOracleClient.cursor`, `.query`, and `.execute` in the doubles file.
- Lines `14-32`, `2380-2412`, and `2680-2708` of `test_oracle_adapter.py`: the imports and the two pooled cursor tests.
- `_OracleMatrixStubClient` in `test_sqlspec_vtb_stacks.py`.

## Contracts & Edge Cases

### `oracle_test_doubles.py`, three errors

1. `exit-return` on `_StubSyncOracleCursor.__exit__`. The signature becomes `def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None`. The body is `self.close()`. The `return False` line is deleted.
2. Two `arg-type` errors on `_StubSyncOracleCursor(self, **opts)` in `_StubSyncOracleClient.cursor`. Annotate the local: `opts: dict[str, Any] = {...}`. The dict contents, the `cursor_calls.append(opts)` line, and the constructor call stay.

### `test_oracle_adapter.py`, ten errors

The pooled driver's `checkout_cursor()` and `cursor()` are typed to return the native cursor class. In these two tests the object is the stub cursor, and the tests read stub-only attributes.

- Add `cast` to the `typing` import: `from typing import TYPE_CHECKING, Any, cast`.
- Add `_StubAsyncOracleCursor` and `_StubSyncOracleCursor` to the import from `tests.unit.db.oracle_test_doubles`, in the list's existing sort order.

Sync test, around lines `2392-2408`:

- First line inside `with pooled_driver.checkout_cursor(lazy_begin=lazy_begin) as cur:` becomes `stub_cur = cast("_StubSyncOracleCursor", cur)`.
- `cur._client.queued_results.append(...)` becomes `stub_cur._client.queued_results.append(...)`.
- `assert cur.lazy_begin is lazy_begin` becomes `assert stub_cur.lazy_begin is lazy_begin`.
- `assert cur.closed is True` after the block becomes `assert stub_cur.closed is True`.
- `tx_cur = pooled_driver.cursor(lazy_begin=lazy_begin)` becomes `tx_cur = cast("_StubSyncOracleCursor", pooled_driver.cursor(lazy_begin=lazy_begin))`. The three lines that use `tx_cur` stay.

Async test, around lines `2688-2704`:

- First line inside `async with pooled_driver.checkout_cursor(lazy_begin=lazy_begin) as cur:` becomes `stub_cur = cast("_StubAsyncOracleCursor", cur)`.
- `cur._sync._client.queued_results.append(...)` becomes `stub_cur._sync._client.queued_results.append(...)`.
- `assert cur.lazy_begin is lazy_begin` becomes `assert stub_cur.lazy_begin is lazy_begin`.
- `assert cur.closed is True` after the block becomes `assert stub_cur.closed is True`.
- `tx_cur = pooled_driver.cursor(lazy_begin=lazy_begin)` becomes `tx_cur = cast("_StubAsyncOracleCursor", pooled_driver.cursor(lazy_begin=lazy_begin))`. The three lines that use `tx_cur` stay.

`cur.execute(...)` and `cur.fetchall()` keep using `cur`.

### `test_sqlspec_vtb_stacks.py`, two errors

`_OracleMatrixStubClient.query` and `_OracleMatrixStubClient.execute` override the stub client with `enable_lowercase_column_names: bool = True`. The stub declares `bool | None = None`. Change the parameter in both overrides to `enable_lowercase_column_names: bool | None = None`. Both bodies discard the value, so nothing else changes.

Edge cases:

- This task runs before any `single-call-paths` task starts, so the three files are as the planning tree has them.
- When `mypy` reports a further error in these three files after the edits, fix it the same way: an annotation or a `cast` in the test file. A fix that needs a production or stub change stops the task with the error text.
- No `# type: ignore` is added.

## Test Matrix

| Check | Expected |
| --- | --- |
| `mypy` | No error in the three files |
| `pytest` on the three files | Same pass count as before the task; no new failure |
| `ruff check` on the three files | No finding |
| `git grep -n "type: ignore" -- <the three files>` | No new match |

## Verification

```bash
uv run --no-sync --frozen mypy | grep "oracle_test_doubles.py\|test_oracle_adapter.py\|test_sqlspec_vtb_stacks.py"
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/tests/unit/db/oracle_test_doubles.py src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py
git diff --stat -- src/py/vertebra src/rs
```

The `grep` prints nothing. `pytest` reports no failure. The last command prints nothing.

## Acceptance Criteria

- [ ] `mypy` reports no error in the three files.
- [ ] No assertion changed meaning: the two pooled cursor tests check the same attributes on the same objects.
- [ ] No `# type: ignore`, and no production or stub file has a diff from this task.

[^oracle-doubles]: `src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient`
[^oracle-adapter-tests]: `src/py/tests/unit/db/test_oracle_adapter.py`
[^stacks-tests]: `src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py::_OracleMatrixStubClient`
