---
type: Task
id: "native-cursor-data-path:wrapper:t02"
chapter_id: wrapper-layers
title: "SQLAlchemy asyncio adapter stops calling cursor.__aenter__"
description: "Make the SQLAlchemy asyncio cursor adapter return the DB-API cursor without calling cursor.__aenter__(), and test description with is not None, so it works over a native async cursor."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:wrapper:t01"
read_set:
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py:1-135"
  - "src/py/vertebra/extensions/sqlalchemy/dialects/_base.py:176-222"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:148-200"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:303-340"
files:
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 80
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sa-async-adapter
    resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py::AsyncAdapt_vertebra_cursor
    title: "SQLAlchemy asyncio cursor adapter"
tags: [sqlalchemy, dbapi, async]
---

# t02 — SQLAlchemy asyncio adapter stops calling `cursor.__aenter__`

## Objective & Scope Boundary

After this task, the SQLAlchemy asyncio adapter no longer depends on the cursor having `__aenter__`. Every engine keeps working, because the existing `AsyncDBAPICursor.__aenter__` only returns the cursor. This prepares the adapter for the native async cursor in t03, which has no `__aenter__`.

The adapter opens no transaction. The DB-API connection keeps begin-on-execute: the old cursor class awaits `begin()` in `execute`, and the native cursor opens the transaction inside the extension.

In scope: `AsyncAdapt_vertebra_cursor._aenter_cursor`, `_execute_async`, and `_executemany_async`. This is the only file in the SQLAlchemy extension that this chapter edits.

Out of scope:

- `src/py/vertebra/db/dbapi/_async.py` (t03).
- `src/py/vertebra/extensions/sqlalchemy/pool.py`, `dialects/`, and `types.py`. The sync dialects and `pool.py` are untouched. `pool.py` does not create or use cursors.
- `setinputsizes`, `var`, and `nextset` on the adapter cursor. Only the Oracle and SQL Server dialects call them.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlalchemy/async_adapter.py:1-135` | Imports and the cursor adapter |
| `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py:176-222` | Async dialect mixin, to confirm it needs no edit |
| `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:148-200`, `303-340` | Async SQLite transaction tests |

SQLAlchemy's `AsyncAdapt_dbapi_cursor.__init__` sets `self._connection`, calls `self._make_new_cursor(self._connection)`, then `self._aenter_cursor(cursor)`. Its default `_aenter_cursor` awaits `cursor.__aenter__()`.

## Contracts & Edge Cases

`src/py/vertebra/extensions/sqlalchemy/async_adapter.py`, in `AsyncAdapt_vertebra_cursor`:

```python
def _aenter_cursor(self, cursor: Any) -> Any:
    """Return the cursor unchanged; Vertebra cursors need no asynchronous entry."""
    return cursor
```

In `_execute_async` and `_executemany_async`, the two tests `if self._cursor.description:` become `if self._cursor.description is not None:`.

| Case | Behavior |
| --- | --- |
| Autocommit off, first statement of a transaction | The DB-API cursor opens the transaction when it executes. The adapter makes no `begin()` call |
| Cursor created after `commit()` or `rollback()` | Its first statement opens a new transaction |
| `AUTOCOMMIT` isolation level | No transaction is opened |
| Engines on the existing cursor class | `AsyncDBAPICursor.execute` still awaits `begin()` itself |
| Statement with an empty result set | `description` is not `None`, so the empty row list is fetched |

## Test Matrix

| Test | File | Asserts |
| --- | --- | --- |
| `test_async_adapter_never_calls_cursor_aenter` (new) | `test_sqlalchemy_vtb_dialects_and_dbapi.py` | With a `sqlite+vtb_async` engine and `AsyncDBAPICursor.__aenter__` patched to raise, a `select 1` through the engine still returns its row |
| `test_async_adapter_transaction_opens_on_first_statement` (new) | same | Inside `async with engine.connect()`, the driver reports `in_transaction` as `True` after the first statement; after `await conn.rollback()` an inserted row is gone |
| `test_async_adapter_autocommit_isolation_level_opens_no_transaction` (new) | same | With `isolation_level="AUTOCOMMIT"` on a file database under `tmp_path`: an insert is visible to a second engine before any commit, and the driver's `in_transaction` is `False` |
| `test_async_dialect_sqlite_transaction_ownership`, `test_sqlite_vtb_async_engine_ddl_dml_and_rollback` (existing) | same | Still pass unchanged |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/async_adapter.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/async_adapter.py
```

## Acceptance Criteria

- [ ] `AsyncAdapt_vertebra_cursor` never calls `cursor.__aenter__()` and never calls `begin()`.
- [ ] The three new tests pass, and the listed test files pass for every engine they cover.
- [ ] `async_adapter.py` is the only file under `src/py/vertebra/extensions/sqlalchemy/` with a diff.
