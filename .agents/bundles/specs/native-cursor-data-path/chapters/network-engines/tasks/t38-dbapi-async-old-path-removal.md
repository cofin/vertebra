---
type: Task
id: "native-cursor-data-path:network:t38"
chapter_id: network-engines
title: "Delete the old asynchronous DB-API cursor path and map errors at connect time"
description: "Delete AsyncDBAPICursor and the engine-set branch from dbapi/_async.py so the async connection returns the native async cursor only, and add the connect-time error mapping that replaces the translator."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t36"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/dbapi/_async.py:1-60"
  - "src/py/vertebra/db/dbapi/_async.py:147-426"
  - "src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection"
  - "src/py/vertebra/db/dbapi/_async.py::_build_async_driver"
  - "src/py/vertebra/db/dbapi/_sync.py::_ErrorTranslator"
  - "src/py/vertebra/db/dbapi/_sync.py::_translate_vertebra_error"
  - "src/py/vertebra/db/dbapi/__init__.py"
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py:1-80"
files:
  - "src/py/vertebra/db/dbapi/_async.py"
  - "src/py/vertebra/extensions/sqlalchemy/async_adapter.py"
  - "src/py/vertebra/db/dbapi/_sync.py"
  - "src/py/vertebra/db/dbapi/__init__.py"
tests:
  - "src/py/tests/unit/db/test_dbapi_standalone.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
  - "src/py/tests/unit/extensions/test_postgres_compiler_transport.py"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
  - "src/py/tests/unit/db/test_postgres_native_queries.py"
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py"
estimated_loc: 220
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
  - id: dbapi-async
    resource: "src/py/vertebra/db/dbapi/_async.py::AsyncDBAPICursor"
    title: "Old async Python cursor"
tags: [dbapi, python, async, cleanup]
---

# Delete the old asynchronous DB-API cursor path and map errors at connect time

## Objective & Scope Boundary

The asynchronous DB-API module holds the async connection and module object only. `AsyncDBAPIConnection.cursor()` returns the engine's native async cursor. This task runs before t37, because `dbapi/_async.py` imports the helpers t37 deletes. It adds `_connect_error` to `dbapi/_sync.py` and makes no other edit there.

Out of scope:

- `AsyncSQLDriverProtocol`. The cleanup chapter trims driver protocol members.
- `extensions/sqlalchemy/async_adapter.py`. The wrapper-layers chapter already wraps the awaitable cursor.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/dbapi/_async.py:147-426` is `AsyncDBAPICursor` with `_resolve_driver_lastrowid`.
- The wrapper-layers chapter spec names the branch on `NATIVE_CURSOR_ENGINES` it added to `AsyncDBAPIConnection`.

## Contracts & Edge Cases

- Delete the class `AsyncDBAPICursor` and every import it alone used from `_sync.py`.
- In `_sync.py`, add `def _connect_error(exc: Exception) -> Exception | None` with the docstring "Map a configuration or driver-build failure to its PEP 249 class, or return None to re-raise it unchanged." It returns `None` for a `DatabaseError` or `Warning` instance. It returns `InterfaceError(str(exc))` for `ImproperConfigurationError`, `UnsupportedBackendError`, and `MissingDependencyError`. It returns `DatabaseError(str(exc))` for any other `VertebraError`. It returns `InterfaceError(str(exc))` for `ValueError` and `TypeError`. It returns `None` for everything else. This is the body of `_translate_vertebra_error` plus the two checks `_ErrorTranslator.__exit__` makes. Make no other edit to `_sync.py`.
- In `_build_async_driver`, replace `with _translate_errors():` with `try:` around the same statements and `except Exception as exc:` that calls `_connect_error(exc)`, re-raises with a bare `raise` when the result is `None`, and otherwise raises the result `from exc`.
- In every other function of `_async.py`, remove the `with _translate_errors():` wrapper and keep its body.
- Remove from the `_sync` import list in `_async.py` every name only the deleted cursor used: `_DBAPIOutBindVar`, `_extract_out_bind_vars`, `_populate_out_bind_vars`, `_translate_errors`, `coerce_dbapi_rows_and_desc`, `is_insert_sql`, `is_read_only_sql`, and `is_row_returning_sql`. After this task `_async.py` imports none of them.
- In `AsyncDBAPIConnection`, delete the branch that reads `NATIVE_CURSOR_ENGINES` and keep only the native-cursor side. `cursor(self, *args, **kwargs)` returns the native async cursor.
- `AsyncDBAPIConnection.__getattr__` stays only if it forwards to the driver for names the SQLAlchemy async adapter reads; check with `git grep -n 'AsyncDBAPIConnection' -- src/py`. If it forwarded names of the deleted connection methods, remove those uses.
- Remove `AsyncDBAPICursor` from `__all__` and from the exports and lazy-export map in `__init__.py`.
- Delete tests that built `AsyncDBAPICursor` directly. Keep tests that go through the async connection.
- No alias, re-export, or fallback for a deleted name is added.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Cursor type | `AsyncDBAPIConnection(...).cursor()` for SQLite | Native async cursor class |
| Removed class | `from vertebra.db.dbapi import AsyncDBAPICursor` | `ImportError` |
| Async transaction control | `begin`, `commit`, `rollback` | Behave as before |
| SQLAlchemy async | Existing async dialect tests | Pass |
| Build error mapping | The existing async `_FailingConfig` assertion in the test file | Pass unchanged |
| Connect error mapping | `_connect_error(ImproperConfigurationError("x"))`, `_connect_error(TypeError("x"))`, `_connect_error(OperationalError("x"))` | `InterfaceError`, `InterfaceError`, `None` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/dbapi/_async.py src/py/vertebra/db/dbapi/__init__.py`
- `uv run --no-sync mypy src/py/vertebra/db/dbapi/_async.py src/py/vertebra/db/dbapi/__init__.py`
- `test -z "$(git grep -n 'AsyncDBAPICursor' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_translate_errors\|_DBAPIOutBindVar\|_extract_out_bind_vars\|is_row_returning_sql\|coerce_dbapi_rows_and_desc' -- src/py/vertebra/db/dbapi/_async.py)"`
- `test -z "$(git grep -n 'NATIVE_CURSOR_ENGINES' -- src/py/vertebra/db/dbapi/_async.py)"`

## Acceptance Criteria

- [ ] `AsyncDBAPICursor` has no reference under `src/py`.
- [ ] `_async.py` imports none of the helpers t37 deletes, and `_connect_error` is the only addition to `_sync.py`.
- [ ] `dbapi/_async.py` does not read `NATIVE_CURSOR_ENGINES`.

## Notes & Discoveries

- `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` and the five test files referencing `AsyncDBAPICursor` are now in scope; drop `AsyncDBAPICursor` from `async_adapter.py` and update the fake drivers in tests to expose `cursor()`.
- Completed t38: deleted `AsyncDBAPICursor`, removed `NATIVE_CURSOR_ENGINES` and `_translate_errors` from `dbapi/_async.py`, added `_connect_error` to `dbapi/_sync.py` (only edit there), dropped `AsyncDBAPICursor` from `dbapi/__init__.py` and `extensions/sqlalchemy/async_adapter.py`, and updated tests with zero `AsyncDBAPICursor` references remaining under `src/py`.
- Caveat for t37: 13 pre-existing sync-path tests in `test_dbapi_standalone.py` (11) and `test_sqlalchemy_vtb_dialects_and_dbapi.py` (2) plus 6 in `test_db_unified_driver_contracts.py` fail on `SyncDBAPIConnection` (`SyncSpannerDriver` lacks `in_transaction`, sync fake drivers lack `cursor()`, and `NATIVE_CURSOR_ENGINES` now includes spanner/spangres/googlesql/bigquery from t35/t36). All async DB-API and SQLAlchemy async tests pass; `_sync.py` was left untouched beyond `_connect_error` per contract.
