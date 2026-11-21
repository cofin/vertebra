---
type: Task
id: "native-cursor-data-path:network:t37"
chapter_id: network-engines
title: "Delete the old synchronous DB-API cursor path, the error translator, and the Python out-bind variable"
description: "With all eight engines on the native cursor, delete SyncDBAPICursor, the SQL tokenizer and classifiers, row coercion, out-bind helpers, the error translator, the Python out-bind variable, and the engine-set branch from dbapi/_sync.py."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t38"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/db/dbapi/_sync.py::_translate_vertebra_error"
  - "src/py/vertebra/db/dbapi/_sync.py::_ErrorTranslator"
  - "src/py/vertebra/db/dbapi/_sync.py::_resolve_dbapi_config"
  - "src/py/vertebra/db/dbapi/_sync.py::_build_driver"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPICursor"
  - "src/py/vertebra/db/dbapi/_sync.py::_DBAPIOutBindVar"
  - "src/py/vertebra/db/dbapi/_sync.py::_apply_dialect_metadata"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection"
  - "src/py/vertebra/db/dbapi/__init__.py"
  - "src/py/vertebra/extensions/django/db/backends/mysql/base.py"
  - "src/py/vertebra/extensions/django/db/backends/oracle/base.py"
  - "src/py/vertebra/extensions/django/db/backends/postgresql/base.py"
  - "src/py/vertebra/extensions/django/db/backends/spanner/base.py"
  - "src/py/vertebra/extensions/django/db/backends/sqlite3/base.py"
  - "src/py/tests/unit/db/test_dbapi_standalone.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py"
  - "src/py/tests/unit/extensions/test_postgres_compiler_transport.py"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py"
files:
  - "src/py/vertebra/db/dbapi/_sync.py"
  - "src/py/vertebra/db/dbapi/__init__.py"
  - "src/py/vertebra/extensions/django/db/backends/mysql/base.py"
  - "src/py/vertebra/extensions/django/db/backends/oracle/base.py"
  - "src/py/vertebra/extensions/django/db/backends/postgresql/base.py"
  - "src/py/vertebra/extensions/django/db/backends/spanner/base.py"
  - "src/py/vertebra/extensions/django/db/backends/sqlite3/base.py"
tests:
  - "src/py/tests/unit/db/test_dbapi_standalone.py"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py"
  - "src/py/tests/unit/extensions/test_postgres_compiler_transport.py"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py"
estimated_loc: 280
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
  - id: dbapi-sync
    resource: "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPICursor"
    title: "Old Python cursor and its helpers"
tags: [dbapi, python, cleanup]
---

# Delete the old synchronous DB-API cursor path, the error translator, and the Python out-bind variable

## Objective & Scope Boundary

The synchronous DB-API module holds names only: the exception name bindings, type objects, constructors, `connect`, and a connection with transaction control whose `cursor()` returns the engine's native cursor. Every helper of the old Python cursor is gone. Configuration errors are mapped when the connection is built and nowhere else.

Out of scope:

- `SyncSQLDriverProtocol`. The cleanup chapter trims driver protocol members.
- `_DBAPITypeObject`. The type objects use it.
- `dbapi/_async.py`. t38 has already removed its imports of the names deleted here.
- `_common.py` (t40).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- In `src/py/vertebra/db/dbapi/_sync.py`, the block from `_translate_vertebra_error` to `_populate_out_bind_vars` holds the translator, classifiers, coercion, tokenizer, and out-bind helpers. Locate it by symbol.
- `_resolve_dbapi_config` and `_build_driver` are the connect-time callers of `_translate_errors`.
- `SyncDBAPICursor` is the old cursor class.
- The wrapper-layers chapter spec names the branch on `NATIVE_CURSOR_ENGINES` it added to `SyncDBAPIConnection` and the connection methods that serve only the old path.

## Contracts & Edge Cases

- Delete, when present: `_TSQL_DML_VERBS`, `_TSQL_OUTPUT_TARGETS`, `_translate_vertebra_error`, `_ErrorTranslator`, `_TRANSLATE_ERRORS`, `_translate_errors`, `_DBAPIOutBindVar`, `is_insert_sql`, `is_read_only_sql`, `_coerce_column_description`, `coerce_dbapi_rows_and_desc`, `_match_dollar_quote_tag`, `_scan_unquoted_sql_tokens`, `_has_tsql_output_clause`, `is_row_returning_sql`, `_extract_out_bind_vars`, `_populate_out_bind_vars`, and the class `SyncDBAPICursor`.
- `_connect_error` exists from t38. In `_resolve_dbapi_config` (two sites) and `_build_driver`, replace `with _translate_errors():` with `try:` around the same statements and `except Exception as exc:` that calls `_connect_error(exc)`, re-raises with a bare `raise` when the result is `None`, and otherwise raises the result `from exc`.
- In every other function of `_sync.py`, remove the `with _translate_errors():` wrapper and keep its body. A native error from `begin`, `commit`, `rollback`, or `close` reaches the caller unchanged.
- `_DBAPIOutBindVar` loses its last callers here: the `var` method of `SyncDBAPICursor` and `_extract_out_bind_vars`. Delete the class. The Oracle cursor returns the native `OracleOutBindVar` from t25.
- In `_apply_dialect_metadata`, when the dialect is `oracle`, set `module.Var` to the class `OracleOutBindVar` imported from `vertebra._oracle` inside that branch. When the import raises `ImportError`, set nothing. Other dialects get no `Var` attribute. Both DB-API module classes call this function, so the sync and async Oracle module objects expose the same class.
- In `SyncDBAPIConnection`, delete the branch that reads `NATIVE_CURSOR_ENGINES` and keep only the native-cursor side. Delete the import of the constant.
- Delete the connection methods that existed only for the old cursor: `execute_with_description`, `execute_dml`, `execute_dml_with_lastrowid`, `execute_many_dml`, and `execute_raw_tuples`. Before deleting each, run `git grep -n '<name>' -- src/py`. A caller under `src/py/vertebra/extensions/sqlalchemy` means the wrapper-layers chapter left a path on it: stop and mark the task blocked with the path.
- `SyncDBAPIConnection.execute` (the convenience that returns a cursor) creates a native cursor and calls `execute` on it.
- Remove every deleted name from `__all__` in `_sync.py` and from the exports and lazy-export map in `__init__.py`.
- Delete tests that exercised a deleted helper directly. Keep tests that go through `connect`, `connection.cursor()`, and the exception classes; they already assert cursor behavior.
- Imports left unused are removed. `ruff` reports no unused name in either file.
- No alias, re-export, or fallback for a deleted name is added.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Cursor type per engine | `connect(url).cursor()` for SQLite and DuckDB | Native cursor class |
| Removed class | `from vertebra.db.dbapi import SyncDBAPICursor` | `ImportError` |
| Removed classifier | `from vertebra.db.dbapi._sync import is_row_returning_sql` | `ImportError` |
| Exception names kept | `vertebra.db.dbapi.IntegrityError` and the other eight error names | Importable; each is the `vertebra._vertebra` class |
| Configuration error at connect | A config whose driver build raises `ImproperConfigurationError` | `InterfaceError` |
| Other build errors | The existing `_FailingConfig` assertions in the test file | Pass unchanged |
| Native error in a transaction call | A driver double whose `commit` raises `OperationalError` | The same object reaches the caller |
| Oracle out-bind name | `SyncDBAPIModule(dialect="oracle").Var` and the async module's `Var`, with `pytest.importorskip("vertebra._oracle")` | Both are `vertebra._oracle.OracleOutBindVar` |
| Other dialects | `hasattr(SyncDBAPIModule(dialect="sqlite"), "Var")` | `False` |
| Python out-bind variable gone | `hasattr(vertebra.db.dbapi._sync, "_DBAPIOutBindVar")` | `False` |
| Type objects kept | `STRING`, `NUMBER`, `DATETIME`, `BINARY`, `ROWID` | Importable |
| Transaction control | `begin`, `commit`, `rollback` on a SQLite connection | Behave as before |
| Existing DB-API tests | The kept tests in the file | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/dbapi/_sync.py src/py/vertebra/db/dbapi/__init__.py`
- `uv run --no-sync mypy src/py/vertebra/db/dbapi/_sync.py src/py/vertebra/db/dbapi/__init__.py`
- `test -z "$(git grep -n 'is_row_returning_sql' -- src/py/vertebra)"`
- `test -z "$(git grep -n 'coerce_dbapi_rows_and_desc' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_translate_vertebra_error\|_ErrorTranslator\|_translate_errors\|_TRANSLATE_ERRORS' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_DBAPIOutBindVar\|_extract_out_bind_vars\|_populate_out_bind_vars' -- src/py/vertebra)"`
- `test -z "$(git grep -n 'NATIVE_CURSOR_ENGINES' -- src/py/vertebra/db/dbapi)"`
- `test -z "$(git grep -n '_scan_unquoted_sql_tokens' -- src/py/vertebra)"`
- `test -z "$(git grep -n 'SyncDBAPICursor' -- src/py/vertebra)"`

## Acceptance Criteria

- [ ] Every name in the delete list has no reference under `src/py`.
- [ ] Configuration errors are mapped only in `_resolve_dbapi_config`, `_build_driver`, and `_build_async_driver`.
- [ ] `_DBAPIOutBindVar` is gone, and the Oracle DB-API module objects expose `OracleOutBindVar` as `Var`.
- [ ] `dbapi/_sync.py` does not import or read `NATIVE_CURSOR_ENGINES`.
- [ ] SQLAlchemy dialect tests for all engines pass.

## Notes & Discoveries

- The 5 Django backend `base.py` files and 5 test files referencing `SyncDBAPICursor` are now in scope; replace `SyncDBAPICursor` annotations with `Any` in Django backends and update fake drivers in tests to expose `cursor()`.
