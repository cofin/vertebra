---
type: Task
id: "native-cursor-data-path:wrapper:t07"
chapter_id: wrapper-layers
title: "SQLite SQLSpec adapter without normalizers"
description: "Delete the SQLite SQLSpec adapter's unwrap_typed_parameters copy, parameter normalizers, and _prepare_native_parameters overrides, with the tests that asserted them."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:wrapper:t06"
read_set:
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:1-120"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:187-242"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:1-158"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:180-200"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:530-560"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:251-262"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:606-652"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:740-838"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 160
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-sqlite-core
    resource: src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py::normalize_sqlite_parameters
    title: "SQLite adapter parameter normalizers"
  - id: sqlspec-sqlite-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py::SQLiteSyncDriver
    title: "SQLite adapter driver classes"
tags: [sqlspec, sqlite, cleanup]
---

# t07 — SQLite SQLSpec adapter without normalizers

## Objective & Scope Boundary

After this task, the SQLite SQLSpec adapter holds no parameter-walking code. SQLSpec's compiled SQL and parameters go to the native cursor unchanged, and the Arrow and stream paths receive the same positional parameters from the base.

In scope: deletions in the SQLite adapter `core.py` and `driver.py`, and the tests that named the deleted symbols.

Out of scope:

- `native_statement_config`, `driver_profile`, `default_statement_config`, `map_sqlite_exception`, and every other function in `core.py`.
- The base class hooks in `_driver.py`.
- Other adapters.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:1-120` | Imports, `__all__`, `_QUOTED_SQL_SEGMENT`, `unwrap_typed_parameters` |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:187-242` | `_extract_named_colon_placeholders` and the two normalizers |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py:1-158` | Whole file |
| `src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:180-200`, `530-560` | Import of `normalize_sqlite_parameters` and its test |
| `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:251-262`, `606-652`, `740-838` | Tests that name `unwrap_typed_parameters` for SQLite and the module surface list |

## Contracts & Edge Cases

Delete from `adapters/sqlite/core.py`:

| Symbol | Also remove |
| --- | --- |
| `unwrap_typed_parameters` | Its `__all__` entry; the `TypedParameter` import when no use remains |
| `_QUOTED_SQL_SEGMENT` | The `re` import when no use remains |
| `_extract_named_colon_placeholders` | Nothing else |
| `normalize_sqlite_parameters` | Its `__all__` entry |
| `normalize_sqlite_many_parameters` | Its `__all__` entry |

Delete from `adapters/sqlite/driver.py`:

- `SQLiteSyncDriver._prepare_native_parameters` and `SQLiteAsyncDriver._prepare_native_parameters`.
- The imports of `normalize_sqlite_parameters` and `SQL`.

No alias, re-export, or replacement function is added.

| Case | Behavior after the deletion |
| --- | --- |
| `:name` statement with a dict | SQLSpec compiles it to `?` with positional values (t04). The adapter does nothing |
| `select_to_arrow` or a stream with parameters on SQLite | The base `_compile_for_native` passes the positional values through its own no-op `_prepare_native_parameters` |
| Statement with no parameters | The base passes `()` to the cursor and `None` to the Arrow and stream calls |
| Another module imports a deleted name | None does at `06e99fb` outside the two files and the tests listed here. `git grep` in the verification confirms it |

## Test Matrix

| Test | File | Change |
| --- | --- | --- |
| The test near line 541 that calls `normalize_sqlite_parameters`, and its import near line 195 | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | Deleted. The named-parameter tests from t04 cover the behavior through the session |
| `test_sqlite_driver_imports_unwrap_typed_parameters_from_sqlite_core` | `test_sqlspec_driver_dispatch.py` | Deleted |
| `test_isolated_adapter_unwrap_typed_parameters` | `test_sqlspec_driver_dispatch.py` | `"sqlite"` removed from its parameter list |
| `test_base_driver_module_surface_slots_and_lazy_store_attr` | `test_sqlspec_driver_dispatch.py` | Any expected-name list for the SQLite core module drops the five deleted names |
| `test_sqlite_adapter_holds_no_parameter_normalizers` (new) | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | The SQLite `core` module has none of the five deleted names, and neither SQLite driver class defines `_prepare_native_parameters` or `dispatch_execute_many` in its own `__dict__` |
| `test_sqlite_select_to_arrow_with_named_parameters` (new) | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | `select_to_arrow("select :a as a", {"a": 7})` returns one row with `a == 7`, sync and async |
| Named-parameter and `execute_many` tests from t04 | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | Pass unchanged |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -q
git grep -n -E "normalize_sqlite_parameters|normalize_sqlite_many_parameters|_extract_named_colon_placeholders|_QUOTED_SQL_SEGMENT" -- src tools docs
git grep -n "unwrap_typed_parameters" -- src/py/vertebra/extensions/sqlspec/adapters/sqlite
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite
```

Both `git grep` commands print nothing, apart from the string literals inside `test_sqlite_adapter_holds_no_parameter_normalizers`.

## Acceptance Criteria

- [ ] The five symbols are gone from `adapters/sqlite/core.py` and from its `__all__`.
- [ ] Neither SQLite driver class overrides `_prepare_native_parameters` or `dispatch_execute_many`.
- [ ] `ruff` reports no unused import in the two files.
- [ ] The two new tests pass, and the listed test files pass in full.
