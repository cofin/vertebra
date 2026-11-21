---
type: Task
id: "native-cursor-data-path:wrapper:t08"
chapter_id: wrapper-layers
title: "DuckDB SQLSpec adapter without normalizers"
description: "Delete the DuckDB SQLSpec adapter's unwrap_typed_parameters copy, parameter normalizers, and _prepare_native_parameters overrides, point prepare_copy_to_query at SQLSpec's compiled output, and correct the knowledge page sentence."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:wrapper:t07"
read_set:
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:1-72"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:126-176"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:376-426"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:1-80"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py::DuckDBSyncDriver.prepare_copy_to_query"
  - "src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py::DuckDBAsyncDriver.prepare_copy_to_query"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:606-652"
  - ".agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:153"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
tests:
  - src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py
  - src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
estimated_loc: 170
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-duckdb-core
    resource: src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py::normalize_duckdb_parameters
    title: "DuckDB adapter parameter normalizers"
  - id: sqlspec-duckdb-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py::DuckDBSyncDriver
    title: "DuckDB adapter driver classes"
  - id: sqlspec-knowledge
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec and SQLAlchemy extension knowledge page"
tags: [sqlspec, duckdb, cleanup]
---

# t08 — DuckDB SQLSpec adapter without normalizers

## Objective & Scope Boundary

After this task, the DuckDB SQLSpec adapter holds no parameter-walking code. Value conversion for `UUID`, `Decimal`, `date`, and `datetime` stays where it already is: the type coercions in the DuckDB parameter profile, which SQLSpec applies and caches.

In scope: deletions in the DuckDB adapter `core.py` and `driver.py`, one call change in `prepare_copy_to_query`, the tests that named the deleted symbols, and one sentence in a knowledge page.

Out of scope:

- `build_profile`, `build_statement_config`, `collect_rows`, `map_duckdb_exception`, and every other function in `core.py`.
- The savepoint methods and `should_bypass_arrow_materialization` in `driver.py`.
- The base class hooks in `_driver.py`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:1-72` | Imports and `__all__` |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:126-176` | The profile and its `custom_type_coercions` |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py:376-426` | The four functions to delete |
| `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py:1-80` | Imports and the sync `_prepare_native_parameters` |
| `prepare_copy_to_query` on both driver classes | The two `_compile_for_native` calls |
| `src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py:606-652` | `test_isolated_adapter_unwrap_typed_parameters` |
| `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md:153` | The sentence that says each adapter isolates `unwrap_typed_parameters` |

## Contracts & Edge Cases

Delete from `adapters/duckdb/core.py`:

| Symbol | Also remove |
| --- | --- |
| `normalize_duckdb_scalar` | The `UUID` import when no use remains |
| `unwrap_typed_parameters` | Its `__all__` entry; the `TypedParameter` import when no use remains |
| `normalize_duckdb_parameters` | Its `__all__` entry |
| `normalize_duckdb_many_parameters` | Its `__all__` entry |

Change in `adapters/duckdb/driver.py`:

- Delete `DuckDBSyncDriver._prepare_native_parameters` and `DuckDBAsyncDriver._prepare_native_parameters`.
- In both `prepare_copy_to_query` methods, replace `compiled_sql, compiled_params = self._compile_for_native(prepared)` with `compiled_sql, compiled_params = self._compiled_sql(prepared, cfg)`. The lines after it already accept `None`, a mapping, or a sequence.
- Remove the imports of `normalize_duckdb_parameters` and `SQL` when no use remains.

No alias, re-export, or replacement function is added.

Knowledge page: the table row at line 153 holds a sentence beginning "Each adapter isolates `unwrap_typed_parameters`". Rewrite it in two parts. The PostgreSQL, MySQL, Oracle, SQL Server, BigQuery, and Spanner adapters isolate `unwrap_typed_parameters` in their own `core.py`. Every adapter isolates `native_statement_config` and `map_native_database_exception`. Nothing else in the page changes.

| Case | Behavior after the deletion |
| --- | --- |
| `UUID` parameter | The profile coercion converts it to `str` during SQLSpec compilation |
| `Decimal`, `date`, `datetime` parameters | Converted by the profile coercions, as before |
| `execute_many` parameter set that is a bare scalar | Passed through unchanged (see t05). Callers pass tuples |
| `prepare_copy_to_query` with no parameters | `_compiled_sql` returns an empty sequence. The ordered tuple holds only the destination |
| Another module imports a deleted name | None does at `06e99fb` outside the two files and the tests listed here. `git grep` in the verification confirms it |

## Test Matrix

| Test | File | Change |
| --- | --- | --- |
| `test_isolated_adapter_unwrap_typed_parameters` | `test_sqlspec_driver_dispatch.py` | `"duckdb"` removed from its parameter list |
| `test_base_driver_module_surface_slots_and_lazy_store_attr` | `test_sqlspec_driver_dispatch.py` | Any expected-name list for the DuckDB core module drops the four deleted names |
| Any test that imports a deleted DuckDB name | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | Rewritten to assert the same value through a session query |
| `test_duckdb_adapter_holds_no_parameter_normalizers` (new) | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | The DuckDB `core` module has none of the four deleted names, and neither DuckDB driver class defines `_prepare_native_parameters` or `dispatch_execute_many` in its own `__dict__` |
| `test_duckdb_uuid_decimal_and_datetime_parameters_round_trip` (new) | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | `select ?::uuid`, `select ?::decimal(10,2)`, and `select ?::timestamp` with Python values return equal values, sync and async |
| `test_duckdb_prepare_copy_to_query_orders_parameters` (new, or the existing COPY test extended) | `test_sqlspec_sqlite_mysql_mssql_duckdb.py` | With one query parameter and a destination, the returned tuple has the two values in the order `duckdb_copy_filename_follows_query()` implies; with no query parameter it holds only the destination |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q
git grep -n -E "normalize_duckdb_parameters|normalize_duckdb_many_parameters|normalize_duckdb_scalar" -- src tools docs .agents/bundles/knowledge
git grep -n "unwrap_typed_parameters" -- src/py/vertebra/extensions/sqlspec/adapters/duckdb
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/duckdb src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/duckdb
make review-check
```

Both `git grep` commands print nothing, apart from the string literals inside `test_duckdb_adapter_holds_no_parameter_normalizers`.

## Acceptance Criteria

- [ ] The four symbols are gone from `adapters/duckdb/core.py` and from its `__all__`.
- [ ] Neither DuckDB driver class overrides `_prepare_native_parameters` or `dispatch_execute_many`.
- [ ] `adapters/duckdb/driver.py` does not call `_compile_for_native`.
- [ ] The knowledge page sentence names the six adapters that still isolate `unwrap_typed_parameters`.
- [ ] The three new tests pass, and the listed test files pass in full.
