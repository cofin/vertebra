---
type: Task
id: "native-cursor-data-path:wrapper:t04"
chapter_id: wrapper-layers
title: "SQLite SQLSpec statement config executes with ? only"
description: "Limit the SQLite SQLSpec execution parameter style to QMARK so SQLSpec converts :name input once per statement and no named mapping reaches native code."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:1-100"
  - "src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:122-240"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:180-200"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:500-580"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py
estimated_loc: 90
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlspec-sqlite
    resource: src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py::default_statement_config
    title: "SQLite adapter default statement config"
tags: [sqlspec, sqlite]
---

# t04 — SQLite SQLSpec statement config executes with `?` only

## Objective & Scope Boundary

After this task, a SQLSpec caller can still write `:name` placeholders and pass a dict on SQLite. SQLSpec rewrites the statement to `?` placeholders with positional values when it compiles it, and caches that result. No named mapping reaches the native driver or cursor from SQLSpec.

This runs before the base switch in t05, so named SQLite statements keep working when the base stops calling the adapter normalizers.

In scope: `default_statement_config()` and `driver_profile()` in the SQLite adapter `core.py`, and tests.

Out of scope:

- `normalize_sqlite_parameters`, `normalize_sqlite_many_parameters`, `unwrap_typed_parameters`, and the driver overrides. They stay until t07 and keep working on positional input.
- `native_statement_config`. Its signature and defaults do not change.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:1-100` | Imports and `__all__` |
| `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py:122-240` | `native_statement_config`, `driver_profile`, `default_statement_config`, normalizers |
| `src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py:180-200`, `500-580` | Imports and the SQLite parameter tests |

## Contracts & Edge Cases

- `default_statement_config()` passes `supported_execution_styles={ParameterStyle.QMARK}`. `supported_styles` stays `{ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}`.
- `driver_profile()` sets `supported_execution_styles={ParameterStyle.QMARK}`. Every other field is unchanged.
- Docstrings of both functions say that `:name` input is accepted and executed as `?`.

Measured on the tree at `06e99fb` with this config:

| Input | Compiled SQL | Compiled parameters |
| --- | --- | --- |
| `select * from t where a = :a and b = :b and c = :a` with `{"a": 1, "b": 2}` | `select * from t where a = ? and b = ? and c = ?` | `(1, 2, 1)` |
| `select * from t where a = ?` with `1` | unchanged | `[1]` |
| `insert into t values (:a, :b)` with two dicts, `is_many=True` | `insert into t values (?, ?)` | `[(1, 2), (3, 4)]` |
| `select 1` | unchanged | `[]` |

| Case | Behavior |
| --- | --- |
| Dict with a key the statement does not use | SQLSpec raises its parameter count error, as it does today |
| Repeated `:name` | The value is repeated positionally |
| A colon inside a string literal or comment | SQLSpec's parser leaves it alone |
| Caller passes an explicit `statement_config` | That config is used as given |

## Test Matrix

All in `src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py`, against a real in-memory SQLite session, sync and async.

| Test | Asserts |
| --- | --- |
| `test_sqlite_default_statement_config_executes_qmark_only` (new) | `supported_execution_parameter_styles == {QMARK}`; `supported_parameter_styles` holds `QMARK` and `NAMED_COLON`; the four compile rows above |
| `test_sqlite_named_parameters_round_trip_through_session` (new) | `select :a as a, :b as b, :a as c` with a dict returns `{"a": 1, "b": 2, "c": 1}`; a string literal `':x'` in the statement is returned as text |
| `test_sqlite_named_execute_many_inserts_all_rows` (new) | `execute_many` with three dicts inserts three rows and reports `rows_affected == 3` |
| The existing test near line 541 that calls `normalize_sqlite_parameters` | Stays. The helper still exists until t07 |
| Any existing assertion that the SQLite config supports `NAMED_COLON` execution | Changed to assert `QMARK` only |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py -q
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py -q -k "sqlite"
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
```

## Acceptance Criteria

- [x] `default_statement_config()` and `driver_profile()` list `QMARK` as the only execution style.
- [x] The three new tests pass, sync and async.
- [x] The listed test commands pass in full.
- [x] No other function in `core.py` has a diff.

## Notes & Discoveries

- `driver_profile()` in `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py` previously passed `build_decimal_converter(mode="str")`, which raised `ValueError: Unsupported decimal converter mode: str` when called (the sibling DuckDB and MySQL adapters pass `mode="string"`). Updated to `mode="string"` inside `driver_profile()` so the profile constructs cleanly and can be verified directly in tests.
- Full repo `make lint` was not run in this worker step; targeted Python `ruff check`, `ruff format --check`, `mypy`, and `pyright` all passed on the touched files (`UV_CACHE_DIR=/tmp/uv-cache` required in the sandbox because `~/.cache/uv` is read-only).
