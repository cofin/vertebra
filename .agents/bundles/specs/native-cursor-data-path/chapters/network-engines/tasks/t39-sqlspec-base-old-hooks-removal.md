---
type: Task
id: "native-cursor-data-path:network:t39"
chapter_id: network-engines
title: "Delete the old SQLSpec adapter base hooks"
description: "Delete the parameter re-walk, compile, row-probe, and per-call exception handler hooks and the engine-set branch from the SQLSpec adapter base classes."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t36"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:185-195"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:117-235"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:237-300"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:518-612"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:876-896"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:916-1060"
  - "src/py/vertebra/extensions/sqlspec/_driver.py:1330-1350"
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py"
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
files:
  - "src/py/vertebra/extensions/sqlspec/_driver.py"
  - "src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py"
tests:
  - "src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py"
  - "src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py"
  - "src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py"
estimated_loc: 260
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
  - id: sqlspec-base
    resource: "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver"
    title: "Adapter base classes"
tags: [sqlspec, python, cleanup]
---

# Delete the old SQLSpec adapter base hooks

## Objective & Scope Boundary

`BaseSyncDriver` and `BaseAsyncDriver` have one path: `with_cursor` yields a native cursor, `dispatch_execute` calls `cursor.execute`, and results come from `cursor.description`, `cursor.rowcount`, and `cursor.fetchall()`. The hooks of the old path are gone.

Out of scope:

- Adapter packages. Tasks t10, t14, t18, t28, t32, and t36 already removed their uses.
- `proxy.py`, `provider.py`, `events.py`, and `storage.py`.
- The row-stream sources `_VertebraSyncRowSource` and `_VertebraAsyncRowSource`, unless they call a deleted hook; see the contract.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/extensions/sqlspec/_driver.py:268-298` and `:947-956` hold `_unwrap_typed_parameters`, `_normalize_bound_params`, and `_normalize_statement_params`.
- `src/py/vertebra/extensions/sqlspec/_driver.py:523-612` and `:967-1060` hold `handle_database_exceptions`, `with_cursor`, `_prepare_native_parameters`, `_compile_for_native`, `_fetch_rows_with_columns`, and the dispatch methods.
- `src/py/vertebra/extensions/sqlspec/_driver.py:117-159` are the two deferred exception handler classes.

## Contracts & Edge Cases

- Delete from both base classes, when present: `_unwrap_typed_parameters`, `_normalize_bound_params`, `_prepare_native_parameters`, `_compile_for_native`, and `_fetch_rows_with_columns`.
- Delete the branch that reads `NATIVE_CURSOR_ENGINES` in `with_cursor`, `dispatch_execute`, `dispatch_execute_many`, `collect_rows`, and `resolve_rowcount`, keeping only the native-cursor side. Delete the import of the constant.
- `_DeferredSyncExceptionHandler` and `_DeferredAsyncExceptionHandler`: the root spec requires that no handler object is built per call. If `handle_database_exceptions` still constructs one per call after the wrapper-layers change, replace it with one instance created in `__init__` and returned on every call. If the wrapper-layers chapter already did this, change nothing.
- `_normalize_statement_params` is deleted when `git grep` shows no caller under `src/py`.
- Before deleting each hook, run `git grep -n '<name>' -- src/py/vertebra`. A match outside `_driver.py` means an adapter still overrides or calls it: stop and mark the task blocked with the path.
- The row-stream source classes call the native driver's `select` in chunks today. If they call a deleted hook, change that call to `cursor.execute` followed by `cursor.fetchmany(chunk_size)` on a native cursor from `with_cursor`.
- Delete tests that exercised a deleted hook directly. Keep the dispatch tests that assert results; they run on the native cursor path for every engine.
- No alias, re-export, or fallback for a deleted name is added.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One native execute | Cache-hit select on SQLite through the base driver | One `execute`, one `fetchall` |
| Removed hook | `BaseSyncDriver._compile_for_native` | `AttributeError` |
| Removed probe | `BaseSyncDriver._fetch_rows_with_columns` | `AttributeError` |
| Handler reuse | Two calls to `handle_database_exceptions` on one driver | Same object |
| Row stream | `select_stream` on SQLite with a chunk size of two over five rows | Chunks of two, two, one |
| Existing dispatch tests | The kept tests in the file | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py -q`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/_driver.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/_driver.py`
- `test -z "$(git grep -n '_compile_for_native' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_fetch_rows_with_columns' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_prepare_native_parameters' -- src/py/vertebra)"`
- `test -z "$(git grep -n '_unwrap_typed_parameters' -- src/py/vertebra)"`

## Acceptance Criteria

- [ ] The five hook names have no reference under `src/py`.
- [ ] `_driver.py` does not import or read `NATIVE_CURSOR_ENGINES`.
- [ ] SQLSpec tests for every engine pass.

## Notes & Discoveries

- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`, `src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py`, and `src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py` are now in scope; remove `self._native_cursor` branches from `OracleSyncDriver`/`OracleAsyncDriver` and update the fake test doubles to expose `cursor()`/`checkout_cursor()`.
