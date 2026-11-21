---
type: Task
id: "native-cursor-data-path:network:t27"
chapter_id: network-engines
title: "Asynchronous Oracle driver on the native cursor and helper removal"
description: "AsyncOracleDriver gets the cursor path and checkout leases, and the OracleCore helpers the cursor replaced are deleted."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t26"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/oracle/_async.py::AsyncOracleDriver"
  - "src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver"
  - "src/py/vertebra/db/adapters/oracle/_sync.py::_acquire_pool_client"
  - "src/py/vertebra/db/adapters/oracle/core.py:657-715"
  - "src/py/vertebra/db/adapters/oracle/core.py:741-750"
  - "src/py/vertebra/db/adapters/oracle/core.py:797-967"
  - "src/py/vertebra/db/adapters/oracle/core.py:1018-1041"
  - "src/py/vertebra/db/adapters/oracle/pipeline.py:1-80"
files:
  - "src/py/vertebra/db/adapters/oracle/_async.py"
  - "src/py/vertebra/db/adapters/oracle/core.py"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
  - "src/py/tests/unit/db/oracle_test_doubles.py"
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
  - id: oracle-async-driver
    resource: "src/py/vertebra/db/adapters/oracle/_async.py::AsyncOracleDriver"
    title: "Async native driver query path"
tags: [oracle, python, driver, async, cleanup]
---

# Asynchronous Oracle driver on the native cursor and helper removal

## Objective & Scope Boundary

The async Oracle driver gets the same change as the sync driver in t26. With both drivers on the cursor, the Python helpers for out-binds, statement kind, and result coercion have no caller and are deleted.

Out of scope:

- Notifications, queues, pipelines, embeddings, and Arrow paths.
- `OracleCore.track_session_context_sql`, `is_fatal_transport_error`, and the codec functions. They stay.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/oracle/_async.py` mirrors `_sync.py` method for method. Use the sync file after t26 as the reference.
- `src/py/vertebra/db/adapters/oracle/core.py:741-750`, `:815-967`, and `:1019-1041` are the helpers to delete.

## Contracts & Edge Cases

- Mirror every contract in t26 with coroutines: `cursor()` stays a plain method returning `AsyncOracleCursor`; `checkout_cursor()` is an async context manager; `_execute_cursor`, `execute`, `execute_many`, `execute_with_description`, `execute_tuples`, and `select` await the cursor.
- Lease calls move to the async `_acquire_pool_client` and `_release_pool_client`, and to `connect` and `close` for a driver without a pool, exactly as in t26. The async acquire is awaited, the release callable is attached with `client.attach_lease(...)`, and the driver stores no lease. `cursor` and `checkout_cursor` take `lazy_begin` as in t26.
- Delete from `core.py`: `OracleCore.is_dml_or_plsql`, `OracleCore.coerce_result_rows_and_cols`, `OracleCore.split_out_bind_parameters`, `OracleCore.populate_out_bind_vars`, `OracleCore._is_out_bind_var`, and `OracleCore._out_bind_type_hint`.
- Before deleting each one, run `git grep -n '<name>' -- src/py`. A match outside `core.py` and tests means a caller this chapter did not plan for: stop and mark the task blocked with the path.
- `OracleCore.coerce_parameters`, `coerce_bind_parameter`, and `coerce_row_cell` are deleted only when `git grep` shows no caller outside `core.py` and tests. The Arrow and pipeline paths call `coerce_parameters` today, so the expected result is that `coerce_parameters` and `coerce_bind_parameter` stay and `coerce_row_cell` goes. Record the kept names in the commit message for the cleanup chapter.
- `OracleCore.normalize_sql` is deleted when `git grep` shows no caller outside `core.py` and tests.
- Tests that called a deleted helper directly are deleted. Their behavior is covered by the cursor tests of t24 and t25.
- Imports left unused in `core.py` are removed.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `await driver.select('SELECT 1 AS a FROM dual')` | `[{'a': 1}]` |
| DML | `await driver.execute(...)` on two rows | `2` |
| Out-bind | Block with a variable | Variable filled |
| execute_many | Three sets | `3` |
| No broker | Ten statements with `broker=None` | Recording broker double never called |
| Lease per checkout | Transaction with five statements | One acquire, one release |
| Lease released on cancel | Cancel a task inside a checkout | Acquire count equals release count |
| Acquire refused at connect | Broker double whose `acquire` raises `RuntimeError`; connect, then run a statement | The statement succeeds; no `attach_lease` call; zero releases at `close()` |
| Driver dropped without close | Recording broker; connect, run a statement, `del driver`, `gc.collect()` | One release recorded |
| Removed helper | `OracleCore.is_dml_or_plsql` | `AttributeError` |
| Error class | ORA-00001 | `IntegrityError` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/_async.py src/py/vertebra/db/adapters/oracle/core.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/_async.py src/py/vertebra/db/adapters/oracle/core.py`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/oracle)"`
- `test -z "$(git grep -n 'is_dml_or_plsql' -- src/py)"`
- `test -z "$(git grep -n 'split_out_bind_parameters' -- src/py)"`
- `make oracle-test`

## Acceptance Criteria

- [ ] No file under the Oracle adapter package calls `_lookup_or_cache_statement`.
- [ ] The six named helpers have no reference under `src/py`.
- [ ] `make oracle-test` passes.

## Notes & Discoveries

- Keep `coerce_result_rows_and_cols` (and `coerce_row_cell`) in `src/py/vertebra/db/adapters/oracle/core.py` because `src/py/vertebra/db/adapters/oracle/pipeline.py` still calls it; delete `is_dml_or_plsql`, `split_out_bind_parameters`, and `populate_out_bind_vars`, and update `src/py/tests/unit/db/oracle_test_doubles.py` so stub cursors do not call deleted helpers.
