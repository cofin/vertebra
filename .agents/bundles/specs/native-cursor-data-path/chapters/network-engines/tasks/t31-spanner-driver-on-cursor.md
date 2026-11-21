---
type: Task
id: "native-cursor-data-path:network:t31"
chapter_id: network-engines
title: "Spanner native drivers on the native cursor"
description: "SyncSpannerDriver and AsyncSpannerDriver query and DML methods call the native cursor, and the per-statement shared-memory lookup and DDL text test are deleted."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t30"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/spanner/_sync.py:186-240"
  - "src/py/vertebra/db/adapters/spanner/_sync.py:387-596"
  - "src/py/vertebra/db/adapters/spanner/_sync.py:904-953"
  - "src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerDriver"
  - "src/py/vertebra/db/adapters/spanner/transactions.py:360-490"
  - "src/py/vertebra/db/adapters/spanner/transactions.py:600-660"
  - "src/py/vertebra/db/adapters/spanner/core.py:105-140"
  - "src/py/tests/unit/db/test_spanner.py"
files:
  - "src/py/vertebra/db/adapters/spanner/_sync.py"
  - "src/py/vertebra/db/adapters/spanner/_async.py"
  - "src/py/vertebra/db/adapters/spanner/transactions.py"
tests:
  - "src/py/tests/unit/db/test_spanner.py"
estimated_loc: 280
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
  - id: spanner-sync-driver
    resource: "src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver"
    title: "Native driver query path"
tags: [spanner, python, driver]
---

# Spanner native drivers on the native cursor

## Objective & Scope Boundary

`execute`, `execute_with_description`, and the `select` family on the two Spanner drivers, and the matching methods on their transaction objects, run one native cursor call. Python no longer tests the SQL text for DDL or consults the shared-memory statement cache for each statement.

Out of scope:

- `execute_batch_dml`, `execute_partitioned_dml`, `execute_script`, mutations, `update_ddl`, Arrow, and object-store methods.
- The process leases around mutation and commit calls in `transactions.py`. They guard connections held for a commit, not statements, and stay.
- The SQLSpec adapter (t32).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/spanner/_sync.py:387-423` is `execute`; `:512-596` is `execute_with_description` and the `select` family; `:911-953` holds `_prepare_sql_and_params` and `_run_query_sync`.
- `src/py/vertebra/db/adapters/spanner/transactions.py:396-420` and `:638-660` are the sync and async `_query_raw`.

## Contracts & Edge Cases

- `cursor` takes keyword-only `lazy_begin: bool = False` and passes it to the client cursor, which rejects `True` with `NotSupportedError`.
- Add `_encode_parameters(self, parameters: Any) -> str | None` on both drivers, returning `SpannerCore.encode_parameters_json(parameters, dialect=self.dialect)`.
- Add `cursor(self, *, staleness: StalenessBound | None = None, request_tag: str | None = None, transaction_tag: str | None = None)`. With an active transaction and no `staleness` it returns `self._active_transaction.cursor(request_tag=request_tag)`. Otherwise it returns `self._ensure_client().cursor(staleness_mode=bound.mode, staleness_value=bound.effective_value(), request_tag=request_tag, transaction_tag=transaction_tag, param_encoder=self._encode_parameters)` with `bound = staleness or StalenessBound.strong()`.
- The transaction classes in `transactions.py` gain `cursor(self, *, request_tag: str | None = None)` returning the native transaction's cursor with the driver's `_encode_parameters` as `param_encoder`.
- Add `_execute_cursor(self, sql, parameters, **options)` that applies `SpannerCore.annotate_sql_with_trace(sql, self._propagator)` and calls `cursor.execute(annotated, parameters or ())`.
- `execute` returns `max(cursor.rowcount, 0)`. The `SpannerCore.is_ddl_statement` branch and the direct `update_ddl` and `execute_dml` calls are removed from it; the cursor routes by statement kind. The DDL-in-transaction error now comes from the cursor as `NotSupportedError`.
- `execute_with_description` returns `(cursor.fetchall(), names)` or `([], ())`. `select`, `select_one`, `select_one_or_none`, and `select_value` are rewritten on it and keep their return types.
- Transaction `execute`, `query`, and `query_one` in `transactions.py` use the transaction cursor. `_query_raw` is deleted when no caller remains in the package.
- Delete `_run_query_sync` and its async form when no caller remains. Delete `_prepare_sql_and_params`'s shared-memory lookup and insert; the method keeps only the parameter encoding, and is deleted when its remaining callers can call `_encode_parameters` directly. Callers in the out-of-scope methods are updated to `_encode_parameters`.
- The `_statement_cache` and `_dialect_tag` attributes are removed from `__init__` when nothing reads them.
- The process lease around the autocommit DML call in `execute` is removed with that call. Leases in the out-of-scope methods stay.
- `SpannerCore.is_ddl_statement` stays if `execute_script` or another method still calls it; the cleanup chapter audits it.
- Tests that assert a `vertebra._vertebra` error class from a query method stay unchanged.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Tuples and names | `execute_with_description('SELECT 1 AS a, 2 AS b')` | `([(1, 2)], ('a', 'b'))` |
| DML | `execute` of an `UPDATE` on two rows | `2` |
| DDL | `execute('CREATE TABLE ...')` | `0`; table exists |
| DDL in transaction | Same inside `driver.transaction()` | `NotSupportedError` |
| Transaction read | `select` inside a transaction | Runs on the transaction |
| Stale read | `select(..., staleness=StalenessBound.max_staleness(...))` | Client cursor created with that bound |
| Mapping parameters | `select('SELECT @a AS a', {'a': 5})` | `[{'a': 5}]` |
| No shared-memory lookup | Ten statements with a recording statement-cache double on the broker | Never called |
| Error class | Duplicate key through `execute` | `SpannerError` with `code == 6`, the class raised today |
| Async | The same cases through `AsyncSpannerDriver` | Same results |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner/_sync.py src/py/vertebra/db/adapters/spanner/_async.py src/py/vertebra/db/adapters/spanner/transactions.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/spanner/_sync.py src/py/vertebra/db/adapters/spanner/_async.py src/py/vertebra/db/adapters/spanner/transactions.py`
- `test -z "$(git grep -n '_run_query_sync' -- src/py/vertebra/db/adapters/spanner)"`
- `test -z "$(git grep -n '_statement_cache.lookup' -- src/py/vertebra/db/adapters/spanner)"`

## Acceptance Criteria

- [ ] No file under the Spanner adapter package reads the shared-memory statement cache.
- [ ] Query and DML methods contain no test of the SQL text.
- [ ] Queue and change-stream tests pass unchanged.
