---
type: Task
id: "native-cursor-data-path:network:t35"
chapter_id: network-engines
title: "BigQuery native drivers on the native cursor"
description: "SyncBigQueryDriver and AsyncBigQueryDriver query methods call the native cursor and read rows, row counts, and job metadata from it."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t34"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:102-200"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:198-336"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:434-468"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:571-641"
  - "src/py/vertebra/db/adapters/bigquery/_async.py::AsyncBigQueryDriver"
  - "src/py/vertebra/db/adapters/bigquery/core.py:216-232"
  - "src/py/tests/unit/db/test_bigquery.py"
files:
  - "src/py/vertebra/db/adapters/bigquery/_sync.py"
  - "src/py/vertebra/db/adapters/bigquery/_async.py"
tests:
  - "src/py/tests/unit/db/test_bigquery.py"
estimated_loc: 240
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
  - id: bigquery-sync-driver
    resource: "src/py/vertebra/db/adapters/bigquery/_sync.py::SyncBigQueryDriver"
    title: "Native driver query path"
tags: [bigquery, python, driver]
---

# BigQuery native drivers on the native cursor

## Objective & Scope Boundary

`execute`, `execute_with_description`, `select`, `query`, and their single-row forms on the two BigQuery drivers run one native cursor call. Rows arrive as tuples from Rust; the driver no longer re-tuples a dict reply.

Out of scope:

- `dry_run`, `select_to_arrow`, `load_from_arrow`, object-store and object-table methods, and the process leases around Storage API calls.
- `core.py`. No helper there loses its last caller in this task.
- The SQLSpec adapter (t36).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/bigquery/_sync.py:198-336` holds the query methods; `:578-641` holds `_prepare_sql_and_params` and `_run_query_sync`.
- `src/py/vertebra/db/adapters/bigquery/_sync.py:434-468` holds `begin`, `commit`, and `rollback`, which run statements through `_run_query_sync` today.

## Contracts & Edge Cases

- `cursor` takes keyword-only `lazy_begin: bool = False` and passes it to the client cursor, which rejects `True` with `NotSupportedError`.
- Add `cursor(self, *, job_labels: Mapping[str, str] | None = None, destination_table: str | None = None, write_disposition: str | None = None, maximum_bytes_billed: int | None = None)`. It returns `self._ensure_client().cursor(session_id=self._session_id, destination_table=..., write_disposition=..., maximum_bytes_billed=<argument or config value>, job_timeout_ms=self.config.job_timeout_ms, priority=self.config.priority, labels=<merged and sanitized labels or None>, param_encoder=self._encode_for_cursor)`.
- Add `_encode_for_cursor(self, sql: str, parameters: Any) -> tuple[str, str | None]` that returns the result of the existing `_prepare_sql_and_params(sql, parameters)`.
- Add `_execute_cursor(self, sql, parameters, **job_options)` that applies `BigQueryCore.annotate_sql_with_trace`, creates the cursor with the job options, calls `execute(annotated, parameters or ())`, then calls `_after_query(cursor.query_metadata)`.
- Add `_after_query(self, metadata: Mapping[str, Any]) -> None` that stores a returned session id on `self._session_id`, stores the metadata dict on `self._last_query_metadata` with the keys it has today, and calls `BigQueryCore.record_finops_metrics(self.project_id, self.config.location, metadata)`.
- `execute` returns `max(cursor.rowcount, 0)` when the cursor has no description, and the row count of `cursor.fetchall()` otherwise. This equals today's result for DML, DDL, and queries.
- `execute_with_description` returns `(cursor.fetchall(), names)` or `([], ())`. `select`, `select_one`, `select_one_or_none`, `query`, `query_one`, and `select_value` are rewritten on it and keep their return types.
- `begin`, `commit`, and `rollback` run their statements through `_execute_cursor`.
- Delete `_run_query_sync` and its async form when `git grep` shows no caller left in the package. Out-of-scope methods that still call it keep it; name them in the commit message for the cleanup chapter.
- If `record_finops_metrics` reads a reply key that `query_metadata` does not carry, stop and mark the task blocked with the key name; task t33 owns the metadata keys.
- Tests that assert a `vertebra._vertebra` error class from a query method stay unchanged.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Tuples and names | `execute_with_description('SELECT 1 AS a, 2 AS b')` | `([(1, 2)], ('a', 'b'))` |
| DML | `execute` of an `UPDATE` on two rows | `2` |
| Query through execute | `execute('SELECT 1 UNION ALL SELECT 2')` | `2` |
| Metadata | `driver.last_query_metadata` after a select | Has `job_id` and the keys it has today |
| Session | `begin()`, a statement, `commit()` | All three jobs share one session id; `in_transaction` toggles as today |
| Mapping parameters | `select('SELECT @a AS a', {'a': 5})` | `[{'a': 5}]` |
| Job options | `execute(..., job_labels={'team': 'x'}, maximum_bytes_billed=10)` | Cursor created with both |
| Error class | Invalid query through `execute` | `BigQueryError` with `code == 3`, the class raised today |
| Async | The same cases through `AsyncBigQueryDriver` | Same results |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_bigquery.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/bigquery/_sync.py src/py/vertebra/db/adapters/bigquery/_async.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/bigquery/_sync.py src/py/vertebra/db/adapters/bigquery/_async.py`

## Acceptance Criteria

- [x] Query methods read rows and counts from the cursor and build no row tuples in Python.
- [x] `last_query_metadata` and session handling behave as before.
- [x] Arrow, dry-run, and object-table tests pass unchanged.

## Notes & Discoveries

- `SyncBigQueryClient.cursor()` and `AsyncBigQueryClient.cursor()` (`src/rs/vertebra-bigquery/src/cursor.rs::CursorQueryOptions`) hardcode `create_session: false` and do not expose `create_session`. When `self._session_id is None`, `begin()` uses `client.query(annotated, None, None, True)` + `self._after_query(res)` to create the initial BigQuery session, while subsequent `begin()` calls, all transaction queries, `commit()`, and `rollback()` run through `_execute_cursor`.
- `_run_query_sync` and `_run_query_async` remain because out-of-scope `select_to_arrow` still calls them on its REST fallback path (named for the cleanup chapter).
- `_BigQueryToolClient` and `_AsyncBigQueryToolClient` in `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` (outside `t35` owned files) do not yet implement `.cursor()` and will need updating in `t36` or cleanup.
