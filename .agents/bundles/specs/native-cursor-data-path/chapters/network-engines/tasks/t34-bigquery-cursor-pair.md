---
type: Task
id: "native-cursor-data-path:network:t34"
chapter_id: network-engines
title: "SyncBigQueryCursor and AsyncBigQueryCursor"
description: "Add the BigQuery cursor pair, created by client.cursor(), over the existing query job call."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t33"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-bigquery/src/lib.rs:182-200"
  - "src/rs/vertebra-bigquery/src/lib.rs:310-350"
  - "src/rs/vertebra-bigquery/src/lib.rs:432-450"
  - "src/rs/vertebra-bigquery/src/lib.rs:560-600"
  - "src/rs/vertebra-bigquery/src/cursor.rs"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py:601-641"
  - "src/py/vertebra/_bigquery.pyi:85-120"
  - "src/py/vertebra/_bigquery.pyi:192-225"
  - "src/py/tests/unit/db/test_bigquery.py:1-80"
files:
  - "src/rs/vertebra-bigquery/src/cursor.rs"
  - "src/rs/vertebra-bigquery/src/lib.rs"
  - "src/py/vertebra/_bigquery.pyi"
tests:
  - "src/py/tests/unit/db/test_bigquery.py"
estimated_loc: 260
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
  - id: bigquery-client
    resource: "src/rs/vertebra-bigquery/src/lib.rs::PySyncBigQueryClient"
    title: "Client classes that create the cursors"
tags: [bigquery, rust, pyo3, dbapi]
---

# SyncBigQueryCursor and AsyncBigQueryCursor

## Objective & Scope Boundary

`SyncBigQueryClient.cursor()` and `AsyncBigQueryClient.cursor()` return PEP 249 cursors. One `execute` runs one query job and stores tuple rows, the description, the row count, and the job metadata.

Out of scope:

- Python driver changes (t35).
- Dry runs, Storage API reads, appends, and object tables.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `query` on `PySyncBigQueryClient` in `lib.rs` takes the SQL, parameter JSON, session id, a dry-run flag, destination table, write disposition, byte limit, timeout, priority, and labels. The cursor calls the same Rust client function.
- `src/py/vertebra/db/adapters/bigquery/_sync.py:601-641` shows the argument order the Python driver passes today.
- `src/rs/vertebra-bigquery/src/cursor.rs::register` is called from the module init since t33. This task fills its body.

## Contracts & Edge Cases

- Each client `cursor` also takes keyword-only `lazy_begin: bool = False`. `lazy_begin=True` raises `NotSupportedError`, because the DB-API connection for this engine requires autocommit. Test row: that call raises and sends nothing.
- Classes `SyncBigQueryCursor` and `AsyncBigQueryCursor`, module `vertebra._bigquery`, defined in `cursor.rs` and added in `register` in `cursor.rs`. Each holds the shared native client, the options below, and a `std::sync::Mutex<CursorState>`.
- On both client classes: `cursor(self, *, session_id: str | None = None, destination_table: str | None = None, write_disposition: str | None = None, maximum_bytes_billed: int | None = None, job_timeout_ms: int | None = None, priority: str | None = None, labels: Mapping[str, str] | None = None, param_encoder: Callable[[str, Any], tuple[str, str | None]] | None = None)`.
- `execute(self, sql: str, params: str | Sequence[Any] | Mapping[str, Any] = ()) -> Self`. A `str` is taken as encoded parameter JSON. An empty value means no parameters. Any other value is passed to `param_encoder(sql, params)`, which returns the SQL to send and the parameter JSON; with no encoder that case raises `InterfaceError` with the message `BigQuery cursor needs encoded parameters or a param_encoder`.
- `execute` runs one query job with the cursor options and the dry-run flag off.
- A read-only property `query_metadata` returns the dict from `query_metadata_to_py` for the last `execute`, and `None` before any.
- When the reply carries a session id, later `execute` calls on the same cursor use it.
- `executemany(self, sql, seq_of_params) -> Self` runs one job per parameter set, in order, and sets `rowcount` to the sum of the DML counts. `description` is `None`. A failure stops the loop and raises; sets already run are not undone.
- Async methods return `RustDbTaskAwaitable`. Parameter encoding runs on the calling thread.
- Fetch methods, properties, `arraysize`, `close`, and the failed-execute and concurrent-execute rules follow chapter spec section 2. Engine errors leave through `bigquery_error_to_py`, unchanged; an exception raised by `param_encoder` propagates unchanged.
- Remove the `allow(dead_code)` attribute from `cursor.rs`.
- Stub: two cursor classes and one `cursor` method on each client class.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `SELECT 1 AS a, 'x' AS b` | `[(1, 'x')]`; names `a`, `b` |
| DML | `UPDATE` touching two rows | `rowcount` 2; `description` is `None` |
| DDL | `CREATE TABLE ...` | `description` is `None`; `rowcount` -1 |
| Empty set | `SELECT 1 AS a FROM UNNEST([]) AS x` | `[]`; description present |
| Metadata | After a select | `query_metadata['job_id']` is a string |
| Session reuse | Cursor with a session id, two executes | Both jobs carry the session id |
| Encoded parameters | `execute(sql, '[...]')` | Parameters applied |
| Encoder | `execute(sql, {'a': 1})` with an encoder | Encoder called once with the SQL and the mapping |
| No encoder | Mapping without one | `InterfaceError` |
| Labels and limit | Cursor created with labels and a byte limit | Job request carries both |
| Invalid query | `SELEC 1` | `BigQueryError` with `code == 3`, as `bigquery_error_to_py` raises today |
| Closed | `close()` then `execute` | `InterfaceError` |
| Async | The same cases through `AsyncBigQueryCursor` | Same results |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._bigquery`
- `uv run --no-sync pytest src/py/tests/unit/db/test_bigquery.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-bigquery cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-bigquery --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-bigquery/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` and `test_async_cursor_*` use the client doubles or emulator fixture the existing BigQuery tests use and cover every matrix case.
- [ ] The stub diff adds two classes and two methods.
