---
type: Task
id: "native-cursor-data-path:network:t33"
chapter_id: network-engines
title: "BigQuery cursor state and state-error helpers in Rust"
description: "Add the shared cursor state, description and tuple builders, and the two state-error helpers to vertebra-bigquery."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-bigquery/src/errors.rs:30-220"
  - "src/rs/vertebra-bigquery/src/lib.rs:30-110"
  - "src/rs/vertebra-bigquery/src/lib.rs:706-714"
  - "src/py/vertebra/_bigquery.pyi:9-48"
files:
  - "src/rs/vertebra-bigquery/src/cursor.rs"
  - "src/rs/vertebra-bigquery/src/errors.rs"
  - "src/rs/vertebra-bigquery/src/lib.rs"
tests:
  - "src/rs/vertebra-bigquery/src/cursor.rs"
  - "src/rs/vertebra-bigquery/src/errors.rs"
estimated_loc: 200
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
  - id: bigquery-lib
    resource: "src/rs/vertebra-bigquery/src/lib.rs::query_result_to_py"
    title: "Existing result conversion to Python"
tags: [bigquery, rust, dbapi]
---

# BigQuery cursor state and state-error helpers in Rust

## Objective & Scope Boundary

Give the BigQuery extension the Rust parts both cursor classes share. BigQuery reports in its reply whether a job produced a schema and how many rows DML touched, so no statement kind cache is needed.

Out of scope:

- The cursor classes (t34).
- Exception classes raised by existing client methods.
- Parameter encoding. It stays in `BigQueryCore`.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-bigquery/src/lib.rs:36-107` holds `cell_to_py`, `field_schema_to_py`, and `query_result_to_py`.
- `src/rs/vertebra-bigquery/src/errors.rs:30-220` holds the exception type cache, stand-ins, and the conversion to Python errors.
- `src/py/vertebra/_bigquery.pyi:22-48` lists the reply fields, including `num_dml_affected_rows`, `total_rows`, `job_id`, and `session_id`.

## Contracts & Edge Cases

- `cursor.rs` defines `CursorState` with the fields and methods in chapter spec section 2, all `pub(crate)`.
- `describe_fields(py, schema) -> PyResult<Py<PyTuple>>` builds `(name, type_name, None, None, None, None, null_ok)` from the field schema. `type_name` is the BigQuery type as a Python `str`. `null_ok` is `True` for `NULLABLE`, `False` for `REQUIRED`, and `None` otherwise.
- `rows_to_tuples(py, result) -> PyResult<Vec<Py<PyTuple>>>` converts each cell with `cell_to_py`. Nested and repeated fields keep the Python shapes `cell_to_py` gives them.
- `description` is `None` when the reply schema has no fields. `rowcount` is `num_dml_affected_rows` when the reply carries it, else the row count for a reply with a schema, else `-1`. `lastrowid` is always `None`.
- `pub(crate) fn query_metadata_to_py(py, result) -> PyResult<Py<PyDict>>` returns a dict with the keys `job_id`, `session_id`, `total_rows`, `total_bytes_processed`, `total_bytes_billed`, `slot_ms`, `cache_hit`, and `num_dml_affected_rows`, read from the reply.
- `cursor.rs` defines `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` with a body that returns `Ok(())`. `lib.rs` adds `pub mod cursor;` and calls `cursor::register(m)?`. Task t34 fills the body.
- Errors: cursor code raises engine errors through `bigquery_error_to_py`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| State rules | The state cases listed for t03 | Same results |
| Description | Schema with a `REQUIRED` `INT64` and a `NULLABLE` `STRING` | Two seven-item tuples; `null_ok` `False` and `True` |
| DML row count | Reply with `num_dml_affected_rows` 4 and no schema | `rowcount` 4; no description |
| DDL | Reply with no schema and no DML count | `rowcount` -1; no description |
| Metadata | Reply with a job id and a session id | Dict carries both |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-bigquery cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-bigquery errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-bigquery --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing BigQuery Rust tests pass unchanged.
- [ ] No Python-visible name is added.
