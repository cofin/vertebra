---
type: Task
id: "native-cursor-data-path:network:t15"
chapter_id: network-engines
title: "SQL Server cursor state, statement plan cache, and state-error helpers in Rust"
description: "Add the shared cursor state, a placeholder rewrite cached per SQL text, and the two state-error helpers to vertebra-mssql."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-mssql/src/errors.rs:15-60"
  - "src/rs/vertebra-mssql/src/errors.rs:330-440"
  - "src/rs/vertebra-mssql/src/params.rs:125-135"
  - "src/rs/vertebra-mssql/src/params.rs:320-420"
  - "src/rs/vertebra-mssql/src/client.rs:20-120"
  - "src/rs/vertebra-mssql/src/lib.rs:1-30"
  - "src/rs/vertebra-mssql/src/lib.rs:1330-1340"
  - "src/py/vertebra/db/adapters/mssql/_sync.py:427-437"
files:
  - "src/rs/vertebra-mssql/src/cursor.rs"
  - "src/rs/vertebra-mssql/src/errors.rs"
  - "src/rs/vertebra-mssql/src/lib.rs"
tests:
  - "src/rs/vertebra-mssql/src/cursor.rs"
  - "src/rs/vertebra-mssql/src/errors.rs"
estimated_loc: 230
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
  - id: mssql-params
    resource: "src/rs/vertebra-mssql/src/params.rs::prepare_sql_and_params"
    title: "Existing Rust placeholder rewrite"
tags: [mssql, rust, dbapi]
---

# SQL Server cursor state, statement plan cache, and state-error helpers in Rust

## Objective & Scope Boundary

Give the SQL Server extension the Rust parts both cursor classes share. The placeholder rewrite already lives in Rust; this task caches its result per SQL text for the cursor so the Python-side `_prepare_sql` and its shared-memory lookup can be deleted in t17.

Out of scope:

- The cursor classes (t16).
- `params.rs`. Its functions are called, not changed.
- Exception classes raised by existing client methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-mssql/src/params.rs:320-420` holds `prepare_sql_and_params`, `prepare_many_sql_and_params`, and `rewrite_named_mssql_placeholders`.
- `src/rs/vertebra-mssql/src/errors.rs:15-60` is `MssqlErrorKind`; `:330-440` holds the exception type cache and the conversion to a Python error.
- `src/rs/vertebra-mssql/src/client.rs:20-120` is `MssqlClient` and the shape of its query result.

## Contracts & Edge Cases

- `cursor.rs` defines `CursorState` with the fields and methods in chapter spec section 2, `describe_columns`, and `rows_to_tuples`, all `pub(crate)`.
- `cursor.rs` defines `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` with a body that returns `Ok(())`. `lib.rs` adds `pub mod cursor;` and calls `cursor::register(m)?` in the module init. Task t16 fills the body.
- `describe_columns` builds `(name, type_name, None, None, None, None, None)` with the SQL Server type name as a Python `str` when the reply carries one and `None` otherwise.
- `description` is `None` when the reply has no column metadata. `rowcount` is the row count for a result set and the server's affected-row count otherwise, or `-1` when `SET NOCOUNT ON` suppressed it. `lastrowid` is always `None`.
- `cursor.rs` adds `pub(crate) struct RewritePlan { sql: Box<str>, param_count: usize }` and `pub(crate) fn rewrite_plan_for(sql: &str) -> Arc<RewritePlan>` backed by a standard-library `Mutex` around an LRU keyed by SQL text with `CURSOR_PLAN_CACHE_CAPACITY: usize = 512`. The plan is filled by calling the existing `rewrite_mssql_placeholders`.
- `pub(crate) fn bind_positional(py, sql: &str, params: &Bound<PyAny>) -> Result<(Arc<RewritePlan>, Vec<MssqlParam>), MssqlError>` reads the plan and converts each item with `py_any_to_mssql_param`. A mapping is not handled here: the cursor passes mappings to the existing `prepare_sql_and_params`, which rewrites named parameters per call.
- A positional parameter count that differs from `plan.param_count` raises the error `prepare_sql_and_params` raises today for that case.
- Errors: cursor code raises engine errors through `mssql_error_to_py`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| State rules | The state cases listed for t03 | Same results |
| Description | Two columns with type names | Two seven-item tuples |
| Rewrite cached | `rewrite_plan_for` twice with one text | Same `Arc` |
| Rewrite result | `SELECT ? + ?` | Same text `rewrite_mssql_placeholders` returns; count 2 |
| Literal skipped | `SELECT '?'` | No rewrite; count 0 |
| Count mismatch | Two placeholders, one value | The existing programming error |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing `params.rs` and `errors.rs` tests pass unchanged.
- [ ] No Python-visible name is added.
