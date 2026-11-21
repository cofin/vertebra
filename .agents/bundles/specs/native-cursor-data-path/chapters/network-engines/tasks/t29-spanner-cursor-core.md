---
type: Task
id: "native-cursor-data-path:network:t29"
chapter_id: network-engines
title: "Spanner cursor state, statement kind cache, and state-error helpers in Rust"
description: "Add the shared cursor state, a per-SQL-text statement kind, and the two state-error helpers to vertebra-spanner."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-spanner/src/errors.rs:30-180"
  - "src/rs/vertebra-spanner/src/lib.rs:1-140"
  - "src/rs/vertebra-spanner/src/lib.rs:1076-1086"
  - "src/py/vertebra/db/adapters/spanner/core.py:79-104"
  - "src/py/vertebra/db/adapters/spanner/_sync.py:387-423"
files:
  - "src/rs/vertebra-spanner/src/cursor.rs"
  - "src/rs/vertebra-spanner/src/errors.rs"
  - "src/rs/vertebra-spanner/src/lib.rs"
tests:
  - "src/rs/vertebra-spanner/src/cursor.rs"
  - "src/rs/vertebra-spanner/src/errors.rs"
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
  - id: spanner-lib
    resource: "src/rs/vertebra-spanner/src/lib.rs::query_result_to_py"
    title: "Existing result conversion to Python"
tags: [spanner, rust, dbapi]
---

# Spanner cursor state, statement kind cache, and state-error helpers in Rust

## Objective & Scope Boundary

Give the Spanner extension the Rust parts both cursor classes share. Spanner needs to know whether a statement is a query, DML, or DDL before it sends it; that decision is computed once per SQL text in Rust, so Python stops reading the SQL.

Out of scope:

- The cursor classes (t30).
- Exception classes raised by existing client and transaction methods.
- Parameter encoding. It stays in `SpannerCore.encode_parameters_json`.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-spanner/src/lib.rs:53-113` holds `spanner_val_to_py`, `struct_field_to_py`, and `query_result_to_py`.
- `src/rs/vertebra-spanner/src/errors.rs:30-180` holds the exception type cache, stand-ins, `select_spanner_type`, and `attach_gcp_attrs`.
- `is_ddl_statement` in `src/py/vertebra/db/adapters/spanner/core.py` is the Python DDL test to port.

## Contracts & Edge Cases

- `cursor.rs` defines `CursorState` with the fields and methods in chapter spec section 2, all `pub(crate)`.
- `describe_fields(py, fields) -> PyResult<Py<PyTuple>>` builds `(name, type_name, None, None, None, None, None)` from the result field schema, with the Spanner type name as a Python `str`.
- `rows_to_tuples(py, result) -> PyResult<Vec<Py<PyTuple>>>` converts each value with `spanner_val_to_py`.
- `pub(crate) enum StatementKind { Query, Dml, Ddl }` and `pub(crate) fn statement_kind(sql: &str) -> StatementKind`, cached in a standard-library `Mutex` LRU keyed by SQL text with `CURSOR_PLAN_CACHE_CAPACITY: usize = 512`.
- Kind rules, applied to the first keyword after whitespace, `--` comments, and block comments, in any letter case: `INSERT`, `UPDATE`, `DELETE` give `Dml`; every keyword `SpannerCore.is_ddl_statement` accepts gives `Ddl`; all else gives `Query`. A statement that starts with `@{` hints skips the hint block first.
- `description` is `None` for `Ddl` and for `Dml` whose reply has no fields. `Dml` with `THEN RETURN` sets both `description` and `rowcount`. `rowcount` is the row count for `Query`, the reported affected-row count for `Dml`, and `-1` for `Ddl`. `lastrowid` is always `None`.
- `cursor.rs` defines `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` with a body that returns `Ok(())`. `lib.rs` adds `pub mod cursor;` and calls `cursor::register(m)?`. Task t30 fills the body.
- Errors: cursor code raises engine errors through `spanner_error_to_py`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| State rules | The state cases listed for t03 | Same results |
| Kind query | `SELECT 1`, `WITH x AS (...) SELECT ...` | `Query` |
| Kind DML | `  -- c\n update t set a = 1 where true` | `Dml` |
| Kind DDL | `CREATE TABLE t (...)`, `ALTER TABLE ...`, `DROP INDEX ...` | `Ddl` |
| Hint prefix | `@{FORCE_INDEX=idx} SELECT ...` | `Query` |
| Kind cached | Same text twice | Cache length 1 |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |
| Error attributes | Code 10 | `code` 10, `is_transient` true, `sqlstate` `40001` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-spanner --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing Spanner Rust tests pass unchanged.
- [ ] No Python-visible name is added.
