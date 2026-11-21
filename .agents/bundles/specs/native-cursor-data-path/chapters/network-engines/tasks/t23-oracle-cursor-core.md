---
type: Task
id: "native-cursor-data-path:network:t23"
chapter_id: network-engines
title: "Oracle cursor state and state-error helpers in Rust"
description: "Add the shared cursor state, description and tuple builders, and the two state-error helpers to vertebra-oracle."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-oracle/src/errors.rs:1-60"
  - "src/rs/vertebra-oracle/src/errors.rs:255-310"
  - "src/rs/vertebra-oracle/src/errors.rs:400-420"
  - "src/rs/vertebra-oracle/src/types.rs:52-150"
  - "src/rs/vertebra-oracle/src/lib.rs:400-427"
files:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/errors.rs"
  - "src/rs/vertebra-oracle/src/lib.rs"
tests:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/errors.rs"
estimated_loc: 220
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
  - id: oracle-errors
    resource: "src/rs/vertebra-oracle/src/errors.rs::to_py_err"
    title: "Existing exception conversion"
tags: [oracle, rust, dbapi]
---

# Oracle cursor state and state-error helpers in Rust

## Objective & Scope Boundary

Give the Oracle extension the Rust parts both cursor classes share: buffered result state, the description builder, and the two state-error helpers.

Out of scope:

- The cursor classes (t24).
- Value coercion (t22). The tuple builder takes already-converted Python cells.
- Exception classes raised by existing client, pool, and listener methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-oracle/src/errors.rs:1-60` holds `OracleErrorKind` and the classification type; `:255-310` resolves exception types; `:406-420` is `to_py_err`.
- `src/rs/vertebra-oracle/src/types.rs:52-150` is `OracleColumn`, which carries the column name and Oracle type name.
- `src/rs/vertebra-oracle/src/lib.rs:406-427` is the module init.

## Contracts & Edge Cases

- `cursor.rs` defines `CursorState` with the fields and methods in chapter spec section 2, all `pub(crate)`.
- `describe_columns(py, columns: &[OracleColumn], lowercase: bool) -> PyResult<Py<PyTuple>>` builds `(name, type_name, None, None, None, None, None)`. `name` is lower-cased when `lowercase` is true. `type_name` is the canonical Oracle type name as a Python `str`.
- `description` is `None` when the reply has no columns and no out-bind values. `rowcount` is the row count for a query and the affected-row count for DML and PL/SQL. `lastrowid` is always `None`.
- `cursor.rs` defines `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` with a body that returns `Ok(())`. `lib.rs` adds `pub mod cursor;` and calls `cursor::register(m)?`. Task t24 fills the body.
- Errors: cursor code raises engine errors through `to_py_err` on `OracleError`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| State rules | The state cases listed for t03 | Same results |
| Description lower | Column `ID`, `lowercase` true | Name `id` |
| Description upper | Column `ID`, `lowercase` false | Name `ID` |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |
| Error attributes | ORA-00001 | `sqlstate` `23505`, `vendor_code` 1 |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing `errors.rs` tests pass unchanged.
- [ ] `make dev` builds and `mypy.stubtest vertebra._oracle` passes, since no Python surface changed.
