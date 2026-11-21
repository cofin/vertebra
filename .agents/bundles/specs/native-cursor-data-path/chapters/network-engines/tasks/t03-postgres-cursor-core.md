---
type: Task
id: "native-cursor-data-path:network:t03"
chapter_id: network-engines
title: "PostgreSQL cursor state and state-error helpers in Rust"
description: "Add the shared cursor state, description builder, row tuple builder, and the two state-error helpers to vertebra-postgres."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t01"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-postgres/src/errors.rs:150-250"
  - "src/rs/vertebra-postgres/src/python_values.rs:1145-1150"
  - "src/rs/vertebra-postgres/src/python_values.rs:1406-1450"
  - "src/rs/vertebra-postgres/src/query.rs:165-186"
  - "src/rs/vertebra-postgres/src/lib.rs:1-60"
  - "src/py/vertebra/db/adapters/postgres/types.py::_project_public_row"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/errors.rs"
  - "src/rs/vertebra-postgres/src/lib.rs"
tests:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/errors.rs"
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
  - id: pg-errors
    resource: "src/rs/vertebra-postgres/src/errors.rs"
    title: "Existing exception type cache and SQLSTATE mapping"
tags: [postgres, rust, dbapi]
---

# PostgreSQL cursor state and state-error helpers in Rust

## Objective & Scope Boundary

Give the PostgreSQL extension the Rust parts every cursor class shares: buffered result state, the seven-item description, tuple rows, and the two state-error helpers. No Python class is added in this task.

Out of scope:

- The `SyncPostgresCursor` and `AsyncPostgresCursor` classes. Tasks t04 to t06 add them.
- Changing the exception classes raised by existing client, pool, COPY, portal, or replication methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-postgres/src/errors.rs:150-250` holds `PostgresPyExceptionTypes`, the stand-in builder, and `instantiate_exc`. The state-error helpers take their classes from this cache.
- `src/rs/vertebra-postgres/src/python_values.rs:1145` (`pg_value_to_python`) converts one cell. `:1406-1450` shows how a `QueryResult` is walked.
- `src/py/vertebra/db/adapters/postgres/types.py::_project_public_row` is the Python row projection the tuple builder replaces.

## Contracts & Edge Cases

- `cursor.rs` follows the module layout in chapter spec section 2. It defines `CursorState`, `describe_columns`, and `row_to_tuple`, all `pub(crate)`.
- `CursorState` fields: `rows: Vec<Py<PyTuple>>`, `pos: usize`, `description: Option<Py<PyTuple>>`, `rowcount: i64`, `lastrowid: Option<Py<PyAny>>`, `arraysize: usize`, `executed: bool`, `closed: bool`. `new()` sets `rowcount` to `-1` and `arraysize` to `1`.
- `CursorState` methods: `store(description, rows, rowcount, lastrowid)`, `fetchone(py)`, `fetchmany(py, size: Option<usize>)`, `fetchall(py)`, `close()`, `ensure_open(py)`. Their behavior is the cursor contract table in the root spec.
- `describe_columns(py, columns: &[Column]) -> PyResult<Py<PyTuple>>` builds one `(name, type_oid, None, None, None, None, None)` tuple per column. `type_oid` is the column type OID as a Python `int`.
- `row_to_tuple(py, row) -> PyResult<Py<PyTuple>>` converts each cell with `pg_value_to_python` and applies every conversion `_project_public_row` applies today, so the tuple equals the current public row.
- `rowcount` for a statement with a result set is the row count. For a command it is the affected-row count from the command tag. For commands that report none, it is `-1`.
- `lastrowid` is always `None` for PostgreSQL.
- Errors: cursor code raises engine errors through `postgres_error_to_py`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only. Task t04 removes it.
- `lib.rs` adds `mod cursor;`. Nothing is registered with the Python module yet.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Fresh state | `CursorState::new()` | `rowcount` -1, `arraysize` 1, no description |
| Fetch before execute | `fetchone` on fresh state | `ProgrammingError` |
| Fetch after command | `store(None, [], 3, None)` then `fetchall` | `ProgrammingError`; `rowcount` 3 |
| Empty result set | `store(Some(desc), [], 0, None)` | `fetchall` returns `[]`; description present |
| fetchmany default | Five rows, `arraysize` 2, `fetchmany(None)` | Two rows, then two, then one, then none |
| fetchall remainder | Five rows after one `fetchone` | Four rows, then `[]` |
| Closed | `close()` then `fetchone` | `InterfaceError`; second `close()` is a no-op |
| Description shape | Two columns | Two seven-item tuples, OID in position 1 |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |
| Error attributes | Any server error | `sqlstate` and `is_transient` set |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing `errors.rs` tests pass unchanged.
- [ ] `make dev` builds and `mypy.stubtest vertebra._postgres` passes, since no Python surface changed.
