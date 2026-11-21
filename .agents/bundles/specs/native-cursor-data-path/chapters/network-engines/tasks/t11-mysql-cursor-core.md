---
type: Task
id: "native-cursor-data-path:network:t11"
chapter_id: network-engines
title: "MySQL cursor state, named-bind plan cache, and state-error helpers in Rust"
description: "Add the shared cursor state, a cached named-parameter plan, parameter coercion, and the two state-error helpers to vertebra-mysql."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-mysql/src/client.rs:40-140"
  - "src/rs/vertebra-mysql/src/errors.rs:1-60"
  - "src/rs/vertebra-mysql/src/errors.rs:180-215"
  - "src/rs/vertebra-mysql/src/lib.rs:300-325"
  - "src/py/vertebra/db/adapters/mysql/core.py:114-178"
  - "src/py/vertebra/db/adapters/mysql/core.py:322-420"
files:
  - "src/rs/vertebra-mysql/src/cursor.rs"
  - "src/rs/vertebra-mysql/src/errors.rs"
  - "src/rs/vertebra-mysql/src/lib.rs"
tests:
  - "src/rs/vertebra-mysql/src/cursor.rs"
  - "src/rs/vertebra-mysql/src/errors.rs"
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
  - id: mysql-core-py
    resource: "src/py/vertebra/db/adapters/mysql/core.py::compile_for_driver"
    title: "Python named-bind compile that moves to Rust"
tags: [mysql, rust, dbapi]
---

# MySQL cursor state, named-bind plan cache, and state-error helpers in Rust

## Objective & Scope Boundary

Give the MySQL extension the Rust parts both cursor classes share. They are buffered result state, description and tuple builders, and a per-SQL-text plan for named parameters. The task also ports the value coercion the Python driver does today and adds the two state-error helpers. No Python class is added here.

Out of scope:

- The cursor classes and `client.cursor()` (t12).
- Deleting the Python compile helpers (t13).
- Exception classes raised by existing client methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-mysql/src/client.rs:111-140` is `DecodedResultSet` with `columns`, `rows`, `affected_rows`, and `last_insert_id`. `:40-110` is `into_py_any` on `DecodedCell`.
- `src/rs/vertebra-mysql/src/errors.rs:1-60` is `MySQLErrorKind`; `:180-215` is `to_py_err`.
- `src/py/vertebra/db/adapters/mysql/core.py:114-141` is `coerce_param`; `:166-178` is `compile_for_driver`; `:322-420` holds the literal and comment skipper and `_compile_named_mysql_binds`.

## Contracts & Edge Cases

- `cursor.rs` follows the module layout in chapter spec section 2 and defines `CursorState` with the fields and methods listed there, all `pub(crate)`.
- `describe_columns(py, columns: &[(String, u8)]) -> PyResult<Py<PyTuple>>` builds `(name, type_code, None, None, None, None, None)` with the MySQL column type code as a Python `int`.
- `rows_to_tuples(py, rows: Vec<Vec<DecodedCell>>) -> PyResult<Vec<Py<PyTuple>>>` converts each cell with `into_py_any` on `DecodedCell`.
- `description` is `None` when `DecodedResultSet.columns` is empty. `rowcount` is the row count for a result set and `affected_rows` otherwise. `lastrowid` is `last_insert_id` as a Python `int`, or `None` when the reply has none or it is `0`.
- `pub(crate) struct NamedPlan { sql: Box<str>, names: Box<[Box<str>]> }` and `pub(crate) fn named_plan_for(sql: &str) -> Result<Arc<NamedPlan>, MySQLError>`. It ports `_compile_named_mysql_binds` with target placeholder `?`: `:name` and `%(name)s` outside string literals, backtick identifiers, and comments become `?`, and `names` records one entry per placeholder occurrence in order.
- The plan cache is a standard-library `Mutex` around an LRU keyed by SQL text with `CURSOR_PLAN_CACHE_CAPACITY: usize = 512`.
- `pub(crate) fn bind_parameters(py, sql: &str, params: &Bound<PyAny>) -> PyResult<(Cow<str>, Vec<Bound<PyAny>>)>`: empty input returns the SQL unchanged and no values; a mapping uses `named_plan_for` and looks up each name; a sequence returns the SQL unchanged and the items in order. Positional `?` text is never rewritten.
- A name missing from the mapping raises `ProgrammingError` with the message text `_compile_named_mysql_binds` uses today.
- `pub(crate) fn coerce_value(py, value: Bound<PyAny>) -> PyResult<Bound<PyAny>>` ports `coerce_param`: `bytearray` and `memoryview` become `bytes`; `uuid.UUID` becomes its string; `msgspec.Struct`, `dict`, and `list` become JSON through the cached callable `vertebra.utils.serializers.encode_json`; every other value passes through. Exact-type checks for `NoneType`, `bool`, `int`, `float`, `str`, and `bytes` run first and return without further checks.
- Errors: cursor code raises engine errors through `to_py_err` on `MySQLError`, the conversion the client methods use, so every class is the one raised today, including the specific subclasses. `cursor.rs` adds `cursor_interface_err(py, message: &str) -> PyErr` and `cursor_programming_err(py, message: &str) -> PyErr` for state and parameter errors, as chapter spec section 2 describes. Both take their class from the class cache `errors.rs` already holds. When that cache has no entry for `InterfaceError` or `ProgrammingError`, add the entry to the same cache and make no other edit to `errors.rs`. No second class table, no second conversion function, and no import of the DB-API module is added.
- `cursor.rs` defines `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` with a body that returns `Ok(())`. `lib.rs` adds `mod cursor;` and calls `cursor::register(m)?` in the module init. Task t12 fills the body.
- `cursor.rs` carries a module-level `allow(dead_code)` inner attribute in this task only.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| State rules | The state cases listed for t03 | Same results |
| Description | Columns `[("id", 3), ("name", 253)]` | Two seven-item tuples with those codes |
| lastrowid zero | `last_insert_id` of `Some(0)` | `None` |
| Named plan | `WHERE a = :a AND b = %(b)s AND c = :a` | Three `?`; names `a`, `b`, `a` |
| Literal skipped | `SELECT ':x', `:y`, /* :z */ 1` | No rewrite; no names |
| Positional untouched | `WHERE a = ?` with a tuple | SQL unchanged |
| Missing name | `:a` with an empty mapping | `ProgrammingError` |
| Plan cache | Same text twice | Same `Arc` |
| Coercion | `bytearray`, `UUID`, `dict` | `bytes`, `str`, JSON text |
| State error classes | `cursor_interface_err(py, "x")`, `cursor_programming_err(py, "x")` | Type names `InterfaceError` and `ProgrammingError`; message `x` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql cursor`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql errors`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing `errors.rs` and `client.rs` tests pass unchanged.
- [ ] No Python-visible name is added.
