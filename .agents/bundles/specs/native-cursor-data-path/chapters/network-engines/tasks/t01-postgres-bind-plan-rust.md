---
type: Task
id: "native-cursor-data-path:network:t01"
chapter_id: network-engines
title: "PostgreSQL bind plan and placeholder rewriting in Rust"
description: "Port the Python placeholder scanner to a Rust bind plan cached per SQL text and expose it as one extension function."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/postgres/binds.py:36-53"
  - "src/py/vertebra/db/adapters/postgres/binds.py:124-333"
  - "src/py/vertebra/db/adapters/postgres/binds.py:335-480"
  - "src/rs/vertebra-postgres/src/lib.rs:972-1043"
  - "src/rs/vertebra-postgres/src/statements.rs:209-240"
  - "src/py/vertebra/_postgres.pyi:1035-1035"
files:
  - "src/rs/vertebra-postgres/src/bind_plan.rs"
  - "src/rs/vertebra-postgres/src/lib.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/rs/vertebra-postgres/src/bind_plan.rs"
estimated_loc: 300
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
  - id: binds-py
    resource: "src/py/vertebra/db/adapters/postgres/binds.py::_scan_sql_bind_plan"
    title: "Python scanner that this task ports"
tags: [postgres, rust, performance]
---

# PostgreSQL bind plan and placeholder rewriting in Rust

## Objective & Scope Boundary

PostgreSQL callers that pass `%s`, `%(name)s`, or `$N` placeholders get the rewritten `$N` text and the parameter order from Rust. The plan is computed once per SQL text and reused.

Add `bind_plan.rs` with the scanner, the plan type, a process-wide cache, and the extension function `compile_binds`. Register the module and the function in `lib.rs`. Add the stub entry.

Out of scope:

- Calling the plan from the cursor. Tasks t04 to t06 do that.
- Editing `binds.py`. Task t02 moves it onto `compile_binds`.
- Parameter value normalization. It stays in `normalize_postgres_parameter` in `binds.py`.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/postgres/binds.py:124-333` is the behavior to port: `_scan_sql_bind_plan` and `compile_postgres_binds`.
- `src/py/vertebra/db/adapters/postgres/binds.py:335-480` holds the scanner helpers for quotes, dollar quotes, comments, and chunk emission.
- `src/rs/vertebra-postgres/src/lib.rs:972-1043` is the module init where the function is registered.
- `src/rs/vertebra-postgres/src/statements.rs:209-240` shows the crate's existing cache structure for naming and style.

## Contracts & Edge Cases

- `pub enum BindStyle { None, Numeric, Format, Named }`. `Numeric` is `$N`, `Format` is `%s`, `Named` is `%(name)s`.
- `pub struct BindPlan { pub sql: Box<str>, pub style: BindStyle, pub param_count: usize, pub names: Box<[Box<str>]> }`. `sql` is the text sent to the server. `names` lists each distinct name in first-use order and is empty for other styles.
- `pub fn scan(sql: &str) -> Result<BindPlan, PostgresError>` ports `_scan_sql_bind_plan` one for one. Placeholders inside single quotes, `E''` strings, double-quoted identifiers, dollar-quoted bodies, line comments, and nested block comments are left alone. `%%` becomes `%` only when the statement uses `Format` or `Named`.
- `pub fn plan_for(sql: &str) -> Result<Arc<BindPlan>, PostgresError>` reads a process-wide cache keyed by the SQL text. The cache is a standard-library `Mutex` around an LRU with `BIND_PLAN_CACHE_CAPACITY: usize = 1024`. A scan error is returned and never cached.
- `pub fn order_parameters<'py>(py: Python<'py>, plan: &BindPlan, parameters: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>>` returns the object to bind. `None` and empty input return an empty tuple. A mapping with `Named` returns a tuple ordered by `plan.names`. A sequence is returned unchanged when it is already a tuple and converted to a tuple otherwise.
- Five error cases raise the same exception type and message text as `compile_postgres_binds` does today. They are a missing name, a mapping passed to a non-`Named` statement, a sequence passed to a `Named` statement, mixed placeholder styles, and a count mismatch. Read the messages from `binds.py:267-333` and copy them.
- `#[pyfunction] compile_binds(sql: &str, parameters: Option<&Bound<PyAny>> = None) -> PyResult<(String, Py<PyAny>)>` returns `(plan.sql, ordered_parameters)`. Stub: `def compile_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]: ...`.
- A statement with no placeholder returns its text unchanged and `None` on `BindStyle`. The returned `sql` for that case is the same string content, so the server statement cache key does not change.
- The cache key is the full SQL text. Trace comments appended by the driver make a distinct key. That matches the current behavior and is not changed here.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| No placeholders | `SELECT 1` | Text unchanged, style `None`, count 0 |
| Format | `SELECT %s, %s` | `SELECT $1, $2`, count 2 |
| Named reuse | `WHERE a = %(x)s OR b = %(x)s AND c = %(y)s` | `$1`, `$1`, `$2`; names `x`, `y` |
| Numeric passthrough | `SELECT $2, $1` | Text unchanged, count 2 |
| Quoted text | `SELECT '%s', "%s", $$ %s $$` | No rewrite, count 0 |
| Comments | `SELECT 1 -- %s` and nested `/* /* %s */ */` | No rewrite |
| Escaped percent | `SELECT 10 %% 3, %s` | `SELECT 10 % 3, $1` |
| Mixed styles | `SELECT %s, $1` | Error with the Python message |
| Missing name | `%(x)s` with `{}` | Error with the Python message |
| Cache reuse | Same text twice | Second call returns the same `Arc` |
| Cache bound | 1025 distinct texts | Cache length stays at 1024 |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres bind_plan`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._postgres`

## Acceptance Criteria

- [ ] Every matrix case has a Rust unit test in `bind_plan.rs` that fails before the scanner exists and passes after.
- [ ] `vertebra._postgres.compile_binds` is importable and its stub passes `mypy.stubtest` with no allow-list change.
- [ ] No file outside `files` is edited.
