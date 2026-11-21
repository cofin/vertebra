---
type: Task
id: "native-cursor-data-path:network:t25"
chapter_id: network-engines
title: "Oracle out-binds on the native cursor"
description: "The Oracle extension defines the out-bind variable class, and the Oracle cursors detect out-bind variables in the parameters, run the statement with native out-bind hints, and fill the variables from the reply."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t24"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/oracle/core.py:815-967"
  - "src/py/vertebra/db/adapters/oracle/core.py:1018-1041"
  - "src/rs/vertebra-oracle/src/client.rs:212-272"
  - "src/rs/vertebra-oracle/src/client.rs:539-620"
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/py/vertebra/db/dbapi/_sync.py::_DBAPIOutBindVar"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPICursor"
  - "src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py:66-140"
  - "src/rs/vertebra-oracle/src/lib.rs"
files:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/lib.rs"
  - "src/py/vertebra/_oracle.pyi"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
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
  - id: oracle-out-binds-py
    resource: "src/py/vertebra/db/adapters/oracle/core.py::split_out_bind_parameters"
    title: "Python out-bind handling that moves into the cursor"
tags: [oracle, rust, pyo3, dbapi]
---

# Oracle out-binds on the native cursor

## Objective & Scope Boundary

Out-binds keep working through the native driver and the wrappers after the Python helpers are gone. A caller passes an out-bind variable among the parameters, and after `execute` the variable holds the value. The cursor also offers `var`, the PEP 249 extension the Oracle wrappers call. `var` returns `OracleOutBindVar`, a native class this task adds to the Oracle extension. No name is imported from the DB-API module.

Out of scope:

- Deleting the Python helpers (t27).
- `client.rs`. The existing out-bind spec types and worker commands are used as they are.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/oracle/core.py:883-967` are `split_out_bind_parameters` and `populate_out_bind_vars`; `:815-880` shows how out-bind values become a result row; `:1019-1041` are the out-bind test and the type hint mapping.
- `src/rs/vertebra-oracle/src/client.rs:225-272` are `OutBindSpec` and `OutBindsPayload`; `:539-620` is `extract_out_bind_spec`.
- In `src/py/vertebra/db/dbapi/_sync.py`, the class `_DBAPIOutBindVar` is the Python out-bind variable whose surface the native class carries over, and the `var` method of `SyncDBAPICursor` has the signature to keep. Locate both by name. They are read only.
- `src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py:66-140` shows what the SQLAlchemy dialect reads from the variable: `outconverter`, `values`, and `getvalue`.
- `src/rs/vertebra-oracle/src/lib.rs` for class registration and `__all__`.

## Contracts & Edge Cases

- `execute` gains a keyword-only argument: `execute(self, sql, params=(), *, out_binds: Mapping[str, str] | Sequence[str] | None = None) -> Self`.
- A parameter value is an out-bind variable when it has a callable `setvalue` attribute and a `type_code` attribute. This is the test `_is_out_bind_var` uses.
- Out-bind variables are removed from the input binds. Each gets a type hint from `type_code` using the mapping in `_out_bind_type_hint`: `BLOB`; `CLOB` for `CLOB` and `NCLOB`; `RAW` for `BINARY`, `BFILE`, `BYTES`, `RAW`; `NUMBER` for `NUMBER`, `INT`, `FLOAT`, `DECIMAL`; `TIMESTAMP` for `DATETIME`, `DATE`, `TIMESTAMP`; `BOOLEAN` for `BOOL`, `BOOLEAN`; otherwise `VARCHAR2`. An explicit entry in `out_binds` for the same name wins.
- The merged hints go to the worker through the existing out-bind spec. The execute command is used whenever any out-bind is present, whatever the statement kind.
- After the reply, each variable receives `setvalue(0, value)` with the value coerced by `coerce_cell_to_py` using its hint as the column type. Named variables match reply names without regard to case or a leading colon.
- A read-only property `out_binds` returns the coerced out-bind values of the last `execute`: a `dict` keyed by name without the colon for named binds, a `tuple` for positional binds, and `None` when the statement had none.
- When the reply has no rows and has out-bind values, the cursor stores one row made of those values. The description names are the out-bind names (lower-cased when the cursor lower-cases names) or `out_1`, `out_2`, and so on for positional binds. This keeps `INSERT ... RETURNING ... INTO` readable through `fetchall`.
- Add the native class `OracleOutBindVar` in `cursor.rs`, registered from `lib.rs` and listed in `__all__`. It carries over the surface `_DBAPIOutBindVar` has today:

| Member | Contract |
| --- | --- |
| Constructor | `OracleOutBindVar(type_code, size=0, arraysize=1, outconverter=None)`, callable from Python |
| `type_code` | read-write attribute; any object |
| `size` | read-write attribute; `int` |
| `arraysize` | read-write attribute; the constructor stores `max(int(arraysize), 1)` |
| `outconverter` | read-write attribute; a callable or `None` |
| `values` | read-write attribute; a Python `list`, created as `[None] * arraysize` |
| `getvalue(pos=0)` | returns `values[pos]`; when `outconverter` is set and the value is not `None`, returns `outconverter(value)`. An index out of range raises `IndexError` |
| `setvalue(pos, value)` | extends `values` with `None` up to `pos` when needed, then assigns |

  The Rust form is `#[pyclass(name = "OracleOutBindVar")]` with a `#[new]` constructor and `#[pyo3(get, set)]` on the five fields. `values` is held as `Py<PyList>`, so a caller that mutates the list sees the same object.
- Add `var(self, type_code, size=0, arraysize=1, outconverter=None) -> OracleOutBindVar` on both cursors. This is the parameter list the `var` method of `SyncDBAPICursor` has today. It builds the native class directly. `var`, `setinputsizes`, and `setoutputsize` exist on the Oracle cursors only.
- No `PyOnceLock`, import, or attribute lookup of a `vertebra.db.dbapi` name is added anywhere in the crate.
- The out-bind test above stays duck-typed, so a variable object from another library with `setvalue` and `type_code` still works.
- Stub: add the `OracleOutBindVar` class with the members above to `src/py/vertebra/_oracle.pyi` and to its `__all__`.
- Add `setinputsizes(self, *sizes) -> None` and `setoutputsize(self, size, column=None) -> None`. Both do nothing.
- The async cursor gets the same members. `var`, `setinputsizes`, and `setoutputsize` are plain methods on it.
- `executemany` rejects out-bind variables with `NotSupportedError`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Named out-bind | `BEGIN :out := :a + 1; END;` with a number variable for `out` | `var.getvalue()` is `a + 1`; `cursor.out_binds['out']` equal |
| Positional out-bind | Sequence parameters with one variable | `cursor.out_binds` is a one-item tuple |
| Returning into | `INSERT ... RETURNING id INTO :id` | `fetchall()` returns one row with the id; description name `id` |
| Explicit hint wins | Variable typed as string, `out_binds={'out': 'NUMBER'}` | Value is numeric |
| Case-insensitive match | Variable keyed `OUT`, reply key `out` | Variable filled |
| No out-binds | Plain select | `cursor.out_binds` is `None` |
| var | `cursor.var(NUMBER)` | An `OracleOutBindVar` from `vertebra._oracle`; `values == [None]`, `arraysize == 1`, `size == 0` |
| var surface | `OracleOutBindVar(NUMBER, arraysize=0, outconverter=str)`; `setvalue(2, 5)`; `getvalue(2)`; `getvalue(9)` | `arraysize == 1`; `values == [None, None, 5]`; `"5"`; `IndexError` |
| Converter skips `None` | `getvalue(0)` with `outconverter=str` and no value set | `None` |
| No DB-API import | `git grep -n "vertebra.db.dbapi\|dbapi" -- src/rs/vertebra-oracle/src` | No match |
| executemany | A set containing a variable | `NotSupportedError` |
| Async | The named case through the async cursor | Same result |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k "out_bind"`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -q`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] New tests named `test_cursor_out_bind_*` cover every matrix case.
- [ ] The stub diff adds the `out_binds` keyword, the `out_binds` property, `var`, `setinputsizes`, and `setoutputsize` on both cursor classes, and the `OracleOutBindVar` class.
- [ ] The Oracle crate holds no reference to a `vertebra.db.dbapi` name.
