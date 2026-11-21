---
type: Task
id: "native-cursor-data-path:single:t02"
chapter_id: single-call-paths
title: "SQLite bind fast path and borrowed SQL text"
description: "Check exact tuple, list, and dict types first in SQLite parameter extraction, dispatch common value types before the conversion chain, and pass the sync cursor's SQL text to the pool without copying it."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t03"
read_set:
  - "src/rs/vertebra-sqlite/src/value.rs:1-10"
  - "src/rs/vertebra-sqlite/src/value.rs:51-132"
  - "src/rs/vertebra-sqlite/src/value.rs:134-205"
  - "src/rs/vertebra-sqlite/src/py_client.rs:738-776"
  - "src/rs/vertebra-sqlite/src/py_client.rs:826-854"
  - "src/rs/vertebra-sqlite/tests/sqlite_client.rs:1-20"
  - "src/py/tests/unit/db/test_sqlite_driver.py:1-21"
  - "src/py/tests/unit/db/test_sqlite_driver.py:684-725"
files:
  - src/rs/vertebra-sqlite/src/value.rs
  - src/rs/vertebra-sqlite/src/py_client.rs
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 190
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: sqlite-binds
    resource: src/rs/vertebra-sqlite/src/value.rs::BoundParams
    title: "SQLite parameter extraction from Python sequences and mappings"
tags: [sqlite, pyo3, binds, performance]
---

# SQLite bind fast path and borrowed SQL text

## Objective & Scope Boundary

Passing a `tuple`, `list`, or `dict` of parameters to SQLite no longer runs an abstract-base-class check, and an empty container costs the same as no parameters. The sync cursor stops copying the SQL text.

Every conversion that works today keeps working with the same result. The protocol checks stay as the fallback for other types.

This task depends on t03, which gives `run` on `SqlitePool` its `&str` parameter. It does not touch `worker.rs`.

## Bounded Read Manifest

- `value.rs:1-10` — imports.
- `value.rs:51-132` — `from_py` on `SqliteValue`, the conversion chain.
- `value.rs:134-205` — `BoundParams` and `from_py`.
- `py_client.rs:738-776` — `run_and_install`, the bound on its closure, and the `py.detach` call.
- `py_client.rs:826-854` — `SyncSQLiteCursor.execute` and `executemany`.
- `tests/sqlite_client.rs:1-20` — imports of the crate's integration test file.
- `test_sqlite_driver.py:1-21`, `684-725` — imports and the cursor outcome test style.

## Symbol & Signature Contracts

Signatures do not change:

```rust
impl BoundParams {
    pub fn from_py(parameters: Option<&Bound<'_, PyAny>>) -> Result<Self, SqliteClientError>;
}

impl SqliteValue {
    pub fn from_py(obj: &Bound<'_, PyAny>) -> Result<Self, SqliteClientError>;
}
```

### Parameter extraction (`from_py` on `BoundParams`), new order

1. Absent, or `None`: `Empty`.
2. `cast_exact::<PyTuple>()` succeeds: `Empty` when its length is `0`. Otherwise `Positional`, filled by iterating the tuple with `iter()`.
3. `cast_exact::<PyList>()` succeeds: `Empty` when its length is `0`. Otherwise `Positional`, filled by iterating the list with `iter()`.
4. `cast_exact::<PyDict>()` succeeds: `Empty` when its length is `0`. Otherwise `Named`, filled with `locked_for_each`. A key that is not `str` fails with the existing message `parameter key must be str: {e}`.
5. The existing `PyMapping` branch, unchanged.
6. The existing `PySequence` branch, unchanged.
7. The existing error `parameters must be a sequence or mapping`.

Steps 2 to 4 use no `PySequence` or `PyMapping` call. The vector and the map are created with the container's length as capacity. No allocation happens for an empty container.

### Value conversion (`from_py` on `SqliteValue`), new order

1. `None`: `Null`. Unchanged.
2. Exact-type dispatch. These types exclude each other, so their order among themselves cannot be observed:
   - exact `str` (`cast_exact::<PyString>()`): `Text`.
   - exact `int` (`cast_exact::<PyInt>()`): `Integer` when `extract::<i64>()` succeeds. When it overflows, fall through to step 3.
   - exact `float` (`cast_exact::<PyFloat>()`): `Real`.
   - exact `bytes` (`cast_exact::<PyBytes>()`): `Blob`.
3. The existing chain, in its existing order, unchanged: `bool`, `extract::<i64>`, `float`, `str`, `bytes`, `bytearray`, `isoformat()`, `TypedParameter`, `Decimal` and `UUID`, JSON for `dict` and `list`, then the unsupported-type error.

`bool` is not an exact `int`, so `True` still reaches the `bool` branch before the integer extraction. Subclasses of `str`, `int`, `float`, and `bytes` reach the existing chain and convert as today.

Import changes in `value.rs`: add `PyInt`, `PyList`, and `PyTuple` to the `types` import from `pyo3`, and use the imported `PyList` in the JSON branch in place of the full path.

### Borrowed SQL text

`SyncSQLiteCursor.execute` already receives `sql: &str`. Delete the `sql_owned` copy and pass the borrow:

```rust
this.run_and_install(py, this.lazy_begin, move |pool| pool.run(sql, bound))?;
```

Why this is sound:

- PyO3 borrows the `&str` from the UTF-8 buffer cached on the Python `str` object. The interpreter keeps a reference to that argument for the whole call, and a `str` cannot change.
- `run_and_install` requires its closure to be `Send`, not `'static`. `&str` is `Send`, so the closure may cross `py.detach`.
- `py.detach` returns before `execute` returns. The borrow ends inside the call, while the argument is still referenced.
- The pool does not keep the text. `run` on `SqlitePool` uses it for the facts lookup and for `prepare_cached`, and both copy what they store.

`executemany` keeps `sql.to_string()`, because `run_many` takes an owned `String`. `AsyncSQLiteCursor.execute` keeps its owned copy, because the statement runs in a spawned task after the call returns.

## Edge Cases & Error Semantics

- `()`, `[]`, and `{}` give `Empty`. A statement that expects a parameter then fails in the engine exactly as it does with no parameters.
- A `tuple` subclass such as a named tuple, and a `list` subclass, take the `PySequence` branch and bind by position.
- A `dict` subclass and any other `Mapping`, such as `types.MappingProxyType`, take the `PyMapping` branch and bind by name.
- A `str` passed as the parameters object is still a sequence of characters, as today.
- A generator is rejected with `parameters must be a sequence or mapping`, as today.
- A value that fails conversion inside steps 2 to 4 returns the same error the fallback branches return for that value.
- A list or dict changed by another thread during extraction: `iter` on `PyList` checks its bounds on each step, and `locked_for_each` holds the dict's critical section on a free-threaded build.
- An exact `int` outside the 64-bit range ends in the unsupported-type error, as today.

## Public-Seam Test Blueprint

Rust tests, added to `tests/sqlite_client.rs`. Each attaches the interpreter with `attach` on `Python` and builds the input with `py.eval`. Add `use pyo3::prelude::*;` and the needed `pyo3` types to the file's imports; the crate's dev dependency on `pyo3` already enables `auto-initialize`.

| Test | Input | Expected |
| --- | --- | --- |
| `test_bound_params_exact_containers` | `(1, 'a')`, `[1, 'a']`, `{'k': 1}` | `Positional([Integer(1), Text("a")])` twice; `Named` with `k -> Integer(1)` |
| `test_bound_params_empty_containers_are_empty` | `()`, `[]`, `{}`, `None`, and no object | the `Empty` variant in each case |
| `test_bound_params_subclasses_and_mappings_use_fallback` | a named tuple `(1, 2)`; a `list` subclass; a `dict` subclass; `types.MappingProxyType({'k': 1})` | positional, positional, named, named, with the same values |
| `test_bound_params_rejects_generator_and_non_str_key` | `(x for x in [1])`; `{1: 'a'}` | `InvalidInput` with `parameters must be a sequence or mapping`; `InvalidInput` with `parameter key must be str` |
| `test_sqlite_value_conversion_order` | `True`, `1`, `2**70`, `1.5`, `'s'`, `b'b'`, `bytearray(b'b')`, an `int` subclass instance, a `str` subclass instance, `datetime.date(2026, 1, 2)`, `decimal.Decimal('1.5')`, `{'a': 1}` | `Integer(1)`, `Integer(1)`, an `InvalidInput` error, `Real(1.5)`, `Text("s")`, `Blob`, `Blob`, `Integer`, `Text`, `Text("2026-01-02")`, `Text("1.5")`, `Text` holding the JSON text |

Python tests, added to `src/py/tests/unit/db/test_sqlite_driver.py`, on `SyncSQLiteClient(":memory:").cursor()`:

| Case | Steps | Expected |
| --- | --- | --- |
| Tuple, list, dict | `SELECT ?, ?` with `(1, "a")` and with `[1, "a"]`; `SELECT :v` with `{"v": 5}` | `[(1, "a")]` twice; `[(5,)]` |
| Empty tuple, empty dict, `None` | `SELECT 1` with `()`, `{}`, `[]`, and `None` | `[(1,)]` each time |
| Tuple subclass | `SELECT ?, ?` with a named tuple `(1, 2)` | `[(1, 2)]` |
| Mapping that is not a dict | `SELECT :v` with `types.MappingProxyType({"v": 5})` | `[(5,)]` |
| Generator | `SELECT ?` with `(x for x in [1])` | `InterfaceError`; the cursor result is cleared |
| Same SQL object reused | one `sql` string executed 100 times on one cursor with `(i,)` | each `fetchall()` is `[(i,)]` |

## Verification

- Exports for raw `cargo test`: copy the interpreter exports from the `rs-test` recipe in `Makefile`.
- Red then green, Rust: `(cd src/rs && cargo test -p vertebra-sqlite --test sqlite_client bound_params --locked && cargo test -p vertebra-sqlite --test sqlite_client sqlite_value --locked)`
- Whole crate: `(cd src/rs && cargo test -p vertebra-sqlite --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-sqlite && cargo clippy -p vertebra-sqlite --all-targets --locked -- -D warnings)`
- Rebuild: `make dev`
- Python: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q`
- Removed copy, must print nothing: `git grep -n "pool.run(&sql_owned" -- src/rs/vertebra-sqlite/src/py_client.rs`

## Acceptance Criteria

- [ ] The five Rust tests and the six Python cases pass.
- [ ] `from_py` on `BoundParams` reaches the `PyMapping` and `PySequence` casts only for an object that is not an exact `tuple`, `list`, or `dict`.
- [ ] `SyncSQLiteCursor.execute` makes no owned copy of the SQL text.
- [ ] No existing test in `test_sqlite_driver.py` or `tests/sqlite_client.rs` changes its assertions.

## Notes & Discoveries

- The measured cost this removes is in the hot-path research brief: an empty tuple costs 0.35 us more than no parameters on the cursor path.
