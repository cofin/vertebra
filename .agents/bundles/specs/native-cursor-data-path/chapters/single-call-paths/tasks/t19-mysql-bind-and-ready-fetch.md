---
type: Task
id: "native-cursor-data-path:single:t19"
chapter_id: single-call-paths
title: "MySQL bind fast path and completed fetch awaitables"
description: "Check exact tuple, list, and dict types first in the MySQL bind code, and return completed awaitables from the async MySQL cursor's fetch and close methods."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:cleanup:t14"
read_set:
  - "src/rs/vertebra-mysql/src/cursor.rs:1-22"
  - "src/rs/vertebra-mysql/src/cursor.rs:104-170"
  - "src/rs/vertebra-mysql/src/cursor.rs:455-543"
  - "src/rs/vertebra-mysql/src/cursor.rs:640-660"
  - "src/rs/vertebra-mysql/src/cursor.rs:980-996"
  - "src/rs/vertebra-mysql/src/cursor.rs:1183-1224"
  - "src/rs/vertebra-mysql/src/cursor.rs:1234-1247"
  - "src/rs/vertebra-mysql/src/cursor.rs:1447-1466"
  - "src/py/tests/unit/db/test_mysql_driver.py:2396-2412"
  - "Makefile:215-232"
files:
  - "src/rs/vertebra-mysql/src/cursor.rs"
tests:
  - "src/py/tests/unit/db/test_mysql_driver.py"
estimated_loc: 130
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: mysql-cursor
    resource: src/rs/vertebra-mysql/src/cursor.rs::bind_parameters
    title: "MySQL parameter binding and the async cursor fetch methods"
tags: [mysql, pyo3, performance]
---

# t19 — MySQL bind fast path and completed fetch awaitables

## Objective & Scope Boundary

After this task, an async MySQL cursor returns rows that are already in memory without spawning a task, and an empty parameter container costs the same as no parameters.[^mysql-cursor]

In scope, all in `src/rs/vertebra-mysql/src/cursor.rs`:

- `bind_parameters`: exact-type checks first.
- `fetchone`, `fetchmany`, `fetchall`, and `close` on `AsyncMySQLCursor`: return the completed awaitable from t01.
- The private `ready_awaitable` function: deleted.
- Rust tests in the file's `mod tests` and Python tests in `test_mysql_driver.py`.

Out of scope:

- `execute` and `executemany` on either cursor. They still spawn a task.
- `coerce_value`, `named_plan_for`, `normalize_positional_sql`, and `prepare_cursor_many_params`.
- `src/py/vertebra/_mysql.pyi`. The stub return types stay `Awaitable[T]`.
- The sync cursor.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `cursor.rs:1-22` | Imports to edit |
| `cursor.rs:104-170` | `ensure_result_set`, `fetchone`, `fetchmany`, `fetchall`, `close` on `CursorState` and the errors they raise |
| `cursor.rs:455-543` | `bind_parameters` and `normalize_positional_sql` |
| `cursor.rs:640-660` | `prepare_cursor_params`, the one caller that turns bound values into `Params` |
| `cursor.rs:980-996` | `ready_awaitable`, deleted here |
| `cursor.rs:1183-1224` | The four async methods to change |
| `cursor.rs:1234-1247`, `1447-1466` | Test module header and the existing bind test |
| `test_mysql_driver.py:2396-2412` | Setup lines of `test_async_cursor_matrix_and_fetch_controls`, reused by the new test |
| `Makefile:215-232` | Interpreter exports that `cargo test` needs for a PyO3 crate |

Delivered by t01 and read from `src/rs/vertebra-db/src/py_async.rs`: `ReadyAwaitable`, and `ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>`. If either name is missing from the `py_async` module of `vertebra-db`, set the task to `blocked`.

## Contracts & Edge Cases

### Imports

- `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr};`
- No other import changes. `ready_err` is not used in this crate.

### `bind_parameters`

The signature is unchanged. The checks run in this order:

1. `params.is_none()`: return `(Cow::Borrowed(sql), Vec::new())`.
2. `params.cast_exact::<PyTuple>()`: compute `normalize_positional_sql(sql)`. An empty tuple returns it with `Vec::new()`. Otherwise return it with `tuple.iter().collect()`.
3. `params.cast_exact::<PyList>()`: the same two cases for a list.
4. `params.cast_exact::<PyDict>()`: an empty dict returns `(Cow::Borrowed(sql), Vec::new())` and does not call `named_plan_for`. Otherwise call the new helper `bind_named_dict`.
5. Fallback, in the order the function uses now: `params.cast::<PyDict>()` calls `bind_named_dict`; `params.cast::<PyTuple>()` and `params.cast::<PyList>()` keep their current bodies; then the `Mapping` check through `resolve_coerce_types`; then the `str`, `bytes`, and `bytearray` rejection; then iteration.

New private helper, placed directly above `bind_parameters`:

```rust
fn bind_named_dict<'py, 's>(
    py: Python<'py>,
    sql: &'s str,
    dict: &Bound<'py, PyDict>,
) -> PyResult<(Cow<'s, str>, Vec<Bound<'py, PyAny>>)>
```

Its body is the current dict branch at `cursor.rs:466-477`, moved without change. Both dict branches call it.

Rules:

- `Vec::new()` does not allocate, and `normalize_positional_sql` borrows the SQL unless it holds a `%`. That is the no-parameter value for this crate.
- An empty tuple or list still passes through `normalize_positional_sql`. A statement written with `%%` and an empty parameter container keeps today's result.
- An empty exact `dict` equals `None`. A statement such as `SELECT :a` with `{}` is sent unchanged, and the server reports the error. Today the extension raises `ProgrammingError` with `Missing MySQL bind parameter 'a'` for that input. A non-empty dict that lacks the name still raises that error.
- A subclass of `tuple`, `list`, or `dict` takes the fallback and behaves as it does today, including an empty subclass instance.

### Async fetch methods

When state errors surface: at the call, as today. `fetchone`, `fetchmany`, and `fetchall` take the state lock, run the `CursorState` method, and propagate its error with `?` before any awaitable exists. A closed cursor raises `InterfaceError` and a missing result set raises `ProgrammingError` from the method call itself, not from the `await`. This task keeps that rule.

```rust
pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
#[pyo3(signature = (size = None))]
pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>
pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
```

- `fetchone`: keep the locked block. Return `ready_ok(py, row.map_or_else(|| py.None(), Py::into_any))`.
- `fetchmany`: keep the locked block with `ensure_open`, `check_fetchmany_size`, and `state.fetchmany`. Return `ready_ok(py, PyList::new(py, rows)?.into_any().unbind())`.
- `fetchall`: keep the locked block. Return `ready_ok(py, PyList::new(py, rows)?.into_any().unbind())`.
- `close`: close the state under the lock, then return `ready_ok(py, py.None())`.
- Delete `ready_awaitable`. No caller remains.

| Case | Behavior |
| --- | --- |
| `fetchall()` called outside a running event loop | Returns an awaitable. Today it raises because a task cannot be spawned |
| `close()` on a closed cursor | Completed awaitable with `None`. Idempotent |
| `fetchmany(-1)` | `ProgrammingError` at the call |
| `fetchmany(0)` | Completed awaitable with `[]` |
| Fetch while an `execute` is in flight | Reads the state as stored before that `execute` completes, as today |

## Test Matrix

Rust, in `mod tests` of `cursor.rs`:

| Test | Input | Expected |
| --- | --- | --- |
| `test_cursor_bind_parameters_exact_empty_containers` | `()`, `[]`, `{}` with `SELECT 1` | Borrowed SQL equal to the input, no values |
| same test | `()` with `SELECT '50%%'` | Owned SQL from `normalize_positional_sql`, no values |
| same test | `{}` with `SELECT :a` | Borrowed SQL `SELECT :a`, no values, no error |
| `test_cursor_bind_parameters_exact_and_subclass_containers` | `[1, 2]` with `SELECT ?, ?` | Two values in order |
| same test | `{"a": 1}` with `SELECT :a` | SQL `SELECT ?`, one value |
| same test | a `collections.namedtuple` instance and a `collections.OrderedDict` built with `py.import` | Same results as the plain tuple and dict |
| `test_cursor_bind_parameters_positional_and_missing_name`, existing | change the missing-name input from an empty dict to `{"b": 1}` | Still `ProgrammingError` with `Missing MySQL bind parameter 'a'` |

Python, in `src/py/tests/unit/db/test_mysql_driver.py`, placed after `test_async_cursor_matrix_and_fetch_controls` and using the same `mysql_server` fixture and `AsyncMySQLClient(mysql_server.dsn)` setup:

| Test | Steps | Expected |
| --- | --- | --- |
| `test_async_cursor_fetch_returns_completed_awaitable` | `await cur.execute("SELECT 2 AS a, 'x' AS b")`, then `step = cur.fetchall().__await__()` and `next(step)` | `StopIteration` on the first `next`, with `value == [(2, "x")]`. The loop does not iterate |
| `test_async_cursor_ready_methods_complete_without_loop`, `@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "close"])` | After an `execute`, call the method and drive `__await__` with one `next` | `StopIteration` on the first `next` |
| `test_async_cursor_state_errors_raise_at_call` | `cur.fetchall()` before any `execute`, and `cur.fetchone()` after `await cur.close()`, neither awaited | `ProgrammingError` and `InterfaceError` raised by the call |
| `test_async_cursor_empty_container_matches_no_parameters`, `@pytest.mark.parametrize("params", [(), [], {}])` | `await cur.execute("SELECT 1 AS a", params)` | `await cur.fetchall() == [(1,)]` |

Each extension compiles its own copy of the awaitable class. Tests assert behavior and never use `isinstance` against a class from another module.

## Verification

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export PYO3_PYTHON="$(realpath .venv/bin/python)"
export PYTHONHOME="$("$PYO3_PYTHON" -c 'import sys; print(sys.base_prefix)')"
export LD_LIBRARY_PATH="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$PWD/src/py:$("$PYO3_PYTHON" -c 'import os, site; print(os.pathsep.join(site.getsitepackages()))')"
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql cursor
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql --all-targets -- -D warnings
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_mysql_driver.py -p no:xdist -q -k "cursor"
uv run --no-sync --frozen python -m mypy.stubtest vertebra._mysql
test -z "$(git grep -n 'fn ready_awaitable' -- src/rs/vertebra-mysql/src)"
```

The exports mirror the `rs-test` recipe. `make dev` rebuilds the extension before the Python tests.

## Acceptance Criteria

- [ ] `bind_parameters` tests `PyTuple`, `PyList`, and `PyDict` with `cast_exact` before any other container check.
- [ ] An empty exact tuple, list, or dict produces no values and no named-plan lookup.
- [ ] The four async methods return `PyResult<Py<ReadyAwaitable>>` and call no `spawn_rust_db_task_with_attr`.
- [ ] State errors from the three fetch methods are raised at the call.
- [ ] `ready_awaitable` is gone from the crate.
- [ ] `src/py/vertebra/_mysql.pyi` has no diff and `mypy.stubtest` passes.
- [ ] The new Rust and Python tests pass, and the existing cursor tests pass.

[^mysql-cursor]: `src/rs/vertebra-mysql/src/cursor.rs::bind_parameters`
