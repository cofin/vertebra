---
type: Task
id: "native-cursor-data-path:single:t20"
chapter_id: single-call-paths
title: "SQL Server bind fast path and completed fetch awaitables"
description: "Check exact tuple, list, and dict types first in the SQL Server bind code, and return completed awaitables from the async SQL Server cursor's fetch and close methods."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:cleanup:t18"
read_set:
  - "src/rs/vertebra-mssql/src/cursor.rs:1-20"
  - "src/rs/vertebra-mssql/src/cursor.rs:100-158"
  - "src/rs/vertebra-mssql/src/cursor.rs:200-296"
  - "src/rs/vertebra-mssql/src/cursor.rs:382-397"
  - "src/rs/vertebra-mssql/src/cursor.rs:928-1016"
  - "src/rs/vertebra-mssql/src/cursor.rs:1026-1041"
  - "src/rs/vertebra-mssql/src/cursor.rs:1234-1249"
  - "src/rs/vertebra-mssql/src/params.rs:345-420"
  - "src/rs/vertebra-mssql/src/params.rs:529-552"
  - "src/py/tests/unit/db/test_mssql_driver.py:1568-1592"
  - "Makefile:215-232"
files:
  - "src/rs/vertebra-mssql/src/cursor.rs"
  - "src/rs/vertebra-mssql/src/params.rs"
tests:
  - "src/py/tests/unit/db/test_mssql_driver.py"
estimated_loc: 200
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: mssql-cursor
    resource: src/rs/vertebra-mssql/src/cursor.rs::bind_positional
    title: "SQL Server positional binding and the async cursor fetch methods"
  - id: mssql-params
    resource: src/rs/vertebra-mssql/src/params.rs::prepare_sql_and_params
    title: "SQL Server parameter extraction used by the client methods"
tags: [mssql, pyo3, performance]
---

# t20 — SQL Server bind fast path and completed fetch awaitables

## Objective & Scope Boundary

After this task, an async SQL Server cursor returns rows that are already in memory without spawning a task. A tuple or list of parameters is read directly, with no sequence protocol check.[^mssql-cursor][^mssql-params]

In scope:

- `src/rs/vertebra-mssql/src/cursor.rs`: `bind_positional`, `prepare_cursor_execute`, and `fetchone`, `fetchmany`, `fetchall`, and `close` on `PyAsyncMSSQLCursor`.
- `src/rs/vertebra-mssql/src/params.rs`: `prepare_sql_and_params`.
- Rust tests in both files' `mod tests`, and Python tests in `test_mssql_driver.py`.

Out of scope:

- `py_any_to_mssql_param`, `rewrite_named_mssql_placeholders`, `rewrite_plan_for`, and `prepare_many_sql_and_params`. The last one calls `prepare_sql_and_params` per item and gains the fast path through it.
- `execute` and `executemany` on either cursor.
- `src/py/vertebra/_mssql.pyi`. The stub return types stay `Awaitable[T]`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `cursor.rs:1-20` | Imports to edit |
| `cursor.rs:100-158` | `bind_positional` |
| `cursor.rs:200-296` | `ensure_open`, `fetchone`, `fetchmany`, `fetchall`, `close` on `CursorState` |
| `cursor.rs:382-397` | `prepare_cursor_execute`, which picks the positional or the named path |
| `cursor.rs:928-1016` | The four async methods to change |
| `cursor.rs:1026-1041`, `1234-1249` | Test module header and the existing bind test |
| `params.rs:345-420` | `prepare_sql_and_params` and `prepare_many_sql_and_params` |
| `params.rs:529-552` | Test module of `params.rs` |
| `test_mssql_driver.py:1568-1592` | Setup lines of `test_async_cursor_matrix_and_concurrent_guard`, reused by the new test |
| `Makefile:215-232` | Interpreter exports that `cargo test` needs for a PyO3 crate |

Delivered by t01 and read from `src/rs/vertebra-db/src/py_async.rs`: `ReadyAwaitable`, `ready_ok(py, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>`, and `ready_err(py, err: PyErr) -> PyResult<Py<ReadyAwaitable>>`. If a name is missing from the `py_async` module of `vertebra-db`, set the task to `blocked`.

## Contracts & Edge Cases

### Imports in `cursor.rs`

- `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, ready_err, ready_ok, spawn_rust_db_task_with_attr};`
- `PySequence` stays imported. The fallback uses it.

### `bind_positional` in `cursor.rs`

The signature is unchanged. New private helper, placed directly above it:

```rust
fn bind_exact_items<'py>(
    plan: Arc<RewritePlan>,
    len: usize,
    items: impl Iterator<Item = Bound<'py, PyAny>>,
) -> Result<(Arc<RewritePlan>, Vec<MssqlParam>), MssqlError>
```

- When `len != plan.param_count`, it returns the existing count-mismatch error with the existing message text and `got {len}`.
- Otherwise it converts each item with `py_any_to_mssql_param` into `Vec::with_capacity(len)` and returns `(plan, out)`.

Order of checks in `bind_positional` after `rewrite_plan_for(sql)`:

1. `params.is_none()`: unchanged.
2. `params.cast_exact::<PyTuple>()`: `bind_exact_items(plan, tuple.len(), tuple.iter())`.
3. `params.cast_exact::<PyList>()`: `bind_exact_items(plan, list.len(), list.iter())`.
4. Fallback, unchanged: reject `dict`, `str`, and `bytes`; cast to `PySequence`; check the count; read items by index.

An empty exact tuple or list gives the result of the `None` branch: the cached plan and `Vec::new()` when the statement has no placeholder, and the count-mismatch error with `got 0` when it has one. `Vec::with_capacity(0)` does not allocate.

### `prepare_cursor_execute` in `cursor.rs`

The match becomes, in this order:

1. `None` and a Python `None`: `bind_positional` with `None`, unchanged.
2. `Some(p)` where `p.is_exact_instance_of::<PyTuple>() || p.is_exact_instance_of::<PyList>()`: `bind_positional(py, sql, p)`.
3. `Some(p)` where `p.cast::<PyDict>()` succeeds: when the dict is empty and `p.is_exact_instance_of::<PyDict>()`, take the `None` branch. Otherwise `prepare_sql_and_params(sql, Some(p))`.
4. Any other `Some(p)`: `bind_positional(py, sql, p)`.

Every arm keeps the `.map(|(plan, bound)| (plan.sql.to_string(), bound))` mapping it has today.

### `prepare_sql_and_params` in `params.rs`

The signature is unchanged. Add `PyList` and `PyTuple` to the import from the `types` module of `pyo3`. Two private helpers hold the bodies that exist today:

```rust
fn positional_params<'py>(
    sql: &str,
    len: usize,
    items: impl Iterator<Item = Bound<'py, PyAny>>,
) -> Result<(String, Vec<MssqlParam>), MssqlError>

fn named_params(sql: &str, dict: &Bound<'_, PyDict>) -> Result<(String, Vec<MssqlParam>), MssqlError>
```

- `positional_params` converts each item with `py_any_to_mssql_param` and returns `(rewrite_mssql_placeholders(sql).0, out)`.
- `named_params` is the current dict branch, moved without change.

Order of checks after the two `None` returns:

1. `params_obj.cast_exact::<PyTuple>()`: an empty tuple returns what the `None` branch returns. Otherwise `positional_params(sql, tuple.len(), tuple.iter())`.
2. `params_obj.cast_exact::<PyList>()`: the same two cases.
3. `params_obj.cast_exact::<PyDict>()`: an empty dict returns what the `None` branch returns. Otherwise `named_params(sql, dict)`.
4. Fallback, in the order the function uses now: `cast::<PyDict>()` calls `named_params`; `cast::<PySequence>()` keeps the index loop; then the existing error.

The no-parameter value in this function is `(rewrite_mssql_placeholders(sql).0, Vec::new())`. The parameter vector is not allocated.

### Async fetch methods

When state errors surface today, read from `cursor.rs:928-1016`:

- `fetchone` and `fetchall`: on `await`. The call returns an awaitable, and a closed cursor raises `InterfaceError` when it is awaited.
- `fetchmany`: a closed cursor and a negative size raise at the call. Nothing else fails.
- `close`: never raises.

This task keeps those rules. What changes is when the state is read: at the call, not when the event loop runs the completion.

```rust
fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
#[pyo3(signature = (size = None))]
fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>
fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
```

- `fetchone`: lock the state and call `state.fetchone(py)`. `Ok(Some(tuple))` returns `ready_ok(py, tuple.into_any())`. `Ok(None)` returns `ready_ok(py, py.None())`. `Err(err)` returns `ready_err(py, err)`.
- `fetchmany`: under one lock, `ensure_open(py)?`, then the negative-size check that raises `ProgrammingError` with `fetchmany size must be non-negative`, then `state.fetchmany(py, wanted)`. `Ok(rows)` returns `ready_ok(py, PyList::new(py, rows)?.into_any().unbind())`. `Err(err)` returns `ready_err(py, err)`.
- `fetchall`: lock and call `state.fetchall(py)`. `Ok(rows)` returns `ready_ok` with the list. `Err(err)` returns `ready_err(py, err)`.
- `close`: lock, call `state.close()`, return `ready_ok(py, py.None())`.
- The lock guard is dropped before `ready_ok` or `ready_err` is called.

| Case | Behavior |
| --- | --- |
| `cur.close()` called and not awaited | The cursor is closed. Today it closes only when the loop runs the completion |
| `pending = cur.fetchall()`, then a new `execute` completes, then `await pending` | `pending` holds the rows from before the new `execute` |
| `fetchone()` on a closed cursor | The call returns an awaitable. `InterfaceError` on `await` |
| `fetchone()` before any `execute` | Completed awaitable with `None`, as today |
| `fetchmany(-1)` | `ProgrammingError` at the call |
| A fetch called outside a running event loop | Returns an awaitable. Today it raises because a task cannot be spawned |

## Test Matrix

Rust, in `mod tests` of `cursor.rs`:

| Test | Input | Expected |
| --- | --- | --- |
| `bind_positional_exact_tuple_and_list_match_sequence_path` | `(1, 2)` and `[1, 2]` with `SELECT ? + ?` | Plan SQL `SELECT @P1 + @P2`, two values |
| same test | `()` and `[]` with `SELECT 1` | No values, no error |
| same test | `()` with `SELECT ?` | the `Programming` kind of `MssqlErrorKind`, message ends with `got 0` |
| same test | a `collections.namedtuple` instance with two fields | Same result as the plain tuple |

Rust, in `mod tests` of `params.rs`:

| Test | Input | Expected |
| --- | --- | --- |
| `prepare_sql_and_params_exact_containers_and_empty_inputs` | `()`, `[]`, `{}` with `SELECT 1` | `("SELECT 1", [])` each, equal to the `None` result |
| same test | `(1,)` with `SELECT ?` | `SELECT @P1`, one value |
| same test | `{"a": 1}` with `SELECT @a` | Named rewrite to `@P1`, one value |
| same test | a `collections.OrderedDict` with key `a` | Same result as the plain dict |

Python, in `src/py/tests/unit/db/test_mssql_driver.py`, placed after `test_async_cursor_matrix_and_concurrent_guard` and using the same `_LoopbackTdsServer` and `mssql_native.AsyncMSSQLClient(server.dsn)` setup inside an `async def _run()` body:

| Test | Steps | Expected |
| --- | --- | --- |
| `test_async_cursor_fetch_returns_completed_awaitable` | `await cur.execute("SELECT 1 AS a, N'x' AS b")`, then `step = cur.fetchall().__await__()` and `next(step)` | `StopIteration` on the first `next`, with `value == [(1, "x")]`. The loop does not iterate |
| `test_async_cursor_close_takes_effect_at_call` | `cur.close()` without `await`, then `pending = cur.fetchone()` | `pending` is returned without an error, and `await pending` raises `InterfaceError` |
| `test_async_cursor_fetchmany_errors_raise_at_call` | `cur.fetchmany(-1)` on an open cursor, not awaited | `ProgrammingError` raised by the call |

Each extension compiles its own copy of the awaitable class. Tests assert behavior and never use `isinstance` against a class from another module.

## Verification

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export PYO3_PYTHON="$(realpath .venv/bin/python)"
export PYTHONHOME="$("$PYO3_PYTHON" -c 'import sys; print(sys.base_prefix)')"
export LD_LIBRARY_PATH="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$PWD/src/py:$("$PYO3_PYTHON" -c 'import os, site; print(os.pathsep.join(site.getsitepackages()))')"
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql cursor
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql params
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets -- -D warnings
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_mssql_driver.py -p no:xdist -q -k "cursor"
uv run --no-sync --frozen python -m mypy.stubtest vertebra._mssql
```

The exports mirror the `rs-test` recipe. `make dev` rebuilds the extension before the Python tests.

## Acceptance Criteria

- [ ] `bind_positional` and `prepare_sql_and_params` test `PyTuple` and `PyList` with `cast_exact` before any `PySequence` cast.
- [ ] An empty exact tuple, list, or dict gives the same result as `None` in both functions and in `prepare_cursor_execute`.
- [ ] The four async methods return `PyResult<Py<ReadyAwaitable>>` and call no `spawn_rust_db_task_with_attr`.
- [ ] `fetchone` and `fetchall` state errors surface on `await`. `fetchmany` closed-cursor and negative-size errors surface at the call.
- [ ] `src/py/vertebra/_mssql.pyi` has no diff and `mypy.stubtest` passes.
- [ ] The new Rust and Python tests pass, and the existing cursor tests pass.

[^mssql-cursor]: `src/rs/vertebra-mssql/src/cursor.rs::bind_positional`
[^mssql-params]: `src/rs/vertebra-mssql/src/params.rs::prepare_sql_and_params`
