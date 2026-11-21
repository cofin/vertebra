---
type: Task
id: "native-cursor-data-path:single:t22"
chapter_id: single-call-paths
title: "BigQuery bind fast path and completed fetch awaitables"
description: "Check exact tuple, list, and dict types first in the BigQuery cursor's parameter encoding, and return completed awaitables from the async BigQuery cursor's fetch and close methods."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
read_set:
  - "src/rs/vertebra-bigquery/src/cursor.rs:1-22"
  - "src/rs/vertebra-bigquery/src/cursor.rs:128-135"
  - "src/rs/vertebra-bigquery/src/cursor.rs:189-249"
  - "src/rs/vertebra-bigquery/src/cursor.rs:344-420"
  - "src/rs/vertebra-bigquery/src/cursor.rs:721-738"
  - "src/rs/vertebra-bigquery/src/cursor.rs:909-941"
  - "src/rs/vertebra-bigquery/src/cursor.rs:951-961"
  - "src/py/tests/unit/db/test_bigquery.py:1954-1976"
  - "Makefile:215-232"
files:
  - "src/rs/vertebra-bigquery/src/cursor.rs"
tests:
  - "src/py/tests/unit/db/test_bigquery.py"
estimated_loc: 120
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: bigquery-cursor
    resource: src/rs/vertebra-bigquery/src/cursor.rs::encode_sql_and_params
    title: "BigQuery cursor parameter encoding and the async cursor fetch methods"
tags: [bigquery, pyo3, performance]
---

# t22 — BigQuery bind fast path and completed fetch awaitables

## Objective & Scope Boundary

After this task, an async BigQuery cursor returns rows that are already in memory without spawning a task. A tuple, list, or dict of parameters is recognized by its exact type before any other check.[^bigquery-cursor]

In scope, all in `src/rs/vertebra-bigquery/src/cursor.rs`:

- `encode_sql_and_params`: exact-type checks first.
- `fetchone`, `fetchmany`, `fetchall`, and `close` on `PyAsyncBigQueryCursor`: return the completed awaitable from t01.
- The private `ready_awaitable` function: deleted.
- Rust tests in the file's `mod tests` and Python tests in `test_bigquery.py`.

Out of scope:

- `extract_labels`. It reads job labels, not statement parameters.
- The client methods in `src/rs/vertebra-bigquery/src/lib.rs`. They take `params_json: Option<&str>` and read no Python container.
- `parse_query_parameters_json` and the Python encoder in `src/py/vertebra/db/adapters/bigquery/`.
- `encode_executemany_statements`. It calls `encode_sql_and_params` per item and gains the fast path through it.
- `execute` and `executemany` on either cursor.
- `src/py/vertebra/_bigquery.pyi`. The stub return types stay `Awaitable[T]`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `cursor.rs:1-22` | Imports to edit and `NEED_ENCODER_MSG` |
| `cursor.rs:128-135`, `189-249` | `ensure_open`, `fetchone`, `fetchmany`, `fetchall`, `close` on `CursorState` |
| `cursor.rs:344-420` | `encode_sql_and_params`, `encode_executemany_statements`, `parse_fetchmany_size` |
| `cursor.rs:721-738` | `ready_awaitable`, deleted here |
| `cursor.rs:909-941` | The four async methods to change |
| `cursor.rs:951-961` | Test module header and its `super::{...}` import list |
| `test_bigquery.py:1954-1976` | Setup lines of `test_async_cursor_select_dml_ddl_empty_and_metadata`, reused by the new test |
| `Makefile:215-232` | Interpreter exports that `cargo test` needs for a PyO3 crate |

Delivered by t01 and read from `src/rs/vertebra-db/src/py_async.rs`: `ReadyAwaitable`, and `ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>`. If either name is missing from the `py_async` module of `vertebra-db`, set the task to `blocked`.

## Contracts & Edge Cases

### Imports

- `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr};`
- `PyDict`, `PyList`, and `PyTuple` are already imported from the `types` module of `pyo3`.

### `encode_sql_and_params`

The signature is unchanged. The cursor accepts three parameter forms: nothing, encoded JSON text, and a Python container that the `param_encoder` turns into rewritten SQL and JSON text. All three stay.

New private helper, placed directly above `encode_sql_and_params`:

```rust
fn encode_with_encoder(
    py: Python<'_>,
    sql: &str,
    obj: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<(String, Vec<BigQueryQueryParameter>)>
```

Its body is the current tail of `encode_sql_and_params`, from the `let Some(enc) = encoder else` line to the final `Ok((rewritten_sql, parsed))`, moved without change.

Order of checks:

1. `params` is absent or a Python `None`: `Ok((sql.to_string(), Vec::new()))`, unchanged.
2. `obj.cast_exact::<PyTuple>()`: an empty tuple returns `Ok((sql.to_string(), Vec::new()))`. Otherwise `encode_with_encoder(py, sql, obj, encoder)`.
3. `obj.cast_exact::<PyList>()`: the same two cases.
4. `obj.cast_exact::<PyDict>()`: the same two cases.
5. `obj.cast::<PyString>()`: encoded JSON text, unchanged. Blank text returns the SQL with no parameters.
6. `if let Ok(0) = obj.len()`: the SQL with no parameters, unchanged. This covers empty subclass instances and other sized objects.
7. `encode_with_encoder(py, sql, obj, encoder)`.

Where the exact-type check sits: after the `None` check and before the encoded-text check. A `str` is never an exact tuple, list, or dict, so encoded JSON text is handled as today. A non-empty exact container skips the `str` cast and the `len()` call and goes to the encoder.

Rules:

- The no-parameter value is `(sql.to_string(), Vec::new())`, the same value the `None` branch returns. The parameter vector is not allocated.
- An empty exact container never calls the encoder and never raises the missing-encoder error. The SQL is not rewritten.
- A non-empty container with no encoder raises `InterfaceError` with `NEED_ENCODER_MSG`, as today.
- A subclass of `tuple`, `list`, or `dict` reaches steps 6 and 7 and behaves as today.

### Async fetch methods

When state errors surface: at the call, as today. `fetchone`, `fetchmany`, and `fetchall` run the `CursorState` method under the state lock and propagate its error with `?` before any awaitable exists. A closed cursor raises `InterfaceError` from the method call, not from the `await`. A fetch with no result set returns `None` or `[]`. This task keeps that rule.

```rust
fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
#[pyo3(signature = (size=None))]
fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>
fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>
```

- `fetchone`: `let row = self.state().fetchone(py)?;` then `ready_ok(py, row.map_or_else(|| py.None(), Py::into_any))`.
- `fetchmany`: keep the block with `ensure_open`, `parse_fetchmany_size`, and `state.fetchmany`. Return `ready_ok(py, PyList::new(py, rows)?.into_any().unbind())`.
- `fetchall`: `let rows = self.state().fetchall(py)?;` then `ready_ok` with the list.
- `close`: `self.state().close();` then `ready_ok(py, py.None())`.
- The state guard is a temporary or is dropped at the end of its block, so it is released before `ready_ok` runs.
- Delete `ready_awaitable`. No caller remains.

| Case | Behavior |
| --- | --- |
| `fetchall()` called outside a running event loop | Returns an awaitable. Today it raises because a task cannot be spawned |
| `close()` on a closed cursor | Completed awaitable with `None`. Idempotent |
| `fetchmany(-1)` | `ProgrammingError` at the call |
| `query_metadata` after a fetch | Unchanged. Fetch methods do not touch it |

## Test Matrix

Rust, in `mod tests` of `cursor.rs`. Add `encode_sql_and_params` to the `super::{...}` import list:

| Test | Input | Expected |
| --- | --- | --- |
| `cursor_encode_empty_exact_containers_need_no_encoder` | `()`, `[]`, `{}` with `SELECT 1` and `encoder = None` | `Ok(("SELECT 1", []))` each |
| `cursor_encode_non_empty_exact_containers_call_encoder` | `(1,)`, `[1]`, `{"a": 1}` with an encoder built by `py.eval(c"lambda sql, params: (sql + ' /*enc*/', None)", None, None)` | SQL ends with ` /*enc*/`, no parameters |
| same test | `(1,)` with `encoder = None` | `InterfaceError` with `BigQuery cursor needs encoded parameters or a param_encoder` |
| `cursor_encode_text_and_subclass_inputs` | the text `[]` with the same encoder | SQL unchanged, no parameters. The encoder is not called |
| same test | an empty `collections.OrderedDict` and a one-field `collections.namedtuple` instance | SQL unchanged for the first; SQL ends with ` /*enc*/` for the second |

Python, in `src/py/tests/unit/db/test_bigquery.py`, placed after `test_async_cursor_select_dml_ddl_empty_and_metadata` and using the same `_CursorBigQueryRestServer` and `AsyncBigQueryClient` setup:

| Test | Steps | Expected |
| --- | --- | --- |
| `test_async_cursor_fetch_returns_completed_awaitable` | `await cur.execute("SELECT 1 AS a, 'x' AS b")` and `expected = await cur.fetchall()`. Run the same `execute` again, then `step = cur.fetchall().__await__()` and `next(step)` | `StopIteration` on the first `next`, with `value == expected` and `expected` not empty. The loop does not iterate |
| `test_async_cursor_ready_methods_complete_without_loop`, `@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "close"])` | After an `execute`, call the method and drive `__await__` with one `next` | `StopIteration` on the first `next` |
| `test_async_cursor_state_errors_raise_at_call` | `await cur.close()`, then `cur.fetchone()` not awaited | `InterfaceError` raised by the call |
| `test_async_cursor_empty_container_matches_no_parameters`, `@pytest.mark.parametrize("params", [(), [], {}])` | A client cursor created with no `param_encoder`; `await cur.execute("SELECT 1 AS a, 'x' AS b", params)` | No error, and `await cur.fetchall()` equals the result with no parameters |

Each extension compiles its own copy of the awaitable class. Tests assert behavior and never use `isinstance` against a class from another module.

## Verification

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export PYO3_PYTHON="$(realpath .venv/bin/python)"
export PYTHONHOME="$("$PYO3_PYTHON" -c 'import sys; print(sys.base_prefix)')"
export LD_LIBRARY_PATH="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$PWD/src/py:$("$PYO3_PYTHON" -c 'import os, site; print(os.pathsep.join(site.getsitepackages()))')"
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-bigquery cursor
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-bigquery --all-targets -- -D warnings
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_bigquery.py -p no:xdist -q -k "cursor"
uv run --no-sync --frozen python -m mypy.stubtest vertebra._bigquery
test -z "$(git grep -n 'fn ready_awaitable' -- src/rs/vertebra-bigquery/src)"
```

The exports mirror the `rs-test` recipe. `make dev` rebuilds the extension before the Python tests.

## Acceptance Criteria

- [ ] `encode_sql_and_params` tests `PyTuple`, `PyList`, and `PyDict` with `cast_exact` directly after the `None` check and before the encoded-text check.
- [ ] An empty exact container returns the SQL unchanged with no parameters and no encoder call.
- [ ] Encoded JSON text and the `param_encoder` path behave as before.
- [ ] The four async methods return `PyResult<Py<ReadyAwaitable>>` and call no `spawn_rust_db_task_with_attr`.
- [ ] State errors from the three fetch methods are raised at the call.
- [ ] `ready_awaitable` is gone from the crate.
- [ ] `src/py/vertebra/_bigquery.pyi` has no diff and `mypy.stubtest` passes.
- [ ] The new Rust and Python tests pass, and the existing cursor tests pass.

[^bigquery-cursor]: `src/rs/vertebra-bigquery/src/cursor.rs::encode_sql_and_params`
