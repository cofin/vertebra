---
type: Task
id: "native-cursor-data-path:single:t21"
chapter_id: single-call-paths
title: "Spanner bind fast path and completed fetch awaitables"
description: "Check exact tuple, list, and dict types first in the Spanner cursor's parameter encoding, and return completed awaitables from the async Spanner cursor's fetch and close methods."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
read_set:
  - "src/rs/vertebra-spanner/src/cursor.rs:1-21"
  - "src/rs/vertebra-spanner/src/cursor.rs:240-248"
  - "src/rs/vertebra-spanner/src/cursor.rs:308-367"
  - "src/rs/vertebra-spanner/src/cursor.rs:436-441"
  - "src/rs/vertebra-spanner/src/cursor.rs:509-586"
  - "src/rs/vertebra-spanner/src/cursor.rs:946-963"
  - "src/rs/vertebra-spanner/src/cursor.rs:1130-1162"
  - "src/rs/vertebra-spanner/src/cursor.rs:1172-1200"
  - "src/py/tests/unit/db/test_spanner.py:3023-3040"
  - "Makefile:215-232"
files:
  - "src/rs/vertebra-spanner/src/cursor.rs"
tests:
  - "src/py/tests/unit/db/test_spanner.py"
estimated_loc: 120
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: spanner-cursor
    resource: src/rs/vertebra-spanner/src/cursor.rs::encode_one_param_set
    title: "Spanner cursor parameter encoding and the async cursor fetch methods"
tags: [spanner, pyo3, performance]
---

# t21 — Spanner bind fast path and completed fetch awaitables

## Objective & Scope Boundary

After this task, an async Spanner cursor returns rows that are already in memory without spawning a task. A tuple, list, or dict of parameters is recognized by its exact type before any other check.[^spanner-cursor]

In scope, all in `src/rs/vertebra-spanner/src/cursor.rs`:

- `encode_one_param_set`: exact-type checks first.
- `fetchone`, `fetchmany`, `fetchall`, and `close` on `PyAsyncSpannerCursor`: return the completed awaitable from t01.
- The private `ready_awaitable` function: deleted.
- Rust tests in the file's `mod tests` and Python tests in `test_spanner.py`.

Out of scope:

- The client methods in `src/rs/vertebra-spanner/src/lib.rs` and `rw_tx.rs`. They take `params_json: Option<&str>` and read no Python container.
- `parse_spanner_parameters_json` and the Python encoder in `src/py/vertebra/db/adapters/spanner/`.
- `encode_batch_dml_statements`. It calls `encode_one_param_set` per item and gains the fast path through it.
- `execute` and `executemany` on either cursor.
- `src/py/vertebra/_spanner.pyi`. The stub return types stay `Awaitable[T]`.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| `cursor.rs:1-21` | Imports to edit |
| `cursor.rs:240-248`, `308-367` | `ensure_open`, `fetchone`, `fetchmany`, `fetchall`, `close` on `CursorState` |
| `cursor.rs:436-441` | `NEED_ENCODER_MSG` |
| `cursor.rs:509-586` | `encode_one_param_set`, `encode_batch_dml_statements`, `parse_fetchmany_size` |
| `cursor.rs:946-963` | `ready_awaitable`, deleted here |
| `cursor.rs:1130-1162` | The four async methods to change |
| `cursor.rs:1172-1200` | Test module header and its `super::{...}` import list |
| `test_spanner.py:3023-3040` | Setup lines of `test_async_cursor_select_stale_dml_ddl_and_fetch_controls`, reused by the new test |
| `Makefile:215-232` | Interpreter exports that `cargo test` needs for a PyO3 crate |

Delivered by t01 and read from `src/rs/vertebra-db/src/py_async.rs`: `ReadyAwaitable`, and `ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>`. If either name is missing from the `py_async` module of `vertebra-db`, set the task to `blocked`.

## Contracts & Edge Cases

### Imports

- `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr};`
- `PyDict`, `PyList`, and `PyTuple` are already imported from the `types` module of `pyo3`.

### `encode_one_param_set`

The signature is unchanged. The cursor accepts three parameter forms: nothing, encoded JSON text, and a Python container that the `param_encoder` turns into JSON text. All three stay.

New private helper, placed directly above `encode_one_param_set`:

```rust
fn encode_with_encoder(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<Vec<SpannerQueryParameter>>
```

Its body is the current tail of `encode_one_param_set`, from the `let Some(enc) = encoder else` line to the final `param_encoder must return str or None` error, moved without change.

Order of checks:

1. `params` is absent or a Python `None`: `Ok(Vec::new())`, unchanged.
2. `obj.cast_exact::<PyTuple>()`: an empty tuple returns `Ok(Vec::new())`. Otherwise `encode_with_encoder(py, obj, encoder)`.
3. `obj.cast_exact::<PyList>()`: the same two cases.
4. `obj.cast_exact::<PyDict>()`: the same two cases.
5. `obj.cast::<PyString>()`: encoded JSON text, unchanged. Blank text returns `Ok(Vec::new())`.
6. `if let Ok(0) = obj.len()`: `Ok(Vec::new())`, unchanged. This covers empty subclass instances and other sized objects.
7. `encode_with_encoder(py, obj, encoder)`.

Where the exact-type check sits: after the `None` check and before the encoded-text check. A `str` is never an exact tuple, list, or dict, so encoded JSON text is handled as today. A non-empty exact container skips the `str` cast and the `len()` call and goes to the encoder.

Rules:

- The no-parameter value is `Vec::new()`. It does not allocate.
- An empty exact container never calls the encoder and never raises the missing-encoder error.
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
| Fetch on a cursor bound to a read-write transaction | Same as a client cursor. The rows are in the cursor state |

## Test Matrix

Rust, in `mod tests` of `cursor.rs`. Add `encode_one_param_set` to the `super::{...}` import list and `PyDict`, `PyList` to the import from the `types` module of `pyo3`:

| Test | Input | Expected |
| --- | --- | --- |
| `encode_one_param_set_empty_exact_containers_need_no_encoder` | `()`, `[]`, `{}` with `encoder = None` | `Ok` with an empty vector each |
| `encode_one_param_set_non_empty_exact_containers_call_encoder` | `(1,)`, `[1]`, `{"a": 1}` with an encoder that is the bound `append` of a Python list | `Ok` with an empty vector, and the list holds the three inputs in order |
| same test | `(1,)` with `encoder = None` | `InterfaceError` with `Spanner cursor needs encoded parameters or a param_encoder` |
| `encode_one_param_set_text_and_subclass_inputs` | the text `{}` | `Ok` with an empty vector, and the encoder list stays empty |
| same test | an empty `collections.OrderedDict` and a one-field `collections.namedtuple` instance | Empty vector with no encoder call for the first; one encoder call for the second |

`list.append` returns `None`, which the encoder contract treats as no parameters. That lets the test observe the call without building parameter JSON.

Python, in `src/py/tests/unit/db/test_spanner.py`, placed after `test_async_cursor_select_stale_dml_ddl_and_fetch_controls` and using the same `_in_process_spanner_wire_server()` and `AsyncSpannerClient` setup:

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
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner cursor
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-spanner --all-targets -- -D warnings
make dev
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_spanner.py -p no:xdist -q -k "cursor"
uv run --no-sync --frozen python -m mypy.stubtest vertebra._spanner
test -z "$(git grep -n 'fn ready_awaitable' -- src/rs/vertebra-spanner/src)"
```

The exports mirror the `rs-test` recipe. `make dev` rebuilds the extension before the Python tests.

## Acceptance Criteria

- [ ] `encode_one_param_set` tests `PyTuple`, `PyList`, and `PyDict` with `cast_exact` directly after the `None` check and before the encoded-text check.
- [ ] An empty exact container returns no parameters without an encoder call.
- [ ] Encoded JSON text and the `param_encoder` path behave as before.
- [ ] The four async methods return `PyResult<Py<ReadyAwaitable>>` and call no `spawn_rust_db_task_with_attr`.
- [ ] State errors from the three fetch methods are raised at the call.
- [ ] `ready_awaitable` is gone from the crate.
- [ ] `src/py/vertebra/_spanner.pyi` has no diff and `mypy.stubtest` passes.
- [ ] The new Rust and Python tests pass, and the existing cursor tests pass.

[^spanner-cursor]: `src/rs/vertebra-spanner/src/cursor.rs::encode_one_param_set`
