---
type: Task
id: "native-cursor-data-path:single:t01"
chapter_id: single-call-paths
title: "Completed awaitable in vertebra-core"
description: "Add ReadyAwaitable with ready_ok and ready_err to vertebra-core, re-export it for the extension crates, register it on vertebra._vertebra, and stub it."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/rs/vertebra-core/src/py_async.rs:1-35"
  - "src/rs/vertebra-core/src/py_async.rs:340-394"
  - "src/rs/vertebra-core/src/py_async.rs:570-652"
  - "src/rs/vertebra-db/src/py_async.rs:1-21"
  - "src/rs/vertebra-py/src/async_bridge.rs:1-53"
  - "src/py/vertebra/_vertebra.pyi:66-80"
  - "src/py/vertebra/_vertebra.pyi:1780-1783"
  - "src/py/tests/unit/ffi/test_ffi.py:20-40"
  - "src/py/tests/unit/ffi/test_ffi.py:300-306"
  - "src/py/tests/unit/ffi/test_ffi.py:540-567"
files:
  - src/rs/vertebra-core/src/py_async.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/async_bridge.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/src/py_async.rs
  - src/py/tests/unit/ffi/test_ffi.py
estimated_loc: 170
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: async-bridge
    resource: src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable
    title: "Awaitable backed by a Tokio task and a loop completion queue"
tags: [pyo3, asyncio, vertebra-core, free-threaded]
---

# Completed awaitable in vertebra-core

## Objective & Scope Boundary

A Rust method that already has its result can hand Python an awaitable that is complete. Awaiting it returns the value, or raises the stored error, without a task, a future, or a turn of the event loop.

This task adds the class and its two constructors, the re-export that extension crates use, the registration on `vertebra._vertebra`, and the stub. It changes no existing awaitable and no caller. The tasks that return it from cursor fetch methods come later.

## Bounded Read Manifest

- `vertebra-core/src/py_async.rs:1-35` — module doc and imports. Each extension compiles its own copy of every class in this module.
- `vertebra-core/src/py_async.rs:340-394` — `RustTaskAwaitable`: the `#[pyclass]` attributes to mirror.
- `vertebra-core/src/py_async.rs:570-652` — test module imports and the embedded-script test style.
- `vertebra-db/src/py_async.rs:1-21` — the `pub use` list that extension crates import from.
- `vertebra-py/src/async_bridge.rs:1-53` — the `pub use` list and `register`.
- `_vertebra.pyi:66-80` — `__all__` around the new entry. `_vertebra.pyi:1780-1783` — the `RustDbTaskAwaitable` stub.
- `test_ffi.py:20-40`, `300-306`, `540-567` — the import block, `test_async_bridge_exports_preserved`, and the instantiation test.

## Symbol & Signature Contracts

In `src/rs/vertebra-core/src/py_async.rs`, placed directly after the `Drop` impl of `RustTaskAwaitable`:

```rust
enum ReadyOutcome {
    Value(Py<PyAny>),
    Error(PyErr),
}

/// Awaitable that is already complete with a value or an error.
#[pyclass(
    name = "ReadyAwaitable",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct ReadyAwaitable {
    outcome: ReadyOutcome,
}

#[pymethods]
impl ReadyAwaitable {
    fn __await__(slf: Bound<'_, Self>) -> Bound<'_, Self>;
    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self>;
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

/// Return an awaitable that completes with `value`.
pub fn ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>;

/// Return an awaitable that raises `err` when awaited.
pub fn ready_err(py: Python<'_>, err: PyErr) -> PyResult<Py<ReadyAwaitable>>;
```

- The class has no `#[new]`. Python cannot construct it.
- `__await__` and `__iter__` return the same object.
- `__next__` never returns `Ok`. For `Value(v)` it returns `Err(PyStopIteration::new_err((v.clone_ref(py),)))`. The value is wrapped in a one-item tuple so a tuple value is not unpacked into the exception arguments. For `Error(e)` it returns `Err(e.clone_ref(py))`.
- `ready_ok` and `ready_err` allocate the object with the `new` constructor of `Py`.
- Add `use pyo3::exceptions::PyStopIteration;` to the imports, next to the existing `PyRuntimeError` import from `pyo3`.

In `src/rs/vertebra-db/src/py_async.rs`, add `ReadyAwaitable`, `ready_err`, and `ready_ok` to the existing `pub use vertebra_core::py_async::{...}` list. They keep their names. The module doc gains one sentence: extension crates import the completed awaitable from here.

In `src/rs/vertebra-py/src/async_bridge.rs`, add the same three names to the `pub use` list, and add to `register`:

```rust
m.add("ReadyAwaitable", m.py().get_type::<ReadyAwaitable>())?;
```

How extension crates reach the helpers: all eight SQL extension crates depend on `vertebra-db` with the `pyo3` feature. They write `use vertebra_db::py_async::{ReadyAwaitable, ready_ok, ready_err};`. `vertebra-storage` depends on `vertebra-core` only and would import from the `py_async` module of `vertebra_core`; no task in this spec needs that.

Stub, in `src/py/vertebra/_vertebra.pyi`. Add `"ReadyAwaitable"` to `__all__` between `"RateLimiter"` and `"Request"`. Add the class directly before `RustDbTaskAwaitable`:

```python
@final
class ReadyAwaitable[T]:
    """Awaitable that is already complete with a value or an error."""

    def __await__(self) -> Generator[object, None, T]: ...
    def __iter__(self) -> Self: ...
    def __next__(self) -> object: ...
```

`Generator`, `Self`, and `final` are already imported in the stub.

## Edge Cases & Error Semantics

- Awaiting does not suspend the coroutine. A callback scheduled with `loop.call_soon` before the await has not run when the await returns.
- The object can be awaited again. Each await returns the same value object or raises the same error.
- The object holds no lock and no mutable field. It is `frozen`, and `Py<PyAny>` and `PyErr` are `Send` and `Sync`, so two threads may await one object on a free-threaded build.
- It needs no running event loop. `ready_ok` works when called outside a loop.
- `asyncio.create_task` rejects it, as it rejects `RustTaskAwaitable`, because it is not a coroutine. `asyncio.ensure_future` and `asyncio.wait_for` accept it.
- A stored `StopIteration` error would read as a normal completion. No caller passes one. `ready_err` does not check for it.
- Each extension `.so` builds its own type object for this class. `isinstance(x, vertebra._vertebra.ReadyAwaitable)` is true only for objects created inside `vertebra._vertebra`. Tests elsewhere check behavior or `type(x).__name__`.
- Dropping it without awaiting does nothing. There is no task to cancel.

## Public-Seam Test Blueprint

Rust tests, added to the existing `tests` module of `vertebra-core/src/py_async.rs`. Each test wraps `ready_ok` or `ready_err` in a `PyCFunction` closure, puts it in a globals dict, and runs a script with `asyncio.run`, as `ok_and_err_resolution_work_across_event_loop` does.

| Test | Script | Expected |
| --- | --- | --- |
| `ready_ok_returns_value_without_loop_iteration` | `ran = []`; `loop.call_soon(ran.append, 1)`; `val = await make_ok()`; record `list(ran)`; `await asyncio.sleep(0)`; record `list(ran)` | value `123`; `[]` right after the await; `[1]` after the sleep |
| `ready_err_raises_stored_error_without_loop_iteration` | same shape with `make_err()` inside `try/except ValueError` | message `boom`; `[]` right after the await |
| `ready_ok_keeps_tuple_value_intact` | `await make_tuple()` where the value is `(1, 2)` | `(1, 2)`, type `tuple` |
| `ready_awaitable_can_be_awaited_twice` | `obj = make_ok()`; `a = await obj`; `b = await obj` | both `123` |
| `ready_ok_needs_no_running_loop` | call `make_ok()` at module level, then `asyncio.run` a coroutine that awaits it | `123` |
| `ready_awaitable_works_with_wait_for` | `await asyncio.wait_for(make_ok(), 1)` | `123` |

The module has no free-threaded test today, so none is added. The class is covered on free-threaded builds by the same tests when the workspace runs under a free-threaded interpreter.

Python tests, in `src/py/tests/unit/ffi/test_ffi.py`:

| Test | Steps | Expected |
| --- | --- | --- |
| `test_async_bridge_exports_preserved` (extended) | import `ReadyAwaitable` from `vertebra._vertebra` | `hasattr` is true for `__await__`, `__iter__`, and `__next__` |
| `test_internal_native_classes_reject_direct_instantiation` (one more parameter) | `ReadyAwaitable()` | `TypeError` |

Add `ReadyAwaitable` to the file's existing `from vertebra._vertebra import (...)` block in sorted position. Behavior through a real extension method is tested in t05 and t08.

## Verification

- Exports for raw `cargo test`: copy the `PYO3_PYTHON`, `PYTHONHOME`, `PYTHONPATH`, and `LD_LIBRARY_PATH` exports from the `rs-test` recipe in `Makefile`, with `PYO3_PYTHON` set to the project virtual environment interpreter.
- Red then green: `(cd src/rs && cargo test -p vertebra-core --features pyo3 --lib py_async::tests::ready --locked)`
- Whole module: `(cd src/rs && cargo test -p vertebra-core --features pyo3 --lib py_async --locked)`
- Dependents still build: `(cd src/rs && cargo check -p vertebra-db --features pyo3 --locked && cargo check -p vertebra-py --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-core -p vertebra-db -p vertebra-py && cargo clippy -p vertebra-core --features pyo3 --all-targets --locked -- -D warnings)`
- Rebuild the root extension: `make dev`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._vertebra`
- Python: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/ffi/test_ffi.py -q`

## Acceptance Criteria

- [ ] The six Rust tests and the two Python test changes pass.
- [ ] The `py_async` modules of `vertebra_core` and `vertebra_db` both export `ReadyAwaitable`, `ready_ok`, and `ready_err`.
- [ ] `vertebra._vertebra.ReadyAwaitable` exists and cannot be constructed from Python.
- [ ] `mypy.stubtest vertebra._vertebra` passes with no allow-list change.
- [ ] No existing test in `py_async.rs` or `test_ffi.py` changes its assertions.

## Notes & Discoveries

- The class is its own iterator so that one `#[pyclass]` covers `__await__`, `__iter__`, and `__next__`.
