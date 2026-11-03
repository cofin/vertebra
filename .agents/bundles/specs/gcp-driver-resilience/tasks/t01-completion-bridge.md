---
type: Task
id: gcp-driver-resilience:t01
title: Panic-safe, cancellable completion bridge
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/rs/vertebra-py/src/completion.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-py/src/completion.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: executor
claimed_at: 2026-11-03T20:09:10Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T20:13:00Z
last_verified_commit: f6a57aa
verification_evidence: "cd src/rs && cargo test -p vertebra-py completion::tests && cargo clippy -p vertebra-py --all-targets -- -D warnings && cargo fmt --check && cd ../.. && make dev && uv run python -m mypy.stubtest vertebra._vertebra && uv run pytest src/py/tests/unit/db -q"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T20:13:00Z
commit: null
tags: [rust, pyo3, asyncio, bugfix]
---

# t01 — Panic-safe, cancellable completion bridge

## Objective

`spawn_rust_db_task` resolves the Python future with `TaskPanicError` when the Rust future or `map_ok` panics, aborts the Tokio task when the Python future is cancelled, and `_vertebra.pyi` stops declaring a constructor for `RustDbTaskAwaitable`.

## Context

- Spec R1, R2 (`../spec.md`).
- `regression_tdd`: today a panic leaves the future pending forever and cancellation does not stop the Rust work.
- Current design (`src/rs/vertebra-py/src/completion.rs`): `db_runtime_handle().spawn(async move { let output = fut.await; ring.completion.push(CompletedDbTask { future, produce }) ... })`; `DbCompletionDrainer::__call__` runs on the event loop, skips cancelled futures, and calls `(item.produce)(py)`.
- PyO3 0.30: `pyo3::create_exception!(module, Name, Base, "doc")`; `PyCFunction::new_closure`; the crate's dev-dependency enables `auto-initialize`, so inline tests can use `Python::attach`. Never call `Python::attach` inside a Tokio task.
- `tokio::task::JoinHandle::abort_handle()`, `JoinError::{is_panic, into_panic}`; `AbortHandle` is `Send + Sync`.
- Release profile is `panic = "abort"`; unwinding (and therefore `TaskPanicError`) applies to `make dev` and test builds. State this in the module doc.

## Steps

1. **Write failing tests (RED)** — add at the bottom of `src/rs/vertebra-py/src/completion.rs`:

   ```rust
   #[cfg(test)]
   mod tests {
       use std::sync::Arc;
       use std::sync::atomic::{AtomicBool, Ordering};
       use std::time::{Duration, Instant};

       use pyo3::prelude::*;
       use pyo3::types::{PyCFunction, PyDict, PyTuple};

       use super::spawn_rust_db_task;

       fn run_coroutine(py: Python<'_>, spawn: Bound<'_, PyCFunction>, body: &str) -> PyResult<Bound<'_, PyAny>> {
           let globals = PyDict::new(py);
           globals.set_item("spawn", spawn)?;
           py.run(&std::ffi::CString::new(format!("import asyncio\n{body}")).unwrap(), Some(&globals), None)?;
           globals.get_item("result")?.ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("result"))
       }

       #[test]
       fn panicking_task_raises_task_panic_error() {
           Python::attach(|py| {
               let spawn = PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                   spawn_rust_db_task(
                       args.py(),
                       async { if true { panic!("boom") } Ok::<i64, String>(1) },
                       |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                       pyo3::exceptions::PyRuntimeError::new_err,
                   )
               }).unwrap();
               let result = run_coroutine(py, spawn, concat!(
                   "async def main():\n",
                   "    try:\n        await spawn()\n    except Exception as exc:\n        return (type(exc).__name__, str(exc))\n",
                   "result = asyncio.run(main())\n",
               )).unwrap();
               let (name, message): (String, String) = result.extract().unwrap();
               assert_eq!(name, "TaskPanicError");
               assert_eq!(message, "Rust task panicked: boom");
           });
       }
   }
   ```

   Add `panicking_map_ok_raises_task_panic_error` (same shape; the future returns `Ok(1)` and `map_ok` panics with `"map boom"`; expected message `"Rust task panicked: map boom"`) and `cancelled_await_aborts_rust_task`: the future holds a guard whose `Drop` sets `dropped: Arc<AtomicBool>`, sleeps 5 s with `tokio::time::sleep`, then sets `completed: Arc<AtomicBool>`; the Python body is `async def main():\n    task = asyncio.ensure_future(spawn())\n    await asyncio.sleep(0.05)\n    task.cancel()\n    try:\n        await task\n    except asyncio.CancelledError:\n        pass\n    return True\nresult = asyncio.run(main())\n`; after `run_coroutine`, poll `dropped` for up to 1 s (`Instant::now()` + 10 ms sleeps, outside `Python::attach`'s GIL using `py.detach`) and assert `dropped == true` and `completed == false`.
   - RED: `cd src/rs && cargo test -p vertebra-py completion::tests` → the panic tests hang or fail (no `TaskPanicError`), the cancel test fails (`dropped == false`). Use `timeout 120` on the command if the panic test hangs: `cd src/rs && timeout 120 cargo test -p vertebra-py completion::tests`.

2. **Implement (GREEN) in `completion.rs`.**
   - `pyo3::create_exception!(vertebra._vertebra, TaskPanicError, pyo3::exceptions::PyRuntimeError, "Raised when a Rust background task panics before producing a result.");`
   - `fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String`: `&str` / `String` payloads → `format!("Rust task panicked: {text}")`; otherwise `"Rust task panicked".to_string()`.
   - `#[pyclass(name = "_DbTaskCanceller", frozen, immutable_type)] struct DbTaskCanceller { abort: tokio::task::AbortHandle }` with `fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()>`: when `future.call_method0("cancelled")?.extract::<bool>()?` is true call `self.abort.abort()`.
   - `spawn_rust_db_task`: `let worker = db_runtime_handle().spawn(fut);` then `py_fut.call_method1("add_done_callback", (Py::new(py, DbTaskCanceller { abort: worker.abort_handle() })?,))?;` then `db_runtime_handle().spawn(async move { let produce: DbOutcomeProducer = match worker.await { Ok(Ok(value)) => Box::new(move |py| map_ok(py, value)), Ok(Err(err)) => Box::new(move |_| Err(map_err(err))), Err(join) if join.is_panic() => { let message = panic_message(join.into_panic()); Box::new(move |_| Err(TaskPanicError::new_err(message))) } Err(_) => Box::new(|_| Err(pyo3::exceptions::asyncio::CancelledError::new_err("Rust task was cancelled"))) }; push to ring and signal as today });`.
   - `DbCompletionDrainer::__call__`: run the producer as `std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (item.produce)(py)))`; `Err(payload)` → `set_exception(TaskPanicError::new_err(panic_message(payload)))`.
   - `register`: also `m.add("TaskPanicError", m.py().get_type::<TaskPanicError>())?`.
   - Module doc: add the panic/cancel contract and the `panic = "abort"` release note.

3. **Stub (`src/py/vertebra/_vertebra.pyi`).** Remove `def __new__(cls) -> RustDbTaskAwaitable: ...` from `RustDbTaskAwaitable`; add `class TaskPanicError(RuntimeError): ...` next to it and `"TaskPanicError"` to the stub's `__all__` in alphabetical position.

4. **Refactor & verify.**

## Verification

- RED: `cd src/rs && timeout 120 cargo test -p vertebra-py completion::tests` → failures/timeouts as described.
- GREEN:
  - `cd src/rs && cargo test -p vertebra-py completion::tests` → 3 passed.
  - `cd src/rs && cargo clippy -p vertebra-py --all-targets -- -D warnings && cargo fmt --check` → exit 0.
  - `make dev && uv run python -m mypy.stubtest vertebra._vertebra` → exit 0.
  - `uv run pytest src/py/tests/unit/db -q` → pass.
- Commit: `git commit -S -m "fix(rs-py): resolve panicking DB tasks with TaskPanicError and abort cancelled tasks"`.

## Acceptance Criteria

- [x] R1: future and `map_ok` panics raise `TaskPanicError` with the panic message.
- [x] R2: cancellation drops the Rust future within 1 s; `RustDbTaskAwaitable` has no stub constructor.

## Notes & Discoveries

- Verified RED failure on `cancelled_await_aborts_rust_task`, implemented `TaskPanicError`, `DbTaskCanceller`, and `catch_unwind` around `(item.produce)(py)`, and verified 3 passed in `completion::tests`, `mypy.stubtest`, and `pytest src/py/tests/unit/db`.
