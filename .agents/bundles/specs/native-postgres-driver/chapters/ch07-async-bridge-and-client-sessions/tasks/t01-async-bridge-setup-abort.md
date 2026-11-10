---
type: Task
id: "native-postgres-driver:ch07:t01"
chapter_id: ch07-async-bridge-and-client-sessions
title: Abort shared database workers when Python completion setup fails
description: Replace manual abort branches in spawn_rust_db_task_with_attr with a single armed RAII abort-on-drop worker guard that owns the JoinHandle until the completion monitor joins.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch06:t04"
read_set:
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-core/src/notify.rs
  - .agents/skills/pyo3-bridge/SKILL.md
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-db/src/py_async.rs
tests:
  - src/rs/vertebra-db/src/py_async.rs
estimated_loc: 150
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared database task spawner, DbTaskCanceller, and completion monitor
  - id: core-notify
    resource: src/rs/vertebra-core/src/notify.rs::AsyncCompletionQueue
    title: Lock-free completion queue backing DbCompletionRing
tags:
  - postgres
  - pyo3
  - async
  - cancellation
---

# Task t01: Abort Shared Database Workers When Python Completion Setup Fails

## Objective & Scope Boundary

Harden `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` by replacing its separate ad-hoc `abort.abort()` error branches with one private armed RAII abort-on-drop worker guard constructed immediately after `db_runtime_handle().spawn(fut)` and moved into the completion monitor task only after all fallible Python setup (`Py::new(DbTaskCanceller)` and `py_future.call_method1("add_done_callback", ...)`) succeeds [^py-async-rs] [^core-notify]. Preserve original `PyErr` exceptions, existing panic/error/cancellation mapping, and per-extension `attr_name` drainer isolation without introducing new queues or public helpers.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr`, `DbTaskCanceller`, `DbCompletionDrainer`, and existing `py_async::tests` suite [^py-async-rs].
- `src/rs/vertebra-core/src/notify.rs` — `AsyncCompletionQueue` and `Notifier` wakeup semantics [^core-notify].
- `.agents/skills/pyo3-bridge/SKILL.md` — Worker-thread GIL prohibition and `py.detach` synchronization rules.
- `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — Cancellation and resource drop ownership patterns.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) async setup-abort requirements.

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/py_async.rs`:
  - Define private `struct ArmedWorkerGuard<T>(Option<tokio::task::JoinHandle<T>>)`:
    - `fn new(handle: tokio::task::JoinHandle<T>) -> Self`
    - `fn abort_handle(&self) -> tokio::task::AbortHandle`
    - `async fn join_and_disarm(mut self) -> Result<T, tokio::task::JoinError>`: `take()`s the inner `JoinHandle` and awaits it (if the monitor itself is dropped/cancelled mid-await before completion, keep abort-on-drop active via a nested drop guard or disarm only after `.await` resolves so a dropped monitor still aborts an unfinished worker).
    - `impl<T> Drop for ArmedWorkerGuard<T>`: if `self.0` is `Some(handle)`, calls `handle.abort()`.
  - Update `spawn_rust_db_task_with_attr` (keeping its exact public signature unchanged):
    1. Immediately wrap `let worker_guard = ArmedWorkerGuard::new(db_runtime_handle().spawn(fut));` and grab `let abort = worker_guard.abort_handle();`.
    2. Run `let canceller = Py::new(py, DbTaskCanceller { abort })?;` and `py_future.call_method1("add_done_callback", (canceller.bind(py),))?;` using standard `?` propagation — any early return drops `worker_guard`, requesting worker abort and preserving the exact original `PyErr`.
    3. Move `worker_guard` into `db_runtime_handle().spawn(async move { let outcome = worker_guard.join_and_disarm().await; ... })` so the worker cannot leak across any setup failure or early monitor drop.

## Edge Cases & Error Semantics

- If `add_done_callback` raises before the Tokio runtime has polled `fut` even once, `ArmedWorkerGuard::drop` aborts the task and drops all values captured inside `fut`'s closure (`DropFlag` / `ConnectAttempt`) without ever invoking `map_ok`.
- If `add_done_callback` raises while `fut` is already suspended at an `.await` point, `ArmedWorkerGuard::drop` aborts `fut` at its await point and drops captured resources asynchronously on the Tokio worker thread; Python returns immediately with the original `PyErr` without blocking on `JoinHandle` while holding the GIL.
- Tests waiting for asynchronous drop acknowledgment **must** release the GIL via `py.detach(|| rx.recv_timeout(Duration::from_secs(5)))` over a deterministic channel/`Condvar` (never `std::thread::sleep` and never blocking while holding the GIL).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-db/src/py_async.rs` (`py_async::tests::callback_registration_failure_aborts_worker` plus existing `py_async::tests`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `add_done_callback` raises on suspended worker | Custom `asyncio.Future` subclass raising `RuntimeError("boom-callback")` in `add_done_callback`; worker holds `DropNotify` guard while suspended on `tokio::sync::Notify` | `spawn_rust_db_task_with_attr` returns `Err(PyRuntimeError("boom-callback"))`; `py.detach` channel wait receives `DropNotify` drop signal; `map_ok` counter equals `0`. |
| `add_done_callback` raises before first poll | Worker future owns `DropNotify` inside an `async move` block gated before poll or immediately pending; `add_done_callback` raises `ValueError("bad-cb")` | Original `ValueError("bad-cb")` preserved; `DropNotify` drop signal received via `py.detach` channel wait; `map_ok` never executes. |
| Existing completion, panic, error, and cancel regressions | Existing `py_async::tests` suite (`spawn_rust_db_task` success, `Err` mapping, worker panic -> `RuntimeError`, Python `Future.cancel()` -> worker abort) | All existing tests continue to pass with `0` regressions. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib callback_registration_failure_aborts_worker`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib callback_registration_failure_aborts_worker`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `spawn_rust_db_task_with_attr` uses a single armed RAII abort-on-drop worker guard across all fallible Python setup steps and disarms only after the completion monitor joins the worker (R7).
- [ ] `callback_registration_failure_aborts_worker` proves original `PyErr` preservation, zero `map_ok` invocations, and deterministic captured-resource drop via `py.detach` channel synchronization without sleeps (R7).
- [ ] All existing `py_async::tests` pass unchanged and commit `fix(db): abort workers on completion setup errors` passes Clippy (R7).

## Notes & Discoveries

- None.

[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared database Tokio task spawner and Python completion bridge.
[^core-notify]: `src/rs/vertebra-core/src/notify.rs::AsyncCompletionQueue` — Lock-free completion queue used by `DbCompletionRing`.
