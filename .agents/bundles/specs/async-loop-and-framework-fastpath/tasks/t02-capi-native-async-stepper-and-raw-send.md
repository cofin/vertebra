---
type: Task
id: async-loop-and-framework-fastpath:t02
title: "Add C-API drive_native_batch (PyIter_Send), AsgiScope.body_bytes(), AsgiProtocol.send_raw_response, AsgiSend.send_raw_response, and _WsgiStartResponse.send_raw_response in vertebra-py"
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - async-loop-and-framework-fastpath:t01
files:
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/wsgi/runner.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_dispatch.py
tests:
  - src/py/tests/unit/server/test_asgi_capi_runner.py
  - src/py/tests/unit/server/test_wsgi_batch_runner.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:03:00Z
commit: null
tags: [rust, pyo3, c-api, asgi, wsgi, dispatch, tdd]
---

# Task t02: Add C-API `drive_native_batch` (`PyIter_Send`), `AsgiScope.body_bytes()`, `AsgiProtocol.send_raw_response`, `AsgiSend.send_raw_response`, and `_WsgiStartResponse.send_raw_response` in `vertebra-py`

## Objective
Add C-API single-step coroutine driving (`drive_native_batch` via `ffi::PyIter_Send`) for native `async def` handlers alongside `AsgiScope.body_bytes()`, `AsgiProtocol.body_bytes()`, `AsgiProtocol.__call__`, `AsgiProtocol.send_raw_response`, `AsgiSend.send_raw_response` (`src/py/vertebra/server/_asgi/_queues.py`), and `WsgiStartResponse.send_raw_response` (`_WsgiStartResponse` in `src/rs/vertebra-py/src/wsgi/runner.rs`), and flatten `compile_handler_spec` in `src/py/vertebra/server/_dispatch.py` so synchronous parameter extraction on `async def` routes returns the handler coroutine directly without outer coroutine wrappers.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing tests in `src/py/tests/unit/server/test_asgi_capi_runner.py` and `src/py/tests/unit/server/test_wsgi_batch_runner.py` verifying `drive_native_batch` (both `PYGEN_RETURN` zero-Task fastpath and `PYGEN_NEXT` `SteppedCoroutine` promotion), `AsgiScope.body_bytes()`, `AsgiProtocol.send_raw_response` (including when `proto_bound` is passed directly as `send` in `drive_asgi_targets_inline`), `AsgiSend.send_raw_response`, `_WsgiStartResponse.send_raw_response`, and flattened `compile_handler_spec` in `_dispatch.py`.
- **Architectural Invariants**:
  - Keep `#[pymodule(gil_used = false)]` and conditional `freelist = 128` (`#[cfg_attr(not(Py_GIL_DISABLED), ...)]`).
  - Update `src/py/vertebra/_vertebra.pyi` so `python -m mypy.stubtest vertebra._vertebra` passes with zero errors.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/server/test_asgi_capi_runner.py`, add tests for:
     - `AsgiScope.body_bytes()` and `AsgiProtocol.body_bytes()` returning the request body `bytes` with zero awaitable allocation.
     - `AsgiProtocol.send_raw_response(status, headers, body)` and `AsgiSend.send_raw_response(status, headers, body)` writing status, headers, and body in one call, and `AsgiProtocol.__call__(message)` acting as `send(message)` so `drive_asgi_targets_inline` can pass `proto_bound` directly as `send`.
     - `drive_native_batch` stepping non-suspending `async def` coroutines via `PyIter_Send` with zero `loop.create_task` calls and promoting suspending `async def` coroutines (`await asyncio.sleep(0)`) via `SteppedCoroutine`.
     - `compile_handler_spec` for an `async def` handler with path/query/`msgspec.Struct` parameters returning the handler's direct coroutine without an outer `_invoke_compiled_async` wrapper when guards/dependencies are synchronous.
   - In `src/py/tests/unit/server/test_wsgi_batch_runner.py`, add a test verifying `start_response.send_raw_response(status, headers, body)` inside `drive_wsgi_batch`.
2. **Implement Rust C-API methods and flattened `_dispatch.py` (`GREEN`)**:
   - In `src/rs/vertebra-py/src/asgi.rs`:
     - Add `pub fn body_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes>` to `AsgiScope` (`PyBytes::new(py, self.batch.body(self.index))`) and `AsgiProtocol` (`PyBytes::new(py, self.body.as_slice())`).
     - Add `pub fn __call__(&self, py: Python<'_>, message: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>>` on `AsgiProtocol` delegating to `self.send(py, message)`, and pass `proto_bound.clone()` as the `send` argument in `drive_asgi_targets_inline` so `send.send_raw_response(...)` is directly accessible on `send`.
     - Add `#[pyo3(signature = (status, headers=None, body=b""))] pub fn send_raw_response(&self, py: Python<'_>, status: u16, headers: Option<&Bound<'_, PyAny>>, body: &[u8]) -> PyResult<PyEmptyAwaitable>` to `AsgiProtocol` using `encode_py_headers_into` and `ResponseSubmitter::submit_raw_slices` (when `immediate_submit` is set) or populating `AsgiProtocolState` (`started = true`, `complete = true`, `status`, `headers_raw`, `single_body_py`) and returning `PyEmptyAwaitable`.
     - Add `ProtocolFinalizer::Native { request_id: u64, default_status: u16, response_mode: u8 }` to `SteppedCoroutine` and implement `#[pyfunction] #[pyo3(signature = (items, loop_obj=None))] pub fn drive_native_batch(py: Python<'_>, items: &Bound<'_, PyList>, loop_obj: Option<&Bound<'_, PyAny>>) -> PyResult<usize>` in `src/rs/vertebra-py/src/asgi.rs`, registering `drive_native_batch` in `src/rs/vertebra-py/src/lib.rs` and `src/py/vertebra/_vertebra.pyi`.
   - In `src/py/vertebra/server/_asgi/_queues.py`:
     - Add `def send_raw_response(self, status: int, headers: Sequence[tuple[bytes, bytes]] | None = None, body: bytes = b"") -> _EmptySendAwaitable` to `AsgiSend` setting `self._status_code = status`, `self._headers = list(headers) if headers else []`, `self._single_body = body`, `self._response_started = True`, `self._response_complete.set()`, and returning `_EMPTY_SEND_AWAITABLE`.
   - In `src/rs/vertebra-py/src/wsgi/runner.rs`:
     - Add `#[pyo3(signature = (status, headers=None, body=b""))] pub fn send_raw_response<'py>(&self, py: Python<'py>, status: u16, headers: Option<&Bound<'py, PyAny>>, body: &[u8]) -> PyResult<Bound<'py, PyList>>` to `WsgiStartResponse` (`_WsgiStartResponse`) encoding `headers` via `encode_py_headers_into`, setting `called = true`, `status`, `headers_raw`, `has_content_length`, and returning a 1-element `PyList` containing `PyBytes::new(py, body)`.
   - In `src/py/vertebra/server/_dispatch.py`:
     - In `compile_handler_spec`, when `is_async` is True and `not async_dep_fns and not async_guards and not has_generator_deps`, compile a synchronous function `def _invoke_compiled_sync_to_coro(request: Any) -> Any` that executes sync guards and `_extract_call_args_fast` synchronously and returns `handler(*args)` directly.
     - In `dispatch_batch`, when `drive_native_batch` is available and running inline on the worker event loop, step `partitioned.async_native` coroutines via `drive_native_batch` so `PYGEN_RETURN` handlers complete and flush in a single batch without `_await_async_item` or `loop.create_task`.
3. **Rebuild extensions & verify (`REFACTOR`)**:
   - Run `make dev`, `mypy.stubtest`, and the canonical unit tests.

## Verification
```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra --allowlist tools/stubtest_allowlist.txt
uv run --no-sync pytest src/py/tests/unit/server/test_asgi_capi_runner.py src/py/tests/unit/server/test_wsgi_batch_runner.py -q
```

## Acceptance Criteria
- [ ] `AsgiScope.body_bytes()`, `AsgiProtocol.body_bytes()`, `AsgiProtocol.__call__`, `AsgiProtocol.send_raw_response`, `AsgiSend.send_raw_response`, and `_WsgiStartResponse.send_raw_response` work with zero intermediate ASGI response dicts or header string round-trips (R2).
- [ ] `drive_native_batch` steps native `async def` coroutines via `PyIter_Send`, flushing `PYGEN_RETURN` results with zero `asyncio.Task` allocations and promoting `PYGEN_NEXT` coroutines via `SteppedCoroutine` (R2).
- [ ] `compile_handler_spec` avoids wrapping `async def` handlers in `_invoke_compiled_async` when all guards and dependencies are synchronous (R2).

## Notes & Discoveries
