---
type: Pattern
title: "PyO3 0.30.1 FFI & GIL Discipline"
description: "PyO3 0.30.1 conventions across Python 3.12-3.15/3.15t: gil_used = false, conditional freelist/immutable_type, PEP 782/814 C-API fast paths, anchored Py_buffer, and C-API PyIter_Send drivers."
status: stable
tags: [patterns, rust, pyo3, ffi, gil, python315]
sources:
  - resource: src/rs/vertebra-py/build.rs
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/asgi.rs
  - resource: src/rs/vertebra-py/src/wsgi/environ.rs
  - resource: src/rs/vertebra-py/src/rsgi/scope.rs
  - resource: src/rs/vertebra-py/src/rsgi/http.rs
  - resource: src/rs/vertebra-http/src/main.rs
---

# PyO3 0.30.1 FFI & GIL Discipline

The PyO3 extensions are `_vertebra` ([`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs)), `_bundler` (`vertebra-bundler`), and `_storage` (`vertebra-storage`). All three use PyO3 `0.30.1` with the `Bound<'py, T>` API and declare `#[pymodule(gil_used = false)]` so importing them on `python3.14t` and `python3.15t` leaves the GIL disabled. Keep that attribute on every new `#[pymodule]`.

## Release the GIL Around Blocking Work, Inline Fast Non-Blocking Submits

Any `#[pyfunction]` that blocks on a ring, condition, or the filesystem releases the GIL with `py.detach(|| ...)`. Conversely, small non-blocking response writes (`<= 64 KiB` in `submit_response_raw` and `submit_batch_raw` in [`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs)) copy the `PyBuffer` payload directly into the SHM `SpscRing` slot or `ResponseSink` *without* `py.detach` thread-state churn, detaching only when a full ring requires a blocking spin/futex retry.

<!-- truth: start -->
```rust
#[pyfunction]
#[pyo3(name = "get_batch", signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<RequestBatch>> {
    // SHM path: poll from SPSC ring
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }

    // In-process path: poll from BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "transport not initialized; call init_transport() first",
        )
    })?;

    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| RequestBatch { snapshot: s }))
}
```
<!-- truth: end -->

## One `_vertebra` Module When Embedding

`vertebra-server` links `vertebra-py` statically. [`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs) calls `append_to_inittab!(pymodule_init)` before the first `Python::attach`, and worker initialization imports `_vertebra` and aliases `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing any Vertebra Python code. Any new embedding entrypoint must do the same; otherwise Python loads the wheel's `_vertebra.cpython-*.so`, which has its own uninitialized `static BRIDGE: OnceLock<BatchBridge>` and `SHM_TRANSPORT`.

## Conditional `freelist`, `immutable_type`, Anchored `Py_buffer` & `PyIter_Send` Drivers

- **Conditional `freelist` & `immutable_type`**: Hot-path `#[pyclass]` types (`Request`, `Response`, `Headers`, `AsgiScope`, `AsgiProtocol`, `PyEmptyAwaitable`, `PyValueAwaitable`, `_SteppedCoroutine`, `RsgiScope`, `RsgiHttpProtocol`, `_RsgiReadyAwaitable`, `WsgiInput`) apply `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(..., freelist = 128, immutable_type))]` and `#[cfg_attr(Py_GIL_DISABLED, pyclass(..., immutable_type))]` because PyO3 `0.30.1` guards `freelist` behind a global `Mutex` under `Py_GIL_DISABLED` (contending across `3.14t`/`3.15t` worker threads vs mimalloc thread-local heaps), while `immutable_type` (`Py_TPFLAGS_IMMUTABLETYPE`) enables CPython 3.14/3.15 JIT and inline-cache specialization.
- **Anchored Zero-Copy Buffer Protocol (`Request`)**: [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) implements `__getbuffer__` passing borrowed `slf.as_ptr()` to `PyBuffer_FillInfo` (which calls `Py_XNewRef` internally, keeping `Arc<BatchSnapshot>` alive until the `memoryview` is released) and exposes `memoryview(req)` (`req.body_view`) with PEP 688 `def __buffer__(self, flags: int, /) -> memoryview: ...` in `_vertebra.pyi`.
- **PEP 782 `PyBytesWriter` & Python 3.15 PEP 814 / Immortal Constants**: Use `PyBytes::new_with(py, len, |dst| ...)` (backed by CPython 3.15's PEP 782 `PyBytesWriter` C-API in PyO3 `0.30.1`) to populate byte buffers in place without intermediate `Vec<u8>` copies. [`vertebra-py/build.rs`](../../../../../src/rs/vertebra-py/build.rs) runs `pyo3_build_config::use_pyo3_cfgs()`, gating PEP 814 `PyFrozenDict::empty(py)` (`build_empty_capability_entry` in `asgi.rs`) and `PyUnstable_SetImmortal` on static protocol constants under `#[cfg(Py_3_15)]`.
- **C-API `PyIter_Send` Coroutine Stepping**: [`drive_asgi_batch`](../../../../../src/rs/vertebra-py/src/asgi.rs) and [`drive_rsgi_batch`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) step ASGI/RSGI coroutines directly via `pyo3::ffi::PyIter_Send(coro_ptr, Py_None(), &mut result_ptr)`: `PYGEN_RETURN` completes in `0` `asyncio.Task` allocations, while `PYGEN_NEXT` wraps the already-stepped coroutine in `_SteppedCoroutine` (`first_yielded`) and schedules `loop.create_task` once. `RsgiReadyAwaitable` (`_RsgiReadyAwaitable`) is `frozen` (`&self` on `__await__`/`__next__`) so concurrent awaits on `3.14t`/`3.15t` never trigger `PyBorrowMutError`.

## Intern Repeated Keys & Free-Threading Flags

Use `pyo3::intern!(py, "...")` for fixed header names and scope/environ keys ([`headers.rs`](../../../../../src/rs/vertebra-py/src/headers.rs), [`asgi.rs`](../../../../../src/rs/vertebra-py/src/asgi.rs), [`rsgi/scope.rs`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs), [`wsgi/environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs)) and `PyOnceLock` for process-wide cached objects. `_vertebra` exports `FREE_THREADED` (`cfg(Py_GIL_DISABLED)`) and `GIL_USED = False`.
