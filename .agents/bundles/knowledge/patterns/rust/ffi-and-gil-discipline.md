---
type: Pattern
title: "PyO3 0.30.1 FFI & GIL Discipline"
description: "PyO3 conventions in the Vertebra extensions: gil_used = false, py.detach around blocking work, one embedded _vertebra module, lazy batch-backed request proxies, and interned keys."
status: stable
tags: [patterns, rust, pyo3, ffi, gil]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/asgi_scope.rs
  - resource: src/rs/vertebra-py/src/wsgi_environ.rs
  - resource: src/rs/vertebra-py/src/rsgi_protocol.rs
  - resource: src/rs/vertebra-http/src/main.rs
---

# PyO3 0.30.1 FFI & GIL Discipline

The PyO3 extensions are `_vertebra` ([`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs)), `_bundler` (`vertebra-bundler`), and `_storage` (`vertebra-storage`). All three use PyO3 `0.30.1` with the `Bound<'py, T>` API and declare `#[pymodule(gil_used = false)]` so importing them on `python3.14t` leaves the GIL disabled. Keep that attribute on every new `#[pymodule]`.

## Release the GIL Around Blocking Work (`py.detach`)

Any `#[pyfunction]` that waits on a ring, channel, condition, or the filesystem releases the GIL with `py.detach(|| ...)`. Current sites: `get_batch` and the submit paths in [`api.rs`](../../../../../src/rs/vertebra-py/src/api.rs), file metadata and range reads in [`rsgi_protocol.rs`](../../../../../src/rs/vertebra-py/src/rsgi_protocol.rs), and file metadata in `wsgi_worker.rs`. Copy owned data out of `PyRef`s (and `drop` the borrow) before entering `detach`, as `submit_response` does.

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

`vertebra-server` links `vertebra-py` statically. [`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs) calls `append_to_inittab!(pymodule_init)` before the first `Python::attach`, and `spawn_python_worker` imports `_vertebra` and stores it as `sys.modules["vertebra._vertebra"]` before importing any Vertebra Python code. Any new embedding entrypoint must do the same; otherwise Python loads the wheel's `_vertebra.cpython-*.so`, which has its own uninitialized `static BRIDGE: OnceLock<BatchBridge>` and `SHM_TRANSPORT`.

## Lazy, Batch-Backed `#[pyclass]` Proxies

- [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) holds `Arc<BatchSnapshot>` plus a node index and a cached `Headers`. Path, query, headers, and body are sliced from the snapshot only when the property is read. Keep new request accessors lazy; do not convert whole batches into Python objects on receipt.
- `request.body_view` returns a read-only `memoryview` built with `PyMemoryView_FromMemory` directly over the slab. Nothing in the `memoryview` keeps the snapshot alive, so it is valid only while the handler runs; call `bytes(request.body_view)` to keep the data.
- `Headers` is `#[pyclass(frozen, freelist = 128, from_py_object)]`.

## Intern Repeated Keys

Use `pyo3::intern!(py, "...")` for fixed header names and scope/environ keys, as in [`headers.rs`](../../../../../src/rs/vertebra-py/src/headers.rs), [`asgi_scope.rs`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs), and [`wsgi_environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs). Use `pyo3::sync::PyOnceLock` for process-wide cached Python objects (`SYS_STDERR` in `wsgi_environ.rs`).

## Free-Threading Flags

`_vertebra` exports `FREE_THREADED` (compiled from `cfg(Py_GIL_DISABLED)`) and `GIL_USED = False`. `vertebra-py` is the only Rust crate that branches on `Py_GIL_DISABLED`; HTTP code stays build-agnostic, and Python decides pool sizing with `is_free_threaded()`.
