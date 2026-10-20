---
type: Pattern
title: "PyO3 0.30.1 FFI & GIL Discipline"
description: "PyO3 0.30.1 FFI conventions: gil_used = false, py.detach around blocking waits, zero-copy Py_buffer views, PyOnceLock interning, and append_to_inittab!."
status: stable
tags: [patterns, rust, pyo3, ffi, gil, zero-copy]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/response.rs
  - resource: src/rs/vertebra-py/src/headers.rs
  - resource: src/rs/vertebra-py/src/batch.rs
---

# PyO3 0.30.1 FFI & GIL Discipline

[`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) and [`vertebra-bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs) use PyO3 `0.30.1` (`Bound<'py, PyAny>`, `#[pymodule(gil_used = false)]`, `#[pyclass(from_py_object)]` where `Clone` is required) and enforce strict GIL release, zero-copy `Py_buffer` borrowing, and string interning.

## 1. Always Release the GIL (`py.detach`) Around Blocking Waits & Bulk Copies

Any Rust function called from Python that blocks on a condition variable, futex/ulock, channel, subprocess wait, or bulk buffer copy must release the GIL with `py.detach(|| ...)`:

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

## 2. Embedded Inittab Registration (`append_to_inittab!`)

When embedding Python inside a Rust binary (`vertebra-server` or `vertebra-http` tests), always call [`vertebra_py::register_inittab()`](../../../../../src/rs/vertebra-py/src/lib.rs) *before* `Python::initialize()` and alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]`. Never allow Python to `dlopen` a second copy of `_vertebra.cpython-*.so` in `Topology::Embedded`, or `static BRIDGE: OnceLock<BatchBridge>` will split across two DSOs.

## 3. Lazy Slab-Backed `#[pyclass]` Types & Zero-Copy `Py_buffer` (`Request`, `Headers`, `Response`)

- Never eagerly convert all request headers, query strings, or bodies into Python `dict` or `str` objects during batch receipt. [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) holds `Arc<DataSlab>` + `RequestSlot` and slices bytes out of `DataSlab` only when Python accesses a specific property (`request.path`, `request.headers`, `request.body`).
- Use `request.body_view` (`PyMemoryView_FromMemory` pinned by `_SlabAnchor`) when feeding request bodies into `msgspec.json.decode` so zero intermediate `bytes` allocations occur.
- Use `PyOnceLock<Py<PyString>>` and `pyo3::intern!` in [`headers.rs`](../../../../../src/rs/vertebra-py/src/headers.rs) for known HTTP header names and scope keys so repeated lookups share immortal Python string objects.
