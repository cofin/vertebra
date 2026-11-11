# PyO3 FFI Lifetimes, C-API Hydration, and Free-Threaded Python 3.14t/3.15t

## `py.detach` Boundary Pattern (`vertebra-py/src/transport.rs`)

```rust
#[pyfunction]
#[pyo3(name = "get_batch", signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<RequestBatch>> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        if let Some(s) = transport.poll_batch_async() {
            return Ok(Some(RequestBatch { snapshot: s }));
        }
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }
    let bridge = BatchBridge::get()
        .ok_or_else(|| PyRuntimeError::new_err("transport not initialized; call init_transport() first"))?;
    if let Some(s) = bridge.try_poll_batch() {
        return Ok(Some(RequestBatch { snapshot: s }));
    }
    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| RequestBatch { snapshot: s }))
}
```

- `try_get_batch()` in `vertebra-py/src/transport.rs` provides the non-blocking fast path (`poll_batch_async()` / `try_poll_batch()`) for event-loop reader callbacks without parking the OS thread.

## Direct C-API Decoding & Struct Hydration (`msgspec_bridge.rs`)

- `decode_msgspec_from_buffer` and `decode_msgspec_from_slice` cache bound `msgspec.{json,msgpack}.Decoder(ty).decode` callables per target type and invoke them via CPython single-argument vectorcall (`PyObject_Vectorcall` / `PyObject_CallOneArg` semantics) over zero-copy PEP 688 / PEP 3118 `__getbuffer__` `PyMemoryView` views (`PyMemoryView_FromObject` and `PyMemoryView_FromMemory`).
- `instantiate_struct_vectorcall`, `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, and `orm_to_struct_vectorcall` hydrate `msgspec.Struct`, `@dataclass`, `attrs`, and Pydantic models directly via `PyObject_Vectorcall` / `PyObject_VectorcallDict` with cached `__struct_fields__` keyword tuples, avoiding intermediate `PyBytes` or `PyDict` allocations.

## Free-Threaded Build Isolation

- Standard GIL-enabled builds use `.venv` and `src/rs/target`.
- Free-threaded 3.14t/3.15t builds (`make bench-ft`) isolate artifacts in `.venv-ft` and `CARGO_TARGET_DIR=src/rs/target-ft` because `Py_GIL_DISABLED` alters the CPython C-API ABI header layout (`PyObject` reference count fields).
