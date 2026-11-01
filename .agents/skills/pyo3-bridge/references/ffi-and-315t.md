# PyO3 FFI Lifetimes, C-API Hydration, and Free-Threaded Python 3.14t/3.15t

## `py.detach` Boundary Pattern

```rust
#[pymethods]
impl ShmWorkerBridge {
    fn poll_batch<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyList>> {
        let frames = py.detach(|| self.inner.wait_and_drain_batch(timeout_ms))
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        self.hydrate_python_batch(py, &frames)
    }
}
```

## Direct C-API Struct Hydration (`msgspec_bridge.rs`)

- Hot-path frame hydration populates `msgspec.Struct(..., frozen=True, gc=False, array_like=True)` attributes directly via CPython C-API slot offsets cached at module initialization.
- Avoid intermediate `PyDict` allocation when passing `(method, path, headers, body_view, params)` from `vertebra-py` into `vertebra.server`.

## Free-Threaded Build Isolation

- Standard GIL-enabled builds use `.venv` and `src/rs/target`.
- Free-threaded 3.14t/3.15t builds (`make bench-ft`) isolate artifacts in `.venv-ft` and `CARGO_TARGET_DIR=src/rs/target-ft` because `Py_GIL_DISABLED` alters the CPython C-API ABI header layout (`PyObject` reference count fields).
