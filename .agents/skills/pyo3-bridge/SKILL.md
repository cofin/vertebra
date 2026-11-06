---
name: pyo3-bridge
description: Use when implementing or debugging PyO3 native extension modules, GIL release discipline, free-threaded Python 3.14t/3.15t safety, zero-copy FFI buffer views, or msgspec C-API bridges in vertebra-py, vertebra-bundler, vertebra-storage, vertebra-bigquery, and vertebra-spanner.
---

<!-- truth: start -->
- **Crates & Stubs**: `src/rs/vertebra-py`, `src/rs/vertebra-bundler`, `src/rs/vertebra-storage`, `src/rs/vertebra-bigquery`, `src/rs/vertebra-spanner`, and `src/py/vertebra/{_vertebra,_bundler,_storage,_bigquery,_spanner}.pyi`.
- **Free-Threaded Declaration**: Every `#[pymodule]` declares `#[pymodule(gil_used = false)]`, and `#[pyclass]` types set `immutable_type` with `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(freelist = 128))]` (`FFI-01`).
- **GIL Detachment**: Release the GIL via `py.detach(|| ...)` around all blocking I/O, SHM futex/ulock waits, compression, and bundler builds (`FFI-02`, `FFI-03`).
- **View Lifetimes**: Zero-copy views (`Request.__getbuffer__`, `body_view`, `decode_msgspec`) anchor or borrow within the active `Arc<BatchSnapshot>` lease and never outlive their backing SHM slot (`FFI-04`).
- **Stub Parity**: Any `#[pyfunction]` or `#[pyclass]` signature change updates the corresponding `.pyi` stub and passes `mypy.stubtest` (`FFI-09`).
- **Verify**: `make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner`
<!-- truth: end -->

# PyO3 Native Bridge & Free-Threaded Runtime (`pyo3-bridge`)

Implementation guide for PyO3 native extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner` across `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, and `vertebra-spanner`), zero-copy batch dispatch (`AsgiProtocol`, `RsgiScope`, `WsgiInput`), C-API `msgspec` struct hydration (`msgspec_bridge.rs`), and free-threaded Python 3.14t/3.15t concurrency.

## Architecture & Domain References

- [PyO3 FFI and batch dispatch](../../bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md)
- [PyO3 and Python runtime gotchas](../../bundles/knowledge/gotchas/pyo3-and-python-runtime.md)
- [FFI and GIL discipline](../../bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md)
- [Benchmarks and free-threaded runtime](../../bundles/knowledge/testing/benchmarks-and-free-threaded.md)
- Free-threaded and FFI reference: [`references/ffi-and-315t.md`](references/ffi-and-315t.md)
- Review checklist & suppressions: [`review-pyo3-bridge`](../review-pyo3-bridge/SKILL.md)

## Core Implementation Rules

1. **Free-Threaded 3.14t/3.15t Concurrency (`FFI-01`, `FFI-05`)**:
   - Annotate all native modules with `#[pymodule(gil_used = false)]`.
   - Gate PyO3 class freelists behind `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(..., freelist = 128, immutable_type))]` so free-threaded builds use CPython's lock-free thread-local `mimalloc` instead of a global `Mutex<PyObjectFreeList>`.
   - Use `pyo3::intern!(py, "...")` rather than runtime `PyString::intern` on hot paths.

2. **GIL Release Discipline (`FFI-02`, `FFI-03`)**:
   - Extract owned Rust arguments or slice coordinates before entering `py.detach(|| ...)`.
   - Re-acquire `Python<'_>` only when constructing final Python objects (`PyBytes::new_with`, `PyFrozenDict`, or vectorcall `msgspec.Struct` instances).

3. **Zero-Copy Buffer Lifetimes & Error Mapping (`FFI-04`, `FFI-06`)**:
   - In `Request.__getbuffer__`, pass `slf.as_ptr()` (borrowed, letting `PyBuffer_FillInfo` `Py_INCREF` the owner once) rather than `slf.into_ptr()`.
   - Map all fallible Rust operations on caller inputs to typed `PyErr` (`PyValueError`, `PyRuntimeError`, `PyTimeoutError`); never `unwrap()` user input.

4. **Rebuild & Type-Stub Synchronization (`FFI-09`)**:
   - After modifying any Rust crate under `src/rs/`, run `make dev` so `.venv` loads the updated native `.so`/`.abi3.so` binaries before running `pytest` or `stubtest`.

## Verification

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner
```
