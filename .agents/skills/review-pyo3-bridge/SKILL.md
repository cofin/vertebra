---
name: review-pyo3-bridge
description: Use when reviewing PyO3 native extension modules, GIL release discipline, free-threaded Python 3.14t/3.15t safety, and FFI view lifetimes across vertebra-py and vertebra-bundler.
---

<!-- truth: start -->
- **GIL**: Every `#[pymodule]` declares `gil_used = false` (`FFI-01`).
- **Detach**: Blocking I/O, waits, and transforms run inside `py.detach(...)` without Python references (`FFI-02`, `FFI-03`).
- **Lifetimes**: Zero-copy views (`PyBuffer`, `body_view`) never outlive their backing SHM slot (`FFI-04`).
- **Free-threaded**: Shared mutable state uses atomics or `Mutex`; no `static mut` (`FFI-05`).
- **Errors**: No `unwrap()` on caller-controlled input; map to typed `PyErr` (`FFI-06`).
- **Suppressions**: Respect `FP-FFI-01`..`FP-FFI-02` in the shared catalog.
<!-- truth: end -->

# PyO3 Bridge Review (`review-pyo3-bridge`)

## Scope

- Paths: `[areas.pyo3-bridge]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `FFI-01`..`FFI-10` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [PyO3 FFI and batch dispatch](../../bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md)
- [PyO3 and Python runtime gotchas](../../bundles/knowledge/gotchas/pyo3-and-python-runtime.md)
- [FFI and GIL discipline](../../bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md)
- Free-threaded reference: [`references/free-threaded-314t.md`](references/free-threaded-314t.md)

## Review Process

1. **GIL flags and detachment**: Verify `gil_used = false` and ensure all blocking calls release the GIL using `py.detach` (`FFI-01`, `FFI-02`).
2. **Boundary isolation**: Verify no Python object reference crosses into `py.detach` closures (`FFI-03`).
3. **Buffer lifetimes**: Audit SHM memoryview exposures to ensure release cannot cause use-after-free (`FFI-04`).
4. **Thread safety**: Check that `#[pyclass]` instances are safe under concurrent multi-threaded access without the GIL (`FFI-05`).
5. **Stub parity**: Verify that `_vertebra.pyi` and `_bundler.pyi` signatures match Rust implementations (`FFI-09`).
6. **Interning**: Verify hot headers and dictionary keys use `intern!` (`FFI-08`).

## Suppressions Index

- `FP-FFI-01`: PyO3 thread detachment
- `FP-FFI-02`: Cloneable PyClass FromPyObject

## Focused Commands

```bash
cargo test -p vertebra-py
uv run mypy src/py/vertebra
```
