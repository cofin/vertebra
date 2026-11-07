# Vertebra False-Positive Suppressions

Consult this catalog before reporting any review finding. If a candidate finding matches an entry below, do not flag it.

---

### FP-RT-01: SeqCst on futex wait words
- **Applies to**: RT-03, RT-04
- **Scope**: src/rs/vertebra-core/src/channel.rs, src/rs/vertebra-core/src/ipc.rs
- **Do not flag**: Suggestions to weaken `Ordering::SeqCst` on `wait_read_ptr`/`wait_write_ptr`/`send_waiters`/`recv_waiters` to `Acquire`/`Release`.
- **Why**: Store-load reordering between the cursor publish and the waiter load drops futex wakeups; see [shm gotchas](../../../bundles/knowledge/gotchas/shm-and-cross-platform-ipc.md).

---

### FP-RT-02: Rust comments and safety invariants
- **Applies to**: RT-01, PY-02
- **Scope**: src/rs/vertebra-core/src/channel.rs
- **Do not flag**: `//` and `// SAFETY:` comments in Rust source code.
- **Why**: The zero-inline-comment rule is Python-only; Rust safety comments are required by RT-01 and documented in [lock-free SHM](../../../bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md).

---

### FP-RT-03: Darwin ulock return code convention
- **Applies to**: RT-08
- **Scope**: src/rs/vertebra-core/src/platform/macos.rs
- **Do not flag**: Treating `rc >= 0` as success for Darwin `__ulock_wait` with `ULF_NO_ERRNO`.
- **Why**: With `ULF_NO_ERRNO`, Darwin XNU `__ulock_wait` returns `>= 0` (remaining waiters) on wake/success and `< 0` (`-errno`, e.g. `-ETIMEDOUT` or `-EINTR`) on timeout or interruption; see [SHM gotchas](../../../bundles/knowledge/gotchas/shm-and-cross-platform-ipc.md).

---

### FP-HTTP-01: Matchit route path syntax
- **Applies to**: HTTP-10
- **Scope**: src/rs/vertebra-http/src/routing.rs
- **Do not flag**: matchit 0.9 `{name}` and `{*rest}` parameter syntax in route definitions.
- **Why**: Vertebra HTTP uses matchit 0.9 syntax paired with Python typed router normalization; see [ingress routing](../../../bundles/knowledge/architecture/http/ingress-and-routing.md).

---

### FP-FFI-01: PyO3 thread detachment
- **Applies to**: FFI-02
- **Scope**: src/rs/vertebra-py/src/lib.rs
- **Do not flag**: Uses of `py.detach(...)` instead of legacy `Python::allow_threads`.
- **Why**: PyO3 0.30 introduces `py.detach` as the canonical off-GIL execution primitive; see [FFI discipline](../../../bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md).

---

### FP-FFI-02: Cloneable PyClass FromPyObject
- **Applies to**: FFI-05
- **Scope**: src/rs/vertebra-py/src/lib.rs
- **Do not flag**: `#[pyclass(from_py_object)]` annotations on immutable Rust structs.
- **Why**: PyO3 0.30 opt-in derives fast value extraction for cloned parameter values; see [PyO3 runtime gotchas](../../../bundles/knowledge/gotchas/pyo3-and-python-runtime.md).

---

### FP-PY-01: Absence of inline comments in Python
- **Applies to**: PY-02
- **Scope**: src/py/vertebra/__init__.py
- **Do not flag**: Clean Python code lacking `#` inline comments, and never suggest adding `# noqa` or `# type: ignore`.
- **Why**: Vertebra mandates zero inline `#` comments; intent is documented exclusively in Google-style docstrings per [module layout](../../../bundles/knowledge/patterns/python/module-and-class-layout.md).

---

### FP-PY-02: PEP 562 lazy attribute loading
- **Applies to**: PY-06, PY-08
- **Scope**: src/py/vertebra/__init__.py
- **Do not flag**: Top-level package `__getattr__` functions performing deferred imports and caching into `globals()`.
- **Why**: Lazy attribute loading is required to preserve fast import time budgets; see [module layout](../../../bundles/knowledge/patterns/python/module-and-class-layout.md).

---

### FP-PY-03: ASGI direct send optimization
- **Applies to**: PY-10
- **Scope**: src/py/vertebra/**
- **Do not flag**: Direct synchronous frame emission in ASGI response runners without creating an `asyncio.Task`.
- **Why**: Direct-drive execution removes loop scheduler wakeups on unbuffered paths; see [async dispatch](../../../bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md).

---

### FP-PERF-01: Dispatch spec caching
- **Applies to**: PERF-02
- **Scope**: src/py/vertebra/**
- **Do not flag**: Module-level `@functools.lru_cache(maxsize=512)` on handler dispatch spec compilation.
- **Why**: Bounded function cache eliminates per-request signature inspection and regex compilation; see [async dispatch](../../../bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md).

---

### FP-TS-01: Zero dependency client backoff
- **Applies to**: TS-02
- **Scope**: src/ts/vertebra-client/src/index.ts
- **Do not flag**: Hand-rolled exponential backoff and jitter calculations in `@vertebra/client`.
- **Why**: The TypeScript client core maintains zero runtime dependencies; see [client subpaths](../../../bundles/knowledge/patterns/typescript/client-and-adapter-subpaths.md).

---

### FP-TEST-01: Anyio test markers
- **Applies to**: TEST-07
- **Scope**: src/py/tests/conftest.py
- **Do not flag**: Using `@pytest.mark.anyio` without configuring `pytest-asyncio`.
- **Why**: Vertebra test suites run on AnyIO for multi-loop test compatibility; see [polyglot test matrix](../../../bundles/knowledge/testing/polyglot-test-matrix.md).

---

### FP-DOC-01: KaTeX mathematical notation
- **Applies to**: DOC-02
- **Scope**: AGENTS.md
- **Do not flag**: KaTeX math blocks or symbols in documentation and knowledge prose.
- **Why**: Mathematical formulas and state transitions use standard KaTeX syntax; see [workflow guide](../../../bundles/knowledge/workflow.md).
