---
type: Guide
title: "PyO3 FFI, Embedded Globals & Python Runtime Gotchas"
description: "Footguns in Embedded PyO3 module initialization, gil_used = false on 3.14t, task-free ASGI coroutine driving, unanchored body_view lifetimes, and PyO3 rebuilds."
status: stable
tags: [gotchas, pyo3, asgi, python, free-threaded]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-bundler/src/lib.rs
  - resource: src/rs/vertebra-storage/src/lib.rs
  - resource: src/rs/vertebra-http/src/main.rs
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_asgi/_wire.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/py/vertebra/server/_gil_audit.py
  - resource: pyproject.toml
---

# PyO3 FFI, Embedded Globals & Python Runtime Gotchas

## 1. Dual `.so` Split-Globals Trap in `Topology::Embedded`

- **Symptom**: `RuntimeError: transport not initialized; call init_transport() first` even though Rust initialized `BatchBridge` right before spawning the Python worker thread.
- **Root Cause**: The `vertebra-server` binary (package `vertebra-http`, feature `python`) statically links `vertebra-py` (`_vertebra`), while `.venv/lib/python3.*/site-packages/vertebra/_vertebra.cpython-*.so` is a separate dynamically loadable shared object. If Python executes a file-backed `import vertebra._vertebra`, `dlopen` loads the `.so` from disk with a **second** copy of `static BRIDGE: OnceLock<BatchBridge>` that is uninitialized.
- **Rule**: In [`src/rs/vertebra-http/src/main.rs`](../../../../src/rs/vertebra-http/src/main.rs), always call `pyo3::append_to_inittab!(pymodule_init)` (imported from `_vertebra`) before the first `Python::attach` (`auto-initialize`), and on the worker thread alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing the application or `vertebra.server._dispatch`.

## 2. Missing `#[pymodule(gil_used = false)]` Re-Enables the GIL on Python `3.14t`

- **Symptom**: `sys._is_gil_enabled()` flips from `False` to `True` with a `RuntimeWarning` when importing native extensions under `python3.14t`.
- **Root Cause**: CPython `3.14t` automatically re-enables the GIL if any imported C/PyO3 extension module omits `Py_MOD_GIL_NOT_USED`.
- **Rule**: Every `#[pymodule]` in the workspace ([`vertebra-py/src/lib.rs`](../../../../src/rs/vertebra-py/src/lib.rs), [`vertebra-bundler/src/lib.rs`](../../../../src/rs/vertebra-bundler/src/lib.rs), and [`vertebra-storage/src/lib.rs`](../../../../src/rs/vertebra-storage/src/lib.rs)) must keep `#[pymodule(gil_used = false)]`. At worker startup, [`GilAuditor.audit()`](../../../../src/py/vertebra/server/_gil_audit.py) inspects loaded extensions, logs warnings if the GIL was re-enabled, and raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING=1`.

## 3. Task-Free `AsgiRunner` vs `asyncio.current_task()` (`_asgi/_runner.py`)

- **Symptom**: ASGI middlewares or libraries that inspect `asyncio.current_task()` before their first `await` observe `None` under [`AsgiRunner`](../../../../src/py/vertebra/server/_asgi/_runner.py).
- **Root Cause**: By default (`VERTEBRA_ASGI_DIRECT_SEND=1`), `_drive_coro` executes the synchronous first `coro.send(None)` step directly without creating an `asyncio.Task`. On the first suspension, `_handle_yielded` promotes the coroutine into a real `asyncio.Task` running `_resume_coro_in_task`, so subsequent steps have a valid current task.
- **Rule**: Set `VERTEBRA_ASGI_DIRECT_SEND=0` *before* importing `vertebra.server._asgi._runner` (the flag is read once at module import) when running ASGI code that requires `asyncio.current_task()` during synchronous setup before the first `await`.

## 4. Unanchored `Request.body_view`, Shared Class Names & `submit_raw_response`

- **Unanchored `Request.body_view` Lifetime**: [`Request.body_view`](../../../../src/rs/vertebra-py/src/request.rs) constructs a read-only `memoryview` via `PyMemoryView_FromMemory` over the `BatchSnapshot` slab without a lifetime anchor. It is valid only during the synchronous handler turn; call `bytes(request.body_view)` before retaining the payload across `await` points or background tasks.
- **Native vs Fallback Class Name Collisions**: [`vertebra._vertebra.AsgiScope`](../../../../src/rs/vertebra-py/src/lib.rs) and [`vertebra.server._asgi._wire.AsgiScope`](../../../../src/py/vertebra/server/_asgi/_wire.py) (as well as native vs fallback `WsgiEnviron` in [`_wsgi/_adapter.py`](../../../../src/py/vertebra/server/_wsgi/_adapter.py) and `Request` / `Response` in `vertebra.framework._router`) share class names. Import native request-path types from `vertebra._vertebra`; the Python classes are pure-Python fallbacks for unit tests and non-native callers.
- **`submit_raw_response` In-Process Behavior**: [`submit_raw_response`](../../../../src/rs/vertebra-py/src/transport.rs) transmits frames only when `SHM_TRANSPORT` is initialized; in in-process `BatchBridge` mode it returns the encoded `ResponseSlot` byte frames without submitting them. Use `Response` + `submit_response` / `submit_results` for in-process workers.

## 5. Stale PyO3 Extension Binaries & `bench` Dependency Isolation

- **Rebuild Extensions After Rust Edits**: `uv run pytest` imports compiled `.so` binaries from `.venv` (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`). Always run `make dev` (or `make py-test`) after modifying `vertebra-core`, `vertebra-py`, `vertebra-bundler`, or `vertebra-storage`.
- **Keep `bench` Dependencies Out of `dev`**: Keep `duckdb`, `spikard`, and `granian` inside `[dependency-groups].bench` in [`pyproject.toml`](../../../../pyproject.toml) so `uv run` on Python `3.14t` never compiles `duckdb` from source during normal test runs.

## 6. `AsyncLogCore` Batched Newline-Delimited JSONL Buffers vs Pure-Python Fallback

- **Symptom**: `assert len(log_sink.drain()) == 4` passes in pure-Python fallback tests but fails with `assert 1 == 4` when running with the compiled `_vertebra` extension in CI.
- **Root Cause**: The Rust native `AsyncLogCore` batches multiple JSONL records into a single `bytes` chunk (`b'{...}\n{...}\n{...}\n{...}\n'`) for zero-copy I/O efficiency, whereas the pure-Python `BoundedLogQueue` fallback appends separate single-record `bytes` items to a Python `collections.deque`.
- **Rule**: When asserting on drained audit logs in unit tests, always split chunks on line boundaries: `[line for chunk in log_sink.drain() for line in chunk.splitlines() if line.strip()]`.

## 7. TNS Descriptor Validation & Socket Teardown Timeouts

- **TNS Character Validation**: Oracle Connect Descriptors constructed via `OracleConnectDescriptor.to_tns_descriptor()` must strictly validate host, port, protocol, service, and SID inputs against safe identifier regexes (`^[A-Za-z0-9._-]+$`) to prevent TNS descriptor injection attacks.
- **Socket Teardown in Async Database Drivers**: In asyncio driver implementations (such as `AsyncRedisDriver` or loopback server mock harnesses), calling `writer.close()` must be paired with `asyncio.wait_for(writer.wait_closed(), timeout=...)` with a bounded timeout (e.g. 50 ms) to avoid hanging worker event loops during process shutdown.
