---
type: Guide
title: "PyO3 FFI, Embedded Globals & Python Runtime Gotchas"
description: "Footguns in Embedded PyO3 module initialization, task-free ASGI coroutine driving, and Python 3.14t dependency resolution."
status: stable
tags: [gotchas, pyo3, asgi, python, free-threaded]
sources:
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: pyproject.toml
---

# PyO3 FFI, Embedded Globals & Python Runtime Gotchas

## 1. Dual `.so` Split-Globals Trap in `Topology::Embedded`

- **Symptom**: `RuntimeError: transport not initialized; call init_transport() first` even though Rust initialized `BatchBridge` right before spawning the Python worker thread.
- **Root Cause**: `vertebra-server` (and `vertebra-http`) statically links `vertebra-py`, while `.venv/lib/python3.*/site-packages/vertebra/_vertebra.cpython-*.so` is a separate dynamically loadable shared object. If Python executes a normal file-backed `import vertebra._vertebra`, `dlopen` loads the `.so` from disk with a **second** copy of `static BRIDGE: OnceLock<BatchBridge>` that is uninitialized.
- **Rule**: In embedded Rust binaries ([`src/rs/vertebra-http/src/server.rs`](../../../../src/rs/vertebra-http/src/server.rs)), always call [`vertebra_py::register_inittab()`](../../../../src/rs/vertebra-py/src/lib.rs) (`pyo3::append_to_inittab!(_vertebra)`) *before* `Python::initialize()` and alias `sys.modules["vertebra._vertebra"] = sys.modules["_vertebra"]` before importing `vertebra.server._native`.

## 2. Task-Free `AsgiRunner` vs `asyncio.current_task()` (`_asgi/_runner.py`)

- **Symptom**: Third-party ASGI frameworks or middlewares (e.g., AnyIO `TaskGroup` or Starlette `BaseHTTPMiddleware`) raise `RuntimeError: no running task` under [`AsgiRunner`](../../../../src/py/vertebra/server/_asgi/_runner.py).
- **Root Cause**: By default (`VERTEBRA_ASGI_DIRECT_SEND=1`), `AsgiRunner` drives the root ASGI coroutine directly via `coro.send(None)` + [`_CoroResumer`](../../../../src/py/vertebra/server/_asgi/_runner.py) without wrapping each HTTP request in `asyncio.create_task()`. Inside `_drive_coro`, `asyncio.current_task()` returns `None`.
- **Rule**: Set `VERTEBRA_ASGI_DIRECT_SEND=0` when running ASGI middlewares or task groups that inspect `asyncio.current_task()`.

## 3. Stale PyO3 Extension Binaries After Rust Edits

- **Symptom**: Editing `src/rs/vertebra-core`, `src/rs/vertebra-py`, or `src/rs/vertebra-bundler` has no effect during `uv run pytest`, or new PyO3 methods raise `AttributeError`.
- **Root Cause**: `uv run pytest` imports the compiled `.so` extensions installed in `.venv` (`vertebra._vertebra` and `vertebra._bundler`); it does not invoke Cargo automatically.
- **Rule**: Always run `make py-test` (which depends on `dev`) or run `make dev` before invoking `uv run pytest` directly after any Rust modification.

## 4. Keep `bench` Dependencies Out of `dev` (`pyproject.toml`)

- **Rule**: Never move `duckdb`, `spikard`, or `granian` from `[dependency-groups].bench` into `[dependency-groups].dev` in [`pyproject.toml`](../../../../pyproject.toml), or `uv run` on Python `3.14t` will attempt to compile `duckdb` C++ from source during standard test runs.
