---
name: worker-asgi-and-wsgi-runners
description: Task-free AsgiRunner coroutine driver, slab-backed AsgiScope and WsgiEnviron wrappers, and synchronous WsgiAdapter.
sources:
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_asgi/_scope.py
  - src/py/vertebra/server/_wsgi/_adapter.py
  - src/rs/vertebra-py/src/asgi_scope.rs
  - src/rs/vertebra-py/src/wsgi_environ.rs
---

# Task-Free ASGI & Sync WSGI Runners

Vertebra bridges third-party ASGI 3.0 (Litestar, Starlette, FastAPI) and PEP 3333 WSGI (Flask, Django) applications into its Rust HTTP ingress and vectorized batch pipeline via [`server/_asgi/`](../../../../../src/py/vertebra/server/_asgi/__init__.py), [`server/_wsgi/`](../../../../../src/py/vertebra/server/_wsgi/__init__.py), and native PyO3 scope builders in [`asgi_scope.rs`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) and [`wsgi_environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs).

## 1. Native PyO3 `AsgiScope` (`vertebra-py/src/asgi_scope.rs`)

[`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) (`#[pyclass]` exported as `vertebra._vertebra.AsgiScope` with [`AsgiScopeIterator`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs)):

- Constructs an ASGI 3.0 HTTP scope in Rust from a `RequestSlot` and `Arc<DataSlab>` (`type`, `asgi`, `http_version`, `method`, `scheme`, `path`, `raw_path`, `query_string`, `root_path`, `headers`, `server`).
- Implements the Python mapping protocol (`__getitem__`, `__setitem__`, `__contains__`, `get`, `keys`, `values`, `items`, `__len__`, `__iter__`, `copy`) with a lazy copy-on-write `overrides: Option<Py<PyDict>>` dict so unmutated scopes never allocate a Python dictionary.

## 2. Task-Free `AsgiRunner` Coroutine Driver (`_asgi/_runner.py`, `_asgi/_queues.py`)

[`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) drives ASGI applications on the worker's `asyncio` event loop with two execution paths:

1. **Default Task-Free Direct Driver (`_drive_coro` + `_CoroResumer`)**:
   - Steps `coro.send(None)` directly without `asyncio.create_task()`.
   - [`AsgiReceive`](../../../../../src/py/vertebra/server/_asgi/_queues.py) and [`AsgiSend`](../../../../../src/py/vertebra/server/_asgi/_queues.py) (`_CompletedAwaitable`) complete standard `http.request`, `http.response.start`, and `http.response.body` exchanges in 0 suspension steps.
   - If the handler awaits a real `asyncio.Future`, `_drive_coro` attaches a lightweight [`_CoroResumer`](../../../../../src/py/vertebra/server/_asgi/_runner.py) callback via `future.add_done_callback` that resumes `coro.send(res)` or `coro.throw(exc)`.
2. **Task-Compatible Fallback (`VERTEBRA_ASGI_DIRECT_SEND=0`)**:
   - Frameworks or middlewares that call `asyncio.current_task()` (such as AnyIO task groups or Starlette `BaseHTTPMiddleware`) require an active `asyncio.Task` context. Setting `VERTEBRA_ASGI_DIRECT_SEND=0` routes execution through `loop.create_task`.

## 3. Native `WsgiEnviron` & Synchronous `WsgiAdapter` (`wsgi_environ.rs`, `_wsgi/_adapter.py`)

- **[`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs)**: Builds a PEP 3333 environment in Rust (`REQUEST_METHOD`, `SCRIPT_NAME`, `PATH_INFO`, `QUERY_STRING`, `CONTENT_TYPE`, `CONTENT_LENGTH`, `HTTP_*` headers, `wsgi.input`, `wsgi.errors`) with lazy `overrides` dict fallback.
- **[`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py)**: Invokes `app(environ, start_response)` synchronously on the shared `vertebra-wsgi` `ThreadPoolExecutor`, concatenates body chunks, calls `result.close()` per PEP 3333, and returns a native `Response`.
