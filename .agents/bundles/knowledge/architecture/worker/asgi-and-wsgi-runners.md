---
type: Guide
title: "ASGI, RSGI & WSGI Runners"
description: "How the Python worker runs mounted ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI apps over native AsgiScope, RsgiScope, and WsgiEnviron objects, including coroutine driving and Python fallbacks."
status: stable
tags: [architecture, worker, asgi, wsgi, python]
sources:
  - resource: src/py/vertebra/server/_native.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_asgi/_adapter.py
  - resource: src/py/vertebra/server/_asgi/_queues.py
  - resource: src/py/vertebra/server/_asgi/_scope.py
  - resource: src/py/vertebra/server/_asgi/_extensions.py
  - resource: src/py/vertebra/server/_rsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_environ.py
  - resource: src/py/vertebra/server/_wsgi/_extensions.py
  - resource: src/rs/vertebra-py/src/asgi_scope.rs
  - resource: src/rs/vertebra-py/src/asgi_bridge.rs
  - resource: src/rs/vertebra-py/src/rsgi_scope.rs
  - resource: src/rs/vertebra-py/src/rsgi_protocol.rs
  - resource: src/rs/vertebra-py/src/wsgi_environ.rs
  - resource: src/rs/vertebra-py/src/wsgi_worker.rs
---

# ASGI, RSGI & WSGI Runners

Apps mounted with `App.mount(prefix, app, protocol=...)` are served by protocol adapters. `run_worker` in [`_native.py`](../../../../../src/py/vertebra/server/_native.py) resolves `protocol="auto"` with `_detect_protocol` (RSGI if `is_rsgi_app`, then ASGI or WSGI) and builds one adapter per mount in `_instantiate_mount_runners`. Requests whose route is a mount land in `BatchDispatchPlan.mounts` (see [PyO3 FFI & Batch Dispatch](pyo3-ffi-and-batch-dispatch.md)). Litestar-specific acceleration lives in [Litestar Integration](../../domains/extensions/litestar.md).

## ASGI 3.0

### Native `AsgiScope` (`asgi_scope.rs`)

[`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi_scope.rs) (`#[pyclass(mapping, freelist = 128)]`) holds `Arc<BatchSnapshot>` plus an index and builds values on demand:

- A 20-slot inline cache keyed by `KNOWN_KEYS`: `type`, `asgi`, `http_version`, `method`, `scheme`, `path`, `raw_path`, `query_string`, `root_path`, `headers`, `server`, `client`, `state`, `extensions`, and the Litestar keys `app`, `litestar_app`, `path_params`, `route_handler`, `path_template`, `connection`. Keys are interned with `pyo3::intern!`.
- `extensions` advertises `http.response.pathsend`, `http.response.zerocopysend`, `http.response.trailers`, `http.response.early_hint`, `http.response.debug`, `http.response.sse`, `websocket.http.response`, and `tls`.
- Writes to known keys go to the inline cache; other keys go to a Rust `extras: HashMap<String, Py<PyAny>>`. A `deleted_known` bitmask tracks deletions. No backing Python dict is allocated.
- Mapping protocol: `__getitem__`, `__setitem__`, `__delitem__`, `__contains__`, `__len__`, `__iter__`, `get`, `setdefault`, `keys`, `values`, `items`, `update`, `__eq__`, and `copy` (returns a plain `dict`). `AsgiScope.from_parts` builds a scope for tests.

### `AsgiRunner` and coroutine driving (`_asgi/_runner.py`)

- [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) wraps [`AsgiAdapter`](../../../../../src/py/vertebra/server/_asgi/_adapter.py). Built with `loop=`, it uses that loop; built without one, it owns an `AsyncRuntime`. `run_worker` always passes the scheduler's loop.
- `spawn_batch(requests)` posts one `_BatchProcessor` per batch through `call_soon_threadsafe`. `_BatchProcessor` calls `_drive_coro` on each coroutine.
- `_drive_coro` sends into the coroutine once. If it finishes, no `asyncio.Task` exists. If it yields, `_handle_yielded` creates a Task running `_resume_coro_in_task`, which awaits each yielded future and resumes the coroutine with `send` or `throw`.
- `VERTEBRA_ASGI_DIRECT_SEND` is read once at import. `0`, `false`, `no`, `off`, or an empty value disables direct driving and uses `loop.create_task` for every coroutine.
- With an `AsgiEventLoopPool` of more than one loop, `_AsgiMountRunner` creates one `AsgiRunner` per loop and splits requests round-robin across them.
- Responses are built as native `Response` objects and completed per request with `submit_response`.

### Receive, send, and response shape (`_asgi/_queues.py`)

- `AsgiReceive.__call__` and `AsgiSend.__call__` are `async def` methods that never await, so a unary exchange completes without suspending. `AsgiReceive` returns `http.request` with the body copied to `bytes`, then `http.disconnect`.
- `AsgiSend` collects into an `AsgiResponse` struct (`status`, `headers`, `body`, `trailers`, `early_hints`, `file_path`, `file_range`, `zero_copy_view`, `debug_info`). `trailers` are collected but not forwarded to the native `Response`, which has no trailer field.
- `AsgiStreamQueuePair` provides bounded inbound and outbound queues (`DEFAULT_STREAM_QUEUE_MAXSIZE` 256, high and low watermarks 192 and 64).
- `stream_file_chunks` (`asgi_bridge.rs`) reads a file or range in chunks with the GIL released.

`_asgi/_extensions.py` defines `LitestarExtension`, `FastApiExtension`, `HeaderFilter`, and `register_extension`/`get_extensions`. Nothing calls `get_extensions` or an extension's `try_handle`, so registered extensions have no effect on dispatch.

## RSGI 1.6 (`_rsgi/_adapter.py`, `rsgi_scope.rs`, `rsgi_protocol.rs`)

- [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py) calls `startup(loop)` once at mount creation and exposes `run_single`, `handle_request`, `handle_http`, `run_batch`, and `shutdown`. `run_worker` schedules `handle_request` coroutines on the scheduler.
- `is_rsgi_app` and `detect_interface(app, configured="auto")` classify apps. `detect_interface` has no caller in the package.
- Native types: `RsgiScope` and `RsgiHeaders` (`get_all`), and `RsgiHttpProtocol`, `RsgiTransport`, and `RsgiWebsocketProtocol`. `RsgiHttpProtocol` releases the GIL for file metadata and range reads.

## WSGI (PEP 3333)

- [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi_environ.rs) (`#[pyclass(mapping, freelist = 128)]`) builds `REQUEST_METHOD`, `SCRIPT_NAME`, `PATH_INFO`, `QUERY_STRING`, `SERVER_NAME`, `SERVER_PORT`, `SERVER_PROTOCOL`, `CONTENT_TYPE`, `CONTENT_LENGTH`, `HTTP_*`, `wsgi.version`, `wsgi.url_scheme`, `wsgi.input` (native `WsgiInput`), `wsgi.errors`, `wsgi.multithread`, `wsgi.multiprocess`, `wsgi.run_once`, and `wsgi.file_wrapper` (native `WsgiFileWrapper`). Writes go to a Rust `extras` map.
- [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) runs the app synchronously in the calling thread. `run_worker` dispatches with `async_mode=True`, which submits each WSGI mount request to the shared `vertebra-wsgi` `ThreadPoolExecutor` (`max(128, cpu_count * 8)` threads) and completes it with `submit_response`. The blocking `async_mode=False` path uses `_dispatch_wsgi_concurrent`, which runs a single request inline.
- `run_environ` returns a one-chunk body without joining and joins multi-chunk bodies. A `WsgiFileWrapper` body is read with `read_all_bytes()` and tagged with `x-vertebra-sendfile`. It always calls `close()`, raises `WsgiError` on contract violations, and returns a native `Response`.
- `_wsgi/_extensions.py` adds `DjangoWsgiApp`, `FlaskWsgiApp`, `LargeBodyBypass`, `resolve_wsgi_file_wrapper`, `configure_django`, and `create_flask_app`. `WsgiRunner` (`_wsgi/_runner.py`) is exported but has no caller.
- `run_wsgi_callable` and `_WsgiStartResponse` (`wsgi_worker.rs`) invoke a WSGI app from Rust. No Python code calls them.

## Python fallback classes

`vertebra.server._asgi._scope.AsgiScope` (a `dict` subclass) and `vertebra.server._wsgi._environ.WsgiEnviron` (a `Mapping`) share names with the native classes. They are used only when the request object has no `asgi_scope` or `wsgi_environ` method. Import native types from `vertebra._vertebra` for `isinstance` checks.
