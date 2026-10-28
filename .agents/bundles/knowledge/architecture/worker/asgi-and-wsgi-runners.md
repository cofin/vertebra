---
type: Guide
title: "ASGI, RSGI & WSGI Runners"
description: "How the Python worker drives mounted ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI apps via Rust C-API PyIter_Send batch drivers, lazy scopes, and zero-suspension awaitables."
status: stable
tags: [architecture, worker, asgi, rsgi, wsgi, python]
sources:
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_asgi/_queues.py
  - resource: src/py/vertebra/server/_asgi/_wire.py
  - resource: src/py/vertebra/server/_rsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/rs/vertebra-py/src/asgi.rs
  - resource: src/rs/vertebra-py/src/rsgi/mod.rs
  - resource: src/rs/vertebra-py/src/rsgi/scope.rs
  - resource: src/rs/vertebra-py/src/rsgi/http.rs
  - resource: src/rs/vertebra-py/src/wsgi/environ.rs
  - resource: src/rs/vertebra-py/src/wsgi/runner.rs
---

# ASGI, RSGI & WSGI Runners

Applications mounted via `App.mount(prefix, app, protocol=...)` (or passed directly to `vtb run app --asgi`) are served by protocol adapters initialized in [`Worker`](../../../../../src/py/vertebra/server/worker.py) (`detect_interface` selects RSGI when `__rsgi__` is present, then ASGI or WSGI). Requests routed to a mount carry `MOUNT_HANDLER_BASE | mount_idx` for `O(1)` lookup (see [PyO3 FFI & Batch Dispatch](pyo3-ffi-and-batch-dispatch.md)). Litestar-specific fastpath and offload integration is detailed in [Litestar Integration](../../domains/extensions/litestar.md).

## ASGI 3.0 (`asgi.rs`, `_asgi/_runner.py`)

### Native `AsgiScope`, `AsgiProtocol`, and `drive_asgi_batch` (`asgi.rs`)

- [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi.rs) (`#[pyclass(mapping, immutable_type)]`, conditional `freelist = 128` on GIL builds) wraps `Arc<BatchSnapshot>` + index with a 20-slot inline cache (`KNOWN_KEYS`: `type`, `asgi`, `http_version`, `method`, `scheme`, `path`, `raw_path`, `query_string`, `root_path`, `headers`, `server`, `client`, `state`, `extensions`, and Litestar keys `app`, `litestar_app`, `path_params`, `route_handler`, `path_template`, `connection`), interned via `pyo3::intern!` (with read-only cached extension capability dicts frozen via PEP 814 `PyFrozenDict::empty` and singletons marked immortal on Python 3.15).
- [`PyEmptyAwaitable`](../../../../../src/rs/vertebra-py/src/asgi.rs) and [`PyValueAwaitable`](../../../../../src/rs/vertebra-py/src/asgi.rs) (`frozen, immutable_type`) complete immediately on `__next__` via `PyStopIteration` with zero event-loop yield.
- [`AsgiProtocol`](../../../../../src/rs/vertebra-py/src/asgi.rs) implements `receive` and `send` directly in Rust: `receive()` returns the `http.request` dict via `PyValueAwaitable` (and parks on a disconnect future only if called again after body delivery), while `send()` encodes `http.response.start` headers directly into compact binary bytes, handles `http.response.body` (single-chunk immediate submission when `more_body=False` or multi-chunk accumulation) and `http.response.pathsend` (`stream_file_chunks` with GIL released), and submits directly into the active SHM or in-process transport.
- [`drive_asgi_batch(app, batch, loop, root_path)`](../../../../../src/rs/vertebra-py/src/asgi.rs) steps each `coro = app(scope, proto.receive, proto.send)` once via CPython C-API `PyIter_Send(coro, None, &mut result)`:
  - `PYGEN_RETURN`: unary handler finished without suspending — **zero** `asyncio.Task` allocations; responses coalesce a single Dekker `notify_if_sleeping` wakeup at batch end.
  - `PYGEN_NEXT`: wraps `(coro, first_yielded, proto)` in a Rust `_SteppedCoroutine` (`__await__`, `__iter__`, `__next__`, `send`, `throw`, `close`) accepted natively by both `asyncio.BaseEventLoop.create_task` and `uvloop.Loop.create_task` without an intermediate Python wrapper frame.
  - `PYGEN_ERROR`: logs unhandled app exceptions and emits a `500` response if headers were not yet sent.

### `AsgiRunner` and `AsgiLifespan` (`_asgi/_runner.py`, `_asgi/_queues.py`, `worker.py`)

- [`AsgiLifespan`](../../../../../src/py/vertebra/server/_asgi/_runner.py) drives the ASGI 3.0 `lifespan` protocol (`lifespan.startup` -> `lifespan.startup.complete` / `lifespan.startup.failed` on worker entry, and `lifespan.shutdown` -> `lifespan.shutdown.complete` on worker exit). [`Worker.run`](../../../../../src/py/vertebra/server/worker.py) enters `AsgiLifespan` for every mounted ASGI application before serving requests and passes the shared `lifespan_state` dict into `AsgiRunner` so `scope["state"]` exposes lifespan-initialized resources. Apps that do not implement `lifespan` gracefully degrade to HTTP-only mode.
- [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) finalizes attached `LitestarPlugin` state (`ensure_finalized`), exposes `dispatch_app` (wrapping `__vertebra_fastpath__.dispatch` ahead of `app` when present), and routes unary HTTP batches directly to `_vertebra.drive_asgi_batch` on the active `uvloop` / `asyncio` loop.
- `AsgiReceive`, `AsgiSend`, `AsgiResponse`, and bounded `AsgiStreamQueuePair` (`_asgi/_queues.py`) back standalone `run_single` test invocations, WebSocket upgrades, and full-duplex streaming exchanges.

## RSGI 1.6 (`rsgi/`, `_rsgi/_adapter.py`)

- [`RsgiScope`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs) (`frozen, immutable_type`, conditional `freelist = 128`) stores `(Arc<BatchSnapshot>, index, root_path, OnceLock<Py<Headers>>)` with **zero** eager Rust `String` allocations on construction, slicing `path` / `root_path` lazily from the snapshot, caching `.headers` (`Headers`), and returning static `pyo3::intern!` strings for `proto`, `http_version`, `scheme`, and standard HTTP methods.
- [`RsgiReadyAwaitable`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) (`_RsgiReadyAwaitable`) is `frozen, immutable_type` (`&self` on `__await__` and `__next__`), eliminating `PyBorrowMutError` races when multiple threads await the cached singleton concurrently under free-threaded `3.14t`/`3.15t`.
- [`RsgiHttpProtocol`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) submits responses directly into the Rust SHM / `BatchBridge` transport inside `response_empty`, `response_str`, `response_bytes`, and `response_file` (releasing the GIL for file reads, and buffering for `take_response()` only when `direct_submit=False` in isolated unit tests).
- [`drive_rsgi_batch(app, batch, loop, root_path)`](../../../../../src/rs/vertebra-py/src/rsgi/mod.rs) resolves `getattr(app, "__rsgi__", app)` once per batch, steps each handler via `PyIter_Send`, completes non-suspending handlers in zero `asyncio.Task` allocations, promotes suspending handlers via `_SteppedCoroutine` onto `uvloop`, and coalesces Dekker response notifications at batch completion. [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py) delegates batch execution directly to `drive_rsgi_batch`.

## WSGI (PEP 3333) (`wsgi/`, `_wsgi/_adapter.py`)

- [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs) (`#[pyclass(mapping, immutable_type)]`, conditional `freelist = 128`) lazily materializes PEP 3333 keys (`REQUEST_METHOD`, `SCRIPT_NAME`, `PATH_INFO`, `QUERY_STRING`, `SERVER_NAME`, `SERVER_PORT`, `SERVER_PROTOCOL`, `CONTENT_TYPE`, `CONTENT_LENGTH`, `HTTP_*`, `wsgi.input` -> `WsgiInput`, `wsgi.file_wrapper` -> `WsgiFileWrapper`) using static `pyo3::intern!` lookups for common methods, schemes, protocols, and `HTTP_*` header names rather than runtime `PyString::intern`.
- [`WsgiAdapter`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) and `run_wsgi_callable` (`wsgi/runner.rs`) execute synchronous WSGI callables on the worker `ThreadPoolExecutor` and submit native `Response` frames.

## Python wire and synthetic helpers

[`_asgi/_wire.py`](../../../../../src/py/vertebra/server/_asgi/_wire.py) provides ASGI header/scope wire helpers and [`_wsgi/_adapter.py`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) builds `environ` mappings from synthetic test requests when native `.wsgi_environ()` is absent. Always import native FFI classes (`AsgiScope`, `WsgiEnviron`, `Request`, `Response`, `Headers`) from `vertebra._vertebra` for `isinstance` checks.
