---
type: Guide
title: "ASGI, RSGI & WSGI Runners"
description: "How the Python worker drives mounted ASGI 3.0, RSGI 1.6, and PEP 3333 WSGI apps via Rust C-API PyIter_Send batch drivers, lazy scopes, and zero-suspension awaitables."
status: stable
tags:
  - architecture
  - worker
  - asgi
  - rsgi
  - wsgi
  - python
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-sync
    at: "2026-11-12T01:15:00Z"
    commit: c8722c76
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Python implementation module (src/py/vertebra/server/worker.py)"
  - id: src-server-loops-py
    resource: src/py/vertebra/server/_loops.py
    title: "Python implementation module (src/py/vertebra/server/_loops.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-server-fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: "Python implementation module (src/py/vertebra/server/_fastpath_core.py)"
  - id: src-server-fastpath-py
    resource: src/py/vertebra/server/_fastpath.py
    title: "Python implementation module (src/py/vertebra/server/_fastpath.py)"
  - id: src-server-asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py
    title: "Python implementation module (src/py/vertebra/server/_asgi/_runner.py)"
  - id: src-server-asgi-queues-py
    resource: src/py/vertebra/server/_asgi/_queues.py
    title: "Python implementation module (src/py/vertebra/server/_asgi/_queues.py)"
  - id: src-server-asgi-wire-py
    resource: src/py/vertebra/server/_asgi/_wire.py
    title: "Python implementation module (src/py/vertebra/server/_asgi/_wire.py)"
  - id: src-server-rsgi-adapter-py
    resource: src/py/vertebra/server/_rsgi/_adapter.py
    title: "Python implementation module (src/py/vertebra/server/_rsgi/_adapter.py)"
  - id: src-server-wsgi-adapter-py
    resource: src/py/vertebra/server/_wsgi/_adapter.py
    title: "Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py)"
  - id: src-vertebra-py-src-asgi-rs
    resource: src/rs/vertebra-py/src/asgi.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/asgi.rs)"
  - id: src-vertebra-py-src-rsgi-mod-rs
    resource: src/rs/vertebra-py/src/rsgi/mod.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/rsgi/mod.rs)"
  - id: src-vertebra-py-src-rsgi-scope-rs
    resource: src/rs/vertebra-py/src/rsgi/scope.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/rsgi/scope.rs)"
  - id: src-vertebra-py-src-rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs)"
  - id: src-vertebra-py-src-wsgi-environ-rs
    resource: src/rs/vertebra-py/src/wsgi/environ.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/wsgi/environ.rs)"
  - id: src-vertebra-py-src-wsgi-runner-rs
    resource: src/rs/vertebra-py/src/wsgi/runner.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs)"
---

# ASGI, RSGI & WSGI Runners

<!-- truth: start -->
- `drive_asgi_batch` and `drive_native_batch` step synchronous coroutine prefixes directly in Rust via `PyIter_Send`, promoting only suspended coroutines into `asyncio.Task` objects via `_SteppedCoroutine`.
- `_loops.py` resolves `LoopBackend` (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) via `_LOOP_BACKEND_SPECS`, probing `winloop` on `win32` and `uvloop` then `rloop` on `posix` before falling back to stdlib `asyncio`.
- `_fastpath_core.py` defines `FastPathProvider`, `DirectPlan`, and `QueryParamSpec` so `attach_framework_fastpath` resolves lazy framework adapters without importing unselected third-party frameworks.
- `drive_rsgi_batch` drives RSGI 1.6 scopes with frozen `_RsgiReadyAwaitable` singletons, and `drive_wsgi_batch` executes PEP 3333 apps over zero-copy `WsgiInput::from_snapshot` buffers and state-tracked `_WsgiStartResponse`.
<!-- truth: end -->

Applications mounted via `App.mount(prefix, app, protocol=...)` (or passed directly to `vtb run app --asgi`) are served by protocol adapters initialized in [`Worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] (`detect_interface` selects RSGI when `__rsgi__` is present, then ASGI or WSGI) on the event loop resolved by [`resolve_loop_backend`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py] (`_LOOP_BACKEND_SPECS` orders `winloop` on Windows and `uvloop` then `rloop` on POSIX ahead of `asyncio`). Requests routed to a mount carry `MOUNT_HANDLER_BASE | mount_idx` for `O(1)` lookup (see [PyO3 FFI & Batch Dispatch](pyo3-ffi-and-batch-dispatch.md)). Litestar-specific fastpath and offload integration is detailed in [Litestar Integration](../../domains/extensions/litestar.md).

## ASGI 3.0 (`asgi.rs`, `_asgi/_runner.py`)

### Native `AsgiScope`, `AsgiProtocol`, and `drive_asgi_batch` (`asgi.rs`)

- [`AsgiScope`](../../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs] (`#[pyclass(mapping, immutable_type)]`, conditional `freelist = 128` on GIL builds) wraps `Arc<BatchSnapshot>` + index with a 20-slot inline cache (`KNOWN_KEYS`: `type`, `asgi`, `http_version`, `method`, `scheme`, `path`, `raw_path`, `query_string`, `root_path`, `headers`, `server`, `client`, `state`, `extensions`, and Litestar keys `app`, `litestar_app`, `path_params`, `route_handler`, `path_template`, `connection`), interned via `pyo3::intern!` (with read-only cached extension capability dicts frozen via PEP 814 `PyFrozenDict::empty` and singletons marked immortal on Python 3.15). `AsgiScope.body_bytes()` and `AsgiProtocol.body_bytes()` return the request body `bytes` directly from `BatchSnapshot` without allocating an ASGI `http.request` dict or awaitable.
- [`PyEmptyAwaitable`](../../../../../src/rs/vertebra-py/src/asgi.rs) and [`PyValueAwaitable`](../../../../../src/rs/vertebra-py/src/asgi.rs) (`frozen, immutable_type`) complete immediately on `__next__` via `PyStopIteration` with zero event-loop yield.
- [`AsgiProtocol`](../../../../../src/rs/vertebra-py/src/asgi.rs) implements `receive`, `send`, `__call__` (allowing `proto_bound` itself to be passed as the `send` callable in `drive_asgi_targets_inline`), and `send_raw_response(status, headers, body)` directly in Rust: `receive()` returns the `http.request` dict via `PyValueAwaitable` (and parks on a disconnect future only if called again after body delivery), `send_raw_response()` encodes `(status, headers, body)` and submits in a single C-API call without intermediate `http.response.start` / `http.response.body` dicts, and `send()` handles standard multi-message ASGI responses and `http.response.pathsend`.
- [`drive_asgi_batch(app, batch, loop, root_path)`](../../../../../src/rs/vertebra-py/src/asgi.rs) and [`drive_native_batch(items, loop)`](../../../../../src/rs/vertebra-py/src/asgi.rs) step each coroutine once via CPython C-API `PyIter_Send(coro, None, &mut result)`:
  - `PYGEN_RETURN`: unary handler finished without suspending — **zero** `asyncio.Task` allocations; responses coalesce a single Dekker `notify_if_sleeping` wakeup at batch end.
  - `PYGEN_NEXT`: wraps `(coro, first_yielded, finalizer)` in a Rust `_SteppedCoroutine` (`ProtocolFinalizer::Asgi`, `ProtocolFinalizer::Rsgi`, or `ProtocolFinalizer::Native`) accepted natively by `asyncio`, `uvloop`, `winloop`, and `rloop` `create_task` without an intermediate Python wrapper frame.
  - `PYGEN_ERROR`: logs unhandled app exceptions and emits a `500` response if headers were not yet sent.

### `AsgiRunner`, `AsgiLifespan`, and `attach_framework_fastpath` (`_asgi/_runner.py`, `_asgi/_queues.py`, `_fastpath_core.py`, `_fastpath.py`, `worker.py`)

- [`AsgiLifespan`](../../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py] drives the ASGI 3.0 `lifespan` protocol (`lifespan.startup` -> `lifespan.startup.complete` / `lifespan.startup.failed` on worker entry, and `lifespan.shutdown` -> `lifespan.shutdown.complete` on worker exit). [`Worker.run`](../../../../../src/py/vertebra/server/worker.py) enters `AsgiLifespan` for every mounted ASGI application before serving requests and passes the shared `lifespan_state` dict into `AsgiRunner` so `scope["state"]` exposes lifespan-initialized resources. Apps that do not implement `lifespan` gracefully degrade to HTTP-only mode. A startup exception is logged (`"ASGI lifespan startup failed for mount %r"`) and swallowed, and RSGI `__rsgi_init__` failures are handled the same way, so the worker keeps serving the mount ([Server Runtime Limitations](../../domains/server/server-runtime-limitations.md)).
- [`_fastpath_core.py`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py] defines the `FastPathProvider` protocol (`name`, `detect(app) -> bool`, `build(app, *, mount_prefix="") -> Any`), the provider table helpers `register_fastpath_provider(provider, *, prepend=False)`, `unregister_fastpath_provider(name)`, `get_fastpath_providers()`, and `resolve_fastpath_provider(app)` (first provider whose `detect` returns `True`), built-in lazy providers for Litestar, FastAPI, Starlette, Flask, and Django whose `build` imports `vertebra.extensions.<framework>.fastpath` and returns `FastPath.from_app(app, mount_prefix=...)`, and shared `DirectPlan` / `QueryParamSpec` compiled route descriptors.
- [`attach_framework_fastpath(app, *, mount_prefix="", protocol="auto")`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] is invoked by `Worker._instantiate_mount_runners`, `AsgiRunner.__init__`, and `WsgiAdapter.__init__`. It returns apps already marked `__vertebra_fastpath_attached__` (or their wrapped `wsgi_app`) unchanged, wraps an app that is itself a `FastPath` (`wrap_asgi` + `dispatch`) directly, finalizes attached `VertebraPlugin` state, reuses an existing `FastPath` found on `__vertebra_fastpath__`, `fastpath`, `state["vertebra_fastpath_bypass"]`, or `extensions["vertebra"].fastpath`, and otherwise calls `provider.build(...)` and stores the result on `state["vertebra_fastpath_bypass"]` and `__vertebra_fastpath__`. It then binds `mount_prefix` and returns `FastPath.wrap_asgi(...)` or `FastPath.wrap_wsgi(...)` (each framework's `wrap_asgi` delegates to `wrap_asgi_fastpath`).
- [`AsgiRunner`](../../../../../src/py/vertebra/server/_asgi/_runner.py) finalizes attached `VertebraPlugin` state (`ensure_finalized`), exposes `dispatch_app` (wrapping `__vertebra_fastpath__` ahead of `app` when present), and routes unary HTTP batches directly to `_vertebra.drive_asgi_batch` on the active event loop.
- `AsgiReceive`, `AsgiSend` (including `send_raw_response`), `AsgiResponse`, and bounded `AsgiStreamQueuePair` (`_asgi/_queues.py`) back standalone `run_single` test invocations, WebSocket upgrades, and full-duplex streaming exchanges.

## RSGI 1.6 (`rsgi/`, `_rsgi/_adapter.py`)

- [`RsgiScope`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs)[^src-vertebra-py-src-rsgi-scope-rs] (`frozen, immutable_type`, conditional `freelist = 128`) stores `(Arc<BatchSnapshot>, index, root_path, OnceLock<Py<Headers>>)` with **zero** eager Rust `String` allocations on construction, slicing `path` / `root_path` lazily from the snapshot, caching `.headers` (`Headers`), and returning static `pyo3::intern!` strings for `proto`, `http_version`, `scheme`, and standard HTTP methods.
- [`RsgiReadyAwaitable`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs] (`_RsgiReadyAwaitable`) is `frozen, immutable_type` (`&self` on `__await__` and `__next__`), eliminating `PyBorrowMutError` races when multiple threads await the cached singleton concurrently under free-threaded `3.14t`/`3.15t`.
- [`RsgiHttpProtocol`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs) submits responses directly into the Rust SHM / `BatchBridge` transport inside `response_empty`, `response_str`, `response_bytes`, and `response_file` (releasing the GIL for file reads, and buffering for `take_response()` only when `direct_submit=False` in isolated unit tests).
- [`drive_rsgi_batch(app, batch, loop, root_path)`](../../../../../src/rs/vertebra-py/src/rsgi/mod.rs)[^src-vertebra-py-src-rsgi-mod-rs] resolves `getattr(app, "__rsgi__", app)` once per batch, steps each handler via `PyIter_Send`, completes non-suspending handlers in zero `asyncio.Task` allocations, promotes suspending handlers via `_SteppedCoroutine` onto the active event loop, and coalesces Dekker response notifications at batch completion. [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py)[^src-server-rsgi-adapter-py] delegates batch execution directly to `drive_rsgi_batch`.

## WSGI (PEP 3333) (`wsgi/`, `_wsgi/_adapter.py`)

- [`WsgiEnviron`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs)[^src-vertebra-py-src-wsgi-environ-rs] (`#[pyclass(mapping, immutable_type)]`, conditional `freelist = 128`) lazily materializes PEP 3333 keys (`REQUEST_METHOD`, `SCRIPT_NAME`, `PATH_INFO`, `QUERY_STRING`, `SERVER_NAME`, `SERVER_PORT`, `SERVER_PROTOCOL`, `CONTENT_TYPE`, `CONTENT_LENGTH`, `HTTP_*`, `wsgi.input` -> `WsgiInput`, `wsgi.file_wrapper` -> `WsgiFileWrapper`) directly from `Arc<BatchSnapshot>` using static `pyo3::intern!` lookups, a cached `WSGI_VERSION_TUPLE` (`(1, 0)`), and lazy `HTTP_*` header map construction only when `HTTP_*` keys are queried.
- [`WsgiInput`](../../../../../src/rs/vertebra-py/src/wsgi/input.rs) (`WsgiInput::from_snapshot`) reads request bodies zero-copy from `Arc<BatchSnapshot>` without copying payloads into an owned `Bytes` buffer.
- [`drive_wsgi_batch(app, batch, root_path)`](../../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs] and `_WsgiStartResponse` execute synchronous PEP 3333 callables (and non-suspending `async def` Flask/Django fastpath views) across an entire `RequestBatch` inline on the worker loop, exposing `_WsgiStartResponse.send_raw_response(status, headers, body)` so `Flask` and `Django` `FastPath.dispatch_wsgi` bypass `status_line` string formatting and `(str, str)` header list allocations, encoding `response_headers` directly into compact binary frames (`encode_py_headers_into`), zero-copying single-chunk `[b"..."]` list/tuple responses via `PreparedRawBody::PyBytes`, supporting the PEP 3333 `write(data)` callable and `exc_info` recovery before `headers_sent`, and flushing all responses in a single `ResponseSubmitter::submit_batch_raw_inner` call. [`WsgiAdapter.spawn_batch`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py)[^src-server-wsgi-adapter-py] and `single_root_wsgi` in [`Worker`](../../../../../src/py/vertebra/server/worker.py) auto-attach framework `FastPath` via `attach_framework_fastpath` and invoke `drive_wsgi_batch` directly without `ThreadPoolExecutor` per-request hop overhead.

## Python wire and synthetic helpers

[`_asgi/_wire.py`](../../../../../src/py/vertebra/server/_asgi/_wire.py)[^src-server-asgi-wire-py] provides ASGI header/scope wire helpers and [`_wsgi/_adapter.py`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) builds `environ` mappings from synthetic test requests when native `.wsgi_environ()` is absent. Always import native FFI classes (`AsgiScope`, `WsgiEnviron`, `Request`, `Response`, `Headers`) from `vertebra._vertebra` for `isinstance` checks.

Provenance sources: [^src-server-dispatch-py], [^src-server-asgi-queues-py].

[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Python implementation module (src/py/vertebra/server/_loops.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Python implementation module (src/py/vertebra/server/_fastpath_core.py).
[^src-server-fastpath-py]: `src/py/vertebra/server/_fastpath.py` — Python implementation module (src/py/vertebra/server/_fastpath.py).
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Python implementation module (src/py/vertebra/server/_asgi/_runner.py).
[^src-server-asgi-queues-py]: `src/py/vertebra/server/_asgi/_queues.py` — Python implementation module (src/py/vertebra/server/_asgi/_queues.py).
[^src-server-asgi-wire-py]: `src/py/vertebra/server/_asgi/_wire.py` — Python implementation module (src/py/vertebra/server/_asgi/_wire.py).
[^src-server-rsgi-adapter-py]: `src/py/vertebra/server/_rsgi/_adapter.py` — Python implementation module (src/py/vertebra/server/_rsgi/_adapter.py).
[^src-server-wsgi-adapter-py]: `src/py/vertebra/server/_wsgi/_adapter.py` — Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py).
[^src-vertebra-py-src-asgi-rs]: `src/rs/vertebra-py/src/asgi.rs` — Rust implementation module (src/rs/vertebra-py/src/asgi.rs).
[^src-vertebra-py-src-rsgi-mod-rs]: `src/rs/vertebra-py/src/rsgi/mod.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/mod.rs).
[^src-vertebra-py-src-rsgi-scope-rs]: `src/rs/vertebra-py/src/rsgi/scope.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/scope.rs).
[^src-vertebra-py-src-rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs` — Rust implementation module (src/rs/vertebra-py/src/rsgi/http.rs).
[^src-vertebra-py-src-wsgi-environ-rs]: `src/rs/vertebra-py/src/wsgi/environ.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/environ.rs).
[^src-vertebra-py-src-wsgi-runner-rs]: `src/rs/vertebra-py/src/wsgi/runner.rs` — Rust implementation module (src/rs/vertebra-py/src/wsgi/runner.rs).
