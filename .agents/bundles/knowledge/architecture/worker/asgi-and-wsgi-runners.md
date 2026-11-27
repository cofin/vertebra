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
  - by: agent:flow-executor
    at: "2026-11-13T06:06:00Z"
    commit: 05d88d00
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
  - id: src-vertebra-py-src-wsgi-pool-rs
    resource: src/rs/vertebra-py/src/wsgi/pool.rs
    title: "Rust adaptive WSGI executor and lazy thread pool (src/rs/vertebra-py/src/wsgi/pool.rs)"
---

# ASGI, RSGI & WSGI Runners

<!-- truth: start -->
- `drive_asgi_batch` and `drive_native_batch` step synchronous coroutine prefixes in Rust via `PyIter_Send`, promoting only suspended coroutines into `asyncio.Task` objects via `_SteppedCoroutine`.
- `_loops.py` resolves `LoopBackend` (`auto`, `uvloop`, `winloop`, `rloop`, `zuvloop`, `asyncio`) via `_LOOP_BACKEND_SPECS`, probing platform fast loops before falling back to stdlib `asyncio`.
- `_fastpath_core.py` defines `CompiledRouteBase`, `FastPathProvider`, `DirectPlan`, and `QueryParamSpec` so `attach_framework_fastpath` resolves lazy framework adapters and `__vertebra_fastpath__` without importing unselected frameworks.
- `AsgiProtocol` and `RsgiHttpProtocol` stream chunks progressively via `submit_stream_frame` while `AsgiRunner.__call__` stays buffered; ASGI lifespan and RSGI `__rsgi_init__` failures raise `WorkerStartupError` (`EXIT_STARTUP_FAILURE = 78`).
- `drive_rsgi_batch` drives RSGI 1.6 scopes with frozen `_RsgiReadyAwaitable` singletons, shared `RsgiScope` metadata, and lazy `RsgiHttpProtocol` initialization; `drive_wsgi_batch` routes PEP 3333 batches through the adaptive `WsgiExecutor` (`2,000 µs` stall threshold, `cpus * 12` clamped `64..128` lazy threads) over shared `WsgiServerConfig` and zero-copy `WsgiInput::from_snapshot` buffers.
<!-- truth: end -->

Mounted applications (`App.mount(prefix, app, protocol=...)` or `vtb run app --asgi`) run through protocol adapters created in [`Worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] (`detect_interface` checks `__rsgi__`, then ASGI, then WSGI) on the loop selected by [`resolve_loop_backend`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py]. Mounts resolve in `O(1)` via `MOUNT_HANDLER_BASE | mount_idx` (see [PyO3 FFI & Batch Dispatch](pyo3-ffi-and-batch-dispatch.md) and [Framework Extensions](../../domains/extensions/litestar.md)).

## ASGI 3.0 (`asgi.rs`, `_asgi/_runner.py`)

### Rust `AsgiScope`, `AsgiProtocol` & `drive_asgi_batch` ([`asgi.rs`](../../../../../src/rs/vertebra-py/src/asgi.rs)[^src-vertebra-py-src-asgi-rs])

- **`AsgiScope`**: Lazy mapping (`immutable_type`, conditional `freelist = 128` on GIL builds) over `Arc<BatchSnapshot>` with a 20-slot inline cache (`KNOWN_KEYS`, interned via `pyo3::intern!`, using `PyFrozenDict::empty` and immortal singletons on Python 3.15) tracked by a `u32` populated-slot bitmask so `Drop` decrements only materialized slots via `trailing_zeros()` instead of scanning all 20 slots. `body_bytes()` reads directly from `BatchSnapshot` without allocating an ASGI request dict.
- **Immediate Awaitables**: `EmptyAwaitable` and `ValueAwaitable` (`frozen, immutable_type`) complete on `__next__` via `PyStopIteration` without yielding to the event loop.
- **`AsgiProtocol`**: Implements `receive`, `send`, `__call__`, and `send_raw_response(status, headers, body)` in Rust. `receive()` returns `http.request` via `ValueAwaitable` and parks on a disconnect future only on subsequent reads. `send_raw_response()` bypasses intermediate ASGI dicts; `send()` streams progressive chunks via `submit_stream_frame` (and `http.response.pathsend`), emitting `STREAM_FLAG_ERROR` on mid-stream failure.
- **Coroutine Stepping (`drive_asgi_batch`, `drive_native_batch`)**: Steps each coroutine once with `PyIter_Send(coro, None, &mut result)`:
  - `PYGEN_RETURN`: completes with zero `asyncio.Task` allocations and one coalesced Dekker wakeup at batch end.
  - `PYGEN_NEXT`: wraps `(coro, first_yielded, finalizer)` in Rust `_SteppedCoroutine`, scheduled directly via `loop.create_task`.
  - `PYGEN_ERROR`: logs unhandled exceptions and emits `500` if headers were not sent.

### `AsgiRunner`, `AsgiLifespan` & Fastpath Attachment

- **Lifespan**: [`AsgiLifespan`](../../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py] runs ASGI `lifespan.startup` on worker entry and `lifespan.shutdown` on exit, passing `lifespan_state` into `scope["state"]`. Apps without lifespan degrade to HTTP-only; startup failures (and RSGI `__rsgi_init__` exceptions) raise `WorkerStartupError` (exit code `78`).
- **Lazy Fastpath Registry**: [`_fastpath_core.py`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py] defines `FastPathProvider`, `get_attached_fastpath` / `set_attached_fastpath` (falling back to `app.state` for slotted apps), and lazy providers for Litestar, FastAPI, Starlette, Flask, and Django. [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] finalizes `VertebraPlugin` state and attaches matching fastpaths, checking exact `type(resp) in BUFFERED_RESPONSE_TYPES and resp.background is None` before falling back to wrapped ASGI.
- **Test & Streaming Queues**: `AsgiReceive`, `AsgiSend`, `AsgiResponse`, and bounded `AsgiStreamQueuePair` ([`_asgi/_queues.py`](../../../../../src/py/vertebra/server/_asgi/_queues.py)[^src-server-asgi-queues-py]) support `run_single`, WebSocket upgrades, and full-duplex streaming.

## RSGI 1.6 (`rsgi/`, `_rsgi/_adapter.py`)

- **`RsgiScope`** ([`rsgi/scope.rs`](../../../../../src/rs/vertebra-py/src/rsgi/scope.rs)[^src-vertebra-py-src-rsgi-scope-rs]): Frozen `immutable_type` storing `(Arc<BatchSnapshot>, index, shared Arc<RsgiScopeMeta>, OnceLock<Py<Headers>>)` with zero eager string allocations; slices `path`/`root_path` lazily and returns interned constants for `proto`, `http_version`, `scheme`, and standard methods.
- **`RsgiReadyAwaitable` & Lazy `RsgiHttpProtocol`** ([`rsgi/http.rs`](../../../../../src/rs/vertebra-py/src/rsgi/http.rs)[^src-vertebra-py-src-rsgi-http-rs]): `_RsgiReadyAwaitable` is frozen (`&self` on `__await__`/`__next__`) so concurrent awaits under free-threaded Python never race on mutable borrows. `RsgiHttpProtocol` lazily initializes buffered/streaming state only on demand and submits directly into SHM/`BatchBridge` on `response_empty`, `response_str`, `response_bytes`, and `response_file` (detaching the GIL for file reads; buffering only when `direct_submit=False` in tests).
- **`drive_rsgi_batch`** ([`rsgi/mod.rs`](../../../../../src/rs/vertebra-py/src/rsgi/mod.rs)[^src-vertebra-py-src-rsgi-mod-rs]): Resolves `getattr(app, "__rsgi__", app)` once per batch, shares one `Arc<RsgiScopeMeta>` across the batch, steps handlers via `PyIter_Send`, promotes suspending coroutines via `_SteppedCoroutine`, and coalesces Dekker wakeups. [`RsgiAdapter`](../../../../../src/py/vertebra/server/_rsgi/_adapter.py)[^src-server-rsgi-adapter-py] delegates directly to it.

## WSGI / PEP 3333 (`wsgi/`, `_wsgi/_adapter.py`)

- **`WsgiEnviron`, `WsgiServerConfig` & `WsgiInput`** ([`wsgi/environ.rs`](../../../../../src/rs/vertebra-py/src/wsgi/environ.rs)[^src-vertebra-py-src-wsgi-environ-rs]): Lazy PEP 3333 mapping over `Arc<BatchSnapshot>` backed by a batch-shared `Arc<WsgiServerConfig>` (pre-interning `SERVER_NAME`, `SERVER_PORT`, `SCRIPT_NAME`, and `wsgi.url_scheme`), interned keys, cached `WSGI_VERSION_TUPLE`, and on-demand `HTTP_*` construction. `WsgiInput::from_snapshot` reads request bodies zero-copy from the snapshot.
- **Adaptive `WsgiExecutor`, `drive_wsgi_batch` & `_WsgiStartResponse`** ([`wsgi/pool.rs`](../../../../../src/rs/vertebra-py/src/wsgi/pool.rs)[^src-vertebra-py-src-wsgi-pool-rs], [`wsgi/runner.rs`](../../../../../src/rs/vertebra-py/src/wsgi/runner.rs)[^src-vertebra-py-src-wsgi-runner-rs]): [`WsgiExecutor`](../../../../../src/rs/vertebra-py/src/wsgi/pool.rs)[^src-vertebra-py-src-wsgi-pool-rs] keeps zero-I/O PEP 3333 handlers (and Flask/Django fastpaths) on the inline batch path while automatically spilling blocking I/O batches into a lazy Rust worker thread pool:
  - `VERTEBRA_WSGI_MODE`: `"auto"` (default on GIL builds: adaptive inline-to-pool fallback), `"concurrent"` (default on free-threaded `Py_GIL_DISABLED` builds: always dispatch to the Rust thread pool), or `"inline"` (strictly inline).
  - `VERTEBRA_WSGI_THREADS`: defaults to `available_cpus * 12` clamped to `64..=128`, spawning OS worker threads lazily on first stall so zero-I/O apps pay zero thread overhead.
  - `VERTEBRA_WSGI_STALL_US`: defaults to `2_000` (`2 ms`); inline execution timestamps once per item (reusing `item_end` as the next `item_start`) and switches the remainder of the batch plus subsequent batches to the concurrent pool when an item exceeds the stall threshold, decaying back to inline once latency clears.
  - `_WsgiStartResponse.send_raw_response(status, headers, body)` lets Flask/Django fastpaths bypass status-line formatting and header-list allocation; single-chunk `[b"..."]` bodies submit zero-copy via `PreparedRawBody::PyBytes` through `ResponseSubmitter::submit_batch_flushed` on inline batches (and `submit_raw_inner` from concurrent worker threads) while preserving PEP 3333 `write()` and pre-header `exc_info`. [`WsgiAdapter.spawn_batch`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py)[^src-server-wsgi-adapter-py] and `single_root_wsgi` invoke `drive_wsgi_batch` directly without Python `ThreadPoolExecutor` hops.

## Wire Helpers & `isinstance` Rule

[`_asgi/_wire.py`](../../../../../src/py/vertebra/server/_asgi/_wire.py)[^src-server-asgi-wire-py] provides ASGI wire helpers, and [`_wsgi/_adapter.py`](../../../../../src/py/vertebra/server/_wsgi/_adapter.py) constructs synthetic `environ` dicts for unit tests. Always import native FFI classes (`AsgiScope`, `WsgiEnviron`, `Request`, `Response`, `Headers`) from `vertebra._vertebra` for `isinstance` checks.

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
[^src-vertebra-py-src-wsgi-pool-rs]: `src/rs/vertebra-py/src/wsgi/pool.rs` — Rust adaptive WSGI executor and lazy thread pool (src/rs/vertebra-py/src/wsgi/pool.rs).
