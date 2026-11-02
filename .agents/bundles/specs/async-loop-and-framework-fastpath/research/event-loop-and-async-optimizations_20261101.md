---
type: Research
research_id: "event-loop-and-async-optimizations-20261101"
title: "Primary-Source Audit: Event Loop Backends (uvloop, winloop, rloop) and Multi-Framework C-API Zero-Copy Async Execution"
description: "Audit of uvloop 0.23.0, winloop 0.8.1, rloop 0.5.0, Python 3.14/3.15 EventLoopPolicy deprecations, Rust C-API PyIter_Send native coroutine stepping, per-loop RustCompletionRing + Notifier in vertebra-storage and PyDatabasePool, and unified 6-framework stacked FastPath dispatch."
scope: architecture
status: stable
state: promoted
promoted_to: async-loop-and-framework-fastpath
tags: [research, server, pyo3, asgi, wsgi, fastpath, uvloop, winloop, rloop]
generated:
  by: flow/0.26.0
  at: 2026-11-01T23:33:00Z
verified:
  - by: agent:researcher
    at: 2026-11-01T23:33:00Z
sources:
  - id: loops
    resource: src/py/vertebra/server/_loops.py::create_event_loop
    title: Event loop creation and EventLoopPool
  - id: gil-audit
    resource: src/py/vertebra/server/_gil_audit.py::GilAuditReport
    title: Free-threaded GIL audit and module quarantine
  - id: dispatch
    resource: src/py/vertebra/server/_dispatch.py::compile_handler_spec
    title: Native route compilation and batch dispatch
  - id: asgi-rs
    resource: src/rs/vertebra-py/src/asgi.rs::SteppedCoroutine
    title: Rust C-API PyIter_Send ASGI coroutine stepper
  - id: wsgi-rs
    resource: src/rs/vertebra-py/src/wsgi/runner.rs::_WsgiStartResponse
    title: Rust PEP 3333 WSGI batch runner
  - id: storage-rs
    resource: src/rs/vertebra-storage/src/lib.rs::spawn_rust_task
    title: Rust async storage task bridge
  - id: sync-tools
    resource: src/py/vertebra/utils/sync_tools.py::SyncPool
    title: Worker thread pool and LoopShardedResource async bridge
  - id: db-registry
    resource: src/py/vertebra/db/_registry.py::DatabaseRegistry
    title: Native database driver registry and URL parser
  - id: litestar-fp
    resource: src/py/vertebra/extensions/litestar/fastpath.py::LitestarFastPathBypass
    title: Litestar direct fastpath dispatcher
  - id: fastapi-fp
    resource: src/py/vertebra/extensions/fastapi/fastpath.py::FastAPIFastPath
    title: FastAPI direct fastpath dispatcher
  - id: starlette-fp
    resource: src/py/vertebra/extensions/starlette/fastpath.py::StarletteFastPath
    title: Starlette direct fastpath dispatcher
  - id: flask-fp
    resource: src/py/vertebra/extensions/flask/fastpath.py::FlaskFastPath
    title: Flask direct fastpath dispatcher
  - id: django-fp
    resource: src/py/vertebra/extensions/django/fastpath.py::DjangoFastPath
    title: Django direct fastpath dispatcher
---

# Primary-Source Audit: Event Loop Backends & Multi-Framework C-API Zero-Copy Async Execution

## 1. Event Loop Ecosystem Matrix (`uvloop`, `winloop`, `rloop`, `asyncio`)

- **`uvloop` (`v0.23.0`)**: Ships official wheels for Python 3.12–3.15 including free-threaded `cp314t` and `cp315t` (`#758`), adds `eager_start` support in `loop.create_task()` (`#748`, previously ignored in `<=0.22.1`), and upgrades `libuv` to `1.52.1`.
- **`winloop` (`v0.8.1`)**: Windows `libuv`/IOCP port of `uvloop` (~5x faster than Windows stdlib event loop) with wheels for `cp312`–`cp314` and `cp314t`. Currently imported in `src/py/vertebra/server/_loops.py::create_event_loop` on `win32` but missing from `pyproject.toml`.
- **`rloop` (`v0.5.0`)**: Pure-Rust (`PyO3` + `mio`) event loop with wheels for `cp312`–`cp315t` on Linux and macOS. Alpha status (lacks SSL/TLS, Unix Domain Sockets, and `sendfile`), making it suitable as an opt-in backend (`VERTEBRA_LOOP=rloop` / `--loop rloop`) for workloads where all socket/TLS I/O is offloaded to `vertebra-http`.
- **Python 3.14/3.15 `EventLoopPolicy` Deprecation**: `src/py/vertebra/server/_gil_audit.py::GilAuditReport.quarantine_unsafe_modules` calls `asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())`, which triggers `DeprecationWarning` on Python 3.14/3.15 (slated for removal in 3.16) and is a functional no-op because `create_event_loop()` calls `uvloop.new_event_loop()` directly.

## 2. Native `async def` C-API `PyIter_Send` Stepping (`drive_native_batch`)

- `src/rs/vertebra-py/src/asgi.rs::drive_asgi_targets_inline` and `src/rs/vertebra-py/src/rsgi/http.rs::drive_rsgi_targets_inline` step coroutines once in C via `ffi::PyIter_Send(coro.as_ptr(), ffi::Py_None(), &mut yielded)` and flush `PYGEN_RETURN` results via `ResponseSubmitter::submit_batch_raw_inner` with zero `asyncio.Task` or `Future` allocations.
- By contrast, `src/py/vertebra/server/_dispatch.py::compile_handler_spec` wraps `async def` handlers with parameters in an outer `_invoke_compiled_async` coroutine frame even when all extractors/guards/dependencies are synchronous, and `dispatch_batch` wraps them again in `_await_async_item` + `_AsyncErrorStub` + `loop.create_task()`.

## 3. Per-Loop `RustCompletionRing` + `Notifier` for `_storage` and `SyncPool` / Native Async Drivers

- `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task` currently acquires the GIL on Tokio worker threads (`Python::attach`) and calls `loop.call_soon_threadsafe` (1 self-pipe `write`/`read` syscall per operation), while `src/py/vertebra/utils/sync_tools.py::SyncPool.submit_async` dispatches through `loop.run_in_executor` (triggering per-operation `call_soon_threadsafe` wakeups across `AsyncPostgreSQLDriver`, `AsyncSQLiteDriver`, `AsyncMySQLDriver`, `AsyncOracleDriver`, `AsyncMSSQLDriver`, `AsyncDuckDBDriver`, and `SHMBrokerTransport.acquire_async`).
- Replacing per-op `call_soon_threadsafe` with a per-loop MPSC completion queue + single `vertebra_core::notify::Notifier` (`eventfd`/`kqueue`) registered once via `loop.add_reader` eliminates Tokio-thread GIL acquisition and coalesces N concurrent completions into a single wakeup and batched GIL turn.

## 4. Unified 6-Framework Zero-Copy `FastPath`, Stacked Mounts (`App.mount`) & Shared Native Database Pool

- **Litestar, FastAPI, and Starlette (`fastpath.py`)**: `wrap_asgi` allocates `buffered_messages = []`, `_buffering_receive`, and `_tracked_send` closures before checking route match, then constructs two ASGI dicts (`http.response.start`, `http.response.body`) and awaits `send()` twice. `FastAPI` also unconditionally instantiates `starlette.requests.Request` even when unused and dispatches sync `def` endpoints via `anyio.to_thread.run_sync(lambda: ...)`.
- **Flask and Django (`fastpath.py`)**: Both bail out (`return None`) on `async def` views, and `dispatch_wsgi` decodes response headers back to `latin-1` strings, formats `f"{status} OK"`, calls `start_response`, and wraps `[bytes(resp.body)]`. `Django` `MsgspecView.as_view` also re-instantiates `cls()` on every request when `initkwargs` is empty.
- **Multi-Framework Stacking (`App.mount`) & Shared Native Database Pool (`DatabaseRegistry.default()`)**:
  - `_instantiate_mount_runners` in `src/py/vertebra/server/worker.py` wraps mounted sub-applications in `AsgiRunner` or `WsgiAdapter` without attaching their framework `FastPath` or stripping `mount_prefix` during `FastPath` route matching.
  - When multiple mounted frameworks declare native database adapters pointing to the same database endpoint (e.g., `Litestar` `PostgreSQLAsyncConfig` / `SQLAlchemyAsyncConfig("postgresql+vtb://...")` + `Django` `vertebra.extensions.django.db.backends.postgresql`), each previously created an isolated `LeaseBroker`, separate `CloudSQLDialer`/`AlloyDBDialer` instances, and separate native driver connection pools. Adding `pool_fingerprint()` and `DatabaseRegistry.default()` (`default_broker()`, `get_shared_driver`, `get_shared_sync_driver`) allows all native drivers across all 6 frameworks to share the same connection pool, cloud dialer, SHM `LeaseBroker`, and `ShmStatementCache`.
