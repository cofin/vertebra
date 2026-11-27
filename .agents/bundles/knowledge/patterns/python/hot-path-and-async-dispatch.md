---
type: Pattern
title: "Hot-Path Caching & Single-Wake Async Dispatch"
description: "Hot-path request dispatch patterns covering startup signature caching, inline vectorized sync batches, LoopBackend Dekker polling, Rust ASGI/RSGI drivers, and zero-copy response coercion."
status: stable
tags:
  - patterns
  - python
  - performance
  - batch-dispatch
  - async
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Python implementation module (src/py/vertebra/server/worker.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-server-loops-py
    resource: src/py/vertebra/server/_loops.py
    title: "Python implementation module (src/py/vertebra/server/_loops.py)"
  - id: src-server-fastpath-py
    resource: src/py/vertebra/server/_fastpath.py
    title: "Python implementation module (src/py/vertebra/server/_fastpath.py)"
  - id: src-server-fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: "Shared FastPathProvider registry and direct route compilation core (src/py/vertebra/server/_fastpath_core.py)"
  - id: src-server-asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py
    title: "Python implementation module (src/py/vertebra/server/_asgi/_runner.py)"
  - id: src-server-rsgi-adapter-py
    resource: src/py/vertebra/server/_rsgi/_adapter.py
    title: "Python implementation module (src/py/vertebra/server/_rsgi/_adapter.py)"
  - id: src-server-wsgi-adapter-py
    resource: src/py/vertebra/server/_wsgi/_adapter.py
    title: "Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py)"
  - id: src-utils-sync-tools-py
    resource: src/py/vertebra/utils/sync_tools.py
    title: "Python implementation module (src/py/vertebra/utils/sync_tools.py)"
  - id: src-extensions-litestar-fastpath-py
    resource: src/py/vertebra/extensions/litestar/fastpath.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py)"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Rust Request PyO3 binding and direct Request::body access (src/rs/vertebra-py/src/request.rs)"
---

# Hot-Path Caching & Single-Wake Async Dispatch

<!-- truth: start -->
- Route and framework handlers compile signatures at startup (`HandlerSpec`, `DirectPlan`, `FastPathProvider`), specializing 1-argument simple extractors (`request`/`req`, `body`, `headers`, `method`, `path`, raw `query`) in `compile_handler_spec` without `inspect.signature()` on the request path.
- Synchronous and mixed sync/async `native_only` routes bypass `partition_batch` in a single pass, flushing completed replies via `submit_batch_raw` and stepping async coroutines via `drive_native_batch` without `ThreadPoolExecutor` hops.
- `Worker._run_inline_event_loop` splits `_on_notify_readable` and `_drain_batches` with a 1-turn `call_soon` grace check before re-arming Dekker `prepare_request_sleep` / `cancel_request_sleep` barriers on `loop.add_reader(notify_fd)`.
- `Request::body` returns Python `bytes` directly from the underlying `BatchSnapshot` slice, and suspended `async_native` coroutines drop `Request` references before `await` so in-flight I/O never pins `Arc<BatchSnapshot>` slabs in `BufferPool`.
<!-- truth: end -->

Python worker code on the request hot path ([`worker.py`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py], [`_dispatch.py`](../../../../../src/py/vertebra/server/_dispatch.py)[^src-server-dispatch-py], [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py], [`_asgi/_runner.py`](../../../../../src/py/vertebra/server/_asgi/_runner.py)[^src-server-asgi-runner-py]) avoids per-request reflection, GIL-thrashing thread pools, per-request cross-thread wakeups, and unnecessary `asyncio.Task` allocations.

## 1. Startup Signature Caching (`HandlerSpec` / `compile_handler_spec`)

Never call `inspect.signature()` on the request path. [`compile_handler_spec(handler, sync_to_thread=...)`](../../../../../src/py/vertebra/server/_dispatch.py) (pre-compiled at worker startup via `compile_handler_table(handlers)`) compiles a `HandlerSpec` and specialized invoker closure once, including direct 1-argument simple extractors for built-in request accessors (`request`/`req`, `body`, `headers`, `method`, `path`, raw `query` — excluding handlers requiring parsed path or query maps) and zero-argument handlers that avoid `dict`/`kwargs` packing on the hot path:

```python
class HandlerSpec(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Pre-compiled dispatch specification and invoker for a route handler."""

    has_params: bool
    is_async: bool
    param_names: tuple[str, ...]
    invoker: Callable[..., Any]
    sync_to_thread: bool = False
    status_code: int = 200
    arg_resolver: Callable[[Any], tuple[Any, ...]] | None = None

def compile_handler_table(handlers: Mapping[int, Callable[..., Any]]) -> dict[int, HandlerSpec]:
    """Pre-compile dispatch specs for all registered handler callables."""
    return {hid: compile_handler_spec(fn) for hid, fn in handlers.items()}
```

## 2. Single-Pass `native_only` Dispatch, Split Inline Drain & `O(1)` Mounts

- **Single-Pass `native_only` & Inline Vectorized Sync Execution (`"sync_inline"`)**: Non-async native handlers default to `sync_to_thread=False`. When no mounted apps or `sync_to_thread=True` handlers are registered, [`Worker._process_batch`](../../../../../src/py/vertebra/server/worker.py) sets `native_only=True` and routes pure-sync and mixed sync/async native batches in a single pass—bypassing `partition_batch` bucket allocation, flushing sync completions through one `submit_batch_raw(raw_items)` call, and handing async items straight to `drive_native_batch` (`0` `ThreadPoolExecutor` submissions). Only handlers explicitly opted into `sync_to_thread=True` (`__vertebra_sync_to_thread__`) allocate the lazy `ThreadPoolExecutor` (`max_workers = max(4, min(32, cpu_count))` on GIL builds).
- **Split `_on_notify_readable` / `_drain_batches` & Dekker Polling**: [`create_event_loop()`](../../../../../src/py/vertebra/server/_loops.py) resolves `LoopBackend` via the table-driven `_LOOP_BACKEND_SPECS` registry (`auto` -> `uvloop>=0.23.0` on POSIX, `winloop>=0.8.1` on Windows, opt-in `zuvloop>=0.0.17` on Python 3.14+ and `rloop>=0.5.0` on POSIX, or stdlib `asyncio`, honoring `--loop` / `VERTEBRA_LOOP` and module-specific `GilAuditReport` `quarantined_modules` without deprecated `EventLoopPolicy` APIs). In SHM mode and single-worker Embedded mode (`get_request_notify_fd() >= 0`), `Worker._run_inline_event_loop` registers `_on_notify_readable` on `loop.add_reader(notify_fd, ...)` to drain `eventfd` only on wakeup and delegates batch draining to `_drain_batches`, which schedules a 1-turn `loop.call_soon` grace check when the ring empties before arming Dekker `prepare_request_sleep` / `cancel_request_sleep` barriers—cutting redundant `eventfd` reads/writes under steady load while executing async, ASGI, and RSGI coroutines inline with zero `call_soon_threadsafe` hops.
- **`O(1)` `MOUNT_HANDLER_BASE` Mount Dispatch, `drive_native_batch` & `FastPathProvider` Stacking**: Mounted prefixes encode `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`) directly in the Rust `matchit` route table so `partition_batch` resolves mounts via `O(1)` bitmask indexing instead of linear prefix scans. Native `async def` handlers with synchronous guards/dependencies compile to `_invoke_compiled_sync_to_coro` and step via C-API `drive_native_batch` (`PyIter_Send`). Standalone and `App.mount("/prefix", sub_app)` applications across `Litestar`, `FastAPI`, `Starlette`, `Flask`, and `Django` auto-attach their zero-closure `FastPath` (`__vertebra_fastpath__`) via [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] and [`_fastpath_core.py`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py], enforcing `extract_scope_body_fast(..., max_body_size=...)` (`413 Payload Too Large` on oversized chunked streams), zero-copy `ArrowCursor` buffer-view responses, and raw `bytes` header preservation in `AsgiSend.send_raw_response`.

## 3. Direct `Request::body`, Zero-Copy Response Coercion & Snapshot Unpinning

- **Direct `Request::body` & Zero-Copy Buffer Coercion (`_serialize_result` / `coerce_response`)**: [`Request::body`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] materializes Python `bytes` directly from the `BatchSnapshot` body slice (`PyBytes::new`), while `req.body_view` exposes a zero-copy `memoryview`. `_serialize_result` accepts `bytes | bytearray | memoryview`, `str`, `Response`, and `msgspec`-serializable values (`encode_json_bytes`), forwarding buffers directly to Rust `submit_batch_raw` / `submit_response_raw` (`PyBuffer` extraction).
- **Unpinning `BatchSnapshot` Across `await`**: Before awaiting an `async_native` handler coroutine, `_dispatch_async_item` extracts `request_id` into a lightweight `_AsyncErrorStub` and drops the `Request` reference so suspended I/O tasks never pin `Arc<BatchSnapshot>` slabs in `BufferPool`.
- **Loop-Local & Fused Fast Paths**: Use [`SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]`](../../../../../src/py/vertebra/utils/sync_tools.py) to shard non-thread-safe primitives across `EventLoopPool` loops on free-threaded Python `3.14t`/`3.15t`, and [`FastPath` / `dispatch_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py] for fused Litestar direct dispatch.
- **Prepared Database & Tool Boundaries**: Prepared statement retention, native parameter bind modes, and Arrow zero-copy rules are documented in [Database Drivers, Native Dialers & Arrow Zero-Copy](../../domains/data/database-drivers-and-dialers.md), while typed tool schema compilation and confirmation gates live in [MCP, WebMCP & A2A Protocol Harness](../../domains/agent/mcp-and-a2a-protocol-harness.md).

Provenance sources: [^src-server-rsgi-adapter-py], [^src-server-wsgi-adapter-py], [^src-utils-sync-tools-py].

[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Python implementation module (src/py/vertebra/server/_loops.py).
[^src-server-fastpath-py]: `src/py/vertebra/server/_fastpath.py` — Python implementation module (src/py/vertebra/server/_fastpath.py).
[^src-server-fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPathProvider registry and direct route compilation core (src/py/vertebra/server/_fastpath_core.py).
[^src-server-asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py` — Python implementation module (src/py/vertebra/server/_asgi/_runner.py).
[^src-server-rsgi-adapter-py]: `src/py/vertebra/server/_rsgi/_adapter.py` — Python implementation module (src/py/vertebra/server/_rsgi/_adapter.py).
[^src-server-wsgi-adapter-py]: `src/py/vertebra/server/_wsgi/_adapter.py` — Python implementation module (src/py/vertebra/server/_wsgi/_adapter.py).
[^src-utils-sync-tools-py]: `src/py/vertebra/utils/sync_tools.py` — Python implementation module (src/py/vertebra/utils/sync_tools.py).
[^src-extensions-litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py` — Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust Request PyO3 binding and direct Request::body access (src/rs/vertebra-py/src/request.rs).
