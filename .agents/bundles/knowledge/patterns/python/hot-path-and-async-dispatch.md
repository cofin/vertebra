---
type: Pattern
title: "Hot-Path Caching & Single-Wake Async Dispatch"
description: "Hot-path request dispatch patterns: startup signature caching, inline vectorized sync batches, uvloop Dekker polling, Rust ASGI/RSGI drivers, and zero-copy response coercion."
status: stable
tags: [patterns, python, performance, batch-dispatch, async]
sources:
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_loops.py
  - resource: src/py/vertebra/server/_fastpath.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/server/_rsgi/_adapter.py
  - resource: src/py/vertebra/server/_wsgi/_adapter.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/vertebra/extensions/litestar/fastpath.py
---

# Hot-Path Caching & Single-Wake Async Dispatch

Python worker code on the request hot path ([`worker.py`](../../../../../src/py/vertebra/server/worker.py), [`_dispatch.py`](../../../../../src/py/vertebra/server/_dispatch.py), [`_loops.py`](../../../../../src/py/vertebra/server/_loops.py), [`_asgi/_runner.py`](../../../../../src/py/vertebra/server/_asgi/_runner.py)) avoids per-request reflection, GIL-thrashing thread pools, per-request cross-thread wakeups, and unnecessary `asyncio.Task` allocations.

## 1. Startup Signature Caching (`HandlerSpec` / `compile_handler_spec`)

Never call `inspect.signature()` on the request path. [`compile_handler_spec(handler, sync_to_thread=...)`](../../../../../src/py/vertebra/server/_dispatch.py) (pre-compiled at worker startup via `compile_handler_table(handlers)`) compiles a `HandlerSpec` and specialized invoker closure once:

<!-- truth: start -->
```python
class HandlerSpec(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Pre-compiled dispatch specification and invoker for a route handler."""

    has_params: bool
    is_async: bool
    param_names: tuple[str, ...]
    invoker: Callable[..., Any]
    sync_to_thread: bool = False


def compile_handler_table(handlers: Mapping[int, Callable[..., Any]]) -> dict[int, HandlerSpec]:
    """Pre-compile dispatch specs for all registered handler callables."""
    return {hid: compile_handler_spec(fn) for hid, fn in handlers.items()}
```
<!-- truth: end -->

## 2. Inline Vectorized Sync Batches, `uvloop` Dekker Polling & `O(1)` Mounts

- **Inline Vectorized Sync Execution (`"sync_inline"`)**: Non-async native handlers default to `sync_to_thread=False`. Both [`Worker._process_batch`](../../../../../src/py/vertebra/server/worker.py) (when `all_sync_native` is true) and [`dispatch_batch`](../../../../../src/py/vertebra/server/_dispatch.py) run inline sync handlers in a tight loop on the worker thread and flush the entire batch via one `submit_batch_raw(raw_items)` call (`0` `ThreadPoolExecutor` submissions). Only handlers explicitly opted into `sync_to_thread=True` (`__vertebra_sync_to_thread__`) allocate the lazy `ThreadPoolExecutor` (`max_workers = max(4, min(32, cpu_count))` on GIL builds).
- **Single-Thread `LoopBackend` (`uvloop`, `winloop`, `rloop`, `asyncio`) + `loop.add_reader` Dekker Polling**: [`create_event_loop()`](../../../../../src/py/vertebra/server/_loops.py) resolves `LoopBackend` (`auto` -> `uvloop>=0.23.0` on POSIX, `winloop>=0.8.1` on Windows, opt-in `rloop>=0.5.0`, or stdlib `asyncio`, honoring `--loop` / `VERTEBRA_LOOP` and `GilAuditReport` `quarantined_modules` without deprecated `EventLoopPolicy` APIs). In SHM mode and single-worker Embedded mode (`get_request_notify_fd() >= 0`), `Worker._run_inline_event_loop` registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep` / `cancel_request_sleep` barriers so async, ASGI, and RSGI coroutines execute inline on the worker's own event loop without a background loop thread or `call_soon_threadsafe` hop.
- **`O(1)` `MOUNT_HANDLER_BASE` Mount Dispatch, `drive_native_batch` & 6-Framework `FastPath` Stacking**: Mounted prefixes encode `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`) directly in the Rust `matchit` route table so `partition_batch` resolves mounts via `O(1)` bitmask indexing instead of linear prefix scans. Native `async def` handlers with synchronous guards/dependencies compile to `_invoke_compiled_sync_to_coro` and step via C-API `drive_native_batch` (`PyIter_Send`). Standalone and `App.mount("/prefix", sub_app)` applications across `Litestar`, `FastAPI`, `Starlette`, `Flask`, and `Django` auto-attach their zero-closure `FastPath` via [`attach_framework_fastpath`](../../../../../src/py/vertebra/server/_fastpath.py) and emit unary responses in 1 C-API call (`send_raw_response`).

## 3. Zero-Copy Response Coercion, Snapshot Unpinning & Loop-Sharded Resources

- **Zero-Copy Buffer Coercion (`_serialize_result` / `coerce_response`)**: `_serialize_result` accepts `bytes | bytearray | memoryview` (including native `req.body_view` zero-copy returns), `str`, `Response`, and `msgspec`-serializable values (`encode_json_bytes`), forwarding buffers directly to Rust `submit_batch_raw` / `submit_response_raw` (`PyBuffer` extraction).
- **Unpinning `BatchSnapshot` Across `await`**: Before awaiting an `async_native` handler coroutine, `_dispatch_async_item` extracts `request_id`, `method`, and `path` into a lightweight `_AsyncErrorStub` and drops the `Request` reference so suspended I/O tasks never pin `Arc<BatchSnapshot>` slabs in `BufferPool`.
- **Loop-Local & Fused Fast Paths**: Use [`SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]`](../../../../../src/py/vertebra/utils/sync_tools.py) to shard non-thread-safe primitives across `EventLoopPool` loops on free-threaded Python `3.14t`/`3.15t`, and [`LitestarFastPathBypass`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) for fused Litestar direct dispatch.
