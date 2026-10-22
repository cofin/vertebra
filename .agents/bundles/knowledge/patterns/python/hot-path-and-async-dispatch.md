---
type: Pattern
title: "Hot-Path Caching & Single-Wake Async Dispatch"
description: "Hot-path request dispatch patterns: startup signature caching, BatchDispatchPlan partitioning, event-loop wakeups, and response coercion."
status: stable
tags: [patterns, python, performance, batch-dispatch, async]
sources:
  - resource: src/py/vertebra/server/_native.py
  - resource: src/py/vertebra/server/_runtime_tuning.py
  - resource: src/py/vertebra/server/_asgi/_runner.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/vertebra/extensions/litestar/fastpath.py
---

# Hot-Path Caching & Single-Wake Async Dispatch

Python worker code on the request hot path ([`_native.py`](../../../../../src/py/vertebra/server/_native.py), [`_runtime_tuning.py`](../../../../../src/py/vertebra/server/_runtime_tuning.py), [`_asgi/_runner.py`](../../../../../src/py/vertebra/server/_asgi/_runner.py)) avoids per-request reflection, per-request cross-thread wakeups, and unnecessary `asyncio.Task` allocations.

## 1. Startup Signature Caching (`_handler_dispatch_spec`)

Never call `inspect.signature()` on the request path. [`_handler_dispatch_spec(handler)`](../../../../../src/py/vertebra/server/_native.py) (`@functools.lru_cache(maxsize=512)`, pre-warmed at worker startup for up to 512 registered handlers) compiles a specialized invoker closure once:

<!-- truth: start -->
```python
@functools.lru_cache(maxsize=512)
def _handler_dispatch_spec(
    handler: Any,
) -> tuple[bool, bool, tuple[str, ...], Callable[[Any], Any]]:
    """Compile and cache ``(has_params, is_async, param_names, invoker)`` for a handler."""
    params = tuple(inspect.signature(handler).parameters.keys())
    has_params = bool(params)
    is_async = inspect.iscoroutinefunction(handler)
    if not has_params:

        def _invoke_no_args(_request: Any, _h: Any = handler) -> Any:
            return _h()

        invoker: Callable[[Any], Any] = _invoke_no_args
    elif len(params) == 1 and params[0] in _REQUEST_PARAMS:
        invoker = handler
    else:
        extractors = tuple(_PARAM_EXTRACTORS.get(name, _extract_request) for name in params)

        def _invoke_with_extractors(
            request: Any,
            _h: Any = handler,
            _ext: tuple[Callable[[Any], Any], ...] = extractors,
        ) -> Any:
            return _h(*(fn(request) for fn in _ext))

        invoker = _invoke_with_extractors
    return has_params, is_async, params, invoker
```
<!-- truth: end -->

## 2. Batch Partitioning & Event-Loop Wakeups

[`_partition_batch()`](../../../../../src/py/vertebra/server/_native.py) classifies each batch once into `BatchDispatchPlan(sync_native, async_native, mounts, unmatched)`:

- `async_native` coroutines are submitted via `spawn_batch`: [`AsyncRuntime.spawn_batch()`](../../../../../src/py/vertebra/server/_native.py) wakes the background loop thread once per batch; [`AsgiEventLoopPool.spawn_batch()`](../../../../../src/py/vertebra/server/_runtime_tuning.py) wakes once per non-empty loop shard (round-robin bucketing); and the SHM `asyncio` loop calls `loop.create_task` directly without a cross-thread wake.
- Mounted ASGI requests are grouped by prefix and dispatched via [`AsgiRunner.spawn_batch()`](../../../../../src/py/vertebra/server/_asgi/_runner.py) (or sharded across [`_AsgiMountRunner`](../../../../../src/py/vertebra/server/_native.py) when using `AsgiEventLoopPool`), which posts one `_BatchProcessor` per loop and steps each coroutine with `_drive_coro`, promoting to an `asyncio.Task` (`_resume_coro_in_task`) only if the coroutine suspends.

## 3. Response Coercion, Completion & Loop-Sharded Resources

- **Response Coercion (`_coerce_response`)**: [`_coerce_response(result, request_id)`](../../../../../src/py/vertebra/server/_native.py) passes native `Response` objects through (backfilling `request_id`), wraps `bytes` and `str` bodies directly, and serializes structured values (`dict`, `list`, `msgspec.Struct`) via `encode_json_bytes` from `vertebra.utils.serializers._json`. Unhandled exceptions become `500` (`b"internal server error"`) and unmatched routes become `404` (`b"not found"`).
- **Per-Request vs Batch Completion**: Async and executor-backed handlers complete individually via `submit_response(resp)` (which detaches from the GIL during transport write), whereas synchronous blocking callers use `submit_results(responses)`.
- **Loop-Local & Fused Fast Paths**: Use [`LoopShardedResource[T]`](../../../../../src/py/vertebra/utils/sync_tools.py) to shard non-thread-safe async primitives across `AsgiEventLoopPool` loops without cross-loop lock contention, and [`CompiledLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) (`array_like=True` `msgspec.Struct`) for fused Litestar direct dispatch.
