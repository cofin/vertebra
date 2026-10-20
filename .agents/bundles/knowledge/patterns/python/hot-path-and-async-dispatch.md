---
name: pattern-python-hot-path-and-async-dispatch
description: Hot-path request dispatch patterns: startup signature caching, 4-way BatchDispatchPlan, single-wake AsyncRuntime, and vectorized FFI completion.
sources:
  - src/py/vertebra/server/_native.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/framework/_app.py
---

# Hot-Path Caching & Single-Wake Async Dispatch

Python worker code on the request hot path ([`_native.py`](../../../../../src/py/vertebra/server/_native.py), [`_asgi/_runner.py`](../../../../../src/py/vertebra/server/_asgi/_runner.py), [`_app.py`](../../../../../src/py/vertebra/framework/_app.py)) avoids per-request reflection, per-request thread wakeups, and unnecessary `asyncio.Task` allocations.

## 1. Startup Signature Caching (`_handler_dispatch_spec`)

Never call `inspect.signature()` on the request path. [`_handler_dispatch_spec(handler)`](../../../../../src/py/vertebra/server/_native.py) (`@functools.lru_cache(maxsize=512)`) compiles a specialized invoker closure once at worker startup:

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

## 2. 4-Way Batch Partitioning & Single-Wake `AsyncRuntime`

[`_partition_batch()`](../../../../../src/py/vertebra/server/_native.py) classifies each batch once into `BatchDispatchPlan(sync_native, async_native, mounts, unmatched)`:

- `async_native` coroutines are submitted together via [`AsyncRuntime.spawn_batch()`](../../../../../src/py/vertebra/server/_native.py) or [`AsyncRuntime.schedule_batch()`](../../../../../src/py/vertebra/server/_native.py), waking the background event loop thread with a single `self._loop.call_soon_threadsafe(_callback)` call per batch.
- Mounted ASGI requests are grouped by prefix and dispatched via [`AsgiRunner.spawn_batch()`](../../../../../src/py/vertebra/server/_asgi/_runner.py), which steps each ASGI coroutine via `_drive_coro` + `_CoroResumer` without creating an `asyncio.Task` when no I/O suspension occurs.
