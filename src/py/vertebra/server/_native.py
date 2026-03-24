"""Vertebra batch runner — concurrent batch dispatch.

This module implements the core request dispatch loop for native mode workers.
Batches are partitioned into sync (sequential), async (gathered), and mount
(batched per prefix) groups for optimal throughput.
"""

import asyncio
import concurrent.futures
import contextlib
import functools
import inspect
import logging
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from vertebra._vertebra import VertebraBatch, VertebraRequest, VertebraResponse

log = logging.getLogger(__name__)
_RUNNING = True
_T = TypeVar("_T")


class CallbackScheduler:
    """Persistent event loop thread for scheduling async callbacks from sync threads.

    Drastically reduces per-batch overhead by avoiding loop start/stop and
    enabling continuous IO overlap across batch boundaries.
    """

    __slots__ = ("_loop", "_thread")

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="vertebra-event-loop",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the event loop and wait for the thread to exit."""
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5.0)
            self._loop.close()

    def schedule(self, coro: "Coroutine[Any, Any, _T]") -> "concurrent.futures.Future[_T]":
        """Schedule a coroutine onto the event loop and return a Future."""
        future: concurrent.futures.Future[_T] = concurrent.futures.Future()

        def _callback() -> None:
            async def _wrap() -> None:
                try:
                    result = await coro
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)

            self._loop.create_task(_wrap())

        self._loop.call_soon_threadsafe(_callback)
        return future

    def schedule_batch(
        self,
        coros: "list[Coroutine[Any, Any, _T]]",
    ) -> "list[concurrent.futures.Future[_T]]":
        """Schedule multiple coroutines and return their Futures."""
        return [self.schedule(c) for c in coros]

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Access the underlying event loop."""
        return self._loop


def _install_signal_handlers() -> None:
    """Install SIGTERM and SIGINT handlers for clean batch-boundary shutdown.

    Signal handlers can only be set from the main thread. When the Rust binary
    spawns Python via PyO3, the worker runs on a background thread — in that
    case we skip installation (the Rust side handles shutdown signals).
    """
    if threading.current_thread() is not threading.main_thread():
        return

    def _handle_signal(signum: int, frame: object) -> None:  # noqa: ARG001
        global _RUNNING  # noqa: PLW0603
        _RUNNING = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def _detect_protocol(app: Any) -> str:
    """Detect whether an app is WSGI or ASGI by inspecting its callable signature."""
    # Check the app itself first (handles bare async def functions).
    if inspect.iscoroutinefunction(app):
        return "asgi"
    # For class instances, check if the class's __call__ is async.
    cls_call = type(app).__dict__.get("__call__")
    if cls_call is not None and inspect.iscoroutinefunction(cls_call):
        return "asgi"
    # Fallback: count parameters (3+ = ASGI scope/receive/send).
    try:
        sig = inspect.signature(app)
        params = [p for p in sig.parameters.values() if p.name != "self"]
        if len(params) >= 3:
            return "asgi"
    except (ValueError, TypeError):
        pass
    return "wsgi"


def _build_mount_adapters(
    mounts: list[tuple[str, Any, str]],
) -> list[tuple[str, Any, str]]:
    """Build adapter instances for each mount, resolving protocol="auto"."""
    resolved: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in mounts:
        if protocol == "auto":
            protocol = _detect_protocol(app)
        resolved.append((prefix, app, protocol))
    return resolved


# ---------------------------------------------------------------------------
# Batch partitioning
# ---------------------------------------------------------------------------


@dataclass
class _PartitionedBatch:
    """Result of partitioning a batch into dispatch groups."""

    sync_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    async_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    mounts: dict[str, list[tuple[int, Any]]] = field(default_factory=dict)
    unmatched: list[tuple[int, Any]] = field(default_factory=list)


def _partition_batch(
    batch: "list[Any] | VertebraBatch",
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
) -> _PartitionedBatch:
    """Classify each request into sync, async, mount, or unmatched groups.

    O(N) dict lookups + prefix checks — negligible vs I/O.
    """
    result = _PartitionedBatch()

    for idx, request in enumerate(batch):
        handler = handlers.get(request.handler_id)
        if handler is not None:
            _, is_async = _handler_meta(handler)
            if is_async:
                result.async_native.append((idx, request, handler))
            else:
                result.sync_native.append((idx, request, handler))
            continue

        # Try mounted apps by prefix match.
        path = request.path
        matched = False
        for prefix, _adapter, _protocol in mount_runners:
            if path.startswith(prefix):
                result.mounts.setdefault(prefix, []).append((idx, request))
                matched = True
                break

        if not matched:
            result.unmatched.append((idx, request))

    return result


# ---------------------------------------------------------------------------
# WSGI concurrent dispatch
# ---------------------------------------------------------------------------

# Module-level executor shared across all workers. Lazy-initialized on first
# WSGI dispatch.  Thread pool for IO-bound WSGI apps (DB queries, HTTP calls).
_wsgi_executor: "ThreadPoolExecutor | None" = None


def _get_wsgi_executor() -> ThreadPoolExecutor:
    """Return the shared WSGI thread pool, creating it on first use."""
    global _wsgi_executor  # noqa: PLW0603
    if _wsgi_executor is None:
        import os

        # Increase default to 128 to handle high-concurrency benchmarks
        # (e.g. io_10ms with c=128).
        pool_size = max(128, (os.cpu_count() or 1) * 8)
        _wsgi_executor = ThreadPoolExecutor(
            max_workers=pool_size,
            thread_name_prefix="vertebra-wsgi",
        )
    return _wsgi_executor


def _dispatch_wsgi_concurrent(
    adapter: Any,
    entries: list[tuple[int, Any]],
    results: list[Any],
    vr_cls: type,
) -> None:
    """Dispatch WSGI requests concurrently via thread pool.

    All requests are submitted up front, then all results collected. This
    maximizes IO overlap for blocking WSGI apps. For trivially fast WSGI
    apps (hello), the thread pool overhead is ~2x vs sequential — an
    acceptable trade-off for the massive IO improvement (1.3s → ~10ms).
    """
    if len(entries) == 1:
        # Single request — no concurrency benefit, skip pool overhead.
        idx, request = entries[0]
        try:
            results[idx] = adapter(request)
        except Exception:
            print(f"[vertebra] wsgi handler error for request {request.request_id}", file=sys.stderr)  # noqa: T201
            results[idx] = vr_cls(status=500, body=b"internal server error", request_id=request.request_id)
        return

    executor = _get_wsgi_executor()
    indexed_futures: list[tuple[int, Any, Any]] = []
    for idx, request in entries:
        future = executor.submit(adapter, request)
        indexed_futures.append((idx, request, future))

    for idx, request, future in indexed_futures:
        try:
            results[idx] = future.result()
        except Exception:
            print(f"[vertebra] wsgi handler error for request {request.request_id}", file=sys.stderr)  # noqa: T201
            results[idx] = vr_cls(status=500, body=b"internal server error", request_id=request.request_id)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _dispatch_batch(
    partitioned: _PartitionedBatch,
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
    vr_cls: type,
    scheduler: CallbackScheduler,
) -> list[Any]:
    """Dispatch a partitioned batch with concurrent execution.

    Sync native handlers run sequentially (zero overhead for trivial handlers).
    Async native handlers run concurrently via CallbackScheduler.
    WSGI mounts run concurrently via ThreadPoolExecutor.
    ASGI mounts run concurrently via the persistent event loop.
    Returns responses in original batch order.
    """
    total = (
        len(partitioned.sync_native)
        + len(partitioned.async_native)
        + sum(len(v) for v in partitioned.mounts.values())
        + len(partitioned.unmatched)
    )
    results: list[Any] = [None] * total

    # 1. Sync native handlers — sequential, zero overhead for trivial handlers.
    for idx, request, handler in partitioned.sync_native:
        response = _call_handler(handler, request, loop=scheduler.loop if partitioned.async_native else None)
        results[idx] = _coerce_response(response, request.request_id, vr_cls)

    # 2. Async native handlers — scheduled onto persistent loop.
    if partitioned.async_native:
        coros = []
        for _idx, request, handler in partitioned.async_native:
            has_params, _ = _handler_meta(handler)
            if has_params:
                coros.append(handler(*_resolve_handler_args(handler, request)))
            else:
                coros.append(handler())

        futures = scheduler.schedule_batch(coros)
        concurrent.futures.wait(futures)

        for (idx, request, _handler), fut in zip(partitioned.async_native, futures, strict=True):
            try:
                raw = fut.result()
                results[idx] = _coerce_response(raw, request.request_id, vr_cls)
            except Exception as e:
                print(f"[vertebra] async handler error: {e}", file=sys.stderr)  # noqa: T201
                results[idx] = vr_cls(
                    status=500,
                    body=b"internal server error",
                    request_id=request.request_id,
                )

    # 3. Mounts — batched per prefix.
    mount_lookup = {prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners}
    for prefix, entries in partitioned.mounts.items():
        adapter, protocol = mount_lookup[prefix]
        if protocol == "asgi":
            requests_list = [req for _idx, req in entries]
            responses = adapter(requests_list)
            for (idx, request), resp in zip(entries, responses, strict=True):
                results[idx] = vr_cls(
                    status=resp.status,
                    body=resp.body,
                    headers=getattr(resp, "headers", []),
                    request_id=request.request_id,
                )
        elif protocol == "wsgi":
            _dispatch_wsgi_concurrent(adapter, entries, results, vr_cls)

    # 4. Unmatched — 404.
    for idx, request in partitioned.unmatched:
        results[idx] = vr_cls(status=404, body=b"not found", request_id=request.request_id)

    return results


async def _shm_worker_loop(
    handlers: dict[int, Any],
    resolved_mounts: list[tuple[str, Any, str]],
    scheduler: CallbackScheduler,
    batch_timeout_ms: int,
) -> None:
    """Asyncio-driven SHM worker loop using eventfd notifications."""
    from vertebra._vertebra import (
        VertebraResponse as VR,
    )
    from vertebra._vertebra import (
        clear_request_notification,
        get_request_notify_fd,
        submit_results,
        try_get_batch,
    )

    loop = asyncio.get_running_loop()
    notify_fd = get_request_notify_fd()
    ready_event = asyncio.Event()

    # Create mount adapters once
    active_mounts: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in resolved_mounts:
        if protocol == "asgi":
            from vertebra.server._asgi._runner import AsgiRunner

            # AsgiRunner is a synchronous batch interface, so it must submit
            # onto the scheduler's off-thread loop rather than re-enter the
            # currently running SHM event loop.
            active_mounts.append((prefix, AsgiRunner(app, mount_prefix=prefix, loop=scheduler.loop), "asgi"))
        elif protocol == "wsgi":
            from vertebra.server._wsgi._adapter import WsgiAdapter

            active_mounts.append((prefix, WsgiAdapter(app), "wsgi"))

    def _on_readable() -> None:
        ready_event.set()

    loop.add_reader(notify_fd, _on_readable)
    ready_event.set()

    try:
        while _RUNNING:
            # Wait for next notification or small timeout
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(ready_event.wait(), timeout=batch_timeout_ms / 1000.0)
            ready_event.clear()

            # Clear the notification BEFORE draining to avoid TOCTOU race conditions
            clear_request_notification()

            # Drain all available batches
            while True:
                batch = try_get_batch()
                if batch is None:
                    break

                partitioned = _partition_batch(batch, handlers, active_mounts)
                results = _dispatch_batch(partitioned, handlers, active_mounts, VR, scheduler)
                submit_results(results)
    finally:
        loop.remove_reader(notify_fd)


def run_worker(
    handlers: "dict[int, Callable[[VertebraRequest], VertebraResponse]]",
    *,
    mounts: "list[tuple[str, Any, str]] | None" = None,
    batch_timeout_ms: int = 100,
    error_handler: "Callable[[Exception, VertebraRequest], VertebraResponse] | None" = None,
    worker_count: "int | None" = None,
) -> None:
    """Execute the batch runner loop with Hybrid Architecture.

    - **IO Multiplexer**: Single thread managing a persistent event loop for ALL
      async handlers and ASGI mounts. Preserves batch-first IO amortization.
    - **CPU Workers**: N threads for sequential sync handlers and WSGI mounts.
      Benefits from multi-core/free-threading.
    - **Shared Transport**: Lock-free MPMC bridge.
    """
    import os

    from vertebra._vertebra import VertebraResponse as VR
    from vertebra._vertebra import get_batch, get_request_notify_fd, init_transport, submit_results

    _install_signal_handlers()

    global _RUNNING  # noqa: PLW0603
    _RUNNING = True

    # Initialize the transport channel once. SHM workers may already have
    # initialized the transport in the process entrypoint before delegating
    # here, so we must not call init_transport(mode="shm") twice.
    transport_mode = os.environ.get("VERTEBRA_TRANSPORT_MODE", "inprocess")
    notify_fd = get_request_notify_fd()
    is_shm = transport_mode == "shm" or notify_fd > -1

    if is_shm:
        if notify_fd <= -1:
            init_transport(mode="shm")
    else:
        init_transport()

    # 1. Shared Infrastructure
    scheduler = CallbackScheduler()
    cpu_count = worker_count or (os.cpu_count() or 1)

    if is_shm:
        cpu_count = 1

    global _wsgi_executor  # noqa: PLW0603
    _wsgi_executor = ThreadPoolExecutor(
        max_workers=max(128, cpu_count * 8),
        thread_name_prefix="vertebra-sync",
    )

    resolved_mounts = _build_mount_adapters(mounts or [])

    # 2. Worker Loops
    def _io_multiplexer_loop() -> None:
        """Single thread for all async work."""
        # IO-multiplexer specific mounts (ASGI only)
        io_mounts: list[tuple[str, Any, str]] = []
        for prefix, app, protocol in resolved_mounts:
            if protocol == "asgi":
                from vertebra.server._asgi._runner import AsgiRunner

                io_mounts.append((prefix, AsgiRunner(app, mount_prefix=prefix, loop=scheduler.loop), "asgi"))

        try:
            while _RUNNING:
                batch = get_batch(timeout_ms=batch_timeout_ms)
                if batch is None:
                    continue

                partitioned = _partition_batch(batch, handlers, io_mounts)
                # Only dispatch async native and ASGI mounts
                results = _dispatch_batch(partitioned, handlers, io_mounts, VR, scheduler)
                submit_results(results)
        finally:
            # Note: scheduler stopped by main thread
            pass

    def _cpu_worker_loop(worker_id: int) -> None:
        """N threads for all sync work."""
        # CPU-worker specific mounts (WSGI only)
        cpu_mounts: list[tuple[str, Any, str]] = []
        for prefix, app, protocol in resolved_mounts:
            if protocol == "wsgi":
                from vertebra.server._wsgi._adapter import WsgiAdapter

                cpu_mounts.append((prefix, WsgiAdapter(app), "wsgi"))

        try:
            while _RUNNING:
                batch = get_batch(timeout_ms=batch_timeout_ms)
                if batch is None:
                    continue

                partitioned = _partition_batch(batch, handlers, cpu_mounts)
                # Only dispatch sync native and WSGI mounts
                results = _dispatch_batch(partitioned, handlers, cpu_mounts, VR, scheduler)
                submit_results(results)
        finally:
            pass

    # 3. Spawn Hybrid Workers
    if is_shm:
        # SHM mode: Run event-driven loop in main thread
        try:
            asyncio.run(_shm_worker_loop(handlers, resolved_mounts, scheduler, batch_timeout_ms))
        finally:
            _RUNNING = False
            scheduler.stop()
            if _wsgi_executor is not None:
                _wsgi_executor.shutdown(wait=False)
                _wsgi_executor = None
        return

    threads: list[threading.Thread] = []

    # One IO Multiplexer
    io_thread = threading.Thread(target=_io_multiplexer_loop, name="vertebra-io-mux", daemon=True)
    io_thread.start()
    threads.append(io_thread)

    # N CPU Workers (minus 1, as this thread becomes worker 0)
    for i in range(1, cpu_count):
        t = threading.Thread(target=_cpu_worker_loop, args=(i,), name=f"vertebra-cpu-{i}", daemon=True)
        t.start()
        threads.append(t)

    try:
        _cpu_worker_loop(0)
    finally:
        _RUNNING = False
        scheduler.stop()
        if _wsgi_executor is not None:
            _wsgi_executor.shutdown(wait=False)
            _wsgi_executor = None
        for t in threads:
            t.join(timeout=2.0)


def _is_free_threaded() -> bool:
    """Check if the current Python build is free-threaded."""
    try:
        from vertebra._vertebra import FREE_THREADED

        return FREE_THREADED
    except ImportError:
        return False


@functools.lru_cache(maxsize=256)
def _handler_meta(handler: Any) -> tuple[bool, bool]:
    """Return (has_params, is_async) for a handler."""
    has_params = bool(inspect.signature(handler).parameters)
    is_async = inspect.iscoroutinefunction(handler)
    return has_params, is_async


@functools.lru_cache(maxsize=256)
def _handler_params(handler: Any) -> tuple[str, ...]:
    """Return parameter names for a handler (cached)."""
    return tuple(inspect.signature(handler).parameters.keys())


# Parameter names that receive the whole request object.
_REQUEST_PARAMS = frozenset({"request", "req"})


def _resolve_handler_args(handler: Any, request: Any) -> tuple[Any, ...]:
    """Build positional args for a handler based on its parameter names."""
    params = _handler_params(handler)
    if not params:
        return ()
    args: list[Any] = []
    for name in params:
        if name in _REQUEST_PARAMS:
            args.append(request)
        elif name == "body":
            body = request.body
            args.append(body if isinstance(body, bytes) else bytes(body or b""))
        elif name == "headers":
            args.append(request.headers)
        elif name == "method":
            args.append(request.method)
        elif name == "path":
            args.append(request.path)
        elif name == "query":
            args.append(request.query)
        else:
            # Unknown param name — pass request as fallback.
            args.append(request)
    return tuple(args)


def _call_handler(handler: Any, request: Any, *, loop: "asyncio.AbstractEventLoop | None" = None) -> Any:
    """Call a handler, resolving parameters by name."""
    has_params, is_async = _handler_meta(handler)
    result = handler(*_resolve_handler_args(handler, request)) if has_params else handler()
    if is_async:
        if loop is None:
            msg = "async handler called without an event loop"
            raise RuntimeError(msg)
        return loop.run_until_complete(result)
    return result


def _coerce_response(raw: Any, request_id: int, vr_cls: type) -> Any:
    """Turn a handler return value into a VertebraResponse."""
    if isinstance(raw, vr_cls):
        if raw.request_id == 0:  # type: ignore[attr-defined]
            return vr_cls(status=raw.status, body=bytes(raw.body), request_id=request_id)  # type: ignore[attr-defined]
        return raw
    # Handler returned a primitive — wrap it.
    if isinstance(raw, bytes):
        body = raw
    elif isinstance(raw, str):
        body = raw.encode()
    else:
        import json

        body = json.dumps(raw).encode()
    return vr_cls(status=200, body=body, request_id=request_id)


def _internal_error(request_id: int) -> "VertebraResponse":
    from vertebra._vertebra import VertebraResponse as VR

    return VR(status=500, body=b"internal server error", request_id=request_id)


__all__ = ("run_worker",)
