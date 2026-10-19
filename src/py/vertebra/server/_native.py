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
import os
import signal
import threading
from collections.abc import Callable, Coroutine, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vertebra.utils.serializers._json import encode_json

if TYPE_CHECKING:
    from vertebra._vertebra import VertebraBatch, VertebraRequest, VertebraResponse

_NATIVE_FREE_THREADED = False
_submit_response: Any = None
with contextlib.suppress(ImportError):
    from vertebra._vertebra import (
        FREE_THREADED as _NATIVE_FREE_THREADED,
    )
    from vertebra._vertebra import (
        submit_response as _submit_response,
    )

__all__ = ("CallbackScheduler", "run_worker")

log = logging.getLogger(__name__)
_REQUEST_PARAMS = frozenset({"request", "req"})
_RUNNING = True
_wsgi_executor: ThreadPoolExecutor | None = None


class CallbackScheduler:
    """Persistent event loop thread for scheduling async callbacks from sync threads.

    Reduces per-batch overhead by avoiding loop start/stop and enabling
    continuous I/O overlap across batch boundaries.
    """

    __slots__ = ("_loop", "_tasks", "_thread")

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="vertebra-event-loop",
            daemon=True,
        )
        self._thread.start()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the underlying event loop."""
        return self._loop

    def stop(self) -> None:
        """Stop the event loop and wait for the background thread to exit."""
        if not self._loop.is_running():
            return

        def _shutdown() -> None:
            async def _wait_tasks() -> None:
                if self._tasks:
                    await asyncio.wait(list(self._tasks), timeout=2.0)
                self._loop.stop()

            self._loop.create_task(_wait_tasks())

        self._loop.call_soon_threadsafe(_shutdown)
        self._thread.join(timeout=5.0)
        self._loop.close()

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Schedule a coroutine onto the event loop without blocking."""

        def _callback() -> None:
            task = self._loop.create_task(coro)
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        self._loop.call_soon_threadsafe(_callback)

    def spawn_batch(self, coros: Iterable[Coroutine[Any, Any, Any]]) -> None:
        """Schedule multiple coroutines onto the event loop in a single cross-thread wake."""
        coro_list = list(coros)
        if not coro_list:
            return

        def _callback() -> None:
            loop = self._loop
            tasks = self._tasks
            discard = tasks.discard
            for coro in coro_list:
                task = loop.create_task(coro)
                tasks.add(task)
                task.add_done_callback(discard)

        self._loop.call_soon_threadsafe(_callback)

    def schedule[T](self, coro: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        """Schedule a coroutine onto the event loop and return a Future."""
        future: concurrent.futures.Future[T] = concurrent.futures.Future()

        def _callback() -> None:
            async def _wrap() -> None:
                try:
                    result = await coro
                    future.set_result(result)
                except Exception as exc:
                    future.set_exception(exc)

            self._loop.create_task(_wrap())

        self._loop.call_soon_threadsafe(_callback)
        return future

    def schedule_batch[T](
        self,
        coros: list[Coroutine[Any, Any, T]],
    ) -> list[concurrent.futures.Future[T]]:
        """Schedule multiple coroutines under a single loop wake and return their Futures."""
        if not coros:
            return []
        futures: list[concurrent.futures.Future[T]] = [concurrent.futures.Future() for _ in coros]

        def _callback() -> None:
            loop = self._loop
            for coro, fut in zip(coros, futures, strict=True):

                async def _wrap(
                    c: Coroutine[Any, Any, T] = coro,
                    f: concurrent.futures.Future[T] = fut,
                ) -> None:
                    try:
                        f.set_result(await c)
                    except Exception as exc:
                        f.set_exception(exc)

                loop.create_task(_wrap())

        self._loop.call_soon_threadsafe(_callback)
        return futures


@dataclass(slots=True)
class _PartitionedBatch:
    """Result of partitioning a batch into dispatch groups."""

    sync_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    async_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    mounts: dict[str, list[tuple[int, Any]]] = field(default_factory=dict)
    unmatched: list[tuple[int, Any]] = field(default_factory=list)


def run_worker(
    handlers: "dict[int, Callable[[VertebraRequest], VertebraResponse]]",
    *,
    mounts: "list[tuple[str, Any, str]] | None" = None,
    batch_timeout_ms: int = 100,
    error_handler: "Callable[[Exception, VertebraRequest], VertebraResponse] | None" = None,
    worker_count: int | None = None,
) -> None:
    """Execute the batch runner loop with Unified Dispatch Architecture.

    A single IO Multiplexer thread is the sole batch consumer: it partitions
    each batch once, routes async work to the persistent ``CallbackScheduler``
    event loop, and routes sync work to a shared ``ThreadPoolExecutor``.
    Individual responses are submitted via ``submit_response()`` as they
    complete.
    """
    global _RUNNING, _wsgi_executor
    _ = error_handler
    from vertebra._vertebra import VertebraResponse as VR
    from vertebra._vertebra import get_batch, get_request_notify_fd, init_transport

    _install_signal_handlers()
    _RUNNING = True

    transport_mode = os.environ.get("VERTEBRA_TRANSPORT_MODE", "inprocess")
    notify_fd = get_request_notify_fd()
    is_shm = transport_mode == "shm" or notify_fd > -1

    if is_shm:
        if notify_fd <= -1:
            init_transport(mode="shm")
    else:
        init_transport()

    scheduler = CallbackScheduler()

    threads_env = os.environ.get("VERTEBRA_THREADS")
    default_threads = int(threads_env) if threads_env else (os.cpu_count() or 1)
    pool_size = worker_count if worker_count is not None else default_threads

    executor = ThreadPoolExecutor(
        max_workers=max(128, pool_size * 8),
        thread_name_prefix="vertebra-sync",
    )
    _wsgi_executor = executor

    resolved_mounts = _build_mount_adapters(mounts or [])
    active_mounts = _instantiate_mount_runners(resolved_mounts, scheduler.loop)

    try:
        if is_shm:
            asyncio.run(_shm_worker_loop(handlers, active_mounts, scheduler, batch_timeout_ms))
            return

        while _RUNNING:
            batch = get_batch(timeout_ms=batch_timeout_ms)
            if batch is None:
                continue

            partitioned = _partition_batch(batch, handlers, active_mounts)
            _dispatch_batch(
                partitioned,
                handlers,
                active_mounts,
                VR,
                scheduler,
                async_mode=True,
            )
    finally:
        _RUNNING = False
        scheduler.stop()
        if _wsgi_executor is not None:
            _wsgi_executor.shutdown(wait=False)
            _wsgi_executor = None


def _install_signal_handlers() -> None:
    """Install SIGTERM and SIGINT handlers for clean batch-boundary shutdown."""
    if threading.current_thread() is not threading.main_thread():
        return

    def _handle_signal(_signum: int, _frame: object) -> None:
        global _RUNNING
        _RUNNING = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def _detect_protocol(app: Any) -> str:
    """Detect whether an app is WSGI or ASGI by inspecting its callable signature."""
    if inspect.iscoroutinefunction(app):
        return "asgi"
    cls_call = type(app).__dict__.get("__call__")
    if cls_call is not None and inspect.iscoroutinefunction(cls_call):
        return "asgi"
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
    """Resolve protocol='auto' for each mount configuration tuple."""
    resolved: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in mounts:
        if protocol == "auto":
            protocol = _detect_protocol(app)
        resolved.append((prefix, app, protocol))
    return resolved


def _instantiate_mount_runners(
    resolved_mounts: list[tuple[str, Any, str]],
    loop: asyncio.AbstractEventLoop,
) -> list[tuple[str, Any, str]]:
    """Instantiate AsgiRunner and WsgiAdapter wrappers once for all resolved mounts."""
    from vertebra.server._asgi._runner import AsgiRunner
    from vertebra.server._wsgi._adapter import WsgiAdapter

    active_mounts: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in resolved_mounts:
        if protocol == "asgi":
            active_mounts.append((prefix, AsgiRunner(app, mount_prefix=prefix, loop=loop), "asgi"))
        elif protocol == "wsgi":
            active_mounts.append((prefix, WsgiAdapter(app), "wsgi"))
    return active_mounts


def _partition_batch(
    batch: "list[Any] | VertebraBatch",
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
) -> _PartitionedBatch:
    """Classify each request into sync, async, mount, or unmatched groups."""
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

        if mount_runners:
            path = request.path
            matched = False
            for prefix, _adapter, _protocol in mount_runners:
                if path.startswith(prefix):
                    result.mounts.setdefault(prefix, []).append((idx, request))
                    matched = True
                    break
            if matched:
                continue

        result.unmatched.append((idx, request))

    return result


def _get_wsgi_executor() -> ThreadPoolExecutor:
    """Return the shared WSGI thread pool, creating it on first use."""
    global _wsgi_executor
    if _wsgi_executor is None:
        pool_size = max(128, (os.cpu_count() or 1) * 8)
        executor = ThreadPoolExecutor(
            max_workers=pool_size,
            thread_name_prefix="vertebra-wsgi",
        )
        _wsgi_executor = executor
        return executor
    return _wsgi_executor


def _dispatch_wsgi_concurrent(
    adapter: Any,
    entries: list[tuple[int, Any]],
    results: list[Any],
    vr_cls: Any,
) -> None:
    """Dispatch WSGI requests concurrently via the shared thread pool."""
    if len(entries) == 1:
        idx, request = entries[0]
        try:
            results[idx] = adapter(request)
        except Exception:
            log.exception("WSGI handler error for request %s", request.request_id)
            results[idx] = vr_cls(status=500, body=b"internal server error", request_id=request.request_id)
        return

    executor = _get_wsgi_executor()
    indexed_futures = [(idx, request, executor.submit(adapter, request)) for idx, request in entries]

    for idx, request, future in indexed_futures:
        try:
            results[idx] = future.result()
        except Exception:
            log.exception("WSGI handler error for request %s", request.request_id)
            results[idx] = vr_cls(status=500, body=b"internal server error", request_id=request.request_id)


def _dispatch_batch(
    partitioned: _PartitionedBatch,
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
    vr_cls: Any,
    scheduler: CallbackScheduler | asyncio.AbstractEventLoop,
    *,
    async_mode: bool = False,
) -> list[Any] | None:
    """Dispatch a partitioned batch with concurrent execution.

    In blocking mode (default), returns responses in original batch order.
    In async mode, spawns fire-and-forget tasks and returns None.
    """
    _ = handlers
    loop = scheduler.loop if isinstance(scheduler, CallbackScheduler) else scheduler

    if not async_mode:
        total = (
            len(partitioned.sync_native)
            + len(partitioned.async_native)
            + sum(len(v) for v in partitioned.mounts.values())
            + len(partitioned.unmatched)
        )
        results: list[Any] = [None] * total

        for idx, request, handler in partitioned.sync_native:
            response = _call_handler(handler, request, loop=loop if partitioned.async_native else None)
            results[idx] = _coerce_response(response, request.request_id, vr_cls)

        if partitioned.async_native:
            coros = []
            for _idx, request, handler in partitioned.async_native:
                has_params, _ = _handler_meta(handler)
                if has_params:
                    coros.append(handler(*_resolve_handler_args(handler, request)))
                else:
                    coros.append(handler())

            if isinstance(scheduler, CallbackScheduler):
                futures = scheduler.schedule_batch(coros)
                concurrent.futures.wait(futures)
                for (idx, request, _handler), fut in zip(partitioned.async_native, futures, strict=True):
                    try:
                        raw = fut.result()
                        results[idx] = _coerce_response(raw, request.request_id, vr_cls)
                    except Exception:
                        log.exception("Async handler error for request %s", request.request_id)
                        results[idx] = vr_cls(
                            status=500,
                            body=b"internal server error",
                            request_id=request.request_id,
                        )
            else:
                for (idx, request, _handler), coro in zip(partitioned.async_native, coros, strict=True):
                    try:
                        raw = loop.run_until_complete(coro)
                        results[idx] = _coerce_response(raw, request.request_id, vr_cls)
                    except Exception:
                        log.exception("Async handler error for request %s", request.request_id)
                        results[idx] = vr_cls(
                            status=500,
                            body=b"internal server error",
                            request_id=request.request_id,
                        )

        if partitioned.mounts:
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

        for idx, request in partitioned.unmatched:
            results[idx] = vr_cls(status=404, body=b"not found", request_id=request.request_id)

        return results

    submit = _submit_response
    if submit is None:
        from vertebra._vertebra import submit_response as submit

    if partitioned.sync_native:
        executor = _get_wsgi_executor()
        for _idx, request, handler in partitioned.sync_native:

            def _dispatch_sync(req: Any = request, h: Any = handler) -> None:
                try:
                    response = _call_handler(h, req, loop=loop)
                    submit(_coerce_response(response, req.request_id, vr_cls))
                except Exception:
                    log.exception("Sync handler error for request %s", req.request_id)
                    submit(vr_cls(status=500, body=b"internal server error", request_id=req.request_id))

            executor.submit(_dispatch_sync)

    if partitioned.async_native:
        async_coros: list[Coroutine[Any, Any, None]] = []
        for _idx, request, handler in partitioned.async_native:

            async def _dispatch_async(req: Any = request, h: Any = handler) -> None:
                try:
                    has_params, _ = _handler_meta(h)
                    raw = await (h(*_resolve_handler_args(h, req)) if has_params else h())
                    submit(_coerce_response(raw, req.request_id, vr_cls))
                except Exception:
                    log.exception("Async handler error for request %s", req.request_id)
                    submit(
                        vr_cls(
                            status=500,
                            body=b"internal server error",
                            request_id=req.request_id,
                        )
                    )

            async_coros.append(_dispatch_async())

        if isinstance(scheduler, CallbackScheduler):
            scheduler.spawn_batch(async_coros)
        else:
            for coro in async_coros:
                loop.call_soon_threadsafe(loop.create_task, coro)

    if partitioned.mounts:
        mount_lookup = {prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners}
        for prefix, entries in partitioned.mounts.items():
            adapter, protocol = mount_lookup[prefix]
            if protocol == "asgi":
                adapter.spawn_batch(req for _idx, req in entries)
            elif protocol == "wsgi":
                executor = _get_wsgi_executor()
                for _idx, request in entries:

                    def _dispatch_wsgi(req: Any = request, a: Any = adapter) -> None:
                        try:
                            submit(a(req))
                        except Exception:
                            submit(vr_cls(status=500, body=b"internal server error", request_id=req.request_id))

                    executor.submit(_dispatch_wsgi)

    for _idx, request in partitioned.unmatched:
        submit(vr_cls(status=404, body=b"not found", request_id=request.request_id))

    return None


async def _shm_worker_loop(
    handlers: dict[int, Any],
    active_mounts: list[tuple[str, Any, str]],
    scheduler: CallbackScheduler,
    batch_timeout_ms: int,
) -> None:
    """Run the asyncio-driven SHM worker loop using eventfd notifications."""
    from vertebra._vertebra import (
        VertebraResponse as VR,
    )
    from vertebra._vertebra import (
        clear_request_notification,
        get_request_notify_fd,
        try_get_batch,
    )

    loop = asyncio.get_running_loop()
    notify_fd = get_request_notify_fd()
    ready_event = asyncio.Event()

    def _on_readable() -> None:
        ready_event.set()

    loop.add_reader(notify_fd, _on_readable)
    ready_event.set()

    try:
        while _RUNNING:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(ready_event.wait(), timeout=batch_timeout_ms / 1000.0)
            ready_event.clear()

            clear_request_notification()

            while True:
                batch = try_get_batch()
                if batch is None:
                    break

                partitioned = _partition_batch(batch, handlers, active_mounts)
                _dispatch_batch(
                    partitioned,
                    handlers,
                    active_mounts,
                    VR,
                    scheduler,
                    async_mode=True,
                )
    finally:
        loop.remove_reader(notify_fd)


def _is_free_threaded() -> bool:
    """Return True if the current Python build is free-threaded."""
    return bool(_NATIVE_FREE_THREADED)


@functools.lru_cache(maxsize=256)
def _handler_meta(handler: Any) -> tuple[bool, bool]:
    """Return (has_params, is_async) for a handler callable."""
    has_params = bool(inspect.signature(handler).parameters)
    is_async = inspect.iscoroutinefunction(handler)
    return has_params, is_async


@functools.lru_cache(maxsize=256)
def _handler_params(handler: Any) -> tuple[str, ...]:
    """Return cached parameter names for a handler callable."""
    return tuple(inspect.signature(handler).parameters.keys())


def _resolve_handler_args(handler: Any, request: Any) -> tuple[Any, ...]:
    """Build positional arguments for a handler based on its parameter names."""
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
            args.append(request)
    return tuple(args)


def _call_handler(handler: Any, request: Any, *, loop: asyncio.AbstractEventLoop | None = None) -> Any:
    """Call a handler, resolving parameters by name."""
    has_params, is_async = _handler_meta(handler)
    result = handler(*_resolve_handler_args(handler, request)) if has_params else handler()
    if is_async:
        if loop is None:
            msg = "async handler called without an event loop"
            raise RuntimeError(msg)
        return loop.run_until_complete(result)
    return result


def _coerce_response(raw: Any, request_id: int, vr_cls: Any) -> Any:
    """Turn a handler return value into a VertebraResponse."""
    if isinstance(raw, vr_cls):
        if raw.request_id == 0:
            try:
                raw.request_id = request_id
            except AttributeError:
                return vr_cls(
                    status=raw.status,
                    body=bytes(raw.body),
                    headers=getattr(raw, "headers", None),
                    request_id=request_id,
                )
        return raw
    if isinstance(raw, bytes):
        body = raw
    elif isinstance(raw, str):
        body = raw.encode("utf-8")
    else:
        body = encode_json(raw, as_bytes=True)
    return vr_cls(status=200, body=body, request_id=request_id)
