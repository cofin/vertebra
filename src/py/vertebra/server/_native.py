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

from vertebra.server._rsgi._adapter import RsgiAdapter, is_rsgi_app
from vertebra.server._runtime_tuning import (
    AsgiEventLoopPool,
    GilAuditReport,
    audit_gil_state,
    configure_worker_affinity,
    is_free_threaded,
    snapshot_native_extensions,
)
from vertebra.server._wsgi._adapter import WsgiAdapter
from vertebra.utils.serializers._json import encode_json_bytes

if TYPE_CHECKING:
    from vertebra._vertebra import Request, RequestBatch, Response

__all__ = (
    "AsgiEventLoopPool",
    "AsyncRuntime",
    "BatchDispatchPlan",
    "GilAuditReport",
    "audit_gil_state",
    "is_free_threaded",
    "run_worker",
    "snapshot_native_extensions",
)

_submit_response: Any = None
with contextlib.suppress(ImportError):
    from vertebra._vertebra import (
        submit_response as _submit_response,
    )

log = logging.getLogger(__name__)
_REQUEST_PARAMS = frozenset({"request", "req"})
_RUNNING = True
_wsgi_executor: ThreadPoolExecutor | None = None


class AsyncRuntime:
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
class BatchDispatchPlan:
    """Result of partitioning a batch into dispatch groups."""

    sync_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    async_native: list[tuple[int, Any, Any]] = field(default_factory=list)
    mounts: dict[str, list[tuple[int, Any]]] = field(default_factory=dict)
    unmatched: list[tuple[int, Any]] = field(default_factory=list)


def run_worker(
    handlers: "dict[int, Callable[[Request], Response]]",
    *,
    mounts: "list[tuple[str, Any, str]] | None" = None,
    batch_timeout_ms: int = 100,
    error_handler: "Callable[[Exception, Request], Response] | None" = None,
    worker_count: int | None = None,
) -> None:
    """Execute the batch runner loop with Unified Dispatch Architecture.

    A single IO Multiplexer thread is the sole batch consumer: it partitions
    each batch once, routes async work to the persistent ``AsyncRuntime``
    event loop, and routes sync work to a shared ``ThreadPoolExecutor``.
    Individual responses are submitted via ``submit_response()`` as they
    complete.
    """
    global _RUNNING, _wsgi_executor
    _ = error_handler
    from vertebra._vertebra import Response as VR
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
        ring_name = os.environ.get("VERTEBRA_RING_NAME", "default")
        slot_size = int(os.environ.get("VERTEBRA_SLOT_SIZE", "4096"))
        slots = int(os.environ.get("VERTEBRA_SLOTS", "64"))
        batch_size = int(os.environ.get("VERTEBRA_BATCH_SIZE", "64"))
        if ring_name == "default" and slot_size == 4096 and slots == 64 and batch_size == 64:
            init_transport()
        else:
            init_transport(
                mode="inprocess",
                ring_name=ring_name,
                slot_size=slot_size,
                slots=slots,
                batch_size=batch_size,
            )

    for handler in handlers.values():
        _handler_dispatch_spec(handler)

    threads_env = os.environ.get("VERTEBRA_THREADS")
    default_threads = int(threads_env) if threads_env else (os.cpu_count() or 1)
    pool_size = worker_count if worker_count is not None else default_threads

    enable_affinity = os.environ.get("VERTEBRA_CPU_AFFINITY", "0") in {"1", "true", "TRUE"}
    worker_id = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))
    configure_worker_affinity(worker_id, enable_pinning=enable_affinity)
    audit_gil_state()

    resolved_mounts = _build_mount_adapters(mounts or [])
    use_pool = pool_size > 1 or is_free_threaded()
    scheduler: AsyncRuntime | AsgiEventLoopPool | None = (
        AsgiEventLoopPool(size=pool_size) if use_pool else (None if is_shm else AsyncRuntime())
    )

    try:
        if is_shm:
            asyncio.run(_shm_worker_loop(handlers, resolved_mounts, scheduler, batch_timeout_ms))
            return

        assert scheduler is not None
        active_mounts = _instantiate_mount_runners(resolved_mounts, scheduler)
        mount_lookup = {prefix: (adapter, protocol) for prefix, adapter, protocol in active_mounts}

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
                mount_lookup=mount_lookup,
            )
    finally:
        _RUNNING = False
        if scheduler is not None:
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
    """Detect whether an app is RSGI, ASGI, or WSGI by inspecting its callable signature."""
    if is_rsgi_app(app):
        return "rsgi"
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


class _ShardedAsgiMountRunner:
    """Round-robin per-loop shard ASGI runner wrapper for AsgiEventLoopPool."""

    __slots__ = ("_idx", "_runners")

    def __init__(self, app: Any, *, mount_prefix: str, loops: tuple[asyncio.AbstractEventLoop, ...]) -> None:
        from vertebra.server._asgi._runner import AsgiRunner

        self._runners = tuple(AsgiRunner(app, mount_prefix=mount_prefix, loop=lp) for lp in loops)
        self._idx = 0

    def __call__(self, requests: list[Any]) -> list[Any]:
        runner = self._runners[self._idx % len(self._runners)]
        self._idx += 1
        return runner(requests)

    def spawn_batch(self, requests: Iterable[Any]) -> None:
        req_list = list(requests)
        if not req_list:
            return
        n_shards = len(self._runners)
        if n_shards == 1 or len(req_list) == 1:
            runner = self._runners[self._idx % n_shards]
            self._idx += 1
            runner.spawn_batch(req_list)
            return
        buckets: list[list[Any]] = [[] for _ in range(n_shards)]
        for pos, req in enumerate(req_list):
            buckets[(self._idx + pos) % n_shards].append(req)
        self._idx += len(req_list)
        for runner, bucket in zip(self._runners, buckets, strict=True):
            if bucket:
                runner.spawn_batch(bucket)


def _instantiate_mount_runners(
    resolved_mounts: list[tuple[str, Any, str]],
    loop_or_scheduler: asyncio.AbstractEventLoop | AsyncRuntime | AsgiEventLoopPool,
) -> list[tuple[str, Any, str]]:
    """Instantiate AsgiRunner, RsgiAdapter, and WsgiAdapter wrappers once for all resolved mounts."""
    from vertebra.server._asgi._runner import AsgiRunner

    primary_loop = (
        loop_or_scheduler.loop
        if isinstance(loop_or_scheduler, (AsyncRuntime, AsgiEventLoopPool))
        else loop_or_scheduler
    )
    shard_loops = loop_or_scheduler.loops if isinstance(loop_or_scheduler, AsgiEventLoopPool) else (primary_loop,)

    active_mounts: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in resolved_mounts:
        if protocol == "asgi":
            if len(shard_loops) > 1:
                runner: Any = _ShardedAsgiMountRunner(app, mount_prefix=prefix, loops=shard_loops)
            else:
                runner = AsgiRunner(app, mount_prefix=prefix, loop=primary_loop)
            active_mounts.append((prefix, runner, "asgi"))
        elif protocol == "rsgi":
            rsgi_adapter = RsgiAdapter(app)
            rsgi_adapter.startup(primary_loop)
            active_mounts.append((prefix, rsgi_adapter, "rsgi"))
        elif protocol == "wsgi":
            active_mounts.append((prefix, WsgiAdapter(app), "wsgi"))
    return active_mounts


def _partition_batch(
    batch: "list[Any] | RequestBatch",
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
) -> BatchDispatchPlan:
    """Classify each request into sync, async, mount, or unmatched groups."""
    result = BatchDispatchPlan()
    sync_native = result.sync_native
    async_native = result.async_native
    mounts_map = result.mounts
    unmatched = result.unmatched
    get_handler = handlers.get

    for idx, request in enumerate(batch):
        handler = get_handler(request.handler_id)
        if handler is not None:
            _, is_async, _, _ = _handler_dispatch_spec(handler)
            if is_async:
                async_native.append((idx, request, handler))
            else:
                sync_native.append((idx, request, handler))
            continue

        if mount_runners:
            path = request.path
            matched = False
            for prefix, _adapter, _protocol in mount_runners:
                if path.startswith(prefix):
                    mounts_map.setdefault(prefix, []).append((idx, request))
                    matched = True
                    break
            if matched:
                continue

        unmatched.append((idx, request))

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
    partitioned: BatchDispatchPlan,
    handlers: dict[int, Any],
    mount_runners: list[tuple[str, Any, str]],
    vr_cls: Any,
    scheduler: AsyncRuntime | AsgiEventLoopPool | asyncio.AbstractEventLoop,
    *,
    async_mode: bool = False,
    mount_lookup: dict[str, tuple[Any, str]] | None = None,
) -> list[Any] | None:
    """Dispatch a partitioned batch with concurrent execution.

    In blocking mode (default), returns responses in original batch order.
    In async mode, spawns fire-and-forget tasks and returns None.
    """
    _ = handlers
    loop = scheduler.loop if isinstance(scheduler, (AsyncRuntime, AsgiEventLoopPool)) else scheduler

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
                _, _, _, invoker = _handler_dispatch_spec(handler)
                coros.append(invoker(request))

            if isinstance(scheduler, (AsyncRuntime, AsgiEventLoopPool)):
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
            resolved_lookup = mount_lookup or {
                prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners
            }
            for prefix, entries in partitioned.mounts.items():
                adapter, protocol = resolved_lookup[prefix]
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
                elif protocol == "rsgi":
                    for idx, request in entries:
                        coro = adapter.handle_request(request)
                        if isinstance(scheduler, (AsyncRuntime, AsgiEventLoopPool)):
                            resp = scheduler.schedule(coro).result()
                        elif loop.is_running():
                            resp = asyncio.run_coroutine_threadsafe(coro, loop).result()
                        else:
                            resp = loop.run_until_complete(coro)
                        results[idx] = _coerce_response(resp, request.request_id, vr_cls)
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
            _, _, _, invoker = _handler_dispatch_spec(handler)

            def _dispatch_sync(req: Any = request, inv: Any = invoker) -> None:
                try:
                    response = inv(req)
                    submit(_coerce_response(response, req.request_id, vr_cls))
                except Exception:
                    log.exception("Sync handler error for request %s", req.request_id)
                    submit(vr_cls(status=500, body=b"internal server error", request_id=req.request_id))

            executor.submit(_dispatch_sync)

    if partitioned.async_native:
        async_coros: list[Coroutine[Any, Any, None]] = []
        for _idx, request, handler in partitioned.async_native:
            _, _, _, invoker = _handler_dispatch_spec(handler)

            async def _dispatch_async(req: Any = request, inv: Any = invoker) -> None:
                try:
                    raw = await inv(req)
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

        if isinstance(scheduler, (AsyncRuntime, AsgiEventLoopPool)):
            scheduler.spawn_batch(async_coros)
        elif asyncio._get_running_loop() is loop:
            for coro in async_coros:
                loop.create_task(coro)
        else:
            for coro in async_coros:
                loop.call_soon_threadsafe(loop.create_task, coro)

    if partitioned.mounts:
        resolved_lookup = mount_lookup or {prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners}
        for prefix, entries in partitioned.mounts.items():
            adapter, protocol = resolved_lookup[prefix]
            if protocol == "asgi":
                adapter.spawn_batch(req for _idx, req in entries)
            elif protocol == "rsgi":
                rsgi_coros: list[Coroutine[Any, Any, None]] = []
                for _idx, request in entries:

                    async def _dispatch_rsgi(req: Any = request, a: Any = adapter) -> None:
                        try:
                            resp = await a.handle_request(req)
                            submit(_coerce_response(resp, req.request_id, vr_cls))
                        except Exception:
                            log.exception("RSGI handler error for request %s", req.request_id)
                            submit(
                                vr_cls(
                                    status=500,
                                    body=b"internal server error",
                                    request_id=req.request_id,
                                )
                            )

                    rsgi_coros.append(_dispatch_rsgi())

                if isinstance(scheduler, (AsyncRuntime, AsgiEventLoopPool)):
                    scheduler.spawn_batch(rsgi_coros)
                elif asyncio._get_running_loop() is loop:
                    for coro in rsgi_coros:
                        loop.create_task(coro)
                else:
                    for coro in rsgi_coros:
                        loop.call_soon_threadsafe(loop.create_task, coro)
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
    mounts: list[tuple[str, Any, str]],
    scheduler: AsyncRuntime | AsgiEventLoopPool | None,
    batch_timeout_ms: int,
) -> None:
    """Run the asyncio-driven SHM worker loop using eventfd notifications."""
    from vertebra._vertebra import (
        Response as VR,
    )
    from vertebra._vertebra import (
        clear_request_notification,
        get_request_notify_fd,
        try_get_batch,
    )

    loop = asyncio.get_running_loop()
    notify_fd = get_request_notify_fd()
    ready_event = asyncio.Event()
    dispatch_target: AsyncRuntime | AsgiEventLoopPool | asyncio.AbstractEventLoop = (
        scheduler if isinstance(scheduler, AsgiEventLoopPool) else loop
    )
    active_mounts = (
        mounts
        if all(
            protocol in {"asgi", "rsgi", "wsgi"}
            and type(adapter).__name__ in {"AsgiRunner", "_ShardedAsgiMountRunner", "RsgiAdapter", "WsgiAdapter"}
            for _p, adapter, protocol in mounts
        )
        else _instantiate_mount_runners(mounts, dispatch_target)
    )
    mount_lookup = {prefix: (adapter, protocol) for prefix, adapter, protocol in active_mounts}

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
                    dispatch_target,
                    async_mode=True,
                    mount_lookup=mount_lookup,
                )
    finally:
        loop.remove_reader(notify_fd)


def _extract_request(request: Any) -> Any:
    return request


def _extract_body(request: Any) -> bytes:
    body = request.body
    return body if isinstance(body, bytes) else bytes(body or b"")


def _extract_headers(request: Any) -> Any:
    return request.headers


def _extract_method(request: Any) -> Any:
    return request.method


def _extract_path(request: Any) -> Any:
    return request.path


def _extract_query(request: Any) -> Any:
    return request.query


_PARAM_EXTRACTORS: dict[str, Callable[[Any], Any]] = {
    "request": _extract_request,
    "req": _extract_request,
    "body": _extract_body,
    "headers": _extract_headers,
    "method": _extract_method,
    "path": _extract_path,
    "query": _extract_query,
}


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


def _handler_meta(handler: Any) -> tuple[bool, bool]:
    """Return ``(has_params, is_async)`` for a handler callable."""
    has_params, is_async, _, _ = _handler_dispatch_spec(handler)
    return has_params, is_async


def _handler_params(handler: Any) -> tuple[str, ...]:
    """Return cached parameter names for a handler callable."""
    return _handler_dispatch_spec(handler)[2]


def _resolve_handler_args(handler: Any, request: Any) -> tuple[Any, ...]:
    """Build positional arguments for a handler based on its parameter names."""
    params = _handler_dispatch_spec(handler)[2]
    if not params:
        return ()
    return tuple(_PARAM_EXTRACTORS.get(name, _extract_request)(request) for name in params)


def _call_handler(handler: Any, request: Any, *, loop: asyncio.AbstractEventLoop | None = None) -> Any:
    """Call a handler, resolving parameters by name."""
    _, is_async, _, invoker = _handler_dispatch_spec(handler)
    result = invoker(request)
    if is_async:
        if loop is None:
            msg = "async handler called without an event loop"
            raise RuntimeError(msg)
        return loop.run_until_complete(result)
    return result


def _coerce_response(raw: Any, request_id: int, vr_cls: Any) -> Any:
    """Turn a handler return value into a Response."""
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
        body = encode_json_bytes(raw)
    return vr_cls(status=200, body=body, request_id=request_id)
