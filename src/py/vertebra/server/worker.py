"""Worker lifecycle, signal draining, Litestar lifespan entry, and mount orchestration."""

import asyncio
import concurrent.futures
import logging
import os
import selectors
import signal
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import Any

from vertebra._vertebra import (
    Response,
    clear_request_notification,
    get_batch,
    get_request_notify_fd,
    init_transport,
    submit_response,
    submit_response_raw,
    try_get_batch,
)
from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan
from vertebra.server._asgi._queues import AsgiResponse
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._dispatch import (
    HandlerSpec,
    build_error_response,
    compile_handler_table,
    dispatch_batch,
    partition_batch,
)
from vertebra.server._gil_audit import GilAuditReport
from vertebra.server._loops import (
    EventLoopPool,
    configure_worker_affinity,
    resolve_loop_shards,
)
from vertebra.server._protocol import detect_interface
from vertebra.server._rsgi._adapter import RsgiAdapter
from vertebra.server._wsgi._adapter import WsgiAdapter
from vertebra.server.platform import (
    CgroupQuota,
    LameDuckController,
    ShmAllocationPolicy,
    ZPagesRegistry,
)

__all__ = (
    "Worker",
    "_AsgiMountRunner",
    "_build_mount_adapters",
    "_instantiate_mount_runners",
    "run_worker",
)

logger = logging.getLogger(__name__)


class _AsgiMountRunner:
    """Sharded ASGI mount runner dispatching across an ``EventLoopPool``."""

    __slots__ = ("_pool", "_runner")

    def __init__(
        self,
        app: Any,
        pool: EventLoopPool,
        *,
        mount_prefix: str = "",
        lifespan_state: dict[str, Any] | None = None,
    ) -> None:
        self._pool = pool
        self._runner = AsgiRunner(
            app,
            loop=pool.primary_loop,
            mount_prefix=mount_prefix,
            lifespan_state=lifespan_state,
        )

    @property
    def runner(self) -> AsgiRunner:
        """Return the underlying ``AsgiRunner``."""
        return self._runner

    def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run a batch of requests across the sharded event loop pool."""
        if not requests:
            return []
        coros = [self._runner.run_single(req) for req in requests]
        futures = self._pool.schedule_batch(coros)
        concurrent.futures.wait(futures)
        results: list[AsgiResponse] = []
        for req, fut in zip(requests, futures, strict=True):
            try:
                results.append(fut.result())
            except Exception:
                req_id = getattr(req, "request_id", 0)
                results.append(
                    AsgiResponse(
                        status=500,
                        headers=[("content-type", "text/plain")],
                        body=b"Internal Server Error",
                        request_id=req_id,
                    )
                )
        return results

    def spawn_batch(self, requests: Iterable[Any]) -> None:
        """Schedule each request in ``requests`` across the sharded event loop pool."""
        coros = [self._execute_and_submit(req) for req in requests]
        if coros:
            self._pool.spawn_batch(coros)

    async def _execute_and_submit(self, request: Any) -> None:
        req_id = int(getattr(request, "request_id", 0))
        try:
            resp = await self._runner.run_single(request)
            native_resp = self._runner._to_native_response(resp, req_id)
            submit_response(native_resp)
        except Exception:
            logger.exception("Unhandled error in sharded ASGI mount task (request_id=%d)", req_id)
            submit_response(Response.error(500, req_id))


def _instantiate_mount_runners(
    mounts: Sequence[tuple[str, Any, str]],
    scheduler: Any = None,
    *,
    lifespan_states: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, Any, str]]:
    """Build ``(prefix, runner_callable, resolved_protocol)`` tuples for each mount."""
    primary_loop = (
        scheduler.primary_loop
        if hasattr(scheduler, "primary_loop")
        else (scheduler.loop if hasattr(scheduler, "loop") else scheduler)
    )
    result: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in mounts:
        resolved: str = detect_interface(app) if protocol == "auto" else protocol
        state = lifespan_states.get(prefix) if lifespan_states is not None else None
        if resolved == "asgi":
            if isinstance(scheduler, EventLoopPool) and scheduler.size > 1:
                runner: Any = _AsgiMountRunner(
                    app,
                    scheduler,
                    mount_prefix=prefix,
                    lifespan_state=state,
                )
            else:
                runner = AsgiRunner(
                    app,
                    loop=primary_loop if isinstance(primary_loop, asyncio.AbstractEventLoop) else None,
                    mount_prefix=prefix,
                    lifespan_state=state,
                )
            result.append((prefix, runner, "asgi"))
        elif resolved == "rsgi":
            init_fn = getattr(app, "__rsgi_init__", None)
            if callable(init_fn) and isinstance(primary_loop, asyncio.AbstractEventLoop):
                try:
                    init_fn(primary_loop)
                except Exception:
                    logger.exception("RSGI __rsgi_init__ failed for mount %r", prefix)
            adapter = RsgiAdapter(app, mount_prefix=prefix)
            result.append((prefix, adapter, "rsgi"))
        elif resolved == "wsgi":
            wsgi_adapter = WsgiAdapter(app, mount_prefix=prefix)
            result.append((prefix, wsgi_adapter.handle_request, "wsgi"))
        else:
            msg = f"Unknown mount protocol: {resolved!r}"
            raise ValueError(msg)
    return result


def _build_mount_adapters(
    mounts: Sequence[tuple[str, Any, str]],
    loop: Any = None,
) -> list[tuple[str, Any, str]]:
    """Instantiate mount adapters for ``mounts`` using ``loop`` or an ``EventLoopPool``."""
    return _instantiate_mount_runners(mounts, loop)


class Worker:
    """Single-responsibility worker orchestrating SHM validation, loop pools, and batch dispatch."""

    __slots__ = (
        "_batch_timeout_ms",
        "_before_extensions",
        "_cgroup_quota",
        "_error_handler",
        "_gil_report",
        "_handlers",
        "_lame_duck",
        "_max_batches",
        "_mounts",
        "_shm_policy",
        "_stop_event",
        "_worker_count",
        "_zpages",
    )

    def __init__(
        self,
        handlers: Mapping[int, Callable[..., Any]] | Any,
        *,
        mounts: Sequence[tuple[str, Any, str]] | None = None,
        batch_timeout_ms: int = 100,
        error_handler: Callable[..., Any] | None = None,
        worker_count: int = 1,
        before_extensions: frozenset[str] | None = None,
        shm_policy: ShmAllocationPolicy | None = None,
        lame_duck: LameDuckController | None = None,
        zpages: ZPagesRegistry | None = None,
        stop_event: Any = None,
        max_batches: int | None = None,
    ) -> None:
        if hasattr(handlers, "_handlers") and not isinstance(handlers, Mapping):
            app_obj = handlers
            self._handlers: Mapping[int, Callable[..., Any]] = getattr(app_obj, "_handlers", {})
            if mounts is None:
                raw_mounts = getattr(app_obj, "mounts", None)
                if isinstance(raw_mounts, Sequence):
                    mounts = list(raw_mounts)
            if error_handler is None:
                error_handler = getattr(app_obj, "error_handler", None)
        else:
            self._handlers = handlers

        self._mounts: list[tuple[str, Any, str]] = list(mounts) if mounts else []
        self._batch_timeout_ms = batch_timeout_ms
        self._error_handler = error_handler
        self._worker_count = worker_count
        self._before_extensions = before_extensions
        self._shm_policy = shm_policy if shm_policy is not None else ShmAllocationPolicy()
        self._lame_duck = lame_duck if lame_duck is not None else LameDuckController()
        self._zpages = zpages if zpages is not None else ZPagesRegistry(lame_duck=self._lame_duck)
        self._stop_event = stop_event
        self._max_batches = max_batches
        self._cgroup_quota: CgroupQuota | None = None
        self._gil_report: GilAuditReport | None = None

    @property
    def lame_duck(self) -> LameDuckController:
        """Return the worker's lame-duck drain controller."""
        return self._lame_duck

    @property
    def shm_policy(self) -> ShmAllocationPolicy:
        """Return the worker's shared-memory allocation policy."""
        return self._shm_policy

    @property
    def zpages(self) -> ZPagesRegistry:
        """Return the worker's zPages diagnostics registry."""
        return self._zpages

    @property
    def cgroup_quota(self) -> CgroupQuota | None:
        """Return the detected cgroup v2 CPU quota."""
        return self._cgroup_quota

    @property
    def gil_report(self) -> GilAuditReport | None:
        """Return the free-threaded GIL audit report collected at startup."""
        return self._gil_report

    def handle_sigterm(self, signum: int = signal.SIGTERM, frame: Any = None) -> None:
        """Enter lame-duck draining mode and validate SHM policy on SIGTERM/SIGINT."""
        del signum, frame
        self._shm_policy.validate_environment()
        self._lame_duck.enter_lame_duck()
        self._zpages.set_var("worker_state", "draining")
        if self._stop_event is not None and hasattr(self._stop_event, "set"):
            self._stop_event.set()

    def run(self) -> None:
        """Validate the environment, initialize transport and loops, and run the batch loop."""
        self._shm_policy.validate_environment()
        self._cgroup_quota = CgroupQuota.detect()
        self._gil_report = GilAuditReport.audit(before_extensions=self._before_extensions)

        worker_idx = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))
        pin_enabled = os.environ.get("VERTEBRA_PIN_AFFINITY", "0") in {"1", "true", "TRUE"}
        configure_worker_affinity(worker_idx, enable_pinning=pin_enabled)

        transport_mode = os.environ.get("VERTEBRA_TRANSPORT_MODE", "inprocess")
        slot_size = int(os.environ.get("VERTEBRA_SLOT_SIZE", "4096"))
        slots = int(os.environ.get("VERTEBRA_SLOTS", "64"))
        batch_size = int(os.environ.get("VERTEBRA_BATCH_SIZE", "64"))

        init_transport(
            mode=transport_mode,
            slot_size=slot_size,
            slots=slots,
            batch_size=batch_size,
        )

        shard_count = resolve_loop_shards(audit_report=self._gil_report)
        loop_pool = EventLoopPool(size=shard_count)
        executor = ThreadPoolExecutor(
            max_workers=max(32, self._worker_count * 16),
            thread_name_prefix="vertebra-worker-sync",
        )

        compiled_handlers = compile_handler_table(self._handlers)
        lifespan_stack = ExitStack()
        lifespan_states: dict[str, dict[str, Any]] = {}

        try:
            for prefix, app_obj, protocol in self._mounts:
                resolved = detect_interface(app_obj) if protocol == "auto" else protocol
                if resolved == "asgi":
                    try:
                        lifespan_stack.enter_context(enter_litestar_server_lifespan(app_obj))
                    except Exception:
                        logger.exception("ASGI lifespan startup failed for mount %r", prefix)

            mount_runners = _instantiate_mount_runners(
                self._mounts,
                loop_pool,
                lifespan_states=lifespan_states,
            )
            mount_lookup = {prefix: (adapter, proto) for prefix, adapter, proto in mount_runners}
            has_mounts = bool(mount_runners)
            all_sync_native = not has_mounts and all(not spec.is_async for spec in compiled_handlers.values())

            self._zpages.set_var("worker_state", "ready")
            self._zpages.set_var("worker_id", worker_idx)

            notify_fd = get_request_notify_fd()
            selector: selectors.BaseSelector | None = None
            if notify_fd >= 0:
                try:
                    sel = selectors.DefaultSelector()
                    sel.register(notify_fd, selectors.EVENT_READ)
                    selector = sel
                except Exception:
                    selector = None

            timeout_sec = max(0.001, self._batch_timeout_ms / 1000.0)
            batches_processed = 0

            try:
                while True:
                    if self._stop_event is not None and self._stop_event.is_set():
                        break
                    if self._lame_duck.is_draining and self._lame_duck.in_flight == 0:
                        break
                    if self._max_batches is not None and batches_processed >= self._max_batches:
                        break

                    if selector is not None:
                        batch = try_get_batch()
                        if batch is None or len(batch) == 0:
                            events = selector.select(timeout=timeout_sec)
                            if events:
                                clear_request_notification()
                            batch = try_get_batch()
                            if batch is None or len(batch) == 0:
                                continue
                    else:
                        batch = get_batch(self._batch_timeout_ms)
                        if batch is None or len(batch) == 0:
                            continue

                    self._process_batch(
                        batch,
                        compiled_handlers=compiled_handlers,
                        mount_runners=mount_runners,
                        mount_lookup=mount_lookup,
                        all_sync_native=all_sync_native,
                        loop_pool=loop_pool,
                        executor=executor,
                    )
                    batches_processed += 1
            finally:
                if selector is not None:
                    selector.close()
        finally:
            self._zpages.set_var("worker_state", "stopped")
            try:
                lifespan_stack.close()
            except Exception:
                logger.exception("ASGI lifespan shutdown error")
            executor.shutdown(wait=False)
            loop_pool.stop()

    def _process_batch(
        self,
        batch: Any,
        *,
        compiled_handlers: dict[int, HandlerSpec],
        mount_runners: list[tuple[str, Any, str]],
        mount_lookup: dict[str, tuple[Any, str]],
        all_sync_native: bool,
        loop_pool: EventLoopPool,
        executor: ThreadPoolExecutor,
    ) -> None:
        batch_len = len(batch)
        self._lame_duck.acquire_request(batch_len)
        self._zpages.set_var("active_requests", self._lame_duck.in_flight)
        try:
            if all_sync_native and batch_len == 1:
                req = batch[0]
                spec = compiled_handlers.get(req.handler_id)
                if spec is not None:
                    try:
                        raw = spec.invoker(req)
                        if isinstance(raw, bytes):
                            submit_response_raw(req.request_id, 200, raw)
                            return
                        if isinstance(raw, str):
                            submit_response_raw(req.request_id, 200, raw.encode("utf-8"))
                            return
                    except Exception as exc:
                        err_resp = build_error_response(exc, req, self._error_handler, Response)
                        submit_response(err_resp)
                        return

            partitioned = partition_batch(batch, compiled_handlers, mount_runners)
            dispatch_batch(
                partitioned,
                compiled_handlers,
                mount_runners,
                Response,
                loop_pool,
                async_mode=True,
                mount_lookup=mount_lookup,
                submit=submit_response,
                submit_raw=submit_response_raw,
                error_handler=self._error_handler,
                executor=executor,
            )
        finally:
            self._lame_duck.release_request(batch_len)
            self._zpages.set_var("active_requests", self._lame_duck.in_flight)


def run_worker(
    handlers: Mapping[int, Callable[..., Any]] | Any,
    *,
    mounts: Sequence[tuple[str, Any, str]] | None = None,
    batch_timeout_ms: int = 100,
    error_handler: Callable[..., Any] | None = None,
    worker_count: int = 1,
    before_extensions: frozenset[str] | None = None,
    shm_policy: ShmAllocationPolicy | None = None,
    lame_duck: LameDuckController | None = None,
    zpages: ZPagesRegistry | None = None,
    stop_event: Any = None,
    max_batches: int | None = None,
) -> None:
    """Entry point that constructs and runs a ``Worker``."""
    worker = Worker(
        handlers,
        mounts=mounts,
        batch_timeout_ms=batch_timeout_ms,
        error_handler=error_handler,
        worker_count=worker_count,
        before_extensions=before_extensions,
        shm_policy=shm_policy,
        lame_duck=lame_duck,
        zpages=zpages,
        stop_event=stop_event,
        max_batches=max_batches,
    )
    worker.run()
