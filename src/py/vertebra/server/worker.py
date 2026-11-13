"""Worker lifecycle, signal draining, Litestar lifespan entry, and mount orchestration."""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import signal
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, suppress
from typing import TYPE_CHECKING, Any, Literal

from vertebra._vertebra import (
    Request as _NativeRequest,
)
from vertebra._vertebra import (
    RequestBatch,
    Response,
    cancel_request_sleep,
    clear_request_notification,
    drive_asgi_batch,
    drive_rsgi_batch,
    drive_wsgi_batch,
    get_batch,
    get_request_notify_fd,
    init_transport,
    prepare_request_sleep,
    submit_batch_raw,
    submit_response,
    submit_response_raw,
    submit_stream_frame,
    try_get_batch,
)
from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan
from vertebra.server._asgi._queues import AsgiResponse
from vertebra.server._asgi._runner import AsgiLifespan, AsgiRunner
from vertebra.server._dispatch import (
    _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS,
    _JSON_CONTENT_TYPE_COMPACT,
    _RAW_BUFFER_TYPES,
    HandlerSpec,
    _is_potential_special_return,
    _resolve_asgi_batch_timeout,
    _submit_coerced,
    build_error_response,
    compile_handler_table,
    dispatch_batch,
    partition_batch,
)
from vertebra.server._fastpath import attach_framework_fastpath
from vertebra.server._gil_audit import GilAuditReport
from vertebra.server._loops import (
    EventLoopPool,
    configure_worker_affinity,
    resolve_loop_shards,
)
from vertebra.server._protocol import InterfaceMode, detect_interface
from vertebra.server._rsgi._adapter import RsgiAdapter
from vertebra.server._wsgi._adapter import WsgiAdapter
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY, WorkerRunner
from vertebra.server.platform import (
    CgroupQuota,
    LameDuckController,
    SHMAllocationPolicy,
    ZPagesRegistry,
)
from vertebra.typing import VertebraError
from vertebra.utils.module_loader import import_app
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from vertebra.framework.queues.base import QueueService

__all__ = (
    "Worker",
    "WorkerStartupError",
    "_AsgiMountRunner",
    "_build_mount_adapters",
    "_instantiate_mount_runners",
    "run_worker",
)

logger = logging.getLogger(__name__)
_INLINE_DRAIN_BUDGET = 16


class WorkerStartupError(VertebraError, RuntimeError):
    """Raised when an ASGI lifespan or RSGI ``__rsgi_init__`` hook fails during worker startup."""

    def __init__(self, prefix: str, phase: Literal["lifespan", "rsgi_init"]) -> None:
        self.prefix = prefix
        self.phase = phase
        super().__init__(f"Worker startup failed during {phase} for mount {prefix!r}")


class _AsgiMountRunner:
    """Sharded ASGI mount runner dispatching across an ``EventLoopPool``."""

    __slots__ = ("_batch_timeout", "_pool", "_runner")

    def __init__(
        self,
        app: Any,
        pool: EventLoopPool,
        *,
        mount_prefix: str = "",
        lifespan_state: dict[str, Any] | None = None,
        batch_timeout: float | None = _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the sharded ASGI mount runner.

        Args:
            app: ASGI 3.0 application callable.
            pool: Sharded event loop pool.
            mount_prefix: Path prefix stripped before building the ASGI scope.
            lifespan_state: Shared lifespan state dictionary.
            batch_timeout: Seconds to wait for a buffered batch before answering 504 and
                cancelling the request tasks. ``None`` waits without a timeout.
        """
        self._pool = pool
        self._batch_timeout = batch_timeout
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

    @property
    def batch_timeout(self) -> float | None:
        """Return the batch timeout in seconds, or None when batches wait without a timeout."""
        return self._batch_timeout

    @property
    def __vertebra_fastpath_attached__(self) -> bool:
        """Return True when a zero-copy FastPath wrapper is attached to the underlying runner."""
        return self._runner.__vertebra_fastpath_attached__

    def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run a batch across the sharded loop pool, answering 504 for requests past ``batch_timeout``."""
        if not requests:
            return []
        coros = [self._runner.run_single(req) for req in requests]
        futures = self._pool.schedule_batch(coros)
        _done, not_done = concurrent.futures.wait(futures, timeout=self._batch_timeout)
        for fut in not_done:
            fut.cancel()
        results: list[AsgiResponse] = []
        for req, fut in zip(requests, futures, strict=True):
            req_id = getattr(req, "request_id", 0)
            if fut in not_done:
                results.append(
                    AsgiResponse(
                        status=504,
                        headers=[("content-type", "text/plain")],
                        body=b"Gateway Timeout",
                        request_id=req_id,
                    )
                )
                continue
            try:
                results.append(fut.result())
            except Exception:
                logger.exception("Sharded ASGI mount request failed (request_id=%d)", req_id)
                results.append(
                    AsgiResponse(
                        status=500,
                        headers=[("content-type", "text/plain")],
                        body=b"Internal Server Error",
                        request_id=req_id,
                    )
                )
        return results

    def spawn_batch(self, requests: RequestBatch | Iterable[Any]) -> None:
        """Schedule a batch across the sharded event loop pool via drive_asgi_batch."""
        if self._runner._on_chunk is None:
            if isinstance(requests, RequestBatch):
                if len(requests) == 0:
                    return
                drive_asgi_batch(
                    self._runner.dispatch_app,
                    requests,
                    loop_obj=self._pool.next_loop(),
                    root_path=self._runner.mount_prefix,
                    lifespan_state=self._runner.lifespan_state or None,
                )
                return

            req_list = requests if isinstance(requests, list | tuple) else list(requests)
            if not req_list:
                return
            if isinstance(req_list[0], _NativeRequest):
                drive_asgi_batch(
                    self._runner.dispatch_app,
                    req_list,
                    loop_obj=self._pool.next_loop(),
                    root_path=self._runner.mount_prefix,
                    lifespan_state=self._runner.lifespan_state or None,
                )
                return
            coros = [self._execute_and_submit(req) for req in req_list]
            self._pool.spawn_batch(coros)
            return

        coros = [self._execute_and_submit(req) for req in requests]
        if coros:
            self._pool.spawn_batch(coros)

    async def _execute_and_submit(self, request: Any) -> None:
        req_id = int(getattr(request, "request_id", 0))
        await self._runner._execute_and_submit(
            request,
            req_id,
            stream_submitter=submit_stream_frame,
            unary_submitter=submit_response,
        )


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
    configured_iface = _resolve_configured_interface()
    asgi_batch_timeout = _resolve_asgi_batch_timeout()
    result: list[tuple[str, Any, str]] = []
    for prefix, app, protocol in mounts:
        resolved: str = detect_interface(app, configured=configured_iface) if protocol == "auto" else protocol
        app = attach_framework_fastpath(app, mount_prefix=prefix, protocol=resolved)
        state = lifespan_states.get(prefix) if lifespan_states is not None else None
        if resolved == "asgi":
            if isinstance(scheduler, EventLoopPool) and scheduler.size > 1:
                runner: Any = _AsgiMountRunner(
                    app,
                    scheduler,
                    mount_prefix=prefix,
                    lifespan_state=state,
                    batch_timeout=asgi_batch_timeout,
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
                except Exception as exc:
                    logger.exception("RSGI __rsgi_init__ failed for mount %r", prefix)
                    raise WorkerStartupError(prefix, "rsgi_init") from exc
            adapter = RsgiAdapter(app, mount_prefix=prefix)
            result.append((prefix, adapter, "rsgi"))
        elif resolved == "wsgi":
            wsgi_adapter = WsgiAdapter(app, mount_prefix=prefix)
            result.append((prefix, wsgi_adapter, "wsgi"))
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
        "_app_obj",
        "_batch_timeout_ms",
        "_before_extensions",
        "_cgroup_quota",
        "_error_handler",
        "_gil_report",
        "_handlers",
        "_lame_duck",
        "_max_batches",
        "_mounts",
        "_queue_service",
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
        shm_policy: SHMAllocationPolicy | None = None,
        lame_duck: LameDuckController | None = None,
        zpages: ZPagesRegistry | None = None,
        stop_event: Any = None,
        max_batches: int | None = None,
        queue_service: QueueService | None = None,
        app_obj: Any = None,
    ) -> None:
        self._app_obj: Any = app_obj
        if hasattr(handlers, "_handlers") and not isinstance(handlers, Mapping):
            app_obj = handlers
            self._app_obj = app_obj
            ensure_fn = getattr(app_obj, "ensure_dev_welcome_route", None)
            if callable(ensure_fn):
                ensure_fn()
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
        self._shm_policy = shm_policy if shm_policy is not None else SHMAllocationPolicy()
        self._lame_duck = lame_duck if lame_duck is not None else LameDuckController()
        self._zpages = zpages if zpages is not None else ZPagesRegistry(lame_duck=self._lame_duck)
        self._stop_event = stop_event
        self._max_batches = max_batches
        self._queue_service = queue_service
        self._cgroup_quota: CgroupQuota | None = None
        self._gil_report: GilAuditReport | None = None

    @property
    def lame_duck(self) -> LameDuckController:
        """Return the worker's lame-duck drain controller."""
        return self._lame_duck

    @property
    def shm_policy(self) -> SHMAllocationPolicy:
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
        compiled_handlers = compile_handler_table(self._handlers)
        configured_iface = _resolve_configured_interface()
        resolved_mount_protocols = [
            detect_interface(app_obj, configured=configured_iface) if protocol == "auto" else protocol
            for _prefix, app_obj, protocol in self._mounts
        ]
        has_async_handlers = any(spec.is_async for spec in compiled_handlers.values())
        has_sync_thread = any(spec.sync_to_thread for spec in compiled_handlers.values())
        has_async_mounts = any(proto in {"asgi", "rsgi"} for proto in resolved_mount_protocols)
        has_colocated_workers = os.environ.get("VERTEBRA_WITH_WORKERS", "0").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        needs_event_loop = has_async_handlers or has_async_mounts or has_colocated_workers
        notify_fd = get_request_notify_fd()

        use_inline_event_loop = notify_fd >= 0 and shard_count == 1 and needs_event_loop
        loop_pool = EventLoopPool(
            size=shard_count,
            inline_single_loop=(shard_count == 1 and (use_inline_event_loop or not needs_event_loop)),
            gil_report=self._gil_report,
        )
        executor: ThreadPoolExecutor | None = (
            ThreadPoolExecutor(
                max_workers=max(16, self._worker_count * 8),
                thread_name_prefix="vertebra-worker-sync",
            )
            if has_sync_thread
            else None
        )

        colocated_runner: WorkerRunner | None = None
        colocated_future: concurrent.futures.Future[Any] | None = None
        lifespan_stack = ExitStack()
        lifespan_states: dict[str, dict[str, Any]] = {}

        try:
            for (_prefix, app_obj, _protocol), resolved in zip(self._mounts, resolved_mount_protocols, strict=True):
                if resolved == "asgi":
                    try:
                        lifespan_stack.enter_context(enter_litestar_server_lifespan(app_obj))
                        state = lifespan_stack.enter_context(AsgiLifespan(app_obj, loop=loop_pool.primary_loop))
                        lifespan_states[_prefix] = state
                    except Exception as exc:
                        logger.exception("ASGI lifespan startup failed for mount %r", _prefix)
                        raise WorkerStartupError(_prefix, "lifespan") from exc

            mount_runners = _instantiate_mount_runners(
                self._mounts,
                loop_pool,
                lifespan_states=lifespan_states,
            )
            if has_colocated_workers:
                queues_env = os.environ.get("VERTEBRA_WORKER_QUEUES", "default")
                lane_env = os.environ.get("VERTEBRA_WORKER_LANE_CONCURRENCY", "")
                lanes = WorkerRunner.parse_lane_concurrency(queues_env, lane_env)
                resolved_qs = self._resolve_colocated_queue_service(lifespan_states=lifespan_states)
                colocated_runner = WorkerRunner(
                    lanes,
                    queue_service=resolved_qs,
                    lame_duck=self._lame_duck,
                )
                if use_inline_event_loop:
                    loop_pool.primary_loop.create_task(colocated_runner.run())
                else:
                    colocated_future = loop_pool.schedule(colocated_runner.run())
            mount_lookup = {prefix: (adapter, proto) for prefix, adapter, proto in mount_runners}
            has_mounts = bool(mount_runners)
            all_sync_native = not has_mounts and not has_async_handlers and not has_sync_thread

            single_root_asgi: AsgiRunner | None = None
            single_root_rsgi: RsgiAdapter | None = None
            single_root_wsgi: WsgiAdapter | None = None
            if not compiled_handlers and len(mount_runners) == 1 and mount_runners[0][0] == "":
                root_adapter, root_proto = mount_runners[0][1], mount_runners[0][2]
                if root_proto == "asgi" and isinstance(root_adapter, AsgiRunner) and root_adapter._on_chunk is None:
                    single_root_asgi = root_adapter
                elif root_proto == "rsgi" and isinstance(root_adapter, RsgiAdapter):
                    single_root_rsgi = root_adapter
                elif root_proto == "wsgi" and isinstance(root_adapter, WsgiAdapter):
                    single_root_wsgi = root_adapter

            self._zpages.set_var("worker_state", "ready")
            self._zpages.set_var("worker_id", worker_idx)

            timeout_sec = max(0.001, self._batch_timeout_ms / 1000.0)

            if use_inline_event_loop:
                self._run_inline_event_loop(
                    notify_fd=notify_fd,
                    timeout_sec=timeout_sec,
                    compiled_handlers=compiled_handlers,
                    mount_runners=mount_runners,
                    mount_lookup=mount_lookup,
                    all_sync_native=all_sync_native,
                    single_root_asgi=single_root_asgi,
                    single_root_rsgi=single_root_rsgi,
                    single_root_wsgi=single_root_wsgi,
                    loop_pool=loop_pool,
                    executor=executor,
                )
            else:
                batches_processed = 0
                while not self._should_stop(batches_processed):
                    batch = get_batch(self._batch_timeout_ms)
                    if batch is None or len(batch) == 0:
                        continue

                    self._process_batch(
                        batch,
                        compiled_handlers=compiled_handlers,
                        mount_runners=mount_runners,
                        mount_lookup=mount_lookup,
                        all_sync_native=all_sync_native,
                        single_root_asgi=single_root_asgi,
                        single_root_rsgi=single_root_rsgi,
                        single_root_wsgi=single_root_wsgi,
                        loop_pool=loop_pool,
                        executor=executor,
                    )
                    batch = None
                    batches_processed += 1
        finally:
            self._zpages.set_var("worker_state", "stopped")
            if colocated_runner is not None:
                with suppress(Exception):
                    if use_inline_event_loop:
                        loop_pool.primary_loop.run_until_complete(colocated_runner.stop())
                    else:
                        loop_pool.schedule(colocated_runner.stop()).result(timeout=5.0)
                        if colocated_future is not None:
                            colocated_future.result(timeout=2.0)
            try:
                lifespan_stack.close()
            except Exception:
                logger.exception("ASGI lifespan shutdown error")
            if executor is not None:
                executor.shutdown(wait=False)
            loop_pool.stop()

    def _resolve_colocated_queue_service(
        self,
        *,
        lifespan_states: Mapping[str, Mapping[str, Any]],
    ) -> QueueService | None:
        """Resolve QueueService for co-located worker lanes from explicit arg, mounts, lifespan state, or VERTEBRA_APP."""
        if self._queue_service is not None:
            return WorkerRunner.resolve_queue_service(self._queue_service)

        for state in lifespan_states.values():
            if QUEUE_SERVICE_STATE_KEY in state:
                return WorkerRunner.resolve_queue_service(type("_StateHost", (), {"state": state})())

        first_stateful_target: Any = None
        for _prefix, mount_app, _protocol in self._mounts:
            mount_state = getattr(mount_app, "state", None)
            if mount_state is None:
                continue
            if first_stateful_target is None:
                first_stateful_target = mount_app
            has_key = (
                QUEUE_SERVICE_STATE_KEY in mount_state
                if isinstance(mount_state, Mapping)
                else hasattr(mount_state, QUEUE_SERVICE_STATE_KEY)
            )
            if has_key:
                return WorkerRunner.resolve_queue_service(mount_app)

        if self._app_obj is not None and getattr(self._app_obj, "state", None) is not None:
            app_state = self._app_obj.state
            has_key = (
                QUEUE_SERVICE_STATE_KEY in app_state
                if isinstance(app_state, Mapping)
                else hasattr(app_state, QUEUE_SERVICE_STATE_KEY)
            )
            if has_key or first_stateful_target is None:
                return WorkerRunner.resolve_queue_service(self._app_obj)

        if first_stateful_target is not None:
            return WorkerRunner.resolve_queue_service(first_stateful_target)

        if self._handlers:
            app_path = os.environ.get("VERTEBRA_APP", "").strip()
            if app_path:
                target_app = import_app(app_path)
                return WorkerRunner.resolve_queue_service(target_app)

        return None

    def _should_stop(self, batches_processed: int) -> bool:
        if self._stop_event is not None and self._stop_event.is_set():
            return True
        if self._lame_duck.is_draining and self._lame_duck.in_flight == 0:
            return True
        return self._max_batches is not None and batches_processed >= self._max_batches

    def _run_inline_event_loop(
        self,
        *,
        notify_fd: int,
        timeout_sec: float,
        compiled_handlers: dict[int, HandlerSpec],
        mount_runners: list[tuple[str, Any, str]],
        mount_lookup: dict[str, tuple[Any, str]],
        all_sync_native: bool,
        single_root_asgi: AsgiRunner | None,
        single_root_rsgi: RsgiAdapter | None,
        single_root_wsgi: WsgiAdapter | None = None,
        loop_pool: EventLoopPool,
        executor: ThreadPoolExecutor | None,
    ) -> None:
        loop = loop_pool.primary_loop
        asyncio.set_event_loop(loop)
        batches_processed = 0
        watchdog_sec = max(0.01, min(0.1, timeout_sec))

        def _drain_readable() -> None:
            nonlocal batches_processed
            cancel_request_sleep()
            clear_request_notification()
            drained = 0
            while drained < _INLINE_DRAIN_BUDGET:
                if self._should_stop(batches_processed):
                    loop.stop()
                    return
                batch = try_get_batch()
                if batch is None or len(batch) == 0:
                    if not prepare_request_sleep():
                        cancel_request_sleep()
                        loop.call_soon(_drain_readable)
                    elif self._should_stop(batches_processed):
                        cancel_request_sleep()
                        loop.stop()
                    return
                self._process_batch(
                    batch,
                    compiled_handlers=compiled_handlers,
                    mount_runners=mount_runners,
                    mount_lookup=mount_lookup,
                    all_sync_native=all_sync_native,
                    single_root_asgi=single_root_asgi,
                    single_root_rsgi=single_root_rsgi,
                    single_root_wsgi=single_root_wsgi,
                    loop_pool=loop_pool,
                    executor=executor,
                )
                batches_processed += 1
                drained += 1
            loop.call_soon(_drain_readable)

        def _watchdog() -> None:
            nonlocal batches_processed
            if self._should_stop(batches_processed):
                cancel_request_sleep()
                loop.stop()
                return
            batch = try_get_batch()
            if batch is not None and len(batch) > 0:
                cancel_request_sleep()
                self._process_batch(
                    batch,
                    compiled_handlers=compiled_handlers,
                    mount_runners=mount_runners,
                    mount_lookup=mount_lookup,
                    all_sync_native=all_sync_native,
                    single_root_asgi=single_root_asgi,
                    single_root_rsgi=single_root_rsgi,
                    single_root_wsgi=single_root_wsgi,
                    loop_pool=loop_pool,
                    executor=executor,
                )
                batches_processed += 1
                loop.call_soon(_drain_readable)
            loop.call_later(watchdog_sec, _watchdog)

        loop.add_reader(notify_fd, _drain_readable)
        loop.call_soon(_drain_readable)
        loop.call_later(watchdog_sec, _watchdog)
        try:
            loop.run_forever()
        finally:
            cancel_request_sleep()
            with suppress(Exception):
                loop.remove_reader(notify_fd)
            asyncio.set_event_loop(None)

    def _process_batch(
        self,
        batch: Any,
        *,
        compiled_handlers: dict[int, HandlerSpec],
        mount_runners: list[tuple[str, Any, str]],
        mount_lookup: dict[str, tuple[Any, str]],
        all_sync_native: bool,
        loop_pool: EventLoopPool,
        executor: ThreadPoolExecutor | None = None,
        single_root_asgi: AsgiRunner | None = None,
        single_root_rsgi: RsgiAdapter | None = None,
        single_root_wsgi: WsgiAdapter | None = None,
    ) -> None:
        batch_len = len(batch)
        self._lame_duck.acquire_request(batch_len)
        self._zpages.set_var("active_requests", self._lame_duck.in_flight)
        try:
            if all_sync_native:
                raw_items: list[tuple[int, int, Any] | tuple[int, int, Any, bytes]] = []
                append_raw = raw_items.append
                get_spec = compiled_handlers.get
                for req in batch:
                    req_id = req.request_id
                    spec = get_spec(req.handler_id)
                    if spec is None:
                        append_raw((req_id, 404, b"not found"))
                        continue
                    try:
                        raw = spec.invoker(req)
                        status = spec.status_code
                        if isinstance(raw, _RAW_BUFFER_TYPES):
                            append_raw((req_id, status, raw))
                        elif isinstance(raw, str):
                            append_raw((req_id, status, raw.encode("utf-8")))
                        elif isinstance(raw, Response):
                            if raw.request_id == 0:
                                raw.request_id = req_id
                            submit_response(raw)
                        elif _is_potential_special_return(raw):
                            if raw_items:
                                submit_batch_raw(raw_items)
                                raw_items.clear()
                            _submit_coerced(
                                raw,
                                req_id,
                                Response,
                                submit_response,
                                submit_response_raw,
                                status_code=status,
                                scheduler=loop_pool,
                            )
                        else:
                            append_raw((req_id, status, encode_json_bytes(raw), _JSON_CONTENT_TYPE_COMPACT))
                    except Exception as exc:
                        err_resp = build_error_response(exc, req, self._error_handler, Response)
                        submit_response(err_resp)
                if raw_items:
                    submit_batch_raw(raw_items)
                return

            if single_root_asgi is not None and isinstance(batch, RequestBatch):
                drive_asgi_batch(
                    single_root_asgi.dispatch_app,
                    batch,
                    loop_obj=loop_pool.primary_loop,
                    root_path="",
                    lifespan_state=single_root_asgi.lifespan_state or None,
                )
                return

            if single_root_rsgi is not None and isinstance(batch, RequestBatch):
                drive_rsgi_batch(
                    single_root_rsgi._target,
                    batch,
                    loop_obj=loop_pool.primary_loop,
                    root_path="",
                    scheme=single_root_rsgi._scheme,
                    server=single_root_rsgi._server,
                    client=single_root_rsgi._client,
                )
                return

            if single_root_wsgi is not None and isinstance(batch, RequestBatch):
                drive_wsgi_batch(
                    single_root_wsgi.app,
                    batch,
                    root_path="",
                )
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
    shm_policy: SHMAllocationPolicy | None = None,
    lame_duck: LameDuckController | None = None,
    zpages: ZPagesRegistry | None = None,
    stop_event: Any = None,
    max_batches: int | None = None,
    queue_service: QueueService | None = None,
    app_obj: Any = None,
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
        queue_service=queue_service,
        app_obj=app_obj,
    )
    worker.run()


def _resolve_configured_interface() -> InterfaceMode:
    """Resolve optional VERTEBRA_INTERFACE override when mount protocol is 'auto'."""
    raw = os.environ.get("VERTEBRA_INTERFACE", "auto").strip().lower()
    if raw == "asgi":
        return "asgi"
    if raw == "rsgi":
        return "rsgi"
    if raw == "wsgi":
        return "wsgi"
    return "auto"
