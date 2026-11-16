"""Multi-lane worker runner, admission controller, and OpenTelemetry instrumentation."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import signal
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

import msgspec

from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanData, SpanExporter, TraceContext
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from vertebra.framework.di import Container
    from vertebra.framework.queues.base import QueueService, TaskEnvelope, TaskLane
    from vertebra.framework.queues.scheduler import CronScheduler

__all__ = (
    "QUEUE_SERVICE_STATE_KEY",
    "LaneConfig",
    "LaneMetrics",
    "WorkerRunner",
    "parse_lane_concurrency",
    "parse_lanes",
    "run_oneshot",
)

QUEUE_SERVICE_STATE_KEY: Final[str] = "vertebra_queue_service"

log = logging.getLogger(__name__)


class LaneConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Per-queue worker lane concurrency and drain timeout configuration."""

    queue_name: str
    """Logical queue name bound to this execution lane."""
    concurrency: int = 10
    """Maximum number of tasks allowed to execute concurrently in this lane."""
    drain_timeout_s: float = 30.0
    """Grace period in seconds to await in-flight tasks during lame-duck shutdown."""
    lane_kind: TaskLane = "io"
    """Workload scheduling class (`"io"`, `"cpu"`, or `"agent"`)."""


class LaneMetrics(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Observability metrics for a single worker execution lane."""

    queue_name: str
    """Logical queue name tracked by these metrics."""
    in_flight: int = 0
    """Number of tasks currently executing in the lane."""
    processed_total: int = 0
    """Cumulative count of tasks that completed without error."""
    failed_total: int = 0
    """Cumulative count of task executions that raised an exception."""
    dlq_total: int = 0
    """Cumulative count of tasks moved to the dead-letter queue after exhausting retries."""


class WorkerRunner:
    """Admission-controlled multi-lane worker runner with QueueService integration and lame-duck drain."""

    __slots__ = (
        "_container",
        "_heartbeat_interval_seconds",
        "_in_flight",
        "_lame_duck",
        "_lanes",
        "_metrics",
        "_queue_service",
        "_scheduler",
        "_semaphores",
        "_span_exporter",
        "_stopping",
    )

    def __init__(
        self,
        lanes: Sequence[LaneConfig],
        *,
        queue_service: QueueService | None = None,
        lame_duck: LameDuckController | None = None,
        span_exporter: SpanExporter | None = None,
        scheduler: CronScheduler | None = None,
        container: Container | None = None,
        heartbeat_interval_seconds: float = 10.0,
    ) -> None:
        self._lanes: dict[str, LaneConfig] = {lane.queue_name: lane for lane in lanes}
        self._semaphores: dict[str, asyncio.Semaphore] = {
            lane.queue_name: asyncio.Semaphore(lane.concurrency) for lane in lanes
        }
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._stopping = False
        self._queue_service = queue_service
        self._lame_duck = lame_duck or LameDuckController()
        self._span_exporter = span_exporter
        self._scheduler = scheduler
        self._container = container
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._metrics: dict[str, LaneMetrics] = {
            lane.queue_name: LaneMetrics(queue_name=lane.queue_name) for lane in lanes
        }

    @property
    def lanes(self) -> dict[str, LaneConfig]:
        """Return configured worker lanes keyed by queue name."""
        return dict(self._lanes)

    @property
    def lame_duck(self) -> LameDuckController:
        """Return the bound LameDuckController."""
        return self._lame_duck

    @property
    def queue_service(self) -> QueueService | None:
        """Return the bound QueueService if configured."""
        return self._queue_service

    @property
    def scheduler(self) -> CronScheduler | None:
        """Return the bound CronScheduler if configured."""
        return self._scheduler

    @property
    def container(self) -> Container | None:
        """Return the bound Container if configured."""
        return self._container

    @property
    def heartbeat_interval_seconds(self) -> float:
        """Return the interval in seconds between visibility lease heartbeats."""
        return self._heartbeat_interval_seconds

    @property
    def is_stopping(self) -> bool:
        """Return whether runner shutdown has commenced."""
        return self._stopping

    def submit(
        self,
        queue_name: str,
        job_factory: Callable[[], Awaitable[Any] | Any],
    ) -> asyncio.Task[Any]:
        """Schedule a job onto the specified queue lane bounded by that lane's semaphore."""
        if self._stopping:
            msg = f"Cannot submit job to lane {queue_name!r} while runner is stopping"
            raise RuntimeError(msg)
        semaphore = self._semaphores.get(queue_name)
        if semaphore is None:
            semaphore = asyncio.Semaphore(10)
            self._semaphores[queue_name] = semaphore

        async def _guarded() -> Any:
            async with semaphore:
                res = job_factory()
                if inspect.isawaitable(res):
                    return await res
                return res

        task = asyncio.create_task(_guarded())
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def process_once(self) -> int:
        """Pull and process a single task batch across configured lanes, recording telemetry."""
        if self._queue_service is None:
            return 0

        target_lanes: list[TaskLane] = []
        for lane in self._lanes.values():
            if lane.lane_kind not in target_lanes:
                target_lanes.append(lane.lane_kind)
        if not target_lanes:
            target_lanes = ["cpu", "io", "agent"]

        envelopes = await self._queue_service.backend.dequeue(target_lanes, batch_size=1)
        if not envelopes:
            return 0

        env = envelopes[0]
        semaphore = self._semaphores.get(env.lane)
        if semaphore is None:
            semaphore = asyncio.Semaphore(10)
            self._semaphores[env.lane] = semaphore

        async with semaphore:
            return await self._process_envelope_guarded(env)

    async def _process_envelope_guarded(self, env: TaskEnvelope) -> int:
        """Execute single task envelope with lease heartbeats, scoped DI, and OpenTelemetry spans."""
        start_nano = time.time_ns()
        parent_ctx = TraceContext.parse(env.traceparent) if env.traceparent else None
        active_trace = parent_ctx.new_child() if parent_ctx else TraceContext.generate()
        token = TraceContext.set_current(active_trace)

        status = "ok"
        err_str: str | None = None
        assert self._queue_service is not None
        backend = self._queue_service.backend
        store = getattr(backend, "store", None) or getattr(backend, "_store", None)
        heartbeat_task: asyncio.Task[None] | None = None
        stop_heartbeat = asyncio.Event()

        if store is not None and hasattr(store, "heartbeat") and callable(store.heartbeat):

            async def _heartbeat_loop() -> None:
                while not stop_heartbeat.is_set():
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop_heartbeat.wait(), timeout=self._heartbeat_interval_seconds)
                    if stop_heartbeat.is_set():
                        break
                    with contextlib.suppress(Exception):
                        res = store.heartbeat(
                            [env.task_id],
                            worker_id="worker",
                            lease_seconds=self._heartbeat_interval_seconds * 3,
                        )
                        if inspect.isawaitable(res):
                            await res

            heartbeat_task = asyncio.create_task(_heartbeat_loop())

        try:
            await self._queue_service.process_envelope(env, container=self._container)
            m = self._metrics.get(env.lane)
            if m:
                self._metrics[env.lane] = LaneMetrics(
                    queue_name=env.lane,
                    processed_total=m.processed_total + 1,
                    failed_total=m.failed_total,
                    dlq_total=m.dlq_total,
                )
        except Exception as exc:
            status = "error"
            err_str = str(exc)
            m = self._metrics.get(env.lane)
            if m:
                self._metrics[env.lane] = LaneMetrics(
                    queue_name=env.lane,
                    processed_total=m.processed_total,
                    failed_total=m.failed_total + 1,
                    dlq_total=m.dlq_total + (1 if env.attempt >= env.max_retries else 0),
                )
            raise
        finally:
            stop_heartbeat.set()
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
            if store is not None and hasattr(store, "reap_stale") and callable(store.reap_stale):
                with contextlib.suppress(Exception):
                    res = store.reap_stale()
                    if inspect.isawaitable(res):
                        await res
            end_nano = time.time_ns()
            old_ctx = getattr(token, "old_value", None)
            TraceContext.set_current(old_ctx if isinstance(old_ctx, TraceContext) else None)
            if self._span_exporter is not None:
                backend_scheme = self._queue_service.config.backend_url.split("://")[0]
                attrs: dict[str, Any] = {
                    "messaging.system": backend_scheme,
                    "messaging.destination.name": env.lane,
                    "messaging.message.id": env.task_id,
                    "messaging.operation": "process",
                    "messaging.worker.lane": env.lane,
                    "messaging.message.attempt": env.attempt,
                    "status": status,
                }
                if err_str:
                    attrs["error.message"] = err_str
                span = SpanData(
                    name=f"messaging.process {env.task_name}",
                    trace_id=active_trace.trace_id,
                    span_id=active_trace.span_id,
                    parent_span_id=parent_ctx.span_id if parent_ctx else None,
                    start_ns=start_nano,
                    duration_ns=end_nano - start_nano,
                    status_code=200 if status == "ok" else 500,
                    attributes=attrs,
                )
                self._span_exporter.record_span(span)

        return 1

    async def run(
        self,
        *,
        install_signal_handlers: bool = False,
        poll_interval_s: float = 0.05,
    ) -> None:
        """Run continuous pull loop across lanes until stopped."""
        if install_signal_handlers:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    loop.add_signal_handler(
                        sig,
                        lambda: asyncio.create_task(self._handle_shutdown_signal()),
                    )

        while not self._stopping:
            if self._scheduler is not None:
                with contextlib.suppress(Exception):
                    await self._scheduler.tick()
            processed = await self.process_once()
            if processed == 0:
                await asyncio.sleep(poll_interval_s)

    async def _handle_shutdown_signal(self) -> None:
        """Trigger graceful lame-duck transition and stop."""
        self._lame_duck.enter_lame_duck()
        await self.stop()

    async def stop(self) -> None:
        """Stop accepting new jobs and drain in-flight tasks within drain timeout."""
        self._stopping = True
        self._lame_duck.enter_lame_duck()
        if not self._in_flight:
            return
        max_timeout = max((lane.drain_timeout_s for lane in self._lanes.values()), default=30.0)
        pending = tuple(self._in_flight)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=max_timeout)

    @staticmethod
    def parse_lanes(
        queues: str,
        concurrency: str = "10",
        *,
        drain_timeout_s: float = 30.0,
    ) -> tuple[LaneConfig, ...]:
        """Parse comma-separated queue names and concurrency limits into LaneConfig objects."""
        queue_names = [q.strip() for q in queues.split(",") if q.strip()]
        if not queue_names:
            queue_names = ["default"]
        raw_limits = [int(c.strip()) for c in concurrency.split(",") if c.strip()]
        if not raw_limits:
            raw_limits = [10]

        lanes: list[LaneConfig] = []
        for idx, name in enumerate(queue_names):
            limit = raw_limits[idx] if idx < len(raw_limits) else raw_limits[-1]
            lane_kind: TaskLane = "cpu" if name == "cpu" else ("agent" if name == "agent" else "io")
            lanes.append(
                LaneConfig(
                    queue_name=name,
                    concurrency=max(1, limit),
                    drain_timeout_s=drain_timeout_s,
                    lane_kind=lane_kind,
                )
            )
        return tuple(lanes)

    @staticmethod
    def parse_lane_concurrency(
        queues: str,
        lane_concurrency: str = "",
        *,
        default_concurrency: int = 10,
        drain_timeout_s: float = 30.0,
    ) -> tuple[LaneConfig, ...]:
        """Parse comma-separated queue names and lane=limit assignments into LaneConfig objects."""
        queue_names = [q.strip() for q in queues.split(",") if q.strip()]
        if not queue_names:
            queue_names = ["default"]

        explicit_limits: dict[str, int] = {}
        for entry in lane_concurrency.split(","):
            token = entry.strip()
            if not token or "=" not in token:
                continue
            k, v = token.split("=", 1)
            key = k.strip()
            if not key:
                continue
            with contextlib.suppress(ValueError):
                explicit_limits[key] = max(1, int(v.strip()))

        lanes: list[LaneConfig] = []
        for name in queue_names:
            limit = explicit_limits.get(name, max(1, default_concurrency))
            lane_kind: TaskLane = "cpu" if name == "cpu" else ("agent" if name == "agent" else "io")
            lanes.append(
                LaneConfig(
                    queue_name=name,
                    concurrency=limit,
                    drain_timeout_s=drain_timeout_s,
                    lane_kind=lane_kind,
                )
            )
        return tuple(lanes)

    @staticmethod
    def resolve_job(app: Any, job_name: str) -> Callable[..., Any] | None:
        """Look up a named job callable on a Litestar or Vertebra app."""
        state = getattr(app, "state", None)
        if state is not None:
            jobs_map = state.get("vertebra_jobs") if hasattr(state, "get") else getattr(state, "vertebra_jobs", None)
            if isinstance(jobs_map, dict) and job_name in jobs_map:
                candidate: Callable[..., Any] = jobs_map[job_name]
                return candidate
        registry = getattr(app, "jobs", None)
        if isinstance(registry, dict) and job_name in registry:
            reg_candidate: Callable[..., Any] = registry[job_name]
            return reg_candidate
        return None

    @staticmethod
    def resolve_queue_service(target: Any) -> QueueService:
        """Resolve a configured QueueService directly or from ``target.state['vertebra_queue_service']``."""
        queue_service_cls: type[QueueService] = ensure_module("vertebra.framework.queues.base").QueueService
        if isinstance(target, queue_service_cls):
            return target

        state = getattr(target, "state", None)
        candidate: Any = None
        if isinstance(state, Mapping):
            candidate = state.get(QUEUE_SERVICE_STATE_KEY)
        elif state is not None:
            get_fn = getattr(state, "get", None)
            if callable(get_fn):
                with contextlib.suppress(Exception):
                    candidate = get_fn(QUEUE_SERVICE_STATE_KEY)
            if candidate is None:
                candidate = getattr(state, QUEUE_SERVICE_STATE_KEY, None)

        if candidate is None:
            msg = (
                f"No QueueService configured on {type(target).__name__}; "
                f"expected a QueueService or app.state[{QUEUE_SERVICE_STATE_KEY!r}]"
            )
            raise ImproperConfigurationError(msg)
        if not isinstance(candidate, queue_service_cls):
            msg = (
                f"Invalid {QUEUE_SERVICE_STATE_KEY!r} on {type(target).__name__}: "
                f"expected QueueService, got {type(candidate).__name__}"
            )
            raise ImproperConfigurationError(msg)
        return candidate

    @classmethod
    async def run_oneshot(cls, app: Any, job_name: str, *, payload: Any = None) -> int:
        """Enter the application lifespan, run a single named job, and return an exit code."""
        job_fn = cls.resolve_job(app, job_name)
        if job_fn is None:
            log.error("Oneshot job %r is not registered on application", job_name)
            return 1

        def _invoke() -> Any:
            if payload is not None:
                try:
                    sig = inspect.signature(job_fn)
                    if len(sig.parameters) > 0:
                        return job_fn(payload)
                except (TypeError, ValueError):
                    return job_fn(payload)
            return job_fn()

        lifespan_cm = getattr(app, "lifespan", None)
        try:
            if callable(lifespan_cm):
                async with lifespan_cm():
                    res = _invoke()
                    if inspect.isawaitable(res):
                        await res
            else:
                res = _invoke()
                if inspect.isawaitable(res):
                    await res
        except Exception:
            log.exception("Oneshot job %r failed with unhandled exception", job_name)
            return 1
        return 0


parse_lanes = WorkerRunner.parse_lanes
parse_lane_concurrency = WorkerRunner.parse_lane_concurrency
run_oneshot = WorkerRunner.run_oneshot
