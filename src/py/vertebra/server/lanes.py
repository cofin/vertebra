"""Multi-lane worker runner, admission controller, and OpenTelemetry instrumentation."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import itertools
import logging
import signal
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

import msgspec

from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanData, SpanExporter, TraceContext
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from contextlib import AbstractAsyncContextManager

    from vertebra.framework.di import Container
    from vertebra.framework.queues.base import QueueService, TaskEnvelope, TaskLane
    from vertebra.framework.queues.scheduler import CronScheduler

    class _LifespanFactory(Protocol):
        """Callable protocol returning an async lifespan context manager."""

        def __call__(self) -> AbstractAsyncContextManager[Any]:
            """Create a lifespan async context manager."""
            ...

    class _ReapStaleFn(Protocol):
        """Callable protocol for sync or async stale-task reaping."""

        def __call__(self) -> int | Awaitable[int]:
            """Reap stale queued tasks and return the reaped count."""
            ...


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
_HELD_LANE_KEY: contextvars.ContextVar[str | None] = contextvars.ContextVar("vertebra_held_lane_key", default=None)

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
        "_envelope_tasks",
        "_heartbeat_interval_seconds",
        "_in_flight",
        "_lame_duck",
        "_lane_counters",
        "_lanes",
        "_last_reap_at",
        "_metrics",
        "_queue_service",
        "_reap_interval_seconds",
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
        reap_interval_seconds: float = 60.0,
    ) -> None:
        self._lanes: dict[str, LaneConfig] = {lane.queue_name: lane for lane in lanes}
        self._lane_counters: dict[str, itertools.count[int]] = {lane.queue_name: itertools.count(1) for lane in lanes}
        self._semaphores: dict[str, asyncio.Semaphore] = {
            lane.queue_name: asyncio.Semaphore(max(1, lane.concurrency)) for lane in lanes
        }
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._envelope_tasks: set[asyncio.Task[Any]] = set()
        self._stopping = False
        self._queue_service = queue_service
        self._lame_duck = lame_duck or LameDuckController()
        self._span_exporter = span_exporter
        self._scheduler = scheduler
        self._container = container
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._reap_interval_seconds = max(0.0, reap_interval_seconds)
        self._last_reap_at: float = 0.0
        self._metrics: dict[str, LaneMetrics] = {
            lane.queue_name: LaneMetrics(queue_name=lane.queue_name) for lane in lanes
        }

    @property
    def lanes(self) -> dict[str, LaneConfig]:
        """Return configured worker lanes keyed by queue name."""
        return dict(self._lanes)

    @property
    def metrics(self) -> dict[str, LaneMetrics]:
        """Return current observability metrics keyed by configured queue name."""
        return dict(self._metrics)

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
    def reap_interval_seconds(self) -> float:
        """Return the interval in seconds between stale-lease reap maintenance sweeps."""
        return self._reap_interval_seconds

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
        lane_key = self._resolve_lane_key(queue_name)
        semaphore = self._ensure_semaphore(lane_key)

        async def _guarded() -> Any:
            async with semaphore:
                tok = _HELD_LANE_KEY.set(lane_key)
                self._adjust_in_flight_metric(lane_key, 1)
                try:
                    res = job_factory()
                    if inspect.isawaitable(res):
                        return await res
                    return res
                finally:
                    self._adjust_in_flight_metric(lane_key, -1)
                    _HELD_LANE_KEY.reset(tok)

        task = asyncio.get_running_loop().create_task(
            _guarded(),
            name=self._next_lane_task_name(lane_key),
        )
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def process_once(self) -> int:
        """Pull and concurrently process a task batch up to configured lane concurrency."""
        if self._queue_service is None:
            return 0

        await self._maybe_reap_stale()
        target_lanes = self._target_lanes()
        batch_limit = self._total_concurrency()
        envelopes = await self._queue_service.backend.dequeue(target_lanes, batch_size=batch_limit)
        if not envelopes:
            return 0

        held_lane = _HELD_LANE_KEY.get()
        tasks = [
            self._spawn_envelope_task(
                env,
                skip_semaphore=(len(envelopes) == 1 and held_lane == self._resolve_lane_key(env.lane)),
            )
            for env in envelopes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for item in results:
            if isinstance(item, BaseException):
                raise item
        return len(envelopes)

    async def run(
        self,
        *,
        install_signal_handlers: bool = False,
        poll_interval_s: float = 0.05,
    ) -> None:
        """Run continuous concurrent pull loop across lanes until stopped."""
        current_task = asyncio.current_task()
        if current_task is not None:
            current_task.set_name("vertebra.worker.runner")
        if install_signal_handlers:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    loop.add_signal_handler(
                        sig,
                        lambda: loop.create_task(
                            self._handle_shutdown_signal(),
                            name="vertebra.worker.shutdown",
                        ),
                    )

        total_cap = self._total_concurrency()
        while not self._stopping:
            if self._scheduler is not None:
                with contextlib.suppress(Exception):
                    await self._scheduler.tick()
            await self._maybe_reap_stale()
            if self._queue_service is None:
                await asyncio.sleep(poll_interval_s)
                continue

            available = max(0, total_cap - len(self._envelope_tasks))
            if available == 0:
                if self._envelope_tasks:
                    await asyncio.wait(
                        tuple(self._envelope_tasks),
                        timeout=poll_interval_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                else:
                    await asyncio.sleep(poll_interval_s)
                continue

            envelopes = await self._queue_service.backend.dequeue(
                self._target_lanes(),
                batch_size=available,
            )
            if not envelopes:
                if self._envelope_tasks:
                    await asyncio.wait(
                        tuple(self._envelope_tasks),
                        timeout=poll_interval_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                else:
                    await asyncio.sleep(poll_interval_s)
                continue

            for env in envelopes:
                self._spawn_envelope_task(env)

    async def stop(self) -> None:
        """Stop accepting new jobs and drain in-flight tasks within drain timeout."""
        self._stopping = True
        self._lame_duck.enter_lame_duck()
        if not self._in_flight:
            return
        max_timeout = max((lane.drain_timeout_s for lane in self._lanes.values()), default=30.0)
        deadline = time.monotonic() + max_timeout
        while self._in_flight:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            pending = tuple(self._in_flight)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=remaining)
            if any(not t.done() for t in pending):
                break

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
                async with cast("_LifespanFactory", lifespan_cm)():
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

    def _resolve_lane_key(self, lane_or_queue: str) -> str:
        """Resolve a configured queue_name key from either queue_name or TaskLane kind."""
        if lane_or_queue in self._lanes:
            return lane_or_queue
        for key, cfg in self._lanes.items():
            if cfg.lane_kind == lane_or_queue:
                return key
        return lane_or_queue

    def _ensure_semaphore(self, lane_key: str) -> asyncio.Semaphore:
        """Return or initialize the admission semaphore for lane_key."""
        semaphore = self._semaphores.get(lane_key)
        if semaphore is None:
            cfg = self._lanes.get(lane_key)
            limit = max(1, cfg.concurrency) if cfg is not None else 10
            semaphore = asyncio.Semaphore(limit)
            self._semaphores[lane_key] = semaphore
        if lane_key not in self._metrics:
            self._metrics[lane_key] = LaneMetrics(queue_name=lane_key)
        return semaphore

    def _target_lanes(self) -> list[TaskLane]:
        """Return unique TaskLane kinds configured on this runner."""
        target_lanes: list[TaskLane] = []
        for lane in self._lanes.values():
            if lane.lane_kind not in target_lanes:
                target_lanes.append(lane.lane_kind)
        if not target_lanes:
            target_lanes = ["cpu", "io", "agent"]
        return target_lanes

    def _total_concurrency(self) -> int:
        """Return total concurrent task capacity across configured lanes."""
        if not self._lanes:
            return 10
        return max(1, sum(max(1, lane.concurrency) for lane in self._lanes.values()))

    def _adjust_in_flight_metric(self, lane_key: str, delta: int) -> None:
        """Increment or decrement the in_flight counter for lane_key."""
        cur = self._metrics.get(lane_key) or LaneMetrics(queue_name=lane_key)
        self._metrics[lane_key] = LaneMetrics(
            queue_name=cur.queue_name,
            in_flight=max(0, cur.in_flight + delta),
            processed_total=cur.processed_total,
            failed_total=cur.failed_total,
            dlq_total=cur.dlq_total,
        )

    def _next_lane_task_name(self, lane_key: str) -> str:
        """Return the next structured task name for ``lane_key``."""
        counter = self._lane_counters.get(lane_key)
        if counter is None:
            counter = itertools.count(1)
            self._lane_counters[lane_key] = counter
        return f"vertebra.worker.lane.{lane_key}.{next(counter)}"

    def _spawn_envelope_task(self, env: TaskEnvelope, *, skip_semaphore: bool = False) -> asyncio.Task[int]:
        """Spawn a tracked asyncio.Task executing env under its resolved lane semaphore."""
        lane_key = self._resolve_lane_key(env.lane)
        semaphore = self._ensure_semaphore(lane_key)

        async def _runner() -> int:
            if skip_semaphore:
                return await self._process_envelope_guarded(env, lane_key=lane_key, track_metric=False)
            async with semaphore:
                return await self._process_envelope_guarded(env, lane_key=lane_key, track_metric=True)

        task = asyncio.get_running_loop().create_task(
            _runner(),
            name=self._next_lane_task_name(lane_key),
        )
        self._in_flight.add(task)
        self._envelope_tasks.add(task)
        task.add_done_callback(self._in_flight.discard)
        task.add_done_callback(self._envelope_tasks.discard)
        return task

    async def _maybe_reap_stale(self, *, force: bool = False) -> int:
        """Run store.reap_stale() at most once per reap_interval_seconds maintenance window."""
        if self._queue_service is None:
            return 0
        now = time.monotonic()
        if not force and self._last_reap_at > 0.0 and (now - self._last_reap_at) < self._reap_interval_seconds:
            return 0
        backend = self._queue_service.backend
        store = getattr(backend, "store", None) or getattr(backend, "_store", None)
        if store is None or not hasattr(store, "reap_stale") or not callable(store.reap_stale):
            self._last_reap_at = now
            return 0
        self._last_reap_at = now
        with contextlib.suppress(Exception):
            res = cast("_ReapStaleFn", store.reap_stale)()
            if inspect.isawaitable(res):
                return int(await res)
            return int(res)
        return 0

    async def _process_envelope_guarded(
        self,
        env: TaskEnvelope,
        *,
        lane_key: str | None = None,
        track_metric: bool = True,
    ) -> int:
        """Execute single task envelope with lease heartbeats, scoped DI, and OpenTelemetry spans."""
        resolved_key = lane_key or self._resolve_lane_key(env.lane)
        if track_metric:
            self._adjust_in_flight_metric(resolved_key, 1)

        start_nano = time.time_ns()
        parent_ctx = TraceContext.parse(env.traceparent) if env.traceparent else None
        active_trace = parent_ctx.new_child() if parent_ctx else TraceContext.generate()
        token = TraceContext.set_current(active_trace)

        status = "ok"
        err_str: str | None = None
        assert self._queue_service is not None
        backend = self._queue_service.backend
        store = getattr(backend, "store", None) or getattr(backend, "_store", None)
        worker_id = str(getattr(self._queue_service.config, "consumer_name", "worker"))
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
                            worker_id=worker_id,
                            lease_seconds=self._heartbeat_interval_seconds * 3,
                        )
                        if inspect.isawaitable(res):
                            await res

            heartbeat_task = asyncio.get_running_loop().create_task(
                _heartbeat_loop(),
                name=f"vertebra.worker.heartbeat.{resolved_key}",
            )

        try:
            await self._queue_service.process_envelope(env, container=self._container)
            cur = self._metrics.get(resolved_key) or LaneMetrics(queue_name=resolved_key)
            self._metrics[resolved_key] = LaneMetrics(
                queue_name=cur.queue_name,
                in_flight=cur.in_flight,
                processed_total=cur.processed_total + 1,
                failed_total=cur.failed_total,
                dlq_total=cur.dlq_total,
            )
        except Exception as exc:
            status = "error"
            err_str = str(exc)
            cur = self._metrics.get(resolved_key) or LaneMetrics(queue_name=resolved_key)
            self._metrics[resolved_key] = LaneMetrics(
                queue_name=cur.queue_name,
                in_flight=cur.in_flight,
                processed_total=cur.processed_total,
                failed_total=cur.failed_total + 1,
                dlq_total=cur.dlq_total + (1 if env.attempt >= env.max_retries else 0),
            )
            raise
        finally:
            stop_heartbeat.set()
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
            if track_metric:
                self._adjust_in_flight_metric(resolved_key, -1)
            end_nano = time.time_ns()
            old_ctx = getattr(token, "old_value", None)
            TraceContext.set_current(old_ctx if isinstance(old_ctx, TraceContext) else None)
            if self._span_exporter is not None:
                backend_scheme = self._queue_service.config.backend_url.split("://")[0]
                attrs: dict[str, Any] = {
                    "messaging.system": backend_scheme,
                    "messaging.destination.name": resolved_key,
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

    async def _handle_shutdown_signal(self) -> None:
        """Trigger graceful lame-duck transition and stop."""
        self._lame_duck.enter_lame_duck()
        await self.stop()


parse_lanes = WorkerRunner.parse_lanes
parse_lane_concurrency = WorkerRunner.parse_lane_concurrency
run_oneshot = WorkerRunner.run_oneshot
