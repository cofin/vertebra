"""Background task queues, cron schedules, multi-lane dispatch, and worker runners."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import inspect
import logging
import uuid
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra._typing import UnsupportedBackendError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

__all__ = (
    "CronSchedule",
    "LaneConfig",
    "QueueConfig",
    "QueueService",
    "TaskDef",
    "TaskEnvelope",
    "TaskLane",
    "TaskRetryPolicy",
    "WorkerRunner",
    "parse_lanes",
    "run_oneshot",
    "task",
)

log = logging.getLogger(__name__)

type TaskLane = Literal["cpu", "io", "agent"]


def _cron_matches_field(field_spec: str, value: int) -> bool:
    """Check whether a single cron field expression matches an integer value."""
    if field_spec == "*":
        return True
    if field_spec.startswith("*/"):
        step = int(field_spec[2:])
        return value % step == 0
    if "," in field_spec:
        return any(_cron_matches_field(part, value) for part in field_spec.split(","))
    if "-" in field_spec:
        start_str, end_str = field_spec.split("-", 1)
        return int(start_str) <= value <= int(end_str)
    return int(field_spec) == value


def _is_cron_due(expr: str, minute: int, hour: int, day_of_month: int, month: int, day_of_week: int) -> bool:
    """Check whether a standard 5-field cron expression matches the specified date and time components."""
    parts = expr.split()
    if len(parts) != 5:
        return False
    m_spec, h_spec, dom_spec, mon_spec, dow_spec = parts
    return (
        _cron_matches_field(m_spec, minute)
        and _cron_matches_field(h_spec, hour)
        and _cron_matches_field(dom_spec, day_of_month)
        and _cron_matches_field(mon_spec, month)
        and _cron_matches_field(dow_spec, day_of_week)
    )


class TaskRetryPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Retry and exponential backoff policy for background tasks."""

    max_retries: int = 3
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0


class CronSchedule(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Recurring cron schedule descriptor for background jobs."""

    expression: str
    timezone: str = "UTC"


class TaskDef(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Registered background task definition bound to a worker execution lane."""

    name: str
    fn: Callable[..., Any]
    lane: TaskLane = "io"
    retry_policy: TaskRetryPolicy = msgspec.field(default_factory=TaskRetryPolicy)
    cron: CronSchedule | None = None


class TaskEnvelope(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Enqueued task execution envelope."""

    task_id: str
    task_name: str
    lane: TaskLane
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    attempt: int = 1


class QueueConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for QueueService supporting memory backend and concurrency limits."""

    backend_url: str = "memory://"
    default_lane: TaskLane = "io"
    max_concurrency: int = 32


class QueueService:
    """Multi-lane background task queue service with retries, cron scheduling, and semaphore concurrency."""

    __slots__ = ("_config", "_pending", "_semaphores", "_tasks")

    def __init__(self, config: QueueConfig | None = None) -> None:
        """Initialize the queue service, validating that backend_url is memory-backed."""
        self._config = config or QueueConfig()
        if not self._config.backend_url.startswith("memory://"):
            msg = f"QueueService backend {self._config.backend_url!r} is not supported; use 'memory://'"
            raise UnsupportedBackendError(msg)
        self._tasks: dict[str, TaskDef] = {}
        self._pending: collections.deque[TaskEnvelope] = collections.deque()
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    @property
    def config(self) -> QueueConfig:
        """Return the active QueueConfig."""
        return self._config

    @property
    def registered_tasks(self) -> dict[str, TaskDef]:
        """Return registered task definitions keyed by name."""
        return dict(self._tasks)

    def register(self, task_def: TaskDef) -> TaskDef:
        """Register a TaskDef with the queue service."""
        self._tasks[task_def.name] = task_def
        return task_def

    async def enqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Enqueue a task by name and return its TaskEnvelope."""
        if task_name not in self._tasks:
            msg = f"Task {task_name!r} is not registered"
            raise KeyError(msg)
        tdef = self._tasks[task_name]
        envelope = TaskEnvelope(
            task_id=uuid.uuid4().hex,
            task_name=task_name,
            lane=tdef.lane,
            args=args,
            kwargs=kwargs,
        )
        self._pending.append(envelope)
        return envelope

    async def run_next(self) -> Any:
        """Execute the next pending task in FIFO order, applying retry policies on failure."""
        if not self._pending:
            return None
        env = self._pending.popleft()
        tdef = self._tasks.get(env.task_name)
        if tdef is None:
            msg = f"Task {env.task_name!r} is not registered"
            raise KeyError(msg)

        sem = self._semaphores.setdefault(tdef.lane, asyncio.Semaphore(self._config.max_concurrency))
        async with sem:
            while True:
                try:
                    res = tdef.fn(*env.args, **env.kwargs)
                    if inspect.isawaitable(res):
                        res = await res
                    return res
                except Exception as exc:
                    if env.attempt < tdef.retry_policy.max_retries:
                        env = TaskEnvelope(
                            task_id=env.task_id,
                            task_name=env.task_name,
                            lane=env.lane,
                            args=env.args,
                            kwargs=env.kwargs,
                            attempt=env.attempt + 1,
                        )
                        if tdef.retry_policy.initial_backoff_seconds > 0:
                            backoff = min(
                                tdef.retry_policy.max_backoff_seconds,
                                tdef.retry_policy.initial_backoff_seconds
                                * (tdef.retry_policy.backoff_multiplier ** (env.attempt - 2)),
                            )
                            await asyncio.sleep(backoff)
                    else:
                        raise exc

    async def drain_once(self) -> list[Any]:
        """Execute all currently pending tasks in FIFO order and return their results."""
        results: list[Any] = []
        while self._pending:
            res = await self.run_next()
            results.append(res)
        return results

    async def run_due_schedules(
        self,
        *,
        minute: int,
        hour: int,
        day_of_month: int = 1,
        month: int = 1,
        day_of_week: int = 0,
    ) -> list[Any]:
        """Execute registered tasks whose CronSchedule matches the specified time fields."""
        results: list[Any] = []
        for tdef in self._tasks.values():
            if tdef.cron is not None and _is_cron_due(
                tdef.cron.expression,
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month=month,
                day_of_week=day_of_week,
            ):
                res = tdef.fn()
                if inspect.isawaitable(res):
                    res = await res
                results.append(res)
        return results


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry: TaskRetryPolicy | None = None,
    retry_policy: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
) -> Any:
    """Decorator registering a callable as a background TaskDef."""
    effective_retry = retry or retry_policy or TaskRetryPolicy()

    def _decorator(func: Callable[..., Any]) -> TaskDef:
        return TaskDef(
            name=name or func.__name__,
            fn=func,
            lane=lane,
            retry_policy=effective_retry,
            cron=cron,
        )

    if fn is not None:
        return _decorator(fn)
    return _decorator


class LaneConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Per-queue worker lane concurrency and drain timeout configuration."""

    queue_name: str
    concurrency: int = 10
    drain_timeout_s: float = 30.0


class WorkerRunner:
    """Admission-controlled multi-lane worker runner with per-queue asyncio.Semaphore isolation."""

    __slots__ = ("_in_flight", "_lanes", "_semaphores", "_stopping")

    def __init__(self, lanes: Sequence[LaneConfig]) -> None:
        """Initialize runner with lane configurations and concurrency semaphores."""
        self._lanes: dict[str, LaneConfig] = {lane.queue_name: lane for lane in lanes}
        self._semaphores: dict[str, asyncio.Semaphore] = {
            lane.queue_name: asyncio.Semaphore(lane.concurrency) for lane in lanes
        }
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._stopping = False

    @property
    def lanes(self) -> dict[str, LaneConfig]:
        """Return configured worker lanes keyed by queue name."""
        return dict(self._lanes)

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

        job_task = asyncio.create_task(_guarded())
        self._in_flight.add(job_task)
        job_task.add_done_callback(self._in_flight.discard)
        return job_task

    async def stop(self) -> None:
        """Stop accepting new jobs and drain in-flight tasks within the configured drain timeout."""
        self._stopping = True
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
            lanes.append(
                LaneConfig(
                    queue_name=name,
                    concurrency=max(1, limit),
                    drain_timeout_s=drain_timeout_s,
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

    @classmethod
    async def run_oneshot(cls, app: Any, job_name: str) -> int:
        """Enter the application lifespan, run a single named job, and return an exit code."""
        job_fn = cls.resolve_job(app, job_name)
        if job_fn is None:
            log.error("Oneshot job %r is not registered on application", job_name)
            return 1

        lifespan_cm = getattr(app, "lifespan", None)
        try:
            if callable(lifespan_cm):
                async with lifespan_cm():
                    res = job_fn()
                    if inspect.isawaitable(res):
                        await res
            else:
                res = job_fn()
                if inspect.isawaitable(res):
                    await res
        except Exception:
            log.exception("Oneshot job %r failed with unhandled exception", job_name)
            return 1
        return 0


parse_lanes = WorkerRunner.parse_lanes
run_oneshot = WorkerRunner.run_oneshot
