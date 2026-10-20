"""Multi-lane admission-controlled background worker runner and oneshot job executor."""

import asyncio
import contextlib
import dataclasses
import inspect
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

__all__ = (
    "MultiLaneWorkerRunner",
    "WorkerLaneConfig",
    "parse_worker_lanes",
    "resolve_registered_job",
    "run_oneshot_job",
)

log = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True, frozen=True)
class WorkerLaneConfig:
    """Per-queue worker lane concurrency and drain timeout configuration."""

    queue_name: str
    concurrency: int = 10
    drain_timeout_s: float = 30.0


class MultiLaneWorkerRunner:
    """Admission-controlled multi-lane worker runner with per-queue asyncio.Semaphore isolation."""

    __slots__ = ("_in_flight", "_lanes", "_semaphores", "_stopping")

    def __init__(self, lanes: Sequence[WorkerLaneConfig]) -> None:
        self._lanes: dict[str, WorkerLaneConfig] = {lane.queue_name: lane for lane in lanes}
        self._semaphores: dict[str, asyncio.Semaphore] = {
            lane.queue_name: asyncio.Semaphore(lane.concurrency) for lane in lanes
        }
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._stopping = False

    @property
    def lanes(self) -> dict[str, WorkerLaneConfig]:
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

        task = asyncio.create_task(_guarded())
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def stop(self) -> None:
        """Stop accepting new jobs and drain in-flight tasks within the configured drain timeout."""
        self._stopping = True
        if not self._in_flight:
            return
        max_timeout = max((lane.drain_timeout_s for lane in self._lanes.values()), default=30.0)
        pending = tuple(self._in_flight)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=max_timeout)


def parse_worker_lanes(
    queues: str,
    concurrency: str = "10",
    *,
    drain_timeout_s: float = 30.0,
) -> tuple[WorkerLaneConfig, ...]:
    """Parse comma-separated queue names and concurrency limits into WorkerLaneConfig objects."""
    queue_names = [q.strip() for q in queues.split(",") if q.strip()]
    if not queue_names:
        queue_names = ["default"]
    raw_limits = [int(c.strip()) for c in concurrency.split(",") if c.strip()]
    if not raw_limits:
        raw_limits = [10]

    lanes: list[WorkerLaneConfig] = []
    for idx, name in enumerate(queue_names):
        limit = raw_limits[idx] if idx < len(raw_limits) else raw_limits[-1]
        lanes.append(
            WorkerLaneConfig(
                queue_name=name,
                concurrency=max(1, limit),
                drain_timeout_s=drain_timeout_s,
            )
        )
    return tuple(lanes)


def resolve_registered_job(app: Any, job_name: str) -> Callable[..., Any] | None:
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


async def run_oneshot_job(app: Any, job_name: str) -> int:
    """Enter the application lifespan, run a single named job, and return an exit code."""
    job_fn = resolve_registered_job(app, job_name)
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
