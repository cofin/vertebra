"""Background task queues, cron schedules, and multi-lane (`cpu`, `io`, `agent`) dispatch."""

from __future__ import annotations

import inspect
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "CronSchedule",
    "QueueConfig",
    "QueueService",
    "TaskDef",
    "TaskEnvelope",
    "TaskLane",
    "TaskRetryPolicy",
    "task",
)

type TaskLane = Literal["cpu", "io", "agent"]


@dataclass(frozen=True, slots=True)
class TaskRetryPolicy:
    """Retry and exponential backoff policy for background tasks."""

    max_retries: int = 3
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0


@dataclass(frozen=True, slots=True)
class CronSchedule:
    """Recurring cron schedule descriptor for background jobs."""

    expression: str
    timezone: str = "UTC"


@dataclass(frozen=True, slots=True)
class TaskDef:
    """Registered background task definition bound to a worker execution lane."""

    name: str
    fn: Callable[..., Any]
    lane: TaskLane = "io"
    retry_policy: TaskRetryPolicy = field(default_factory=TaskRetryPolicy)
    cron: CronSchedule | None = None


@dataclass(frozen=True, slots=True)
class TaskEnvelope:
    """Enqueued task execution envelope."""

    task_id: str
    task_name: str
    lane: TaskLane
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    attempt: int = 1


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """Configuration for ``QueueService`` supporting memory, Valkey, Redis, Postgres, and Oracle backends."""

    backend_url: str = "memory://"
    default_lane: TaskLane = "io"
    max_concurrency: int = 32


class QueueService:
    """Multi-lane background task queue service."""

    def __init__(self, config: QueueConfig | None = None) -> None:
        self._config = config or QueueConfig()
        self._tasks: dict[str, TaskDef] = {}
        self._pending: list[TaskEnvelope] = []

    @property
    def config(self) -> QueueConfig:
        """Return the active ``QueueConfig``."""
        return self._config

    @property
    def registered_tasks(self) -> dict[str, TaskDef]:
        """Return registered task definitions keyed by name."""
        return dict(self._tasks)

    def register(self, task_def: TaskDef) -> TaskDef:
        """Register a ``TaskDef`` with the queue service."""
        self._tasks[task_def.name] = task_def
        return task_def

    async def enqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Enqueue a task by name and return its ``TaskEnvelope``."""
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

    async def drain_once(self) -> list[Any]:
        """Execute all currently pending tasks in FIFO order and return their results."""
        results: list[Any] = []
        while self._pending:
            env = self._pending.pop(0)
            tdef = self._tasks[env.task_name]
            out = tdef.fn(*env.args, **env.kwargs)
            if inspect.isawaitable(out):
                out = await out
            results.append(out)
        return results


def task(
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry_policy: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
) -> Callable[[Callable[..., Any]], TaskDef]:
    """Decorator registering a callable as a ``TaskDef``."""

    def _decorator(fn: Callable[..., Any]) -> TaskDef:
        return TaskDef(
            name=name or fn.__name__,
            fn=fn,
            lane=lane,
            retry_policy=retry_policy or TaskRetryPolicy(),
            cron=cron,
        )

    return _decorator
