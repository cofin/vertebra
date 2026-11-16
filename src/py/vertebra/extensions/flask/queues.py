"""Background task queue bridge for Flask applications backed by QueueService and Container DI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.framework.di import inspect_callable_bindings
from vertebra.framework.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    TaskDef,
)
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY

if TYPE_CHECKING:
    from collections.abc import Callable

    from vertebra.framework.di import Container

__all__ = (
    "QueueBackend",
    "TaskEnvelope",
)


class TaskEnvelope(msgspec.Struct, frozen=True):
    """Serializable background task envelope."""

    id: str
    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)
    enqueued_at: float = 0.0


class QueueBackend:
    """Dual sync/async background task queue for Flask applications backed by QueueService."""

    __slots__ = ("_container", "_queue_service", "_tasks")

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        queue_service: QueueService | None = None,
        container: Container | None = None,
    ) -> None:
        self._container = container
        self._queue_service = queue_service or QueueService(config=config, container=container)
        self._tasks: dict[str, Callable[..., Any]] = {}

    @property
    def queue_service(self) -> QueueService:
        """Return the underlying active QueueService."""
        return self._queue_service

    @property
    def container(self) -> Container | None:
        """Return the bound Container if configured."""
        return self._container

    def init_app(self, app: Any) -> None:
        """Attach queue service to Flask app extensions."""
        if hasattr(app, "extensions"):
            app.extensions["vertebra_queue_backend"] = self
            app.extensions[QUEUE_SERVICE_STATE_KEY] = self._queue_service

    def task(self, name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator registering a callable as a named background task."""

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or fn.__name__
            self._tasks[task_name] = fn
            self._queue_service.register(TaskDef(name=task_name, fn=fn, lane="io"))
            return fn

        return _decorator

    def enqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Synchronously enqueue a background task."""
        env = self._queue_service.enqueue_sync(task_name, *args, **kwargs)
        return TaskEnvelope(
            id=env.task_id,
            task_name=env.task_name,
            args=tuple(env.args),
            kwargs=dict(env.kwargs),
            enqueued_at=env.enqueued_at,
        )

    async def aenqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Asynchronously enqueue a background task."""
        env = await self._queue_service.enqueue(task_name, *args, **kwargs)
        return TaskEnvelope(
            id=env.task_id,
            task_name=env.task_name,
            args=tuple(env.args),
            kwargs=dict(env.kwargs),
            enqueued_at=env.enqueued_at,
        )

    def dequeue(self) -> TaskEnvelope | None:
        """Synchronously pop the next task envelope from the queue."""
        backend = self._queue_service.backend
        if hasattr(backend, "_lanes"):
            lanes = backend._lanes
            for lane_deque in lanes.values():
                if lane_deque:
                    env = lane_deque.popleft()
                    return TaskEnvelope(
                        id=env.task_id,
                        task_name=env.task_name,
                        args=tuple(env.args),
                        kwargs=dict(env.kwargs),
                        enqueued_at=env.enqueued_at,
                    )
        return None

    async def adequeue(self) -> TaskEnvelope | None:
        """Asynchronously pop the next task envelope from the queue."""
        return self.dequeue()

    def run_next(self) -> Any:
        """Pop and synchronously execute the next queued task."""
        backend = self._queue_service.backend
        if hasattr(backend, "_lanes"):
            lanes = backend._lanes
            for lane_deque in lanes.values():
                if lane_deque:
                    env = lane_deque.popleft()
                    fn = self._tasks.get(env.task_name)
                    if fn is None:
                        tdef = self._queue_service.registered_tasks.get(env.task_name)
                        fn = tdef.fn if tdef else None
                    if fn is None:
                        return None
                    if self._container is not None:
                        with self._container.scope() as scope:
                            bindings = inspect_callable_bindings(fn)
                            resolved_kwargs = dict(env.kwargs)
                            num_pos = len(env.args)
                            for idx, binding in enumerate(bindings):
                                if idx < num_pos or binding.name in resolved_kwargs:
                                    continue
                                dep_key = self._container.graph.resolve_param_key(binding)
                                if dep_key is not None:
                                    resolved_kwargs[binding.name] = scope.resolve_sync(dep_key)
                            return fn(*env.args, **resolved_kwargs)
                    return fn(*env.args, **env.kwargs)
        return None
