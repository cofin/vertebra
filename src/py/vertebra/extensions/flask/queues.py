"""Background task queue bridge for Flask applications backed by QueueService and Container DI."""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.framework.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    TaskDef,
)
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

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
        """Synchronously pop and acknowledge the next task envelope from QueueService."""
        return self._run_coro_sync(self.adequeue())

    async def adequeue(self) -> TaskEnvelope | None:
        """Asynchronously pop and acknowledge the next task envelope from QueueService."""
        await self._queue_service.backend.promote_scheduled()
        envelopes = await self._queue_service.backend.dequeue(("cpu", "io", "agent"), batch_size=1)
        if not envelopes:
            return None
        env = envelopes[0]
        await self._queue_service.backend.ack(env)
        return TaskEnvelope(
            id=env.task_id,
            task_name=env.task_name,
            args=tuple(env.args),
            kwargs=dict(env.kwargs),
            enqueued_at=env.enqueued_at,
        )

    def run_next(self) -> Any:
        """Pop and synchronously execute the next queued task via QueueService."""
        return self._run_coro_sync(self.arun_next())

    async def arun_next(self) -> Any:
        """Pop and asynchronously execute the next queued task via QueueService."""
        await self._queue_service.backend.promote_scheduled()
        envelopes = await self._queue_service.backend.dequeue(("cpu", "io", "agent"), batch_size=1)
        if not envelopes:
            return None
        env = envelopes[0]
        if env.task_name not in self._queue_service.registered_tasks:
            fn = self._tasks.get(env.task_name)
            if fn is None:
                await self._queue_service.backend.nack(
                    env,
                    error=f"Task {env.task_name!r} is not registered",
                    send_to_dlq=True,
                )
                return None
            self._queue_service.register(TaskDef(name=env.task_name, fn=fn, lane=env.lane))
        return await self._queue_service.process_envelope(env, container=self._container)

    @staticmethod
    def _run_coro_sync[T](coro: Coroutine[Any, Any, T]) -> T:
        """Execute an async coroutine synchronously whether or not an event loop is running."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
