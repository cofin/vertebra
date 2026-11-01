"""Background task queue bridge for Flask applications."""

from __future__ import annotations

import time
import uuid
from collections import deque
from typing import TYPE_CHECKING, Any, cast

import msgspec

from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "QueueBackend",
    "TaskEnvelope",
)


class TaskEnvelope(msgspec.Struct, frozen=True, gc=False):
    """Serializable background task envelope."""

    id: str
    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = {}
    enqueued_at: float = 0.0


class QueueBackend:
    """Dual sync/async background task queue for Flask applications."""

    __slots__ = ("_queue", "_tasks")

    def __init__(self) -> None:
        self._queue: deque[bytes] = deque()
        self._tasks: dict[str, Callable[..., Any]] = {}

    def task(self, name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator registering a callable as a named background task."""

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or fn.__name__
            self._tasks[task_name] = fn
            return fn

        return _decorator

    def enqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Synchronously enqueue a background task."""
        envelope = TaskEnvelope(
            id=uuid.uuid4().hex,
            task_name=task_name,
            args=args,
            kwargs=kwargs,
            enqueued_at=time.time(),
        )
        self._queue.append(encode_json_bytes(envelope))
        return envelope

    async def aenqueue(self, task_name: str, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Asynchronously enqueue a background task."""
        return self.enqueue(task_name, *args, **kwargs)

    def dequeue(self) -> TaskEnvelope | None:
        """Synchronously pop the next task envelope from the queue."""
        if not self._queue:
            return None
        raw = self._queue.popleft()
        return cast("TaskEnvelope", decode_json(raw, type=TaskEnvelope))

    async def adequeue(self) -> TaskEnvelope | None:
        """Asynchronously pop the next task envelope from the queue."""
        return self.dequeue()

    def run_next(self) -> Any:
        """Pop and synchronously execute the next queued task."""
        env = self.dequeue()
        if env is None:
            return None
        fn = self._tasks[env.task_name]
        return fn(*env.args, **env.kwargs)
