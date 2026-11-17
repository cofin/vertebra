"""Django 6.0+ django.tasks compatible TaskBackend."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import traceback
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import msgspec

from vertebra.framework.queues import QueueConfig, QueueService, TaskDef
from vertebra.utils.module_loader import ensure_module, import_optional

_tasks_backend_mod = import_optional("django.tasks.backends.base")

if TYPE_CHECKING:
    from collections.abc import Callable

    from django.tasks import TaskResult, TaskResultStatus
    from django.tasks.backends.base import BaseTaskBackend as _BaseTaskBackend
    from django.tasks.base import TaskError

    from vertebra.framework.di import Container
    from vertebra.framework.queues.base import TaskLane
else:
    _BaseTaskBackend = _tasks_backend_mod.BaseTaskBackend if _tasks_backend_mod is not None else object

__all__ = ("TaskBackend",)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django task base classes on demand."""
    if name == "BaseTaskBackend":
        return ensure_module("django.tasks.backends.base", package="django", extra="django").BaseTaskBackend
    if name in {"TaskResult", "TaskResultStatus"}:
        return getattr(ensure_module("django.tasks", package="django", extra="django"), name)
    if name in {"DjangoTask", "TaskError"}:
        attr = "Task" if name == "DjangoTask" else name
        return getattr(ensure_module("django.tasks.base", package="django", extra="django"), attr)
    if name == "TaskResultDoesNotExist":
        return ensure_module("django.tasks.exceptions", package="django", extra="django").TaskResultDoesNotExist
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def _ensure_django_tasks() -> tuple[Any, Any, Any]:
    """Import and return ``(django.tasks, django.tasks.base, django.tasks.exceptions)``."""
    return (
        ensure_module("django.tasks", package="django", extra="django"),
        ensure_module("django.tasks.base", package="django", extra="django"),
        ensure_module("django.tasks.exceptions", package="django", extra="django"),
    )


_SHARED_RESULTS: dict[str, dict[str, _TaskRecord]] = {}
_RESULTS_LOCK = threading.Lock()


class _TaskRecord(msgspec.Struct, kw_only=True):
    task: Any
    id: str
    status: TaskResultStatus
    enqueued_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_attempted_at: datetime | None = None
    args: list[Any] = msgspec.field(default_factory=list)
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)
    queue_name: str = "default"
    backend: str = "default"
    errors: list[TaskError] = msgspec.field(default_factory=list)
    worker_ids: list[str] = msgspec.field(default_factory=list)
    return_value: Any = None


class TaskBackend(_BaseTaskBackend):
    """Vertebra background task backend for Django 6.0+."""

    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str = "default", params: dict[str, Any] | None = None) -> None:
        """Initialize the task backend."""
        if _tasks_backend_mod is None:
            ensure_module("django.tasks.backends.base", package="django", extra="django")
        params_dict = dict(params or {})
        super().__init__(alias=alias, params=params_dict)
        self._container: Container | None = self.options.get("container")
        if "queue_service" in self.options:
            self._queue_service: QueueService = self.options["queue_service"]
        else:
            backend_url = self.options.get("BACKEND_URL", "memory://")
            self._queue_service = QueueService(QueueConfig(backend_url=backend_url), container=self._container)
        self._registered_callables: dict[str, Callable[..., Any]] = {}
        with _RESULTS_LOCK:
            if alias not in _SHARED_RESULTS:
                _SHARED_RESULTS[alias] = {}
            self._results: dict[str, _TaskRecord] = _SHARED_RESULTS[alias]

    @property
    def queue_service(self) -> QueueService:
        """Return the underlying QueueService."""
        return self._queue_service

    @property
    def container(self) -> Container | None:
        """Return the bound DI Container if configured."""
        return self._container

    def task(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a callable for testing or manual dispatch."""

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._registered_callables[name] = fn
            self._queue_service.register(TaskDef(name=name, fn=fn, lane="io"))
            return fn

        return decorator

    def enqueue(
        self,
        task: Any,
        *args: Any,
        **kwargs: Any,
    ) -> TaskResult[Any, Any]:
        """Queue up a task to be executed synchronously."""
        tasks_mod, tasks_base_mod, _ = _ensure_django_tasks()
        if hasattr(task, "validate_task") or hasattr(task, "func"):
            self.validate_task(task)

        if (
            isinstance(task, tasks_base_mod.Task)
            and len(args) == 2
            and isinstance(args[0], (tuple, list))
            and isinstance(args[1], dict)
            and not kwargs
        ):
            resolved_args = list(args[0])
            resolved_kwargs = dict(args[1])
        else:
            resolved_args = list(args)
            resolved_kwargs = dict(kwargs)

        now = datetime.now(UTC)

        task_name, func, queue_name, _priority, run_after = self._resolve_task_metadata(task)
        sched_time = run_after.timestamp() if run_after else None
        lane: TaskLane = "io"
        if queue_name in ("cpu", "io", "agent"):
            lane = cast("TaskLane", queue_name)

        if func is not None and task_name not in self._queue_service.registered_tasks:
            self._queue_service.register(TaskDef(name=task_name, fn=func, lane=lane))

        envelope = self._queue_service.enqueue_sync(
            task_name,
            *resolved_args,
            lane=lane,
            scheduled_at=sched_time,
            **resolved_kwargs,
        )
        task_id = envelope.task_id

        record = _TaskRecord(
            task=task,
            id=task_id,
            status=tasks_mod.TaskResultStatus.READY,
            enqueued_at=now,
            args=resolved_args,
            kwargs=resolved_kwargs,
            queue_name=queue_name,
            backend=self.alias,
        )
        with _RESULTS_LOCK:
            self._results[task_id] = record

        return self._build_result(record)

    async def aenqueue(
        self,
        task: Any,
        *args: Any,
        **kwargs: Any,
    ) -> TaskResult[Any, Any]:
        """Queue up a task to be executed asynchronously."""
        tasks_mod, tasks_base_mod, _ = _ensure_django_tasks()
        if hasattr(task, "validate_task") or hasattr(task, "func"):
            self.validate_task(task)

        if (
            isinstance(task, tasks_base_mod.Task)
            and len(args) == 2
            and isinstance(args[0], (tuple, list))
            and isinstance(args[1], dict)
            and not kwargs
        ):
            resolved_args = list(args[0])
            resolved_kwargs = dict(args[1])
        else:
            resolved_args = list(args)
            resolved_kwargs = dict(kwargs)

        now = datetime.now(UTC)

        task_name, func, queue_name, _priority, run_after = self._resolve_task_metadata(task)
        sched_time = run_after.timestamp() if run_after else None
        lane: TaskLane = "io"
        if queue_name in ("cpu", "io", "agent"):
            lane = cast("TaskLane", queue_name)

        if func is not None and task_name not in self._queue_service.registered_tasks:
            self._queue_service.register(TaskDef(name=task_name, fn=func, lane=lane))

        envelope = await self._queue_service.enqueue(
            task_name,
            *resolved_args,
            lane=lane,
            scheduled_at=sched_time,
            **resolved_kwargs,
        )
        task_id = envelope.task_id

        record = _TaskRecord(
            task=task,
            id=task_id,
            status=tasks_mod.TaskResultStatus.READY,
            enqueued_at=now,
            args=resolved_args,
            kwargs=resolved_kwargs,
            queue_name=queue_name,
            backend=self.alias,
        )
        with _RESULTS_LOCK:
            self._results[task_id] = record

        return self._build_result(record)

    def get_result(self, result_id: str) -> TaskResult[Any, Any]:
        """Retrieve a task result by id."""
        _, _, tasks_exc_mod = _ensure_django_tasks()
        with _RESULTS_LOCK:
            record = self._results.get(result_id)
        if record is None:
            msg = f"Task result {result_id!r} does not exist."
            raise tasks_exc_mod.TaskResultDoesNotExist(msg)
        return self._build_result(record)

    def get_task_queue_name(self, task_id: str) -> str | None:
        """Return the Django queue_name recorded for task_id, if tracked."""
        with _RESULTS_LOCK:
            record = self._results.get(task_id)
        return record.queue_name if record is not None else None

    async def aget_result(self, result_id: str) -> TaskResult[Any, Any]:
        """Retrieve a task result by id asynchronously."""
        return self.get_result(result_id)

    def update_task_running(self, task_id: str, *, worker_id: str = "worker") -> None:
        """Mark task as running with tracking metadata."""
        tasks_mod, _, _ = _ensure_django_tasks()
        with _RESULTS_LOCK:
            record = self._results.get(task_id)
            if record is not None:
                record.status = tasks_mod.TaskResultStatus.RUNNING
                now = datetime.now(UTC)
                record.started_at = record.started_at or now
                record.last_attempted_at = now
                if worker_id not in record.worker_ids:
                    record.worker_ids.append(worker_id)

    def update_task_success(self, task_id: str, *, result: Any, worker_id: str = "worker") -> None:
        """Mark task as successfully finished with return value."""
        tasks_mod, _, _ = _ensure_django_tasks()
        with _RESULTS_LOCK:
            record = self._results.get(task_id)
            if record is not None:
                record.status = tasks_mod.TaskResultStatus.SUCCESSFUL
                now = datetime.now(UTC)
                record.finished_at = now
                record.last_attempted_at = now
                record.return_value = result
                if worker_id not in record.worker_ids:
                    record.worker_ids.append(worker_id)

    def update_task_failed(
        self,
        task_id: str,
        *,
        error: BaseException,
        traceback_str: str,
        worker_id: str = "worker",
    ) -> None:
        """Mark task as failed with error details."""
        tasks_mod, tasks_base_mod, _ = _ensure_django_tasks()
        with _RESULTS_LOCK:
            record = self._results.get(task_id)
            if record is not None:
                record.status = tasks_mod.TaskResultStatus.FAILED
                now = datetime.now(UTC)
                record.finished_at = now
                record.last_attempted_at = now
                exc_path = f"{type(error).__module__}.{type(error).__qualname__}"
                record.errors.append(tasks_base_mod.TaskError(exception_class_path=exc_path, traceback=traceback_str))
                if worker_id not in record.worker_ids:
                    record.worker_ids.append(worker_id)

    def run_next(self, *, worker_id: str = "worker", container: Container | None = None) -> Any:
        """Synchronously execute the next queued task and record its result."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun_next(worker_id=worker_id, container=container))

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(
                asyncio.run,
                self.arun_next(worker_id=worker_id, container=container),
            ).result()

    async def arun_next(self, *, worker_id: str = "worker", container: Container | None = None) -> Any:
        """Asynchronously execute the next queued task and record its result."""
        envelopes = await self._queue_service.backend.dequeue(["cpu", "io", "agent"], batch_size=1)
        if not envelopes:
            return None
        env = envelopes[0]
        self.update_task_running(env.task_id, worker_id=worker_id)
        eff_container = container if container is not None else self._container
        try:
            res = await self._queue_service.process_envelope(env, container=eff_container)
            self.update_task_success(env.task_id, result=res, worker_id=worker_id)
            return res
        except Exception as exc:
            self.update_task_failed(
                env.task_id,
                error=exc,
                traceback_str=traceback.format_exc(),
                worker_id=worker_id,
            )
            raise

    def _resolve_task_metadata(
        self,
        task: Any,
    ) -> tuple[str, Callable[..., Any] | None, str, int, datetime | None]:
        if hasattr(task, "module_path"):
            return (
                task.module_path,
                getattr(task, "func", None),
                getattr(task, "queue_name", "default"),
                getattr(task, "priority", 0),
                getattr(task, "run_after", None),
            )
        if callable(task):
            for reg_name, reg_fn in self._registered_callables.items():
                if reg_fn is task:
                    return (reg_name, task, "default", 0, None)
            fallback_name = getattr(task, "__name__", str(task))
            return (fallback_name, task, "default", 0, None)
        task_name = str(task)
        func = self._registered_callables.get(task_name)
        return (task_name, func, "default", 0, None)

    def _build_result(self, record: _TaskRecord) -> TaskResult[Any, Any]:
        tasks_mod, _, _ = _ensure_django_tasks()
        result: TaskResult[Any, Any] = tasks_mod.TaskResult(
            task=record.task,
            id=record.id,
            status=record.status,
            enqueued_at=record.enqueued_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
            last_attempted_at=record.last_attempted_at,
            args=record.args,
            kwargs=record.kwargs,
            backend=record.backend,
            errors=record.errors,
            worker_ids=record.worker_ids,
        )
        if record.status == tasks_mod.TaskResultStatus.SUCCESSFUL:
            object.__setattr__(result, "_return_value", record.return_value)
        return result
