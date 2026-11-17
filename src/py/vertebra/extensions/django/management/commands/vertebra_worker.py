"""Management command to run the Vertebra task worker for Django 6.0+ tasks."""

from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING, Any

from vertebra.framework.di import Container
from vertebra.framework.queues.base import TaskDef, TaskLane
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.utils.module_loader import ensure_module, import_optional

_base_cmd_mod = import_optional("django.core.management.base")

if TYPE_CHECKING:
    from django.core.management.base import BaseCommand as _BaseCommand
else:
    _BaseCommand = _base_cmd_mod.BaseCommand if _base_cmd_mod is not None else object

__all__ = ("Command",)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django management base classes on demand."""
    if name == "BaseCommand":
        return ensure_module("django.core.management.base", package="django", extra="django").BaseCommand
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def _queue_to_lane(queue_name: str) -> TaskLane:
    """Map a Django queue name onto its corresponding Vertebra TaskLane."""
    if queue_name == "cpu":
        return "cpu"
    if queue_name == "agent":
        return "agent"
    return "io"


class Command(_BaseCommand):
    """Run Vertebra worker processing tasks from Django task backends."""

    help = "Run the Vertebra worker to process queued Django 6.0+ background tasks."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if _base_cmd_mod is None:
            ensure_module("django.core.management.base", package="django", extra="django")
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser: Any) -> None:
        """Add CLI arguments for worker execution."""
        parser.add_argument(
            "--backend",
            default="default",
            help="The task backend to consume from (default: default).",
        )
        parser.add_argument(
            "--queues",
            nargs="*",
            default=None,
            help="Queue names to consume from (default: all configured queues).",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=1,
            help="Maximum concurrent tasks per lane (default: 1).",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run ready tasks once and exit.",
        )
        parser.add_argument(
            "--burst",
            action="store_true",
            help="Run ready tasks until queues are empty and exit.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Execute the worker command with configured options."""
        _ = args
        tasks_mod = ensure_module("django.tasks", package="django", extra="django")
        backend_alias = options.get("backend", "default")
        backend = tasks_mod.task_backends[backend_alias]
        explicit_queues: list[str] | None = options.get("queues")
        queues = list(explicit_queues) if explicit_queues else list(backend.queues)
        concurrency = options.get("concurrency", 1)
        once = options.get("once", False)
        burst = options.get("burst", False)
        container = getattr(backend, "container", None) or Container()

        asyncio.run(
            self._run_async(
                backend=backend,
                queues=queues,
                explicit_queues=explicit_queues is not None and len(explicit_queues) > 0,
                concurrency=concurrency,
                once=once,
                burst=burst,
                container=container,
            )
        )

    async def _run_async(
        self,
        *,
        backend: Any,
        queues: list[str],
        explicit_queues: bool = False,
        concurrency: int,
        once: bool,
        burst: bool,
        container: Container,
    ) -> None:
        effective_queues = queues if queues else ["default"]
        lanes = [
            LaneConfig(
                queue_name=q,
                concurrency=max(1, concurrency),
                lane_kind=_queue_to_lane(q),
            )
            for q in effective_queues
        ]
        if explicit_queues or set(effective_queues) != {"default"}:
            target_lanes: list[TaskLane] = list(dict.fromkeys(_queue_to_lane(q) for q in effective_queues))
        else:
            target_lanes = ["cpu", "io", "agent"]

        allowed_queues = set(effective_queues)
        runner = WorkerRunner(
            lanes=lanes,
            queue_service=backend.queue_service,
            container=container,
        )

        worker_id = f"worker-{backend.alias}"

        async def _drain_step() -> int:
            envelopes = await backend.queue_service.backend.dequeue(target_lanes, batch_size=1)
            if not envelopes:
                return 0
            env = envelopes[0]
            if explicit_queues:
                queue_lookup = getattr(backend, "get_task_queue_name", None)
                recorded_queue = queue_lookup(env.task_id) if callable(queue_lookup) else None
                if (
                    recorded_queue is not None
                    and recorded_queue not in allowed_queues
                    and env.lane not in allowed_queues
                ):
                    await backend.queue_service.backend.nack(env, error="Queue filter mismatch", delay_seconds=0.0)
                    return 0
            backend.update_task_running(env.task_id, worker_id=worker_id)
            if env.task_name not in backend.queue_service.registered_tasks:
                try:
                    mod_loader = ensure_module("django.utils.module_loading", package="django", extra="django")
                    obj = mod_loader.import_string(env.task_name)
                    func = getattr(obj, "func", obj)
                    backend.queue_service.register(TaskDef(name=env.task_name, fn=func, lane=env.lane))
                except Exception as exc:
                    tb = traceback.format_exc()
                    await backend.queue_service.backend.nack(env, error=exc, send_to_dlq=True)
                    backend.update_task_failed(env.task_id, error=exc, traceback_str=tb, worker_id=worker_id)
                    return 1

            try:
                result = await backend.queue_service.process_envelope(env, container=container)
                backend.update_task_success(env.task_id, result=result, worker_id=worker_id)
            except Exception as exc:
                tb = traceback.format_exc()
                backend.update_task_failed(env.task_id, error=exc, traceback_str=tb, worker_id=worker_id)
            return 1

        if once:
            await _drain_step()
            return

        if burst:
            while True:
                processed = await _drain_step()
                if processed == 0:
                    break
            return

        await runner.run()
