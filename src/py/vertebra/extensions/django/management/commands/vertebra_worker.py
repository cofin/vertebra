"""Management command to run the Vertebra task worker for Django 6.0+ tasks."""

from __future__ import annotations

import asyncio
import traceback
from typing import Any

from django.core.management.base import BaseCommand
from django.tasks import task_backends
from django.utils.module_loading import import_string

from vertebra.framework.di import Container
from vertebra.server.lanes import LaneConfig, WorkerRunner


class Command(BaseCommand):
    """Run Vertebra worker processing tasks from Django task backends."""

    help = "Run the Vertebra worker to process queued Django 6.0+ background tasks."

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
        backend_alias = options.get("backend", "default")
        backend = task_backends[backend_alias]
        queues = options.get("queues") or list(backend.queues)
        concurrency = options.get("concurrency", 1)
        once = options.get("once", False)
        burst = options.get("burst", False)
        container = getattr(backend, "container", None) or Container()

        asyncio.run(
            self._run_async(
                backend=backend,
                queues=queues,
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
        concurrency: int,
        once: bool,
        burst: bool,
        container: Container,
    ) -> None:
        lanes = [LaneConfig(queue_name=q, concurrency=concurrency) for q in queues]
        runner = WorkerRunner(
            lanes=lanes,
            queue_service=backend.queue_service,
            container=container,
        )

        worker_id = f"worker-{backend.alias}"

        async def _drain_step() -> int:
            envelopes = await backend.queue_service.backend.dequeue(["cpu", "io", "agent"], batch_size=1)
            if not envelopes:
                return 0
            env = envelopes[0]
            backend.update_task_running(env.task_id, worker_id=worker_id)
            if env.task_name not in backend.queue_service.registered_tasks:
                try:
                    obj = import_string(env.task_name)
                    func = getattr(obj, "func", obj)
                    from vertebra.framework.queues.base import TaskDef

                    backend.queue_service.register(TaskDef(name=env.task_name, fn=func, lane=env.lane))
                except Exception as exc:
                    tb = traceback.format_exc()
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
