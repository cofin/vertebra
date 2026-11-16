"""Background task queue extension and lifespan management for Starlette applications."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.extensions.litestar.queues import (
    SHMExecutionBackend,
    SHMQueueBackend,
    create_shm_queue_config,
    create_sqlspec_queue_config,
)
from vertebra.framework.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    TaskDef,
    TaskEnvelope,
    task,
)
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY, LaneConfig, WorkerRunner

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from vertebra.framework.di import Container

__all__ = (
    "QueueBackendSettings",
    "QueueConfig",
    "QueueService",
    "SHMExecutionBackend",
    "SHMQueueBackend",
    "TaskDef",
    "TaskEnvelope",
    "attach_queue_service",
    "create_shm_queue_config",
    "create_sqlspec_queue_config",
    "get_queue_service",
    "queue_lifespan",
    "task",
)


def attach_queue_service(
    app: Any,
    queue_service: QueueService | None = None,
    *,
    config: QueueConfig | QueueBackendSettings | None = None,
    container: Container | None = None,
) -> QueueService:
    """Attach or initialize a QueueService on a Starlette application state."""
    service = queue_service or QueueService(config=config, container=container)
    if hasattr(app, "state"):
        if hasattr(app.state, "_state") and isinstance(app.state._state, dict):
            app.state._state[QUEUE_SERVICE_STATE_KEY] = service
        else:
            setattr(app.state, QUEUE_SERVICE_STATE_KEY, service)
    return service


def get_queue_service(app: Any) -> QueueService:
    """Resolve the active QueueService from a Starlette application."""
    return WorkerRunner.resolve_queue_service(app)


@contextlib.asynccontextmanager
async def queue_lifespan(
    app: Any,
    *,
    lanes: Sequence[LaneConfig] | None = None,
    run_worker: bool = False,
    container: Container | None = None,
) -> AsyncIterator[QueueService]:
    """Async context manager managing QueueService and optional background WorkerRunner lifespan."""
    service = get_queue_service(app)
    worker_task: asyncio.Task[None] | None = None
    runner: WorkerRunner | None = None
    if run_worker:
        eff_lanes = lanes or [LaneConfig(queue_name="default", concurrency=10, lane_kind="io")]
        runner = WorkerRunner(eff_lanes, queue_service=service, container=container or service.container)
        worker_task = asyncio.create_task(runner.run())
    try:
        yield service
    finally:
        if runner is not None:
            await runner.stop()
        if worker_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        await service.close()
