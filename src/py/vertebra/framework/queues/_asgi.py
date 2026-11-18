"""Framework-neutral ASGI queue attachment and lifespan helpers."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.framework.queues.base import QueueBackendSettings, QueueConfig, QueueService
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY, LaneConfig, WorkerRunner

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from vertebra.framework.di import Container

__all__ = (
    "attach_queue_service",
    "get_queue_service",
    "queue_lifespan",
)


def attach_queue_service(
    app: Any,
    queue_service: QueueService | None = None,
    *,
    config: QueueConfig | QueueBackendSettings | None = None,
    container: Container | None = None,
) -> QueueService:
    """Attach or initialize a ``QueueService`` on an ASGI application ``.state``."""
    service = queue_service or QueueService(config=config, container=container)
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state[QUEUE_SERVICE_STATE_KEY] = service
        elif hasattr(state, "_state") and isinstance(state._state, dict):
            state._state[QUEUE_SERVICE_STATE_KEY] = service
        else:
            setattr(state, QUEUE_SERVICE_STATE_KEY, service)
    return service


def get_queue_service(app: Any) -> QueueService:
    """Resolve the active ``QueueService`` from an ASGI application."""
    return WorkerRunner.resolve_queue_service(app)


@contextlib.asynccontextmanager
async def queue_lifespan(
    app: Any,
    *,
    lanes: Sequence[LaneConfig] | None = None,
    run_worker: bool = False,
    container: Container | None = None,
) -> AsyncIterator[QueueService]:
    """Manage ``QueueService`` and optional background ``WorkerRunner`` lifespan."""
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
            if not worker_task.done():
                worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        await service.close()
