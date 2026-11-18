"""Background task queue extension and lifespan management for Starlette applications."""

from __future__ import annotations

from vertebra.framework.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    TaskDef,
    TaskEnvelope,
    attach_queue_service,
    get_queue_service,
    queue_lifespan,
    task,
)
from vertebra.framework.queues.backends import SHMQueueBackend

__all__ = (
    "QueueBackendSettings",
    "QueueConfig",
    "QueueService",
    "SHMQueueBackend",
    "TaskDef",
    "TaskEnvelope",
    "attach_queue_service",
    "get_queue_service",
    "queue_lifespan",
    "task",
)
