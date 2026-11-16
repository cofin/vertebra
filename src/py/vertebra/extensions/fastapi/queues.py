"""Background task queue extension and lifespan management for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions.starlette.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    SHMExecutionBackend,
    SHMQueueBackend,
    TaskDef,
    TaskEnvelope,
    attach_queue_service,
    create_shm_queue_config,
    create_sqlspec_queue_config,
    get_queue_service,
    queue_lifespan,
    task,
)

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
