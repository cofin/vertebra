"""Vertebra litestar-queues integration package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.queues.backend import SHMExecutionBackend, SHMQueueBackend
    from vertebra.extensions.litestar.queues.config import create_shm_queue_config, create_sqlspec_queue_config

__all__ = (
    "SHMExecutionBackend",
    "SHMQueueBackend",
    "create_shm_queue_config",
    "create_sqlspec_queue_config",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "SHMExecutionBackend": ("vertebra.extensions.litestar.queues.backend", "SHMExecutionBackend"),
    "SHMQueueBackend": ("vertebra.extensions.litestar.queues.backend", "SHMQueueBackend"),
    "create_shm_queue_config": ("vertebra.extensions.litestar.queues.config", "create_shm_queue_config"),
    "create_sqlspec_queue_config": ("vertebra.extensions.litestar.queues.config", "create_sqlspec_queue_config"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
