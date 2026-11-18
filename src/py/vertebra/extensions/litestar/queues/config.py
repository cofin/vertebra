"""Configuration helpers for Vertebra queue backends."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.extensions.litestar.channels import ChannelsBackend
from vertebra.extensions.litestar.queues.backend import StoreQueueBackendConfig
from vertebra.extensions.litestar.queues.shm_backend import SHMQueueBackendConfig
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from datetime import timedelta

    from vertebra.db.driver.capabilities import AsyncNotificationSource, AsyncQueueStore

try:
    from litestar_queues.config import QueueConfig, WorkerConfig
    from litestar_queues.events import (
        EventDeliveryConfig,
        EventHistoryConfig,
        EventStreamConfig,
        QueueEventsConfig,
    )
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-queues",
        install_package="litestar-queues",
        extra="queues",
    ) from exc


def create_sqlspec_queue_config(
    sqlspec_config: Any,
    *,
    channels_backend: Any = None,
    queue_table_name: str = "queue_task",
    event_history_table_name: str = "queue_task_event_history",
    task_reservation_table_name: str = "queue_task_reservation",
    maintenance_table_name: str = "queue_maintenance",
    worker_wakeups: Any = None,
    manage_schema: bool = False,
    queue_store_class: Any = None,
    events_store_class: Any = None,
    worker: WorkerConfig | None = None,
    delivery: EventDeliveryConfig | None = None,
    stream: EventStreamConfig | None = None,
    history: EventHistoryConfig | None = None,
    execution_backend: Any = "local",
    **kwargs: Any,
) -> QueueConfig:
    """Create a QueueConfig backed by SQLSpec persistence and ChannelsBackend events."""
    ensure_module(
        "litestar_queues",
        package="litestar-queues",
        extra="queues",
    )
    sqlspec_backend_mod = ensure_module(
        "litestar_queues.backends.sqlspec",
        package="litestar-queues",
        extra="queues",
    )
    events_mod = ensure_module(
        "litestar_queues.events",
        package="litestar-queues",
        extra="queues",
    )

    resolved_channels = channels_backend if channels_backend is not None else ChannelsBackend()
    resolved_delivery = (
        delivery
        if delivery is not None
        else events_mod.EventDeliveryConfig(publish_task_channel=True, publish_global_lifecycle=True)
    )

    events_config = events_mod.QueueEventsConfig(
        channels=resolved_channels,
        delivery=resolved_delivery,
        stream=stream,
        history=history,
    )

    if queue_store_class is not None:
        with contextlib.suppress(AttributeError, TypeError):
            sqlspec_config.queue_store_class = queue_store_class
    if events_store_class is not None:
        with contextlib.suppress(AttributeError, TypeError):
            sqlspec_config.events_store_class = events_store_class

    backend_config = sqlspec_backend_mod.SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        queue_table_name=queue_table_name,
        event_history_table_name=event_history_table_name,
        task_reservation_table_name=task_reservation_table_name,
        maintenance_table_name=maintenance_table_name,
        worker_wakeups=worker_wakeups,
        manage_schema=manage_schema,
    )

    resolved_worker = worker if worker is not None else WorkerConfig()

    return QueueConfig(
        queue_backend=backend_config,
        execution_backend=execution_backend,
        worker=resolved_worker,
        events=events_config,
        **kwargs,
    )


def create_shm_queue_config(
    *,
    min_wakeup_timeout: float = 0.005,
    max_wakeup_timeout: float = 30.0,
    worker: WorkerConfig | None = None,
    events: QueueEventsConfig | None = None,
    task_dependency_provider: Any = None,
    **kwargs: Any,
) -> QueueConfig:
    """Create a QueueConfig using Vertebra SHMQueueBackend and SHMExecutionBackend."""
    ensure_module(
        "litestar_queues",
        package="litestar-queues",
        extra="queues",
    )

    backend_config = SHMQueueBackendConfig(
        min_wakeup_timeout=min_wakeup_timeout,
        max_wakeup_timeout=max_wakeup_timeout,
    )
    resolved_worker = worker if worker is not None else WorkerConfig()

    return QueueConfig(
        queue_backend=backend_config,
        execution_backend="vertebra.extensions.litestar.queues.backend.SHMExecutionBackend",
        worker=resolved_worker,
        events=events,
        task_dependency_provider=task_dependency_provider,
        **kwargs,
    )


def create_store_queue_config(
    store: AsyncQueueStore,
    *,
    notification_source: AsyncNotificationSource | None = None,
    terminal_ttl: timedelta | None = None,
    worker: WorkerConfig | None = None,
    events: QueueEventsConfig | None = None,
    task_dependency_provider: Any = None,
    **kwargs: Any,
) -> QueueConfig:
    """Create a QueueConfig backed by StoreQueueBackendConfig."""
    ensure_module(
        "litestar_queues",
        package="litestar-queues",
        extra="queues",
    )

    backend_config = StoreQueueBackendConfig(
        store=store,
        notification_source=notification_source,
        terminal_ttl=terminal_ttl,
    )
    resolved_worker = worker if worker is not None else WorkerConfig()

    return QueueConfig(
        queue_backend=backend_config,
        worker=resolved_worker,
        events=events,
        task_dependency_provider=task_dependency_provider,
        **kwargs,
    )
