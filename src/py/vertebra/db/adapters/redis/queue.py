"""Redis synchronous and asynchronous durable queue stores."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.db.driver._resp_queue import AsyncRESPQueueStore, SyncRESPQueueStore

if TYPE_CHECKING:
    from vertebra.db.adapters.redis._async import AsyncRedisDriver
    from vertebra.db.adapters.redis._sync import SyncRedisDriver
    from vertebra.db.adapters.redis.notify import (
        AsyncRedisNotificationSource,
        SyncRedisNotificationSource,
    )

__all__ = (
    "AsyncRedisQueueStore",
    "SyncRedisQueueStore",
)


class SyncRedisQueueStore(SyncRESPQueueStore):
    """Synchronous Redis queue store implementing SyncQueueStore over RESP3 Lua scripts."""

    __slots__ = ()

    def __init__(
        self,
        driver: SyncRedisDriver,
        *,
        notification_source: SyncRedisNotificationSource | None = None,
        key_prefix: str = "vtb:queue",
    ) -> None:
        """Initialize the Redis synchronous queue store."""
        super().__init__(driver, notification_source=notification_source, key_prefix=key_prefix)


class AsyncRedisQueueStore(AsyncRESPQueueStore):
    """Asynchronous Redis queue store implementing AsyncQueueStore over RESP3 Lua scripts."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncRedisDriver,
        *,
        notification_source: AsyncRedisNotificationSource | None = None,
        key_prefix: str = "vtb:queue",
    ) -> None:
        """Initialize the Redis asynchronous queue store."""
        super().__init__(driver, notification_source=notification_source, key_prefix=key_prefix)
