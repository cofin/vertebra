"""Valkey synchronous and asynchronous durable queue stores."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.db.driver._resp_queue import AsyncRESPQueueStore, SyncRESPQueueStore

if TYPE_CHECKING:
    from vertebra.db.adapters.valkey._async import AsyncValkeyDriver
    from vertebra.db.adapters.valkey._sync import SyncValkeyDriver
    from vertebra.db.adapters.valkey.notify import (
        AsyncValkeyNotificationSource,
        SyncValkeyNotificationSource,
    )

__all__ = (
    "AsyncValkeyQueueStore",
    "SyncValkeyQueueStore",
)


class SyncValkeyQueueStore(SyncRESPQueueStore):
    """Synchronous Valkey queue store implementing SyncQueueStore over RESP3 Lua scripts."""

    __slots__ = ()

    def __init__(
        self,
        driver: SyncValkeyDriver,
        *,
        notification_source: SyncValkeyNotificationSource | None = None,
        key_prefix: str = "vtb:queue",
    ) -> None:
        """Initialize the Valkey synchronous queue store."""
        super().__init__(driver, notification_source=notification_source, key_prefix=key_prefix)


class AsyncValkeyQueueStore(AsyncRESPQueueStore):
    """Asynchronous Valkey queue store implementing AsyncQueueStore over RESP3 Lua scripts."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncValkeyDriver,
        *,
        notification_source: AsyncValkeyNotificationSource | None = None,
        key_prefix: str = "vtb:queue",
    ) -> None:
        """Initialize the Valkey asynchronous queue store."""
        super().__init__(driver, notification_source=notification_source, key_prefix=key_prefix)
