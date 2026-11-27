"""Redis Streams queue backend with visibility timeout and DLQ routing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.framework.queues._resp import (
    _PROMOTE_SCHEDULED_LUA as _PROMOTE_SCHEDULED_LUA,
)
from vertebra.framework.queues._resp import (
    _RENEW_LEASE_LUA as _RENEW_LEASE_LUA,
)
from vertebra.framework.queues._resp import (
    REDIS_QUEUE_DIALECT,
    AsyncRESPStreamClient,
    RESPQueueDialect,
    RESPStreamQueueBackend,
    SyncRESPStreamClient,
    _BaseRESPStreamAdapter,
    _BaseSyncRESPStreamAdapter,
    _decode_str,
    _decode_stream_entries,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra._redis import AsyncRedisClient, SyncRedisClient
    from vertebra.db.adapters.redis.notify import (
        AsyncRedisNotificationSource,
        SyncRedisNotificationSource,
    )
    from vertebra.framework.queues.base import QueueBackendSettings, QueueConfig

__all__ = (
    "AsyncRESPStreamClient",
    "RedisQueueBackend",
    "SyncRESPStreamClient",
)


class _RedisStreamAdapter(_BaseRESPStreamAdapter):
    """Adapter bridging AsyncRedisClient to the AsyncRESPStreamClient protocol."""

    __slots__ = ()

    def __init__(self, client: AsyncRedisClient) -> None:
        super().__init__(client)

    async def xadd(
        self, stream: str, fields: Mapping[str, Any], id: str = "*", maxlen: int | None = None, approximate: bool = True
    ) -> str:
        """Append entry to stream."""
        res = await self._client.xadd(stream, dict(fields), id=id, maxlen=maxlen, approximate=approximate)
        return _decode_str(res)

    async def xreadgroup(
        self, group: str, consumer: str, streams: Mapping[str, str], count: int | None = None, block: int | None = None
    ) -> list[Any]:
        """Read entries from streams via consumer group."""
        res = await self._client.xreadgroup(group, consumer, dict(streams), count=count, block_ms=block)
        return list(res) if res is not None else []

    async def xrange(
        self, stream: str, min: str = "-", max: str = "+", count: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read range of stream entries."""
        return _decode_stream_entries(await self._client.xrange(stream, start=min, end=max, count=count))

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(await self._client.zadd(key, dict(mapping)))

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        return await self._client.eval_script(script, list(keys_and_args[:numkeys]), list(keys_and_args[numkeys:]))

    async def aclose(self) -> None:
        """Close client connection."""
        self._client.close()


class _SyncRedisStreamAdapter(_BaseSyncRESPStreamAdapter):
    """Adapter bridging SyncRedisClient to the SyncRESPStreamClient protocol."""

    __slots__ = ()

    def __init__(self, client: SyncRedisClient) -> None:
        super().__init__(client)

    def xadd(
        self, stream: str, fields: Mapping[str, Any], id: str = "*", maxlen: int | None = None, approximate: bool = True
    ) -> str:
        """Append entry to stream."""
        return _decode_str(self._client.xadd(stream, dict(fields), id=id, maxlen=maxlen, approximate=approximate))

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(self._client.zadd(key, dict(mapping)))


class RedisQueueBackend(RESPStreamQueueBackend):
    """Distributed task queue backend powered by Redis Streams and Sorted Sets."""

    _DIALECT: ClassVar[RESPQueueDialect] = REDIS_QUEUE_DIALECT

    __slots__ = ()

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        client: AsyncRESPStreamClient | None = None,
        sync_client: SyncRESPStreamClient | None = None,
        notification_source: AsyncRedisNotificationSource | None = None,
        sync_notification_source: SyncRedisNotificationSource | None = None,
    ) -> None:
        """Bind the queue backend to Redis Streams clients and notification sources."""
        super().__init__(
            config,
            client=client,
            sync_client=sync_client,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
        )

    @classmethod
    def _wrap_async_client(cls, client: Any) -> AsyncRESPStreamClient:
        return _RedisStreamAdapter(client)

    @classmethod
    def _wrap_sync_client(cls, client: Any) -> SyncRESPStreamClient:
        return _SyncRedisStreamAdapter(client)


_ = (_PROMOTE_SCHEDULED_LUA, _RENEW_LEASE_LUA)
