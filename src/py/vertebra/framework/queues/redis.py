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
    RESPQueueDialect,
    RESPStreamClient,
    RESPStreamQueueBackend,
    SyncRESPStreamClient,
    _normalize_stream_fields,
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
    "RedisQueueBackend",
    "RedisStreamClient",
    "SyncRedisStreamClient",
)

RedisStreamClient = RESPStreamClient
SyncRedisStreamClient = SyncRESPStreamClient


class _RedisStreamAdapter:
    """Adapter bridging AsyncRedisClient to the RedisStreamClient protocol."""

    __slots__ = ("_client",)

    def __init__(self, client: AsyncRedisClient) -> None:
        self._client = client

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any:
        """Create consumer group on stream."""
        return await self._client.xgroup_create(stream, group, id=id, mkstream=mkstream)

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append entry to stream."""
        res = await self._client.xadd(stream, dict(fields), id=id, maxlen=maxlen, approximate=approximate)
        return res.decode("utf-8") if isinstance(res, bytes) else str(res)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        """Read entries from streams via consumer group."""
        res = await self._client.xreadgroup(group, consumer, dict(streams), count=count, block_ms=block)
        return res if res is not None else []

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Acknowledge processed stream entries."""
        return int(await self._client.xack(stream, group, list(ids)))

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]], list[str]]:
        """Reclaim pending entries exceeding idle time."""
        res = await self._client.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time,
            start_id=start_id,
            count=count,
        )
        if not res or len(res) < 3:
            return "0-0", [], []
        next_id, raw_entries, deleted = res
        next_id_str = next_id.decode("utf-8") if isinstance(next_id, bytes) else str(next_id)
        entries: list[tuple[str, dict[str, Any]]] = []
        for item in raw_entries:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                eid = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
                entries.append((eid, _normalize_stream_fields(item[1])))
        deleted_list = [d.decode("utf-8") if isinstance(d, bytes) else str(d) for d in deleted]
        return next_id_str, entries, deleted_list

    async def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read range of stream entries."""
        res = await self._client.xrange(stream, start=min, end=max, count=count)
        if not res:
            return []
        entries: list[tuple[str, dict[str, Any]]] = []
        for item in res:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                eid = item[0].decode("utf-8") if isinstance(item[0], bytes) else str(item[0])
                entries.append((eid, _normalize_stream_fields(item[1])))
        return entries

    async def xdel(self, stream: str, *ids: str) -> int:
        """Delete stream entries by ID."""
        return int(await self._client.xdel(stream, list(ids)))

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(await self._client.zadd(key, dict(mapping)))

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        return await self._client.eval_script(script, keys, args)

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> Any:
        """Set key with optional NX and PX options."""
        return await self._client.set(key, value, nx=nx, px=px)

    async def get(self, key: str) -> str | None:
        """Get string value by key."""
        raw = await self._client.get(key)
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    async def aclose(self) -> None:
        """Close client connection."""
        self._client.close()


class _SyncRedisStreamAdapter:
    """Adapter bridging SyncRedisClient to the SyncRedisStreamClient protocol."""

    __slots__ = ("_client",)

    def __init__(self, client: SyncRedisClient) -> None:
        self._client = client

    def xadd(
        self,
        stream: str,
        fields: Mapping[str, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """Append entry to stream."""
        res = self._client.xadd(stream, dict(fields), id=id, maxlen=maxlen, approximate=approximate)
        return res.decode("utf-8") if isinstance(res, bytes) else str(res)

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(self._client.zadd(key, dict(mapping)))

    def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
    ) -> Any:
        """Set key with optional NX and PX options."""
        return self._client.set(key, value, nx=nx, px=px)

    def get(self, key: str) -> str | None:
        """Get string value by key."""
        raw = self._client.get(key)
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel."""
        return int(self._client.publish(channel, message))

    def close(self) -> None:
        """Close client connection."""
        self._client.close()


class RedisQueueBackend(RESPStreamQueueBackend):
    """Distributed task queue backend powered by Redis Streams and Sorted Sets."""

    _DIALECT: ClassVar[RESPQueueDialect] = REDIS_QUEUE_DIALECT

    __slots__ = ()

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        client: RESPStreamClient | None = None,
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
    def _wrap_async_client(cls, client: Any) -> RESPStreamClient:
        return _RedisStreamAdapter(client)

    @classmethod
    def _wrap_sync_client(cls, client: Any) -> SyncRESPStreamClient:
        return _SyncRedisStreamAdapter(client)


_ = (_PROMOTE_SCHEDULED_LUA, _RENEW_LEASE_LUA)
