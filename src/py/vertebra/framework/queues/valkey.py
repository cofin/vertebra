"""Valkey Streams queue backend with visibility timeout and DLQ routing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.framework.queues._resp import (
    _PROMOTE_SCHEDULED_LUA as _PROMOTE_SCHEDULED_LUA,
)
from vertebra.framework.queues._resp import (
    _RENEW_LEASE_LUA as _RENEW_LEASE_LUA,
)
from vertebra.framework.queues._resp import (
    VALKEY_QUEUE_DIALECT,
    RESPQueueDialect,
    RESPStreamClient,
    RESPStreamQueueBackend,
    SyncRESPStreamClient,
    _normalize_stream_fields,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertebra._valkey import AsyncValkeyClient, SyncValkeyClient
    from vertebra.db.adapters.valkey.notify import (
        AsyncValkeyNotificationSource,
        SyncValkeyNotificationSource,
    )
    from vertebra.framework.queues.base import QueueBackendSettings, QueueConfig

__all__ = (
    "SyncValkeyStreamClient",
    "ValkeyQueueBackend",
    "ValkeyStreamClient",
)

ValkeyStreamClient = RESPStreamClient
SyncValkeyStreamClient = SyncRESPStreamClient


class _ValkeyStreamAdapter:
    """Adapter bridging AsyncValkeyClient to the ValkeyStreamClient protocol."""

    __slots__ = ("_client",)

    def __init__(self, client: AsyncValkeyClient) -> None:
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
        field_seq: list[tuple[bytes | str, bytes | str]] = [
            (k, v if isinstance(v, (str, bytes)) else str(v)) for k, v in fields.items()
        ]
        res = await self._client.xadd(stream, field_seq, id=id, maxlen=maxlen, approximate=approximate)
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
        stream_seq: list[tuple[bytes | str, str]] = list(streams.items())
        res = await self._client.xreadgroup(group, consumer, stream_seq, count=count, block_ms=block)
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
        res = await self._client.xrange(stream, min=min, max=max, count=count)
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
        member_seq: list[tuple[bytes | str, float]] = list(mapping.items())
        return int(await self._client.zadd(key, member_seq))

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        return await self._client.eval(script, keys, args)

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
        res = self._client.aclose()
        if hasattr(res, "__await__"):
            await res


class _SyncValkeyStreamAdapter:
    """Adapter bridging SyncValkeyClient to the SyncValkeyStreamClient protocol."""

    __slots__ = ("_client",)

    def __init__(self, client: SyncValkeyClient) -> None:
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
        field_seq: list[tuple[bytes | str, bytes | str]] = [
            (
                k.decode("utf-8") if isinstance(k, bytes) else str(k),
                v if isinstance(v, (str, bytes)) else str(v),
            )
            for k, v in fields.items()
        ]
        res = self._client.xadd(stream, field_seq, id=id, maxlen=maxlen, approximate=approximate)
        return res.decode("utf-8") if isinstance(res, bytes) else str(res)

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        member_seq: list[tuple[bytes | str, float]] = [
            (k.decode("utf-8") if isinstance(k, bytes) else str(k), float(v)) for k, v in mapping.items()
        ]
        return int(self._client.zadd(key, member_seq))

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


class ValkeyQueueBackend(RESPStreamQueueBackend):
    """Distributed task queue backend powered by Valkey Streams and Sorted Sets."""

    _DIALECT: ClassVar[RESPQueueDialect] = VALKEY_QUEUE_DIALECT

    __slots__ = ()

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        client: RESPStreamClient | None = None,
        sync_client: SyncRESPStreamClient | None = None,
        notification_source: AsyncValkeyNotificationSource | None = None,
        sync_notification_source: SyncValkeyNotificationSource | None = None,
    ) -> None:
        """Bind the queue backend to Valkey Streams clients and notification sources."""
        super().__init__(
            config,
            client=client,
            sync_client=sync_client,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
        )

    @classmethod
    def _wrap_async_client(cls, client: Any) -> RESPStreamClient:
        return _ValkeyStreamAdapter(client)

    @classmethod
    def _wrap_sync_client(cls, client: Any) -> SyncRESPStreamClient:
        return _SyncValkeyStreamAdapter(client)


_ = (_PROMOTE_SCHEDULED_LUA, _RENEW_LEASE_LUA)
