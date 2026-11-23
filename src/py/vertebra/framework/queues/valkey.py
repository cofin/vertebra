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
    _BaseRESPStreamAdapter,
    _BaseSyncRESPStreamAdapter,
    _decode_str,
    _decode_stream_entries,
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


class _ValkeyStreamAdapter(_BaseRESPStreamAdapter):
    """Adapter bridging AsyncValkeyClient to the ValkeyStreamClient protocol."""

    __slots__ = ()

    def __init__(self, client: AsyncValkeyClient) -> None:
        super().__init__(client)

    async def xadd(
        self, stream: str, fields: Mapping[str, Any], id: str = "*", maxlen: int | None = None, approximate: bool = True
    ) -> str:
        """Append entry to stream."""
        field_seq: list[tuple[bytes | str, bytes | str]] = [
            (k, v if isinstance(v, (str, bytes)) else str(v)) for k, v in fields.items()
        ]
        return _decode_str(await self._client.xadd(stream, field_seq, id=id, maxlen=maxlen, approximate=approximate))

    async def xreadgroup(
        self, group: str, consumer: str, streams: Mapping[str, str], count: int | None = None, block: int | None = None
    ) -> list[Any]:
        """Read entries from streams via consumer group."""
        res = await self._client.xreadgroup(group, consumer, list(streams.items()), count=count, block_ms=block)
        return list(res) if res is not None else []

    async def xrange(
        self, stream: str, min: str = "-", max: str = "+", count: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read range of stream entries."""
        return _decode_stream_entries(await self._client.xrange(stream, min=min, max=max, count=count))

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        return int(await self._client.zadd(key, list(mapping.items())))

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Evaluate Lua script."""
        return await self._client.eval(script, list(keys_and_args[:numkeys]), list(keys_and_args[numkeys:]))

    async def aclose(self) -> None:
        """Close client connection."""
        res = self._client.aclose()
        if hasattr(res, "__await__"):
            await res


class _SyncValkeyStreamAdapter(_BaseSyncRESPStreamAdapter):
    """Adapter bridging SyncValkeyClient to the SyncValkeyStreamClient protocol."""

    __slots__ = ()

    def __init__(self, client: SyncValkeyClient) -> None:
        super().__init__(client)

    def xadd(
        self, stream: str, fields: Mapping[str, Any], id: str = "*", maxlen: int | None = None, approximate: bool = True
    ) -> str:
        """Append entry to stream."""
        field_seq: list[tuple[bytes | str, bytes | str]] = [
            (_decode_str(k), v if isinstance(v, (str, bytes)) else str(v)) for k, v in fields.items()
        ]
        return _decode_str(self._client.xadd(stream, field_seq, id=id, maxlen=maxlen, approximate=approximate))

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        """Add members to sorted set."""
        member_seq: list[tuple[bytes | str, float]] = [(_decode_str(k), float(v)) for k, v in mapping.items()]
        return int(self._client.zadd(key, member_seq))


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
