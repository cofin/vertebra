"""PostgreSQL notification transport and logical replication message builders."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING

from vertebra.db.driver import AsyncDatabaseDriver, NotificationEvent, NotificationSource, NotSupportedError
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.driver import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
    from vertebra.db.cdc import ChangeFeed

__all__ = ("PostgresNotificationSource", "PostgresNotifyConfig")


class PostgresNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """PostgreSQL notification and logical replication configuration."""

    channel: str = "vertebra_events"
    """Default PostgreSQL notification channel identifier."""

    spill_threshold_bytes: int = 7500
    """Maximum inline notification payload bytes before ChangeFeed spill storage."""

    spill_table: str = "vertebra_notify_spill"
    """Relational fallback spill table identifier."""

    spill_prefix: str = "cdc/spill"
    """Object key prefix for spilled notification payloads."""

    replication_slot_name: str = "vertebra_cdc_slot"
    """PostgreSQL logical replication slot identifier."""

    publication_name: str = "vertebra_pub"
    """PostgreSQL logical replication publication identifier."""

    max_slot_wal_keep_size_mb: int = 4096
    """Maximum WAL retention size in megabytes for logical replication slots."""

    def __post_init__(self) -> None:
        validate_identifier(self.channel, "channel", allow_dots=False)
        validate_identifier(self.spill_table, "spill_table", allow_dots=False)
        validate_identifier(self.replication_slot_name, "replication_slot_name", allow_dots=False)
        validate_identifier(self.publication_name, "publication_name", allow_dots=False)


class PostgresNotificationSource(NotificationSource):
    """Publish PostgreSQL notifications and delegate streaming to a capable driver."""

    __slots__ = ("_change_feed", "_config", "_driver", "_subscribed_channels")

    def __init__(
        self,
        driver: AsyncPostgreSQLDriver | SyncPostgreSQLDriver,
        config: PostgresNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        self._driver = driver
        self._config = config or PostgresNotifyConfig()
        self._change_feed = change_feed
        self._subscribed_channels: dict[str, None] = {}

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional feed used to wrap published notification payloads."""
        return self._change_feed

    @staticmethod
    def build_emit_message_sql(
        prefix: str,
        payload: bytes | str,
        *,
        transactional: bool = True,
    ) -> tuple[str, tuple[bool, str, bytes]]:
        """Build a logical replication message statement with bound payload bytes."""
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        return "SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, raw)

    @staticmethod
    def build_notify_sql(channel: str, payload: str) -> tuple[str, tuple[str, str]]:
        """Build a notification statement with bound channel and payload values."""
        validate_identifier(channel, "channel", allow_dots=False)
        return "SELECT pg_notify($1, $2)", (channel, payload)

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a raw payload or canonical change-feed envelope through the driver."""
        validate_identifier(channel, "channel", allow_dots=False)
        if self._change_feed is not None:
            raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
            wire_payload, _ = self._change_feed.publish_event(
                "PUBLISH", {"channel": channel, "data_b64": base64.b64encode(raw_bytes).decode("ascii")}
            )
        else:
            wire_payload = (
                payload if isinstance(payload, str) else "b64:" + base64.b64encode(bytes(payload)).decode("ascii")
            )
        sql, parameters = self.build_notify_sql(channel, wire_payload)
        if isinstance(self._driver, AsyncDatabaseDriver):
            await self._driver.execute(sql, parameters)
        else:
            await asyncio.to_thread(self._driver.execute, "SELECT pg_notify(%s, %s)", parameters)

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Register validated channel identifiers for the next driver stream."""
        validated = tuple(validate_identifier(channel, "channel", allow_dots=False) for channel in channels)
        self._subscribed_channels.update(dict.fromkeys(validated))

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        """Remove validated channel identifiers from future driver streams."""
        validated = tuple(validate_identifier(channel, "channel", allow_dots=False) for channel in channels)
        for channel in validated:
            self._subscribed_channels.pop(channel, None)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Yield driver notification frames or fail explicitly when streaming is unavailable."""
        listen_notifications = getattr(self._driver, "listen_notifications", None)
        if listen_notifications is None:
            msg = "Live PostgreSQL LISTEN streaming requires the native Rust PostgreSQL driver"
            raise NotSupportedError(msg)
        async for event in listen_notifications(tuple(self._subscribed_channels)):
            yield event
