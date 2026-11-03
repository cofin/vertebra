"""Shared multi-engine database and SHM pub/sub channel backend."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.cdc import CDCConfig, ChangeFeed, SpannerChangeStreamSource
from vertebra.db.oracle import AsyncOracleDriver, OracleConfig
from vertebra.db.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig
from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from vertebra.storage import ObjectStore

__all__ = ("DatabaseChannelBackend",)


class DatabaseChannelBackend:
    """Multi-engine pub/sub channel backend supporting SHM, Valkey, Redis, PostgreSQL LISTEN/NOTIFY, Oracle TxEventQ, and ChangeFeed."""

    __slots__ = (
        "_change_feed",
        "_driver",
        "_history",
        "_max_history",
        "_oracle_driver",
        "_oracle_queue_name",
        "_pg_driver",
        "_queues",
        "_reader_task",
        "_running",
        "_sqlspec_source",
        "_subscribed_channels",
        "_subscribers",
    )

    def __init__(
        self,
        backend: DatabaseChannelBackend | None = None,
        *,
        history: int = 0,
        driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        postgres_driver: AsyncPostgreSQLDriver | None = None,
        oracle_driver: AsyncOracleDriver | None = None,
        oracle_queue_name: str = "VERTEBRA_EVENTS",
        change_feed: ChangeFeed | None = None,
        sqlspec_source: Any = None,
    ) -> None:
        """Initialize the channel backend with optional history buffer size and database driver."""
        self._max_history: int = backend._max_history if backend is not None and history == 0 else max(0, history)
        self._driver: AsyncValkeyDriver | AsyncRedisDriver | None = (
            driver if driver is not None else (backend._driver if backend is not None else None)
        )
        self._pg_driver: AsyncPostgreSQLDriver | None = (
            postgres_driver if postgres_driver is not None else (backend._pg_driver if backend is not None else None)
        )
        self._oracle_driver: AsyncOracleDriver | None = (
            oracle_driver if oracle_driver is not None else (backend._oracle_driver if backend is not None else None)
        )
        self._oracle_queue_name: str = (
            oracle_queue_name if backend is None else (oracle_queue_name or backend._oracle_queue_name)
        )
        self._change_feed: ChangeFeed | None = (
            change_feed if change_feed is not None else (backend._change_feed if backend is not None else None)
        )
        self._sqlspec_source: Any = (
            sqlspec_source if sqlspec_source is not None else (backend._sqlspec_source if backend is not None else None)
        )
        self._history: dict[str, deque[bytes]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[tuple[str, bytes] | None]]] = {}
        self._subscribed_channels: set[str] = set()
        self._queues: set[asyncio.Queue[tuple[str, bytes] | None]] = set()
        self._reader_task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def backend(self) -> Self:
        """Return this channel backend instance."""
        return self

    @property
    def channels(self) -> tuple[str, ...]:
        """Return all tracked channel names."""
        return tuple(sorted(self._subscribed_channels | set(self._history.keys())))

    @property
    def driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

    @property
    def postgres_driver(self) -> AsyncPostgreSQLDriver | None:
        """Return the underlying direct-session PostgreSQL driver when configured."""
        return self._pg_driver

    @property
    def oracle_driver(self) -> AsyncOracleDriver | None:
        """Return the underlying Oracle driver when configured."""
        return self._oracle_driver

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the attached ChangeFeed when configured."""
        return self._change_feed

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str, *, history: int = 0) -> Self:
        """Create a channel backend backed by Valkey configuration or connection URL."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncValkeyDriver(cfg))

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str, *, history: int = 0) -> Self:
        """Create a channel backend backed by Redis configuration or connection URL."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncRedisDriver(cfg))

    @classmethod
    def from_postgres(
        cls,
        config_or_driver: PostgreSQLConfig | AsyncPostgreSQLDriver | str,
        *,
        history: int = 0,
        spill_store: ObjectStore | None = None,
        object_store: ObjectStore | None = None,
        spill_threshold_bytes: int = 7500,
    ) -> Self:
        """Create a channel backend backed by PostgreSQL LISTEN/NOTIFY with direct-DSN pooler bypass and ObjectStore spill."""
        if isinstance(config_or_driver, AsyncPostgreSQLDriver):
            base_cfg = config_or_driver.config
        elif isinstance(config_or_driver, str):
            base_cfg = PostgreSQLConfig(dsn=config_or_driver)
        else:
            base_cfg = config_or_driver

        direct_cfg = base_cfg.derive_direct_config()
        pg_driver = (
            config_or_driver
            if isinstance(config_or_driver, AsyncPostgreSQLDriver) and base_cfg.pooler_mode == "session"
            else AsyncPostgreSQLDriver(direct_cfg)
        )
        resolved_store = spill_store if spill_store is not None else object_store
        feed = ChangeFeed(
            CDCConfig(spill_threshold_bytes=spill_threshold_bytes, max_history_size=max(1024, history)),
            store=resolved_store,
        )
        return cls(history=history, postgres_driver=pg_driver, change_feed=feed)

    @classmethod
    def from_oracle(
        cls,
        config_or_driver: OracleConfig | AsyncOracleDriver,
        *,
        queue_name: str = "VERTEBRA_EVENTS",
        history: int = 0,
    ) -> Self:
        """Create a channel backend backed by Oracle Transactional Event Queues (TxEventQ / DBMS_AQ)."""
        ora_driver = (
            config_or_driver if isinstance(config_or_driver, AsyncOracleDriver) else AsyncOracleDriver(config_or_driver)
        )
        return cls(history=history, oracle_driver=ora_driver, oracle_queue_name=queue_name)

    @classmethod
    def from_changefeed(cls, feed: ChangeFeed, *, history: int = 0) -> Self:
        """Create a channel backend backed by a Vertebra ChangeFeed instance."""
        instance = cls(history=history, change_feed=feed)
        if hasattr(feed, "add_listener"):
            feed.add_listener(instance._on_changefeed_event)
        return instance

    @classmethod
    def from_spanner_change_stream(
        cls,
        driver: Any,
        stream_name: str,
        *,
        channel: str = "spanner_cdc",
        history: int = 0,
        spill_store: ObjectStore | None = None,
        object_store: ObjectStore | None = None,
        spill_threshold_bytes: int = 7500,
        heartbeat_milliseconds: int = 10_000,
    ) -> tuple[Self, SpannerChangeStreamSource]:
        """Create a channel backend and ``SpannerChangeStreamSource`` wired through a ``ChangeFeed``."""
        resolved_store = spill_store if spill_store is not None else object_store
        feed = ChangeFeed(
            CDCConfig(
                channel=channel,
                spill_threshold_bytes=spill_threshold_bytes,
                spill_prefix=f"cdc/spanner/{stream_name}",
                max_history_size=max(1024, history),
            ),
            store=resolved_store,
        )
        source = SpannerChangeStreamSource(
            driver,
            stream_name,
            change_feed=feed,
            store=resolved_store,
            channel=channel,
            heartbeat_milliseconds=heartbeat_milliseconds,
            spill_threshold_bytes=spill_threshold_bytes,
        )
        backend = cls.from_changefeed(feed, history=history)
        return backend, source

    @classmethod
    def from_sqlspec(cls, sqlspec_config_or_channel: Any, *, history: int = 0) -> Self:
        """Create a channel backend bridged to a SQLSpec configuration or EventChannel."""
        return cls(history=history, sqlspec_source=sqlspec_config_or_channel)

    async def on_startup(self) -> None:
        """Initialize the channels backend on application startup."""
        self._running = True

    async def on_shutdown(self) -> None:
        """Flush and clear active subscriptions on application shutdown."""
        self._running = False
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
            self._reader_task = None
        for queue in tuple(self._queues):
            queue.put_nowait(None)
        self._subscribers.clear()
        self._subscribed_channels.clear()

    async def publish(self, data: bytes | str, channels: Iterable[str]) -> None:
        """Publish data to all subscribers across the specified channels."""
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        for channel in channels:
            if self._driver is not None:
                await self._driver.publish(channel, payload)
            elif self._pg_driver is not None and self._change_feed is not None:
                wire_str, _ = self._change_feed.publish_event(
                    "PUBLISH",
                    {"channel": channel, "data_hex": payload.hex()},
                )
                with contextlib.suppress(Exception):
                    await self._pg_driver.execute_tuples("SELECT pg_notify($1, $2)", (channel, wire_str))
            elif self._change_feed is not None:
                self._change_feed.publish_event(
                    "PUBLISH",
                    {"channel": channel, "data_hex": payload.hex()},
                )
            elif self._oracle_driver is not None:
                envelope = encode_json_bytes({"channel": channel, "data_hex": payload.hex()})
                with contextlib.suppress(Exception):
                    await self._oracle_driver.execute(
                        "BEGIN DBMS_AQ.ENQUEUE(queue_name => :1, enqueue_options => DBMS_AQ.ENQUEUE_OPTIONS_T(), "
                        "message_properties => DBMS_AQ.MESSAGE_PROPERTIES_T(), payload => UTL_RAW.CAST_TO_RAW(:2), "
                        "msgid => :3); END;",
                        (self._oracle_queue_name, envelope.decode("latin-1"), None),
                    )
            elif self._sqlspec_source is not None:
                pub_fn = getattr(self._sqlspec_source, "publish", None)
                if callable(pub_fn):
                    res = pub_fn(channel, payload)
                    if asyncio.iscoroutine(res):
                        await res
            self._fan_in_local(channel, payload)

    async def subscribe(self, channels: Iterable[str]) -> None:
        """Ensure subscription topic slots exist for channels and attach active stream queues."""
        for channel in channels:
            self._subscribed_channels.add(channel)
            self._subscribers.setdefault(channel, set()).update(self._queues)
        if self._driver is not None and not self._driver.config.url.startswith("memory://") and self._running:
            if self._reader_task is not None:
                self._reader_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._reader_task
            self._reader_task = asyncio.create_task(self._fan_in_loop())

    async def unsubscribe(self, channels: Iterable[str]) -> None:
        """Remove subscription topic slots for channels and detach active stream queues."""
        for channel in channels:
            self._subscribed_channels.discard(channel)
            self._subscribers.pop(channel, None)

    async def stream_events(self) -> AsyncGenerator[tuple[str, bytes], None]:
        """Yield (channel, data) events as they are published to subscribed topics."""
        queue: asyncio.Queue[tuple[str, bytes] | None] = asyncio.Queue()
        self._queues.add(queue)
        for channel in tuple(self._subscribed_channels):
            self._subscribers.setdefault(channel, set()).add(queue)
        try:
            while self._running:
                item = await queue.get()
                if item is None:
                    break
                channel, payload = item
                if channel in self._subscribed_channels:
                    yield channel, payload
        finally:
            self._queues.discard(queue)
            for queues in self._subscribers.values():
                queues.discard(queue)

    async def get_history(self, channel: str, limit: int | None = None) -> list[bytes]:
        """Return buffered event history for channel up to limit entries."""
        ring = self._history.get(channel)
        if not ring:
            return []
        items = list(ring)
        if limit is not None and limit > 0:
            return items[-limit:]
        return items

    def ingest_pg_notification(self, channel: str, wire_envelope: str | bytes) -> bytes:
        """Hydrate a PostgreSQL LISTEN/NOTIFY wire envelope (including spilled payloads) and fan in locally."""
        try:
            if self._change_feed is not None:
                evt = self._change_feed.hydrate_notification(wire_envelope)
                data_hex = evt.payload.get("data_hex")
                payload = bytes.fromhex(data_hex) if isinstance(data_hex, str) else encode_json_bytes(evt.payload)
            else:
                decoded = decode_json(wire_envelope)
                data_hex = decoded.get("data_hex") if isinstance(decoded, dict) else None
                payload = (
                    bytes.fromhex(data_hex)
                    if isinstance(data_hex, str)
                    else (wire_envelope.encode("utf-8") if isinstance(wire_envelope, str) else bytes(wire_envelope))
                )
        except Exception:
            payload = wire_envelope.encode("utf-8") if isinstance(wire_envelope, str) else bytes(wire_envelope)
        self._fan_in_local(channel, payload)
        return payload

    def _on_changefeed_event(self, evt: Any) -> None:
        data_hex = evt.payload.get("data_hex") if isinstance(evt.payload, dict) else None
        if isinstance(data_hex, str):
            ch = str(evt.payload.get("channel") or evt.channel)
            self._fan_in_local(ch, bytes.fromhex(data_hex))
        else:
            self._fan_in_local(evt.channel, encode_json_bytes(evt))

    def _fan_in_local(self, channel: str, payload: bytes) -> None:
        if self._max_history > 0:
            ring = self._history.get(channel)
            if ring is None:
                ring = deque(maxlen=self._max_history)
                self._history[channel] = ring
            ring.append(payload)
        for queue in tuple(self._subscribers.get(channel, ())):
            queue.put_nowait((channel, payload))

    async def _fan_in_loop(self) -> None:
        """Consume remote Redis/Valkey pub/sub stream and fan in to local subscriber queues."""
        if self._driver is None or not self._subscribed_channels:
            return
        try:
            async for ch, payload in self._driver.subscribe(*self._subscribed_channels):
                if not self._running:
                    break
                self._fan_in_local(ch, payload)
        except (asyncio.CancelledError, Exception):
            pass
