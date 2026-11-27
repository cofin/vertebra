"""PostgreSQL notification transport, active subscription streaming, and message builders."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from collections.abc import AsyncIterator, Awaitable, Buffer, Callable, Iterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Final, Self

from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.driver import (
    AsyncNotificationSource,
    DatabaseError,
    InterfaceError,
    NotificationEvent,
    NotSupportedError,
    SyncNotificationSource,
)
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._postgres import AsyncPostgresListener, SyncPostgresListener
    from vertebra.db.adapters.postgres.events import (
        AsyncPostgresNotificationDriver,
        SyncPostgresNotificationDriver,
    )
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "AsyncPostgresNotificationSource",
    "PostgresNotifyConfig",
    "SyncPostgresNotificationSource",
)


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
        """Validate channel and replication SQL identifiers without I/O."""
        _validate_channel(self.channel)
        validate_identifier(self.spill_table, "spill_table", allow_dots=False)
        validate_identifier(self.replication_slot_name, "replication_slot_name", allow_dots=False)
        validate_identifier(self.publication_name, "publication_name", allow_dots=False)


class AsyncPostgresNotificationSource(AsyncNotificationSource):
    """Publish PostgreSQL notifications and stream live AsyncPostgresListener frames."""

    __slots__ = (
        "_change_feed",
        "_config",
        "_driver",
        "_generation",
        "_listener",
        "_listening",
        "_orphaned_listeners",
        "_pending_closes",
        "_queue_capacity",
        "_reconnect",
        "_subscribed_channels",
        "_subscription_lock",
    )

    def __init__(
        self,
        driver: AsyncPostgresNotificationDriver,
        config: PostgresNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> None:
        """Initialize a protocol-typed PostgreSQL notification source without auto-subscribing."""
        if isinstance(queue_capacity, bool) or not isinstance(queue_capacity, int):
            msg = "queue_capacity must be an int, not bool"
            raise TypeError(msg)
        if queue_capacity < 1:
            msg = f"queue_capacity must be >= 1 (got {queue_capacity})"
            raise ValueError(msg)
        if not isinstance(reconnect, bool):
            msg = "reconnect must be a bool"
            raise TypeError(msg)
        self._driver = driver
        self._config = config or PostgresNotifyConfig()
        self._change_feed = change_feed
        self._queue_capacity = queue_capacity
        self._reconnect = reconnect
        self._subscribed_channels: dict[str, None] = {}
        self._subscription_lock = asyncio.Lock()
        self._listener: AsyncPostgresListener | None = None
        self._listening = False
        self._generation = 0
        self._pending_closes: set[asyncio.Task[None]] = set()
        self._orphaned_listeners: list[AsyncPostgresListener] = []

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional feed used to wrap and hydrate notification payloads."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return an ordered snapshot of explicitly subscribed PostgreSQL channel names."""
        return list(self._subscribed_channels)

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
        """Build a notification statement with a validated 1..63-byte channel and payload."""
        validated = _validate_channel(channel)
        return "SELECT pg_notify($1, $2)", (validated, payload)

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a raw, base64-prefixed binary, or ChangeFeed payload through the driver."""
        validated = _validate_channel(channel)
        if not isinstance(payload, (str, Buffer)):
            msg = "payload must be str or bytes-like"
            raise TypeError(msg)
        if self._change_feed is not None:
            raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
            wire_payload, _ = self._change_feed.publish_event(
                "PUBLISH",
                {"channel": validated, "data_b64": base64.b64encode(raw_bytes).decode("ascii")},
            )
        else:
            wire_payload = (
                payload if isinstance(payload, str) else "b64:" + base64.b64encode(bytes(payload)).decode("ascii")
            )
        await self._driver._publish_notification(validated, wire_payload)

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Atomically validate channels and synchronize both desired and active LISTEN state."""
        validated = self._validate_channel_sequence(channels)
        if not validated:
            return
        async with self._subscription_lock:
            self._subscribed_channels.update(dict.fromkeys(validated))
            listener = self._listener
            if listener is not None and not listener.closed:
                await listener.subscribe(validated)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        """Atomically validate channels and synchronize both desired and active UNLISTEN state."""
        validated = self._validate_channel_sequence(channels)
        if not validated:
            return
        async with self._subscription_lock:
            for channel in validated:
                self._subscribed_channels.pop(channel, None)
            listener = self._listener
            if listener is not None and not listener.closed:
                await listener.unsubscribe(validated)

    def listen(self) -> AsyncIterator[NotificationEvent]:
        """Return a single-consumer async iterator streaming hydrated NotificationEvent frames."""
        return _PostgresNotificationStream(self)

    async def aclose(self) -> None:
        """Idempotently close any active or detached notification listener handle."""
        self._generation += 1
        self._listening = False
        listener = self._listener
        self._listener = None
        if listener is not None:
            await listener.aclose()
        await self._drain_pending_closes()

    async def __aenter__(self) -> Self:
        """Enter an async context returning this AsyncPostgresNotificationSource."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """Close any active notification listener on context exit without suppressing errors."""
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()

    @staticmethod
    def _validate_channel_sequence(channels: Sequence[str]) -> tuple[str, ...]:
        """Validate a non-string sequence of channels atomically and deduplicate in order."""
        if isinstance(channels, (str, bytes, bytearray, memoryview)):
            msg = "channels must be a sequence of channel strings, not str or bytes"
            raise TypeError(msg)
        validated = tuple(_validate_channel(channel) for channel in channels)
        return tuple(dict.fromkeys(validated))

    def _validate_direct_session_config(self) -> None:
        """Reject non-direct/session pooler modes lacking direct_dsn before listener dial."""
        cfg = getattr(self._driver, "config", None)
        if not isinstance(cfg, PostgreSQLConfig):
            return
        direct_cfg = cfg.derive_direct_config()
        if direct_cfg.pooler_mode not in ("direct", "session") or direct_cfg.pgbouncer:
            msg = (
                "PostgreSQL LISTEN/NOTIFY requires a direct or session-mode connection "
                f"(got pooler_mode={direct_cfg.pooler_mode!r}); configure direct_dsn for pooled deployments"
            )
            raise ImproperConfigurationError(msg)

    def _resolve_open_listener(
        self,
    ) -> Callable[..., Awaitable[AsyncPostgresListener]]:
        """Return the driver's async listener opener or raise NotSupportedError."""
        open_listener: Callable[..., Awaitable[AsyncPostgresListener]] | None = getattr(
            self._driver, "_open_notification_listener", None
        )
        if not callable(open_listener):
            msg = "Live PostgreSQL LISTEN streaming requires the native Rust PostgreSQL driver"
            raise NotSupportedError(msg)
        return open_listener

    async def _reconcile_initial_channels(
        self,
        listener: AsyncPostgresListener,
        initial_channels: tuple[str, ...],
        generation: int,
    ) -> None:
        """Converge subscribe or unsubscribe deltas raced across open_listener before activation."""
        async with self._subscription_lock:
            if self._generation != generation or listener.closed:
                return
            self._listener = listener
            desired = tuple(self._subscribed_channels)
            initial_set = set(initial_channels)
            desired_set = set(desired)
            if desired_set == initial_set:
                return
            added = tuple(ch for ch in desired if ch not in initial_set)
            removed = tuple(ch for ch in initial_channels if ch not in desired_set)
            try:
                if added and self._generation == generation and not listener.closed:
                    await listener.subscribe(added)
                if removed and self._generation == generation and not listener.closed:
                    await listener.unsubscribe(removed)
            except (InterfaceError, DatabaseError):
                if self._generation != generation:
                    return
                raise

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        text = raw_payload.decode("utf-8")
        if self._change_feed is not None:
            evt = self._change_feed.hydrate_notification(text)
            return base64.b64decode(evt.payload["data_b64"])
        if text.startswith("b64:"):
            return base64.b64decode(text[4:])
        return bytes(raw_payload)

    def _schedule_detached_close(self, listener: AsyncPostgresListener) -> None:
        """Schedule asynchronous listener teardown when an async-for loop exits via break or raise."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._orphaned_listeners.append(listener)
            return
        task = loop.create_task(self._aclose_listener(listener))
        self._pending_closes.add(task)
        task.add_done_callback(self._pending_closes.discard)

    @staticmethod
    async def _aclose_listener(listener: AsyncPostgresListener) -> None:
        """Close a detached AsyncPostgresListener suppressing teardown transport errors."""
        with contextlib.suppress(Exception):
            await listener.aclose()

    async def _drain_pending_closes(self) -> None:
        """Await all detached listener close tasks and orphaned handles before reuse or shutdown."""
        if self._orphaned_listeners:
            orphans = list(self._orphaned_listeners)
            self._orphaned_listeners.clear()
            for orphan in orphans:
                with contextlib.suppress(Exception):
                    await orphan.aclose()
        if self._pending_closes:
            pending = tuple(self._pending_closes)
            await asyncio.gather(*pending, return_exceptions=True)


_MAX_CHANNEL_BYTES: Final[int] = 63


class _PostgresNotificationStream(AsyncIterator[NotificationEvent]):
    """Generation-fenced async notification iterator closing its native listener on exit."""

    __slots__ = ("_closed", "_generation", "_listener", "_source", "_started")

    def __init__(self, source: AsyncPostgresNotificationSource) -> None:
        """Bind a fresh unstarted stream handle to source without holding a reverse reference."""
        self._source = source
        self._listener: AsyncPostgresListener | None = None
        self._generation = 0
        self._started = False
        self._closed = False

    def __aiter__(self) -> Self:
        """Return self as an AsyncIterator of NotificationEvent items."""
        return self

    async def __anext__(self) -> NotificationEvent:
        """Open the dedicated listener on first pull and yield the next NotificationEvent."""
        if self._closed:
            raise StopAsyncIteration
        if not self._started:
            if self._source._listening:
                self._closed = True
                msg = "AsyncPostgresNotificationSource.listen() is already active on this source"
                raise InterfaceError(msg)
            self._source._validate_direct_session_config()
            open_listener = self._source._resolve_open_listener()
            self._source._generation += 1
            self._generation = self._source._generation
            self._started = True
            self._source._listening = True
            try:
                await self._source._drain_pending_closes()
                async with self._source._subscription_lock:
                    if self._source._generation != self._generation:
                        self._closed = True
                        raise StopAsyncIteration
                    initial_channels = tuple(self._source._subscribed_channels)
                listener = await open_listener(
                    initial_channels,
                    queue_capacity=self._source._queue_capacity,
                    reconnect=self._source._reconnect,
                )
                if self._source._generation != self._generation:
                    self._closed = True
                    await listener.aclose()
                    raise StopAsyncIteration
                self._listener = listener
                await self._source._reconcile_initial_channels(listener, initial_channels, self._generation)
                if self._source._generation != self._generation:
                    await self.aclose()
                    raise StopAsyncIteration
            except StopAsyncIteration:
                raise
            except BaseException:
                await self.aclose()
                raise

        active = self._listener
        if active is None:
            await self.aclose()
            raise StopAsyncIteration
        try:
            frame = await active.next()
            if frame is None:
                await self.aclose()
                raise StopAsyncIteration
            pid, channel, raw_payload, reconnected_gap = frame
            hydrated = self._source._hydrate_payload(raw_payload)
            return NotificationEvent(
                channel=channel,
                payload=hydrated,
                sender_id=str(pid),
                metadata={"reconnected": bool(reconnected_gap), "delivery": "at_most_once"},
            )
        except StopAsyncIteration:
            raise
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        """Idempotently close the underlying native listener and release source ownership."""
        listener = self._detach_listener()
        if listener is not None:
            await listener.aclose()

    def __del__(self) -> None:
        """Release source ownership and schedule listener teardown on broken async-for loops."""
        listener = self._detach_listener()
        if listener is not None:
            self._source._schedule_detached_close(listener)

    def _detach_listener(self) -> AsyncPostgresListener | None:
        """Mark the stream closed, release active generation state, and return the listener."""
        if self._closed:
            return None
        self._closed = True
        listener = self._listener
        self._listener = None
        if self._started:
            self._started = False
            if self._source._generation == self._generation:
                self._source._generation += 1
                self._source._listener = None
                self._source._listening = False
        return listener


class SyncPostgresNotificationSource(SyncNotificationSource):
    """Publish PostgreSQL notifications and stream live SyncPostgresListener frames."""

    __slots__ = (
        "_change_feed",
        "_config",
        "_driver",
        "_listener",
        "_listening",
        "_queue_capacity",
        "_reconnect",
        "_subscribed_channels",
    )

    def __init__(
        self,
        driver: SyncPostgresNotificationDriver,
        config: PostgresNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> None:
        """Initialize a synchronous protocol-typed PostgreSQL notification source."""
        if isinstance(queue_capacity, bool) or not isinstance(queue_capacity, int):
            msg = "queue_capacity must be an int, not bool"
            raise TypeError(msg)
        if queue_capacity < 1:
            msg = f"queue_capacity must be >= 1 (got {queue_capacity})"
            raise ValueError(msg)
        if not isinstance(reconnect, bool):
            msg = "reconnect must be a bool"
            raise TypeError(msg)
        self._driver = driver
        self._config = config or PostgresNotifyConfig()
        self._change_feed = change_feed
        self._queue_capacity = queue_capacity
        self._reconnect = reconnect
        self._subscribed_channels: dict[str, None] = {}
        self._listener: SyncPostgresListener | None = None
        self._listening = False

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional feed used to wrap and hydrate notification payloads."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return an ordered snapshot of explicitly subscribed PostgreSQL channel names."""
        return list(self._subscribed_channels)

    @staticmethod
    def build_emit_message_sql(
        prefix: str,
        payload: bytes | str,
        *,
        transactional: bool = True,
    ) -> tuple[str, tuple[bool, str, bytes]]:
        """Build a logical replication message statement with bound payload bytes."""
        return AsyncPostgresNotificationSource.build_emit_message_sql(prefix, payload, transactional=transactional)

    @staticmethod
    def build_notify_sql(channel: str, payload: str) -> tuple[str, tuple[str, str]]:
        """Build a notification statement with a validated 1..63-byte channel and payload."""
        return AsyncPostgresNotificationSource.build_notify_sql(channel, payload)

    def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a raw, base64-prefixed binary, or ChangeFeed payload through the driver."""
        validated = _validate_channel(channel)
        if not isinstance(payload, (str, Buffer)):
            msg = "payload must be str or bytes-like"
            raise TypeError(msg)
        if self._change_feed is not None:
            raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
            wire_payload, _ = self._change_feed.publish_event(
                "PUBLISH",
                {"channel": validated, "data_b64": base64.b64encode(raw_bytes).decode("ascii")},
            )
        else:
            wire_payload = (
                payload if isinstance(payload, str) else "b64:" + base64.b64encode(bytes(payload)).decode("ascii")
            )
        self._driver._publish_notification_sync(validated, wire_payload)

    def subscribe(self, channels: Sequence[str]) -> None:
        """Atomically validate channels and synchronize both desired and active LISTEN state."""
        validated = AsyncPostgresNotificationSource._validate_channel_sequence(channels)
        if not validated:
            return
        self._subscribed_channels.update(dict.fromkeys(validated))
        listener = self._listener
        if listener is not None and not listener.closed:
            listener.subscribe(validated)

    def unsubscribe(self, channels: Sequence[str]) -> None:
        """Atomically validate channels and synchronize both desired and active UNLISTEN state."""
        validated = AsyncPostgresNotificationSource._validate_channel_sequence(channels)
        if not validated:
            return
        for channel in validated:
            self._subscribed_channels.pop(channel, None)
        listener = self._listener
        if listener is not None and not listener.closed:
            listener.unsubscribe(validated)

    def listen(self) -> Iterator[NotificationEvent]:
        """Return a single-consumer sync iterator streaming hydrated NotificationEvent frames."""
        return _SyncPostgresNotificationStream(self)

    def close(self) -> None:
        """Idempotently close any active notification listener handle."""
        self._listening = False
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()

    def __enter__(self) -> Self:
        """Enter a sync context returning this SyncPostgresNotificationSource."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """Close any active notification listener on context exit without suppressing errors."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def _validate_direct_session_config(self) -> None:
        """Reject non-direct/session pooler modes lacking direct_dsn before listener dial."""
        cfg = getattr(self._driver, "config", None)
        if not isinstance(cfg, PostgreSQLConfig):
            return
        direct_cfg = cfg.derive_direct_config()
        if direct_cfg.pooler_mode not in ("direct", "session") or direct_cfg.pgbouncer:
            msg = (
                "PostgreSQL LISTEN/NOTIFY requires a direct or session-mode connection "
                f"(got pooler_mode={direct_cfg.pooler_mode!r}); configure direct_dsn for pooled deployments"
            )
            raise ImproperConfigurationError(msg)

    def _resolve_open_listener(
        self,
    ) -> Callable[..., SyncPostgresListener]:
        """Return the driver's sync listener opener or raise NotSupportedError."""
        open_listener: Callable[..., SyncPostgresListener] | None = getattr(
            self._driver, "_open_sync_notification_listener", None
        )
        if not callable(open_listener):
            msg = "Live PostgreSQL LISTEN streaming requires the native Rust PostgreSQL driver"
            raise NotSupportedError(msg)
        return open_listener

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        text = raw_payload.decode("utf-8")
        if self._change_feed is not None:
            evt = self._change_feed.hydrate_notification(text)
            return base64.b64decode(evt.payload["data_b64"])
        if text.startswith("b64:"):
            return base64.b64decode(text[4:])
        return bytes(raw_payload)


class _SyncPostgresNotificationStream(Iterator[NotificationEvent]):
    """Synchronous notification iterator closing its native listener on exit."""

    __slots__ = ("_closed", "_listener", "_source", "_started")

    def __init__(self, source: SyncPostgresNotificationSource) -> None:
        """Bind a fresh unstarted stream handle to source."""
        self._source = source
        self._listener: SyncPostgresListener | None = None
        self._started = False
        self._closed = False

    def __iter__(self) -> Self:
        """Return self as an Iterator of NotificationEvent items."""
        return self

    def __next__(self) -> NotificationEvent:
        """Open the dedicated listener on first pull and yield the next NotificationEvent."""
        if self._closed:
            raise StopIteration
        if not self._started:
            if self._source._listening:
                self._closed = True
                msg = "SyncPostgresNotificationSource.listen() is already active on this source"
                raise InterfaceError(msg)
            self._source._validate_direct_session_config()
            open_listener = self._source._resolve_open_listener()
            self._started = True
            self._source._listening = True
            try:
                initial_channels = tuple(self._source._subscribed_channels)
                listener = open_listener(
                    initial_channels,
                    queue_capacity=self._source._queue_capacity,
                    reconnect=self._source._reconnect,
                )
                self._listener = listener
                self._source._listener = listener
            except StopIteration:
                raise
            except BaseException:
                self.close()
                raise

        active = self._listener
        if active is None:
            self.close()
            raise StopIteration
        try:
            frame = active.next()
            if frame is None:
                self.close()
                raise StopIteration
            pid, channel, raw_payload, reconnected_gap = frame
            hydrated = self._source._hydrate_payload(raw_payload)
            return NotificationEvent(
                channel=channel,
                payload=hydrated,
                sender_id=str(pid),
                metadata={"reconnected": bool(reconnected_gap), "delivery": "at_most_once"},
            )
        except StopIteration:
            raise
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Idempotently close the underlying native listener and release source ownership."""
        if self._closed:
            return
        self._closed = True
        listener = self._listener
        self._listener = None
        if self._started:
            self._started = False
            self._source._listener = None
            self._source._listening = False
        if listener is not None:
            listener.close()

    def __del__(self) -> None:
        """Release source ownership and close listener when garbage collected."""
        self.close()


def _validate_channel(channel: str) -> str:
    """Validate channel identifier syntax and PostgreSQL 1..63 UTF-8 byte length."""
    validated = validate_identifier(channel, "channel", allow_dots=False)
    encoded_len = len(validated.encode("utf-8"))
    if encoded_len < 1 or encoded_len > _MAX_CHANNEL_BYTES:
        msg = f"Invalid channel {channel!r}: PostgreSQL channel names must be 1..63 UTF-8 bytes (got {encoded_len})"
        raise ValueError(msg)
    return validated
