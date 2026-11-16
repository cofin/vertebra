"""Change data capture (CDC) event backends for SQLSpec integration."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import hashlib
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final
from uuid import uuid4

from vertebra.db.cdc import CDCConfig, ChangeEvent, ChangeFeed
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.typing import ImproperConfigurationError, MissingDependencyError

try:
    from sqlspec.extensions.events import (
        MAX_NOTIFY_BYTES,
        AsyncEventBackendProtocol,
        AsyncTableEventQueue,
        BaseEventQueueStore,
        EventMessage,
        SyncEventBackendProtocol,
        SyncTableEventQueue,
        decode_notify_payload,
        encode_notify_payload,
        fits_notify_payload,
        measure_notify_payload,
    )
    from sqlspec.utils.serializers import from_json, to_json
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.driver.capabilities import AsyncNotificationSource, SyncNotificationSource

__all__ = (
    "ChangeFeedAsyncEventBackend",
    "ChangeFeedSyncEventBackend",
)

_CHECKPOINT_STATUS: Final[str] = "checkpoint"
_CHECKPOINT_CHANNEL_PREFIX: Final[str] = "__vtb_checkpoint__:"
_CHECKPOINT_EVENT_PREFIX: Final[str] = "ckpt:"


def _checkpoint_event_id(checkpoint_key: str, channel: str) -> str:
    """Return a bounded primary-key identifier for a durable stream checkpoint."""
    digest = hashlib.sha256(f"{checkpoint_key}:{channel}".encode()).hexdigest()[:48]
    return f"{_CHECKPOINT_EVENT_PREFIX}{digest}"


def _checkpoint_channel(channel: str) -> str:
    """Return the internal queue channel marker isolating checkpoint rows."""
    return f"{_CHECKPOINT_CHANNEL_PREFIX}{channel}"[:128]


def _extract_row_field(row: Any, key: str, index: int = 0) -> Any:
    """Extract a scalar column value from a mapping or sequence driver row."""
    if isinstance(row, dict):
        return row.get(key)
    if isinstance(row, (list, tuple)) and len(row) > index:
        return row[index]
    return getattr(row, key, None)


def _exceeds_notify_ceiling(
    event_id: str,
    payload: dict[str, Any],
    metadata: dict[str, Any] | None,
    ceiling_bytes: int,
) -> bool:
    """Return True when the serialized NOTIFY envelope exceeds ``ceiling_bytes``."""
    measured = measure_notify_payload(payload, metadata, event_id=event_id)
    if measured > ceiling_bytes:
        return True
    if ceiling_bytes == MAX_NOTIFY_BYTES:
        return not fits_notify_payload(payload, metadata, event_id=event_id)
    return False


def _decode_wakeup_event_id(raw_payload: bytes | str) -> str | None:
    """Extract the spilled EventQueueStore event_id when a notify frame is a wakeup marker."""
    text = raw_payload.decode("utf-8") if isinstance(raw_payload, (bytes, bytearray, memoryview)) else raw_payload
    try:
        decoded = from_json(text)
    except Exception:
        return None
    if isinstance(decoded, dict) and decoded.get("spilled_to_queue") is True:
        event_id = decoded.get("event_id")
        if event_id is not None:
            return str(event_id)
    return None


def _encode_notify_frame(event_id: str, payload: dict[str, Any], metadata: dict[str, Any] | None) -> str:
    """Encode an inline notification envelope for NotificationSource delivery."""
    return encode_notify_payload(event_id, payload, metadata)


def _encode_wakeup_frame(event_id: str, channel: str) -> str:
    """Encode a compact wakeup envelope referencing a spilled EventQueueStore row."""
    return to_json(
        {
            "event_id": event_id,
            "channel": channel,
            "wakeup": True,
            "spilled_to_queue": True,
        }
    )


def _decode_inline_notify(channel: str, raw_payload: bytes | str) -> EventMessage:
    """Decode an inline NotificationSource payload into an EventMessage."""
    text = raw_payload.decode("utf-8") if isinstance(raw_payload, (bytes, bytearray, memoryview)) else raw_payload
    return decode_notify_payload(channel, text)


class ChangeFeedSyncEventBackend(SyncEventBackendProtocol):
    """Synchronous SQLSpec event backend wrapping ``vertebra.db.cdc.ChangeFeed``.

    Supports optional durable checkpoint persistence via a configured
    ``BaseEventQueueStore`` and transparent ``EventQueueStore`` table spilling
    for notification payloads exceeding ``notify_ceiling_bytes`` (default ``7999``).
    """

    supports_sync: ClassVar[bool] = True
    backend_name: ClassVar[str] = "changefeed"
    __slots__ = (
        "_checkpoint_key",
        "_checkpoints",
        "_cond",
        "_event_channels",
        "_feed",
        "_notify_buffer",
        "_notify_ceiling_bytes",
        "_notify_done",
        "_notify_error",
        "_notify_source",
        "_notify_stop",
        "_notify_thread",
        "_queue_leased",
        "_queue_store",
        "_queues",
        "_schema_ready",
        "_store",
        "_subscribed_channels",
        "_table_queue",
        "_unacked",
    )

    def __init__(
        self,
        feed: ChangeFeed | None = None,
        *,
        config: CDCConfig | None = None,
        store: SyncObjectStore | None = None,
        queue_store: BaseEventQueueStore[Any] | None = None,
        notify_source: SyncNotificationSource | None = None,
        checkpoint_key: str = "default",
        notify_ceiling_bytes: int = MAX_NOTIFY_BYTES,
    ) -> None:
        """Initialize backend with optional ChangeFeed, ObjectStore, queue store, and notify source."""
        if notify_ceiling_bytes < 1:
            msg = "notify_ceiling_bytes must be positive"
            raise ValueError(msg)
        self._store = store
        self._feed = feed or ChangeFeed(config=config, store=store)
        self._queue_store = queue_store
        self._notify_source = notify_source
        self._checkpoint_key = checkpoint_key
        self._notify_ceiling_bytes = notify_ceiling_bytes
        self._table_queue: SyncTableEventQueue | None = (
            SyncTableEventQueue(queue_store._config) if queue_store is not None else None
        )
        self._schema_ready = False
        self._cond = threading.Condition()
        self._queues: dict[str, collections.deque[str]] = {}
        self._unacked: dict[str, tuple[str, str]] = {}
        self._queue_leased: set[str] = set()
        self._event_channels: dict[str, str] = {}
        self._checkpoints: dict[str, str] = {}
        self._subscribed_channels: set[str] = set()
        self._notify_buffer: dict[str, collections.deque[bytes | str]] = {}
        self._notify_thread: threading.Thread | None = None
        self._notify_done = False
        self._notify_stop = False
        self._notify_error: BaseException | None = None

    @property
    def feed(self) -> ChangeFeed:
        """Return the underlying ChangeFeed instance."""
        return self._feed

    def _ensure_queue_schema(self) -> None:
        """Create the backing EventQueueStore table idempotently if configured."""
        if self._schema_ready or self._queue_store is None:
            return
        with self._queue_store._config.provide_session() as driver:
            for statement in self._queue_store.create_statements():
                driver.execute(statement)
        self._schema_ready = True

    def save_checkpoint(self, channel: str, resume_token: str | int) -> str:
        """Persist a resume token for ``channel`` via the configured BaseEventQueueStore."""
        token = str(resume_token)
        self._checkpoints[channel] = token
        if self._queue_store is None:
            return token
        custom_saver = getattr(self._queue_store, "save_checkpoint", None)
        if callable(custom_saver):
            custom_saver(channel, token, checkpoint_key=self._checkpoint_key)
            return token
        self._ensure_queue_schema()
        now = datetime.now(UTC)
        event_id = _checkpoint_event_id(self._checkpoint_key, channel)
        payload_json = to_json(
            {
                "checkpoint_key": self._checkpoint_key,
                "channel": channel,
                "resume_token": token,
                "updated_at": now.isoformat(),
            }
        )
        table = self._queue_store.table_name
        with self._queue_store._config.provide_session() as driver:
            driver.execute(
                f"DELETE FROM {table} WHERE event_id = :event_id",
                {"event_id": event_id},
            )
            driver.execute(
                f"INSERT INTO {table} "
                "(event_id, channel, payload_json, metadata_json, status, "
                "available_at, lease_expires_at, attempts, created_at) "
                "VALUES (:event_id, :channel, :payload_json, :metadata_json, :status, "
                ":available_at, :lease_expires_at, :attempts, :created_at)",
                {
                    "event_id": event_id,
                    "channel": _checkpoint_channel(channel),
                    "payload_json": payload_json,
                    "metadata_json": None,
                    "status": _CHECKPOINT_STATUS,
                    "available_at": now,
                    "lease_expires_at": None,
                    "attempts": 0,
                    "created_at": now,
                },
            )
            driver.commit()
        return token

    def load_checkpoint(self, channel: str) -> str | None:
        """Load the latest persisted resume token for ``channel``."""
        if self._queue_store is None:
            return self._checkpoints.get(channel)
        custom_loader = getattr(self._queue_store, "load_checkpoint", None)
        if callable(custom_loader):
            loaded = custom_loader(channel, checkpoint_key=self._checkpoint_key)
            if loaded is not None:
                token = str(loaded)
                self._checkpoints[channel] = token
                return token
            return None
        self._ensure_queue_schema()
        event_id = _checkpoint_event_id(self._checkpoint_key, channel)
        table = self._queue_store.table_name
        with self._queue_store._config.provide_session() as driver:
            rows = driver.select(
                f"SELECT payload_json FROM {table} WHERE event_id = :event_id",
                {"event_id": event_id},
            )
        if not rows:
            return None
        raw = _extract_row_field(rows[0], "payload_json", 0)
        if raw is None:
            return None
        decoded = (
            from_json(bytes(raw) if isinstance(raw, bytearray) else raw)
            if isinstance(raw, (str, bytes, bytearray))
            else raw
        )
        if isinstance(decoded, dict) and decoded.get("resume_token") is not None:
            token = str(decoded["resume_token"])
            self._checkpoints[channel] = token
            return token
        return None

    def publish(
        self,
        channel: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Publish an event to ``channel``, spilling oversized notify payloads to EventQueueStore."""
        if self._notify_source is not None:
            candidate_id = uuid4().hex
            if _exceeds_notify_ceiling(candidate_id, payload, metadata, self._notify_ceiling_bytes):
                if self._table_queue is None:
                    msg = (
                        "Notification payload exceeds notify_ceiling_bytes and requires "
                        "a configured BaseEventQueueStore for durable spill"
                    )
                    raise ImproperConfigurationError(msg)
                self._ensure_queue_schema()
                spilled_id = self._table_queue.publish(channel, payload, metadata)
                self._notify_source.publish(channel, _encode_wakeup_frame(spilled_id, channel))
                self._event_channels[spilled_id] = channel
                return spilled_id
            frame = _encode_notify_frame(candidate_id, payload, metadata)
            self._notify_source.publish(channel, frame)
            self._event_channels[candidate_id] = channel
            return candidate_id

        if self._table_queue is not None and self._store is None:
            self._ensure_queue_schema()
            event_id = self._table_queue.publish(channel, payload, metadata)
            self._event_channels[event_id] = channel
            return event_id

        _ = metadata
        wire_envelope, event = self._feed.publish_event(operation="publish", payload=payload)
        event_id_str = str(event.event_id)
        with self._cond:
            if channel not in self._queues:
                self._queues[channel] = collections.deque()
            self._queues[channel].append(wire_envelope)
            self._event_channels[event_id_str] = channel
            self._cond.notify_all()
        return event_id_str

    def publish_many(
        self,
        events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]],
    ) -> list[str]:
        """Publish multiple events sequentially and return their generated event IDs."""
        return [self.publish(ch, p, meta) for ch, p, meta in events]

    def _pump_notify_sync(self, source: SyncNotificationSource) -> None:
        try:
            for frame in source.listen():
                if self._notify_stop:
                    break
                with self._cond:
                    self._notify_buffer.setdefault(frame.channel, collections.deque()).append(frame.payload)
                    self._cond.notify_all()
        except BaseException as exc:
            with self._cond:
                self._notify_error = exc
                self._cond.notify_all()
        finally:
            with self._cond:
                self._notify_done = True
                self._cond.notify_all()

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue the next event for ``channel``, hydrating durable or ObjectStore spills."""
        if self._notify_source is not None:
            if channel not in self._subscribed_channels:
                self._notify_source.subscribe([channel])
                self._subscribed_channels.add(channel)
            raw_payload: bytes | str | None = None
            with self._cond:
                buffered = self._notify_buffer.get(channel)
                if buffered:
                    raw_payload = buffered.popleft()
                else:
                    if self._notify_thread is None or not self._notify_thread.is_alive():
                        self._notify_done = False
                        self._notify_stop = False
                        self._notify_error = None
                        self._notify_thread = threading.Thread(
                            target=self._pump_notify_sync,
                            args=(self._notify_source,),
                            daemon=True,
                        )
                        self._notify_thread.start()
                    if poll_interval <= 0:
                        if not self._notify_buffer.get(channel) and not self._notify_done:
                            self._cond.wait(timeout=0.01)
                    else:
                        deadline = time.monotonic() + float(poll_interval)
                        while not self._notify_buffer.get(channel) and not self._notify_done and not self._notify_stop:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                break
                            self._cond.wait(timeout=remaining)
                    if self._notify_error is not None:
                        err = self._notify_error
                        self._notify_error = None
                        raise err
                    buffered = self._notify_buffer.get(channel)
                    if buffered:
                        raw_payload = buffered.popleft()
            if raw_payload is None:
                return None
            spilled_id = _decode_wakeup_event_id(raw_payload)
            if spilled_id is not None:
                if self._table_queue is None:
                    msg = "Received spilled wakeup frame without a configured BaseEventQueueStore"
                    raise ImproperConfigurationError(msg)
                self._ensure_queue_schema()
                message = self._table_queue.dequeue_by_event_id(spilled_id)
                if message is not None:
                    self._queue_leased.add(message.event_id)
                    self._event_channels[message.event_id] = channel
                return message
            message = _decode_inline_notify(channel, raw_payload)
            self._event_channels[message.event_id] = channel
            return message

        if self._table_queue is not None and self._store is None:
            self._ensure_queue_schema()
            message = self._table_queue.dequeue(channel, poll_interval)
            if message is not None:
                self._queue_leased.add(message.event_id)
                self._event_channels[message.event_id] = channel
            return message

        with self._cond:
            q = self._queues.get(channel)
            if not q and poll_interval > 0:
                deadline = time.monotonic() + float(poll_interval)
                while not self._queues.get(channel):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._cond.wait(timeout=remaining)
            q = self._queues.get(channel)
            if not q:
                return None
            wire_envelope = q.popleft()
        change_event: ChangeEvent = self._feed.hydrate_notification(wire_envelope)
        event_id_str = str(change_event.event_id)
        with self._cond:
            self._unacked[event_id_str] = (channel, wire_envelope)
            self._event_channels[event_id_str] = channel
        now = datetime.now(UTC)
        return EventMessage(
            event_id=event_id_str,
            channel=channel,
            payload=change_event.payload,
            metadata=None,
            attempts=1,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )

    def ack(self, event_id: str) -> None:
        """Acknowledge successful event processing and persist checkpoint if configured."""
        if event_id in self._queue_leased and self._table_queue is not None:
            self._table_queue.ack(event_id)
            self._queue_leased.discard(event_id)
        else:
            with self._cond:
                self._unacked.pop(event_id, None)
        channel = self._event_channels.pop(event_id, None)
        if channel is not None and self._queue_store is not None:
            self.save_checkpoint(channel, event_id)

    def nack(self, event_id: str) -> None:
        """Return an unacknowledged event to the queue for redelivery."""
        if event_id in self._queue_leased and self._table_queue is not None:
            self._table_queue.nack(event_id)
            self._queue_leased.discard(event_id)
            return
        with self._cond:
            entry = self._unacked.pop(event_id, None)
            if entry is not None:
                channel, wire_envelope = entry
                if channel not in self._queues:
                    self._queues[channel] = collections.deque()
                self._queues[channel].appendleft(wire_envelope)
                self._cond.notify_all()

    def shutdown(self) -> None:
        """Release transient buffers while leaving durable checkpoints and queue rows intact."""
        with self._cond:
            self._notify_stop = True
            self._cond.notify_all()
        if self._notify_source is not None and self._subscribed_channels:
            self._notify_source.unsubscribe(sorted(self._subscribed_channels))
        if self._notify_thread is not None and self._notify_thread.is_alive():
            self._notify_thread.join(timeout=0.2)
        with self._cond:
            self._notify_thread = None
            self._queues.clear()
            self._unacked.clear()
            self._queue_leased.clear()
            self._event_channels.clear()
            self._notify_buffer.clear()
            self._subscribed_channels.clear()


class ChangeFeedAsyncEventBackend(AsyncEventBackendProtocol):
    """Asynchronous SQLSpec event backend wrapping ``vertebra.db.cdc.ChangeFeed``.

    Supports optional durable checkpoint persistence via a configured
    ``BaseEventQueueStore`` and transparent ``EventQueueStore`` table spilling
    for notification payloads exceeding ``notify_ceiling_bytes`` (default ``7999``).
    """

    supports_async: ClassVar[bool] = True
    backend_name: ClassVar[str] = "changefeed"
    __slots__ = (
        "_checkpoint_key",
        "_checkpoints",
        "_event_channels",
        "_feed",
        "_notify_ceiling_bytes",
        "_notify_error",
        "_notify_pump_task",
        "_notify_queues",
        "_notify_source",
        "_notify_stop",
        "_queue_events",
        "_queue_leased",
        "_queue_store",
        "_queues",
        "_schema_ready",
        "_store",
        "_subscribed_channels",
        "_table_queue",
        "_unacked",
    )

    def __init__(
        self,
        feed: ChangeFeed | None = None,
        *,
        config: CDCConfig | None = None,
        store: AsyncObjectStore | SyncObjectStore | None = None,
        queue_store: BaseEventQueueStore[Any] | None = None,
        notify_source: AsyncNotificationSource | None = None,
        checkpoint_key: str = "default",
        notify_ceiling_bytes: int = MAX_NOTIFY_BYTES,
    ) -> None:
        """Initialize async backend with optional ChangeFeed, ObjectStore, queue store, and notify source."""
        if notify_ceiling_bytes < 1:
            msg = "notify_ceiling_bytes must be positive"
            raise ValueError(msg)
        sync_store = store.to_sync() if isinstance(store, AsyncObjectStore) else store
        self._store = sync_store
        self._feed = feed or ChangeFeed(config=config, store=sync_store)
        self._queue_store = queue_store
        self._notify_source = notify_source
        self._checkpoint_key = checkpoint_key
        self._notify_ceiling_bytes = notify_ceiling_bytes
        self._table_queue: AsyncTableEventQueue | None = (
            AsyncTableEventQueue(queue_store._config) if queue_store is not None else None
        )
        self._schema_ready = False
        self._queues: dict[str, collections.deque[str]] = {}
        self._queue_events: dict[str, asyncio.Event] = {}
        self._unacked: dict[str, tuple[str, str]] = {}
        self._queue_leased: set[str] = set()
        self._event_channels: dict[str, str] = {}
        self._checkpoints: dict[str, str] = {}
        self._subscribed_channels: set[str] = set()
        self._notify_queues: dict[str, asyncio.Queue[bytes | str]] = {}
        self._notify_pump_task: asyncio.Task[None] | None = None
        self._notify_stop = False
        self._notify_error: BaseException | None = None

    @property
    def feed(self) -> ChangeFeed:
        """Return the underlying ChangeFeed instance."""
        return self._feed

    async def _ensure_queue_schema(self) -> None:
        """Create the backing EventQueueStore table idempotently if configured."""
        if self._schema_ready or self._queue_store is None:
            return
        async with self._queue_store._config.provide_session() as driver:
            for statement in self._queue_store.create_statements():
                await driver.execute(statement)
        self._schema_ready = True

    async def save_checkpoint(self, channel: str, resume_token: str | int) -> str:
        """Persist a resume token for ``channel`` via the configured BaseEventQueueStore."""
        token = str(resume_token)
        self._checkpoints[channel] = token
        if self._queue_store is None:
            return token
        custom_saver = getattr(self._queue_store, "save_checkpoint_async", None)
        if callable(custom_saver):
            await custom_saver(channel, token, checkpoint_key=self._checkpoint_key)
            return token
        await self._ensure_queue_schema()
        now = datetime.now(UTC)
        event_id = _checkpoint_event_id(self._checkpoint_key, channel)
        payload_json = to_json(
            {
                "checkpoint_key": self._checkpoint_key,
                "channel": channel,
                "resume_token": token,
                "updated_at": now.isoformat(),
            }
        )
        table = self._queue_store.table_name
        async with self._queue_store._config.provide_session() as driver:
            await driver.execute(
                f"DELETE FROM {table} WHERE event_id = :event_id",
                {"event_id": event_id},
            )
            await driver.execute(
                f"INSERT INTO {table} "
                "(event_id, channel, payload_json, metadata_json, status, "
                "available_at, lease_expires_at, attempts, created_at) "
                "VALUES (:event_id, :channel, :payload_json, :metadata_json, :status, "
                ":available_at, :lease_expires_at, :attempts, :created_at)",
                {
                    "event_id": event_id,
                    "channel": _checkpoint_channel(channel),
                    "payload_json": payload_json,
                    "metadata_json": None,
                    "status": _CHECKPOINT_STATUS,
                    "available_at": now,
                    "lease_expires_at": None,
                    "attempts": 0,
                    "created_at": now,
                },
            )
            await driver.commit()
        return token

    async def load_checkpoint(self, channel: str) -> str | None:
        """Load the latest persisted resume token for ``channel``."""
        if self._queue_store is None:
            return self._checkpoints.get(channel)
        custom_loader = getattr(self._queue_store, "load_checkpoint_async", None)
        if callable(custom_loader):
            loaded = await custom_loader(channel, checkpoint_key=self._checkpoint_key)
            if loaded is not None:
                token = str(loaded)
                self._checkpoints[channel] = token
                return token
            return None
        await self._ensure_queue_schema()
        event_id = _checkpoint_event_id(self._checkpoint_key, channel)
        table = self._queue_store.table_name
        async with self._queue_store._config.provide_session() as driver:
            rows = await driver.select(
                f"SELECT payload_json FROM {table} WHERE event_id = :event_id",
                {"event_id": event_id},
            )
        if not rows:
            return None
        raw = _extract_row_field(rows[0], "payload_json", 0)
        if raw is None:
            return None
        decoded = (
            from_json(bytes(raw) if isinstance(raw, bytearray) else raw)
            if isinstance(raw, (str, bytes, bytearray))
            else raw
        )
        if isinstance(decoded, dict) and decoded.get("resume_token") is not None:
            token = str(decoded["resume_token"])
            self._checkpoints[channel] = token
            return token
        return None

    async def publish(
        self,
        channel: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Publish an event asynchronously, spilling oversized notify payloads to EventQueueStore."""
        if self._notify_source is not None:
            candidate_id = uuid4().hex
            if _exceeds_notify_ceiling(candidate_id, payload, metadata, self._notify_ceiling_bytes):
                if self._table_queue is None:
                    msg = (
                        "Notification payload exceeds notify_ceiling_bytes and requires "
                        "a configured BaseEventQueueStore for durable spill"
                    )
                    raise ImproperConfigurationError(msg)
                await self._ensure_queue_schema()
                spilled_id = await self._table_queue.publish(channel, payload, metadata)
                await self._notify_source.publish(channel, _encode_wakeup_frame(spilled_id, channel))
                self._event_channels[spilled_id] = channel
                return spilled_id
            frame = _encode_notify_frame(candidate_id, payload, metadata)
            await self._notify_source.publish(channel, frame)
            self._event_channels[candidate_id] = channel
            return candidate_id

        if self._table_queue is not None and self._store is None:
            await self._ensure_queue_schema()
            event_id = await self._table_queue.publish(channel, payload, metadata)
            self._event_channels[event_id] = channel
            return event_id

        _ = metadata
        wire_envelope, event = self._feed.publish_event(operation="publish", payload=payload)
        if channel not in self._queues:
            self._queues[channel] = collections.deque()
        self._queues[channel].append(wire_envelope)
        self._queue_events.setdefault(channel, asyncio.Event()).set()
        event_id_str = str(event.event_id)
        self._event_channels[event_id_str] = channel
        return event_id_str

    async def publish_many(
        self,
        events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]],
    ) -> list[str]:
        """Publish multiple events asynchronously and return their generated event IDs."""
        return [await self.publish(ch, p, meta) for ch, p, meta in events]

    async def _pump_notify_async(self, source: AsyncNotificationSource) -> None:
        try:
            async for frame in source.listen():
                if self._notify_stop:
                    break
                q = self._notify_queues.setdefault(frame.channel, asyncio.Queue())
                q.put_nowait(frame.payload)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._notify_error = exc

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue the next event asynchronously, hydrating durable or ObjectStore spills."""
        if self._notify_source is not None:
            if channel not in self._subscribed_channels:
                await self._notify_source.subscribe([channel])
                self._subscribed_channels.add(channel)
            q = self._notify_queues.setdefault(channel, asyncio.Queue())
            raw_payload: bytes | str | None = None
            if not q.empty():
                raw_payload = q.get_nowait()
            else:
                if self._notify_pump_task is None or self._notify_pump_task.done():
                    if self._notify_error is not None:
                        err = self._notify_error
                        self._notify_error = None
                        raise err
                    self._notify_stop = False
                    self._notify_pump_task = asyncio.create_task(self._pump_notify_async(self._notify_source))
                if poll_interval <= 0:
                    await asyncio.sleep(0)
                    if self._notify_error is not None:
                        err = self._notify_error
                        self._notify_error = None
                        raise err
                    if not q.empty():
                        raw_payload = q.get_nowait()
                else:
                    get_task = asyncio.create_task(q.get())
                    done, _ = await asyncio.wait(
                        {get_task, self._notify_pump_task},
                        timeout=poll_interval,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if get_task in done:
                        raw_payload = get_task.result()
                    else:
                        get_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await get_task
                        if self._notify_error is not None:
                            err = self._notify_error
                            self._notify_error = None
                            raise err
                        if not q.empty():
                            raw_payload = q.get_nowait()
            if raw_payload is None:
                return None
            spilled_id = _decode_wakeup_event_id(raw_payload)
            if spilled_id is not None:
                if self._table_queue is None:
                    msg = "Received spilled wakeup frame without a configured BaseEventQueueStore"
                    raise ImproperConfigurationError(msg)
                await self._ensure_queue_schema()
                message = await self._table_queue.dequeue_by_event_id(spilled_id)
                if message is not None:
                    self._queue_leased.add(message.event_id)
                    self._event_channels[message.event_id] = channel
                return message
            message = _decode_inline_notify(channel, raw_payload)
            self._event_channels[message.event_id] = channel
            return message

        if self._table_queue is not None and self._store is None:
            await self._ensure_queue_schema()
            message = await self._table_queue.dequeue(channel, poll_interval)
            if message is not None:
                self._queue_leased.add(message.event_id)
                self._event_channels[message.event_id] = channel
            return message

        q_mem = self._queues.get(channel)
        if not q_mem and poll_interval > 0:
            ev = self._queue_events.setdefault(channel, asyncio.Event())
            ev.clear()
            if not self._queues.get(channel):
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(ev.wait(), timeout=poll_interval)
            q_mem = self._queues.get(channel)
        if not q_mem:
            return None
        wire_envelope = q_mem.popleft()
        change_event: ChangeEvent = self._feed.hydrate_notification(wire_envelope)
        event_id_str = str(change_event.event_id)
        self._unacked[event_id_str] = (channel, wire_envelope)
        self._event_channels[event_id_str] = channel
        now = datetime.now(UTC)
        return EventMessage(
            event_id=event_id_str,
            channel=channel,
            payload=change_event.payload,
            metadata=None,
            attempts=1,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )

    async def ack(self, event_id: str) -> None:
        """Acknowledge successful event processing and persist checkpoint if configured."""
        if event_id in self._queue_leased and self._table_queue is not None:
            await self._table_queue.ack(event_id)
            self._queue_leased.discard(event_id)
        else:
            self._unacked.pop(event_id, None)
        channel = self._event_channels.pop(event_id, None)
        if channel is not None and self._queue_store is not None:
            await self.save_checkpoint(channel, event_id)

    async def nack(self, event_id: str) -> None:
        """Return an unacknowledged event to the queue for redelivery."""
        if event_id in self._queue_leased and self._table_queue is not None:
            await self._table_queue.nack(event_id)
            self._queue_leased.discard(event_id)
            return
        entry = self._unacked.pop(event_id, None)
        if entry is not None:
            channel, wire_envelope = entry
            if channel not in self._queues:
                self._queues[channel] = collections.deque()
            self._queues[channel].appendleft(wire_envelope)
            self._queue_events.setdefault(channel, asyncio.Event()).set()

    async def shutdown(self) -> None:
        """Release transient buffers while leaving durable checkpoints and queue rows intact."""
        self._notify_stop = True
        if self._notify_pump_task is not None:
            self._notify_pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._notify_pump_task
            self._notify_pump_task = None
        if self._notify_source is not None and self._subscribed_channels:
            await self._notify_source.unsubscribe(sorted(self._subscribed_channels))
        self._queues.clear()
        self._queue_events.clear()
        self._unacked.clear()
        self._queue_leased.clear()
        self._event_channels.clear()
        self._notify_queues.clear()
        self._subscribed_channels.clear()
