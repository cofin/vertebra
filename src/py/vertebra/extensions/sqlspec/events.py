"""Change data capture (CDC) event backends for SQLSpec integration."""

from __future__ import annotations

import collections
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.cdc import CDCConfig, ChangeEvent, ChangeFeed
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.typing import MissingDependencyError

try:
    from sqlspec.extensions.events import (
        AsyncEventBackendProtocol,
        EventMessage,
        SyncEventBackendProtocol,
    )
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "ChangeFeedAsyncEventBackend",
    "ChangeFeedSyncEventBackend",
)


class ChangeFeedSyncEventBackend(SyncEventBackendProtocol):
    """Synchronous SQLSpec event backend wrapping vertebra.db.cdc.ChangeFeed."""

    supports_sync: ClassVar[bool] = True
    backend_name: ClassVar[str] = "changefeed"
    __slots__ = ("_feed", "_queues", "_unacked")

    def __init__(
        self,
        feed: ChangeFeed | None = None,
        *,
        config: CDCConfig | None = None,
        store: SyncObjectStore | None = None,
    ) -> None:
        """Initialize backend with a ChangeFeed or CDCConfig and optional ObjectStore."""
        self._feed = feed or ChangeFeed(config=config, store=store)
        self._queues: dict[str, collections.deque[str]] = {}
        self._unacked: dict[str, tuple[str, str]] = {}

    @property
    def feed(self) -> ChangeFeed:
        """Return the underlying ChangeFeed instance."""
        return self._feed

    @staticmethod
    def build_emit_message_sql(
        prefix: str,
        payload: bytes | str,
        *,
        transactional: bool = True,
    ) -> tuple[str, tuple[bool, str, bytes]]:
        """Build a pg_logical_emit_message SQL statement and bind parameter tuple."""
        return ChangeFeed.build_emit_message_sql(prefix, payload, transactional=transactional)

    def publish(
        self,
        channel: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Publish an event to channel, spilling to ObjectStore when exceeding threshold."""
        _ = metadata
        wire_envelope, event = self._feed.publish_event(operation="publish", payload=payload)
        if channel not in self._queues:
            self._queues[channel] = collections.deque()
        self._queues[channel].append(wire_envelope)
        return str(event.event_id)

    def publish_many(
        self,
        events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]],
    ) -> list[str]:
        """Publish multiple events sequentially and return their generated event IDs."""
        return [self.publish(ch, p, meta) for ch, p, meta in events]

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue the next event for channel, hydrating spilled ObjectStore payloads."""
        _ = poll_interval
        q = self._queues.get(channel)
        if not q:
            return None
        wire_envelope = q.popleft()
        change_event: ChangeEvent = self._feed.hydrate_notification(wire_envelope)
        event_id_str = str(change_event.event_id)
        self._unacked[event_id_str] = (channel, wire_envelope)
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
        """Acknowledge successful event processing."""
        self._unacked.pop(event_id, None)

    def nack(self, event_id: str) -> None:
        """Return an unacknowledged event to the front of the channel queue for redelivery."""
        entry = self._unacked.pop(event_id, None)
        if entry is not None:
            channel, wire_envelope = entry
            if channel not in self._queues:
                self._queues[channel] = collections.deque()
            self._queues[channel].appendleft(wire_envelope)

    def shutdown(self) -> None:
        """Clear queues and release held unacknowledged events."""
        self._queues.clear()
        self._unacked.clear()


class ChangeFeedAsyncEventBackend(AsyncEventBackendProtocol):
    """Asynchronous SQLSpec event backend wrapping vertebra.db.cdc.ChangeFeed."""

    supports_async: ClassVar[bool] = True
    backend_name: ClassVar[str] = "changefeed"
    __slots__ = ("_feed", "_queues", "_unacked")

    def __init__(
        self,
        feed: ChangeFeed | None = None,
        *,
        config: CDCConfig | None = None,
        store: AsyncObjectStore | SyncObjectStore | None = None,
    ) -> None:
        """Initialize async backend with a ChangeFeed or CDCConfig and optional ObjectStore."""
        sync_store = store.to_sync() if isinstance(store, AsyncObjectStore) else store
        self._feed = feed or ChangeFeed(config=config, store=sync_store)
        self._queues: dict[str, collections.deque[str]] = {}
        self._unacked: dict[str, tuple[str, str]] = {}

    @property
    def feed(self) -> ChangeFeed:
        """Return the underlying ChangeFeed instance."""
        return self._feed

    @staticmethod
    def build_emit_message_sql(
        prefix: str,
        payload: bytes | str,
        *,
        transactional: bool = True,
    ) -> tuple[str, tuple[bool, str, bytes]]:
        """Build a pg_logical_emit_message SQL statement and bind parameter tuple."""
        return ChangeFeed.build_emit_message_sql(prefix, payload, transactional=transactional)

    async def publish(
        self,
        channel: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Publish an event asynchronously, spilling to ObjectStore when exceeding threshold."""
        _ = metadata
        wire_envelope, event = self._feed.publish_event(operation="publish", payload=payload)
        if channel not in self._queues:
            self._queues[channel] = collections.deque()
        self._queues[channel].append(wire_envelope)
        return str(event.event_id)

    async def publish_many(
        self,
        events: Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]],
    ) -> list[str]:
        """Publish multiple events asynchronously and return their generated event IDs."""
        return [await self.publish(ch, p, meta) for ch, p, meta in events]

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue the next event for channel, hydrating spilled ObjectStore payloads."""
        _ = poll_interval
        q = self._queues.get(channel)
        if not q:
            return None
        wire_envelope = q.popleft()
        change_event: ChangeEvent = self._feed.hydrate_notification(wire_envelope)
        event_id_str = str(change_event.event_id)
        self._unacked[event_id_str] = (channel, wire_envelope)
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
        """Acknowledge successful event processing asynchronously."""
        self._unacked.pop(event_id, None)

    async def nack(self, event_id: str) -> None:
        """Return an unacknowledged event to the front of the channel queue for redelivery."""
        entry = self._unacked.pop(event_id, None)
        if entry is not None:
            channel, wire_envelope = entry
            if channel not in self._queues:
                self._queues[channel] = collections.deque()
            self._queues[channel].appendleft(wire_envelope)

    async def shutdown(self) -> None:
        """Clear queues and release held unacknowledged events asynchronously."""
        self._queues.clear()
        self._unacked.clear()
