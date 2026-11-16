"""Runtime-checkable database engine capability protocols and wire value records."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

import msgspec

from vertebra.typing import FrozenBaseStruct

if TYPE_CHECKING:
    from vertebra._vertebra import VectorBatchView, VectorView
    from vertebra.db.cdc import ChangeEvent

__all__ = (
    "AsyncChangeSource",
    "AsyncNotificationSource",
    "AsyncPipeline",
    "AsyncPipelineCapable",
    "AsyncQueueStore",
    "EmbeddingPushdown",
    "NotificationEvent",
    "QueueEntry",
    "SyncChangeSource",
    "SyncNotificationSource",
    "SyncPipeline",
    "SyncPipelineCapable",
    "SyncQueueStore",
)


class NotificationEvent(FrozenBaseStruct, frozen=True, gc=False):
    """Pub/sub notification frame emitted by a ``SyncNotificationSource`` or ``AsyncNotificationSource``."""

    channel: str
    """Logical notification channel or topic name."""

    payload: bytes
    """Raw notification payload bytes."""

    sender_id: str | None = None
    """Optional identifier of the backend connection, session, or worker that sent the event."""

    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Engine-specific notification metadata mapping."""


class QueueEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Durable background job record leased from or enqueued to a queue store."""

    job_id: str
    queue: str
    task_name: str
    payload: bytes
    attempt: int = 1
    max_attempts: int = 3
    lease_until_ms: int = 0
    scheduled_at_ms: int | None = None
    headers: dict[str, str] = msgspec.field(default_factory=dict)


@runtime_checkable
class SyncPipeline(Protocol):
    """Synchronous wire-level query pipeline batching multiple statements into one round-trip."""

    def __enter__(self) -> Self:
        """Enter the synchronous pipeline context."""
        ...

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the synchronous pipeline context."""
        ...

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a statement for batched execution."""
        ...

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a row-returning query for batched selection."""
        ...

    def results(self) -> list[Any]:
        """Flush queued pipeline operations and return their ordered results."""
        ...


@runtime_checkable
class AsyncPipeline(Protocol):
    """Asynchronous wire-level query pipeline batching multiple statements into one round-trip."""

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pipeline context."""
        ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the asynchronous pipeline context."""
        ...

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a statement for batched execution."""
        ...

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a row-returning query for batched selection."""
        ...

    async def results(self) -> list[Any]:
        """Flush queued pipeline operations asynchronously and return their ordered results."""
        ...


@runtime_checkable
class SyncPipelineCapable(Protocol):
    """Capability protocol for synchronous drivers supporting wire-level query pipelining."""

    def pipeline(self) -> SyncPipeline:
        """Create a new synchronous query pipeline bound to this driver."""
        ...


@runtime_checkable
class AsyncPipelineCapable(Protocol):
    """Capability protocol for asynchronous drivers supporting wire-level query pipelining."""

    def pipeline(self) -> AsyncPipeline:
        """Create a new asynchronous query pipeline bound to this driver."""
        ...


@runtime_checkable
class SyncNotificationSource(Protocol):
    """Capability protocol for synchronous database or cache pub/sub notification transports."""

    def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target notification channel."""
        ...

    def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        ...

    def unsubscribe(self, channels: Sequence[str]) -> None:
        """Unsubscribe from one or more notification channels."""
        ...

    def listen(self) -> Iterator[NotificationEvent]:
        """Yield incoming ``NotificationEvent`` frames across subscribed channels."""
        ...


@runtime_checkable
class AsyncNotificationSource(Protocol):
    """Capability protocol for asynchronous database or cache pub/sub notification transports."""

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target notification channel."""
        ...

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        ...

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        """Unsubscribe from one or more notification channels."""
        ...

    def listen(self) -> AsyncIterator[NotificationEvent]:
        """Yield incoming ``NotificationEvent`` frames across subscribed channels."""
        ...


@runtime_checkable
class SyncChangeSource(Protocol):
    """Capability protocol for synchronous Change Data Capture (CDC) stream readers."""

    def stream(self) -> Iterator[ChangeEvent]:
        """Yield ``ChangeEvent`` items synchronously as database changes arrive."""
        ...


@runtime_checkable
class AsyncChangeSource(Protocol):
    """Capability protocol for asynchronous Change Data Capture (CDC) stream readers."""

    def stream(self) -> AsyncIterator[ChangeEvent]:
        """Yield ``ChangeEvent`` items asynchronously as database changes arrive."""
        ...


@runtime_checkable
class SyncQueueStore(Protocol):
    """Capability protocol for synchronous database-backed background job queues."""

    def ensure_schema(self) -> None:
        """Idempotently create or migrate backing queue tables and indexes."""
        ...

    def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry and return its assigned job_id."""
        ...

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return their assigned job_id values."""
        ...

    def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease."""
        ...

    def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        ...

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-lettering."""
        ...

    def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims."""
        ...

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims back into ready status."""
        ...

    def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        ...

    def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        ...

    def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        ...

    def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional status up to limit."""
        ...

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        ...

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        ...

    def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        ...

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        ...

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        ...

    def close(self) -> None:
        """Close backing store connections and release resources."""
        ...


@runtime_checkable
class AsyncQueueStore(Protocol):
    """Capability protocol for asynchronous database-backed background job queues."""

    async def ensure_schema(self) -> None:
        """Idempotently create or migrate backing queue tables and indexes."""
        ...

    async def enqueue(self, entry: QueueEntry) -> str:
        """Enqueue a single job entry and return its assigned job_id."""
        ...

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        """Bulk-enqueue job entries and return their assigned job_id values."""
        ...

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        """Atomically claim up to limit ready jobs under a visibility lease."""
        ...

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        """Mark a fenced (job_id, attempt) claim as completed."""
        ...

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        """Record a failed (job_id, attempt) execution and schedule retry or dead-lettering."""
        ...

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        """Extend the visibility lease for active (job_ids, worker_id) claims."""
        ...

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        """Reap abandoned in-flight claims back into ready status."""
        ...

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready status."""
        ...

    async def cancel(self, job_id: str) -> bool:
        """Cancel an enqueued or scheduled job."""
        ...

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        """Retrieve a specific queue entry by job_id."""
        ...

    async def list_entries(
        self,
        queue: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        """List queue entries matching optional status up to limit."""
        ...

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        """Return the delay in seconds until the next scheduled job is due, or None."""
        ...

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        """Reserve an idempotency key for ttl_seconds."""
        ...

    async def release_unique_key(self, key: str) -> bool:
        """Release an idempotency key reservation."""
        ...

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire a distributed leader lease for a cron or coordination slot."""
        ...

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        """Release a held distributed leader lease."""
        ...

    async def aclose(self) -> None:
        """Close backing store connections and release resources."""
        ...


@runtime_checkable
class EmbeddingPushdown(Protocol):
    """Capability protocol for SQL-pushed vector embedding generation and similarity search."""

    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        """Generate document embeddings inside the database engine and return a ``VectorBatchView``."""
        ...

    async def embed_query(
        self,
        text: str,
        *,
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorView:
        """Generate a single query embedding inside the database engine and return a ``VectorView``."""
        ...

    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        """Execute an engine-native vector similarity query and return top-k row mappings."""
        ...
