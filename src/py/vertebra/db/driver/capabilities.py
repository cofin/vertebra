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
    "AsyncPipeline",
    "AsyncPipelineCapable",
    "ClaimedJob",
    "EmbeddingPushdown",
    "NotificationEvent",
    "NotificationSource",
    "QueueStore",
    "SyncChangeSource",
    "SyncPipeline",
    "SyncPipelineCapable",
)


class NotificationEvent(FrozenBaseStruct, frozen=True, gc=False):
    """Pub/sub notification frame emitted by a ``NotificationSource``."""

    channel: str
    """Logical notification channel or topic name."""

    payload: bytes
    """Raw notification payload bytes."""

    sender_id: str | None = None
    """Optional identifier of the backend connection, session, or worker that sent the event."""

    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Engine-specific notification metadata mapping."""


class ClaimedJob(FrozenBaseStruct, frozen=True, gc=False):
    """Durable background job record leased from a ``QueueStore``."""

    job_id: str
    """Unique job identifier."""

    queue: str
    """Logical queue name from which the job was claimed."""

    task_name: str
    """Registered task handler name."""

    payload: bytes
    """Serialized task invocation payload bytes."""

    attempt: int
    """One-based delivery attempt counter used to fence completion and failure updates."""

    max_attempts: int
    """Maximum delivery attempts permitted before dead-letter transition."""

    lease_until_ms: int
    """Unix epoch timestamp in milliseconds when the active visibility lease expires."""

    headers: dict[str, str] = msgspec.field(default_factory=dict)
    """Optional string metadata headers propagated alongside the job."""


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
class NotificationSource(Protocol):
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
class QueueStore(Protocol):
    """Capability protocol for durable database-backed background job queues."""

    async def ensure_schema(self) -> None:
        """Idempotently create or migrate backing queue tables and indexes."""
        ...

    async def enqueue(
        self,
        queue: str,
        task_name: str,
        payload: bytes,
        *,
        job_id: str | None = None,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """Enqueue a single job and return its assigned ``job_id``."""
        ...

    async def enqueue_many(
        self,
        queue: str,
        items: Sequence[tuple[str, bytes]],
        *,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
    ) -> list[str]:
        """Bulk-enqueue ``(task_name, payload)`` pairs and return their assigned ``job_id`` values."""
        ...

    async def claim(
        self,
        queue: str,
        *,
        limit: int = 1,
        lease_ms: int = 30_000,
        worker_id: str = "",
    ) -> list[ClaimedJob]:
        """Atomically claim up to ``limit`` ready jobs under a visibility lease."""
        ...

    async def heartbeat(self, job_id: str, attempt: int, *, extend_ms: int = 30_000) -> bool:
        """Extend the visibility lease for an active ``(job_id, attempt)`` claim."""
        ...

    async def complete(self, job_id: str, attempt: int) -> bool:
        """Mark a fenced ``(job_id, attempt)`` claim as completed."""
        ...

    async def fail(self, job_id: str, attempt: int, *, error: str, retry_delay_ms: int | None = None) -> None:
        """Record a failed ``(job_id, attempt)`` execution and schedule retry or dead-lettering."""
        ...

    async def promote_scheduled(self, queue: str | None = None, *, now_ms: int | None = None, limit: int = 100) -> int:
        """Promote due scheduled jobs into ready state and return the promoted count."""
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
