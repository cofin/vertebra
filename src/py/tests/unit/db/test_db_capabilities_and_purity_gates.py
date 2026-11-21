"""Unit tests for driver capability protocols, registry URL parsing, and AST purity gates."""

from __future__ import annotations

import ast
import gc
import importlib
import re
import sys
import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

import vertebra
import vertebra.db as vertebra_db
import vertebra.db.driver as vertebra_db_driver
from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.db import (
    AlloyDBConfig,
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    ChangeEvent,
    CloudSQLConfig,
    DatabaseRegistry,
    OperationalError,
    ProgrammingError,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
    provide_driver,
)
from vertebra.db.adapters.bigquery import BigQueryConfig
from vertebra.db.adapters.datafusion import DataFusionConfig, DataFusionEngine
from vertebra.db.adapters.duckdb import DuckDBConfig
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import RedisConfig
from vertebra.db.adapters.spanner import (
    SpannerConfig,
)
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.db.driver.capabilities import (
    AsyncChangeSource,
    AsyncNotificationSource,
    AsyncPipeline,
    AsyncPipelineCapable,
    AsyncQueueStore,
    EmbeddingPushdown,
    NotificationEvent,
    QueueEntry,
    QueueReservation,
    SyncChangeSource,
    SyncNotificationSource,
    SyncPipeline,
    SyncPipelineCapable,
    SyncQueueStore,
)
from vertebra.typing import reset_dependency_cache
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)

if TYPE_CHECKING:
    pass


_CAPABILITY_EXPORT_NAMES: tuple[str, ...] = (
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


class _ConformingSyncPipeline:
    """Minimal conforming stand-in for `SyncPipeline`."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: list[tuple[str, str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("select", sql, parameters))

    def results(self) -> list[Any]:
        return list(self._ops)


class _ConformingAsyncPipeline:
    """Minimal conforming stand-in for `AsyncPipeline`."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: list[tuple[str, str, Any]] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("select", sql, parameters))

    async def results(self) -> list[Any]:
        return list(self._ops)


class _ConformingSyncPipelineDriver:
    """Minimal conforming stand-in for `SyncPipelineCapable`."""

    def pipeline(self) -> SyncPipeline:
        return _ConformingSyncPipeline()


class _ConformingAsyncPipelineDriver:
    """Minimal conforming stand-in for `AsyncPipelineCapable`."""

    def pipeline(self) -> AsyncPipeline:
        return _ConformingAsyncPipeline()


class _ConformingNotificationSource:
    """Minimal conforming stand-in for `AsyncNotificationSource`."""

    __slots__ = ("_events", "_subscribed")

    def __init__(self) -> None:
        self._subscribed: set[str] = set()
        self._events: list[NotificationEvent] = []

    @property
    def subscribed_channels(self) -> list[str]:
        return list(self._subscribed)

    async def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if channel in self._subscribed:
            self._events.append(NotificationEvent(channel=channel, payload=raw, sender_id="standin"))

    async def subscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.update(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.difference_update(channels)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        for event in tuple(self._events):
            yield event

    async def aclose(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        pass


class _ConformingSyncNotificationSource:
    """Minimal conforming stand-in for `SyncNotificationSource`."""

    __slots__ = ("_events", "_subscribed")

    def __init__(self) -> None:
        self._subscribed: set[str] = set()
        self._events: list[NotificationEvent] = []

    @property
    def subscribed_channels(self) -> list[str]:
        return list(self._subscribed)

    def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if channel in self._subscribed:
            self._events.append(NotificationEvent(channel=channel, payload=raw, sender_id="sync-standin"))

    def subscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.update(channels)

    def unsubscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.difference_update(channels)

    def listen(self) -> Iterator[NotificationEvent]:
        yield from tuple(self._events)

    def close(self) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        pass


class _ConformingSyncChangeSource:
    """Minimal conforming stand-in for `SyncChangeSource`."""

    def stream(self) -> Iterator[ChangeEvent]:
        yield ChangeEvent(event_id=1, channel="orders", operation="INSERT", payload={"id": 1})


class _ConformingAsyncChangeSource:
    """Minimal conforming stand-in for `AsyncChangeSource`."""

    async def stream(self) -> AsyncIterator[ChangeEvent]:
        yield ChangeEvent(event_id=2, channel="orders", operation="UPDATE", payload={"id": 1})


class _ConformingSyncQueueStore:
    """Minimal conforming stand-in for `SyncQueueStore`."""

    __slots__ = ("_jobs", "_leases", "_ready", "_reservations", "_unique_keys")

    def __init__(self) -> None:
        self._ready = False
        self._jobs: dict[str, QueueEntry] = {}
        self._leases: dict[str, tuple[str, float]] = {}
        self._reservations: dict[str, QueueReservation] = {}
        self._unique_keys: set[str] = set()

    def ensure_schema(self) -> None:
        self._ready = True

    def enqueue(self, entry: QueueEntry) -> str:
        jid = entry.job_id or f"job-{len(self._jobs) + 1}"
        stored = QueueEntry(
            job_id=jid,
            queue=entry.queue,
            task_name=entry.task_name,
            payload=entry.payload,
            attempt=entry.attempt,
            max_attempts=entry.max_attempts,
            lease_until_ms=entry.lease_until_ms,
            scheduled_at_ms=entry.scheduled_at_ms,
            headers=dict(entry.headers),
        )
        self._jobs[jid] = stored
        return jid

    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        return [self.enqueue(e) for e in entries]

    def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        _ = worker_id
        claimed: list[QueueEntry] = []
        for jid, job in list(self._jobs.items()):
            if job.queue in queues and len(claimed) < limit:
                leased = QueueEntry(
                    job_id=job.job_id,
                    queue=job.queue,
                    task_name=job.task_name,
                    payload=job.payload,
                    attempt=job.attempt,
                    max_attempts=job.max_attempts,
                    lease_until_ms=int(lease_seconds * 1000),
                    headers=dict(job.headers),
                )
                self._jobs[jid] = leased
                claimed.append(leased)
        return claimed

    def ack(self, job_id: str, *, attempt: int) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.attempt != attempt:
            return False
        self._jobs.pop(job_id, None)
        return True

    def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        _ = (attempt, error, retry_delay_s)
        self._jobs.pop(job_id, None)
        return True

    def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        _ = (worker_id, lease_seconds)
        return sum(1 for jid in job_ids if jid in self._jobs)

    def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        _ = lease_seconds
        return 0

    def promote_scheduled(self, *, limit: int = 100) -> int:
        _ = limit
        return 0

    def cancel(self, job_id: str) -> bool:
        return self._jobs.pop(job_id, None) is not None

    def get_entry(self, job_id: str) -> QueueEntry | None:
        return self._jobs.get(job_id)

    def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        _ = status
        if queue is None:
            return list(self._jobs.values())[:limit]
        return [j for j in self._jobs.values() if j.queue == queue][:limit]

    def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        _ = (before_ms, limit)
        return 0

    def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        _ = queue
        res = self._reservations.get(key)
        return self._jobs.get(res.job_id) if res is not None else None

    def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        _ = ttl_seconds
        existing = self._reservations.get(key)
        if existing is not None:
            return existing
        self._reservations[key] = QueueReservation(
            key=key,
            job_id=job_id,
            task_name=task_name,
            created_at_ms=int(time.time() * 1000),
        )
        return None

    def get_reservation(self, key: str) -> QueueReservation | None:
        return self._reservations.get(key)

    def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        existing = self._reservations.get(key)
        if existing is None:
            return False
        if expected_job_id is not None and existing.job_id != expected_job_id:
            return False
        self._reservations.pop(key, None)
        return True

    def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        _ = (worker_id, attempt)
        return job_id in self._jobs

    def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        _ = heartbeat_at_ms
        return sum(1 for jid in job_ids if jid in self._jobs)

    def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        return sum(1 for jid in job_ids if jid in self._jobs)

    def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        _ = error
        return self._jobs.pop(job_id, None) is not None

    def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        _ = (now_ms, limit)
        return 0

    def next_due_delay(self, queues: Sequence[str]) -> float | None:
        _ = queues
        return None

    def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        _ = ttl_seconds
        if key in self._unique_keys:
            return False
        self._unique_keys.add(key)
        return True

    def release_unique_key(self, key: str) -> bool:
        if key in self._unique_keys:
            self._unique_keys.remove(key)
            return True
        return False

    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        now = time.time()
        existing = self._leases.get(lease_key)
        if existing is None or existing[1] <= now or existing[0] == owner_id:
            self._leases[lease_key] = (owner_id, now + ttl_seconds)
            return True
        return False

    def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        existing = self._leases.get(lease_key)
        if existing is not None and existing[0] == owner_id:
            del self._leases[lease_key]
            return True
        return False

    def close(self) -> None:
        self._jobs.clear()
        self._leases.clear()
        self._unique_keys.clear()


class _ConformingAsyncQueueStore:
    """Minimal conforming stand-in for `AsyncQueueStore`."""

    __slots__ = ("_sync",)

    def __init__(self) -> None:
        self._sync = _ConformingSyncQueueStore()

    async def ensure_schema(self) -> None:
        self._sync.ensure_schema()

    async def enqueue(self, entry: QueueEntry) -> str:
        return self._sync.enqueue(entry)

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        return self._sync.enqueue_many(entries)

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        return self._sync.claim(queues, limit=limit, worker_id=worker_id, lease_seconds=lease_seconds)

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        return self._sync.ack(job_id, attempt=attempt)

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        return self._sync.nack(job_id, attempt=attempt, error=error, retry_delay_s=retry_delay_s)

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        return self._sync.heartbeat(job_ids, worker_id=worker_id, lease_seconds=lease_seconds)

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        return self._sync.reap_stale(lease_seconds=lease_seconds)

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        return self._sync.promote_scheduled(limit=limit)

    async def cancel(self, job_id: str) -> bool:
        return self._sync.cancel(job_id)

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        return self._sync.get_entry(job_id)

    async def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[QueueEntry]:
        return self._sync.list_entries(queue, status=status, limit=limit)

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        return self._sync.cleanup_terminal(before_ms=before_ms, limit=limit)

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        return self._sync.get_entry_by_key(key, queue=queue)

    async def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        return self._sync.reserve_identity(key, job_id=job_id, task_name=task_name, ttl_seconds=ttl_seconds)

    async def get_reservation(self, key: str) -> QueueReservation | None:
        return self._sync.get_reservation(key)

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        return self._sync.reset_identity(key, expected_job_id=expected_job_id)

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        return self._sync.assign_worker(job_id, worker_id=worker_id, attempt=attempt)

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        return self._sync.touch_heartbeats(job_ids, heartbeat_at_ms=heartbeat_at_ms)

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        return self._sync.null_heartbeats(job_ids)

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        return self._sync.interrupt_task(job_id, error=error)

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        return self._sync.expire_overdue(now_ms=now_ms, limit=limit)

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        return self._sync.next_due_delay(queues)

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        return self._sync.reserve_unique_key(key, ttl_seconds=ttl_seconds)

    async def release_unique_key(self, key: str) -> bool:
        return self._sync.release_unique_key(key)

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        return self._sync.acquire_lease(lease_key, owner_id=owner_id, ttl_seconds=ttl_seconds)

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        return self._sync.release_lease(lease_key, owner_id=owner_id)

    async def aclose(self) -> None:
        self._sync.close()


class _ConformingEmbeddingPushdown:
    """Minimal conforming stand-in for `EmbeddingPushdown`."""

    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        _ = task
        dim = dimensions or 3
        rows = [[float(idx + 1)] * dim for idx, _ in enumerate(texts)]
        return VectorBatchView.from_f32_rows(rows, normalize=normalize)

    async def embed_query(
        self,
        text: str,
        *,
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorView:
        _ = text
        dim = dimensions or 3
        return VectorView.from_f32([1.0] * dim, normalize=normalize)

    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        vec_len = len(query_vector)
        return [
            {
                "table": table,
                "column": embedding_column,
                "top_k": top_k,
                "metric": metric,
                "dimensions": vec_len,
            }
        ]


@pytest.mark.parametrize(
    ("record_cls", "default_kwargs", "full_kwargs", "mutate_attr", "mutate_val", "dict_attr"),
    [
        (
            NotificationEvent,
            {"channel": "orders", "payload": b"\x00\x01notify"},
            {
                "channel": "orders",
                "payload": b"\x00\x01notify",
                "sender_id": "pg-backend-42",
                "metadata": {"lsn": "0/16B3748", "attempt": 1},
            },
            "channel",
            "mutated",
            "metadata",
        ),
        (
            QueueEntry,
            {
                "job_id": "job-101",
                "queue": "emails",
                "task_name": "send_receipt",
                "payload": b"\x80\x02payload",
                "attempt": 1,
                "max_attempts": 5,
                "lease_until_ms": 1_700_000_030_000,
            },
            {
                "job_id": "job-101",
                "queue": "emails",
                "task_name": "send_receipt",
                "payload": b"\x80\x02payload",
                "attempt": 2,
                "max_attempts": 5,
                "lease_until_ms": 1_700_000_060_000,
                "headers": {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
            },
            "attempt",
            99,
            "headers",
        ),
    ],
)
def test_capability_records_frozen_gc_untracked_and_serializer_roundtrip(
    record_cls: type[Any],
    default_kwargs: dict[str, Any],
    full_kwargs: dict[str, Any],
    mutate_attr: str,
    mutate_val: Any,
    dict_attr: str,
) -> None:
    """Verify NotificationEvent and QueueEntry are frozen, isolated on defaults, and round-trip."""
    first = record_cls(**default_kwargs)
    second = record_cls(**default_kwargs)
    populated = record_cls(**full_kwargs)

    if record_cls is NotificationEvent:
        assert not gc.is_tracked(first)
        assert not gc.is_tracked(populated)
        assert first.sender_id is None
    assert getattr(first, dict_attr) == {}
    assert getattr(first, dict_attr) is not getattr(second, dict_attr)

    with pytest.raises(AttributeError):
        setattr(populated, mutate_attr, mutate_val)

    json_roundtrip = decode_json(encode_json_bytes(populated), type=record_cls)
    assert json_roundtrip == populated
    if record_cls is NotificationEvent:
        assert not gc.is_tracked(json_roundtrip)

    msgpack_roundtrip = decode_msgpack(encode_msgpack_bytes(populated), type=record_cls)
    assert msgpack_roundtrip == populated
    if record_cls is NotificationEvent:
        assert not gc.is_tracked(msgpack_roundtrip)


@pytest.mark.parametrize(
    ("standin_factory", "expected_protocol"),
    [
        (_ConformingSyncPipeline, SyncPipeline),
        (_ConformingAsyncPipeline, AsyncPipeline),
        (_ConformingSyncPipelineDriver, SyncPipelineCapable),
        (_ConformingAsyncPipelineDriver, AsyncPipelineCapable),
        (_ConformingNotificationSource, AsyncNotificationSource),
        (_ConformingSyncNotificationSource, SyncNotificationSource),
        (_ConformingSyncChangeSource, SyncChangeSource),
        (_ConformingAsyncChangeSource, AsyncChangeSource),
        (_ConformingSyncQueueStore, SyncQueueStore),
        (_ConformingAsyncQueueStore, AsyncQueueStore),
        (_ConformingEmbeddingPushdown, EmbeddingPushdown),
    ],
)
def test_capability_protocols_runtime_isinstance_and_sqlite_negative_checks(
    standin_factory: Any,
    expected_protocol: Any,
) -> None:
    """Verify conforming stand-ins satisfy runtime isinstance while SQLite drivers evaluate False."""
    instance = standin_factory()
    assert isinstance(instance, expected_protocol)
    assert not isinstance(object(), expected_protocol)

    sqlite_sync = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    sqlite_async = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    try:
        assert isinstance(sqlite_sync, SyncPipelineCapable) is True
        assert isinstance(sqlite_async, AsyncPipelineCapable) is True
        sync_pipe = sqlite_sync.pipeline()
        async_pipe = sqlite_async.pipeline()
        assert isinstance(sync_pipe, SyncPipeline) is True
        assert isinstance(sync_pipe, AsyncPipeline) is False
        assert isinstance(async_pipe, AsyncPipeline) is True
        assert isinstance(async_pipe, SyncPipeline) is False
        if expected_protocol not in (SyncPipelineCapable, AsyncPipelineCapable):
            assert isinstance(sqlite_sync, expected_protocol) is False
            assert isinstance(sqlite_async, expected_protocol) is False
    finally:
        sqlite_sync.close()


def test_notification_source_legacy_symbol_cleanly_removed() -> None:
    """Verify legacy NotificationSource symbol raises AttributeError or ImportError."""
    import vertebra.db as db_mod
    import vertebra.db.driver as driver_mod

    assert not hasattr(driver_mod, "NotificationSource")
    assert not hasattr(db_mod, "NotificationSource")


def test_queue_store_and_claimed_job_legacy_symbols_cleanly_removed() -> None:
    """Verify legacy QueueStore and ClaimedJob symbols raise AttributeError or ImportError."""
    import vertebra.db as db_mod
    import vertebra.db.driver as driver_mod
    import vertebra.db.driver.capabilities as cap_mod

    assert not hasattr(driver_mod, "QueueStore")
    assert not hasattr(db_mod, "QueueStore")
    assert not hasattr(cap_mod, "QueueStore")
    assert not hasattr(driver_mod, "ClaimedJob")
    assert not hasattr(db_mod, "ClaimedJob")
    assert not hasattr(cap_mod, "ClaimedJob")


_NATIVE_CURSOR_REMOVED_SYMBOLS: tuple[tuple[str, str], ...] = (
    ("vertebra.db.dbapi", "is_row_returning_sql"),
    ("vertebra.db.dbapi", "is_read_only_sql"),
    ("vertebra.db.dbapi", "is_insert_sql"),
    ("vertebra.db.dbapi", "coerce_dbapi_rows_and_desc"),
    ("vertebra.db.dbapi._sync", "is_row_returning_sql"),
    ("vertebra.db.dbapi._sync", "is_read_only_sql"),
    ("vertebra.db.dbapi._sync", "is_insert_sql"),
    ("vertebra.db.dbapi._sync", "_scan_unquoted_sql_tokens"),
    ("vertebra.db.dbapi._sync", "_match_dollar_quote_tag"),
    ("vertebra.db.dbapi._sync", "_has_tsql_output_clause"),
    ("vertebra.db.dbapi._sync", "coerce_dbapi_rows_and_desc"),
    ("vertebra.db.dbapi._sync", "_coerce_column_description"),
    ("vertebra.db.dbapi._sync", "_ErrorTranslator"),
    ("vertebra.db.dbapi._sync", "_translate_errors"),
    ("vertebra.db.dbapi._sync", "_TRANSLATE_ERRORS"),
    ("vertebra.db.dbapi._sync", "_translate_native_error"),
    ("vertebra.db.dbapi._sync", "_translate_vertebra_error"),
    ("vertebra.db.dbapi._sync", "_extract_out_bind_vars"),
    ("vertebra.db.dbapi._sync", "_populate_out_bind_vars"),
    ("vertebra.db.dbapi._sync", "_DBAPIOutBindVar"),
    ("vertebra.db.dbapi._sync", "SyncSQLDriverProtocol.execute_with_description"),
    ("vertebra.db.dbapi._sync", "SyncSQLDriverProtocol.execute_tuples"),
    ("vertebra.db.dbapi._sync", "SyncSQLDriverProtocol.select"),
    ("vertebra.db.dbapi._async", "AsyncSQLDriverProtocol.execute_with_description"),
    ("vertebra.db.dbapi._async", "AsyncSQLDriverProtocol.execute_tuples"),
    ("vertebra.db.dbapi._async", "AsyncSQLDriverProtocol.select"),
    ("vertebra.db.driver._common", "NATIVE_CURSOR_ENGINES"),
    ("vertebra.db.driver._common", "CommonDriverMixin._release_broker_lease"),
    ("vertebra.db.driver._common", "CommonDriverMixin._extract_column_names"),
    ("vertebra.db.driver._sync", "SyncDatabaseDriver.query_with_columns"),
    ("vertebra.db.driver._async", "AsyncDatabaseDriver.query_with_columns"),
    ("vertebra.db.driver._async", "ThreadOffloadAsyncDatabaseDriver.query_with_columns"),
    ("vertebra.db.adapters.postgres.core", "PostgreSQLCore.normalize_parameters"),
    ("vertebra.db.adapters.postgres._sync", "SyncPostgreSQLDriver._execute_cursor"),
    ("vertebra.db.adapters.postgres._sync", "SyncPostgreSQLDriver._execute_on_pool"),
    ("vertebra.db.adapters.postgres._async", "AsyncPostgreSQLDriver._execute_cursor"),
    ("vertebra.db.adapters.postgres._async", "AsyncPostgreSQLDriver._execute_on_pool"),
    ("vertebra.db.adapters.oracle._sync", "SyncOracleDriver._execute_cursor"),
    ("vertebra.db.adapters.oracle._async", "AsyncOracleDriver._execute_cursor"),
    ("vertebra.db.dbapi._sync", "SyncDBAPIConnection._native_cursor"),
    ("vertebra.db.dbapi._async", "AsyncDBAPIConnection._native_cursor"),
    ("vertebra.extensions.sqlspec._driver", "_unwrap_typed_parameters"),
    ("vertebra.extensions.sqlspec._driver", "_normalize_bound_params"),
)


@pytest.mark.parametrize(("module_name", "symbol_name"), _NATIVE_CURSOR_REMOVED_SYMBOLS)
def test_native_cursor_superseded_symbols_removed(module_name: str, symbol_name: str) -> None:
    """Verify superseded native-cursor helpers and protocol members stay removed."""
    if module_name.startswith("vertebra.extensions.sqlspec"):
        pytest.importorskip("sqlspec")
    module = importlib.import_module(module_name)
    if "." in symbol_name:
        owner_name, attr_name = symbol_name.split(".", 1)
        assert not hasattr(getattr(module, owner_name), attr_name)
    else:
        assert not hasattr(module, symbol_name)


@pytest.mark.anyio
async def test_sqlspec_bases_reuse_one_exception_handler() -> None:
    """Verify BaseSyncDriver and BaseAsyncDriver reuse a single exception handler instance across calls."""
    pytest.importorskip("sqlspec")
    from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver

    sync_native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    async_native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    try:
        sync_driver = BaseSyncDriver(sync_native)
        async_driver = BaseAsyncDriver(async_native)
        assert sync_driver.handle_database_exceptions() is sync_driver.handle_database_exceptions()
        assert async_driver.handle_database_exceptions() is async_driver.handle_database_exceptions()
    finally:
        sync_native.close()
        await async_native.close()


@pytest.mark.parametrize(
    ("dbapi_name", "native_name"),
    [
        ("Error", "DatabaseError"),
        ("DatabaseError", "DatabaseError"),
        ("InterfaceError", "InterfaceError"),
        ("DataError", "DataError"),
        ("OperationalError", "OperationalError"),
        ("IntegrityError", "IntegrityError"),
        ("InternalError", "InternalError"),
        ("ProgrammingError", "ProgrammingError"),
        ("NotSupportedError", "NotSupportedError"),
    ],
)
def test_dbapi_exception_names_are_native_classes_after_cleanup(dbapi_name: str, native_name: str) -> None:
    """Verify each PEP 249 error name on vertebra.db.dbapi binds directly to the native vertebra._vertebra class."""
    import vertebra._vertebra as native_mod
    import vertebra.db.dbapi as dbapi_mod

    assert getattr(dbapi_mod, dbapi_name) is getattr(native_mod, native_name)


@pytest.mark.anyio
async def test_capability_protocols_static_subtyping_and_end_to_end_behavior() -> None:
    """Verify static structural typing and end-to-end execution across all capability protocols."""
    sync_capable: SyncPipelineCapable = _ConformingSyncPipelineDriver()
    async_capable: AsyncPipelineCapable = _ConformingAsyncPipelineDriver()
    async_notif_source: AsyncNotificationSource = _ConformingNotificationSource()
    sync_notif_source: SyncNotificationSource = _ConformingSyncNotificationSource()
    sync_cdc: SyncChangeSource = _ConformingSyncChangeSource()
    async_cdc: AsyncChangeSource = _ConformingAsyncChangeSource()
    sync_queue_store: SyncQueueStore = _ConformingSyncQueueStore()
    async_queue_store: AsyncQueueStore = _ConformingAsyncQueueStore()
    pushdown: EmbeddingPushdown = _ConformingEmbeddingPushdown()

    raw_sync_pipe = sync_capable.pipeline()
    assert isinstance(raw_sync_pipe, SyncPipeline)
    assert not isinstance(raw_sync_pipe, AsyncPipeline)
    with raw_sync_pipe as sync_pipe:
        sync_pipe.execute("INSERT INTO items VALUES (1)")
        sync_pipe.select("SELECT 1")
        assert len(sync_pipe.results()) == 2

    raw_async_pipe = async_capable.pipeline()
    assert isinstance(raw_async_pipe, AsyncPipeline)
    assert not isinstance(raw_async_pipe, SyncPipeline)
    async with raw_async_pipe as async_pipe:
        async_pipe.execute("INSERT INTO items VALUES (2)")
        async_pipe.select("SELECT 2")
        assert len(await async_pipe.results()) == 2

    await async_notif_source.subscribe(["events"])
    await async_notif_source.publish("events", "hello")
    received = [evt async for evt in async_notif_source.listen()]
    assert received == [NotificationEvent(channel="events", payload=b"hello", sender_id="standin")]
    await async_notif_source.unsubscribe(["events"])

    sync_notif_source.subscribe(["events"])
    sync_notif_source.publish("events", "hello-sync")
    sync_received = list(sync_notif_source.listen())
    assert sync_received == [NotificationEvent(channel="events", payload=b"hello-sync", sender_id="sync-standin")]
    sync_notif_source.unsubscribe(["events"])

    sync_events = list(sync_cdc.stream())
    assert len(sync_events) == 1 and sync_events[0].operation == "INSERT"
    async_events = [evt async for evt in async_cdc.stream()]
    assert len(async_events) == 1 and async_events[0].operation == "UPDATE"

    sync_queue_store.ensure_schema()
    s_jid = sync_queue_store.enqueue(
        QueueEntry(job_id="", queue="default", task_name="work", payload=b"body", headers={"k": "v"})
    )
    s_bulk_ids = sync_queue_store.enqueue_many(
        [QueueEntry(job_id="", queue="default", task_name="work2", payload=b"b2")]
    )
    assert len(s_bulk_ids) == 1
    s_claimed = sync_queue_store.claim(["default"], limit=2, lease_seconds=15.0, worker_id="w1")
    assert len(s_claimed) == 2 and s_claimed[0].job_id == s_jid
    assert sync_queue_store.heartbeat([s_jid], worker_id="w1", lease_seconds=30.0) == 1
    assert sync_queue_store.ack(s_jid, attempt=1) is True
    assert sync_queue_store.nack(s_bulk_ids[0], attempt=1, error="boom", retry_delay_s=0.5) is True

    await async_queue_store.ensure_schema()
    a_jid = await async_queue_store.enqueue(
        QueueEntry(job_id="", queue="default", task_name="work", payload=b"body", headers={"k": "v"})
    )
    a_bulk_ids = await async_queue_store.enqueue_many(
        [QueueEntry(job_id="", queue="default", task_name="work2", payload=b"b2")]
    )
    assert len(a_bulk_ids) == 1
    a_claimed = await async_queue_store.claim(["default"], limit=2, lease_seconds=15.0, worker_id="w1")
    assert len(a_claimed) == 2 and a_claimed[0].job_id == a_jid
    assert await async_queue_store.heartbeat([a_jid], worker_id="w1", lease_seconds=30.0) == 1
    assert await async_queue_store.ack(a_jid, attempt=1) is True
    assert await async_queue_store.nack(a_bulk_ids[0], attempt=1, error="boom", retry_delay_s=0.5) is True

    batch_vec = await pushdown.embed_documents(["doc-a", "doc-b"], dimensions=4)
    assert batch_vec.count == 2 and batch_vec.dimensions == 4
    query_vec = await pushdown.embed_query("find docs", dimensions=4)
    assert query_vec.dimensions == 4
    hits = await pushdown.similarity_search(query_vec, table="docs", top_k=5)
    assert hits[0]["table"] == "docs" and hits[0]["dimensions"] == 4


@pytest.mark.parametrize("symbol_name", _CAPABILITY_EXPORT_NAMES)
def test_capability_exports_on_driver_and_lazy_db_root_with_zero_upward_imports(symbol_name: str) -> None:
    """Verify capability symbols export on db.driver and lazy db root without upward imports."""
    cap_mod = importlib.import_module("vertebra.db.driver.capabilities")
    assert symbol_name in cap_mod.__all__
    assert symbol_name in vertebra_db_driver.__all__
    assert symbol_name in vertebra_db.__all__
    assert getattr(vertebra_db_driver, symbol_name) is getattr(cap_mod, symbol_name)
    assert getattr(vertebra_db, symbol_name) is getattr(cap_mod, symbol_name)

    repo_root = Path(__file__).resolve().parents[5]
    cap_path = repo_root / "src/py/vertebra/db/driver/capabilities.py"
    tree = ast.parse(cap_path.read_text(encoding="utf-8"), filename=str(cap_path))
    imported_targets: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_targets.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_targets.append(node.module)
    forbidden_prefixes = ("vertebra.embeddings", "vertebra.framework", "vertebra.agent", "vertebra.server")
    assert not any(
        mod == prefix or mod.startswith(f"{prefix}.") for mod in imported_targets for prefix in forbidden_prefixes
    )

    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db"
        or name.startswith("vertebra.db.")
        or name == "vertebra.embeddings"
        or name.startswith("vertebra.embeddings.")
    }
    try:
        fresh_db = importlib.import_module("vertebra.db")
        assert "vertebra.db.driver.capabilities" not in sys.modules
        resolved = getattr(fresh_db, symbol_name)
        assert resolved is not None
        assert "vertebra.db.driver.capabilities" in sys.modules
        assert not any(name == "vertebra.embeddings" or name.startswith("vertebra.embeddings.") for name in sys.modules)
    finally:
        for name in list(sys.modules):
            if (
                name == "vertebra.db"
                or name.startswith("vertebra.db.")
                or name == "vertebra.embeddings"
                or name.startswith("vertebra.embeddings.")
            ):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        if "vertebra.db" in saved:
            vertebra.__dict__["db"] = saved["vertebra.db"]


_TEMPORARY_UPWARD_IMPORT_ALLOWLIST: frozenset[str] = frozenset()


@pytest.mark.parametrize(
    ("url", "expected_cls"),
    [
        ("postgresql://localhost:5432/appdb", PostgreSQLConfig),
        ("postgres://localhost:5432/appdb", PostgreSQLConfig),
        ("postgresql+vtb://localhost:5432/appdb", PostgreSQLConfig),
        ("mysql://localhost:3306/appdb", MySQLConfig),
        ("mariadb://localhost:3306/appdb", MySQLConfig),
        ("sqlite:///tmp.db", SQLiteConfig),
        ("oracle://scott:tiger@localhost:1521/FREEPDB1", OracleConfig),
        ("mssql://sa:secret@localhost:1433/master", MSSQLConfig),
        ("sqlserver://sa:secret@localhost:1433/master", MSSQLConfig),
        ("duckdb:///tmp.duckdb", DuckDBConfig),
        ("bigquery://proj/ds", BigQueryConfig),
        ("bq://proj/ds", BigQueryConfig),
        ("bigquery+vtb://proj/ds", BigQueryConfig),
        ("spanner://projects/p/instances/i/databases/d", SpannerConfig),
        ("spangres://projects/p/instances/i/databases/d", SpannerConfig),
        ("spanner+googlesql://projects/p/instances/i/databases/d", SpannerConfig),
        ("spanner+spangres://projects/p/instances/i/databases/d", SpannerConfig),
        ("spanner+vtb://projects/p/instances/i/databases/d", SpannerConfig),
        ("spangres+vertebra://projects/p/instances/i/databases/d", SpannerConfig),
        ("redis://localhost:6379/0", RedisConfig),
        ("rediss://localhost:6379/0", RedisConfig),
        ("valkey://localhost:6379/0", ValkeyConfig),
        ("valkeys://localhost:6379/0", ValkeyConfig),
        ("datafusion://in_process?target_partitions=4", DataFusionConfig),
        ("cloudsql://proj:us-central1:inst", CloudSQLConfig),
        ("alloydb:///projects/p/locations/l/clusters/c/instances/i", AlloyDBConfig),
    ],
)
def test_polymorphic_registry_parse_url_roundtrip(url: str, expected_cls: type[Any]) -> None:
    """Verify every supported database, cache, analytics, and dialer URL scheme round-trips via from_url."""
    parsed = DatabaseRegistry.parse_url(url)
    assert isinstance(parsed, expected_cls)
    assert parsed == expected_cls.from_url(url)


def test_registry_parse_url_rejects_memory_scheme() -> None:
    """DatabaseRegistry.parse_url rejects the memory:// scheme."""
    with pytest.raises(ValueError, match="Unsupported database URL scheme: 'memory'"):
        DatabaseRegistry.parse_url("memory://")


def test_registry_parse_url_executes_zero_eager_driver_imports() -> None:
    """Verify importing DatabaseRegistry and calling parse_url imports zero adapter driver modules."""
    sample_urls = (
        "postgresql://localhost:5432/appdb",
        "mysql://localhost:3306/appdb",
        "sqlite:///tmp.db",
        "oracle://scott:tiger@localhost:1521/FREEPDB1",
        "mssql://sa:secret@localhost:1433/master",
        "duckdb:///tmp.duckdb",
        "bigquery://proj/ds",
        "spanner://projects/p/instances/i/databases/d",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "datafusion://in_process",
        "cloudsql://proj:us-central1:inst",
        "alloydb:///projects/p/locations/l/clusters/c/instances/i",
    )
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db" or name.startswith("vertebra.db.")
    }
    reset_dependency_cache()
    try:
        reg_mod = importlib.import_module("vertebra.db.registry")
        driver_mods_on_import = [
            name for name in sys.modules if name.startswith("vertebra.db.adapters.") and name.endswith(".driver")
        ]
        assert driver_mods_on_import == []

        parsed_configs = [reg_mod.DatabaseRegistry.parse_url(u) for u in sample_urls]
        driver_mods_after_parse = [
            name for name in sys.modules if name.startswith("vertebra.db.adapters.") and name.endswith(".driver")
        ]
        assert driver_mods_after_parse == []

        sqlite_cfg = parsed_configs[2]
        sync_cls, async_cls = sqlite_cfg.resolve_driver_classes()
        assert sync_cls.__name__ == "SyncSQLiteDriver"
        assert async_cls.__name__ == "AsyncSQLiteDriver"
        assert "vertebra.db.adapters.sqlite.driver" in sys.modules
        assert "vertebra.db.adapters.postgres.driver" not in sys.modules
        assert "vertebra.db.adapters.bigquery.driver" not in sys.modules
    finally:
        for name in list(sys.modules):
            if name == "vertebra.db" or name.startswith("vertebra.db."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        if "vertebra.db" in saved:
            vertebra.__dict__["db"] = saved["vertebra.db"]
        reset_dependency_cache()


@pytest.mark.anyio
async def test_pool_key_memory_isolation_and_provide_driver_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pool_key returns None for isolated memory DBs and provide_driver replaces GCP-only helpers."""
    monkeypatch.chdir(tmp_path)
    assert SQLiteConfig(path=":memory:").pool_key() is None
    assert SQLiteConfig(path="file:memdb1?mode=memory").pool_key() is None
    assert SQLiteConfig(path="file:memdb1?mode=memory&cache=shared").pool_key() is not None
    file_db = tmp_path / "shared_pool.db"
    assert SQLiteConfig(path=str(file_db)).pool_key() is not None

    assert DuckDBConfig(database=":memory:").pool_key() is None
    assert DuckDBConfig(path="/tmp/x.duckdb").pool_key() == ("duckdb", "/tmp/x.duckdb", False)

    assert RedisConfig(url="redis://localhost:6379/0").pool_key() == ("redis", "redis://localhost:6379/0")
    assert ValkeyConfig(url="valkey://localhost:6379/0").pool_key() == ("valkey", "valkey://localhost:6379/0")
    assert DataFusionConfig(target_partitions=2).pool_key() is not None
    assert CloudSQLConfig(instance_connection_name="p:r:i").pool_key() == ("cloudsql", "p:r:i")
    assert AlloyDBConfig(instance_uri="/projects/p/locations/l/clusters/c/instances/i").pool_key() == (
        "alloydb",
        "/projects/p/locations/l/clusters/c/instances/i",
    )

    assert DatabaseRegistry.pool_fingerprint("sqlite:///:memory:") is None
    assert DatabaseRegistry.pool_fingerprint("duckdb:///:memory:") is None

    reg = DatabaseRegistry()
    m1: SyncSQLiteDriver | None = None
    m2: SyncSQLiteDriver | None = None
    am1: AsyncSQLiteDriver | None = None
    am2: AsyncSQLiteDriver | None = None
    s1: SyncSQLiteDriver | None = None
    sm1: SyncSQLiteDriver | None = None
    named_sync: SyncSQLiteDriver | None = None
    named_async: AsyncSQLiteDriver | None = None
    vk_sync: SyncValkeyDriver | None = None
    vk_async: AsyncValkeyDriver | None = None
    try:
        m1 = provide_driver("sqlite:///:memory:", sync=True, registry=reg)
        m2 = provide_driver("sqlite:///:memory:", sync=True, registry=reg)
        assert isinstance(m1, SyncSQLiteDriver)
        assert isinstance(m2, SyncSQLiteDriver)
        assert m1 is not m2
        m1.execute("CREATE TABLE iso_check (id INTEGER PRIMARY KEY)")
        with pytest.raises((OperationalError, ProgrammingError)):
            m2.select("SELECT id FROM iso_check")

        am1 = provide_driver("sqlite:///:memory:", sync=False, registry=reg)
        am2 = provide_driver("sqlite:///:memory:", sync=False, registry=reg)
        assert isinstance(am1, AsyncSQLiteDriver)
        assert isinstance(am2, AsyncSQLiteDriver)
        assert am1 is not am2

        s1 = provide_driver(SQLiteConfig(path=str(file_db)), sync=True, registry=reg)
        s2 = provide_driver(SQLiteConfig(path=str(file_db)), sync=True, registry=reg)
        assert s1 is s2

        shared_mem_cfg = SQLiteConfig(path="file:memdb_shared_t06?mode=memory&cache=shared")
        sm1 = provide_driver(shared_mem_cfg, sync=True, registry=reg)
        sm2 = provide_driver(shared_mem_cfg, sync=True, registry=reg)
        assert sm1 is sm2

        reg.register("sqlite_named", SQLiteConfig(path=str(tmp_path / "named.db")))
        named_sync = provide_driver("sqlite_named", sync=True, registry=reg)
        named_async = provide_driver("sqlite_named", sync=False, registry=reg)
        assert isinstance(named_sync, SyncSQLiteDriver)
        assert isinstance(named_async, AsyncSQLiteDriver)
        assert provide_driver("sqlite_named", sync=True, registry=reg) is named_sync
        assert provide_driver("sqlite_named", sync=False, registry=reg) is named_async

        df_cfg = DataFusionConfig(target_partitions=2)
        df_sync = provide_driver(df_cfg, sync=True, registry=reg)
        df_async = provide_driver(df_cfg, sync=False, registry=reg)
        assert isinstance(df_sync, DataFusionEngine)
        assert isinstance(df_async, DataFusionEngine)
        assert provide_driver("datafusion://in_process?target_partitions=2", sync=True, registry=reg) is df_sync

        vk_sync = provide_driver("valkey://localhost:6379/0", sync=True, registry=reg)
        vk_async = provide_driver("valkey://localhost:6379/0", sync=False, registry=reg)
        assert isinstance(vk_sync, SyncValkeyDriver)
        assert isinstance(vk_async, AsyncValkeyDriver)

        reg_mod = importlib.import_module("vertebra.db.registry")
        assert not hasattr(vertebra_db, "provide_bigquery_driver")
        assert not hasattr(vertebra_db, "provide_spanner_driver")
        assert not hasattr(reg_mod, "provide_bigquery_driver")
        assert not hasattr(reg_mod, "provide_spanner_driver")
    finally:
        for sync_drv in (m1, m2, s1, sm1, named_sync, vk_sync):
            if sync_drv is not None:
                sync_drv.close()
        for async_drv in (am1, am2, named_async, vk_async):
            if async_drv is not None:
                await async_drv.close()


def test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error() -> None:
    """Verify get_dialer caches sync/async dialers separately, wires PG/MySQL/MSSQL, and provide_driver rejects dialers."""
    reg = DatabaseRegistry()
    csql_cfg = CloudSQLConfig(instance_connection_name="proj:us-central1:inst")
    alloy_cfg = AlloyDBConfig(instance_uri="/projects/p/locations/us-central1/clusters/c/instances/i")

    sync_csql = reg.get_dialer(csql_cfg, sync=True)
    async_csql = reg.get_dialer(csql_cfg, sync=False)
    assert isinstance(sync_csql, SyncCloudSQLDialer)
    assert isinstance(async_csql, AsyncCloudSQLDialer)
    assert reg.get_dialer(csql_cfg, sync=True) is sync_csql
    assert reg.get_dialer(csql_cfg) is async_csql
    assert cast("object", sync_csql) is not cast("object", async_csql)

    sync_alloy = reg.get_dialer(alloy_cfg, sync=True)
    async_alloy = reg.get_dialer(alloy_cfg, sync=False)
    assert isinstance(sync_alloy, SyncAlloyDBDialer)
    assert isinstance(async_alloy, AsyncAlloyDBDialer)
    assert reg.get_dialer(alloy_cfg, sync=True) is sync_alloy
    assert reg.get_dialer(alloy_cfg) is async_alloy
    assert cast("object", sync_alloy) is not cast("object", async_alloy)

    assert reg.get_shared_driver(csql_cfg) is async_csql
    assert reg.get_shared_sync_driver(csql_cfg) is sync_csql
    assert reg.get_shared_driver(alloy_cfg) is async_alloy
    assert reg.get_shared_sync_driver(alloy_cfg) is sync_alloy

    pg_cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/appdb", cloud_sql_instance="proj:us-central1:inst")
    my_cfg = MySQLConfig(dsn="mysql://localhost:3306/appdb", cloud_sql_instance="proj:us-central1:inst")
    ms_cfg = MSSQLConfig(host="localhost", port=1433, database="master", cloud_sql_instance="proj:us-central1:inst")
    assert (
        MSSQLConfig.from_url(
            "mssql://localhost:1433/master?cloud_sql_instance=proj:us-central1:inst"
        ).cloud_sql_instance
        == "proj:us-central1:inst"
    )
    assert (
        MSSQLConfig.from_url("mssql://localhost:1433/master?cloudsql_instance=proj:us-central1:inst").cloud_sql_instance
        == "proj:us-central1:inst"
    )

    pg_sync = cast("SyncPostgreSQLDriver", reg.get_shared_sync_driver(pg_cfg))
    pg_async = cast("AsyncPostgreSQLDriver", reg.get_shared_driver(pg_cfg))
    my_sync = cast("SyncMySQLDriver", reg.get_shared_sync_driver(my_cfg))
    my_async = cast("AsyncMySQLDriver", reg.get_shared_driver(my_cfg))
    ms_sync = cast("SyncMSSQLDriver", reg.get_shared_sync_driver(ms_cfg))
    ms_async = cast("AsyncMSSQLDriver", reg.get_shared_driver(ms_cfg))

    assert pg_sync.dialer is sync_csql
    assert pg_async.dialer is async_csql
    assert my_sync.dialer is sync_csql
    assert my_async.dialer is async_csql
    assert ms_sync.dialer is sync_csql
    assert ms_async.dialer is async_csql

    reg.register("dialer_only", csql_cfg)
    bad_targets: tuple[Any, ...] = (
        csql_cfg,
        alloy_cfg,
        "cloudsql://proj:us-central1:inst",
        "alloydb:///projects/p/locations/us-central1/clusters/c/instances/i",
        "dialer_only",
    )
    for bad_target in bad_targets:
        with pytest.raises(TypeError, match="get_dialer"):
            provide_driver(bad_target, sync=False, registry=reg)
        with pytest.raises(TypeError, match="get_dialer"):
            provide_driver(bad_target, sync=True, registry=reg)


def _collect_ast_upward_import_violations(
    tree: ast.AST,
    pkg_parts: list[str],
    forbidden_prefixes: tuple[str, ...],
) -> list[str]:
    """Collect static and dynamic upward module targets inside `tree` matching `forbidden_prefixes`."""
    dynamic_import_callees = frozenset({"import_optional", "ensure_module", "import_string", "import_module"})
    violations: list[str] = []
    for node in ast.walk(tree):
        targets: list[str] = []
        if isinstance(node, ast.Import):
            targets.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                targets.append(node.module)
            elif node.level > 0:
                anchor = pkg_parts[: len(pkg_parts) - (node.level - 1)]
                base_mod = ".".join([*anchor, node.module] if node.module else anchor)
                targets.append(base_mod)
                for alias in node.names:
                    if alias.name != "*":
                        targets.append(f"{base_mod}.{alias.name}" if base_mod else alias.name)
        elif isinstance(node, ast.Call):
            callee = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            )
            if callee in dynamic_import_callees:
                arg_expr: ast.expr | None = node.args[0] if node.args else None
                if arg_expr is None:
                    for kw in node.keywords:
                        if kw.arg in {"module_name", "name"}:
                            arg_expr = kw.value
                            break
                if isinstance(arg_expr, ast.Constant) and isinstance(arg_expr.value, str):
                    raw_target = arg_expr.value
                    if raw_target.startswith("vertebra."):
                        targets.append(raw_target)
                    elif raw_target.startswith("."):
                        dots = len(raw_target) - len(raw_target.lstrip("."))
                        rest = raw_target.lstrip(".")
                        anchor = pkg_parts[: max(0, len(pkg_parts) - (dots - 1))]
                        resolved = ".".join([*anchor, rest] if rest else anchor)
                        targets.append(resolved)
        for target in targets:
            if any(target == prefix or target.startswith(f"{prefix}.") for prefix in forbidden_prefixes):
                violations.append(target)
    return sorted(set(violations))


def test_vertebra_db_ast_upward_import_boundary_gate() -> None:
    """Enforce zero upward imports from vertebra.db into agent, framework, extensions, server, or embeddings."""
    db_root = Path(vertebra_db.__file__).resolve().parent
    src_root = db_root.parent.parent
    forbidden_prefixes = (
        "vertebra.agent",
        "vertebra.framework",
        "vertebra.extensions",
        "vertebra.server",
        "vertebra.embeddings",
    )
    assert _collect_ast_upward_import_violations(
        ast.parse('import_optional("vertebra.extensions.foo")'),
        ["vertebra", "db"],
        forbidden_prefixes,
    ) == ["vertebra.extensions.foo"]
    assert _collect_ast_upward_import_violations(
        ast.parse('ensure_module(module_name="vertebra.server.app")'),
        ["vertebra", "db"],
        forbidden_prefixes,
    ) == ["vertebra.server.app"]

    upward_imports_by_module: dict[str, list[str]] = {}

    for py_file in sorted(db_root.rglob("*.py")):
        rel_parts = list(py_file.relative_to(src_root).with_suffix("").parts)
        if rel_parts[-1] == "__init__":
            pkg_parts = rel_parts[:-1]
            module_name = ".".join(pkg_parts)
        else:
            pkg_parts = rel_parts[:-1]
            module_name = ".".join(rel_parts)

        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        violations = _collect_ast_upward_import_violations(tree, pkg_parts, forbidden_prefixes)
        if violations:
            upward_imports_by_module[module_name] = violations

    assert len(_TEMPORARY_UPWARD_IMPORT_ALLOWLIST) == 0
    unexpected = {
        mod: imports
        for mod, imports in upward_imports_by_module.items()
        if mod not in _TEMPORARY_UPWARD_IMPORT_ALLOWLIST
    }
    assert unexpected == {}

    stale_allowlist = _TEMPORARY_UPWARD_IMPORT_ALLOWLIST - set(upward_imports_by_module)
    assert stale_allowlist == set()


def _engine_purity_violations(source: str, *, registry: bool = False) -> list[str]:
    """Find adapter-bound execution and engine SQL outside declared generic seams."""
    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    registered: set[str] = set()
    allowed_nodes: set[ast.AST] = set()
    annotations: set[ast.AST] = set()
    checking: set[ast.AST] = set()
    docstrings: set[ast.AST] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
        ):
            docstrings.add(node.body[0].value)
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
            checking.update(child for statement in node.body for child in ast.walk(statement))
        annotation = None
        if isinstance(node, (ast.AnnAssign, ast.arg)):
            annotation = node.annotation
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            annotation = node.returns
        elif isinstance(node, ast.TypeAlias):
            annotation = node.value
        if annotation is not None:
            annotations.update(ast.walk(annotation))
        if node in tree.body and isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                isinstance(target, ast.Name) and target.id == "REGISTERED_CONFIG_TYPES" for target in targets
            ) and isinstance(node.value, ast.Tuple):
                for item in node.value.elts:
                    if isinstance(item, ast.Name):
                        registered.add(item.id)
                        allowed_nodes.add(item)
    violations: list[str] = []
    bindings: set[str] = set()
    sql_functions = re.compile(
        r"\b(?:pg_notify|pg_logical_emit_message|set_config)\s*\("
        r"|\b(?:DBMS_AQ|DBMS_SESSION|DBMS_VECTOR)\s*\."
        r"|\bML\s*\.\s*(?:GENERATE_EMBEDDING|PREDICT)\s*\(",
        re.IGNORECASE,
    )
    for node in ast.walk(tree):
        module = node.module if isinstance(node, ast.ImportFrom) else None
        if isinstance(node, ast.ImportFrom) and node.level and module and module.split(".")[0] == "adapters":
            module = f"vertebra.db.{module}"
        if isinstance(node, ast.ImportFrom) and module and module.startswith("vertebra.db.adapters"):
            for imported in node.names:
                name = imported.asname or imported.name
                bindings.add(name)
                config_registration = (
                    registry
                    and re.fullmatch(r"vertebra\.db\.adapters\.[^.]+\.config", module)
                    and parents.get(node) is tree
                    and name in registered
                )
                driver_annotation = (
                    registry and re.fullmatch(r"vertebra\.db\.adapters\.[^.]+\.driver", module) and node in checking
                )
                if not (config_registration or driver_annotation):
                    violations.append(f"line {node.lineno}: forbidden adapter import {node.module}.{imported.name}")
        elif isinstance(node, ast.Import):
            for imported in node.names:
                if imported.name.startswith("vertebra.db.adapters"):
                    violations.append(f"line {node.lineno}: forbidden adapter import {imported.name}")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node not in docstrings
            and node not in annotations
        ):
            if sql_functions.search(node.value):
                violations.append(f"line {node.lineno}: engine SQL construction")
        elif isinstance(node, ast.Call):
            function = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
            if function in {"import_module", "__import__", "ensure_module", "import_optional"}:
                arguments = [*node.args, *(keyword.value for keyword in node.keywords)]
                if any(
                    isinstance(argument, ast.Constant)
                    and isinstance(argument.value, str)
                    and argument.value.startswith("vertebra.db.adapters")
                    for argument in arguments
                ):
                    violations.append(f"line {node.lineno}: runtime adapter loading")
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in bindings:
            parent = parents.get(node)
            construction = isinstance(parent, ast.Call) and parent.func is node
            if construction or (node not in annotations and node not in allowed_nodes):
                violations.append(f"line {node.lineno}: executable adapter binding {node.id}")
    return violations


def test_generic_db_engine_purity() -> None:
    """Generic database modules retain declarative registration and protocol-only execution."""
    root = Path(vertebra_db.__file__).parent
    failures = {
        str(path.relative_to(root)): issues
        for path in [*root.glob("*.py"), *(root / "driver").glob("*.py")]
        if (issues := _engine_purity_violations(path.read_text(), registry=path.name == "registry.py"))
    }
    assert failures == {}


@pytest.mark.parametrize(
    "source,registry",
    [
        ("from vertebra.db.adapters.postgres.driver import SyncPostgreSQLDriver", True),
        ("from vertebra.db.adapters.postgres.config import PostgreSQLConfig", False),
        ("from vertebra.db.adapters.postgres.config import PostgreSQLConfig\nREGISTERED_CONFIG_TYPES = ()", True),
        (
            "from vertebra.db.adapters.postgres.config import PostgreSQLConfig as Pg\nREGISTERED_CONFIG_TYPES = (Pg,)\nif isinstance(config, Pg):\n    pass",
            True,
        ),
        (
            "from vertebra.db.adapters.postgres.config import PostgreSQLConfig as Pg\nREGISTERED_CONFIG_TYPES = (Pg,)\nif type(config) is Pg:\n    pass",
            True,
        ),
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from vertebra.db.adapters.postgres.driver import SyncPostgreSQLDriver as Pg\ndef make():\n    return Pg()",
            True,
        ),
        ('def build():\n    return "SELECT set_config($1, $2, $3)"', False),
        ('def load():\n    return ensure_module("vertebra.db.adapters.postgres.driver")', False),
    ],
)
def test_engine_purity_rejects_adapter_execution(source: str, registry: bool) -> None:
    """Representative import, alias, dispatch, construction, and SQL leaks are rejected."""
    assert _engine_purity_violations(source, registry=registry)


def test_engine_purity_accepts_annotations_metadata_and_polymorphic_loading() -> None:
    """Annotations, broker metadata, docstrings, and polymorphic config loaders remain valid."""
    assert not _engine_purity_violations(
        '''
"""Engine SQL documentation mentions set_config(value)."""
from typing import TYPE_CHECKING
from vertebra.db.adapters.postgres.config import PostgreSQLConfig as Pg
if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.driver import SyncPostgreSQLDriver as Driver
type Config = Pg
type Native = Driver
REGISTERED_CONFIG_TYPES = (Pg,)
SCHEME_REGISTRY = {scheme: config for config in REGISTERED_CONFIG_TYPES for scheme in config.url_schemes}
POSTGRES_DIALECT_TAG = 1
def create(config: Pg) -> Driver:
    return config.build_sync_driver()
def load(self):
    return ensure_module(self.driver_module)
''',
        registry=True,
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_notify($1, $2)",
        "SELECT pg_logical_emit_message($1, $2, $3)",
        "SELECT set_config($1, $2, $3)",
        "BEGIN DBMS_AQ.ENQUEUE(...); END;",
        "BEGIN DBMS_SESSION.SET_CONTEXT(...); END;",
        "SELECT DBMS_VECTOR.UTL_TO_EMBEDDING(:value) FROM DUAL",
        "SELECT ML.GENERATE_EMBEDDING(MODEL endpoint)",
        "SELECT ML.PREDICT(MODEL endpoint)",
    ],
)
def test_engine_purity_rejects_engine_sql_construction(sql: str) -> None:
    """Every banned engine SQL family is rejected from executable string construction."""
    assert _engine_purity_violations(f"def build():\n    return {sql!r}")


def test_engine_purity_rejects_relative_adapter_import() -> None:
    """Relative imports cannot bypass the same generic-database boundary."""
    assert _engine_purity_violations("from .adapters.postgres.config import PostgreSQLConfig")
