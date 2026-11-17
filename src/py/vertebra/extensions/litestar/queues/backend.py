"""Store-backed queue backend and container dependency provider for litestar-queues."""

from __future__ import annotations

import asyncio
import contextlib
import inspect as _inspect
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable
from uuid import UUID, uuid4

import msgspec

from vertebra.db.driver.capabilities import (
    AsyncNotificationSource,
    AsyncQueueStore,
    QueueEntry,
    QueueReservation,
)
from vertebra.extensions.litestar.queues.shm_backend import (
    SHMExecutionBackend,
    SHMQueueBackend,
    SHMQueueBackendConfig,
)
from vertebra.framework.di import inspect_callable_bindings
from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, encode_json_bytes

try:
    from litestar_queues.backends.base import (
        BaseQueueBackend,
        is_external_dispatch_reservation,
        record_matches_filters,
    )
    from litestar_queues.backends.factory import queue_backend
    from litestar_queues.models import (
        HeartbeatTouch,
        HeartbeatTouchResult,
        QueueBackendCapabilities,
        QueuedTaskRecord,
        QueueStatistics,
        StaleTaskRecoveryResult,
        TaskRequest,
        TaskReservation,
        TaskStatus,
    )
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-queues",
        install_package="litestar-queues",
        extra="queues",
    ) from exc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence

    from litestar_queues.config import QueueConfig

    from vertebra.framework.di import Container

__all__ = (
    "ContainerTaskDependencyProvider",
    "SHMExecutionBackend",
    "SHMQueueBackend",
    "SHMQueueBackendConfig",
    "StoreQueueBackend",
    "StoreQueueBackendConfig",
)


@runtime_checkable
class _ClaimJobStore(Protocol):
    async def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None: ...


@runtime_checkable
class _EntryStatusStore(Protocol):
    async def get_entry_status(self, job_id: str) -> str | None: ...


@runtime_checkable
class _SyncClosableStore(Protocol):
    def close(self) -> Any: ...


def _ensure_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        return _ensure_utc(datetime.fromisoformat(value))
    return None


def _resolve_record_status(entry: QueueEntry, store_status: str | None = None) -> TaskStatus:
    """Derive litestar-queues TaskStatus from QueueEntry lease/schedule fields and optional store status."""
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    if store_status is not None:
        normalized = store_status.strip().lower()
        if normalized in {"claimed", "running"}:
            return "running"
        if normalized in {"completed", "acked"}:
            return "completed"
        if normalized in {"dead_letter", "failed"}:
            return "failed"
        if normalized == "cancelled":
            return "cancelled"
        if normalized == "expired":
            return "expired"
        if entry.scheduled_at_ms is not None and entry.scheduled_at_ms > now_ms:
            return "scheduled"
        return "pending"
    if entry.completed_at_ms is not None:
        return "failed" if entry.error is not None else "completed"
    if entry.lease_until_ms > now_ms:
        return "running"
    if entry.scheduled_at_ms is not None and entry.scheduled_at_ms > now_ms:
        return "scheduled"
    return "pending"


def _entry_to_record(entry: QueueEntry, *, store_status: str | None = None) -> QueuedTaskRecord:
    """Decode a QueueEntry into a QueuedTaskRecord using authoritative QueueEntry attempt and schedule state."""
    sched = (
        datetime.fromtimestamp(entry.scheduled_at_ms / 1000.0, tz=UTC) if entry.scheduled_at_ms is not None else None
    )
    expires_at = (
        datetime.fromtimestamp(entry.expires_at_ms / 1000.0, tz=UTC) if entry.expires_at_ms is not None else None
    )
    completed_at = (
        datetime.fromtimestamp(entry.completed_at_ms / 1000.0, tz=UTC) if entry.completed_at_ms is not None else None
    )
    heartbeat_at = (
        datetime.fromtimestamp(entry.heartbeat_at_ms / 1000.0, tz=UTC) if entry.heartbeat_at_ms is not None else None
    )
    retry_count = max(0, entry.attempt - 1)
    max_retries = max(0, entry.max_attempts - 1)
    status = _resolve_record_status(entry, store_status)
    decoded_result: Any = decode_json(entry.result) if entry.result is not None else None
    if entry.payload:
        data = decode_json(entry.payload)
        if isinstance(data, dict):
            task_id = UUID(data["id"]) if "id" in data else UUID(entry.job_id)
            created_at = _parse_dt(data.get("created_at")) or datetime.now(UTC)
            queued_at = _parse_dt(data.get("queued_at")) or created_at
            return QueuedTaskRecord(
                task_name=data.get("task_name", entry.task_name),
                id=task_id,
                args=tuple(data.get("args", ())),
                kwargs=dict(data.get("kwargs", {})),
                queue=data.get("queue", entry.queue),
                execution_backend=data.get("execution_backend", "local"),
                execution_profile=data.get("execution_profile"),
                execution_ref=data.get("execution_ref"),
                worker_id=entry.worker_id or data.get("worker_id"),
                status=status,
                priority=entry.priority if entry.priority != 0 else int(data.get("priority", 0)),
                max_retries=max_retries,
                retry_count=retry_count,
                scheduled_at=sched if sched is not None else _parse_dt(data.get("scheduled_at")),
                expires_at=_parse_dt(data.get("expires_at")) or expires_at,
                created_at=created_at,
                queued_at=queued_at,
                completed_at=completed_at if completed_at is not None else _parse_dt(data.get("completed_at")),
                heartbeat_at=heartbeat_at,
                result=decoded_result if entry.result is not None else data.get("result"),
                error=entry.error if entry.error is not None else data.get("error"),
                key=data.get("key") or entry.headers.get("key"),
                metadata=dict(data.get("metadata", {})),
            )
    return QueuedTaskRecord(
        task_name=entry.task_name,
        id=UUID(entry.job_id),
        queue=entry.queue,
        worker_id=entry.worker_id,
        status=status,
        priority=entry.priority,
        max_retries=max_retries,
        retry_count=retry_count,
        scheduled_at=sched,
        expires_at=expires_at,
        completed_at=completed_at,
        heartbeat_at=heartbeat_at,
        result=decoded_result,
        error=entry.error,
        key=entry.headers.get("key"),
    )


def _build_record_and_entry(
    task_name: str,
    *,
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    queue: str = "default",
    priority: int = 0,
    max_retries: int = 0,
    scheduled_at: datetime | None = None,
    expires_at: datetime | None = None,
    key: str | None = None,
    execution_backend: str = "local",
    execution_profile: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    record_id: UUID | None = None,
    now: datetime | None = None,
) -> tuple[QueuedTaskRecord, QueueEntry]:
    """Construct a QueuedTaskRecord and matching QueueEntry from enqueue arguments."""
    current_time = now or datetime.now(UTC)
    norm_sched = _ensure_utc(scheduled_at) if scheduled_at is not None else None
    norm_expires = _ensure_utc(expires_at) if expires_at is not None else None
    status: TaskStatus = "scheduled" if norm_sched is not None and norm_sched > current_time else "pending"
    resolved_id = record_id or uuid4()
    resolved_kwargs = dict(kwargs or {})
    resolved_metadata = dict(metadata or {})
    record = QueuedTaskRecord(
        task_name=task_name,
        id=resolved_id,
        args=args,
        kwargs=resolved_kwargs,
        queue=queue,
        execution_backend=execution_backend,
        execution_profile=execution_profile,
        status=status,
        priority=priority,
        max_retries=max_retries,
        scheduled_at=norm_sched,
        expires_at=norm_expires,
        created_at=current_time,
        queued_at=current_time,
        key=key,
        metadata=resolved_metadata,
    )
    record_dict = {
        "id": str(record.id),
        "task_name": record.task_name,
        "args": list(record.args),
        "kwargs": record.kwargs,
        "queue": record.queue,
        "execution_backend": record.execution_backend,
        "execution_profile": record.execution_profile,
        "status": record.status,
        "priority": record.priority,
        "max_retries": record.max_retries,
        "retry_count": record.retry_count,
        "scheduled_at": record.scheduled_at.isoformat() if record.scheduled_at is not None else None,
        "expires_at": record.expires_at.isoformat() if record.expires_at is not None else None,
        "created_at": record.created_at.isoformat(),
        "queued_at": record.queued_at.isoformat(),
        "key": record.key,
        "metadata": record.metadata,
    }
    sched_ms = int(record.scheduled_at.timestamp() * 1000) if record.scheduled_at is not None else None
    expires_ms = int(record.expires_at.timestamp() * 1000) if record.expires_at is not None else None
    headers = {"key": key} if key is not None else {}
    entry = QueueEntry(
        job_id=str(record.id),
        queue=queue,
        task_name=task_name,
        payload=encode_json_bytes(record_dict),
        attempt=1,
        max_attempts=max_retries + 1,
        scheduled_at_ms=sched_ms,
        headers=headers,
        priority=priority,
        expires_at_ms=expires_ms,
    )
    return record, entry


def _reservation_to_task_reservation(reservation: QueueReservation) -> TaskReservation:
    """Convert an AsyncQueueStore QueueReservation into a litestar-queues TaskReservation."""
    return TaskReservation(
        key=reservation.key,
        task_id=UUID(reservation.job_id),
        task_name=reservation.task_name,
        created_at=datetime.fromtimestamp(reservation.created_at_ms / 1000.0, tz=UTC),
    )


class StoreQueueBackendConfig(msgspec.Struct, frozen=True):
    """Configuration for vertebra_store queue backend."""

    store: AsyncQueueStore
    """Underlying AsyncQueueStore used for durable task persistence."""

    notification_source: AsyncNotificationSource | None = None
    """Optional AsyncNotificationSource used for low-latency worker wakeups."""

    terminal_ttl: timedelta | None = None
    """Optional retention duration for terminal task records."""

    backend_name: ClassVar[str] = "vertebra_store"

    def create_backend(self, config: QueueConfig | None = None) -> StoreQueueBackend:
        """Create a configured StoreQueueBackend instance."""
        return StoreQueueBackend(
            config=config,
            backend_config=self,
            store=self.store,
            notification_source=self.notification_source,
            terminal_ttl=self.terminal_ttl,
        )


class StoreQueueBackend(BaseQueueBackend):
    """Engine-neutral queue backend adapting any AsyncQueueStore and notification source."""

    __slots__ = ("_notification_source", "_store", "_terminal_ttl", "_wakeup_subscribed")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        store: AsyncQueueStore | None = None,
        notification_source: AsyncNotificationSource | None = None,
        terminal_ttl: timedelta | None = None,
        backend_config: StoreQueueBackendConfig | None = None,
    ) -> None:
        """Initialize the store queue backend."""
        ensure_module(
            "litestar_queues",
            package="litestar-queues",
            extra="queues",
        )
        super().__init__(config=config)
        if backend_config is not None:
            store = backend_config.store
            notification_source = backend_config.notification_source
            terminal_ttl = backend_config.terminal_ttl
        if store is None:
            msg = "StoreQueueBackend requires an AsyncQueueStore instance"
            raise ImproperConfigurationError(msg)
        self._store = store
        self._notification_source = notification_source
        self._terminal_ttl = terminal_ttl
        self._wakeup_subscribed = False

    @property
    def store(self) -> AsyncQueueStore:
        """Return the underlying AsyncQueueStore."""
        return self._store

    @property
    def notification_source(self) -> AsyncNotificationSource | None:
        """Return the bound AsyncNotificationSource if configured."""
        return self._notification_source

    @property
    def terminal_ttl(self) -> timedelta | None:
        """Return the terminal retention duration."""
        return self._terminal_ttl

    @property
    def capabilities(self) -> QueueBackendCapabilities:
        """Return supported backend features."""
        return QueueBackendCapabilities(
            supports_worker_wakeups=self._notification_source is not None,
            supports_completion_events=False,
            supports_maintenance=True,
        )

    async def _get_entry_status(self, job_id: str) -> str | None:
        if isinstance(self._store, _EntryStatusStore):
            return await self._store.get_entry_status(job_id)
        return None

    async def enqueue(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 0,
        scheduled_at: datetime | None = None,
        expires_at: datetime | None = None,
        key: str | None = None,
        execution_backend: str = "local",
        execution_profile: str | None = None,
        metadata: dict[str, Any] | None = None,
        id: UUID | None = None,
    ) -> QueuedTaskRecord:
        """Persist a single queued task into the backing store."""
        record, entry = _build_record_and_entry(
            task_name,
            args=args,
            kwargs=kwargs,
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            key=key,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            metadata=metadata,
            record_id=id,
        )
        await self._store.enqueue(entry)
        if self._notification_source is not None:
            await self._notification_source.publish("vertebra_queue_wakeup", queue)
        return record

    async def enqueue_many(
        self,
        requests: Sequence[TaskRequest],
    ) -> list[QueuedTaskRecord]:
        """Bulk-enqueue task requests into the backing store."""
        if not requests:
            return []
        now = datetime.now(UTC)
        records: list[QueuedTaskRecord] = []
        entries: list[QueueEntry] = []
        for request in requests:
            record, entry = _build_record_and_entry(
                request.task_name,
                args=request.args,
                kwargs=request.kwargs,
                queue=request.queue,
                priority=request.priority,
                max_retries=request.max_retries,
                scheduled_at=request.scheduled_at,
                expires_at=request.expires_at,
                key=request.key,
                execution_backend=request.execution_backend,
                execution_profile=request.execution_profile,
                metadata=request.metadata,
                now=now,
            )
            records.append(record)
            entries.append(entry)
        await self._store.enqueue_many(entries)
        if self._notification_source is not None:
            for queue_name in dict.fromkeys(r.queue for r in records):
                await self._notification_source.publish("vertebra_queue_wakeup", queue_name)
        self._record_enqueue_batch(len(requests))
        return records

    async def assign_worker(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        expected_retry_count: int,
    ) -> QueuedTaskRecord | None:
        """Persist the active worker owner on a fenced running task attempt."""
        target_id = str(task_id)
        assigned = await self._store.assign_worker(
            target_id,
            worker_id=worker_id,
            attempt=expected_retry_count + 1,
        )
        if not assigned:
            return None
        entry = await self._store.get_entry(target_id)
        if entry is None:
            return None
        record = _entry_to_record(entry, store_status="claimed")
        if record.worker_id is None:
            record.worker_id = worker_id
        return record

    async def claim_many(
        self,
        *,
        limit: int,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
        queue_limits: Mapping[str, int] | None = None,
    ) -> list[QueuedTaskRecord]:
        """Claim ready tasks across specified queues or all queues when empty."""
        _ = execution_backend
        if limit <= 0:
            return []
        if queues:
            target_queues = queues
        else:
            ready = await self._store.list_entries(queue=None, status="ready", limit=max(limit * 10, 100))
            target_queues = tuple(dict.fromkeys(entry.queue for entry in ready))
        if queue_limits is not None:
            target_queues = tuple(q for q in target_queues if queue_limits.get(q, 1) > 0)
        if not target_queues:
            return []
        entries = await self._store.claim(
            queues=target_queues,
            limit=limit,
            worker_id="worker",
            lease_seconds=30.0,
        )
        return [_entry_to_record(e, store_status="claimed") for e in entries]

    async def claim_task(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int | None = None,
        expected_execution_ref: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Atomically claim and lease a specific due task by identifier."""
        _ = expected_execution_ref
        target_id = str(task_id)
        expected_attempt = expected_retry_count + 1 if expected_retry_count is not None else None
        if isinstance(self._store, _ClaimJobStore):
            claimed_entry = await self._store.claim_job(
                target_id,
                worker_id="worker",
                lease_seconds=30.0,
                expected_attempt=expected_attempt,
            )
            return _entry_to_record(claimed_entry, store_status="claimed") if claimed_entry is not None else None

        existing = await self._store.get_entry(target_id)
        if existing is None:
            return None
        if expected_attempt is not None and existing.attempt != expected_attempt:
            return None
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        if existing.scheduled_at_ms is not None and existing.scheduled_at_ms > now_ms:
            return None
        ready = await self._store.list_entries(existing.queue, status="ready", limit=100)
        target_idx = next((idx for idx, item in enumerate(ready) if item.job_id == target_id), None)
        if target_idx is None:
            return None
        claimed_batch = await self._store.claim(
            queues=(existing.queue,),
            limit=target_idx + 1,
            worker_id="worker",
            lease_seconds=30.0,
        )
        matched: QueueEntry | None = None
        for entry in claimed_batch:
            if entry.job_id == target_id:
                matched = entry
            else:
                await self._store.nack(
                    entry.job_id,
                    attempt=entry.attempt,
                    error="Requeued non-target batch item",
                    retry_delay_s=0.0,
                )
        return _entry_to_record(matched, store_status="claimed") if matched is not None else None

    async def claim_next(
        self,
        *,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Claim the next ready task in queue."""
        claimed = await self.claim_many(limit=1, queues=queues, execution_backend=execution_backend)
        return claimed[0] if claimed else None

    async def complete_task(
        self,
        task_id: UUID,
        *,
        result: Any = None,
        expected_retry_count: int | None = None,
    ) -> QueuedTaskRecord | None:
        """Acknowledge a completed task with attempt fencing."""
        target_id = str(task_id)
        if expected_retry_count is not None:
            attempt = expected_retry_count + 1
        else:
            current = await self._store.get_entry(target_id)
            attempt = current.attempt if current is not None else 1
        acked = await self._store.ack(target_id, attempt=attempt)
        if not acked:
            return None
        entry = await self._store.get_entry(target_id)
        if entry is None:
            return None
        record = _entry_to_record(entry, store_status="completed")
        if record.result is None and result is not None:
            record.result = result
        return record

    async def fail_task(
        self,
        task_id: UUID,
        error: str,
        *,
        retry: bool = True,
        expected_retry_count: int | None = None,
        retry_at: datetime | None = None,
        queued_at: datetime | None = None,
    ) -> QueuedTaskRecord | None:
        """Record a failed task execution and schedule retry or immediate dead-lettering."""
        _ = queued_at
        target_id = str(task_id)
        if expected_retry_count is not None:
            attempt = expected_retry_count + 1
        else:
            current = await self._store.get_entry(target_id)
            attempt = current.attempt if current is not None else 1

        if not retry:
            delay_s: float | None = None
        elif retry_at is not None:
            delay_s = max(0.0, (_ensure_utc(retry_at) - datetime.now(UTC)).total_seconds())
        else:
            delay_s = 0.0

        nacked = await self._store.nack(
            target_id,
            attempt=attempt,
            error=str(error or "Task execution failed"),
            retry_delay_s=delay_s,
        )
        if not nacked:
            return None
        return await self.get_task(task_id)

    async def touch_heartbeats(
        self,
        touches: Sequence[HeartbeatTouch],
        *,
        worker_id: str | None = None,
    ) -> HeartbeatTouchResult:
        """Extend visibility leases and heartbeat timestamps for in-flight tasks."""
        if not touches:
            return HeartbeatTouchResult()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        touched_ids: set[UUID] = set()
        missed_ids: set[UUID] = set()
        by_worker: dict[str, list[str]] = {}
        touched_job_ids: list[str] = []
        for touch in touches:
            target_id = str(touch.task_id)
            entry = await self._store.get_entry(target_id)
            if entry is None:
                missed_ids.add(touch.task_id)
                continue
            if touch.expected_retry_count is not None and entry.attempt != touch.expected_retry_count + 1:
                missed_ids.add(touch.task_id)
                continue
            store_status = await self._get_entry_status(target_id)
            if store_status is not None and store_status.strip().lower() not in {"claimed", "running"}:
                missed_ids.add(touch.task_id)
                continue
            touched_ids.add(touch.task_id)
            touched_job_ids.append(target_id)
            owner = entry.worker_id or worker_id or "worker"
            by_worker.setdefault(owner, []).append(target_id)
        if touched_job_ids:
            await self._store.touch_heartbeats(touched_job_ids, heartbeat_at_ms=now_ms)
            for owner, job_ids in by_worker.items():
                await self._store.heartbeat(job_ids, worker_id=owner, lease_seconds=30.0)
        return HeartbeatTouchResult(touched_task_ids=touched_ids, missed_task_ids=missed_ids)

    async def null_heartbeats(
        self,
        task_ids: Sequence[UUID],
        *,
        expected_retry_count: int | None = None,
    ) -> None:
        """Clear heartbeat timestamps for task IDs."""
        if not task_ids:
            return
        if expected_retry_count is None:
            await self._store.null_heartbeats([str(tid) for tid in task_ids])
            return
        expected_attempt = expected_retry_count + 1
        matching_ids: list[str] = []
        for tid in task_ids:
            entry = await self._store.get_entry(str(tid))
            if entry is not None and entry.attempt == expected_attempt:
                matching_ids.append(entry.job_id)
        if matching_ids:
            await self._store.null_heartbeats(matching_ids)

    async def requeue_stale_running(
        self,
        *,
        stale_after: timedelta = timedelta(seconds=300),
        limit: int | None = None,
    ) -> StaleTaskRecoveryResult:
        """Reap abandoned in-flight claims back into ready status."""
        _ = limit
        reaped = await self._store.reap_stale(lease_seconds=stale_after.total_seconds())
        return StaleTaskRecoveryResult(requeued=reaped)

    async def expire_overdue(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        """Transition overdue pending or scheduled records to expired."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        fetch_limit = max(limit or 1000, 1000)
        ready_entries = await self._store.list_entries(queue=None, status="ready", limit=fetch_limit)
        overdue_candidates = [
            entry for entry in ready_entries if entry.expires_at_ms is not None and entry.expires_at_ms <= now_ms
        ]
        overdue_candidates.sort(key=lambda entry: (entry.expires_at_ms or 0, entry.job_id))
        expired_count = await self._store.expire_overdue(now_ms=now_ms, limit=limit)
        if expired_count <= 0:
            return []
        expired_records: list[QueuedTaskRecord] = []
        for candidate in overdue_candidates:
            if limit is not None and len(expired_records) >= limit:
                break
            updated = await self._store.get_entry(candidate.job_id)
            if updated is None:
                continue
            updated_status = await self._get_entry_status(candidate.job_id)
            if updated_status == "expired" or (updated_status is None and updated.completed_at_ms is not None):
                expired_records.append(_entry_to_record(updated, store_status="expired"))
        return expired_records

    async def cleanup_terminal(
        self,
        before: datetime,
        *,
        limit: int | None = None,
    ) -> int:
        """Clean up terminal queue records older than before."""
        before_ms = int(_ensure_utc(before).timestamp() * 1000)
        return await self._store.cleanup_terminal(before_ms=before_ms, limit=limit)

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        """Retrieve a task record by UUID."""
        target_id = str(task_id)
        entry = await self._store.get_entry(target_id)
        if entry is None:
            return None
        store_status = await self._get_entry_status(target_id)
        if store_status is None:
            completed = await self._store.list_entries(entry.queue, status="completed", limit=100)
            if any(item.job_id == target_id for item in completed):
                store_status = "completed"
        return _entry_to_record(entry, store_status=store_status)

    async def get_tasks(self, task_ids: Sequence[UUID]) -> list[QueuedTaskRecord]:
        """Retrieve multiple task records by UUID."""
        records: list[QueuedTaskRecord] = []
        for tid in task_ids:
            rec = await self.get_task(tid)
            if rec is not None:
                records.append(rec)
        return records

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        """Retrieve a task record by idempotency key across all queues."""
        entry = await self._store.get_entry_by_key(key, queue=None)
        if entry is None:
            entries = await self._store.list_entries(queue=None, status="ready", limit=1000)
            entry = next(
                (
                    item
                    for item in entries
                    if item.headers.get("key") == key or item.headers.get("idempotency_key") == key
                ),
                None,
            )
        if entry is None:
            return None
        store_status = await self._get_entry_status(entry.job_id)
        return _entry_to_record(entry, store_status=store_status)

    async def cancel_task(
        self,
        task_id: UUID,
        *,
        include_running: bool = False,
        expected_retry_count: int | None = None,
    ) -> bool:
        """Cancel a pending, scheduled, or optionally running task."""
        target_id = str(task_id)
        if not include_running or expected_retry_count is not None:
            entry = await self._store.get_entry(target_id)
            if entry is None:
                return False
            if expected_retry_count is not None and entry.attempt != expected_retry_count + 1:
                return False
            if not include_running:
                store_status = await self._get_entry_status(target_id)
                if store_status is not None:
                    if store_status.strip().lower() in {"claimed", "running"}:
                        return False
                elif entry.lease_until_ms > int(datetime.now(UTC).timestamp() * 1000):
                    return False
        return await self._store.cancel(target_id)

    async def interrupt_task(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int,
        worker_id: str,
        queued_at: datetime,
    ) -> QueuedTaskRecord | None:
        """Interrupt a fenced running task attempt and return the updated record."""
        target_id = str(task_id)
        entry = await self._store.get_entry(target_id)
        if entry is None:
            return None
        if entry.attempt != expected_retry_count + 1 or entry.worker_id != worker_id:
            return None
        store_status = await self._get_entry_status(target_id)
        if store_status is not None:
            if store_status.strip().lower() not in {"claimed", "running"}:
                return None
        elif entry.lease_until_ms <= int(datetime.now(UTC).timestamp() * 1000):
            return None
        interrupted = await self._store.interrupt_task(target_id)
        if not interrupted:
            return None
        updated = await self._store.get_entry(target_id)
        if updated is None:
            return None
        updated_status = await self._get_entry_status(target_id)
        record = _entry_to_record(updated, store_status=updated_status)
        record.queued_at = _ensure_utc(queued_at)
        return record

    async def cancel_tasks(
        self,
        *,
        task_name: str | None = None,
        queue: str | None = None,
        kwargs: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        include_running: bool = False,
    ) -> int:
        """Cancel multiple pending, scheduled, or optionally running tasks."""
        entries = list(await self._store.list_entries(queue, status="ready", limit=1000))
        if include_running:
            entries.extend(await self._store.list_entries(queue, status="claimed", limit=1000))
        cancelled = 0
        for entry in entries:
            record = _entry_to_record(entry)
            if record_matches_filters(
                record,
                task_name=task_name,
                queue=queue,
                kwargs=kwargs,
                metadata=metadata,
            ) and await self._store.cancel(entry.job_id):
                cancelled += 1
        return cancelled

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> list[QueuedTaskRecord]:
        """List due ready pending tasks across queues or a single queue."""
        if limit <= 0:
            return []
        entries = await self._store.list_entries(queue, status="ready", limit=max(limit * 10, 100))
        records: list[QueuedTaskRecord] = []
        for entry in entries:
            record = _entry_to_record(entry, store_status="ready")
            if not record.is_due or record.is_expired:
                continue
            if is_external_dispatch_reservation(record.execution_ref):
                continue
            if execution_backend is not None and record.execution_backend != execution_backend:
                continue
            records.append(record)
        records.sort(key=lambda record: (-record.priority, record.queued_at, record.created_at, record.id.int))
        return records[:limit]

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        """List completed tasks matching task_name across all queues."""
        if limit <= 0:
            return []
        since_utc = _ensure_utc(since) if since is not None else None
        entries = await self._store.list_entries(queue=None, status="completed", limit=max(limit * 10, 1000))
        records: list[QueuedTaskRecord] = []
        for entry in entries:
            record = _entry_to_record(entry, store_status="completed")
            if record.task_name != task_name:
                continue
            if since_utc is not None and (record.completed_at is None or record.completed_at < since_utc):
                continue
            records.append(record)
        records.sort(key=lambda record: record.completed_at or record.created_at, reverse=True)
        return records[:limit]

    async def list_running_external(
        self,
        *,
        limit: int | None = None,
    ) -> list[QueuedTaskRecord]:
        """List running tasks across all queues."""
        fetch_limit = limit if limit is not None else 1000
        entries = await self._store.list_entries(queue=None, status="claimed", limit=fetch_limit)
        records = [_entry_to_record(entry, store_status="claimed") for entry in entries]
        return records[:limit] if limit is not None else records

    async def get_statistics(self, *, queue: str | None = None) -> QueueStatistics:
        """Return operational statistics for the specified queue or across all queues."""
        ready_entries = await self._store.list_entries(queue, status="ready", limit=1000)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        scheduled = sum(1 for e in ready_entries if e.scheduled_at_ms is not None and e.scheduled_at_ms > now_ms)
        pending = len(ready_entries) - scheduled
        claimed_entries = await self._store.list_entries(queue, status="claimed", limit=1000)
        completed_entries = await self._store.list_entries(queue, status="completed", limit=1000)
        dead_entries = await self._store.list_entries(queue, status="dead_letter", limit=1000)
        cancelled_entries = await self._store.list_entries(queue, status="cancelled", limit=1000)
        return QueueStatistics(
            pending=pending,
            scheduled=scheduled,
            running=len(claimed_entries),
            completed=len(completed_entries),
            failed=len(dead_entries),
            cancelled=len(cancelled_entries),
        )

    async def reserve_identity(
        self,
        key: str,
        *,
        task_id: UUID,
        task_name: str,
    ) -> TaskReservation | None:
        """Atomically reserve a forever idempotency key."""
        existing = await self._store.reserve_identity(
            key,
            job_id=str(task_id),
            task_name=task_name,
            ttl_seconds=None,
        )
        return _reservation_to_task_reservation(existing) if existing is not None else None

    async def has_identity(self, key: str) -> TaskReservation | None:
        """Check if a forever idempotency key is active."""
        reservation = await self._store.get_reservation(key)
        return _reservation_to_task_reservation(reservation) if reservation is not None else None

    async def reset_identity(
        self,
        key: str,
        *,
        expected_task_id: UUID | None = None,
    ) -> bool:
        """Release a forever idempotency key reservation."""
        expected_job_id = str(expected_task_id) if expected_task_id is not None else None
        return await self._store.reset_identity(key, expected_job_id=expected_job_id)

    async def acquire_maintenance(
        self,
        name: str,
        token: str,
        *,
        ttl: timedelta,
    ) -> bool:
        """Acquire a distributed maintenance lease."""
        return await self._store.acquire_lease(f"maint:{name}", owner_id=token, ttl_seconds=ttl.total_seconds())

    async def release_maintenance(self, name: str, token: str) -> bool:
        """Release a held distributed maintenance lease."""
        return await self._store.release_lease(f"maint:{name}", owner_id=token)

    async def wait_for_wakeups(self, timeout: float | None = None) -> bool:
        """Wait for new task notifications from the notification source with timeout fallback."""
        effective_timeout = 0.1 if timeout is None else max(0.0, timeout)
        if self._notification_source is None:
            await asyncio.sleep(effective_timeout)
            return False
        if not self._wakeup_subscribed:
            await self._notification_source.subscribe(("vertebra_queue_wakeup",))
            self._wakeup_subscribed = True
        iterator = self._notification_source.listen().__aiter__()
        try:
            await asyncio.wait_for(iterator.__anext__(), timeout=effective_timeout)
            return True
        except (TimeoutError, StopAsyncIteration):
            return False
        finally:
            aclose_fn = getattr(iterator, "aclose", None)
            if callable(aclose_fn):
                with contextlib.suppress(Exception):
                    res = aclose_fn()
                    if _inspect.isawaitable(res):
                        await res

    async def time_until_next_due(self, *, queues: tuple[str, ...] = ()) -> float | None:
        """Return seconds until the next scheduled task is due, if known."""
        if queues:
            return await self._store.next_due_delay(queues)
        ready = await self._store.list_entries(queue=None, status="ready", limit=100)
        if not ready:
            return None
        target_queues = tuple(dict.fromkeys(entry.queue for entry in ready))
        return await self._store.next_due_delay(target_queues)

    async def close(self) -> None:
        """Close backing store resources."""
        if hasattr(self._store, "aclose"):
            await self._store.aclose()
        elif isinstance(self._store, _SyncClosableStore):
            res = self._store.close()
            if _inspect.isawaitable(res):
                await res


queue_backend("vertebra_store")(StoreQueueBackend)


class ContainerTaskDependencyProvider:
    """TaskDependencyProvider entering Container.scope() per task execution."""

    __slots__ = ("_container",)

    def __init__(self, container: Container) -> None:
        """Initialize with parent Container."""
        self._container = container

    @contextlib.asynccontextmanager
    async def __call__(
        self,
        task: Any,
        record: Any,
        context: Any,
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Enter per-task Container.scope() and resolve dependencies."""
        extra: dict[str, Any] = {"task": task, "record": record, "context": context}
        if hasattr(record, "id"):
            extra["task_id"] = str(record.id)
        async with self._container.scope(extra_context=extra) as scope:
            fn = getattr(task, "fn", getattr(task, "callable", None))
            deps: dict[str, Any] = {}
            if fn is not None:
                bindings = inspect_callable_bindings(fn)
                for binding in bindings:
                    dep_key = self._container.graph.resolve_param_key(binding)
                    if dep_key is None:
                        if binding.name in self._container.singletons:
                            dep_key = binding.name
                        elif binding.annotation is not None and _inspect.isclass(binding.annotation):
                            for s_key, s_val in self._container.singletons.items():
                                if isinstance(s_val, binding.annotation):
                                    dep_key = s_key
                                    break
                    if dep_key is not None:
                        deps[binding.name] = await scope.resolve(dep_key)
            yield deps
