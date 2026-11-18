"""Vertebra shared-memory queue and execution backends for litestar-queues."""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, cast
from uuid import UUID

import msgspec

from vertebra._vertebra import ShmQueueRing
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litestar_queues.backends.base import BaseQueueBackend as _BaseQueueBackend
    from litestar_queues.backends.base import DispatchRepairCandidates
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.base import BaseExecutionBackend as _BaseExecutionBackend
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
    from litestar_queues.service import QueueService

__all__ = (
    "SHMExecutionBackend",
    "SHMQueueBackend",
    "SHMQueueBackendConfig",
)

_MIN_DATETIME = datetime(1, 1, 1, tzinfo=UTC)
_VALID_STATUSES: frozenset[str] = frozenset(
    {"pending", "scheduled", "running", "completed", "failed", "cancelled", "expired"}
)
_CACHED_SHM_QUEUE_BACKEND_CLS: type[Any] | None = None
_CACHED_SHM_EXECUTION_BACKEND_CLS: type[Any] | None = None
_SHM_QUEUE_SLOTS: tuple[str, ...] = (
    "_adaptive_clamping",
    "_capacity",
    "_control_epoch",
    "_control_event",
    "_max_wakeup_timeout",
    "_min_wakeup_timeout",
    "_notification_event",
    "_ring",
    "_ring_name",
    "_segment_name",
    "_slot_bytes",
    "_terminal_ttl",
)
_SHM_EXEC_SLOTS: tuple[str, ...] = ("_warm_contexts",)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


def _fmt_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    utc_val = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return utc_val.astimezone(UTC).isoformat()


def _record_to_bytes(record: QueuedTaskRecord) -> bytes:
    payload = {
        "id": str(record.id),
        "task_name": record.task_name,
        "args": list(record.args),
        "kwargs": dict(record.kwargs),
        "queue": record.queue,
        "execution_backend": record.execution_backend,
        "execution_profile": record.execution_profile,
        "execution_ref": record.execution_ref,
        "worker_id": record.worker_id,
        "status": record.status,
        "priority": record.priority,
        "max_retries": record.max_retries,
        "retry_count": record.retry_count,
        "scheduled_at": _fmt_dt(record.scheduled_at),
        "expires_at": _fmt_dt(record.expires_at),
        "created_at": _fmt_dt(record.created_at),
        "queued_at": _fmt_dt(record.queued_at),
        "started_at": _fmt_dt(record.started_at),
        "completed_at": _fmt_dt(record.completed_at),
        "heartbeat_at": _fmt_dt(record.heartbeat_at),
        "result": record.result,
        "error": record.error,
        "key": record.key,
        "metadata": dict(record.metadata),
        "dispatch_checked_at": _fmt_dt(record.dispatch_checked_at),
    }
    return encode_json_bytes(payload)


def _bytes_to_record(raw: bytes) -> QueuedTaskRecord:
    models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
    data = decode_json(raw)
    if not isinstance(data, dict):
        msg = "Corrupt SHM queue task payload"
        raise ValueError(msg)
    status_raw = str(data.get("status", "pending"))
    status: TaskStatus = cast("TaskStatus", status_raw if status_raw in _VALID_STATUSES else "pending")
    return cast(
        "QueuedTaskRecord",
        models_mod.QueuedTaskRecord(
            task_name=str(data["task_name"]),
            id=UUID(str(data["id"])),
            args=tuple(data.get("args", ())),
            kwargs=dict(data.get("kwargs", {})),
            queue=str(data.get("queue", "default")),
            execution_backend=str(data.get("execution_backend", "local")),
            execution_profile=data.get("execution_profile"),
            execution_ref=data.get("execution_ref"),
            worker_id=data.get("worker_id"),
            status=status,
            priority=int(data.get("priority", 0)),
            max_retries=int(data.get("max_retries", 0)),
            retry_count=int(data.get("retry_count", 0)),
            scheduled_at=_parse_dt(data.get("scheduled_at")),
            expires_at=_parse_dt(data.get("expires_at")),
            created_at=_parse_dt(data.get("created_at")) or _utc_now(),
            queued_at=_parse_dt(data.get("queued_at")) or _utc_now(),
            started_at=_parse_dt(data.get("started_at")),
            completed_at=_parse_dt(data.get("completed_at")),
            heartbeat_at=_parse_dt(data.get("heartbeat_at")),
            result=data.get("result"),
            error=data.get("error"),
            key=data.get("key"),
            metadata=dict(data.get("metadata", {})),
            dispatch_checked_at=_parse_dt(data.get("dispatch_checked_at")),
        ),
    )


def _reservation_to_bytes(res: TaskReservation) -> bytes:
    return encode_json_bytes(
        {
            "key": res.key,
            "task_id": str(res.task_id),
            "task_name": res.task_name,
            "created_at": _fmt_dt(res.created_at),
        }
    )


def _bytes_to_reservation(raw: bytes) -> TaskReservation:
    models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
    data = decode_json(raw)
    return cast(
        "TaskReservation",
        models_mod.TaskReservation(
            key=str(data["key"]),
            task_id=UUID(str(data["task_id"])),
            task_name=str(data["task_name"]),
            created_at=_parse_dt(data.get("created_at")) or _utc_now(),
        ),
    )


def _expire_record(record: QueuedTaskRecord, completed_at: datetime) -> None:
    record.status = "expired"
    record.completed_at = completed_at
    record.heartbeat_at = None


def _stale_sort_key(record: QueuedTaskRecord) -> tuple[datetime, str]:
    return (record.heartbeat_at or _MIN_DATETIME, str(record.id))


class SHMQueueBackendConfig(msgspec.Struct, frozen=True):
    """Configuration for vertebra_shm queue backend."""

    backend_name: ClassVar[str] = "vertebra_shm"
    segment_name: str = "vertebra_queue"
    """POSIX shared-memory segment name used for cross-worker queues and wakeups."""

    ring_name: str | None = None
    """Optional alias overriding segment_name for the underlying ShmQueueRing."""

    min_wakeup_timeout: float = 0.005
    """Minimum adaptive wakeup interval in seconds."""

    max_wakeup_timeout: float = 30.0
    """Maximum fallback wakeup interval in seconds."""

    adaptive_clamping: bool = True
    """Whether worker poll timeouts clamp dynamically to upcoming scheduled tasks."""

    terminal_ttl: timedelta | None = None
    """Optional retention duration for completed or failed task records."""

    capacity: int = 4096
    """Slot capacity of the underlying ShmQueueRing."""

    slot_bytes: int = 8192
    """Maximum encoded payload size in bytes per ring/record slot."""

    def create_backend(self, config: QueueConfig | None = None) -> SHMQueueBackend:
        """Create a configured SHMQueueBackend instance."""
        backend_cls = _get_shm_queue_backend_cls()
        resolved_name = self.ring_name if self.ring_name is not None else self.segment_name
        return backend_cls(
            config=config,
            backend_config=self,
            min_wakeup_timeout=self.min_wakeup_timeout,
            max_wakeup_timeout=self.max_wakeup_timeout,
            adaptive_clamping=self.adaptive_clamping,
            segment_name=resolved_name,
            ring_name=resolved_name,
            terminal_ttl=self.terminal_ttl,
            capacity=self.capacity,
            slot_bytes=self.slot_bytes,
        )


class _SHMQueueBackendImpl:
    """Shared-memory BaseQueueBackend implementation backed by Rust ShmQueueRing."""

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        backend_config: SHMQueueBackendConfig | None = None,
        min_wakeup_timeout: float = 0.005,
        max_wakeup_timeout: float = 30.0,
        adaptive_clamping: bool = True,
        segment_name: str = "vertebra_queue",
        ring_name: str | None = None,
        terminal_ttl: timedelta | None = None,
        capacity: int = 4096,
        slot_bytes: int = 8192,
    ) -> None:
        """Initialize the shared-memory queue backend with adaptive wakeup clamping bounds."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        base_mod.BaseQueueBackend.__init__(self, config=config)
        if backend_config is not None:
            min_wakeup_timeout = backend_config.min_wakeup_timeout
            max_wakeup_timeout = backend_config.max_wakeup_timeout
            adaptive_clamping = backend_config.adaptive_clamping
            segment_name = backend_config.ring_name or backend_config.segment_name
            terminal_ttl = backend_config.terminal_ttl
            capacity = backend_config.capacity
            slot_bytes = backend_config.slot_bytes
        if ring_name is not None:
            segment_name = ring_name
        self._min_wakeup_timeout = max(0.001, min_wakeup_timeout)
        self._max_wakeup_timeout = max(self._min_wakeup_timeout, max_wakeup_timeout)
        self._adaptive_clamping = adaptive_clamping
        self._segment_name = segment_name
        self._ring_name = segment_name
        self._terminal_ttl = terminal_ttl
        self._capacity = capacity
        self._slot_bytes = slot_bytes
        self._notification_event = asyncio.Event()
        self._control_event = asyncio.Event()
        self._control_epoch = 0
        self._ring: ShmQueueRing | None = ShmQueueRing(
            self._segment_name,
            capacity=self._capacity,
            slot_bytes=self._slot_bytes,
        )

    @property
    def min_wakeup_timeout(self) -> float:
        """Return the minimum clamped wakeup timeout in seconds."""
        return self._min_wakeup_timeout

    @property
    def max_wakeup_timeout(self) -> float:
        """Return the maximum clamped wakeup timeout in seconds."""
        return self._max_wakeup_timeout

    @property
    def segment_name(self) -> str:
        """Return the shared memory segment name."""
        return self._segment_name

    @property
    def ring_name(self) -> str:
        """Return the shared memory ring name."""
        return self._ring_name

    @property
    def terminal_ttl(self) -> timedelta | None:
        """Return the terminal retention duration."""
        return self._terminal_ttl

    @property
    def ring(self) -> ShmQueueRing:
        """Return the active ShmQueueRing instance, reopening if closed."""
        if self._ring is None:
            self._ring = ShmQueueRing(
                self._segment_name,
                capacity=self._capacity,
                slot_bytes=self._slot_bytes,
            )
        return self._ring

    @property
    def capabilities(self) -> QueueBackendCapabilities:
        """Return shared-memory backend capabilities."""
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        return cast(
            "QueueBackendCapabilities",
            models_mod.QueueBackendCapabilities(
                supports_worker_wakeups=True,
                wakeup_backend="vertebra-shm",
                wakeups_durable=False,
                supports_completion_events=True,
                supports_maintenance=True,
            ),
        )

    async def open(self) -> bool:
        """Ensure the shared-memory queue ring is mapped and ready."""
        _ = self.ring
        return True

    async def close(self) -> None:
        """Close the local SHM ring handle without unlinking the shared segment."""
        if self._ring is not None:
            self._ring.close()
            self._ring = None

    def _emit_wakeup(self) -> None:
        ring = self.ring
        try:
            ring.push(b"\x01")
        except OverflowError:
            ring.wake(False)

    def _drain_wakeups(self) -> None:
        ring = self.ring
        while ring.pop() is not None:
            pass

    def _all_records_with_raw(self) -> list[tuple[str, bytes, QueuedTaskRecord]]:
        items: list[tuple[str, bytes, QueuedTaskRecord]] = []
        for rec_key, raw in self.ring.list_records("task:"):
            items.append((rec_key, raw, _bytes_to_record(raw)))
        return items

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
        """Persist a queued task in SHM and wake waiting workers when due."""
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        ring = self.ring
        if key is not None:
            key_ref = ring.get_record(f"key:{key}")
            if key_ref is not None:
                existing_raw = ring.get_record(f"task:{key_ref.decode('utf-8')}")
                if existing_raw is not None:
                    existing = _bytes_to_record(existing_raw)
                    if not existing.is_terminal:
                        return existing

        now = _utc_now()
        record = cast(
            "QueuedTaskRecord",
            models_mod.QueuedTaskRecord(
                task_name=task_name,
                args=args,
                kwargs=dict(kwargs or {}),
                queue=queue,
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                status="scheduled" if scheduled_at is not None and scheduled_at > now else "pending",
                priority=priority,
                max_retries=max_retries,
                scheduled_at=scheduled_at,
                expires_at=expires_at,
                key=key,
                metadata=dict(metadata or {}),
                created_at=now,
                queued_at=now,
            ),
        )
        if id is not None:
            record.id = id
        ring.put_record(f"task:{record.id}", _record_to_bytes(record))
        if key is not None:
            ring.put_record(f"key:{key}", str(record.id).encode("utf-8"))
        await self.notify_new_task(record)
        return record

    async def enqueue_many(self, requests: Sequence[TaskRequest]) -> list[QueuedTaskRecord]:
        """Persist multiple queued tasks in SHM and emit a coalesced wakeup."""
        if not requests:
            return []
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        ring = self.ring
        records: list[QueuedTaskRecord] = []
        now = _utc_now()
        for request in requests:
            if request.key is not None:
                key_ref = ring.get_record(f"key:{request.key}")
                if key_ref is not None:
                    existing_raw = ring.get_record(f"task:{key_ref.decode('utf-8')}")
                    if existing_raw is not None:
                        existing = _bytes_to_record(existing_raw)
                        if not existing.is_terminal:
                            records.append(existing)
                            continue
            record_now = _utc_now()
            record = cast(
                "QueuedTaskRecord",
                models_mod.QueuedTaskRecord(
                    task_name=request.task_name,
                    args=request.args,
                    kwargs=dict(request.kwargs or {}),
                    queue=request.queue,
                    execution_backend=request.execution_backend,
                    execution_profile=request.execution_profile,
                    status="scheduled"
                    if request.scheduled_at is not None and request.scheduled_at > now
                    else "pending",
                    priority=request.priority,
                    max_retries=request.max_retries,
                    scheduled_at=request.scheduled_at,
                    expires_at=request.expires_at,
                    key=request.key,
                    metadata=dict(request.metadata or {}),
                    created_at=record_now,
                    queued_at=record_now,
                ),
            )
            ring.put_record(f"task:{record.id}", _record_to_bytes(record))
            if request.key is not None:
                ring.put_record(f"key:{request.key}", str(record.id).encode("utf-8"))
            records.append(record)
        await self.notify_new_tasks(records)
        cast("Any", self)._record_enqueue_batch(len(requests))
        return records

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        """Return a queued task record by UUID from SHM."""
        raw = self.ring.get_record(f"task:{task_id}")
        if raw is None:
            return None
        return _bytes_to_record(raw)

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        """Return a queued task record by deduplication key from SHM."""
        key_ref = self.ring.get_record(f"key:{key}")
        if key_ref is None:
            return None
        raw = self.ring.get_record(f"task:{key_ref.decode('utf-8')}")
        if raw is None:
            return None
        return _bytes_to_record(raw)

    async def get_tasks(self, task_ids: Sequence[UUID]) -> list[QueuedTaskRecord]:
        """Return existing queued task records matching task_ids."""
        records: list[QueuedTaskRecord] = []
        for task_id in task_ids:
            record = await self.get_task(task_id)
            if record is not None:
                records.append(record)
        return records

    async def notify_worker_control(self, worker_id: str | None) -> None:
        """Publish a cross-process worker-control hint via SHM."""
        del worker_id
        ring = self.ring
        for _ in range(4):
            raw = ring.get_record("ctrl:epoch")
            next_val = int(raw.decode("utf-8")) + 1 if raw is not None else 1
            if ring.cas_record("ctrl:epoch", raw, str(next_val).encode("utf-8")):
                break
        self._control_event.set()
        ring.wake(True)

    async def wait_for_worker_control(self, *, worker_id: str, timeout: float | None = None) -> bool:
        """Wait for a local or cross-process worker-control hint."""
        del worker_id
        ring = self.ring
        raw = ring.get_record("ctrl:epoch")
        current_epoch = int(raw.decode("utf-8")) if raw is not None else 0
        if current_epoch > self._control_epoch:
            self._control_epoch = current_epoch
            self._control_event.clear()
            return True
        if self._control_event.is_set():
            self._control_event.clear()
            return True
        if timeout is not None and timeout <= 0.0:
            return False
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._control_event.is_set():
                self._control_event.clear()
                return True
            latest_raw = ring.get_record("ctrl:epoch")
            latest_epoch = int(latest_raw.decode("utf-8")) if latest_raw is not None else 0
            if latest_epoch > self._control_epoch:
                self._control_epoch = latest_epoch
                return True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                sleep_s = min(remaining, 0.05)
            else:
                sleep_s = 0.05
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._control_event.wait(), timeout=sleep_s)
                self._control_event.clear()
                return True

    async def assign_worker(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        expected_retry_count: int,
    ) -> QueuedTaskRecord | None:
        """Persist the active worker owner on a fenced running task attempt."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            if record.status != "running" or record.retry_count != expected_retry_count:
                return None
            record.worker_id = worker_id
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                return record
        return None

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> list[QueuedTaskRecord]:
        """Return due pending or scheduled tasks ordered for execution."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        due_records = [
            record
            for _, _, record in self._all_records_with_raw()
            if record.status in {"pending", "scheduled"}
            and record.is_due
            and not record.is_expired
            and not base_mod.is_external_dispatch_reservation(record.execution_ref)
            and (queue is None or record.queue == queue)
            and (execution_backend is None or record.execution_backend == execution_backend)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.queued_at, record.created_at, record.id.int))
        return due_records[:limit]

    async def claim_task(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int | None = None,
        expected_execution_ref: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Atomically claim a pending or scheduled task in SHM."""
        claimed, _ = await self.claim_task_with_expired(
            task_id,
            expected_retry_count=expected_retry_count,
            expected_execution_ref=expected_execution_ref,
        )
        return claimed

    async def claim_task_with_expired(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int | None = None,
        expected_execution_ref: str | None = None,
    ) -> tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]:
        """Claim one task or transition it to expired under atomic CAS."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None, None
        record = _bytes_to_record(raw)
        if (
            record.status not in {"pending", "scheduled"}
            or not record.is_due
            or base_mod.is_external_dispatch_reservation(record.execution_ref)
            or (expected_retry_count is not None and record.retry_count != expected_retry_count)
            or (expected_execution_ref is not None and record.execution_ref != expected_execution_ref)
        ):
            return None, None
        now = _utc_now()
        if record.execution_ref is None and record.expires_at is not None and record.expires_at <= now:
            _expire_record(record, now)
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                ring.wake(True)
                return None, record
            return None, None
        record.status = "running"
        record.started_at = now
        record.heartbeat_at = now
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            return record, None
        return None, None

    async def claim_next(
        self,
        *,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Claim the next due task across requested queues."""
        claimed = await self.claim_many(limit=1, queues=queues, execution_backend=execution_backend)
        return claimed[0] if claimed else None

    async def claim_many(
        self,
        *,
        limit: int,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
        queue_limits: Mapping[str, int] | None = None,
    ) -> list[QueuedTaskRecord]:
        """Claim up to limit due tasks atomically across worker processes."""
        claimed, _ = await self.claim_many_with_expired(
            limit=limit,
            queues=queues,
            execution_backend=execution_backend,
            queue_limits=queue_limits,
        )
        return claimed

    async def claim_many_with_expired(
        self,
        *,
        limit: int,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
        queue_limits: Mapping[str, int] | None = None,
    ) -> tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]:
        """Claim records and expire overdue tasks using per-record SHM CAS."""
        if limit <= 0:
            return [], []
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        ring = self.ring
        now = _utc_now()
        eligible: list[tuple[str, bytes, QueuedTaskRecord]] = []
        expired: list[QueuedTaskRecord] = []

        for rec_key, raw, record in self._all_records_with_raw():
            if record.status not in {"pending", "scheduled"}:
                continue
            if queues and record.queue not in queues:
                continue
            if execution_backend is not None and record.execution_backend != execution_backend:
                continue
            if base_mod.is_external_dispatch_reservation(record.execution_ref):
                continue
            if record.expires_at is not None and record.expires_at <= now:
                _expire_record(record, now)
                if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                    expired.append(record)
                continue
            if record.scheduled_at is not None and record.scheduled_at > now:
                continue
            eligible.append((rec_key, raw, record))

        eligible.sort(
            key=lambda item: (
                -item[2].priority,
                item[2].queued_at,
                item[2].created_at,
                item[2].id.int,
            )
        )
        claimed: list[QueuedTaskRecord] = []
        remaining = dict(queue_limits or {})
        for rec_key, raw, record in eligible:
            if len(claimed) >= limit:
                break
            if record.queue in remaining and remaining[record.queue] <= 0:
                continue
            record.status = "running"
            record.started_at = now
            record.heartbeat_at = now
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                claimed.append(record)
                if record.queue in remaining:
                    remaining[record.queue] -= 1
        if expired:
            ring.wake(True)
        return claimed, expired

    async def complete_task(
        self,
        task_id: UUID,
        *,
        result: Any = None,
        expected_retry_count: int | None = None,
    ) -> QueuedTaskRecord | None:
        """Transition a running task attempt to completed via SHM CAS."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            if expected_retry_count is not None and (
                record.status != "running" or record.retry_count != expected_retry_count
            ):
                return None
            now = _utc_now()
            record.status = "completed"
            record.completed_at = now
            record.heartbeat_at = None
            record.result = result
            record.error = None
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                ring.wake(True)
                return record
        return None

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
        """Record a task failure in SHM and either schedule retry or mark failed."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            if expected_retry_count is not None and (
                record.status != "running" or record.retry_count != expected_retry_count
            ):
                return None
            record.error = error
            should_retry = retry and base_mod.attempts_consumed(record) < record.max_retries
            if should_retry:
                now = queued_at or _utc_now()
                record.retry_count += 1
                record.queued_at = now
                record.scheduled_at = retry_at
                record.status = "scheduled" if retry_at is not None and retry_at > now else "pending"
                record.started_at = None
                record.heartbeat_at = None
                if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                    await self.notify_new_task(record)
                    return record
                continue

            now = _utc_now()
            record.status = "failed"
            record.completed_at = now
            record.heartbeat_at = None
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                ring.wake(True)
                return record
        return None

    async def cancel_task(
        self,
        task_id: UUID,
        *,
        include_running: bool = False,
        expected_retry_count: int | None = None,
    ) -> bool:
        """Cancel a pending, scheduled, or optionally running task via SHM CAS."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        cancellable = {"pending", "scheduled", "running"} if include_running else {"pending", "scheduled"}
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return False
            record = _bytes_to_record(raw)
            if record.status not in cancellable or (
                expected_retry_count is not None and record.retry_count != expected_retry_count
            ):
                return False
            record.status = "cancelled"
            record.completed_at = _utc_now()
            record.heartbeat_at = None
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                ring.wake(True)
                return True
        return False

    async def interrupt_task(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int,
        worker_id: str,
        queued_at: datetime,
    ) -> QueuedTaskRecord | None:
        """Return an owned running task interrupted during worker shutdown to pending."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            if (
                record.status != "running"
                or record.retry_count != expected_retry_count
                or record.worker_id != worker_id
            ):
                return None
            record.status = "pending"
            record.queued_at = queued_at
            record.scheduled_at = None
            record.started_at = None
            record.heartbeat_at = None
            record.completed_at = None
            record.execution_ref = None
            record.worker_id = None
            record.metadata["interruptions"] = base_mod.interruption_count(record) + 1
            record.retry_count += 1
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                await self.notify_new_task(record)
                return record
        return None

    async def cancel_tasks(
        self,
        *,
        task_name: str | None = None,
        queue: str | None = None,
        kwargs: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        include_running: bool = False,
    ) -> int:
        """Cancel all tasks matching the provided filter predicates."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        ring = self.ring
        cancellable = {"pending", "scheduled", "running"} if include_running else {"pending", "scheduled"}
        cancelled = 0
        now = _utc_now()
        for rec_key, raw, record in self._all_records_with_raw():
            if record.status not in cancellable:
                continue
            if not base_mod.record_matches_filters(
                record,
                task_name=task_name,
                queue=queue,
                kwargs=kwargs,
                metadata=metadata,
            ):
                continue
            record.status = "cancelled"
            record.completed_at = now
            record.heartbeat_at = None
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                cancelled += 1
        if cancelled:
            ring.wake(True)
        return cancelled

    async def touch_heartbeats(self, touches: Sequence[HeartbeatTouch]) -> HeartbeatTouchResult:
        """Refresh heartbeat_at for active fenced running task attempts."""
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        result = cast("HeartbeatTouchResult", models_mod.HeartbeatTouchResult())
        if not touches:
            return result
        ring = self.ring
        now = _utc_now()
        for touch in touches:
            rec_key = f"task:{touch.task_id}"
            raw = ring.get_record(rec_key)
            if raw is None:
                result.missed_task_ids.add(touch.task_id)
                continue
            record = _bytes_to_record(raw)
            if record.status != "running":
                result.missed_task_ids.add(touch.task_id)
                continue
            if touch.expected_retry_count is not None and record.retry_count != touch.expected_retry_count:
                result.missed_task_ids.add(touch.task_id)
                continue
            record.heartbeat_at = now
            if touch.metadata_patch:
                record.metadata.update(touch.metadata_patch)
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                result.touched_task_ids.add(touch.task_id)
            else:
                result.missed_task_ids.add(touch.task_id)
        return result

    async def null_heartbeats(self, task_ids: list[UUID], *, expected_retry_count: int | None = None) -> None:
        """Clear heartbeat_at for the given task IDs."""
        ring = self.ring
        for task_id in task_ids:
            rec_key = f"task:{task_id}"
            for _ in range(4):
                raw = ring.get_record(rec_key)
                if raw is None:
                    break
                record = _bytes_to_record(raw)
                if expected_retry_count is not None and record.retry_count != expected_retry_count:
                    break
                record.heartbeat_at = None
                if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                    break

    async def requeue_stale_running(
        self,
        *,
        stale_after: timedelta,
        limit: int | None = None,
    ) -> StaleTaskRecoveryResult:
        """Recover running tasks whose heartbeats are older than stale_after."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        ring = self.ring
        cutoff = _utc_now() - stale_after
        result = cast("StaleTaskRecoveryResult", models_mod.StaleTaskRecoveryResult())
        candidates = [
            (rec_key, raw, record)
            for rec_key, raw, record in self._all_records_with_raw()
            if record.status == "running" and (record.heartbeat_at is None or record.heartbeat_at < cutoff)
        ]
        candidates.sort(key=lambda item: _stale_sort_key(item[2]))
        if limit is not None:
            candidates = candidates[:limit]

        policy = cast("Any", self)._stale_requeue_priority_policy()
        for rec_key, raw, record in candidates:
            requeue_on_stale = record.metadata.get("requeue_on_stale", True) is not False
            if requeue_on_stale and base_mod.attempts_consumed(record) < record.max_retries:
                queued_at, retry_at = base_mod.retry_schedule(record)
                record.status = "scheduled" if retry_at is not None else "pending"
                record.queued_at = queued_at
                record.scheduled_at = retry_at
                record.priority = base_mod.stale_requeue_priority(record.priority, policy)
                record.started_at = None
                record.heartbeat_at = None
                record.error = base_mod.stale_requeue_error(record.error)
                record.retry_count += 1
                if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                    result.requeued += 1
                    result.requeued_task_ids.append(record.id)
                    await self.notify_new_task(record)
                continue
            record.status = "failed"
            record.completed_at = _utc_now()
            record.heartbeat_at = None
            record.error = base_mod.STALE_HEARTBEAT_ERROR
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                result.failed += 1
                result.failed_task_ids.append(record.id)
                if not requeue_on_stale:
                    result.handler_needed += 1
                    result.handler_needed_task_ids.append(record.id)
        if result.failed:
            ring.wake(True)
        return result

    async def expire_overdue(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        """Transition overdue pending or scheduled records to expired."""
        ring = self.ring
        now = _utc_now()
        expired: list[QueuedTaskRecord] = []
        for rec_key, raw, record in self._all_records_with_raw():
            if limit is not None and len(expired) >= limit:
                break
            if record.status not in {"pending", "scheduled"}:
                continue
            if record.execution_ref is not None:
                continue
            if record.expires_at is None or record.expires_at > now:
                continue
            _expire_record(record, now)
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                expired.append(record)
        if expired:
            ring.wake(True)
        return expired

    async def acquire_maintenance(self, name: str, token: str, *, ttl: timedelta) -> bool:
        """Acquire expiring, token-fenced maintenance ownership via SHM CAS."""
        ring = self.ring
        rec_key = f"maint:{name}"
        for _ in range(4):
            now = _utc_now()
            raw = ring.get_record(rec_key)
            if raw is not None:
                existing = decode_json(raw)
                if isinstance(existing, dict):
                    exp = _parse_dt(existing.get("expires_at"))
                    if exp is not None and exp > now and existing.get("token") != token:
                        return False
            payload = encode_json_bytes({"token": token, "expires_at": _fmt_dt(now + ttl)})
            if ring.cas_record(rec_key, raw, payload):
                return True
        return False

    async def release_maintenance(self, name: str, token: str) -> bool:
        """Release maintenance ownership only when token matches the holder."""
        ring = self.ring
        rec_key = f"maint:{name}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return False
        existing = decode_json(raw)
        if not isinstance(existing, dict) or existing.get("token") != token:
            return False
        return ring.cas_record(rec_key, raw, None)

    async def list_dispatch_repair_candidates(
        self,
        execution_backend: str,
        *,
        limit: int,
    ) -> DispatchRepairCandidates:
        """Select and mark a bounded, fair page of pending external deliveries."""
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        exc_mod = ensure_module("litestar_queues.exceptions", package="litestar-queues", extra="queues")
        if limit < 0:
            msg = "Dispatch repair limit must be non-negative."
            raise exc_mod.QueueConfigurationError(msg)
        if limit == 0:
            return cast("DispatchRepairCandidates", base_mod.DispatchRepairCandidates())
        ring = self.ring
        now = _utc_now()
        candidates = [
            (rec_key, raw, record)
            for rec_key, raw, record in self._all_records_with_raw()
            if record.execution_backend == execution_backend
            and record.status in {"pending", "scheduled"}
            and (record.expires_at is None or record.expires_at > now)
        ]
        candidates.sort(key=lambda item: (item[2].dispatch_checked_at or item[2].created_at, str(item[2].id)))
        selected: list[QueuedTaskRecord] = []
        for rec_key, raw, record in candidates[:limit]:
            if record.dispatch_checked_at is None or record.dispatch_checked_at < now:
                record.dispatch_checked_at = now
                ring.cas_record(rec_key, raw, _record_to_bytes(record))
            selected.append(record)
        return cast(
            "DispatchRepairCandidates",
            base_mod.DispatchRepairCandidates(tuple(selected), len(selected), len(selected) == limit),
        )

    async def reserve_scheduled_execution_ref(
        self,
        task_id: UUID,
        execution_backend: str,
        execution_ref: str,
        *,
        expected_retry_count: int,
        expected_execution_ref: str | None,
    ) -> QueuedTaskRecord | None:
        """Atomically reserve an unexpired pending/scheduled attempt for external dispatch."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        now = _utc_now()
        if (
            record.execution_backend != execution_backend
            or record.retry_count != expected_retry_count
            or record.execution_ref != expected_execution_ref
            or record.status not in {"pending", "scheduled"}
            or (record.expires_at is not None and record.expires_at <= now)
        ):
            return None
        record.execution_ref = execution_ref
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            return record
        return None

    async def reserve_external_dispatch(
        self,
        task_id: UUID,
        execution_backend: str,
        reservation_ref: str,
        *,
        execution_profile: str | None = None,
        expected_retry_count: int | None = None,
    ) -> QueuedTaskRecord | None:
        """Reserve a due pending or scheduled task for external execution dispatch."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        now = _utc_now()
        if (
            record.status not in {"pending", "scheduled"}
            or not record.is_due
            or record.execution_ref is not None
            or (expected_retry_count is not None and record.retry_count != expected_retry_count)
        ):
            return None
        if record.expires_at is not None and record.expires_at <= now:
            _expire_record(record, now)
            ring.cas_record(rec_key, raw, _record_to_bytes(record))
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = reservation_ref
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            return record
        return None

    async def clear_execution_ref(
        self,
        task_id: UUID,
        expected_retry_count: int,
        expected_execution_ref: str,
    ) -> QueuedTaskRecord | None:
        """Clear a fenced execution_ref on a pending or scheduled task and wake dispatchers."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        if (
            record.status not in {"pending", "scheduled"}
            or record.retry_count != expected_retry_count
            or record.execution_ref != expected_execution_ref
        ):
            return None
        record.execution_ref = None
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            await self.notify_new_task(record)
            return record
        return None

    async def replace_execution_ref(
        self,
        task_id: UUID,
        expected_retry_count: int,
        expected_execution_ref: str,
        execution_ref: str,
    ) -> QueuedTaskRecord | None:
        """Atomically rotate a fenced execution_ref on a pending or scheduled task."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        if (
            record.status not in {"pending", "scheduled"}
            or record.retry_count != expected_retry_count
            or record.execution_ref != expected_execution_ref
        ):
            return None
        record.execution_ref = execution_ref
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            return record
        return None

    async def release_external_dispatch(
        self,
        task_id: UUID,
        reservation_ref: str,
        execution_backend: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Release a provisional external-dispatch reservation back to the queue."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        if record.execution_ref != reservation_ref:
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = None
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            await self.notify_new_task(record)
            return record
        return None

    async def finalize_external_dispatch(
        self,
        task_id: UUID,
        reservation_ref: str,
        execution_backend: str,
        execution_ref: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Commit the concrete provider execution_ref for a dispatch reservation."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        raw = ring.get_record(rec_key)
        if raw is None:
            return None
        record = _bytes_to_record(raw)
        if record.execution_ref != reservation_ref or record.status not in {"pending", "scheduled"}:
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = execution_ref
        if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
            return record
        return None

    async def set_execution_ref(
        self,
        task_id: UUID,
        execution_backend: str,
        execution_ref: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Persist an external execution reference on a queued task."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = execution_ref
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                return record
        return None

    async def set_execution_backend(
        self,
        task_id: UUID,
        execution_backend: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Update a task's execution backend and clear its execution_ref."""
        ring = self.ring
        rec_key = f"task:{task_id}"
        for _ in range(4):
            raw = ring.get_record(rec_key)
            if raw is None:
                return None
            record = _bytes_to_record(raw)
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = None
            if ring.cas_record(rec_key, raw, _record_to_bytes(record)):
                await self.notify_new_task(record)
                return record
        return None

    async def list_running_external(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        """Return non-terminal tasks carrying an external execution_ref."""
        records = [
            record
            for _, _, record in self._all_records_with_raw()
            if not record.is_terminal and record.execution_ref is not None
        ]
        records.sort(key=lambda record: (record.started_at or record.created_at, str(record.id)))
        return records[:limit] if limit is not None else records

    async def get_statistics(self, *, queue: str | None = None) -> QueueStatistics:
        """Return aggregate task counts grouped by lifecycle status."""
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        statistics = cast("QueueStatistics", models_mod.QueueStatistics())
        for _, _, record in self._all_records_with_raw():
            if queue is not None and record.queue != queue:
                continue
            setattr(statistics, record.status, getattr(statistics, record.status) + 1)
        return statistics

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        """Return recent completed records for task_name ordered newest first."""
        records = [
            record
            for _, _, record in self._all_records_with_raw()
            if record.task_name == task_name
            and record.status == "completed"
            and record.completed_at is not None
            and (since is None or record.completed_at >= since)
        ]
        records.sort(key=lambda record: record.completed_at or record.created_at, reverse=True)
        return records[:limit]

    async def cleanup_terminal(self, before: datetime, *, limit: int | None = None) -> int:
        """Delete terminal task records completed strictly before cutoff."""
        ring = self.ring
        candidates = [
            (rec_key, raw, record)
            for rec_key, raw, record in self._all_records_with_raw()
            if record.is_terminal and record.completed_at is not None and record.completed_at < before
        ]
        candidates.sort(key=lambda item: (item[2].completed_at or _MIN_DATETIME, str(item[2].id)))
        if limit is not None:
            candidates = candidates[:limit]
        removed = 0
        for rec_key, raw, record in candidates:
            if ring.cas_record(rec_key, raw, None):
                removed += 1
                if record.key is not None:
                    key_raw = ring.get_record(f"key:{record.key}")
                    if key_raw is not None and key_raw.decode("utf-8") == str(record.id):
                        ring.cas_record(f"key:{record.key}", key_raw, None)
        return removed

    async def reserve_identity(
        self,
        key: str,
        *,
        task_id: UUID,
        task_name: str,
    ) -> TaskReservation | None:
        """Atomically reserve a forever identity in SHM."""
        models_mod = ensure_module("litestar_queues.models", package="litestar-queues", extra="queues")
        ring = self.ring
        res_key = f"res:{key}"
        reservation = cast(
            "TaskReservation",
            models_mod.TaskReservation(
                key=key,
                task_id=task_id,
                task_name=task_name,
                created_at=_utc_now(),
            ),
        )
        if ring.cas_record(res_key, None, _reservation_to_bytes(reservation)):
            return None
        existing_raw = ring.get_record(res_key)
        if existing_raw is None:
            return None
        return _bytes_to_reservation(existing_raw)

    async def has_identity(self, key: str) -> TaskReservation | None:
        """Return the reservation owning a reserved forever identity, if any."""
        raw = self.ring.get_record(f"res:{key}")
        if raw is None:
            return None
        return _bytes_to_reservation(raw)

    async def reset_identity(self, key: str, *, expected_task_id: UUID | None = None) -> bool:
        """Delete a forever identity reservation, optionally fenced by expected_task_id."""
        ring = self.ring
        res_key = f"res:{key}"
        if expected_task_id is None:
            return ring.delete_record(res_key)
        raw = ring.get_record(res_key)
        if raw is None:
            return False
        existing = _bytes_to_reservation(raw)
        if existing.task_id != expected_task_id:
            return False
        return ring.cas_record(res_key, raw, None)

    async def notify_new_task(self, record: QueuedTaskRecord) -> None:
        """Signal local and cross-process worker waiters when record is due."""
        if record.status in {"pending", "scheduled"} and record.is_due:
            self._notification_event.set()
            self._emit_wakeup()
            cast("Any", self)._record_wakeup_emitted()

    async def notify_new_tasks(self, records: Sequence[QueuedTaskRecord]) -> None:
        """Emit one worker-wakeup hint for a batch of newly available tasks."""
        due = tuple(record for record in records if record.status in {"pending", "scheduled"} and record.is_due)
        if due:
            await self.notify_new_task(due[0])
            cast("Any", self)._record_wakeup_coalesced(len(due) - 1)

    async def wait_for_wakeups(self, timeout: float | None = None) -> bool:
        """Wait for local or cross-process task notifications with adaptive clamping."""
        if self._notification_event.is_set():
            self._notification_event.clear()
            self._drain_wakeups()
            return True
        ring = self.ring
        if ring.pop() is not None:
            self._drain_wakeups()
            return True

        if self._adaptive_clamping:
            requested = self._max_wakeup_timeout if timeout is None else timeout
            effective_timeout = min(max(requested, self._min_wakeup_timeout), self._max_wakeup_timeout)
        else:
            effective_timeout = self._max_wakeup_timeout if timeout is None else max(0.0, timeout)

        if effective_timeout <= 0.0:
            return False

        deadline = time.monotonic() + effective_timeout
        while True:
            if self._notification_event.is_set():
                self._notification_event.clear()
                self._drain_wakeups()
                return True
            if ring.pop() is not None:
                self._drain_wakeups()
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                return False
            slice_ms = max(1, int(min(remaining, 0.05) * 1000))
            event_task = asyncio.create_task(self._notification_event.wait())
            ring_task = asyncio.create_task(asyncio.to_thread(ring.wait, slice_ms))
            try:
                done, pending = await asyncio.wait(
                    {event_task, ring_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                if event_task in done:
                    self._notification_event.clear()
                    self._drain_wakeups()
                    return True
                if ring_task in done and ring_task.result():
                    self._drain_wakeups()
                    return True
            except BaseException:
                for task in (event_task, ring_task):
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                raise

    async def time_until_next_due(self, *, queues: tuple[str, ...] = ()) -> float | None:
        """Return seconds until the earliest not-yet-due pending or scheduled record."""
        now = _utc_now()
        upcoming = [
            record.scheduled_at
            for _, _, record in self._all_records_with_raw()
            if record.status in {"pending", "scheduled"}
            and record.scheduled_at is not None
            and record.scheduled_at > now
            and (not queues or record.queue in queues)
        ]
        if not upcoming:
            return None
        return max((min(upcoming) - _utc_now()).total_seconds(), 0.0)

    async def wait_for_completion(self, task_id: UUID, *, timeout: float | None = None) -> bool:
        """Wait until task_id reaches a terminal status or timeout elapses."""
        current = await self.get_task(task_id)
        if current is not None and current.is_terminal:
            return True
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        ring = self.ring
        while True:
            current = await self.get_task(task_id)
            if current is not None and current.is_terminal:
                return True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                slice_ms = max(1, int(min(remaining, 0.05) * 1000))
            else:
                slice_ms = 50
            await asyncio.to_thread(ring.wait, slice_ms)


class _SHMExecutionBackendImpl:
    """Vertebra shared-memory execution backend with zero-cold-start execution."""

    def __init__(self) -> None:
        """Initialize the shared-memory execution backend."""
        exec_mod = ensure_module("litestar_queues.execution.base", package="litestar-queues", extra="queues")
        exec_mod.BaseExecutionBackend.__init__(self)
        self._warm_contexts: dict[str, Any] = {}

    async def execute(
        self,
        service: QueueService,
        record: QueuedTaskRecord,
        *,
        worker_id: str | None = None,
    ) -> QueuedTaskRecord:
        """Execute the task record directly within the current process."""
        return await service.execute_record(record, worker_id=worker_id)


if TYPE_CHECKING:

    class SHMQueueBackend(_SHMQueueBackendImpl, _BaseQueueBackend):
        """Vertebra shared-memory queue backend with adaptive wakeup clamping."""

    class SHMExecutionBackend(_SHMExecutionBackendImpl, _BaseExecutionBackend):
        """Vertebra shared-memory execution backend with zero-cold-start execution."""


def _get_shm_queue_backend_cls() -> type[SHMQueueBackend]:
    global _CACHED_SHM_QUEUE_BACKEND_CLS
    if _CACHED_SHM_QUEUE_BACKEND_CLS is None:
        base_mod = ensure_module("litestar_queues.backends.base", package="litestar-queues", extra="queues")
        factory_mod = ensure_module("litestar_queues.backends.factory", package="litestar-queues", extra="queues")
        cls = type(
            "SHMQueueBackend",
            (_SHMQueueBackendImpl, base_mod.BaseQueueBackend),
            {
                "__module__": __name__,
                "__doc__": _SHMQueueBackendImpl.__doc__,
                "__slots__": _SHM_QUEUE_SLOTS,
            },
        )
        factory_mod.queue_backend("vertebra_shm")(cls)
        _CACHED_SHM_QUEUE_BACKEND_CLS = cls
    return cast("type[SHMQueueBackend]", _CACHED_SHM_QUEUE_BACKEND_CLS)


def _get_shm_execution_backend_cls() -> type[SHMExecutionBackend]:
    global _CACHED_SHM_EXECUTION_BACKEND_CLS
    if _CACHED_SHM_EXECUTION_BACKEND_CLS is None:
        exec_mod = ensure_module("litestar_queues.execution.base", package="litestar-queues", extra="queues")
        _CACHED_SHM_EXECUTION_BACKEND_CLS = type(
            "SHMExecutionBackend",
            (_SHMExecutionBackendImpl, exec_mod.BaseExecutionBackend),
            {
                "__module__": __name__,
                "__doc__": _SHMExecutionBackendImpl.__doc__,
                "__slots__": _SHM_EXEC_SLOTS,
            },
        )
    return cast("type[SHMExecutionBackend]", _CACHED_SHM_EXECUTION_BACKEND_CLS)


def __getattr__(name: str) -> Any:
    """Lazily resolve SHMQueueBackend and SHMExecutionBackend against litestar_queues."""
    if name == "SHMQueueBackend":
        return _get_shm_queue_backend_cls()
    if name == "SHMExecutionBackend":
        return _get_shm_execution_backend_cls()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound SHM queue classes."""
    return sorted(set(globals()) | set(__all__))


if import_optional("litestar_queues") is not None:
    _get_shm_queue_backend_cls()
