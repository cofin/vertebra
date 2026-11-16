"""Store-backed queue backend and container dependency provider for litestar-queues."""

from __future__ import annotations

import asyncio
import contextlib
import inspect as _inspect
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import UUID, uuid4

import msgspec

from vertebra.db.driver.capabilities import AsyncNotificationSource, AsyncQueueStore, QueueEntry
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
    from litestar_queues.backends.base import BaseQueueBackend
    from litestar_queues.backends.factory import queue_backend
    from litestar_queues.models import (
        HeartbeatTouch,
        HeartbeatTouchResult,
        QueueBackendCapabilities,
        QueuedTaskRecord,
        QueueStatistics,
        StaleTaskRecoveryResult,
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
    retry_count = max(0, entry.attempt - 1)
    max_retries = max(0, entry.max_attempts - 1)
    status = _resolve_record_status(entry, store_status)
    if entry.payload:
        data = decode_json(entry.payload)
        if isinstance(data, dict):
            task_id = UUID(data["id"]) if "id" in data else UUID(entry.job_id)
            return QueuedTaskRecord(
                task_name=data.get("task_name", entry.task_name),
                id=task_id,
                args=tuple(data.get("args", ())),
                kwargs=dict(data.get("kwargs", {})),
                queue=data.get("queue", entry.queue),
                status=status,
                priority=data.get("priority", 0),
                max_retries=max_retries,
                retry_count=retry_count,
                scheduled_at=sched,
                key=data.get("key") or entry.headers.get("key"),
                metadata=dict(data.get("metadata", {})),
            )
    return QueuedTaskRecord(
        task_name=entry.task_name,
        id=UUID(entry.job_id),
        queue=entry.queue,
        status=status,
        max_retries=max_retries,
        retry_count=retry_count,
        scheduled_at=sched,
        key=entry.headers.get("key"),
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


@queue_backend("vertebra_store")
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
        record_id = id or uuid4()
        record = QueuedTaskRecord(
            task_name=task_name,
            id=record_id,
            args=args,
            kwargs=kwargs or {},
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            key=key,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            metadata=metadata or {},
        )
        record_dict = {
            "id": str(record.id),
            "task_name": record.task_name,
            "args": list(record.args),
            "kwargs": record.kwargs,
            "queue": record.queue,
            "status": record.status,
            "priority": record.priority,
            "max_retries": record.max_retries,
            "retry_count": record.retry_count,
            "scheduled_at": record.scheduled_at.isoformat() if record.scheduled_at else None,
            "key": record.key,
            "metadata": record.metadata,
        }
        sched_ms = int(scheduled_at.timestamp() * 1000) if scheduled_at else None
        headers = {"key": key} if key else {}
        entry = QueueEntry(
            job_id=str(record.id),
            queue=queue,
            task_name=task_name,
            payload=encode_json_bytes(record_dict),
            attempt=1,
            max_attempts=max_retries + 1,
            scheduled_at_ms=sched_ms,
            headers=headers,
        )
        await self._store.enqueue(entry)
        if self._notification_source is not None:
            await self._notification_source.publish("vertebra_queue_wakeup", queue)
        return record

    async def enqueue_many(
        self,
        tasks: Sequence[Any],
    ) -> list[QueuedTaskRecord]:
        """Bulk-enqueue task specifications into the backing store."""
        records: list[QueuedTaskRecord] = []
        entries: list[QueueEntry] = []
        for t in tasks:
            task_name = getattr(t, "task_name", "task")
            args = getattr(t, "args", ())
            kwargs = getattr(t, "kwargs", {})
            queue = getattr(t, "queue", "default")
            record_id = getattr(t, "id", None) or uuid4()
            sched = getattr(t, "scheduled_at", None)
            max_retries = getattr(t, "max_retries", 0)
            key = getattr(t, "key", None)
            record = QueuedTaskRecord(
                task_name=task_name,
                id=record_id,
                args=args,
                kwargs=kwargs or {},
                queue=queue,
                scheduled_at=sched,
                max_retries=max_retries,
                key=key,
            )
            records.append(record)
            record_dict = {
                "id": str(record.id),
                "task_name": record.task_name,
                "args": list(record.args),
                "kwargs": record.kwargs,
                "queue": record.queue,
                "status": record.status,
                "max_retries": record.max_retries,
                "retry_count": record.retry_count,
            }
            sched_ms = int(sched.timestamp() * 1000) if sched else None
            entries.append(
                QueueEntry(
                    job_id=str(record.id),
                    queue=queue,
                    task_name=task_name,
                    payload=encode_json_bytes(record_dict),
                    attempt=1,
                    max_attempts=max_retries + 1,
                    scheduled_at_ms=sched_ms,
                )
            )
        await self._store.enqueue_many(entries)
        if self._notification_source is not None:
            queues = {r.queue for r in records}
            for q in queues:
                await self._notification_source.publish("vertebra_queue_wakeup", q)
        return records

    async def claim_many(
        self,
        *,
        limit: int,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
        queue_limits: Mapping[str, int] | None = None,
    ) -> list[QueuedTaskRecord]:
        """Claim ready tasks across specified queues."""
        _ = (execution_backend, queue_limits)
        target_queues = queues if queues else ("default",)
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
        claim_job_fn = getattr(self._store, "claim_job", None)
        if callable(claim_job_fn):
            claimed_entry = await claim_job_fn(
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
        _ = result
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
        return _entry_to_record(entry, store_status="completed") if entry is not None else None

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
            delay_s = max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
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
        """Extend visibility leases for in-flight tasks."""
        ids = [str(t.task_id) for t in touches]
        await self._store.heartbeat(ids, worker_id=worker_id or "worker", lease_seconds=30.0)
        return HeartbeatTouchResult(touched_task_ids={t.task_id for t in touches})

    async def requeue_stale_running(
        self,
        *,
        stale_after: timedelta = timedelta(seconds=300),
        limit: int | None = None,
    ) -> StaleTaskRecoveryResult:
        """Reap abandoned in-flight claims back into ready status."""
        reaped = await self._store.reap_stale(lease_seconds=stale_after.total_seconds())
        return StaleTaskRecoveryResult(requeued=reaped)

    async def cleanup_terminal(
        self,
        before: datetime,
        *,
        limit: int | None = None,
    ) -> int:
        """Clean up terminal queue records older than before."""
        return 0

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        """Retrieve a task record by UUID."""
        target_id = str(task_id)
        entry = await self._store.get_entry(target_id)
        if entry is None:
            return None
        status_fn = getattr(self._store, "get_entry_status", None)
        store_status = await status_fn(target_id) if callable(status_fn) else None
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
        """Retrieve a task record by idempotency key."""
        entries = await self._store.list_entries("default", limit=100)
        for e in entries:
            if e.headers.get("key") == key:
                return _entry_to_record(e)
        return None

    async def cancel_task(
        self,
        task_id: UUID,
        *,
        include_running: bool = False,
        expected_retry_count: int | None = None,
    ) -> bool:
        """Cancel a pending or scheduled task."""
        return await self._store.cancel(str(task_id))

    async def cancel_tasks(
        self,
        *,
        task_name: str | None = None,
        queue: str | None = None,
        kwargs: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        include_running: bool = False,
    ) -> int:
        """Cancel multiple pending or scheduled tasks."""
        entries = await self._store.list_entries(queue or "default", status="ready", limit=100)
        cancelled = 0
        for e in entries:
            if (task_name is None or e.task_name == task_name) and await self._store.cancel(e.job_id):
                cancelled += 1
        return cancelled

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> list[QueuedTaskRecord]:
        """List ready pending tasks."""
        _ = execution_backend
        entries = await self._store.list_entries(queue or "default", status="ready", limit=limit)
        return [_entry_to_record(e, store_status="ready") for e in entries]

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        """List completed tasks matching task_name."""
        _ = since
        entries = await self._store.list_entries("default", status="completed", limit=limit)
        return [_entry_to_record(e, store_status="completed") for e in entries if e.task_name == task_name]

    async def list_running_external(
        self,
        *,
        limit: int | None = None,
    ) -> list[QueuedTaskRecord]:
        """List running external tasks."""
        entries = await self._store.list_entries("default", status="claimed", limit=limit or 100)
        return [_entry_to_record(e, store_status="claimed") for e in entries]

    async def get_statistics(self, *, queue: str | None = None) -> QueueStatistics:
        """Return operational statistics for the specified queue across all lifecycle statuses."""
        target_queue = queue or "default"
        ready_entries = await self._store.list_entries(target_queue, status="ready", limit=1000)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        scheduled = sum(1 for e in ready_entries if e.scheduled_at_ms is not None and e.scheduled_at_ms > now_ms)
        pending = len(ready_entries) - scheduled
        claimed_entries = await self._store.list_entries(target_queue, status="claimed", limit=1000)
        completed_entries = await self._store.list_entries(target_queue, status="completed", limit=1000)
        dead_entries = await self._store.list_entries(target_queue, status="dead_letter", limit=1000)
        cancelled_entries = await self._store.list_entries(target_queue, status="cancelled", limit=1000)
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
        """Reserve an idempotency key."""
        reserved = await self._store.reserve_unique_key(key, ttl_seconds=86400.0)
        if reserved:
            return TaskReservation(key=key, task_id=task_id, task_name=task_name, created_at=datetime.now(UTC))
        return None

    async def has_identity(self, key: str) -> TaskReservation | None:
        """Check if an idempotency key is active."""
        return None

    async def reset_identity(
        self,
        key: str,
        *,
        expected_task_id: UUID | None = None,
    ) -> bool:
        """Release an idempotency key reservation."""
        return await self._store.release_unique_key(key)

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

    async def close(self) -> None:
        """Close backing store resources."""
        if hasattr(self._store, "aclose"):
            await self._store.aclose()
        elif hasattr(self._store, "close"):
            res = self._store.close()
            if asyncio.iscoroutine(res):
                await res


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
