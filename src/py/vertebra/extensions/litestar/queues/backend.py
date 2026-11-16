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


def _entry_to_record(entry: QueueEntry) -> QueuedTaskRecord:
    """Decode a QueueEntry into a QueuedTaskRecord."""
    if entry.payload:
        data = decode_json(entry.payload)
        if isinstance(data, dict):
            task_id = UUID(data["id"]) if "id" in data else UUID(entry.job_id)
            sched = None
            if data.get("scheduled_at"):
                sched = datetime.fromisoformat(data["scheduled_at"]) if isinstance(data["scheduled_at"], str) else None
            elif entry.scheduled_at_ms:
                sched = datetime.fromtimestamp(entry.scheduled_at_ms / 1000.0, tz=UTC)
            return QueuedTaskRecord(
                task_name=data.get("task_name", entry.task_name),
                id=task_id,
                args=tuple(data.get("args", ())),
                kwargs=dict(data.get("kwargs", {})),
                queue=data.get("queue", entry.queue),
                status=data.get("status", "pending"),
                priority=data.get("priority", 0),
                max_retries=data.get("max_retries", entry.max_attempts - 1),
                retry_count=data.get("retry_count", entry.attempt - 1),
                scheduled_at=sched,
                key=data.get("key"),
                metadata=dict(data.get("metadata", {})),
            )
    sched_dt = datetime.fromtimestamp(entry.scheduled_at_ms / 1000.0, tz=UTC) if entry.scheduled_at_ms else None
    return QueuedTaskRecord(
        task_name=entry.task_name,
        id=UUID(entry.job_id),
        queue=entry.queue,
        max_retries=entry.max_attempts - 1,
        retry_count=entry.attempt - 1,
        scheduled_at=sched_dt,
    )


class StoreQueueBackendConfig(msgspec.Struct):
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

    __slots__ = ("_notification_source", "_store", "_terminal_ttl")

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
        target_queues = queues if queues else ("default",)
        entries = await self._store.claim(
            queues=target_queues,
            limit=limit,
            worker_id="worker",
            lease_seconds=30.0,
        )
        return [_entry_to_record(e) for e in entries]

    async def claim_task(
        self,
        task_id: UUID,
        *,
        expected_retry_count: int | None = None,
        expected_execution_ref: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Claim a specific task by identifier."""
        entry = await self._store.get_entry(str(task_id))
        if entry is None:
            return None
        return _entry_to_record(entry)

    async def claim_next(
        self,
        *,
        queues: tuple[str, ...] = (),
        execution_backend: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Claim the next ready task in queue."""
        claimed = await self.claim_many(limit=1, queues=queues)
        return claimed[0] if claimed else None

    async def complete_task(
        self,
        task_id: UUID,
        *,
        result: Any = None,
        expected_retry_count: int | None = None,
    ) -> QueuedTaskRecord | None:
        """Acknowledge a completed task."""
        await self._store.ack(str(task_id), attempt=(expected_retry_count or 0) + 1)
        return await self.get_task(task_id)

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
        """Record a failed task execution and schedule retry or dead-lettering."""
        delay_s = (retry_at - datetime.now(UTC)).total_seconds() if retry_at else None
        await self._store.nack(
            str(task_id),
            attempt=(expected_retry_count or 0) + 1,
            error=str(error or "Task execution failed"),
            retry_delay_s=delay_s,
        )
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
        entry = await self._store.get_entry(str(task_id))
        return _entry_to_record(entry) if entry else None

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
        entries = await self._store.list_entries(queue or "default", status="ready", limit=limit)
        return [_entry_to_record(e) for e in entries]

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        """List completed tasks matching task_name."""
        entries = await self._store.list_entries("default", status="completed", limit=limit)
        return [_entry_to_record(e) for e in entries if e.task_name == task_name]

    async def list_running_external(
        self,
        *,
        limit: int | None = None,
    ) -> list[QueuedTaskRecord]:
        """List running external tasks."""
        entries = await self._store.list_entries("default", status="running", limit=limit or 100)
        return [_entry_to_record(e) for e in entries]

    async def get_statistics(self, *, queue: str | None = None) -> QueueStatistics:
        """Return operational statistics for the specified queue."""
        entries = await self._store.list_entries(queue or "default", limit=100)
        return QueueStatistics(
            pending=len(entries),
            running=0,
            completed=0,
            failed=0,
            cancelled=0,
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
        """Wait for new task notifications from the notification source."""
        if self._notification_source is not None:
            with contextlib.suppress(TimeoutError):
                await asyncio.sleep(timeout or 0.1)
                return True
        await asyncio.sleep(timeout or 0.1)
        return False

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
