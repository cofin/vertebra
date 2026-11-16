"""Aggregated queue backend exports and first-party shared-memory queue implementation."""

from __future__ import annotations

import asyncio
import collections
import time
import uuid
from typing import TYPE_CHECKING

from vertebra.framework.queues.base import (
    CronLease,
    DeadLetterEntry,
    MemoryQueueBackend,
    QueueBackend,
    QueueBackendSettings,
    QueueConfig,
    TaskEnvelope,
    TaskLane,
    TaskResultRecord,
)
from vertebra.framework.queues.oracle import OracleQueueBackend, OracleQueueExecutor
from vertebra.framework.queues.postgres import PostgreSQLQueueBackend, PostgreSQLQueueExecutor
from vertebra.framework.queues.redis import RedisQueueBackend
from vertebra.framework.queues.spanner import SpannerQueueBackend, SpannerQueueExecutor
from vertebra.framework.queues.valkey import ValkeyQueueBackend

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "MemoryQueueBackend",
    "OracleQueueBackend",
    "OracleQueueExecutor",
    "PostgreSQLQueueBackend",
    "PostgreSQLQueueExecutor",
    "QueueBackend",
    "RedisQueueBackend",
    "SHMQueueBackend",
    "SpannerQueueBackend",
    "SpannerQueueExecutor",
    "ValkeyQueueBackend",
)


class SHMQueueBackend:
    """First-party shared-memory queue backend with visibility leases, DLQ, and result storage."""

    __slots__ = (
        "_config",
        "_dlq",
        "_idempotency_keys",
        "_in_flight",
        "_lanes",
        "_leases",
        "_results",
        "_scheduled",
        "_visibility_timeout_seconds",
        "_wakeup_event",
    )

    def __init__(self, config: QueueConfig | QueueBackendSettings | None = None) -> None:
        """Initialize the shared-memory queue backend with configurable visibility timeout."""
        self._config = config or QueueBackendSettings(kind="shm")
        self._lanes: dict[TaskLane, collections.deque[TaskEnvelope]] = {
            "cpu": collections.deque(),
            "io": collections.deque(),
            "agent": collections.deque(),
        }
        self._in_flight: dict[str, tuple[TaskEnvelope, float]] = {}
        self._scheduled: list[TaskEnvelope] = []
        self._idempotency_keys: set[str] = set()
        self._dlq: dict[str, DeadLetterEntry] = {}
        self._leases: dict[str, CronLease] = {}
        self._results: dict[str, TaskResultRecord] = {}
        self._wakeup_event: asyncio.Event = asyncio.Event()
        if isinstance(self._config, QueueBackendSettings):
            self._visibility_timeout_seconds = self._config.visibility_timeout_seconds
        else:
            self._visibility_timeout_seconds = self._config.visibility_timeout_seconds

    @property
    def visibility_timeout_seconds(self) -> float:
        """Return the visibility timeout in seconds."""
        return self._visibility_timeout_seconds

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue an envelope into scheduled storage or active lane deque, waking waiters."""
        if envelope.idempotency_key:
            if envelope.idempotency_key in self._idempotency_keys:
                return envelope
            self._idempotency_keys.add(envelope.idempotency_key)

        now = time.time()
        if envelope.scheduled_at is not None and envelope.scheduled_at > now:
            self._scheduled.append(envelope)
            self._scheduled.sort(key=lambda env: env.scheduled_at or 0.0)
            self._wakeup_event.set()
            return envelope

        lane = envelope.lane if envelope.lane in self._lanes else "io"
        self._lanes[lane].append(envelope)
        self._wakeup_event.set()
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim ready tasks across lanes with visibility timeout leasing and wakeup waits."""
        await self.promote_scheduled()
        now = time.time()
        claimed: list[TaskEnvelope] = []
        target_lanes = set(lanes) if lanes else set(self._lanes.keys())

        expired_handles = [
            handle
            for handle, (env, locked_until) in self._in_flight.items()
            if locked_until <= now and env.lane in target_lanes
        ]
        for handle in expired_handles:
            if len(claimed) >= batch_size:
                break
            env, _ = self._in_flight.pop(handle)
            new_handle = uuid.uuid4().hex
            reclaimed_env = TaskEnvelope(
                task_id=env.task_id,
                task_name=env.task_name,
                lane=env.lane,
                args=env.args,
                kwargs=env.kwargs,
                attempt=env.attempt + 1,
                max_retries=env.max_retries,
                enqueued_at=env.enqueued_at,
                scheduled_at=env.scheduled_at,
                idempotency_key=env.idempotency_key,
                traceparent=env.traceparent,
                receipt_handle=new_handle,
            )
            self._in_flight[new_handle] = (
                reclaimed_env,
                now + self._visibility_timeout_seconds,
            )
            claimed.append(reclaimed_env)

        for lane in lanes:
            if len(claimed) >= batch_size:
                break
            queue = self._lanes.get(lane)
            if not queue:
                continue
            while queue and len(claimed) < batch_size:
                raw_env = queue.popleft()
                handle = uuid.uuid4().hex
                active_env = TaskEnvelope(
                    task_id=raw_env.task_id,
                    task_name=raw_env.task_name,
                    lane=raw_env.lane,
                    args=raw_env.args,
                    kwargs=raw_env.kwargs,
                    attempt=raw_env.attempt,
                    max_retries=raw_env.max_retries,
                    enqueued_at=raw_env.enqueued_at,
                    scheduled_at=raw_env.scheduled_at,
                    idempotency_key=raw_env.idempotency_key,
                    traceparent=raw_env.traceparent,
                    receipt_handle=handle,
                )
                self._in_flight[handle] = (
                    active_env,
                    now + self._visibility_timeout_seconds,
                )
                claimed.append(active_env)

        if not claimed and timeout_seconds > 0.0:
            self._wakeup_event.clear()
            try:
                await asyncio.wait_for(self._wakeup_event.wait(), timeout=timeout_seconds)
                return await self.dequeue(lanes, batch_size=batch_size, timeout_seconds=0.0)
            except TimeoutError:
                return []

        return claimed

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge completed task by clearing receipt handle from in-flight registry."""
        if envelope.receipt_handle:
            self._in_flight.pop(envelope.receipt_handle, None)

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Negatively acknowledge task, routing to DLQ when exhausted or re-enqueuing for retry."""
        if envelope.receipt_handle:
            self._in_flight.pop(envelope.receipt_handle, None)

        err_type = type(error).__name__ if isinstance(error, Exception) else "TaskExecutionError"
        err_msg = str(error)

        if send_to_dlq or envelope.attempt >= envelope.max_retries:
            dlq_id = uuid.uuid4().hex
            entry = DeadLetterEntry(
                dlq_id=dlq_id,
                envelope=envelope,
                error_type=err_type,
                error_message=err_msg,
                failed_at=time.time(),
            )
            self._dlq[dlq_id] = entry
            return entry

        retry_attempt = envelope.attempt + 1
        now = time.time()
        sched_at = now + (delay_seconds if delay_seconds is not None else 0.0)
        rescheduled_env = TaskEnvelope(
            task_id=envelope.task_id,
            task_name=envelope.task_name,
            lane=envelope.lane,
            args=envelope.args,
            kwargs=envelope.kwargs,
            attempt=retry_attempt,
            max_retries=envelope.max_retries,
            enqueued_at=envelope.enqueued_at,
            scheduled_at=sched_at if delay_seconds and delay_seconds > 0 else None,
            idempotency_key=envelope.idempotency_key,
            traceparent=envelope.traceparent,
            receipt_handle=None,
        )
        if delay_seconds and delay_seconds > 0:
            self._scheduled.append(rescheduled_env)
            self._scheduled.sort(key=lambda env: env.scheduled_at or 0.0)
        else:
            lane = envelope.lane if envelope.lane in self._lanes else "io"
            self._lanes[lane].append(rescheduled_env)
        self._wakeup_event.set()
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled envelopes to active lanes."""
        current_time = now if now is not None else time.time()
        promoted = 0
        remaining: list[TaskEnvelope] = []
        for env in self._scheduled:
            if promoted < batch_size and env.scheduled_at is not None and env.scheduled_at <= current_time:
                lane = env.lane if env.lane in self._lanes else "io"
                self._lanes[lane].append(env)
                promoted += 1
            else:
                remaining.append(env)
        self._scheduled = remaining
        if promoted > 0:
            self._wakeup_event.set()
        return promoted

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """List dead-letter entries up to limit."""
        return list(self._dlq.values())[:limit]

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Replay dead-letter entry by re-enqueuing with reset attempt count."""
        entry = self._dlq.pop(dlq_id, None)
        if not entry:
            return None
        replayed_env = TaskEnvelope(
            task_id=entry.envelope.task_id,
            task_name=entry.envelope.task_name,
            lane=entry.envelope.lane,
            args=entry.envelope.args,
            kwargs=entry.envelope.kwargs,
            attempt=1,
            max_retries=entry.envelope.max_retries,
            enqueued_at=time.time(),
            scheduled_at=None,
            idempotency_key=entry.envelope.idempotency_key,
            traceparent=entry.envelope.traceparent,
            receipt_handle=None,
        )
        lane = replayed_env.lane if replayed_env.lane in self._lanes else "io"
        self._lanes[lane].append(replayed_env)
        self._wakeup_event.set()
        return replayed_env

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Acquire lease if free, expired, or owned by caller."""
        now = time.time()
        existing = self._leases.get(lease_key)
        if existing is None or existing.expires_at <= now or existing.owner_id == owner_id:
            lease = CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=now + ttl_seconds,
                last_slot=slot or (existing.last_slot if existing else None),
            )
            self._leases[lease_key] = lease
            return lease
        return None

    async def renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Renew lease only if owner_id matches existing holder."""
        now = time.time()
        existing = self._leases.get(lease_key)
        if existing is not None and existing.owner_id == owner_id:
            lease = CronLease(
                lease_key=lease_key,
                owner_id=owner_id,
                expires_at=now + ttl_seconds,
                last_slot=slot or existing.last_slot,
            )
            self._leases[lease_key] = lease
            return lease
        return None

    def store_result(self, record: TaskResultRecord) -> None:
        """Store a task execution result synchronously."""
        self._results[record.task_id] = record

    async def astore_result(self, record: TaskResultRecord) -> None:
        """Store a task execution result asynchronously."""
        self.store_result(record)

    def get_result(self, task_id: str) -> TaskResultRecord | None:
        """Retrieve a task execution result synchronously."""
        return self._results.get(task_id)

    async def aget_result(self, task_id: str) -> TaskResultRecord | None:
        """Retrieve a task execution result asynchronously."""
        return self.get_result(task_id)

    async def close(self) -> None:
        """Clear all internal queues, in-flight entries, DLQ, and lease records."""
        self._in_flight.clear()
        self._scheduled.clear()
        self._idempotency_keys.clear()
        self._dlq.clear()
        self._leases.clear()
        self._results.clear()
