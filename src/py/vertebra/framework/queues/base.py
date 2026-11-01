"""Core protocols, value objects, in-memory reference backend, and service for distributed queues."""

from __future__ import annotations

import asyncio
import collections
import inspect
import random
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, runtime_checkable

import msgspec

from vertebra.telemetry.otel import TraceContext
from vertebra.typing import UnsupportedBackendError
from vertebra.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = (
    "CronLease",
    "CronSchedule",
    "DeadLetterEntry",
    "MemoryQueueBackend",
    "QueueBackend",
    "QueueConfig",
    "QueueService",
    "TaskDef",
    "TaskEnvelope",
    "TaskLane",
    "TaskRetryPolicy",
    "task",
)

type TaskLane = Literal["cpu", "io", "agent"]


class TaskRetryPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Retry and exponential backoff policy for background tasks."""

    max_retries: int = 3
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_ratio: float = 0.1

    def compute_delay(self, attempt: int) -> float:
        """Compute exponential backoff delay with jitter for a given retry attempt."""
        exponent = max(0, attempt - 1)
        base_delay = min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (self.backoff_multiplier**exponent),
        )
        if self.jitter_ratio <= 0.0:
            return max(0.0, base_delay)
        jitter_range = base_delay * self.jitter_ratio
        jittered = base_delay + random.uniform(-jitter_range, jitter_range)
        return max(0.0, min(self.max_backoff_seconds, jittered))


class CronSchedule(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Recurring cron schedule descriptor for background jobs."""

    expression: str
    timezone: str = "UTC"


class TaskDef(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Registered background task definition bound to a worker execution lane."""

    name: str
    fn: Callable[..., Any]
    lane: TaskLane = "io"
    retry_policy: TaskRetryPolicy = msgspec.field(default_factory=TaskRetryPolicy)
    cron: CronSchedule | None = None
    timeout_seconds: float = 300.0


class TaskEnvelope(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Wire-serializable background task execution envelope."""

    task_id: str
    task_name: str
    lane: TaskLane
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)
    attempt: int = 1
    max_retries: int = 3
    enqueued_at: float = 0.0
    scheduled_at: float | None = None
    idempotency_key: str | None = None
    traceparent: str | None = None
    receipt_handle: str | None = None


class DeadLetterEntry(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Dead-letter queue payload capturing exhausted task envelopes and error metadata."""

    dlq_id: str
    envelope: TaskEnvelope
    error_type: str
    error_message: str
    failed_at: float


class CronLease(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Distributed leader lease metadata for cron tick scheduling."""

    lease_key: str
    owner_id: str
    expires_at: float
    last_slot: str | None = None


class QueueConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for QueueService across memory, Valkey, Redis, Postgres, and Oracle."""

    backend_url: str = "memory://"
    default_lane: TaskLane = "io"
    max_concurrency: int = 32
    visibility_timeout_seconds: float = 60.0
    poll_interval_seconds: float = 0.25
    consumer_group: str = "vertebra-workers"
    consumer_name: str = "worker-0"
    stream_prefix: str = "vtb:queue"
    dlq_max_len: int = 10_000
    lease_ttl_seconds: float = 30.0


@runtime_checkable
class QueueBackend(Protocol):
    """Asynchronous pluggable queue backend contract."""

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue a task envelope for worker delivery."""
        ...

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim a batch of ready task envelopes across the specified lanes."""
        ...

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge successful completion of a task envelope."""
        ...

    async def nack(
        self,
        envelope: TaskEnvelope,
        *,
        error: Exception | str,
        delay_seconds: float | None = None,
        send_to_dlq: bool = False,
    ) -> DeadLetterEntry | None:
        """Negatively acknowledge a task envelope, triggering delayed retry or DLQ routing."""
        ...

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due delayed and scheduled jobs into active lane queues."""
        ...

    async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
        """List dead-letter queue entries up to limit."""
        ...

    async def replay_dlq(self, dlq_id: str) -> TaskEnvelope | None:
        """Replay a dead-letter queue entry by re-enqueuing it into its active lane."""
        ...

    async def acquire_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Attempt to acquire a distributed leader lease for a cron scheduling slot."""
        ...

    async def renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        slot: str | None = None,
    ) -> CronLease | None:
        """Attempt to renew an existing distributed leader lease."""
        ...

    async def close(self) -> None:
        """Release underlying network and pool resources."""
        ...


class MemoryQueueBackend:
    """In-memory reference QueueBackend implementation supporting visibility timeout and DLQ."""

    __slots__ = (
        "_config",
        "_dlq",
        "_idempotency_keys",
        "_in_flight",
        "_lanes",
        "_leases",
        "_scheduled",
    )

    def __init__(self, config: QueueConfig | None = None) -> None:
        self._config = config or QueueConfig()
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

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue an envelope into scheduled storage or active lane deque."""
        if envelope.idempotency_key:
            if envelope.idempotency_key in self._idempotency_keys:
                return envelope
            self._idempotency_keys.add(envelope.idempotency_key)

        now = time.time()
        if envelope.scheduled_at is not None and envelope.scheduled_at > now:
            self._scheduled.append(envelope)
            self._scheduled.sort(key=lambda env: env.scheduled_at or 0.0)
            return envelope

        lane = envelope.lane if envelope.lane in self._lanes else self._config.default_lane
        self._lanes[lane].append(envelope)
        return envelope

    async def dequeue(
        self,
        lanes: Sequence[TaskLane],
        *,
        batch_size: int = 1,
        timeout_seconds: float = 0.0,
    ) -> list[TaskEnvelope]:
        """Claim ready tasks across lanes, reclaiming visibility-timed-out unacked tasks first."""
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
                now + self._config.visibility_timeout_seconds,
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
                    now + self._config.visibility_timeout_seconds,
                )
                claimed.append(active_env)

        return claimed

    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge completed task by removing receipt handle from in-flight registry."""
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
            self._lanes[envelope.lane].append(rescheduled_env)
        return None

    async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
        """Promote due scheduled envelopes to active lanes."""
        current_time = now if now is not None else time.time()
        promoted = 0
        remaining: list[TaskEnvelope] = []
        for env in self._scheduled:
            if promoted < batch_size and env.scheduled_at is not None and env.scheduled_at <= current_time:
                lane = env.lane if env.lane in self._lanes else self._config.default_lane
                self._lanes[lane].append(env)
                promoted += 1
            else:
                remaining.append(env)
        self._scheduled = remaining
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
        lane = replayed_env.lane if replayed_env.lane in self._lanes else self._config.default_lane
        self._lanes[lane].append(replayed_env)
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

    async def close(self) -> None:
        """In-memory cleanup."""
        self._in_flight.clear()
        self._scheduled.clear()
        self._idempotency_keys.clear()
        self._dlq.clear()
        self._leases.clear()


class QueueService:
    """Multi-lane background task queue service supporting pluggable distributed backends."""

    __slots__ = ("_backend", "_config", "_tasks")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        backend: QueueBackend | None = None,
    ) -> None:
        self._config = config or QueueConfig()
        self._tasks: dict[str, TaskDef] = {}
        self._backend: QueueBackend = backend or self._build_backend(self._config)

    @property
    def config(self) -> QueueConfig:
        """Return the active QueueConfig."""
        return self._config

    @property
    def backend(self) -> QueueBackend:
        """Return the underlying active QueueBackend instance."""
        return self._backend

    @property
    def registered_tasks(self) -> dict[str, TaskDef]:
        """Return registered task definitions keyed by name."""
        return dict(self._tasks)

    def register(self, task_def: TaskDef) -> TaskDef:
        """Register a TaskDef with the queue service."""
        self._tasks[task_def.name] = task_def
        return task_def

    async def enqueue(
        self,
        task_name: str,
        *args: Any,
        lane: TaskLane | None = None,
        delay_seconds: float | None = None,
        scheduled_at: float | None = None,
        idempotency_key: str | None = None,
        **kwargs: Any,
    ) -> TaskEnvelope:
        """Enqueue a task by name with optional scheduling, lane override, and idempotency key."""
        if task_name not in self._tasks:
            msg = f"Task {task_name!r} is not registered"
            raise KeyError(msg)
        tdef = self._tasks[task_name]
        now = time.time()
        computed_sched = scheduled_at or (now + delay_seconds if delay_seconds and delay_seconds > 0 else None)
        active_trace = TraceContext.current()
        envelope = TaskEnvelope(
            task_id=uuid.uuid4().hex,
            task_name=task_name,
            lane=lane or tdef.lane,
            args=args,
            kwargs=kwargs,
            attempt=1,
            max_retries=tdef.retry_policy.max_retries,
            enqueued_at=now,
            scheduled_at=computed_sched,
            idempotency_key=idempotency_key,
            traceparent=active_trace.format() if active_trace else None,
        )
        return await self._backend.enqueue(envelope)

    async def enqueue_delayed(
        self,
        delay_seconds: float,
        task_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> TaskEnvelope:
        """Enqueue a task to be executed after delay_seconds elapses."""
        return await self.enqueue(task_name, *args, delay_seconds=delay_seconds, **kwargs)

    async def process_envelope(self, envelope: TaskEnvelope) -> Any:
        """Execute callable bound to envelope, settling with ack or nack upon completion."""
        tdef = self._tasks[envelope.task_name]
        try:
            result = tdef.fn(*envelope.args, **envelope.kwargs)
            if inspect.isawaitable(result):
                result = await result
            await self._backend.ack(envelope)
            return result
        except Exception as exc:
            delay = tdef.retry_policy.compute_delay(envelope.attempt)
            send_dlq = envelope.attempt >= envelope.max_retries
            await self._backend.nack(
                envelope,
                error=exc,
                delay_seconds=delay,
                send_to_dlq=send_dlq,
            )
            raise

    async def drain_once(
        self,
        lanes: Sequence[TaskLane] | None = None,
        *,
        batch_size: int = 100,
    ) -> list[Any]:
        """Execute ready tasks across selected lanes in FIFO order and return results."""
        target_lanes = lanes or ("cpu", "io", "agent")
        await self._backend.promote_scheduled()
        envelopes = await self._backend.dequeue(target_lanes, batch_size=batch_size)
        results: list[Any] = []
        for env in envelopes:
            out = await self.process_envelope(env)
            results.append(out)
        return results

    async def run_next(self) -> Any:
        """Pop and execute the next ready task across all lanes, retrying on transient failure."""
        target_lanes: tuple[TaskLane, ...] = ("cpu", "io", "agent")
        await self._backend.promote_scheduled()
        envelopes = await self._backend.dequeue(target_lanes, batch_size=1)
        if not envelopes:
            return None
        envelope = envelopes[0]
        tdef = self._tasks[envelope.task_name]
        attempt = envelope.attempt
        while True:
            try:
                result = tdef.fn(*envelope.args, **envelope.kwargs)
                if inspect.isawaitable(result):
                    result = await result
                await self._backend.ack(envelope)
                return result
            except Exception as exc:
                if attempt > tdef.retry_policy.max_retries:
                    await self._backend.nack(envelope, error=exc, send_to_dlq=True)
                    raise
                delay = tdef.retry_policy.compute_delay(attempt)
                if delay > 0:
                    await asyncio.sleep(delay)
                attempt += 1

    async def run_due_schedules(
        self,
        *,
        minute: int = 0,
        hour: int = 0,
        day_of_month: int = 1,
        month: int = 1,
        day_of_week: int = 0,
    ) -> list[Any]:
        """Enqueue and execute all registered cron tasks matching the specified timestamp components."""
        enqueued = 0
        for tdef in self._tasks.values():
            if tdef.cron is not None and _cron_matches(
                tdef.cron.expression,
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month=month,
                day_of_week=day_of_week,
            ):
                await self.enqueue(tdef.name)
                enqueued += 1
        if enqueued == 0:
            return []
        return await self.drain_once(batch_size=enqueued)

    async def close(self) -> None:
        """Close underlying backend resources."""
        await self._backend.close()

    @staticmethod
    def _build_backend(config: QueueConfig) -> QueueBackend:
        """Construct matching QueueBackend based on URL scheme."""
        parsed = urllib.parse.urlparse(config.backend_url)
        scheme = parsed.scheme.lower()
        if scheme in ("", "memory"):
            return MemoryQueueBackend(config)
        if scheme in ("redis", "rediss"):
            backend_cls = import_string("vertebra.framework.queues.redis.RedisQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("valkey", "valkeys"):
            backend_cls = import_string("vertebra.framework.queues.redis.ValkeyQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("postgresql", "postgres"):
            backend_cls = import_string("vertebra.framework.queues.postgres.PostgreSQLQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("oracle",):
            backend_cls = import_string("vertebra.framework.queues.oracle.OracleQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        msg = f"Unsupported queue backend URL scheme: {scheme!r}"
        raise UnsupportedBackendError(msg)


def _cron_matches_field(field_spec: str, value: int) -> bool:
    """Check whether a single cron field expression matches an integer value."""
    if field_spec == "*":
        return True
    if field_spec.startswith("*/"):
        step = int(field_spec[2:])
        return step > 0 and value % step == 0
    for part in field_spec.split(","):
        if "-" in part:
            low_s, high_s = part.split("-", 1)
            if int(low_s) <= value <= int(high_s):
                return True
        elif int(part) == value:
            return True
    return False


def _cron_matches(
    expression: str,
    *,
    minute: int = 0,
    hour: int = 0,
    day_of_month: int = 1,
    month: int = 1,
    day_of_week: int = 0,
) -> bool:
    """Evaluate a 5-field cron expression against explicit time fields."""
    fields = expression.strip().split()
    if len(fields) != 5:
        return False
    values = (minute, hour, day_of_month, month, day_of_week)
    return all(_cron_matches_field(spec, val) for spec, val in zip(fields, values, strict=True))


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry_policy: TaskRetryPolicy | None = None,
    retry: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
    timeout_seconds: float = 300.0,
) -> Any:
    """Decorator registering a callable as a TaskDef."""
    effective_retry = retry_policy or retry or TaskRetryPolicy()

    def _decorator(target: Callable[..., Any]) -> TaskDef:
        return TaskDef(
            name=name or target.__name__,
            fn=target,
            lane=lane,
            retry_policy=effective_retry,
            cron=cron,
            timeout_seconds=timeout_seconds,
        )

    if fn is not None:
        return _decorator(fn)
    return _decorator
