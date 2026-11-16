"""Core protocols, value objects, in-memory reference backend, and service for distributed queues."""

from __future__ import annotations

import asyncio
import collections
import inspect
import random
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, overload, runtime_checkable

import msgspec

from vertebra.telemetry.otel import TraceContext
from vertebra.typing import UnsupportedBackendError
from vertebra.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from vertebra.framework.di import Container
    from vertebra.framework.queues.scheduler import CronScheduler

__all__ = (
    "CronLease",
    "CronSchedule",
    "DeadLetterEntry",
    "MemoryQueueBackend",
    "QueueBackend",
    "QueueBackendSettings",
    "QueueConfig",
    "QueueService",
    "TaskDef",
    "TaskEnvelope",
    "TaskLane",
    "TaskResultRecord",
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


class TaskEnvelope(msgspec.Struct, frozen=True, kw_only=True, array_like=True):
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


class DeadLetterEntry(msgspec.Struct, frozen=True, kw_only=True, array_like=True):
    """Dead-letter queue payload capturing exhausted task envelopes and error metadata."""

    dlq_id: str
    envelope: TaskEnvelope
    error_type: str
    error_message: str
    failed_at: float


class TaskResultRecord(msgspec.Struct, frozen=True, kw_only=True):
    """Durable record of a completed or failed background task execution."""

    task_id: str
    status: str
    result: Any = None
    error: str | None = None
    completed_at: float = 0.0


class QueueBackendSettings(msgspec.Struct, frozen=True):
    """Declarative configuration settings for QueueService backend initialization."""

    kind: Literal[
        "postgres",
        "postgresql",
        "oracle",
        "redis",
        "valkey",
        "spanner",
        "spanner+googlesql",
        "spangres",
        "spanner+spangres",
        "shm",
        "memory",
    ] = "shm"
    """Queue backend identifier."""

    dsn: str | None = None
    """Optional connection URI overriding the default scheme."""

    visibility_timeout_seconds: float = 30.0
    """Visibility lease duration in seconds for claimed jobs."""

    poll_interval_seconds: float = 1.0
    """Fallback polling interval in seconds when no wakeup push channel exists."""

    consumer_name: str = "vertebra-worker"
    """Worker consumer identifier recorded on claimed jobs."""

    driver: Any = None
    """Optional pre-constructed asynchronous database driver."""

    sync_driver: Any = None
    """Optional pre-constructed synchronous database driver."""

    store: Any = None
    """Optional pre-constructed AsyncQueueStore."""

    sync_store: Any = None
    """Optional pre-constructed SyncQueueStore."""

    @property
    def backend_url(self) -> str:
        """Return the equivalent connection URL string."""
        if self.dsn:
            return self.dsn
        return f"{self.kind}://"


class CronLease(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Distributed leader lease metadata for cron tick scheduling."""

    lease_key: str
    owner_id: str
    expires_at: float
    last_slot: str | None = None


class QueueConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for QueueService across memory, Redis, Valkey, PostgreSQL, Oracle, and Spanner backends."""

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

    def enqueue_sync(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Synchronously enqueue an envelope into scheduled storage or active lane deque."""
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

    async def enqueue(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Enqueue an envelope into scheduled storage or active lane deque."""
        return self.enqueue_sync(envelope)

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

    __slots__ = ("_backend", "_config", "_container", "_scheduler", "_tasks")

    def __init__(
        self,
        config: QueueConfig | QueueBackendSettings | None = None,
        *,
        backend: QueueBackend | None = None,
        container: Container | None = None,
        scheduler: CronScheduler | None = None,
    ) -> None:
        self._config: QueueConfig | QueueBackendSettings = config or QueueBackendSettings()
        self._tasks: dict[str, TaskDef] = {}
        self._backend: QueueBackend = backend or self._build_backend(self._config)
        self._container = container
        self._scheduler = scheduler

    @property
    def config(self) -> QueueConfig | QueueBackendSettings:
        """Return the active QueueConfig or QueueBackendSettings."""
        return self._config

    @property
    def backend(self) -> QueueBackend:
        """Return the underlying active QueueBackend instance."""
        return self._backend

    @property
    def registered_tasks(self) -> dict[str, TaskDef]:
        """Return registered task definitions keyed by name."""
        return dict(self._tasks)

    @property
    def container(self) -> Container | None:
        """Return the bound Container if configured."""
        return self._container

    @property
    def scheduler(self) -> CronScheduler | None:
        """Return the bound CronScheduler if configured."""
        return self._scheduler

    def register(self, task_def: TaskDef) -> TaskDef:
        """Register a TaskDef with the queue service."""
        self._tasks[task_def.name] = task_def
        return task_def

    def enqueue_sync(
        self,
        task_name: str,
        *args: Any,
        lane: TaskLane | None = None,
        delay_seconds: float | None = None,
        scheduled_at: float | None = None,
        idempotency_key: str | None = None,
        **kwargs: Any,
    ) -> TaskEnvelope:
        """Synchronously enqueue a task by name with optional scheduling, lane override, and idempotency key."""
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
        sync_fn = getattr(self._backend, "enqueue_sync", None)
        if callable(sync_fn):
            return cast("TaskEnvelope", sync_fn(envelope))
        store = getattr(self._backend, "sync_store", None) or getattr(self._backend, "store", None)
        if store is not None and hasattr(store, "enqueue") and callable(store.enqueue):
            from vertebra.db.driver.capabilities import QueueEntry
            from vertebra.utils.serializers import encode_json_bytes

            sched_ms = int(envelope.scheduled_at * 1000) if envelope.scheduled_at is not None else None
            headers_dict: dict[str, str] = {}
            if envelope.idempotency_key:
                headers_dict["idempotency_key"] = envelope.idempotency_key
            entry = QueueEntry(
                job_id=envelope.task_id,
                queue=envelope.lane,
                task_name=envelope.task_name,
                payload=encode_json_bytes(envelope),
                attempt=envelope.attempt,
                max_attempts=envelope.max_retries,
                scheduled_at_ms=sched_ms,
                headers=headers_dict,
            )
            store.enqueue(entry)
            return envelope
        msg = f"Queue backend {type(self._backend).__name__} does not support synchronous enqueue"
        raise UnsupportedBackendError(msg)

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

    async def process_envelope(
        self,
        envelope: TaskEnvelope,
        *,
        container: Container | None = None,
    ) -> Any:
        """Execute callable bound to envelope, settling with ack or nack upon completion."""
        tdef = self._tasks[envelope.task_name]
        eff_container = container if container is not None else self._container
        try:
            if eff_container is not None:
                async with eff_container.scope(
                    extra_context={"envelope": envelope, "task_id": envelope.task_id}
                ) as scope:
                    from vertebra.framework.di import inspect_callable_bindings

                    bindings = inspect_callable_bindings(tdef.fn)
                    resolved_kwargs = dict(envelope.kwargs)
                    num_pos = len(envelope.args)
                    for idx, binding in enumerate(bindings):
                        if idx < num_pos:
                            continue
                        if binding.name in resolved_kwargs:
                            continue
                        dep_key = eff_container.graph.resolve_param_key(binding)
                        if dep_key is None:
                            if binding.name in eff_container.singletons:
                                dep_key = binding.name
                            elif binding.annotation is not None and inspect.isclass(binding.annotation):
                                for s_key, s_val in eff_container.singletons.items():
                                    if isinstance(s_val, binding.annotation):
                                        dep_key = s_key
                                        break
                        if dep_key is not None and (
                            dep_key in eff_container.graph.providers or dep_key in eff_container.singletons
                        ):
                            resolved_kwargs[binding.name] = await scope.resolve(dep_key)
                    result = tdef.fn(*envelope.args, **resolved_kwargs)
                    if inspect.isawaitable(result):
                        result = await result
            else:
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
        from datetime import UTC, datetime

        from vertebra.framework.queues.scheduler import CronScheduler

        sched = self._scheduler or CronScheduler(self)
        target_dt = datetime(2026, month, day_of_month, hour, minute, 0, tzinfo=UTC)
        count = await sched.tick(now=target_dt)
        if count == 0:
            return []
        return await self.drain_once(batch_size=count)

    async def close(self) -> None:
        """Close underlying backend resources."""
        await self._backend.close()

    @staticmethod
    def _build_backend(config: QueueConfig | QueueBackendSettings) -> QueueBackend:
        """Construct matching QueueBackend based on QueueBackendSettings or QueueConfig."""
        if isinstance(config, QueueBackendSettings):
            kind = config.kind.lower()
            if kind == "shm":
                from vertebra.framework.queues.backends import SHMQueueBackend

                return SHMQueueBackend(config=config)
            if kind == "memory":
                return MemoryQueueBackend(QueueConfig(backend_url="memory://"))
            if kind in ("redis",):
                backend_cls = import_string("vertebra.framework.queues.redis.RedisQueueBackend")
                return cast("QueueBackend", backend_cls(config))
            if kind in ("valkey",):
                backend_cls = import_string("vertebra.framework.queues.valkey.ValkeyQueueBackend")
                return cast("QueueBackend", backend_cls(config))
            if kind in ("postgres", "postgresql"):
                backend_cls = import_string("vertebra.framework.queues.postgres.PostgreSQLQueueBackend")
                return cast("QueueBackend", backend_cls(config))
            if kind in ("oracle",):
                backend_cls = import_string("vertebra.framework.queues.oracle.OracleQueueBackend")
                return cast("QueueBackend", backend_cls(config))
            if kind in ("spanner", "spanner+googlesql", "spangres", "spanner+spangres"):
                backend_cls = import_string("vertebra.framework.queues.spanner.SpannerQueueBackend")
                return cast("QueueBackend", backend_cls(config))
            msg = f"Unsupported queue backend kind: {kind!r}"
            raise UnsupportedBackendError(msg)

        parsed = urllib.parse.urlparse(config.backend_url)
        scheme = parsed.scheme.lower()
        if scheme in ("", "memory"):
            return MemoryQueueBackend(config)
        if scheme in ("shm",):
            from vertebra.framework.queues.backends import SHMQueueBackend

            return SHMQueueBackend(config=config)
        if scheme in ("redis", "rediss"):
            backend_cls = import_string("vertebra.framework.queues.redis.RedisQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("valkey", "valkeys"):
            backend_cls = import_string("vertebra.framework.queues.valkey.ValkeyQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("postgresql", "postgres"):
            backend_cls = import_string("vertebra.framework.queues.postgres.PostgreSQLQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("oracle",):
            backend_cls = import_string("vertebra.framework.queues.oracle.OracleQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        if scheme in ("spanner", "spanner+googlesql", "spangres", "spanner+spangres"):
            backend_cls = import_string("vertebra.framework.queues.spanner.SpannerQueueBackend")
            return cast("QueueBackend", backend_cls(config))
        msg = f"Unsupported queue backend URL scheme: {scheme!r}"
        raise UnsupportedBackendError(msg)


@overload
def task(
    fn: Callable[..., Any],
    /,
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry_policy: TaskRetryPolicy | None = None,
    retry: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
    timeout_seconds: float = 300.0,
) -> TaskDef: ...


@overload
def task(
    fn: None = None,
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry_policy: TaskRetryPolicy | None = None,
    retry: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
    timeout_seconds: float = 300.0,
) -> Callable[[Callable[..., Any]], TaskDef]: ...


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    lane: TaskLane = "io",
    retry_policy: TaskRetryPolicy | None = None,
    retry: TaskRetryPolicy | None = None,
    cron: CronSchedule | None = None,
    timeout_seconds: float = 300.0,
) -> TaskDef | Callable[[Callable[..., Any]], TaskDef]:
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
