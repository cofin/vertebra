"""Unit and contract tests for distributed queue backends, cron scheduling, and worker lanes."""

from __future__ import annotations

import asyncio
import sys
import time
import types
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.framework.queues import (
    CronSchedule,
    MemoryQueueBackend,
    OracleQueueBackend,
    OracleQueueExecutor,
    PostgreSQLQueueBackend,
    QueueConfig,
    QueueService,
    RedisQueueBackend,
    TaskEnvelope,
    TaskRetryPolicy,
    ValkeyQueueBackend,
    task,
)
from vertebra.framework.queues.scheduler import CronScheduler
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanExporter, TraceContext
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    reset_dependency_cache,
)


class _FakeOracleQueueExecutor:
    """In-memory transactional SQL simulation of Oracle FOR UPDATE SKIP LOCKED semantics."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock")

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock DML query."""
        async with self._lock:
            q = query.strip().upper()
            if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
                (
                    task_id,
                    task_name,
                    lane,
                    payload,
                    attempt,
                    max_retries,
                    status,
                    enq,
                    sched,
                    locked,
                    cname,
                    handle,
                    ikey,
                    tp,
                ) = params
                if ikey and any(j.get("idempotency_key") == ikey for j in self._jobs.values()):
                    return 0
                self._jobs[task_id] = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "status": status,
                    "enqueued_at": enq,
                    "scheduled_at": sched,
                    "locked_until": locked,
                    "consumer_name": cname,
                    "receipt_handle": handle,
                    "idempotency_key": ikey,
                    "traceparent": tp,
                }
                return 1

            if "DELETE FROM VERTEBRA_QUEUE_JOBS" in q:
                task_id = params[0]
                if task_id in self._jobs:
                    del self._jobs[task_id]
                    return 1
                return 0

            if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
                dlq_id, task_id, task_name, lane, payload, error_type, error_msg, failed_at = params
                self._dlq[dlq_id] = {
                    "dlq_id": dlq_id,
                    "task_id": task_id,
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "error_type": error_type,
                    "error_message": error_msg,
                    "failed_at": failed_at,
                }
                return 1

            if "SET SCHEDULED_AT = NULL" in q:
                current_time = params[0]
                promoted = 0
                for job in self._jobs.values():
                    if (
                        job.get("status") == "pending"
                        and job.get("scheduled_at") is not None
                        and job["scheduled_at"] <= current_time
                    ):
                        job["scheduled_at"] = None
                        promoted += 1
                return promoted

            if "UPDATE VERTEBRA_QUEUE_JOBS" in q:
                status, attempt, sched, locked, handle, task_id = params
                if task_id in self._jobs:
                    self._jobs[task_id].update(
                        {
                            "status": status,
                            "attempt": attempt,
                            "scheduled_at": sched,
                            "locked_until": locked,
                            "receipt_handle": handle,
                        }
                    )
                    return 1
                return 0

            return 0

    async def fetch_all(self, query: str, *params: Any) -> list[dict[str, Any]]:
        """Fetch all matching rows."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE SKIP LOCKED" in q:
                lanes, now_ts, limit, locked_until, consumer_name, receipt_handle = params
                target_lanes = set(lanes)
                matched: list[dict[str, Any]] = []
                sorted_jobs = sorted(
                    self._jobs.values(),
                    key=lambda j: (j.get("scheduled_at") or j.get("enqueued_at") or 0.0, j.get("enqueued_at") or 0.0),
                )
                for job in sorted_jobs:
                    if len(matched) >= limit:
                        break
                    if job.get("lane") not in target_lanes:
                        continue
                    status = job.get("status")
                    sched = job.get("scheduled_at")
                    locked = job.get("locked_until")
                    is_ready_pending = status == "pending" and (sched is None or sched <= now_ts)
                    is_expired_running = status == "running" and locked is not None and locked <= now_ts
                    if is_ready_pending or is_expired_running:
                        job_handle = receipt_handle or uuid.uuid4().hex
                        job["status"] = "running"
                        job["locked_until"] = locked_until
                        job["consumer_name"] = consumer_name
                        job["receipt_handle"] = job_handle
                        if is_expired_running:
                            job["attempt"] = int(job.get("attempt", 1)) + 1
                        matched.append(dict(job))
                return matched

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
                limit = params[0] if params else 100
                return [dict(e) for e in list(self._dlq.values())[:limit]]

            return []

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch single matching row."""
        async with self._lock:
            q = query.strip().upper()
            if "MERGE INTO VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(lease_key)
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    record = {
                        "lease_key": lease_key,
                        "owner_id": owner_id,
                        "expires_at": expires_at,
                        "last_slot": new_slot,
                    }
                    self._leases[lease_key] = record
                    return dict(record)
                return None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q:
                dlq_id = params[0]
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                return None

            return None

    async def commit(self) -> None:
        """Commit active mock transaction."""
        return None

    async def close(self) -> None:
        """Clear state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()


def _create_backend_by_url(url: str, config: QueueConfig) -> Any:
    """Instantiate queue backends, injecting the Oracle executor fake."""
    cfg = QueueConfig(
        backend_url=url,
        visibility_timeout_seconds=config.visibility_timeout_seconds,
        poll_interval_seconds=config.poll_interval_seconds,
        stream_prefix=config.stream_prefix,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
    )
    if url.startswith("memory"):
        return MemoryQueueBackend(cfg)
    if url.startswith("redis"):
        return RedisQueueBackend(cfg)
    if url.startswith("valkey"):
        return ValkeyQueueBackend(cfg)
    if url.startswith("postgres"):
        return PostgreSQLQueueBackend(cfg)
    if url.startswith("oracle"):
        return OracleQueueBackend(cfg, executor=_FakeOracleQueueExecutor())
    msg = f"Unknown backend url: {url}"
    raise ValueError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_fifo_and_lane_isolation(backend_url: str) -> None:
    """Verify FIFO ordering and lane isolation across all queue backends."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    env_cpu = TaskEnvelope(
        task_id="cpu-1",
        task_name="compute",
        lane="cpu",
        args=(10,),
    )
    env_io1 = TaskEnvelope(
        task_id="io-1",
        task_name="fetch",
        lane="io",
        args=("https://example.com/1",),
    )
    env_io2 = TaskEnvelope(
        task_id="io-2",
        task_name="fetch",
        lane="io",
        args=("https://example.com/2",),
    )

    await backend.enqueue(env_cpu)
    await backend.enqueue(env_io1)
    await backend.enqueue(env_io2)

    claimed_cpu = await backend.dequeue(["cpu"], batch_size=10)
    assert len(claimed_cpu) == 1
    assert claimed_cpu[0].task_id == "cpu-1"
    await backend.ack(claimed_cpu[0])

    claimed_io = await backend.dequeue(["io"], batch_size=10)
    assert len(claimed_io) == 2
    assert [e.task_id for e in claimed_io] == ["io-1", "io-2"]
    for e in claimed_io:
        await backend.ack(e)

    empty_check = await backend.dequeue(["cpu", "io"], batch_size=10)
    assert len(empty_check) == 0
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_concurrent_workers_no_duplicate_delivery(backend_url: str) -> None:
    """Verify concurrent workers never receive duplicate task deliveries."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    total_tasks = 40
    for i in range(total_tasks):
        env = TaskEnvelope(
            task_id=f"task-{i}",
            task_name="process_item",
            lane="io",
            args=(i,),
        )
        await backend.enqueue(env)

    delivered_ids: list[str] = []
    delivered_lock = asyncio.Lock()

    async def worker_loop(w_idx: int) -> None:
        while True:
            async with delivered_lock:
                if len(delivered_ids) >= total_tasks:
                    break
            batch = await backend.dequeue(["io"], batch_size=5)
            if not batch:
                await asyncio.sleep(0.005)
                continue
            for item in batch:
                async with delivered_lock:
                    delivered_ids.append(item.task_id)
                await backend.ack(item)

    workers = [asyncio.create_task(worker_loop(i)) for i in range(4)]
    await asyncio.gather(*workers)

    assert len(delivered_ids) == total_tasks
    assert len(set(delivered_ids)) == total_tasks
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_visibility_timeout_reclaims_unacked_tasks(backend_url: str) -> None:
    """Verify unacknowledged tasks are reclaimed after visibility timeout elapses."""
    config = QueueConfig(
        backend_url=backend_url,
        visibility_timeout_seconds=0.05,
        consumer_name="worker-a",
    )
    backend = _create_backend_by_url(backend_url, config)

    env = TaskEnvelope(
        task_id="crash-task",
        task_name="flaky_op",
        lane="io",
        args=("val",),
    )
    await backend.enqueue(env)

    claimed_a = await backend.dequeue(["io"], batch_size=1)
    assert len(claimed_a) == 1
    assert claimed_a[0].task_id == "crash-task"
    assert claimed_a[0].attempt == 1

    await asyncio.sleep(0.06)

    if backend_url.startswith("memory"):
        backend_b = backend
    else:
        config_b = QueueConfig(
            backend_url=backend_url,
            visibility_timeout_seconds=0.05,
            consumer_name="worker-b",
        )
        backend_b = _create_backend_by_url(backend_url, config_b)
        if hasattr(backend, "client") and hasattr(backend_b, "_client"):
            backend_b._client = backend.client
        if hasattr(backend, "_executor") and hasattr(backend_b, "_executor"):
            backend_b._executor = backend._executor

    claimed_b = await backend_b.dequeue(["io"], batch_size=1)
    assert len(claimed_b) == 1
    assert claimed_b[0].task_id == "crash-task"
    assert claimed_b[0].attempt == 2
    await backend_b.ack(claimed_b[0])

    await backend.close()
    if backend_b is not backend:
        await backend_b.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_delayed_schedule_promotion(backend_url: str) -> None:
    """Verify delayed jobs are not visible until scheduled_at and then promoted."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    t0 = time.time()
    env = TaskEnvelope(
        task_id="delayed-1",
        task_name="scheduled_job",
        lane="io",
        scheduled_at=t0 + 10.0,
    )
    await backend.enqueue(env)

    immediate = await backend.dequeue(["io"], batch_size=1)
    assert len(immediate) == 0

    promoted_count = await backend.promote_scheduled(now=t0 + 11.0)
    if "redis" in backend_url or "valkey" in backend_url or "memory" in backend_url:
        assert promoted_count >= 1

    promoted = await backend.dequeue(["io"], batch_size=1)
    assert len(promoted) == 1
    assert promoted[0].task_id == "delayed-1"
    await backend.ack(promoted[0])
    await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "backend_url",
    [
        "memory://",
        "redis://localhost:6379/0",
        "valkey://localhost:6379/0",
        "postgresql://localhost:5432/vtb",
        "oracle://localhost:1521/FREEPDB1",
    ],
)
async def test_queue_backends_retry_backoff_and_dlq_replay(backend_url: str) -> None:
    """Verify failing tasks retry with backoff, move to DLQ when exhausted, and can be replayed."""
    config = QueueConfig(backend_url=backend_url)
    backend = _create_backend_by_url(backend_url, config)

    qsvc = QueueService(config, backend=backend)
    attempts_recorded: list[int] = []

    @task(lane="io", retry_policy=TaskRetryPolicy(max_retries=2, initial_backoff_seconds=0.01))
    async def failing_operation(x: int) -> int:
        attempts_recorded.append(len(attempts_recorded) + 1)
        if len(attempts_recorded) <= 2:
            msg = "transient failure"
            raise RuntimeError(msg)
        return x * 2

    qsvc.register(failing_operation)
    await qsvc.enqueue("failing_operation", 42)

    claimed1 = (await backend.dequeue(["io"], batch_size=1))[0]
    assert claimed1.attempt == 1

    with pytest.raises(RuntimeError, match="transient failure"):
        await qsvc.process_envelope(claimed1)

    await asyncio.sleep(0.03)

    claimed2 = (await backend.dequeue(["io"], batch_size=1))[0]
    assert claimed2.attempt == 2

    with pytest.raises(RuntimeError, match="transient failure"):
        await qsvc.process_envelope(claimed2)

    dlq_entries = await backend.list_dlq()
    assert len(dlq_entries) == 1
    assert dlq_entries[0].envelope.task_name == "failing_operation"
    assert dlq_entries[0].error_type == "RuntimeError"

    replayed = await backend.replay_dlq(dlq_entries[0].dlq_id)
    assert replayed is not None
    assert replayed.attempt == 1

    res = await qsvc.process_envelope(replayed)
    assert res == 84
    await backend.close()


@pytest.mark.anyio
async def test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn() -> None:
    """Verify PostgreSQLQueueBackend binds to direct_dsn and wakes on notification in < 50ms."""
    pg_config = PostgreSQLConfig(
        dsn="postgresql://pooler:6432/db",
        direct_dsn="postgresql://primary:5432/db",
        pooler_mode="transaction",
    )
    backend = PostgreSQLQueueBackend(postgres_config=pg_config)
    assert backend.direct_dsn == "postgresql://primary:5432/db"

    dequeued_result: list[TaskEnvelope] = []

    async def waiter() -> None:
        claimed = await backend.dequeue(["io"], batch_size=1, timeout_seconds=2.0)
        dequeued_result.extend(claimed)

    task_coro = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)

    t_before = time.time()
    env = TaskEnvelope(task_id="fast-wakeup", task_name="ping", lane="io")
    await backend.enqueue(env)

    await task_coro
    elapsed_ms = (time.time() - t_before) * 1000.0

    assert len(dequeued_result) == 1
    assert dequeued_result[0].task_id == "fast-wakeup"
    assert elapsed_ms < 50.0
    await backend.close()


@pytest.mark.anyio
async def test_distributed_cron_scheduler_leader_election_and_failover() -> None:
    """Verify exactly one node enqueues cron tasks and failover is seamless without duplicates."""
    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)

    @task(lane="io", cron=CronSchedule(expression="* * * * *", timezone="America/New_York"))
    def minutely_heartbeat() -> str:
        return "heartbeat"

    qsvc.register(minutely_heartbeat)

    sched1 = CronScheduler(qsvc, node_id="node-1", lease_ttl_seconds=0.1)
    sched2 = CronScheduler(qsvc, node_id="node-2", lease_ttl_seconds=0.1)
    sched3 = CronScheduler(qsvc, node_id="node-3", lease_ttl_seconds=0.1)

    t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    res1 = await sched1.tick(now=t0)
    res2 = await sched2.tick(now=t0)
    res3 = await sched3.tick(now=t0)

    total_t0 = len(res1) + len(res2) + len(res3)
    assert total_t0 == 1
    assert len(res1) == 1
    assert len(res2) == 0
    assert len(res3) == 0

    await asyncio.sleep(0.12)

    t1 = datetime(2026, 10, 22, 12, 1, 0, tzinfo=UTC)
    res_failover_2 = await sched2.tick(now=t1)
    res_failover_3 = await sched3.tick(now=t1)

    total_t1 = len(res_failover_2) + len(res_failover_3)
    assert total_t1 == 1
    assert len(res_failover_2) == 1
    assert len(res_failover_3) == 0

    claimed = await backend.dequeue(["io"], batch_size=10)
    assert len(claimed) == 2
    assert claimed[0].idempotency_key == "cron:minutely_heartbeat:2026-10-22T12:00"
    assert claimed[1].idempotency_key == "cron:minutely_heartbeat:2026-10-22T12:01"
    await backend.close()


@pytest.mark.anyio
async def test_multi_lane_worker_runner_lame_duck_drain_and_otel_spans() -> None:
    """Verify WorkerRunner drains in-flight jobs on lame-duck shutdown and exports spans."""
    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)
    lame_duck = LameDuckController()
    exporter = SpanExporter()

    completed_jobs: list[str] = []

    @task(lane="cpu")
    async def slow_cpu_job() -> str:
        await asyncio.sleep(0.04)
        completed_jobs.append("cpu_done")
        return "cpu_done"

    qsvc.register(slow_cpu_job)

    root_trace = TraceContext.generate(sampled=True)
    token = TraceContext.set_current(root_trace)
    try:
        env = await qsvc.enqueue("slow_cpu_job")
        assert env.task_name == "slow_cpu_job"
    finally:
        TraceContext.set_current(token.old_value if hasattr(token, "old_value") else None)

    runner = WorkerRunner(
        [LaneConfig(queue_name="cpu", concurrency=2, drain_timeout_s=2.0, lane_kind="cpu")],
        queue_service=qsvc,
        lame_duck=lame_duck,
        span_exporter=exporter,
    )

    runner_task = runner.submit("cpu", lambda: runner.process_once())
    await asyncio.sleep(0.01)

    lame_duck.enter_lame_duck()
    status_code, status_payload = lame_duck.readiness_status()
    assert status_code == 503
    assert status_payload["status"] == "lame_duck"

    await runner.stop()
    await runner_task

    assert completed_jobs == ["cpu_done"]

    spans = exporter.drain_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "messaging.process slow_cpu_job"
    assert span.trace_id == root_trace.trace_id
    assert span.attributes["messaging.worker.lane"] == "cpu"
    assert span.attributes["status"] == "ok"
    assert span.status_code == 200
    await backend.close()


def test_oracle_queue_backend_requires_oracledb_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OracleQueueBackend raises MissingDependencyError when oracledb is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "oracledb", None)
    reset_dependency_cache("oracledb")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert exc_info.value.package == "oracledb"
        assert exc_info.value.install_package == "oracledb"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert svc_exc.value.package == "oracledb"

        fake = _FakeOracleQueueExecutor()
        assert isinstance(fake, OracleQueueExecutor)
        backend = OracleQueueBackend(QueueConfig(), executor=fake)
        assert backend._executor is fake

        monkeypatch.setitem(sys.modules, "oracledb", types.ModuleType("oracledb"))
        reset_dependency_cache("oracledb")
        with pytest.raises(ImproperConfigurationError, match="requires an OracleQueueExecutor"):
            OracleQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("oracledb")
