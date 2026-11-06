"""Unit and contract tests for distributed queue backends, cron scheduling, and worker lanes."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any

import pytest

from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.framework.queues import (
    CronSchedule,
    MemoryQueueBackend,
    OracleQueueBackend,
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


def _create_backend_by_url(url: str, config: QueueConfig) -> Any:
    """Helper to instantiate queue backends using their simulated executors."""
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
        return OracleQueueBackend(cfg)
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
