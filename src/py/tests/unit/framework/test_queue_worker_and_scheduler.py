"""Unit tests for distributed queue workers, retry/DLQ lifecycle, cron scheduler, and Django tasks."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from litestar_queues import QueueConfig as LitestarQueueConfig
from litestar_queues import QueueService as LitestarQueueService
from litestar_queues import Worker as LitestarWorker
from litestar_queues import WorkerConfig as LitestarWorkerConfig
from litestar_queues import task as litestar_task
from litestar_queues.models import TaskRequest, TaskReservation

from tests.unit.framework.queue_test_doubles import (
    InMemoryAsyncNotificationSource,
    StatefulAsyncQueueStore,
    create_backend_by_url,
)
from vertebra.extensions.django.management.commands.vertebra_worker import (
    Command as DjangoVertebraWorkerCommand,
)
from vertebra.extensions.django.tasks import TaskBackend as DjangoTaskBackend
from vertebra.extensions.litestar.queues.backend import StoreQueueBackend
from vertebra.framework.di import Container, Provide
from vertebra.framework.queues import (
    CronSchedule,
    MemoryQueueBackend,
    PostgreSQLQueueBackend,
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    TaskEnvelope,
    TaskRetryPolicy,
    task,
)
from vertebra.framework.queues.scheduler import CronScheduler
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.server.platform import LameDuckController
from vertebra.telemetry.otel import SpanExporter, TraceContext


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
    backend = create_backend_by_url(backend_url, config)

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
    backend = create_backend_by_url(backend_url, config)

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

    async def worker_loop() -> None:
        while True:
            async with delivered_lock:
                if len(delivered_ids) >= total_tasks:
                    break
            batch = await backend.dequeue(["io"], batch_size=5)
            if not batch:
                break
            for item in batch:
                async with delivered_lock:
                    delivered_ids.append(item.task_id)
                await backend.ack(item)

    workers = [asyncio.create_task(worker_loop()) for _ in range(4)]
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
async def test_queue_backends_visibility_timeout_reclaims_unacked_tasks(
    backend_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unacknowledged tasks are reclaimed after visibility timeout elapses."""
    fake_now = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: fake_now[0])

    config = QueueConfig(
        backend_url=backend_url,
        visibility_timeout_seconds=0.05,
        consumer_name="worker-a",
    )
    backend = create_backend_by_url(backend_url, config)

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

    fake_now[0] += 0.1

    if backend_url.startswith("memory"):
        backend_b = backend
    else:
        config_b = QueueConfig(
            backend_url=backend_url,
            visibility_timeout_seconds=0.05,
            consumer_name="worker-b",
        )
        backend_b = create_backend_by_url(backend_url, config_b)
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
    backend = create_backend_by_url(backend_url, config)

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
async def test_queue_backends_retry_backoff_and_dlq_replay(
    backend_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify failing tasks retry with backoff, move to DLQ when exhausted, and can be replayed."""
    fake_now = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: fake_now[0])

    config = QueueConfig(backend_url=backend_url)
    backend = create_backend_by_url(backend_url, config)

    qsvc = QueueService(config, backend=backend)
    attempts_recorded: list[int] = []

    async def failing_operation(x: int) -> int:
        attempts_recorded.append(len(attempts_recorded) + 1)
        if len(attempts_recorded) <= 2:
            msg = "transient failure"
            raise RuntimeError(msg)
        return x * 2

    qsvc.register(
        task(lane="io", retry_policy=TaskRetryPolicy(max_retries=2, initial_backoff_seconds=0.01))(failing_operation)
    )
    await qsvc.enqueue("failing_operation", 42)

    claimed1 = (await backend.dequeue(["io"], batch_size=1))[0]
    assert claimed1.attempt == 1

    with pytest.raises(RuntimeError, match="transient failure"):
        await qsvc.process_envelope(claimed1)

    fake_now[0] += 0.05

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
async def test_distributed_cron_scheduler_leader_election_and_failover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify exactly one node enqueues cron tasks and failover is seamless without duplicates."""
    t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 10, 22, 12, 1, 0, tzinfo=UTC)
    fake_now = [t0.timestamp()]
    monkeypatch.setattr(time, "time", lambda: fake_now[0])

    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)

    def minutely_heartbeat() -> str:
        return "heartbeat"

    qsvc.register(
        task(lane="io", cron=CronSchedule(expression="* * * * *", timezone="America/New_York"))(minutely_heartbeat)
    )

    sched1 = CronScheduler(qsvc, node_id="node-1", lease_ttl_seconds=0.1)
    sched2 = CronScheduler(qsvc, node_id="node-2", lease_ttl_seconds=0.1)
    sched3 = CronScheduler(qsvc, node_id="node-3", lease_ttl_seconds=0.1)

    res1 = await sched1.tick(now=t0)
    res2 = await sched2.tick(now=t0)
    res3 = await sched3.tick(now=t0)

    assert res1 + res2 + res3 == 1
    assert res1 == 1
    assert res2 == 0
    assert res3 == 0

    fake_now[0] = t1.timestamp()

    res_failover_2 = await sched2.tick(now=t1)
    res_failover_3 = await sched3.tick(now=t1)

    assert res_failover_2 + res_failover_3 == 1
    assert res_failover_2 == 1
    assert res_failover_3 == 0

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
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_cpu_job() -> str:
        started.set()
        await asyncio.wait_for(release.wait(), timeout=2.0)
        completed_jobs.append("cpu_done")
        return "cpu_done"

    qsvc.register(task(lane="cpu")(slow_cpu_job))

    root_trace = TraceContext.generate(sampled=True)
    token = TraceContext.set_current(root_trace)
    try:
        env = await qsvc.enqueue("slow_cpu_job")
        assert env.task_name == "slow_cpu_job"
    finally:
        old_ctx = getattr(token, "old_value", None)
        TraceContext.set_current(old_ctx if isinstance(old_ctx, TraceContext) else None)

    runner = WorkerRunner(
        [LaneConfig(queue_name="cpu", concurrency=2, drain_timeout_s=2.0, lane_kind="cpu")],
        queue_service=qsvc,
        lame_duck=lame_duck,
        span_exporter=exporter,
    )

    runner_task = runner.submit("cpu", lambda: runner.process_once())
    await asyncio.wait_for(started.wait(), timeout=2.0)

    lame_duck.enter_lame_duck()
    status_code, status_payload = lame_duck.readiness_status()
    assert status_code == 503
    assert status_payload["status"] == "lame_duck"

    stop_task = asyncio.create_task(runner.stop())
    release.set()
    await stop_task
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


@pytest.mark.anyio
async def test_worker_runner_scoped_di_and_cron_scheduler_register() -> None:
    """Verify WorkerRunner per-task Container.scope() DI, generator teardown, and CronScheduler.register."""
    cleaned_up: list[str] = []

    async def get_db_session() -> AsyncIterator[dict[str, str]]:
        session = {"id": "session-1"}
        try:
            yield session
        finally:
            cleaned_up.append("session-closed")

    container = Container()
    container.graph.register("db_session", Provide(dependency=get_db_session, scope="request"))
    container.graph.compile()

    backend = MemoryQueueBackend()
    qsvc = QueueService(backend=backend)

    invoked_with: list[dict[str, str]] = []

    async def task_with_di(db_session: dict[str, str]) -> str:
        invoked_with.append(db_session)
        return "done"

    qsvc.register(task(lane="io")(task_with_di))
    await qsvc.enqueue("task_with_di")

    runner = WorkerRunner(
        [LaneConfig(queue_name="io", concurrency=2, lane_kind="io")],
        queue_service=qsvc,
        container=container,
    )

    processed = await runner.process_once()
    assert processed == 1
    assert len(invoked_with) == 1
    assert invoked_with[0] == {"id": "session-1"}
    assert cleaned_up == ["session-closed"]

    sched = CronScheduler(qsvc, node_id="sched-test", lease_ttl_seconds=10.0)
    sched.register("daily_sync", "0 0 * * *", "task_with_di")

    t_midnight = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    assert await sched.tick(now=t_midnight) == 1

    t_other = datetime(2026, 5, 1, 0, 1, 0, tzinfo=UTC)
    assert await sched.tick(now=t_other) == 0

    await backend.close()


@pytest.mark.anyio
async def test_worker_runner_concurrent_batch_lane_alignment_and_periodic_reap() -> None:
    """WorkerRunner dispatches claimed tasks concurrently up to concurrency, aligns queue_name/lane metrics, and reaps periodically."""
    store = StatefulAsyncQueueStore()
    pg_backend = PostgreSQLQueueBackend(QueueBackendSettings(kind="postgres", store=store))
    qsvc = QueueService(backend=pg_backend)

    active_count = 0
    all_started = asyncio.Event()
    release_gate = asyncio.Event()

    async def barrier_task(idx: int) -> int:
        nonlocal active_count
        active_count += 1
        if active_count == 3:
            all_started.set()
        await release_gate.wait()
        return idx

    qsvc.register(task(name="barrier_task", lane="io")(barrier_task))
    for i in range(3):
        await qsvc.enqueue("barrier_task", i)

    runner = WorkerRunner(
        [LaneConfig(queue_name="default", concurrency=3, drain_timeout_s=2.0, lane_kind="io")],
        queue_service=qsvc,
        reap_interval_seconds=60.0,
    )

    batch_task = asyncio.create_task(runner.process_once())
    await asyncio.wait_for(all_started.wait(), timeout=1.0)
    assert runner.metrics["default"].in_flight == 3
    assert len(runner._in_flight) == 3

    stop_task = asyncio.create_task(runner.stop())
    await asyncio.sleep(0)
    assert not stop_task.done()
    release_gate.set()

    await stop_task
    processed = await batch_task
    assert processed == 3
    assert runner.metrics["default"].in_flight == 0
    assert runner.metrics["default"].processed_total == 3
    assert store.reap_calls == 1
    await pg_backend.close()


@pytest.mark.anyio
async def test_django_task_backend_list_arg_queues_filter_and_bad_import_dlq() -> None:
    """Django TaskBackend preserves list/tuple user args and vertebra_worker honors --queues and DLQs unimportable tasks."""
    backend = DjangoTaskBackend(
        alias="vtb_django_test",
        params={"QUEUES": ["default", "cpu", "agent"], "OPTIONS": {"BACKEND_URL": "memory://"}},
    )

    @backend.task("join_items")
    def join_items(items: list[str] | tuple[str, ...]) -> str:
        return ",".join(items)

    res_list = backend.enqueue(join_items, ["alpha", "beta", "gamma"])
    assert res_list.args == [["alpha", "beta", "gamma"]]
    assert await backend.arun_next() == "alpha,beta,gamma"

    res_tuple = await backend.aenqueue(join_items, ("x", "y"))
    assert len(res_tuple.args) == 1
    assert list(res_tuple.args[0]) == ["x", "y"]
    assert await backend.arun_next() == "x,y"

    await backend.queue_service.backend.enqueue(
        TaskEnvelope(
            task_id="bad-import-1",
            task_name="nonexistent_django_module_xyz.missing_callable",
            lane="io",
            max_retries=3,
        )
    )
    cmd = DjangoVertebraWorkerCommand()
    await cmd._run_async(
        backend=backend,
        queues=["default"],
        explicit_queues=True,
        concurrency=1,
        once=True,
        burst=False,
        container=Container(),
    )
    dlq_entries = await backend.queue_service.backend.list_dlq()
    assert len(dlq_entries) == 1
    assert dlq_entries[0].envelope.task_id == "bad-import-1"

    executed_lanes: list[str] = []

    def cpu_only_task() -> str:
        executed_lanes.append("cpu")
        return "cpu"

    def agent_only_task() -> str:
        executed_lanes.append("agent")
        return "agent"

    backend.queue_service.register(task(name="cpu_only_task", lane="cpu")(cpu_only_task))
    backend.queue_service.register(task(name="agent_only_task", lane="agent")(agent_only_task))
    await backend.queue_service.enqueue("cpu_only_task", lane="cpu")
    await backend.queue_service.enqueue("agent_only_task", lane="agent")

    await cmd._run_async(
        backend=backend,
        queues=["cpu"],
        explicit_queues=True,
        concurrency=1,
        once=False,
        burst=True,
        container=Container(),
    )
    assert executed_lanes == ["cpu"]
    remaining_agent = await backend.queue_service.backend.dequeue(["agent"], batch_size=5)
    assert len(remaining_agent) == 1
    assert remaining_agent[0].task_name == "agent_only_task"
    await backend.queue_service.close()


@litestar_task("vertebra_distributed_store_worker_echo")
async def _distributed_store_worker_echo(item: str) -> str:
    return f"echo:{item}"


@pytest.mark.anyio
async def test_store_queue_backend_stateful_store_worker_and_reservations() -> None:
    """Verify StoreQueueBackend over StatefulAsyncQueueStore executes real Worker batches and retains reservations."""
    store = StatefulAsyncQueueStore()
    notif = InMemoryAsyncNotificationSource()
    backend = StoreQueueBackend(store=store, notification_source=notif)
    service = LitestarQueueService(
        LitestarQueueConfig(worker=LitestarWorkerConfig(batch_size=10, max_concurrency=4)),
        queue_backend=backend,
    )
    worker = LitestarWorker(service)

    forever_id = uuid.uuid4()
    assert await backend.reserve_identity("dist-forever-1", task_id=forever_id, task_name="echo") is None
    res = await backend.has_identity("dist-forever-1")
    assert isinstance(res, TaskReservation)
    assert res.task_id == forever_id

    expires_at = datetime.now(UTC) + timedelta(minutes=5)
    batch = await backend.enqueue_many(
        [
            TaskRequest(
                task_name="vertebra_distributed_store_worker_echo",
                args=("first",),
                queue="billing",
                priority=8,
                expires_at=expires_at,
                key="dist-bill-1",
                execution_backend="local",
                execution_profile="standard",
                metadata={"region": "us"},
            ),
            TaskRequest(
                task_name="vertebra_distributed_store_worker_echo",
                args=("second",),
                queue="emails",
                priority=3,
                expires_at=expires_at,
                key="dist-email-1",
                execution_backend="local",
                execution_profile="fast",
                metadata={"region": "eu"},
            ),
        ]
    )
    assert len(batch) == 2

    processed = await worker.run_once()
    await worker.stop()
    assert processed == 2

    completed = await backend.list_completed_by_task("vertebra_distributed_store_worker_echo", limit=10)
    assert {item.id for item in completed} == {item.id for item in batch}
    assert {item.queue for item in completed} == {"billing", "emails"}

    by_key = await backend.get_task_by_key("dist-email-1")
    assert by_key is not None
    assert by_key.queue == "emails"
    assert by_key.status == "completed"
    assert by_key.execution_profile == "fast"
    assert by_key.metadata == {"region": "eu"}

    deleted = await backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=5), limit=10)
    assert deleted == 2
    retained = await backend.has_identity("dist-forever-1")
    assert isinstance(retained, TaskReservation)
    assert retained.task_id == forever_id
    await backend.close()
