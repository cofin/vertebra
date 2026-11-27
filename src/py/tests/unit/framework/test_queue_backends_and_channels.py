"""Unit tests for queue backend configuration, stores, Lua promotion, and channel wiring."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import sys
import time
import types
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast

import msgspec
import pytest

import vertebra.framework.channels as channels_mod
import vertebra.framework.queues._resp as resp_queue_mod
import vertebra.framework.queues.redis as redis_queue_mod
import vertebra.framework.queues.valkey as valkey_queue_mod
from tests.unit.framework.queue_test_doubles import (
    FakeOracleQueueExecutor,
    FakePostgreSQLQueueExecutor,
    FakeRedisStreamClient,
    InMemoryAsyncNotificationSource,
    InMemoryAsyncRESPQueueStoreDriver,
    InMemoryRESPQueueStoreDriver,
    StatefulAsyncQueueStore,
)
from vertebra.db.adapters.oracle.queue import AsyncOracleQueueStore, SyncOracleQueueStore
from vertebra.db.adapters.postgres import PostgreSQLConfig
from vertebra.db.adapters.postgres.queue import AsyncPostgresQueueStore, SyncPostgresQueueStore
from vertebra.db.adapters.redis import AsyncRedisQueueStore, SyncRedisQueueStore
from vertebra.db.adapters.redis.queue import (
    AsyncRedisQueueStore as DirectAsyncRedisQueueStore,
)
from vertebra.db.adapters.redis.queue import (
    SyncRedisQueueStore as DirectSyncRedisQueueStore,
)
from vertebra.db.driver import AsyncQueueStore, QueueEntry, QueueReservation, SyncQueueStore
from vertebra.extensions.litestar.queues.backend import StoreQueueBackend, StoreQueueBackendConfig
from vertebra.extensions.litestar.queues.shm_backend import SHMQueueBackendConfig
from vertebra.framework.queues import (
    AsyncRESPStreamClient,
    MemoryQueueBackend,
    OracleQueueBackend,
    OracleQueueExecutor,
    PostgreSQLQueueBackend,
    PostgreSQLQueueExecutor,
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    RedisQueueBackend,
    TaskEnvelope,
    ValkeyQueueBackend,
    task,
)
from vertebra.framework.queues.backends import (
    ValkeyQueueBackend as BackendsValkeyQueueBackend,
)
from vertebra.framework.queues.valkey import (
    AsyncRESPStreamClient as DirectValkeyStreamClient,
)
from vertebra.framework.queues.valkey import (
    ValkeyQueueBackend as DirectValkeyQueueBackend,
)
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes


@pytest.mark.anyio
async def test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn() -> None:
    """Verify PostgreSQLQueueBackend binds to direct_dsn and wakes on notification in < 50ms."""
    pg_config = PostgreSQLConfig(
        dsn="postgresql://pooler:6432/db",
        direct_dsn="postgresql://primary:5432/db",
        pooler_mode="transaction",
    )
    executor = FakePostgreSQLQueueExecutor()
    waiter_ready = asyncio.Event()
    executor.waiter_ready = waiter_ready
    backend = PostgreSQLQueueBackend(postgres_config=pg_config, executor=executor)
    assert backend.direct_dsn == "postgresql://primary:5432/db"

    dequeued_result: list[TaskEnvelope] = []

    async def waiter() -> None:
        claimed = await backend.dequeue(["io"], batch_size=1, timeout_seconds=2.0)
        dequeued_result.extend(claimed)

    task_coro = asyncio.create_task(waiter())
    await asyncio.wait_for(waiter_ready.wait(), timeout=2.0)

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
async def test_oracle_queue_backend_requires_oracledb_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OracleQueueBackend raises MissingDependencyError when vertebra._oracle is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "vertebra._oracle", None)
    reset_dependency_cache("vertebra._oracle")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert exc_info.value.package == "vertebra-core[oracle]"
        assert exc_info.value.install_package == "vertebra-core[oracle]"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))
        assert svc_exc.value.package == "vertebra-core[oracle]"

        fake = FakeOracleQueueExecutor()
        assert isinstance(fake, OracleQueueExecutor)
        cfg = QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1")
        backend = OracleQueueBackend(cfg, executor=fake)
        service = QueueService(cfg, backend=backend)

        def oracle_ping(val: int) -> int:
            return val + 1

        service.register(task(name="oracle_ping", lane="io")(oracle_ping))
        await service.enqueue("oracle_ping", 41)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "vertebra._oracle", types.ModuleType("vertebra._oracle"))
        reset_dependency_cache("vertebra._oracle")
        with pytest.raises(ImproperConfigurationError, match="is unsupported"):
            OracleQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("vertebra._oracle")


@pytest.mark.anyio
async def test_postgres_queue_backend_requires_native_postgres_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PostgreSQLQueueBackend raises MissingDependencyError when vertebra._postgres is absent and ImproperConfigurationError without executor."""
    monkeypatch.setitem(sys.modules, "vertebra._postgres", None)
    reset_dependency_cache("vertebra._postgres")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))
        assert exc_info.value.package == "vertebra-core[postgres]"
        assert "vertebra-core[postgres]" in exc_info.value.hint

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))
        assert svc_exc.value.package == "vertebra-core[postgres]"

        fake = FakePostgreSQLQueueExecutor()
        assert isinstance(fake, PostgreSQLQueueExecutor)
        pg_config = PostgreSQLConfig(
            dsn="postgresql://pooler:6432/db",
            direct_dsn="postgresql://primary:5432/db",
        )
        qcfg = QueueConfig(backend_url="postgresql://pooler:6432/db")
        backend = PostgreSQLQueueBackend(qcfg, postgres_config=pg_config, executor=fake)
        assert backend.direct_dsn == "postgresql://primary:5432/db"
        service = QueueService(qcfg, backend=backend)

        def pg_ping(val: int) -> int:
            return val * 2

        service.register(task(name="pg_ping", lane="io")(pg_ping))
        await service.enqueue("pg_ping", 21)
        assert await service.drain_once(("io",)) == [42]
        await service.close()

        monkeypatch.setitem(sys.modules, "vertebra._postgres", types.ModuleType("vertebra._postgres"))
        reset_dependency_cache("vertebra._postgres")
        with pytest.raises(ImproperConfigurationError, match="do not support memory:// DSNs"):
            PostgreSQLQueueBackend(QueueConfig())
    finally:
        reset_dependency_cache("vertebra._postgres")


def test_redis_queue_backend_requires_redis_and_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RedisQueueBackend requires vertebra._redis and a valid URL, while ValkeyQueueBackend requires vertebra._valkey."""
    monkeypatch.setitem(sys.modules, "vertebra._redis", None)
    reset_dependency_cache("vertebra._redis")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            RedisQueueBackend(QueueConfig(backend_url="redis://localhost:6379/0"))
        assert exc_info.value.package == "vertebra-core[redis]"
        assert exc_info.value.install_package == "vertebra-core[redis]"
    finally:
        monkeypatch.delitem(sys.modules, "vertebra._redis", raising=False)
        reset_dependency_cache("vertebra._redis")

    monkeypatch.setitem(sys.modules, "vertebra._valkey", None)
    reset_dependency_cache("vertebra._valkey")
    try:
        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="valkey://localhost:6379/0"))
        assert svc_exc.value.package == "vertebra-core[valkey]"

        fake = FakeRedisStreamClient()
        assert isinstance(fake, AsyncRESPStreamClient)
        injected = ValkeyQueueBackend(client=fake)
        assert injected.client is fake
    finally:
        monkeypatch.delitem(sys.modules, "vertebra._valkey", raising=False)
        reset_dependency_cache("vertebra._valkey")

    with pytest.raises(ImproperConfigurationError, match="has no host"):
        ValkeyQueueBackend(QueueConfig(backend_url="valkey:///0"))

    fake = FakeRedisStreamClient()
    injected_redis = RedisQueueBackend(client=fake)
    assert injected_redis.client is fake

    with pytest.raises(ImproperConfigurationError, match="has no host"):
        RedisQueueBackend(QueueConfig(backend_url="redis:///0"))


@pytest.mark.parametrize(
    ("url", "valid", "expected_err"),
    [
        ("redis://127.0.0.1:6379/0", True, None),
        ("rediss://127.0.0.1:6380/0", True, None),
        ("unix:///tmp/redis.sock", True, None),
        ("memory://", False, "does not support memory://"),
        ("valkey://localhost:6379/0", False, "valkey:// URLs are not supported by RedisQueueBackend"),
        ("valkeys://localhost:6380/0", False, "valkey:// URLs are not supported by RedisQueueBackend"),
    ],
)
def test_redis_queue_backend_url_schemes(url: str, valid: bool, expected_err: str | None) -> None:
    """RedisQueueBackend accepts redis://, rediss://, and unix:// while rejecting memory:// and valkey://."""
    if valid:
        backend = RedisQueueBackend(QueueConfig(backend_url=url), client=FakeRedisStreamClient())
        assert backend is not None
    else:
        with pytest.raises(ImproperConfigurationError, match=expected_err or ""):
            RedisQueueBackend(QueueConfig(backend_url=url))


@pytest.mark.parametrize(
    ("url", "valid", "expected_err"),
    [
        ("valkey://127.0.0.1:6379/0", True, None),
        ("valkeys://127.0.0.1:6380/0", True, None),
        ("unix:///tmp/valkey.sock", True, None),
        ("memory://", False, "does not support memory://"),
        ("redis://localhost:6379/0", False, "redis:// URLs are not supported by ValkeyQueueBackend"),
        ("rediss://localhost:6380/0", False, "redis:// URLs are not supported by ValkeyQueueBackend"),
    ],
)
def test_valkey_queue_backend_url_schemes(url: str, valid: bool, expected_err: str | None) -> None:
    """ValkeyQueueBackend accepts valkey://, valkeys://, and unix:// while rejecting memory:// and redis://."""
    if valid:
        backend = ValkeyQueueBackend(QueueConfig(backend_url=url), client=FakeRedisStreamClient())
        assert backend is not None
    else:
        with pytest.raises(ImproperConfigurationError, match=expected_err or ""):
            ValkeyQueueBackend(QueueConfig(backend_url=url))


def test_valkey_queue_backend_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """ValkeyQueueBackend and AsyncRESPStreamClient are isolated from framework.queues.redis."""
    assert ValkeyQueueBackend is DirectValkeyQueueBackend
    assert BackendsValkeyQueueBackend is DirectValkeyQueueBackend
    assert AsyncRESPStreamClient is DirectValkeyStreamClient

    source = inspect.getsource(valkey_queue_mod)
    assert "vertebra.framework.queues.redis" not in source
    assert "from .redis" not in source
    assert "redis.asyncio" not in source

    fake = FakeRedisStreamClient()
    assert isinstance(fake, DirectValkeyStreamClient)

    monkeypatch.setattr(DirectValkeyQueueBackend, "_build_client", staticmethod(lambda cfg: fake))
    backend = QueueService._build_backend(QueueConfig(backend_url="valkey://localhost:6379/0", stream_prefix="test"))
    assert isinstance(backend, DirectValkeyQueueBackend)
    assert backend.client is fake


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("backend_cls", "backend_url", "lua_src"),
    [
        (RedisQueueBackend, "redis://127.0.0.1:6379/0", redis_queue_mod._PROMOTE_SCHEDULED_LUA),
        (ValkeyQueueBackend, "valkey://127.0.0.1:6379/0", valkey_queue_mod._PROMOTE_SCHEDULED_LUA),
    ],
    ids=["redis", "valkey"],
)
async def test_redis_and_valkey_lua_promote_array_like_envelope_and_nack(
    backend_cls: type[Any],
    backend_url: str,
    lua_src: str,
) -> None:
    """Redis and Valkey promotion Lua scripts extract array_like TaskEnvelope fields and nack has no dead tail."""
    env = TaskEnvelope(
        task_id="arr-1",
        task_name="dispatch_webhook",
        lane="agent",
        scheduled_at=10.0,
        max_retries=3,
    )
    raw = encode_json_bytes(env)
    decoded = decode_json(raw)
    assert isinstance(decoded, list)
    assert decoded[1] == "dispatch_webhook"
    assert decoded[2] == "agent"

    assert "data.lane or data[3]" in lua_src
    assert "data.task_name or data[2]" in lua_src
    assert "if ok and data and lane then" in lua_src

    nack_src = inspect.getsource(backend_cls.nack)
    tree = ast.parse(inspect.cleandoc(nack_src))
    func_node = tree.body[0]
    assert isinstance(func_node, ast.AsyncFunctionDef)
    assert isinstance(func_node.body[-1], ast.Return)
    assert sum(1 for stmt in func_node.body if isinstance(stmt, ast.Return)) == 1

    client = FakeRedisStreamClient()
    backend = backend_cls(QueueConfig(backend_url=backend_url), client=client)
    await backend.enqueue(msgspec.structs.replace(env, scheduled_at=None))
    claimed = await backend.dequeue(["agent"], batch_size=1)
    assert len(claimed) == 1
    dlq_res = await backend.nack(claimed[0], error="retry me", delay_seconds=5.0)
    assert dlq_res is None
    assert len(client._zsets["vtb:queue:scheduled"]) == 1
    promoted = await backend.promote_scheduled(now=time.time() + 10.0)
    assert promoted == 1
    reclaimed = await backend.dequeue(["agent"], batch_size=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].attempt == 2
    await backend.close()


@pytest.mark.anyio
async def test_oracle_executor_multilane_binds_update_commit_and_merge_lease() -> None:
    """OracleQueueBackend executor mode expands multi-lane binds, persists running state, and avoids fetch_one on MERGE."""
    fake = FakeOracleQueueExecutor()
    backend = OracleQueueBackend(
        QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1", consumer_name="ora-worker-1"),
        executor=fake,
    )
    await backend.enqueue(TaskEnvelope(task_id="ora-cpu", task_name="c", lane="cpu"))
    await backend.enqueue(TaskEnvelope(task_id="ora-agent", task_name="a", lane="agent"))

    commits_before = fake.commit_count
    claimed = await backend.dequeue(["cpu", "agent"], batch_size=5)
    assert [c.task_id for c in claimed] == ["ora-cpu", "ora-agent"]
    assert fake._jobs["ora-cpu"]["status"] == "running"
    assert fake._jobs["ora-cpu"]["consumer_name"] == "ora-worker-1"
    assert fake._jobs["ora-cpu"]["receipt_handle"] is not None
    assert fake.commit_count > commits_before

    lease1 = await backend.acquire_lease("cron:hourly", "leader-a", 30.0, slot="2026-11-16T08:00")
    assert lease1 is not None
    assert lease1.owner_id == "leader-a"
    assert lease1.last_slot == "2026-11-16T08:00"

    competing = await backend.acquire_lease("cron:hourly", "leader-b", 30.0, slot="2026-11-16T08:00")
    assert competing is None

    renewed = await backend.renew_lease("cron:hourly", "leader-a", 30.0, slot="2026-11-16T09:00")
    assert renewed is not None
    assert renewed.last_slot == "2026-11-16T09:00"
    await backend.close()


@pytest.mark.anyio
async def test_oracle_and_postgres_queue_stores_reap_fencing_and_sql_validation() -> None:
    """Oracle and Postgres queue stores validate table_name and fence zombie workers in reap_stale."""
    dummy_drv: Any = object()
    with pytest.raises(ValueError, match="table_name"):
        SyncOracleQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        AsyncOracleQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        SyncPostgresQueueStore(dummy_drv, table_name="bad;table")
    with pytest.raises(ValueError, match="table_name"):
        AsyncPostgresQueueStore(dummy_drv, table_name="bad;table")

    executed_oracle_sql: list[str] = []

    class _RecordingOracleDriver:
        def execute(self, sql: str, _params: Any = None) -> int:
            executed_oracle_sql.append(sql)
            return 1

        @contextlib.contextmanager
        def transaction(self) -> Any:
            yield

        def select_one_or_none(self, sql: str, _params: Any = None) -> dict[str, Any] | None:
            executed_oracle_sql.append(sql)
            return {
                "job_id": "ora-dlq-1",
                "queue": "io",
                "task_name": "sync_task",
                "payload": b"{}",
                "attempt": 2,
                "max_attempts": 2,
                "lease_until_ms": 0,
                "scheduled_at_ms": None,
                "headers": None,
            }

    ora_drv_any: Any = _RecordingOracleDriver()
    ora_sync_sql_store = SyncOracleQueueStore(ora_drv_any, table_name="custom_ora_jobs")
    assert ora_sync_sql_store.reap_stale() == 1
    assert "attempt >= max_attempts THEN 'dead_letter'" in executed_oracle_sql[0]
    assert "ELSE attempt + 1" in executed_oracle_sql[0]
    popped_ora = ora_sync_sql_store.pop_dead_letter("ora-dlq-1")
    assert popped_ora is not None
    assert popped_ora.job_id == "ora-dlq-1"
    assert any("DELETE FROM custom_ora_jobs" in s for s in executed_oracle_sql)

    store = StatefulAsyncQueueStore()
    await store.enqueue(
        QueueEntry(
            job_id="zombie-job",
            queue="io",
            task_name="work",
            payload=b"{}",
            attempt=1,
            max_attempts=2,
        )
    )
    first_claim = await store.claim(["io"], limit=1, worker_id="worker-zombie", lease_seconds=0.0)
    assert len(first_claim) == 1
    assert first_claim[0].attempt == 1

    assert await store.reap_stale() == 1
    assert await store.ack("zombie-job", attempt=1) is False
    assert await store.nack("zombie-job", attempt=1, error="late crash") is False

    second_claim = await store.claim(["io"], limit=1, worker_id="worker-healthy", lease_seconds=0.0)
    assert len(second_claim) == 1
    assert second_claim[0].attempt == 2

    assert await store.reap_stale() == 1
    assert await store.get_entry_status("zombie-job") == "dead_letter"
    assert await store.claim(["io"], limit=1, worker_id="worker-3", lease_seconds=30.0) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("backend_kind", "backend_cls"),
    [
        ("postgres", PostgreSQLQueueBackend),
        ("oracle", OracleQueueBackend),
    ],
    ids=["postgres", "oracle"],
)
async def test_native_queue_store_settings_and_dlq_replay(
    backend_kind: Literal["postgres", "oracle"],
    backend_cls: type[Any],
) -> None:
    """Native Postgres and Oracle QueueBackendSettings wire store/sync_store and support DLQ replay."""
    backing_store = StatefulAsyncQueueStore()
    sync_marker: Any = object()
    native_backend = backend_cls(QueueBackendSettings(kind=backend_kind, store=backing_store, sync_store=sync_marker))
    resolved_store: Any = native_backend.store
    assert resolved_store is backing_store
    assert native_backend.sync_store is sync_marker

    await backing_store.reserve_unique_key("dedup-key-1", ttl_seconds=60.0)
    await backing_store.release_unique_key("dedup-key-1")
    env = TaskEnvelope(
        task_id="dlq-replay-task",
        task_name="sync_invoice",
        lane="io",
        max_retries=1,
        idempotency_key="dedup-key-1",
    )
    await native_backend.enqueue(env)
    claimed_batch = await native_backend.dequeue(["io"], batch_size=1)
    assert len(claimed_batch) == 1
    dlq_item = await native_backend.nack(claimed_batch[0], error="boom", send_to_dlq=True)
    assert dlq_item is not None
    dlq_listed = await native_backend.list_dlq()
    assert len(dlq_listed) == 1
    assert dlq_listed[0].dlq_id == "dlq-replay-task"

    replayed_env = await native_backend.replay_dlq(dlq_listed[0].dlq_id)
    assert replayed_env is not None
    assert replayed_env.attempt == 1
    reclaimed_after_replay = await native_backend.dequeue(["io"], batch_size=1)
    assert len(reclaimed_after_replay) == 1
    assert reclaimed_after_replay[0].task_id == "dlq-replay-task"
    await native_backend.ack(reclaimed_after_replay[0])


@pytest.mark.anyio
async def test_litestar_store_and_shm_queue_backend_configs_and_fenced_lifecycle() -> None:
    """StoreQueueBackendConfig and SHMQueueBackendConfig are frozen and StoreQueueBackend enforces retry fencing."""
    store = StatefulAsyncQueueStore()
    notif = InMemoryAsyncNotificationSource()
    store_cfg = StoreQueueBackendConfig(store=store, notification_source=notif)
    shm_cfg = SHMQueueBackendConfig()

    store_cfg_any: Any = store_cfg
    shm_cfg_any: Any = shm_cfg
    with pytest.raises(AttributeError):
        store_cfg_any.terminal_ttl = timedelta(seconds=10)
    with pytest.raises(AttributeError):
        shm_cfg_any.max_concurrency = 99

    backend: StoreQueueBackend = store_cfg.create_backend()
    rec1 = await backend.enqueue("compute_report", args=(7,), max_retries=2)
    claimed1 = await backend.claim_task(rec1.id)
    assert claimed1 is not None
    assert claimed1.status == "running"
    assert claimed1.retry_count == 0
    assert await backend.claim_task(rec1.id) is None

    failed_retry = await backend.fail_task(rec1.id, "transient", retry=True, expected_retry_count=0)
    assert failed_retry is not None
    assert failed_retry.retry_count == 1
    assert failed_retry.status == "pending"

    claimed2 = await backend.claim_next()
    assert claimed2 is not None
    assert claimed2.id == rec1.id
    assert claimed2.retry_count == 1
    assert claimed2.status == "running"

    running_ext = await backend.list_running_external()
    assert [r.id for r in running_ext] == [rec1.id]
    assert running_ext[0].status == "running"

    completed1 = await backend.complete_task(rec1.id, expected_retry_count=claimed2.retry_count)
    assert completed1 is not None
    assert completed1.status == "completed"

    rec_fatal = await backend.enqueue("fatal_job", max_retries=3)
    claimed_fatal = await backend.claim_task(rec_fatal.id)
    assert claimed_fatal is not None
    failed_fatal = await backend.fail_task(rec_fatal.id, "unrecoverable", retry=False)
    assert failed_fatal is not None
    assert failed_fatal.status == "failed"

    rec_pending = await backend.enqueue("pending_job")
    rec_sched = await backend.enqueue("sched_job", scheduled_at=datetime.now(UTC) + timedelta(minutes=10))
    rec_cancel = await backend.enqueue("cancel_job")
    assert await backend.cancel_task(rec_cancel.id) is True

    stats = await backend.get_statistics(queue="default")
    assert stats.pending == 1
    assert stats.scheduled == 1
    assert stats.running == 0
    assert stats.completed == 1
    assert stats.failed == 1
    assert stats.cancelled == 1
    _ = (rec_pending, rec_sched)

    assert await backend.wait_for_wakeups(timeout=0.02) is False

    listener_ready = asyncio.Event()
    notif.listener_ready = listener_ready

    async def _emit_wakeup() -> None:
        await asyncio.wait_for(listener_ready.wait(), timeout=1.0)
        await notif.publish("vertebra_queue_wakeup", "default")

    emitter = asyncio.create_task(_emit_wakeup())
    assert await backend.wait_for_wakeups(timeout=1.0) is True
    await emitter
    await backend.close()


def test_create_channels_backend_postgres_and_oracle_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """create_channels_backend passes dsn=url to PostgreSQLConfig and OracleConfig with canonical driver names."""
    recorded: list[tuple[str, Any, Any]] = []

    class _FakePgConfig:
        def __init__(self, *, dsn: str = "", **kwargs: Any) -> None:
            self.dsn = dsn
            self.kwargs = kwargs

    class _FakeOraConfig:
        def __init__(self, *, dsn: str = "", **kwargs: Any) -> None:
            self.dsn = dsn
            self.kwargs = kwargs

    class _FakePgMod:
        PostgreSQLConfig = _FakePgConfig
        AsyncPostgreSQLDriver = staticmethod(lambda cfg: ("pg_async_drv", cfg))
        SyncPostgreSQLDriver = staticmethod(lambda cfg: ("pg_sync_drv", cfg))
        AsyncPostgresNotificationSource = staticmethod(lambda drv: ("pg_async_src", drv))
        SyncPostgresNotificationSource = staticmethod(lambda drv: ("pg_sync_src", drv))

    class _FakeOraMod:
        OracleConfig = _FakeOraConfig
        AsyncOracleDriver = staticmethod(lambda cfg: ("ora_async_drv", cfg))
        SyncOracleDriver = staticmethod(lambda cfg: ("ora_sync_drv", cfg))
        AsyncOracleNotificationSource = staticmethod(lambda drv: ("ora_async_src", drv))
        SyncOracleNotificationSource = staticmethod(lambda drv: ("ora_sync_src", drv))

    def _fake_import_optional(name: str) -> Any:
        if name == "vertebra.db.adapters.postgres":
            return _FakePgMod
        if name == "vertebra.db.adapters.oracle":
            return _FakeOraMod
        return None

    monkeypatch.setattr(channels_mod, "import_optional", _fake_import_optional)

    pg_bus = channels_mod.create_channels_backend("postgresql", url="postgresql://localhost:5432/vtb")
    pg_async_src: Any = pg_bus.notification_source
    pg_sync_src: Any = pg_bus.sync_notification_source
    recorded.append(("pg", pg_async_src, pg_sync_src))
    assert pg_async_src[0] == "pg_async_src"
    assert pg_async_src[1][0] == "pg_async_drv"
    assert pg_async_src[1][1].dsn == "postgresql://localhost:5432/vtb"
    assert pg_sync_src[1][0] == "pg_sync_drv"

    ora_bus = channels_mod.create_channels_backend("oracle", url="oracle://localhost:1521/FREEPDB1")
    ora_async_src: Any = ora_bus.notification_source
    ora_sync_src: Any = ora_bus.sync_notification_source
    recorded.append(("ora", ora_async_src, ora_sync_src))
    assert ora_async_src[0] == "ora_async_src"
    assert ora_async_src[1][0] == "ora_async_drv"
    assert ora_async_src[1][1].dsn == "oracle://localhost:1521/FREEPDB1"
    assert ora_sync_src[1][0] == "ora_sync_drv"
    assert len(recorded) == 2


@pytest.mark.anyio
async def test_async_queue_store_extended_protocol_and_reservations() -> None:
    """Verify QueueEntry defaults, QueueReservation, and extended AsyncQueueStore protocol methods."""
    store = StatefulAsyncQueueStore()
    assert isinstance(store, AsyncQueueStore)

    minimal_entry = QueueEntry(
        job_id="job-min",
        queue="default",
        task_name="send_email",
        payload=b"{}",
    )
    assert minimal_entry.priority == 0
    assert minimal_entry.expires_at_ms is None
    assert minimal_entry.completed_at_ms is None
    assert minimal_entry.worker_id is None
    assert minimal_entry.heartbeat_at_ms is None
    assert minimal_entry.result is None
    assert minimal_entry.error is None

    first_res = await store.reserve_identity("idem-forever", job_id="job-1", task_name="bill_customer")
    assert first_res is None
    contender_res = await store.reserve_identity("idem-forever", job_id="job-2", task_name="other_task")
    assert isinstance(contender_res, QueueReservation)
    assert contender_res.job_id == "job-1"
    assert contender_res.task_name == "bill_customer"
    assert await store.get_reservation("idem-forever") == contender_res

    res_any: Any = contender_res
    with pytest.raises(AttributeError):
        res_any.job_id = "mutated"

    assert await store.reset_identity("idem-forever", expected_job_id="job-wrong") is False
    assert await store.get_reservation("idem-forever") is not None

    await store.enqueue(
        QueueEntry(
            job_id="job-old-done",
            queue="q-alpha",
            task_name="t1",
            payload=b"{}",
            completed_at_ms=100,
            headers={"key": "lookup-key-1"},
        )
    )
    await store.claim_job("job-old-done", worker_id="w-0", lease_seconds=30.0)
    assert await store.ack("job-old-done", attempt=1) is True

    await store.enqueue(
        QueueEntry(
            job_id="job-active",
            queue="q-beta",
            task_name="t2",
            payload=b"{}",
            priority=5,
            headers={"key": "lookup-key-2"},
        )
    )
    await store.enqueue(
        QueueEntry(
            job_id="job-overdue",
            queue="q-beta",
            task_name="t3",
            payload=b"{}",
            expires_at_ms=500,
        )
    )

    by_key = await store.get_entry_by_key("lookup-key-2")
    assert by_key is not None
    assert by_key.job_id == "job-active"
    assert await store.get_entry_by_key("lookup-key-2", queue="q-alpha") is None

    all_entries = await store.list_entries(queue=None, limit=10)
    assert [e.job_id for e in all_entries] == ["job-old-done", "job-active", "job-overdue"]
    limited_entries = await store.list_entries(queue=None, limit=2)
    assert len(limited_entries) == 2

    assert await store.expire_overdue(now_ms=600) == 1
    assert await store.get_entry_status("job-overdue") == "expired"

    claimed_active = await store.claim_job("job-active", worker_id="w-1", lease_seconds=30.0)
    assert claimed_active is not None
    assert await store.assign_worker("job-active", worker_id="w-2", attempt=1) is True
    assert await store.assign_worker("job-active", worker_id="w-3", attempt=99) is False
    assert await store.touch_heartbeats(["job-active", "missing"], heartbeat_at_ms=12345) == 1
    touched_entry = await store.get_entry("job-active")
    assert touched_entry is not None
    assert touched_entry.worker_id == "w-2"
    assert touched_entry.heartbeat_at_ms == 12345

    assert await store.null_heartbeats(["job-active"]) == 1
    nulled_entry = await store.get_entry("job-active")
    assert nulled_entry is not None
    assert nulled_entry.heartbeat_at_ms is None

    assert await store.cleanup_terminal(before_ms=200) == 1
    assert await store.get_entry("job-old-done") is None
    assert await store.get_entry("job-active") is not None
    assert await store.get_reservation("idem-forever") == contender_res

    assert await store.interrupt_task("job-active", error="shutdown") is True
    interrupted = await store.get_entry("job-active")
    assert interrupted is not None
    assert interrupted.error == "shutdown"
    assert await store.get_entry_status("job-active") == "interrupted"

    assert await store.reset_identity("idem-forever", expected_job_id="job-1") is True
    assert await store.get_reservation("idem-forever") is None

    assert await store.reserve_identity("idem-uncond", job_id="job-u", task_name="t_u") is None
    assert await store.reset_identity("idem-uncond") is True


@pytest.mark.anyio
async def test_redis_queue_backend_dlq_replay_via_shared_resp_module() -> None:
    """Verify RedisQueueBackend inherits RESPStreamQueueBackend and supports DLQ + replay."""
    assert issubclass(RedisQueueBackend, resp_queue_mod.RESPStreamQueueBackend)
    client = FakeRedisStreamClient()
    backend = RedisQueueBackend(QueueConfig(backend_url="redis://127.0.0.1:6379/0"), client=client)
    env = TaskEnvelope(task_id="r-dlq-1", task_name="send", lane="io", max_retries=1)
    await backend.enqueue(env)
    claimed = await backend.dequeue(["io"], batch_size=1)
    assert len(claimed) == 1
    dlq = await backend.nack(claimed[0], error="boom", send_to_dlq=True)
    assert dlq is not None
    entries = await backend.list_dlq()
    assert len(entries) == 1
    assert entries[0].dlq_id == dlq.dlq_id
    replayed = await backend.replay_dlq(dlq.dlq_id)
    assert replayed is not None
    assert replayed.task_id == "r-dlq-1"
    assert await backend.list_dlq() == []
    await backend.close()


def test_sync_redis_queue_store_full_lifecycle() -> None:
    """Verify SyncRedisQueueStore exports and full SyncQueueStore lifecycle over RESP test double."""
    assert SyncRedisQueueStore is DirectSyncRedisQueueStore
    assert issubclass(SyncRedisQueueStore, resp_queue_mod.RESPQueueStoreBase)
    fake_driver = InMemoryRESPQueueStoreDriver()
    store = SyncRedisQueueStore(cast("Any", fake_driver), key_prefix="test:redis")
    assert isinstance(store, SyncQueueStore)
    store.ensure_schema()

    assert store.reserve_identity("idem-r1", job_id="job-r1", task_name="bill") is None
    contender = store.reserve_identity("idem-r1", job_id="job-r2", task_name="other")
    assert isinstance(contender, QueueReservation)
    assert contender.job_id == "job-r1"
    assert store.reset_identity("idem-r1", expected_job_id="wrong") is False
    assert store.get_reservation("idem-r1") == contender

    store.enqueue_many(
        [
            QueueEntry(
                job_id="j-done",
                queue="alpha",
                task_name="t1",
                payload=b'{"x":1}',
                completed_at_ms=100,
                headers={"key": "k-done"},
            ),
            QueueEntry(
                job_id="j-overdue",
                queue="beta",
                task_name="t2",
                payload=b"{}",
                expires_at_ms=500,
            ),
            QueueEntry(
                job_id="j-work",
                queue="beta",
                task_name="t3",
                payload=b"hello",
                max_attempts=2,
                headers={"key": "k-work"},
            ),
        ]
    )
    assert [e.job_id for e in store.list_entries(queue=None)] == ["j-done", "j-overdue", "j-work"]
    by_key = store.get_entry_by_key("k-work", queue="beta")
    assert by_key is not None
    assert by_key.job_id == "j-work"
    assert store.expire_overdue(now_ms=600) == 1
    assert store.claim(["alpha"], limit=1, worker_id="w-0")[0].job_id == "j-done"
    assert store.ack("j-done", attempt=1) is True
    assert store.cleanup_terminal(before_ms=200) == 1
    assert store.get_entry("j-done") is None
    assert store.get_reservation("idem-r1") == contender

    claimed = store.claim(["beta"], limit=1, worker_id="w-1", lease_seconds=10.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == "j-work"
    assert store.assign_worker("j-work", worker_id="w-2", attempt=1) is True
    assert store.touch_heartbeats(["j-work"], heartbeat_at_ms=999) == 1
    touched = store.get_entry("j-work")
    assert touched is not None
    assert touched.heartbeat_at_ms == 999
    assert store.null_heartbeats(["j-work"]) == 1
    nulled = store.get_entry("j-work")
    assert nulled is not None
    assert nulled.heartbeat_at_ms is None
    assert store.nack("j-work", attempt=1, error="retry", retry_delay_s=5.0) is True
    assert store.next_due_delay(["beta"]) is not None
    assert store.interrupt_task("j-work", error="cancelled") is True
    assert store.reset_identity("idem-r1", expected_job_id="job-r1") is True
    assert store.reserve_unique_key("uniq-1", ttl_seconds=10.0) is True
    assert store.reserve_unique_key("uniq-1", ttl_seconds=10.0) is False
    assert store.release_unique_key("uniq-1") is True
    assert store.acquire_lease("cron-1", owner_id="owner-a", ttl_seconds=10.0) is True
    assert store.acquire_lease("cron-1", owner_id="owner-b", ttl_seconds=10.0) is False
    assert store.release_lease("cron-1", owner_id="owner-a") is True
    store.close()
    assert fake_driver.closed is True


@pytest.mark.anyio
async def test_async_redis_queue_store_full_lifecycle() -> None:
    """Verify AsyncRedisQueueStore exports and full AsyncQueueStore lifecycle over RESP test double."""
    assert AsyncRedisQueueStore is DirectAsyncRedisQueueStore
    assert issubclass(AsyncRedisQueueStore, resp_queue_mod.RESPQueueStoreBase)
    fake_driver = InMemoryAsyncRESPQueueStoreDriver()
    store = AsyncRedisQueueStore(cast("Any", fake_driver), key_prefix="test:aredis")
    assert isinstance(store, AsyncQueueStore)
    await store.ensure_schema()

    assert await store.reserve_identity("idem-ar1", job_id="job-ar1", task_name="bill") is None
    contender = await store.reserve_identity("idem-ar1", job_id="job-ar2", task_name="other")
    assert isinstance(contender, QueueReservation)
    assert contender.job_id == "job-ar1"

    await store.enqueue(
        QueueEntry(
            job_id="aj-done",
            queue="q1",
            task_name="t1",
            payload=b"{}",
            completed_at_ms=100,
        )
    )
    await store.enqueue(
        QueueEntry(
            job_id="aj-overdue",
            queue="q2",
            task_name="t2",
            payload=b"{}",
            expires_at_ms=500,
        )
    )
    assert [e.job_id for e in await store.list_entries(queue=None)] == ["aj-done", "aj-overdue"]
    assert await store.expire_overdue(now_ms=600) == 1
    claimed = await store.claim(["q1"], limit=1, worker_id="w-1")
    assert len(claimed) == 1
    assert await store.ack("aj-done", attempt=1) is True
    assert await store.cleanup_terminal(before_ms=200) == 1
    assert await store.get_entry("aj-done") is None
    assert await store.get_reservation("idem-ar1") == contender
    assert await store.reset_identity("idem-ar1", expected_job_id="job-ar1") is True
    await store.aclose()
    assert fake_driver._sync.closed is True


@pytest.mark.parametrize(
    ("platform_name", "expected_kind"),
    [
        ("linux", "shm"),
        ("darwin", "shm"),
        ("win32", "memory"),
    ],
)
def test_queue_backend_settings_default_kind_by_platform(
    monkeypatch: pytest.MonkeyPatch,
    platform_name: str,
    expected_kind: str,
) -> None:
    """QueueBackendSettings defaults to memory on Windows and shm on POSIX platforms."""
    monkeypatch.setattr(sys, "platform", platform_name)
    settings = QueueBackendSettings()
    assert settings.kind == expected_kind
    if platform_name == "win32":
        service = QueueService()
        assert isinstance(service.backend, MemoryQueueBackend)
