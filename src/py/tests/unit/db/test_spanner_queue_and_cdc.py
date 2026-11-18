"""Unit tests for SpannerQueueBackend and SpannerChangeStreamSource (CDC + ObjectStore spill)."""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import types
from collections.abc import Mapping
from typing import Any, Literal, cast

import pytest

from tests.unit.db.test_spanner import FakeSpannerClient
from vertebra._vertebra import SpannerAbortedError
from vertebra.db import cdc
from vertebra.db.adapters import spanner
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    AsyncSpannerQueueStore,
    SpannerConfig,
    SyncSpannerDriver,
    SyncSpannerQueueStore,
)
from vertebra.db.adapters.spanner.change_stream import (
    AsyncSpannerChangeStreamSource,
    SpannerChangeStreamRecord,
    SpannerChildPartition,
    SyncSpannerChangeStreamSource,
)
from vertebra.db.adapters.spanner.queue import (
    compute_spanner_shard_id,
    farm_fingerprint64,
)
from vertebra.db.cdc import CDCConfig, ChangeFeed
from vertebra.db.driver.capabilities import (
    AsyncChangeSource,
    AsyncQueueStore,
    QueueEntry,
    QueueReservation,
    SyncChangeSource,
    SyncQueueStore,
)
from vertebra.extensions.django.tasks import TaskBackend as DjangoVertebraTaskBackend
from vertebra.extensions.litestar.queues.backend import StoreQueueBackend
from vertebra.framework.channels import ChannelBus
from vertebra.framework.queues import (
    QueueBackendSettings,
    QueueConfig,
    QueueService,
    SpannerQueueBackend,
    SpannerQueueExecutor,
    TaskEnvelope,
    task,
)
from vertebra.framework.queues.backends import SpannerQueueBackend as ExportedSpannerQueueBackend
from vertebra.storage import SyncObjectStore
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.uuids import uuid4


class _FakeSpannerQueueExecutor:
    """In-memory Spanner queue executor simulating hash-sharded FOR UPDATE claims."""

    __slots__ = ("_dlq", "_jobs", "_leases", "_lock")

    def __init__(self) -> None:
        self._jobs: dict[tuple[int, str], dict[str, Any]] = {}
        self._dlq: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *params: Any) -> int:
        """Execute mock Spanner SQL statement."""
        async with self._lock:
            q = query.strip().upper()
            if "INSERT INTO VERTEBRA_QUEUE_JOBS" in q:
                (
                    shard_id,
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
                self._jobs[(int(shard_id), str(task_id))] = {
                    "shard_id": int(shard_id),
                    "task_id": str(task_id),
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
                shard_id, task_id = int(params[0]), str(params[1])
                expected_attempt = int(params[2]) if len(params) >= 3 else None
                job = self._jobs.get((shard_id, task_id))
                if job is not None and (expected_attempt is None or int(job.get("attempt", 0)) == expected_attempt):
                    del self._jobs[(shard_id, task_id)]
                    return 1
                return 0

            if "SET STATUS = 'ACKED'" in q:
                shard_id, task_id = int(params[0]), str(params[1])
                expected_attempt = int(params[2]) if len(params) >= 3 else None
                job = self._jobs.get((shard_id, task_id))
                if job is not None and (expected_attempt is None or int(job.get("attempt", 0)) == expected_attempt):
                    job["status"] = "acked"
                    job["locked_until"] = None
                    return 1
                return 0

            if "INSERT INTO VERTEBRA_QUEUE_DLQ" in q:
                dlq_id, shard_id, task_id, task_name, lane, payload, error_type, error_msg, failed_at = params
                self._dlq[str(dlq_id)] = {
                    "dlq_id": str(dlq_id),
                    "shard_id": int(shard_id),
                    "task_id": str(task_id),
                    "task_name": task_name,
                    "lane": lane,
                    "payload": payload,
                    "error_type": error_type,
                    "error_message": error_msg,
                    "failed_at": failed_at,
                }
                return 1

            if "DELETE FROM VERTEBRA_QUEUE_DLQ" in q:
                dlq_id = str(params[0])
                if dlq_id in self._dlq:
                    del self._dlq[dlq_id]
                    return 1
                return 0

            if "SET SCHEDULED_AT = NULL" in q:
                current_time = float(params[0])
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

            if "INSERT INTO VERTEBRA_QUEUE_LEASES" in q:
                lease_key, owner_id, expires_at, last_slot = params
                self._leases[str(lease_key)] = {
                    "lease_key": str(lease_key),
                    "owner_id": str(owner_id),
                    "expires_at": float(expires_at),
                    "last_slot": last_slot,
                }
                return 1

            if "UPDATE VERTEBRA_QUEUE_LEASES" in q:
                owner_id, expires_at, last_slot, lease_key, check_now = params
                existing = self._leases.get(str(lease_key))
                if existing is not None and (
                    float(existing["expires_at"]) <= float(check_now) or str(existing["owner_id"]) == str(owner_id)
                ):
                    existing.update(
                        {
                            "owner_id": str(owner_id),
                            "expires_at": float(expires_at),
                            "last_slot": last_slot,
                        }
                    )
                    return 1
                return 0

            if "UPDATE VERTEBRA_QUEUE_JOBS" in q:
                status, attempt, sched, locked, handle, shard_id, task_id = params
                key = (int(shard_id), str(task_id))
                if key in self._jobs:
                    self._jobs[key].update(
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
        """Fetch matching rows from mock Spanner tables."""
        async with self._lock:
            q = query.strip().upper()
            if "FOR UPDATE" in q and "VERTEBRA_QUEUE_JOBS" in q:
                lanes, now_ts, limit = params
                target_lanes = set(lanes)
                matched: list[dict[str, Any]] = []
                sorted_jobs = sorted(
                    self._jobs.values(),
                    key=lambda j: (
                        j.get("scheduled_at") or j.get("enqueued_at") or 0.0,
                        j.get("enqueued_at") or 0.0,
                    ),
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
                        matched.append(dict(job))
                return matched

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ" in q:
                limit = int(params[0]) if params else 100
                return [dict(e) for e in list(self._dlq.values())[:limit]]

            return []

    async def fetch_one(self, query: str, *params: Any) -> dict[str, Any] | None:
        """Fetch a single row from mock Spanner tables."""
        async with self._lock:
            q = query.strip().upper()
            if "UPDATE VERTEBRA_QUEUE_JOBS" in q and ("THEN RETURN *" in q or "RETURNING *" in q):
                next_attempt, locked_until, consumer_name, handle, shard_id, task_id = params
                job = self._jobs.get((int(shard_id), str(task_id)))
                if job is None:
                    return None
                job["status"] = "running"
                job["attempt"] = int(next_attempt)
                job["locked_until"] = float(locked_until)
                job["consumer_name"] = str(consumer_name)
                job["receipt_handle"] = str(handle)
                return dict(job)

            if "VERTEBRA_QUEUE_LEASES" in q and "ON CONFLICT" in q:
                lease_key, owner_id, expires_at, last_slot, check_now = params
                existing = self._leases.get(str(lease_key))
                if existing is None or existing["expires_at"] <= check_now or existing["owner_id"] == owner_id:
                    new_slot = last_slot or (existing.get("last_slot") if existing else None)
                    record = {
                        "lease_key": str(lease_key),
                        "owner_id": str(owner_id),
                        "expires_at": float(expires_at),
                        "last_slot": new_slot,
                    }
                    self._leases[str(lease_key)] = record
                    return dict(record)
                return None

            if "FROM VERTEBRA_QUEUE_LEASES" in q and "FOR UPDATE" in q:
                lease_key = str(params[0])
                existing = self._leases.get(lease_key)
                return dict(existing) if existing is not None else None

            if "SELECT * FROM VERTEBRA_QUEUE_JOBS WHERE SHARD_ID" in q:
                shard_id, task_id = int(params[0]), str(params[1])
                existing_job = self._jobs.get((shard_id, task_id))
                return dict(existing_job) if existing_job is not None else None

            if "SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID" in q or "WHERE DLQ_ID =" in q:
                dlq_id = str(params[0])
                if dlq_id in self._dlq:
                    return dict(self._dlq[dlq_id])
                for item in self._dlq.values():
                    if item.get("task_id") == dlq_id:
                        return dict(item)
                return None

            if "WHERE RECEIPT_HANDLE =" in q:
                receipt_handle = str(params[0])
                for job in self._jobs.values():
                    if job.get("receipt_handle") == receipt_handle:
                        return dict(job)
                return None

            return None

    async def close(self) -> None:
        """Clear in-memory state."""
        async with self._lock:
            self._jobs.clear()
            self._dlq.clear()
            self._leases.clear()


@pytest.mark.anyio
async def test_spanner_queue_backend_lifecycle_dlq_and_leases() -> None:
    """Verify SpannerQueueBackend hash sharding, DDL, enqueue/dequeue/ack/nack, DLQ, and CronLease on an injected executor."""
    assert ExportedSpannerQueueBackend is SpannerQueueBackend
    cfg = QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d")
    backend = SpannerQueueBackend(cfg, executor=_FakeSpannerQueueExecutor(), shard_count=16)

    gsql_ddls = backend.ddl_statements("googlesql")
    assert any(
        "CREATE INDEX IF NOT EXISTS idx_vtb_spanner_queue_ready" in s and "NULL_FILTERED" not in s and "STORING" in s
        for s in gsql_ddls
    )
    assert any("ROW DELETION POLICY" in s for s in gsql_ddls)
    pg_ddls = backend.ddl_statements("spangres")
    assert any("INCLUDE" in s for s in pg_ddls)
    assert "FOR UPDATE" in backend.claim_sql()
    assert "THEN RETURN *" in backend.update_claim_sql()

    shard_a = SpannerQueueBackend.compute_shard_id("job-123", 16)
    shard_b = SpannerQueueBackend.compute_shard_id("job-123", 16)
    assert 0 <= shard_a < 16
    assert shard_a == shard_b

    env = TaskEnvelope(
        task_id="task-1",
        task_name="send_email",
        lane="io",
        args=("user@example.com",),
        attempt=1,
        max_retries=2,
        enqueued_at=time.time(),
        idempotency_key="idem-1",
    )
    await backend.enqueue(env)
    await backend.enqueue(env)

    claimed = await backend.dequeue(("io",), batch_size=5)
    assert len(claimed) == 1
    assert claimed[0].task_id == "task-1"
    assert claimed[0].receipt_handle is not None

    dlq_none = await backend.nack(claimed[0], error="temporary timeout", delay_seconds=0.0)
    assert dlq_none is None

    reclaimed = await backend.dequeue(("io",), batch_size=5)
    assert len(reclaimed) == 1
    assert reclaimed[0].attempt == 2

    dlq_entry = await backend.nack(reclaimed[0], error=RuntimeError("permanent failure"))
    assert dlq_entry is not None
    assert dlq_entry.error_type == "RuntimeError"

    dlq_items = await backend.list_dlq()
    assert len(dlq_items) == 1
    replayed = await backend.replay_dlq(dlq_entry.dlq_id)
    assert replayed is not None

    final_claim = await backend.dequeue(("io",), batch_size=1)
    assert len(final_claim) == 1
    await backend.ack(final_claim[0])
    assert await backend.dequeue(("io",), batch_size=1) == []

    lease = await backend.acquire_cron_lease("hourly_cleanup", "worker-1", 30.0)
    assert lease is not None
    assert lease.owner_id == "worker-1"
    blocked = await backend.acquire_cron_lease("hourly_cleanup", "worker-2", 30.0)
    assert blocked is None
    renewed = await backend.renew_cron_lease(lease, ttl_s=60.0)
    assert renewed is not None
    assert renewed.expires_at > lease.expires_at

    service_cfg = QueueConfig(backend_url="spanner+googlesql://projects/p/instances/i/databases/d")
    service = QueueService(
        service_cfg,
        backend=SpannerQueueBackend(service_cfg, executor=_FakeSpannerQueueExecutor()),
    )

    @task(name="compute_square", lane="cpu")
    def compute_square(x: int) -> int:
        return x * x

    service.register(compute_square)
    await service.enqueue("compute_square", 7)
    results = await service.drain_once(("cpu",))
    assert results == [49]
    await service.close()
    await backend.close()


@pytest.mark.anyio
async def test_spanner_queue_backend_requires_spanner_extension_and_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SpannerQueueBackend requires vertebra._spanner and an injected SpannerQueueExecutor."""
    monkeypatch.setitem(sys.modules, "vertebra._spanner", None)
    reset_dependency_cache("vertebra._spanner")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SpannerQueueBackend(QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d"))
        assert exc_info.value.package == "vertebra-core[spanner]"
        assert exc_info.value.install_package == "vertebra-core[spanner]"
        assert exc_info.value.hint == "Install it with: uv add vertebra-core[spanner]"

        with pytest.raises(MissingDependencyError) as svc_exc:
            QueueService(QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d"))
        assert svc_exc.value.package == "vertebra-core[spanner]"

        fake = _FakeSpannerQueueExecutor()
        assert isinstance(fake, SpannerQueueExecutor)
        injected_cfg = QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d")
        injected = SpannerQueueBackend(injected_cfg, executor=fake)
        assert injected.dialect == "spangres"
        injected_svc = QueueService(injected_cfg, backend=injected)

        @task(name="spangres_ping", lane="cpu")
        def spangres_ping(x: int) -> int:
            return x + 5

        injected_svc.register(spangres_ping)
        await injected_svc.enqueue("spangres_ping", 37)
        assert await injected_svc.drain_once(("cpu",)) == [42]
        await injected_svc.close()

        monkeypatch.setitem(sys.modules, "vertebra._spanner", types.ModuleType("vertebra._spanner"))
        reset_dependency_cache("vertebra._spanner")
        with pytest.raises(ImproperConfigurationError, match="requires a configured spanner://"):
            SpannerQueueBackend()

        with pytest.raises(ImproperConfigurationError, match="requires a spanner:// backend_url"):
            SpannerQueueBackend(QueueConfig(), executor=_FakeSpannerQueueExecutor())

        auto_backend = SpannerQueueBackend(
            QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d?endpoint=http://localhost:9010")
        )
        assert isinstance(auto_backend.store, AsyncSpannerQueueStore)
        assert isinstance(auto_backend.sync_store, SyncSpannerQueueStore)
        assert not hasattr(auto_backend, "_receipt_index")

        settings_svc = QueueService(
            QueueBackendSettings(
                kind="spangres",
                dsn="spangres://projects/p/instances/i/databases/d?endpoint=http://localhost:9010",
            )
        )
        assert isinstance(settings_svc.backend, SpannerQueueBackend)
        assert settings_svc.backend.dialect == "spangres"
    finally:
        reset_dependency_cache("vertebra._spanner")


@pytest.mark.anyio
async def test_spanner_change_stream_source_partitions_and_object_store_spill() -> None:
    """Verify SpannerChangeStreamSource GoogleSQL/Spangres queries, child partition scheduling, and ObjectStore spill."""
    driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
    store = SyncObjectStore("memory://")
    feed = ChangeFeed(CDCConfig(channel="orders_cdc", spill_threshold_bytes=400), store=store)
    channel_backend = ChannelBus(change_feed=feed, history=16)
    source = SyncSpannerChangeStreamSource(
        driver,
        "orders_stream",
        channel="orders_cdc",
        change_feed=feed,
        store=store,
        spill_threshold_bytes=400,
    )

    assert "READ_orders_stream" in source.build_read_query("googlesql")
    assert "spanner.read_json_orders_stream" in source.build_read_query("spangres")

    source.schedule_partition("parent-token-1", "2026-11-02T00:00:00Z")
    source.schedule_partition("parent-token-2", "2026-11-02T00:00:00Z")
    assert source.active_partitions == ("parent-token-1", "parent-token-2")

    raw_rows = [
        {
            "data_change_record": [
                {
                    "commit_timestamp": "2026-11-02T00:01:00Z",
                    "record_sequence": "0001",
                    "server_transaction_id": "tx-100",
                    "table_name": "orders",
                    "mod_type": "INSERT",
                    "mods": [{"keys": {"order_id": "o1"}, "new_values": {"status": "NEW"}, "old_values": {}}],
                }
            ]
        },
        {
            "heartbeat_record": [
                {"timestamp": "2026-11-02T00:02:00Z"},
            ]
        },
        {
            "child_partitions_record": [
                {
                    "start_timestamp": "2026-11-02T00:03:00Z",
                    "record_sequence": "0002",
                    "child_partitions": [
                        {
                            "token": "child-token-merged",
                            "parent_partition_tokens": ["parent-token-1", "parent-token-2"],
                        }
                    ],
                }
            ]
        },
    ]

    parsed = [
        rec
        for row in raw_rows
        for rec in source.parse_raw_row(cast("Mapping[str, Any] | str", row), partition_token="parent-token-1")
    ]
    assert len(parsed) == 3

    emitted = source.ingest_records(parsed, partition_token="parent-token-1")
    assert len(emitted) == 1
    assert emitted[0].operation == "INSERT"
    assert emitted[0].spilled is False
    assert source.watermarks["parent-token-1"] == "2026-11-02T00:03:00Z"

    assert "child-token-merged" not in source.active_partitions
    promoted_first = source.complete_partition("parent-token-1")
    assert promoted_first == []
    assert "child-token-merged" not in source.active_partitions

    promoted_second = source.complete_partition("parent-token-2")
    assert promoted_second == ["child-token-merged"]
    assert "child-token-merged" in source.active_partitions

    large_blob = "X" * 512
    spangres_json_row = {
        "read_json_orders_stream": (
            '{"data_change_record": [{"commit_timestamp": "2026-11-02T00:04:00Z", '
            '"record_sequence": "0003", "server_transaction_id": "tx-101", '
            '"table_name": "orders", "mod_type": "UPDATE", '
            f'"mods": [{{"keys": {{"order_id": "o1"}}, "new_values": {{"notes": "{large_blob}"}}}}]}}]}}'
        )
    }
    spilled_records = source.parse_raw_row(spangres_json_row, partition_token="child-token-merged")
    spilled_events = source.ingest_records(spilled_records, partition_token="child-token-merged")
    assert len(spilled_events) == 1
    assert spilled_events[0].spilled is True

    history = await channel_backend.get_history("orders_cdc")
    assert len(history) == 2
    driver.close()


_START = "2026-11-02T00:00:00Z"
_END = "2026-11-02T01:00:00Z"


class _RecordingSpannerDriver:
    """Spanner driver stand-in that records select calls and returns no rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        self.calls.append((sql, parameters))
        return []


class _AsyncRecordingSpannerDriver:
    """Async Spanner driver stand-in that records select calls and returns no rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        self.calls.append((sql, parameters))
        return []


_EXPECTED_READ_PARAMS: dict[str, Any] = {
    "spangres": [_START, _END, "tok-1", 10_000],
    "googlesql": {"start_ts": _START, "end_ts": _END, "token": "tok-1", "heartbeat_ms": 10_000},
}


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["spangres", "googlesql"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_spanner_change_stream_poll_binds_params_for_dialect(
    dialect: Literal["spangres", "googlesql"],
    mode: str,
) -> None:
    """Spangres polls bind positional $1..$4 values; GoogleSQL polls bind named parameters."""
    driver: Any = _RecordingSpannerDriver() if mode == "sync" else _AsyncRecordingSpannerDriver()
    if mode == "sync":
        source: Any = SyncSpannerChangeStreamSource(driver, "orders_stream", dialect=dialect)
        events = source.poll_partition("tok-1", start_timestamp=_START, end_timestamp=_END)
    else:
        source = AsyncSpannerChangeStreamSource(driver, "orders_stream", dialect=dialect)
        events = await source.poll_partition("tok-1", start_timestamp=_START, end_timestamp=_END)
    assert events == []
    assert driver.calls == [(source.build_read_query(), _EXPECTED_READ_PARAMS[dialect])]
    assert "tok-1" in source.completed_partitions


@pytest.mark.parametrize(
    "symbol",
    [
        SyncSpannerChangeStreamSource,
        AsyncSpannerChangeStreamSource,
        SpannerChildPartition,
        SpannerChangeStreamRecord,
    ],
)
def test_spanner_change_stream_exports_belong_to_adapter(symbol: type[Any]) -> None:
    """Expose reader and wire types through the adapter without generic CDC aliases."""
    assert getattr(spanner, symbol.__name__) is symbol
    assert not hasattr(cdc, symbol.__name__)


@pytest.mark.parametrize("stream_name", ["orders-stream", "orders.stream", "orders; DROP TABLE orders", ""])
@pytest.mark.parametrize("source_type", [SyncSpannerChangeStreamSource, AsyncSpannerChangeStreamSource])
def test_spanner_change_stream_rejects_invalid_identifiers(stream_name: str, source_type: Any) -> None:
    """Reject stream names that cannot safely form the TVF identifier."""
    with pytest.raises(ValueError):
        source_type(_RecordingSpannerDriver(), stream_name)


class _PartitionSpannerDriver(_RecordingSpannerDriver):
    """Return root and child records in the active SQL dialect's row format."""

    def __init__(self, dialect: Literal["googlesql", "spangres"]) -> None:
        super().__init__()
        self.dialect = dialect

    def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        super().select(sql, parameters)
        token = parameters[2] if self.dialect == "spangres" else parameters["token"]
        record: dict[str, Any] = {
            "data_change_record": [{"table_name": "orders", "mod_type": "INSERT", "mods": [{"keys": {"id": token}}]}]
        }
        if token == "root":
            record["child_partitions_record"] = [
                {
                    "start_timestamp": _END,
                    "child_partitions": [{"token": "child", "parent_partition_tokens": ["root"]}],
                }
            ]
        if self.dialect == "spangres":
            return [{"read_json_orders_stream": encode_json(record)}]
        return [{"ChangeRecord": [record]}]


class _AsyncPartitionSpannerDriver:
    """Return partition fixtures through an asynchronous select method."""

    def __init__(self, dialect: Literal["googlesql", "spangres"]) -> None:
        self.recording = _PartitionSpannerDriver(dialect)
        self.dialect = dialect

    async def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        return self.recording.select(sql, parameters)


@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
def test_sync_spanner_change_source_stream_uses_constructor_defaults(dialect: Literal["googlesql", "spangres"]) -> None:
    """A zero-argument capability stream polls the configured root and promotes children."""
    driver = _PartitionSpannerDriver(dialect)
    source: SyncChangeSource = SyncSpannerChangeStreamSource(
        driver,
        "orders_stream",
        start_timestamp=_START,
        end_timestamp=_END,
        partition_token="root",
        heartbeat_milliseconds=2500,
    )
    assert isinstance(source, SyncChangeSource)
    events = list(source.stream())
    assert [event.payload["partition_token"] for event in events] == ["root", "child"]
    params = [call[1] for call in driver.calls]
    if dialect == "spangres":
        assert params == [[_START, _END, "root", 2500], [_END, _END, "child", 2500]]
    else:
        assert params == [
            {"start_ts": _START, "end_ts": _END, "token": "root", "heartbeat_ms": 2500},
            {"start_ts": _END, "end_ts": _END, "token": "child", "heartbeat_ms": 2500},
        ]


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_async_spanner_change_source_stream_uses_constructor_defaults(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """An async capability stream awaits configured root and promoted child reads."""
    driver = _AsyncPartitionSpannerDriver(dialect)
    reader = AsyncSpannerChangeStreamSource(
        driver,
        "orders_stream",
        start_timestamp=_START,
        end_timestamp=_END,
        partition_token="root",
        heartbeat_milliseconds=2500,
        poll_interval_seconds=0,
    )
    source: AsyncChangeSource = reader
    assert isinstance(source, AsyncChangeSource)
    events = [event async for event in source.stream()]
    assert [event.payload["partition_token"] for event in events] == ["root", "child"]
    params = [call[1] for call in driver.recording.calls]
    if dialect == "spangres":
        assert params == [[_START, _END, "root", 2500], [_END, _END, "child", 2500]]
    else:
        assert params == [
            {"start_ts": _START, "end_ts": _END, "token": "root", "heartbeat_ms": 2500},
            {"start_ts": _END, "end_ts": _END, "token": "child", "heartbeat_ms": 2500},
        ]
    assert reader.completed_partitions == frozenset({"root", "child"})


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_spanner_poll_constructor_defaults_and_call_overrides(
    dialect: Literal["googlesql", "spangres"],
    mode: str,
) -> None:
    """Direct polls resolve bound defaults and honor explicit timestamp, token, and heartbeat overrides."""
    driver: Any = _RecordingSpannerDriver() if mode == "sync" else _AsyncRecordingSpannerDriver()
    source_type: Any = SyncSpannerChangeStreamSource if mode == "sync" else AsyncSpannerChangeStreamSource
    reader = source_type(
        driver,
        "orders_stream",
        dialect=dialect,
        start_timestamp=_START,
        end_timestamp=_END,
        partition_token="configured",
        heartbeat_milliseconds=2500,
    )
    if mode == "sync":
        assert reader.poll_partition() == []
        assert (
            reader.poll_partition("override", start_timestamp=_END, end_timestamp=_START, heartbeat_milliseconds=5000)
            == []
        )
    else:
        assert await reader.poll_partition() == []
        assert (
            await reader.poll_partition(
                "override",
                start_timestamp=_END,
                end_timestamp=_START,
                heartbeat_milliseconds=5000,
            )
            == []
        )
    params = [call[1] for call in driver.calls]
    if dialect == "spangres":
        assert params == [[_START, _END, "configured", 2500], [_END, _START, "override", 5000]]
    else:
        assert params == [
            {"start_ts": _START, "end_ts": _END, "token": "configured", "heartbeat_ms": 2500},
            {"start_ts": _END, "end_ts": _START, "token": "override", "heartbeat_ms": 5000},
        ]


def test_spanner_change_stream_reschedules_completed_partition_and_tracks_root_watermark() -> None:
    """Bounded-window polling allows partition re-scheduling and records root watermarks under ''."""
    driver = _RecordingSpannerDriver()
    source = SyncSpannerChangeStreamSource(driver, "orders_stream")

    source.poll_partition("tok-window", start_timestamp=_START, end_timestamp=_END)
    assert "tok-window" in source.completed_partitions
    assert "tok-window" not in source.active_partitions

    source.schedule_partition("tok-window", _END)
    assert "tok-window" not in source.completed_partitions
    assert source.active_partitions == ("tok-window",)

    root_records = [
        SpannerChangeStreamRecord(
            record_type="heartbeat",
            watermark_timestamp="2026-11-02T00:05:00Z",
            partition_token=None,
        ),
        SpannerChangeStreamRecord(
            record_type="data_change",
            commit_timestamp="2026-11-02T00:06:00Z",
            table_name="orders",
            mod_type="INSERT",
            mods=({"keys": {"id": 1}},),
            partition_token=None,
        ),
    ]
    emitted = source.ingest_records(root_records, partition_token=None)
    assert len(emitted) == 1
    assert source.watermarks[""] == "2026-11-02T00:06:00Z"


class _QueueRwTxHandle:
    """Interactive ReadWrite transaction double staging mutations until commit."""

    def __init__(
        self,
        backend: _InMemorySpannerQueueWire,
        tx_id: bytes,
    ) -> None:
        self._backend = backend
        self._tx_id = tx_id
        self._closed = False
        self._jobs = {k: dict(v) for k, v in backend.jobs.items()}
        self._dlq = {k: dict(v) for k, v in backend.dlq.items()}
        self._leases = {k: dict(v) for k, v in backend.leases.items()}
        self._unique_keys = {k: dict(v) for k, v in backend.unique_keys.items()}

    @property
    def transaction_id(self) -> bytes:
        return self._tx_id

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> dict[str, Any]:
        _ = request_tag
        assert not self._closed
        return self._backend.run_sql_against(
            sql,
            params_json,
            jobs=self._jobs,
            dlq=self._dlq,
            leases=self._leases,
            unique_keys=self._unique_keys,
            tx_id=self._tx_id,
        )

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        res = self.query(sql, params_json, request_tag)
        return int(res.get("row_count_exact", 0))

    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        _ = request_tag
        items = decode_json(statements_json) if statements_json else []
        counts: list[int] = []
        for stmt in items:
            p_str = encode_json(stmt["params"]) if stmt.get("params") is not None else None
            counts.append(self.execute_dml(str(stmt["sql"]), p_str))
        return counts

    def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
        _ = mutations_json
        assert not self._closed
        if self._backend.abort_remaining > 0:
            self._backend.abort_remaining -= 1
            self._closed = True
            err = SpannerAbortedError("Transaction aborted due to lock contention")
            err.retry_delay_ms = 1
            raise err
        self._backend.jobs = self._jobs
        self._backend.dlq = self._dlq
        self._backend.leases = self._leases
        self._backend.unique_keys = self._unique_keys
        self._backend.commits.append(self._tx_id)
        self._closed = True
        return {"commit_timestamp_micros": int(time.time() * 1_000_000), "mutation_count": 0, "retry_count": 0}

    def rollback(self) -> None:
        if not self._closed:
            self._backend.rollbacks.append(self._tx_id)
            self._closed = True


class _AsyncQueueRwTxHandle:
    """Async wrapper around _QueueRwTxHandle matching AsyncSpannerReadWriteTransaction."""

    def __init__(self, sync_tx: _QueueRwTxHandle) -> None:
        self._sync = sync_tx

    @property
    def transaction_id(self) -> bytes:
        return self._sync.transaction_id

    async def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> dict[str, Any]:
        return self._sync.query(sql, params_json, request_tag)

    async def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        return self._sync.execute_dml(sql, params_json, request_tag)

    async def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        return self._sync.execute_batch_dml(statements_json, request_tag)

    async def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
        return self._sync.commit(mutations_json)

    async def rollback(self) -> None:
        self._sync.rollback()


class _InMemorySpannerQueueWire:
    """Wire-faithful in-memory Spanner client validating GoogleSQL vs Spangres SQL and transactions."""

    def __init__(self, dialect: Literal["googlesql", "spangres"]) -> None:
        self.dialect = dialect
        self.jobs: dict[tuple[int, str], dict[str, Any]] = {}
        self.dlq: dict[str, dict[str, Any]] = {}
        self.leases: dict[str, dict[str, Any]] = {}
        self.unique_keys: dict[str, dict[str, Any]] = {}
        self.ddl_statements: list[str] = []
        self.tx_statements: list[tuple[bytes | None, str, dict[str, Any]]] = []
        self.previous_tx_ids: list[bytes | None] = []
        self.commits: list[bytes] = []
        self.rollbacks: list[bytes] = []
        self.abort_remaining = 0
        self._tx_seq = 0

    def begin_sync_tx(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> _QueueRwTxHandle:
        _ = (transaction_tag, exclude_txn_from_change_streams, max_commit_delay_ms)
        self._tx_seq += 1
        self.previous_tx_ids.append(previous_transaction_id)
        return _QueueRwTxHandle(self, f"tx-{self._tx_seq}".encode())

    def sync_client(self) -> Any:
        wire = self

        class _SyncClient:
            def begin_read_write_transaction(
                self,
                transaction_tag: str | None = None,
                previous_transaction_id: bytes | None = None,
                exclude_txn_from_change_streams: bool = False,
                max_commit_delay_ms: int | None = None,
            ) -> _QueueRwTxHandle:
                return wire.begin_sync_tx(
                    transaction_tag,
                    previous_transaction_id,
                    exclude_txn_from_change_streams,
                    max_commit_delay_ms,
                )

            def query(
                self,
                sql: str,
                params_json: str | None = None,
                staleness_mode: str = "strong",
                staleness_value: int | None = None,
            ) -> dict[str, Any]:
                _ = (staleness_mode, staleness_value)
                return wire.run_sql_against(
                    sql,
                    params_json,
                    jobs=wire.jobs,
                    dlq=wire.dlq,
                    leases=wire.leases,
                    unique_keys=wire.unique_keys,
                    tx_id=None,
                )

            def execute_dml(
                self,
                sql: str,
                params_json: str | None = None,
                request_tag: str | None = None,
                transaction_tag: str | None = None,
            ) -> int:
                _ = (request_tag, transaction_tag)
                res = self.query(sql, params_json)
                return int(res.get("row_count_exact", 0))

            def execute_batch_dml(
                self,
                statements_json: str,
                request_tag: str | None = None,
                transaction_tag: str | None = None,
            ) -> list[int]:
                _ = (request_tag, transaction_tag)
                tx = wire.begin_sync_tx()
                counts = tx.execute_batch_dml(statements_json)
                tx.commit()
                return counts

            def update_ddl(
                self,
                statements: list[str],
                operation_id: str | None = None,
                timeout_seconds: float | None = None,
            ) -> str:
                _ = (operation_id, timeout_seconds)
                wire.ddl_statements.extend(statements)
                return "op-ddl"

            def close(self) -> None:
                return None

        return _SyncClient()

    def async_client(self) -> Any:
        sync_c = self.sync_client()

        class _AsyncClient:
            async def begin_read_write_transaction(
                self,
                transaction_tag: str | None = None,
                previous_transaction_id: bytes | None = None,
                exclude_txn_from_change_streams: bool = False,
                max_commit_delay_ms: int | None = None,
            ) -> _AsyncQueueRwTxHandle:
                return _AsyncQueueRwTxHandle(
                    sync_c.begin_read_write_transaction(
                        transaction_tag,
                        previous_transaction_id,
                        exclude_txn_from_change_streams,
                        max_commit_delay_ms,
                    )
                )

            async def query(
                self,
                sql: str,
                params_json: str | None = None,
                staleness_mode: str = "strong",
                staleness_value: int | None = None,
            ) -> dict[str, Any]:
                return dict(sync_c.query(sql, params_json, staleness_mode, staleness_value))

            async def execute_dml(
                self,
                sql: str,
                params_json: str | None = None,
                request_tag: str | None = None,
                transaction_tag: str | None = None,
            ) -> int:
                return int(sync_c.execute_dml(sql, params_json, request_tag, transaction_tag))

            async def execute_batch_dml(
                self,
                statements_json: str,
                request_tag: str | None = None,
                transaction_tag: str | None = None,
            ) -> list[int]:
                return list(sync_c.execute_batch_dml(statements_json, request_tag, transaction_tag))

            async def update_ddl(
                self,
                statements: list[str],
                operation_id: str | None = None,
                timeout_seconds: float | None = None,
            ) -> str:
                return str(sync_c.update_ddl(statements, operation_id, timeout_seconds))

            async def close(self) -> None:
                sync_c.close()

        return _AsyncClient()

    @staticmethod
    def _result_from_dicts(rows: list[dict[str, Any]], row_count: int = 0) -> dict[str, Any]:
        if not rows:
            return {"columns": [], "rows": [], "row_count_exact": row_count}
        cols = list(rows[0].keys())
        return {
            "columns": cols,
            "rows": [[r.get(c) for c in cols] for r in rows],
            "row_count_exact": len(rows),
        }

    def run_sql_against(
        self,
        sql: str,
        params_json: str | None,
        *,
        jobs: dict[tuple[int, str], dict[str, Any]],
        dlq: dict[str, dict[str, Any]],
        leases: dict[str, dict[str, Any]],
        unique_keys: dict[str, dict[str, Any]],
        tx_id: bytes | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = decode_json(params_json) if params_json else {}
        if self.dialect == "spangres":
            assert "@" not in sql
            for k in params:
                assert k.startswith("p") and k[1:].isdigit()
        else:
            assert "$1" not in sql
            assert "RETURNING *" not in sql
        self.tx_statements.append((tx_id, sql.strip(), params))
        upper = " ".join(sql.strip().upper().split())

        def _p(gsql_name: str, pg_ord: int) -> Any:
            return params[f"p{pg_ord}"] if self.dialect == "spangres" else params[gsql_name]

        if upper.startswith("SELECT TASK_ID FROM") and "IDEMPOTENCY_KEY" in upper:
            idem = str(_p("idempotency_key", 1))
            for job in jobs.values():
                if job.get("idempotency_key") == idem and job.get("status") in ("pending", "running"):
                    return self._result_from_dicts([{"task_id": job["task_id"]}])
            return self._result_from_dicts([])

        if upper.startswith("INSERT INTO VERTEBRA_QUEUE_JOBS"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            jobs[(shard_id, task_id)] = {
                "shard_id": shard_id,
                "task_id": task_id,
                "task_name": _p("task_name", 3),
                "lane": _p("lane", 4),
                "payload": _p("payload", 5),
                "status": _p("status", 6) if self.dialect == "googlesql" else params["p6"],
                "attempt": int(_p("attempt", 7)),
                "max_retries": int(_p("max_retries", 8)),
                "scheduled_at": _p("scheduled_at", 9),
                "enqueued_at": float(_p("enqueued_at", 10)),
                "locked_until": _p("locked_until", 11),
                "consumer_name": _p("consumer_name", 12),
                "receipt_handle": _p("receipt_handle", 13),
                "idempotency_key": _p("idempotency_key", 14),
                "traceparent": _p("traceparent", 15),
                "headers": _p("headers", 16),
                "priority": int(_p("priority", 17) or 0),
                "expires_at_ms": _p("expires_at_ms", 18),
                "completed_at_ms": _p("completed_at_ms", 19),
                "worker_id": _p("worker_id", 20),
                "heartbeat_at_ms": _p("heartbeat_at_ms", 21),
                "result": _p("result", 22),
                "error": _p("error", 23),
                "last_error": _p("error", 23),
            }
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("SELECT SHARD_ID, TASK_ID, ATTEMPT FROM") and "FOR UPDATE" in upper:
            assert tx_id is not None
            lanes = set(_p("lanes", 1))
            now_ts = float(_p("now_ts", 2))
            batch_size = int(_p("batch_size", 3))
            ordered = sorted(
                jobs.values(),
                key=lambda j: (
                    -int(j.get("priority") or 0),
                    j.get("scheduled_at") if j.get("scheduled_at") is not None else j["enqueued_at"],
                    j["enqueued_at"],
                ),
            )
            cands: list[dict[str, Any]] = []
            for job in ordered:
                if len(cands) >= batch_size:
                    break
                if job["lane"] not in lanes:
                    continue
                is_pending = job["status"] == "pending" and (
                    job["scheduled_at"] is None or float(job["scheduled_at"]) <= now_ts
                )
                is_expired = (
                    job["status"] == "running"
                    and job["locked_until"] is not None
                    and float(job["locked_until"]) <= now_ts
                )
                if is_pending or is_expired:
                    cands.append({"shard_id": job["shard_id"], "task_id": job["task_id"], "attempt": job["attempt"]})
            return self._result_from_dicts(cands)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'RUNNING'"):
            assert tx_id is not None
            locked_until = float(_p("locked_until", 1))
            consumer_name = str(_p("consumer_name", 2))
            receipt_handle = str(_p("receipt_handle", 3))
            shard_id = int(_p("shard_id", 4))
            task_id = str(_p("task_id", 5))
            claimed_job = jobs.get((shard_id, task_id))
            if claimed_job is None:
                return self._result_from_dicts([])
            claimed_job["status"] = "running"
            claimed_job["attempt"] = int(claimed_job["attempt"]) + 1
            claimed_job["locked_until"] = locked_until
            claimed_job["consumer_name"] = consumer_name
            claimed_job["worker_id"] = consumer_name
            claimed_job["receipt_handle"] = receipt_handle
            return self._result_from_dicts([dict(claimed_job)])

        if upper.startswith("DELETE FROM VERTEBRA_QUEUE_JOBS"):
            if "STATUS IN ('ACKED'" in upper:
                before_ms = int(_p("before_ms", 1))
                to_del = [
                    job_key
                    for job_key, j in jobs.items()
                    if j["status"] in ("acked", "completed", "cancelled", "dead_letter", "expired")
                    and (
                        int(j["completed_at_ms"])
                        if j.get("completed_at_ms") is not None
                        else round(float(j["enqueued_at"]) * 1000)
                    )
                    < before_ms
                ]
                for job_key in to_del:
                    del jobs[job_key]
                return self._result_from_dicts([], row_count=len(to_del))
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            target_job = jobs.get((shard_id, task_id))
            if "STATUS = 'DEAD_LETTER'" in upper:
                if target_job is not None and target_job["status"] == "dead_letter":
                    del jobs[(shard_id, task_id)]
                    return self._result_from_dicts([], row_count=1)
                return self._result_from_dicts([], row_count=0)
            if "ATTEMPT =" in upper or "ATTEMPT =" in upper:
                attempt = int(_p("attempt", 3))
                if (
                    target_job is not None
                    and target_job["status"] == "running"
                    and int(target_job["attempt"]) == attempt
                ):
                    del jobs[(shard_id, task_id)]
                    return self._result_from_dicts([], row_count=1)
                return self._result_from_dicts([], row_count=0)
            if target_job is not None:
                del jobs[(shard_id, task_id)]
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'ACKED'"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            attempt = int(_p("attempt", 3))
            completed_at_ms = int(_p("completed_at_ms", 4))
            acked_job = jobs.get((shard_id, task_id))
            if acked_job is not None and acked_job["status"] == "running" and int(acked_job["attempt"]) == attempt:
                acked_job["status"] = "acked"
                acked_job["locked_until"] = None
                acked_job["worker_id"] = None
                acked_job["consumer_name"] = None
                acked_job["completed_at_ms"] = completed_at_ms
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("SELECT * FROM VERTEBRA_QUEUE_JOBS WHERE SHARD_ID"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            found_job = jobs.get((shard_id, task_id))
            return self._result_from_dicts([dict(found_job)] if found_job is not None else [])

        if upper.startswith("SELECT * FROM VERTEBRA_QUEUE_JOBS"):
            ordered_jobs = sorted(jobs.values(), key=lambda j: float(j["enqueued_at"]))
            if "WHERE LANE =" in upper and "STATUS =" in upper:
                lane = str(_p("lane", 1))
                st = str(_p("status", 2))
                lim = int(_p("limit", 3))
                matched = [dict(j) for j in ordered_jobs if j["lane"] == lane and j["status"] == st][:lim]
            elif "WHERE LANE =" in upper:
                lane = str(_p("lane", 1))
                lim = int(_p("limit", 2))
                matched = [dict(j) for j in ordered_jobs if j["lane"] == lane][:lim]
            elif "WHERE STATUS =" in upper:
                st = str(_p("status", 1))
                lim = int(_p("limit", 2))
                matched = [dict(j) for j in ordered_jobs if j["status"] == st][:lim]
            else:
                lim = int(_p("limit", 1))
                matched = [dict(j) for j in ordered_jobs][:lim]
            return self._result_from_dicts(matched)

        if upper.startswith("SELECT J.* FROM VERTEBRA_QUEUE_JOBS J LEFT JOIN VERTEBRA_QUEUE_UNIQUE_KEYS U"):
            key = str(_p("key", 1))
            lane_filter = str(_p("lane", 2)) if "J.LANE =" in upper else None
            reserved_job_ids = {
                str(r["job_id"]) for r in unique_keys.values() if r.get("unique_key") == key and r.get("job_id")
            }
            candidates = [
                dict(j)
                for j in sorted(jobs.values(), key=lambda item: float(item["enqueued_at"]), reverse=True)
                if (j["task_id"] in reserved_job_ids or j.get("idempotency_key") == key)
                and (lane_filter is None or j["lane"] == lane_filter)
            ]
            return self._result_from_dicts(candidates[:1])

        if upper.startswith("SELECT MIN(SCHEDULED_AT) AS MIN_SCHED"):
            lanes = set(_p("lanes", 1))
            now_ts = float(_p("now_ts", 2))
            vals = [
                float(j["scheduled_at"])
                for j in jobs.values()
                if j["lane"] in lanes
                and j["status"] == "pending"
                and j["scheduled_at"] is not None
                and float(j["scheduled_at"]) > now_ts
            ]
            return self._result_from_dicts([{"min_sched": min(vals) if vals else None}])

        if upper.startswith("INSERT INTO VERTEBRA_QUEUE_DLQ"):
            dlq_id = str(_p("dlq_id", 1))
            dlq[dlq_id] = {
                "dlq_id": dlq_id,
                "shard_id": int(_p("shard_id", 2)),
                "task_id": str(_p("task_id", 3)),
                "task_name": str(_p("task_name", 4)),
                "lane": str(_p("lane", 5)),
                "payload": str(_p("payload", 6)),
                "error_type": str(_p("error_type", 7)),
                "error_message": str(_p("error_message", 8)),
                "failed_at": float(_p("failed_at", 9)),
            }
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("SELECT * FROM VERTEBRA_QUEUE_DLQ ORDER BY"):
            lim = int(_p("limit", 1))
            rows = sorted(dlq.values(), key=lambda d: float(d["failed_at"]), reverse=True)[:lim]
            return self._result_from_dicts([dict(r) for r in rows])

        if upper.startswith("SELECT * FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID"):
            dlq_id = str(_p("dlq_id", 1))
            item = dlq.get(dlq_id)
            return self._result_from_dicts([dict(item)] if item is not None else [])

        if upper.startswith("DELETE FROM VERTEBRA_QUEUE_DLQ WHERE DLQ_ID"):
            dlq_id = str(_p("dlq_id", 1))
            if dlq_id in dlq:
                del dlq[dlq_id]
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'DEAD_LETTER'"):
            last_err = str(_p("last_error", 1))
            shard_id = int(_p("shard_id", 2))
            task_id = str(_p("task_id", 3))
            attempt = int(_p("attempt", 4))
            completed_at_ms = int(_p("completed_at_ms", 5))
            dlq_job = jobs.get((shard_id, task_id))
            if dlq_job is not None and int(dlq_job["attempt"]) == attempt:
                dlq_job["status"] = "dead_letter"
                dlq_job["locked_until"] = None
                dlq_job["worker_id"] = None
                dlq_job["consumer_name"] = None
                dlq_job["receipt_handle"] = None
                dlq_job["last_error"] = last_err
                dlq_job["error"] = last_err
                dlq_job["completed_at_ms"] = completed_at_ms
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'PENDING', SCHEDULED_AT"):
            sched_at = float(_p("scheduled_at", 1))
            last_err = str(_p("last_error", 2))
            shard_id = int(_p("shard_id", 3))
            task_id = str(_p("task_id", 4))
            attempt = int(_p("attempt", 5))
            retry_job = jobs.get((shard_id, task_id))
            if retry_job is not None and int(retry_job["attempt"]) == attempt:
                retry_job["status"] = "pending"
                retry_job["scheduled_at"] = sched_at
                retry_job["locked_until"] = None
                retry_job["consumer_name"] = None
                retry_job["worker_id"] = None
                retry_job["receipt_handle"] = None
                retry_job["last_error"] = last_err
                retry_job["error"] = last_err
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET LOCKED_UNTIL"):
            locked_until = float(_p("locked_until", 1))
            shard_id = int(_p("shard_id", 2))
            task_id = str(_p("task_id", 3))
            worker_id = str(_p("worker_id", 4))
            hb_job = jobs.get((shard_id, task_id))
            if hb_job is not None and hb_job["status"] == "running" and hb_job.get("consumer_name") == worker_id:
                hb_job["locked_until"] = locked_until
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'PENDING', LOCKED_UNTIL = NULL"):
            now_ts = float(_p("now_ts", 1))
            reaped = 0
            for job in jobs.values():
                if (
                    job["status"] == "running"
                    and job["locked_until"] is not None
                    and float(job["locked_until"]) <= now_ts
                ):
                    job["status"] = "pending"
                    job["locked_until"] = None
                    job["consumer_name"] = None
                    job["worker_id"] = None
                    job["receipt_handle"] = None
                    reaped += 1
            return self._result_from_dicts([], row_count=reaped)

        if upper.startswith("SELECT SHARD_ID, TASK_ID FROM VERTEBRA_QUEUE_JOBS WHERE STATUS = 'PENDING'"):
            now_ts = float(_p("now_ts", 1))
            lim = int(_p("limit", 2))
            due = [
                {"shard_id": j["shard_id"], "task_id": j["task_id"]}
                for j in jobs.values()
                if j["status"] == "pending" and j["scheduled_at"] is not None and float(j["scheduled_at"]) <= now_ts
            ][:lim]
            return self._result_from_dicts(due)

        if upper.startswith("SELECT SHARD_ID, TASK_ID FROM VERTEBRA_QUEUE_JOBS WHERE STATUS IN ('ACKED'"):
            before_ms = int(_p("before_ms", 1))
            lim = int(_p("limit", 2))
            term_rows = [
                j
                for j in jobs.values()
                if j["status"] in ("acked", "completed", "cancelled", "dead_letter", "expired")
                and (
                    int(j["completed_at_ms"])
                    if j.get("completed_at_ms") is not None
                    else round(float(j["enqueued_at"]) * 1000)
                )
                < before_ms
            ]
            term_rows.sort(
                key=lambda j: (
                    int(j["completed_at_ms"])
                    if j.get("completed_at_ms") is not None
                    else round(float(j["enqueued_at"]) * 1000)
                )
            )
            return self._result_from_dicts(
                [{"shard_id": j["shard_id"], "task_id": j["task_id"]} for j in term_rows[:lim]]
            )

        if upper.startswith("SELECT SHARD_ID, TASK_ID FROM VERTEBRA_QUEUE_JOBS WHERE STATUS IN ('PENDING', 'RUNNING')"):
            now_ms = int(_p("now_ms", 1))
            lim = int(_p("limit", 2))
            overdue = [
                j
                for j in jobs.values()
                if j["status"] in ("pending", "running")
                and j.get("expires_at_ms") is not None
                and int(j["expires_at_ms"]) <= now_ms
            ]
            overdue.sort(key=lambda j: int(j["expires_at_ms"]))
            return self._result_from_dicts(
                [{"shard_id": j["shard_id"], "task_id": j["task_id"]} for j in overdue[:lim]]
            )

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET SCHEDULED_AT = NULL"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            prom_job = jobs.get((shard_id, task_id))
            if prom_job is not None:
                prom_job["scheduled_at"] = None
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'CANCELLED'"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            completed_at_ms = int(_p("completed_at_ms", 3))
            canc_job = jobs.get((shard_id, task_id))
            if canc_job is not None and canc_job["status"] in ("pending", "running"):
                canc_job["status"] = "cancelled"
                canc_job["locked_until"] = None
                canc_job["worker_id"] = None
                canc_job["consumer_name"] = None
                canc_job["completed_at_ms"] = completed_at_ms
                if "ERROR =" in upper:
                    err_val = str(_p("error", 4))
                    canc_job["error"] = err_val
                    canc_job["last_error"] = err_val
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET WORKER_ID ="):
            worker_id = str(_p("worker_id", 1))
            shard_id = int(_p("shard_id", 2))
            task_id = str(_p("task_id", 3))
            attempt = int(_p("attempt", 4))
            target = jobs.get((shard_id, task_id))
            if target is not None and target["status"] == "running" and int(target["attempt"]) == attempt:
                target["worker_id"] = worker_id
                target["consumer_name"] = worker_id
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET HEARTBEAT_AT_MS = NULL"):
            shard_id = int(_p("shard_id", 1))
            task_id = str(_p("task_id", 2))
            target = jobs.get((shard_id, task_id))
            if target is not None:
                target["heartbeat_at_ms"] = None
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET HEARTBEAT_AT_MS ="):
            hb_ms = int(_p("heartbeat_at_ms", 1))
            shard_id = int(_p("shard_id", 2))
            task_id = str(_p("task_id", 3))
            target = jobs.get((shard_id, task_id))
            if target is not None and target["status"] == "running":
                target["heartbeat_at_ms"] = hb_ms
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_JOBS SET STATUS = 'EXPIRED'"):
            if "WHERE SHARD_ID =" in upper:
                shard_id = int(_p("shard_id", 1))
                task_id = str(_p("task_id", 2))
                now_ms = int(_p("now_ms", 3))
                target = jobs.get((shard_id, task_id))
                if target is not None:
                    target["status"] = "expired"
                    target["locked_until"] = None
                    target["worker_id"] = None
                    target["consumer_name"] = None
                    target["completed_at_ms"] = now_ms
                    return self._result_from_dicts([], row_count=1)
                return self._result_from_dicts([], row_count=0)
            now_ms = int(_p("now_ms", 1))
            expired_count = 0
            for job in jobs.values():
                if (
                    job["status"] in ("pending", "running")
                    and job.get("expires_at_ms") is not None
                    and int(job["expires_at_ms"]) <= now_ms
                ):
                    job["status"] = "expired"
                    job["locked_until"] = None
                    job["worker_id"] = None
                    job["consumer_name"] = None
                    job["completed_at_ms"] = now_ms
                    expired_count += 1
            return self._result_from_dicts([], row_count=expired_count)

        if "VERTEBRA_QUEUE_LEASES" in upper and "ON CONFLICT" in upper:
            lease_key = str(params["p1"])
            owner_id = str(params["p2"])
            expires_at = float(params["p3"])
            slot = params["p4"]
            now_ts = float(params["p5"])
            existing = leases.get(lease_key)
            if existing is None or float(existing["expires_at"]) <= now_ts or existing["owner_id"] == owner_id:
                rec = {
                    "lease_key": lease_key,
                    "owner_id": owner_id,
                    "expires_at": expires_at,
                    "last_slot": slot if slot is not None else (existing.get("last_slot") if existing else None),
                }
                leases[lease_key] = rec
                return self._result_from_dicts([dict(rec)])
            return self._result_from_dicts([])

        if upper.startswith("SELECT LEASE_KEY, OWNER_ID, EXPIRES_AT, LAST_SLOT FROM VERTEBRA_QUEUE_LEASES"):
            lease_key = str(params["lease_key"])
            existing = leases.get(lease_key)
            return self._result_from_dicts([dict(existing)] if existing is not None else [])

        if upper.startswith("INSERT INTO VERTEBRA_QUEUE_LEASES"):
            ins_lease_key = str(params["lease_key"])
            ins_rec: dict[str, Any] = {
                "lease_key": ins_lease_key,
                "owner_id": str(params["owner_id"]),
                "expires_at": float(params["expires_at"]),
                "last_slot": params.get("last_slot"),
            }
            leases[ins_lease_key] = ins_rec
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_LEASES"):
            lease_key = str(params["lease_key"])
            leases[lease_key] = {
                "lease_key": lease_key,
                "owner_id": str(params["owner_id"]),
                "expires_at": float(params["expires_at"]),
                "last_slot": params.get("last_slot"),
            }
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("DELETE FROM VERTEBRA_QUEUE_LEASES"):
            lease_key = str(_p("lease_key", 1))
            owner_id = str(_p("owner_id", 2))
            existing = leases.get(lease_key)
            if existing is not None and existing["owner_id"] == owner_id:
                del leases[lease_key]
                return self._result_from_dicts([], row_count=1)
            return self._result_from_dicts([], row_count=0)

        if "VERTEBRA_QUEUE_UNIQUE_KEYS" in upper and "ON CONFLICT" in upper:
            key = str(params["p1"])
            expires_at = float(params["p2"])
            now_ts = float(params["p3"])
            existing_uk = unique_keys.get(key)
            if existing_uk is None or (
                existing_uk.get("expires_at") is not None and float(existing_uk["expires_at"]) <= now_ts
            ):
                unique_keys[key] = {"unique_key": key, "expires_at": expires_at}
                return self._result_from_dicts([{"unique_key": key}])
            return self._result_from_dicts([])

        if upper.startswith(
            "SELECT UNIQUE_KEY, EXPIRES_AT, JOB_ID, TASK_NAME, CREATED_AT_MS FROM VERTEBRA_QUEUE_UNIQUE_KEYS"
        ):
            key = str(_p("key", 1))
            existing_uk = unique_keys.get(key)
            return self._result_from_dicts([dict(existing_uk)] if existing_uk is not None else [])

        if upper.startswith("SELECT UNIQUE_KEY, JOB_ID, TASK_NAME, CREATED_AT_MS FROM VERTEBRA_QUEUE_UNIQUE_KEYS"):
            key = str(_p("key", 1))
            now_ts = float(_p("now_ts", 2))
            existing_uk = unique_keys.get(key)
            if existing_uk is not None and (
                existing_uk.get("expires_at") is None or float(existing_uk["expires_at"]) > now_ts
            ):
                return self._result_from_dicts([dict(existing_uk)])
            return self._result_from_dicts([])

        if upper.startswith("SELECT UNIQUE_KEY, EXPIRES_AT FROM VERTEBRA_QUEUE_UNIQUE_KEYS"):
            key = str(params["key"])
            existing_uk = unique_keys.get(key)
            return self._result_from_dicts([dict(existing_uk)] if existing_uk is not None else [])

        if upper.startswith("INSERT INTO VERTEBRA_QUEUE_UNIQUE_KEYS"):
            key = str(_p("key", 1))
            raw_exp = _p("expires_at", 2)
            rec_uk: dict[str, Any] = {
                "unique_key": key,
                "expires_at": float(raw_exp) if raw_exp is not None else None,
            }
            if "JOB_ID" in upper:
                rec_uk["job_id"] = _p("job_id", 3)
                rec_uk["task_name"] = _p("task_name", 4)
                rec_uk["created_at_ms"] = _p("created_at_ms", 5)
            unique_keys[key] = rec_uk
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("UPDATE VERTEBRA_QUEUE_UNIQUE_KEYS"):
            key = str(_p("key", 1))
            raw_exp = _p("expires_at", 2)
            rec_uk_upd: dict[str, Any] = {
                "unique_key": key,
                "expires_at": float(raw_exp) if raw_exp is not None else None,
            }
            if "JOB_ID" in upper:
                rec_uk_upd["job_id"] = _p("job_id", 3)
                rec_uk_upd["task_name"] = _p("task_name", 4)
                rec_uk_upd["created_at_ms"] = _p("created_at_ms", 5)
            unique_keys[key] = rec_uk_upd
            return self._result_from_dicts([], row_count=1)

        if upper.startswith("DELETE FROM VERTEBRA_QUEUE_UNIQUE_KEYS"):
            if "EXPIRES_AT IS NOT NULL" in upper:
                before_s = float(_p("before_s", 1))
                to_drop = [
                    k
                    for k, v in unique_keys.items()
                    if v.get("expires_at") is not None and float(v["expires_at"]) <= before_s
                ]
                for k in to_drop:
                    del unique_keys[k]
                return self._result_from_dicts([], row_count=len(to_drop))
            key = str(_p("key", 1))
            existing_uk = unique_keys.get(key)
            if existing_uk is None:
                return self._result_from_dicts([], row_count=0)
            if "JOB_ID =" in upper:
                expected_job_id = str(_p("expected_job_id", 2))
                if existing_uk.get("job_id") != expected_job_id:
                    return self._result_from_dicts([], row_count=0)
            del unique_keys[key]
            return self._result_from_dicts([], row_count=1)

        msg = f"Unhandled SQL in _InMemorySpannerQueueWire ({self.dialect}): {sql}"
        raise AssertionError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_queue_stores_interactive_rw_claim_abort_retry_fencing_and_leases(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """SyncSpannerQueueStore and AsyncSpannerQueueStore claim atomically via run_in_transaction, retry Aborted, fence attempts, and prevent lease theft across GoogleSQL and Spangres."""
    wire = _InMemorySpannerQueueWire(dialect)
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        dialect=dialect,
        endpoint="http://localhost:9010",
    )
    sync_drv = SyncSpannerDriver(cfg, client=wire.sync_client())
    async_drv = AsyncSpannerDriver(cfg, client=wire.async_client())
    sync_store = SyncSpannerQueueStore(sync_drv, shard_count=8)
    async_store = AsyncSpannerQueueStore(async_drv, shard_count=8)

    assert isinstance(sync_store, SyncQueueStore)
    assert isinstance(async_store, AsyncQueueStore)

    await async_store.ensure_schema()
    assert len(wire.ddl_statements) == 5

    job_id = str(uuid4())
    enqueued_id = sync_store.enqueue(
        QueueEntry(
            job_id=job_id,
            queue="io",
            task_name="sync_job",
            payload=b'{"x": 1}',
            attempt=1,
            max_attempts=2,
            headers={"idempotency_key": "idem-spanner-1"},
        )
    )
    assert enqueued_id == job_id
    dup_id = await async_store.enqueue(
        QueueEntry(
            job_id=str(uuid4()),
            queue="io",
            task_name="sync_job",
            payload=b'{"x": 1}',
            attempt=1,
            max_attempts=2,
            headers={"idempotency_key": "idem-spanner-1"},
        )
    )
    assert dup_id == job_id

    wire.abort_remaining = 1
    commits_before = len(wire.commits)
    claimed = await async_store.claim(["io"], limit=2, worker_id="w-1", lease_seconds=30.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == job_id
    assert claimed[0].attempt == 1
    assert len(wire.commits) == commits_before + 1
    assert any(prev is not None for prev in wire.previous_tx_ids)

    claim_tx_id = wire.commits[-1]
    claim_tx_sqls = [s for tid, s, _ in wire.tx_statements if tid == claim_tx_id]
    assert len(claim_tx_sqls) == 2
    assert "FOR UPDATE" in claim_tx_sqls[0].upper()
    if dialect == "googlesql":
        assert "THEN RETURN *" in claim_tx_sqls[1].upper()
    else:
        assert "RETURNING *" in claim_tx_sqls[1].upper()

    assert await async_store.heartbeat([job_id], worker_id="w-1", lease_seconds=60.0) == 1
    assert await async_store.ack(job_id, attempt=99) is False

    assert await async_store.nack(job_id, attempt=1, error="transient", retry_delay_s=0.0) is True
    claimed_retry = sync_store.claim(["io"], limit=1, worker_id="w-2", lease_seconds=30.0)
    assert len(claimed_retry) == 1
    assert claimed_retry[0].attempt == 2
    assert sync_store.ack(job_id, attempt=1) is False
    assert sync_store.nack(job_id, attempt=2, error="fatal", retry_delay_s=None) is True
    assert len(wire.dlq) == 1

    assert await async_store.acquire_lease("cron:hourly", owner_id="node-a", ttl_seconds=60.0) is True
    assert await async_store.acquire_lease("cron:hourly", owner_id="node-b", ttl_seconds=60.0) is False
    assert await async_store.acquire_lease("cron:hourly", owner_id="node-a", ttl_seconds=90.0) is True
    assert await async_store.release_lease("cron:hourly", owner_id="node-b") is False
    assert await async_store.release_lease("cron:hourly", owner_id="node-a") is True
    assert sync_store.acquire_lease("cron:hourly", owner_id="node-b", ttl_seconds=60.0) is True

    assert sync_store.reserve_unique_key("uk-1", ttl_seconds=60.0) is True
    assert await async_store.reserve_unique_key("uk-1", ttl_seconds=60.0) is False
    assert await async_store.release_unique_key("uk-1") is True
    assert await async_store.reserve_unique_key("uk-1", ttl_seconds=60.0) is True

    await async_store.aclose()
    sync_store.close()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_queue_backend_integrations_and_stateless_receipt_handles(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """SpannerQueueBackend powers QueueService, litestar-queues StoreQueueBackend, and Django TaskBackend with zero receipt-index leaks."""
    wire = _InMemorySpannerQueueWire(dialect)
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        dialect=dialect,
        endpoint="http://localhost:9010",
    )
    sync_drv = SyncSpannerDriver(cfg, client=wire.sync_client())
    async_drv = AsyncSpannerDriver(cfg, client=wire.async_client())

    settings = QueueBackendSettings(
        kind="spangres" if dialect == "spangres" else "spanner",
        dsn=f"{'spangres' if dialect == 'spangres' else 'spanner'}://projects/p/instances/i/databases/d",
        driver=async_drv,
        sync_driver=sync_drv,
        visibility_timeout_seconds=0.01,
        poll_interval_seconds=0.01,
    )
    qsvc = QueueService(settings)
    backend = qsvc.backend
    assert isinstance(backend, SpannerQueueBackend)
    assert not hasattr(backend, "_receipt_index")

    for idx in range(25):
        await backend.enqueue(
            TaskEnvelope(
                task_id=f"timeout-{idx}",
                task_name="noop",
                lane="io",
                attempt=1,
                max_retries=3,
                enqueued_at=time.time(),
            )
        )
    batch = await backend.dequeue(("io",), batch_size=25, visibility_timeout_s=0.001)
    assert len(batch) == 25
    assert all((item.receipt_handle or "").startswith("spn:") for item in batch)
    await asyncio.sleep(0.005)
    reclaimed = await backend.dequeue(("io",), batch_size=5, visibility_timeout_s=30.0)
    assert len(reclaimed) == 5
    assert all(item.attempt == 2 for item in reclaimed)
    assert reclaimed[0].receipt_handle is not None
    await backend.ack(reclaimed[0].receipt_handle)
    for rest in reclaimed[1:]:
        await backend.ack(rest)
    for item in await backend.dequeue(("io",), batch_size=25, visibility_timeout_s=30.0):
        await backend.ack(item)

    django_backend = DjangoVertebraTaskBackend(alias=f"spanner_{dialect}", params={"OPTIONS": {"queue_service": qsvc}})

    @django_backend.task("multiply_pair")
    def multiply_pair(a: int, b: int) -> int:
        return a * b

    dj_res = django_backend.enqueue(multiply_pair, 6, 7)
    assert dj_res.id != ""
    drained = await qsvc.drain_once(("io",))
    assert drained == [42]

    assert backend.store is not None
    litestar_backend = StoreQueueBackend(store=backend.store)
    ls_rec = await litestar_backend.enqueue("audit.log", args=("ok",), queue="default", max_retries=1)
    claimed_ls = await litestar_backend.claim_task(ls_rec.id, expected_retry_count=0)
    assert claimed_ls is not None
    assert claimed_ls.id == ls_rec.id
    assert claimed_ls.status == "running"
    assert await litestar_backend.claim_task(ls_rec.id, expected_retry_count=0) is None
    await litestar_backend.complete_task(ls_rec.id, expected_retry_count=0)

    await qsvc.close()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_queue_store_table_validation_and_store_dlq_replay(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """Verify Spanner queue stores reject unsafe table_name and replay_dlq cleans up dead_letter rows."""
    wire = _InMemorySpannerQueueWire(dialect)
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        dialect=dialect,
        endpoint="http://localhost:9010",
    )
    sync_drv = SyncSpannerDriver(cfg, client=wire.sync_client())
    async_drv = AsyncSpannerDriver(cfg, client=wire.async_client())

    with pytest.raises(ValueError, match="table_name"):
        SyncSpannerQueueStore(sync_drv, table_name="bad; DROP TABLE vertebra_queue_jobs")
    with pytest.raises(ValueError, match="table_name"):
        AsyncSpannerQueueStore(async_drv, table_name="bad; DROP TABLE vertebra_queue_jobs")

    settings = QueueBackendSettings(
        kind="spangres" if dialect == "spangres" else "spanner",
        dsn=f"{'spangres' if dialect == 'spangres' else 'spanner'}://projects/p/instances/i/databases/d",
        driver=async_drv,
        sync_driver=sync_drv,
    )
    backend = SpannerQueueBackend(settings)
    env = TaskEnvelope(
        task_id="dlq-replay-1",
        task_name="flaky",
        lane="io",
        attempt=1,
        max_retries=1,
        enqueued_at=time.time(),
    )
    await backend.enqueue(env)
    claimed = await backend.dequeue(("io",), batch_size=1)
    assert len(claimed) == 1
    dlq_entry = await backend.nack(claimed[0], error="boom", send_to_dlq=True)
    assert dlq_entry is not None

    replayed = await backend.replay_dlq(dlq_entry.dlq_id)
    assert replayed is not None
    reclaimed = await backend.dequeue(("io",), batch_size=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].task_id == "dlq-replay-1"
    assert reclaimed[0].attempt == 1
    await backend.ack(reclaimed[0])
    await backend.close()


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ("", -7286425919675154353),
        (b"", -7286425919675154353),
        ("a", -5528939962900187677),
        ("abc", 2640714258260161385),
        ("abcd", 1897425971756105985),
        ("alphabet", -2427165924636348523),
        ("user-42", 5340128202724118949),
        ("hello world", 6381520714923946011),
        ("1234567890123456", -1207638713069763548),
        ("12345678901234567", -8899476072607698506),
        ("a" * 32, -1553158580277161704),
        ("a" * 40, 2168401864796137711),
        ("a" * 64, 5893282057753879417),
        ("a" * 65, 3757222151784111843),
        ("a" * 128, -1828092727725220631),
        ("café-🚀", -3691481111669660328),
    ],
)
def test_farm_fingerprint64_matches_google_reference_vectors(payload: bytes | str, expected: int) -> None:
    """farm_fingerprint64 matches Google FarmHash Fingerprint64 across all byte-length branches and UTF-8."""
    assert farm_fingerprint64(payload) == expected


@pytest.mark.parametrize(
    ("task_id", "shard_count"),
    [
        ("", 16),
        ("abc", 16),
        ("alphabet", 16),
        ("user-42", 8),
        ("café-🚀", 32),
        ("x" * 96, 64),
        ("single-shard", 1),
        ("zero-shard", 0),
    ],
)
def test_compute_spanner_shard_id_uses_farm_fingerprint_and_delegates(task_id: str, shard_count: int) -> None:
    """compute_spanner_shard_id computes MOD(ABS(FARM_FINGERPRINT), shard_count) and matches SpannerQueueBackend."""
    expected_shards = max(1, shard_count)
    expected = abs(farm_fingerprint64(task_id)) % expected_shards
    shard_id = compute_spanner_shard_id(task_id, shard_count)
    assert shard_id == expected
    assert 0 <= shard_id < expected_shards
    assert SpannerQueueBackend.compute_shard_id(task_id, shard_count) == expected


@pytest.mark.parametrize("spill_threshold_bytes", [8, 4096], ids=["spilled", "inline"])
def test_change_feed_concurrent_publish_event_assigns_unique_contiguous_ids(spill_threshold_bytes: int) -> None:
    """Concurrent ChangeFeed.publish_event calls produce strictly unique contiguous 1..N*M event_ids."""
    num_threads = 8
    events_per_thread = 25
    total_events = num_threads * events_per_thread
    feed = ChangeFeed(
        CDCConfig(
            channel="concurrent_cdc",
            spill_threshold_bytes=spill_threshold_bytes,
            max_history_size=total_events,
            max_memory_spills=total_events,
        )
    )
    barrier = threading.Barrier(num_threads)
    results_lock = threading.Lock()
    published: list[tuple[str, Any]] = []
    errors: list[BaseException] = []

    def _worker(worker_id: int) -> None:
        try:
            barrier.wait(timeout=5.0)
            local_results = [
                feed.publish_event("INSERT", {"worker": worker_id, "seq": seq}) for seq in range(events_per_thread)
            ]
            with results_lock:
                published.extend(local_results)
        except BaseException as exc:
            with results_lock:
                errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(idx,)) for idx in range(num_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert not errors
    assert len(published) == total_events
    event_ids = [evt.event_id for _, evt in published]
    assert len(set(event_ids)) == total_events
    assert set(event_ids) == set(range(1, total_events + 1))

    replayed = feed.replay_since(0)
    assert [evt.event_id for evt in replayed] == list(range(1, total_events + 1))

    for wire_envelope, evt in published:
        hydrated = feed.hydrate_notification(wire_envelope)
        assert hydrated == evt


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_queue_store_extended_capabilities_across_dialects(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """SyncSpannerQueueStore and AsyncSpannerQueueStore implement all extended AsyncQueueStore capabilities across dialects."""
    wire = _InMemorySpannerQueueWire(dialect)
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        dialect=dialect,
        endpoint="http://localhost:9010",
    )
    sync_drv = SyncSpannerDriver(cfg, client=wire.sync_client())
    async_drv = AsyncSpannerDriver(cfg, client=wire.async_client())
    sync_store = SyncSpannerQueueStore(sync_drv, shard_count=8, cleanup_on_ack=False)
    async_store = AsyncSpannerQueueStore(async_drv, shard_count=8, cleanup_on_ack=False)

    await async_store.ensure_schema()

    now_ms = int(time.time() * 1000)
    j1 = await async_store.enqueue(
        QueueEntry(
            job_id="ext-job-1",
            queue="q-alpha",
            task_name="task.one",
            payload=b"p1",
            priority=5,
            headers={"idempotency_key": "k-1"},
        )
    )
    j2 = sync_store.enqueue(
        QueueEntry(
            job_id="ext-job-2",
            queue="q-beta",
            task_name="task.two",
            payload=b"p2",
            priority=10,
            expires_at_ms=now_ms - 1000,
            headers={"key": "k-2"},
        )
    )
    j3 = await async_store.enqueue(
        QueueEntry(
            job_id="ext-job-3",
            queue="q-alpha",
            task_name="task.three",
            payload=b"p3",
            expires_at_ms=now_ms + 60_000,
        )
    )

    all_entries = await async_store.list_entries(queue=None, limit=10)
    assert {entry.job_id for entry in all_entries} == {j1, j2, j3}
    alpha_entries = sync_store.list_entries("q-alpha", status="ready", limit=10)
    assert {entry.job_id for entry in alpha_entries} == {j1, j3}

    by_key = await async_store.get_entry_by_key("k-1")
    assert by_key is not None
    assert by_key.job_id == j1
    assert by_key.priority == 5
    assert sync_store.get_entry_by_key("k-2", queue="q-alpha") is None
    by_key_beta = sync_store.get_entry_by_key("k-2", queue="q-beta")
    assert by_key_beta is not None
    assert by_key_beta.job_id == j2

    wire.abort_remaining = 1
    assert await async_store.reserve_identity("id-forever", job_id=j1, task_name="task.one", ttl_seconds=None) is None
    winner = sync_store.reserve_identity("id-forever", job_id="other-job", task_name="task.other", ttl_seconds=60.0)
    assert isinstance(winner, QueueReservation)
    assert winner.key == "id-forever"
    assert winner.job_id == j1
    assert winner.task_name == "task.one"

    fetched_res = await async_store.get_reservation("id-forever")
    assert fetched_res is not None
    assert fetched_res.job_id == j1

    assert sync_store.reset_identity("id-forever", expected_job_id="wrong-job") is False
    assert await async_store.get_reservation("id-forever") is not None

    assert await async_store.reserve_identity("id-temp", job_id=j3, task_name="task.three", ttl_seconds=0.001) is None
    await asyncio.sleep(0.01)
    assert await async_store.get_reservation("id-temp") is None
    assert sync_store.reserve_identity("id-temp", job_id=j3, task_name="task.three", ttl_seconds=60.0) is None
    assert await async_store.reset_identity("id-temp", expected_job_id=j3) is True
    assert await async_store.get_reservation("id-temp") is None

    claimed = await async_store.claim(["q-alpha"], limit=1, worker_id="w-init", lease_seconds=30.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == j1
    assert claimed[0].worker_id == "w-init"

    assert sync_store.assign_worker(j1, worker_id="w-reassigned", attempt=1) is True
    assert await async_store.assign_worker(j1, worker_id="w-wrong", attempt=99) is False
    reloaded = await async_store.get_entry(j1)
    assert reloaded is not None
    assert reloaded.worker_id == "w-reassigned"

    assert await async_store.touch_heartbeats([j1], heartbeat_at_ms=123456) == 1
    hb_entry = sync_store.get_entry(j1)
    assert hb_entry is not None
    assert hb_entry.heartbeat_at_ms == 123456
    assert sync_store.null_heartbeats([j1]) == 1
    cleared_entry = await async_store.get_entry(j1)
    assert cleared_entry is not None
    assert cleared_entry.heartbeat_at_ms is None

    assert await async_store.ack(j1, attempt=1) is True
    assert sync_store.interrupt_task(j3, error="cancelled-by-operator") is True
    interrupted = await async_store.get_entry(j3)
    assert interrupted is not None
    assert interrupted.error == "cancelled-by-operator"

    assert await async_store.expire_overdue(now_ms=now_ms, limit=5) == 1
    expired_entry = sync_store.get_entry(j2)
    assert expired_entry is not None
    assert expired_entry.completed_at_ms == now_ms

    j4_pending = await async_store.enqueue(
        QueueEntry(
            job_id="ext-job-4",
            queue="q-alpha",
            task_name="task.four",
            payload=b"p4",
        )
    )
    future_ms = int(time.time() * 1000) + 60_000
    assert sync_store.cleanup_terminal(before_ms=future_ms, limit=2) == 2
    assert await async_store.cleanup_terminal(before_ms=future_ms) == 1

    remaining = await async_store.list_entries(queue=None, limit=10)
    assert [entry.job_id for entry in remaining] == [j4_pending]
    assert await async_store.get_reservation("id-forever") is not None

    await async_store.aclose()
    sync_store.close()
