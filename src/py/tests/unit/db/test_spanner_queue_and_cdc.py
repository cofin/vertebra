"""Unit tests for SpannerQueueBackend and SpannerChangeStreamSource (CDC + ObjectStore spill)."""

from __future__ import annotations

import time
from typing import Any

import pytest

from tests.unit.db.test_spanner import FakeSpannerClient
from vertebra.db.adapters.spanner import SpannerConfig, SyncSpannerDriver
from vertebra.db.cdc import AsyncSpannerChangeStreamSource, SyncSpannerChangeStreamSource
from vertebra.db.channels import DatabaseChannelBackend
from vertebra.framework.queues import (
    QueueConfig,
    QueueService,
    SpannerQueueBackend,
    TaskEnvelope,
    task,
)
from vertebra.framework.queues.backends import SpannerQueueBackend as ExportedSpannerQueueBackend
from vertebra.storage import SyncObjectStore


@pytest.mark.anyio
async def test_spanner_queue_backend_lifecycle_dlq_and_leases() -> None:
    """Verify SpannerQueueBackend hash sharding, DDL, enqueue/dequeue/ack/nack, DLQ, and CronLease."""
    assert ExportedSpannerQueueBackend is SpannerQueueBackend
    cfg = QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d")
    driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
    backend = SpannerQueueBackend(cfg, driver=driver, shard_count=16)

    gsql_ddls = backend.ddl_statements("googlesql")
    assert any("NULL_FILTERED INDEX" in s and "STORING" in s for s in gsql_ddls)
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

    service = QueueService(QueueConfig(backend_url="spanner+googlesql://projects/p/instances/i/databases/d"))

    @task(name="compute_square", lane="cpu")
    def compute_square(x: int) -> int:
        return x * x

    service.register(compute_square)
    await service.enqueue("compute_square", 7)
    results = await service.drain_once(("cpu",))
    assert results == [49]
    await service.close()
    await backend.close()
    driver.close()


@pytest.mark.anyio
async def test_spanner_change_stream_source_partitions_and_object_store_spill() -> None:
    """Verify SpannerChangeStreamSource GoogleSQL/Spangres queries, child partition scheduling, and ObjectStore spill."""
    driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
    store = SyncObjectStore("memory://")
    channel_backend, source = DatabaseChannelBackend.from_spanner_change_stream(
        driver,
        "orders_stream",
        channel="orders_cdc",
        spill_store=store,
        spill_threshold_bytes=400,
        history=16,
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

    parsed = []
    for row in raw_rows:
        parsed.extend(source.parse_raw_row(row, partition_token="parent-token-1"))
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
async def test_spanner_change_stream_poll_binds_params_for_dialect(dialect: str, mode: str) -> None:
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
