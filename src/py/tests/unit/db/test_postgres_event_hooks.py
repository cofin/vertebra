"""Unit tests for ``listen_notifications()``, ``open_replication()``, and dedicated event cleanup."""

from __future__ import annotations

import asyncio

import pytest

from tests.unit.db.postgres_ports import (
    FakeAsyncPostgresPool,
    FakeDirectClientFactory,
    FakePostgresCluster,
    FakeSyncPostgresPool,
)
from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.change_stream import MemoryPostgresCheckpointStore, PostgresChangeConfig
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.events import PostgresReplicationDriver
from vertebra.db.adapters.postgres.types import PostgresReplicationRecord
from vertebra.db.driver.capabilities import NotificationEvent
from vertebra.typing import ImproperConfigurationError


@pytest.fixture
def anyio_backend() -> str:
    """Restrict async event hook tests cleanly to asyncio."""
    return "asyncio"


def test_pooled_mode_without_direct_dsn_rejects_event_streams() -> None:
    """LISTEN/NOTIFY and logical replication require ``direct_dsn`` when primary uses transaction pooling."""
    pooled_cfg = PostgreSQLConfig(
        dsn="postgresql://pooler.internal:6432/appdb",
        pooler_mode="transaction",
    )
    drv = AsyncPostgreSQLDriver(pooled_cfg, pool=FakeAsyncPostgresPool())
    with pytest.raises(ImproperConfigurationError):
        drv.listen_notifications()
    with pytest.raises(ImproperConfigurationError):
        drv.open_replication(PostgresChangeConfig(slot="vtb_slot", publication="vtb_pub"))


@pytest.mark.anyio
async def test_async_notifications_dial_direct_and_close_on_driver_shutdown() -> None:
    """Notification source dials a dedicated direct connection and cleans up on driver close."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    direct_factory = FakeDirectClientFactory(cluster)
    cfg = PostgreSQLConfig(
        dsn="postgresql://pooler.internal:6432/appdb",
        direct_dsn="postgresql://primary.internal:5432/appdb",
        pooler_mode="transaction",
    )
    drv = AsyncPostgreSQLDriver(cfg, pool=pool, client_factory=direct_factory)
    assert isinstance(drv, PostgresReplicationDriver)

    source = drv.listen_notifications()
    await source.subscribe(["orders"])
    stream = source.listen()

    async def _next_event() -> NotificationEvent:
        return await anext(stream)

    recv_task = asyncio.create_task(_next_event())
    while not cluster.listeners:
        await asyncio.sleep(0)
    await source.publish("orders", "created-1")
    received = await recv_task
    assert received.channel == "orders"
    assert received.payload == b"created-1"
    assert pool.acquire_calls == 1
    assert len(direct_factory.opened_clients) == 1
    assert direct_factory.dialed_kwargs[0]["host"] == "primary.internal"
    assert len(drv._events) == 1

    await drv.close()
    assert len(drv._events) == 0
    assert direct_factory.opened_clients[0].closed is True
    assert pool.closed is True


@pytest.mark.anyio
async def test_async_replication_stream_checkpoints_and_closes_on_driver_shutdown() -> None:
    """Replication source fetches identity, streams WAL frames, saves CAS checkpoints, and closes on teardown."""
    cluster = FakePostgresCluster()
    cluster.set_replication_identity("cdc_slot", "cdc_pub", fingerprint="fp-v1")
    cluster.enqueue_replication_records(
        PostgresReplicationRecord(kind="begin", payload={"xid": 42}),
        PostgresReplicationRecord(kind="insert", payload={"table": "orders", "row": {"id": 1}}),
        PostgresReplicationRecord(kind="commit", payload={"xid": 42}, commit_end_lsn=1024),
    )
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    direct_factory = FakeDirectClientFactory(cluster)
    cfg = PostgreSQLConfig(
        dsn="postgresql://pooler.internal:6432/appdb",
        direct_dsn="postgresql://primary.internal:5432/appdb",
        pooler_mode="transaction",
    )
    drv = AsyncPostgreSQLDriver(cfg, pool=pool, client_factory=direct_factory)
    store = MemoryPostgresCheckpointStore()
    change_cfg = PostgresChangeConfig(slot="cdc_slot", publication="cdc_pub", reconnect=False)

    cdc = drv.open_replication(change_cfg, store)
    events = [evt async for evt in cdc]
    assert [evt.operation for evt in events] == ["begin", "insert", "commit"]
    assert events[1].payload["table"] == "orders"
    await cdc.acknowledge(events[-1].payload["commit_end_lsn"])
    assert cdc.checkpoint is not None
    assert cdc.checkpoint.commit_end_lsn == 1024
    assert pool.acquire_calls == 0
    assert len(direct_factory.opened_clients) == 2
    assert direct_factory.opened_clients[0].closed is True

    await drv.close()
    assert direct_factory.opened_clients[1].closed is True
    assert len(drv._events) == 0


@pytest.mark.anyio
async def test_sync_driver_supports_event_hooks_and_deterministic_close() -> None:
    """``SyncPostgreSQLDriver`` exposes dedicated async event hooks and closes tracked sessions cleanly."""
    cluster = FakePostgresCluster()
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    direct_factory = FakeDirectClientFactory(cluster)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, client_factory=direct_factory)

    listener = await drv._open_notification_listener(["alerts"])
    await drv._publish_notification("alerts", "ping")
    frame = await listener.next()
    assert frame is not None
    assert frame[1] == "alerts"
    assert frame[2] == b"ping"
    assert len(drv._events) == 1

    await drv._events.close_all()
    assert listener.closed is True
    assert direct_factory.opened_clients[0].closed is True
    drv.close()
    assert pool.closed is True
