"""Tests for SyncSHMBrokerTransport, SyncInMemoryBrokerTransport, and SyncSHMSessionBroker."""

from __future__ import annotations

import os
import threading
import time
import warnings

import pytest

from vertebra.extensions.alchemy.broker import AsyncSHMSessionBroker, SyncSHMSessionBroker
from vertebra.extensions.broker import (
    AsyncInMemoryBrokerTransport,
    AsyncSHMBrokerTransport,
    BrokerConfig,
    BrokerPlugin,
    SyncInMemoryBrokerTransport,
    SyncSHMBrokerTransport,
)
from vertebra.server.platform import ZPagesRegistry
from vertebra.telemetry.otel import DbPoolRecorder
from vertebra.utils.serializers import decode_json


def test_in_memory_broker_transport_lifecycle() -> None:
    """Verify in-memory transport acquire, release, and statement caching."""
    cfg = BrokerConfig(pool_size=2, max_overflow=1)
    transport = SyncInMemoryBrokerTransport(cfg)

    l1 = transport.acquire(worker_id=101, bind_key="test")
    assert l1.worker_id == 101
    assert not l1.is_overflow

    l2 = transport.acquire(worker_id=102, bind_key="test")
    assert not l2.is_overflow

    l3 = transport.acquire(worker_id=103, bind_key="test")
    assert l3.is_overflow

    with pytest.raises(TimeoutError):
        transport.acquire(worker_id=104, timeout_ms=50, bind_key="test")

    assert transport.release(l1.lease_id) is True
    l4 = transport.acquire(worker_id=105, timeout_ms=500, bind_key="test")
    assert l4.worker_id == 105

    plan = transport.get_or_compile_statement(
        "postgresql",
        "SELECT 1",
        lambda sql: (sql, ()),
    )
    assert plan.compiled_sql == "SELECT 1"

    reclaimed = transport.reclaim_worker(102)
    assert reclaimed == 1


@pytest.mark.anyio
async def test_shm_broker_transport_async_and_cache() -> None:
    """Verify SHMBrokerTransport async acquire, release, and prepared statement caching."""
    name = f"/vtb-test-shm-trans-{os.getpid()}-{int(time.time())}"
    cfg = BrokerConfig(pool_size=3, max_overflow=1, shm_name=name)
    transport = AsyncSHMBrokerTransport(cfg)

    lease = await transport.acquire(worker_id=os.getpid(), bind_key="app")
    assert lease.worker_id == os.getpid()
    assert not lease.is_overflow

    m = transport.metrics()
    assert m.active_leases == 1

    assert await transport.release(lease.lease_id) is True
    assert transport.metrics().active_leases == 0

    compiler_calls = 0

    def compiler(sql: str) -> tuple[str, tuple[str, ...]]:
        nonlocal compiler_calls
        compiler_calls += 1
        return (f"{sql} -- compiled", ("p1", "p2"))

    plan1 = transport.get_or_compile_statement("postgresql", "SELECT $1, $2", compiler)
    assert plan1.compiled_sql == "SELECT $1, $2 -- compiled"
    assert plan1.param_names == ("p1", "p2")
    assert compiler_calls == 1

    plan2 = transport.get_or_compile_statement("postgresql", "SELECT $1, $2", compiler)
    assert plan2.compiled_sql == "SELECT $1, $2 -- compiled"
    assert plan2.param_names == ("p1", "p2")
    assert compiler_calls == 1


def test_shm_session_broker_checkout_checkin() -> None:
    """Verify SHMSessionBroker checkout and checkin hook lifecycle."""
    name = f"/vtb-test-session-{os.getpid()}-{int(time.time())}"
    cfg = BrokerConfig(pool_size=2, max_overflow=1, shm_name=name)
    transport = SyncSHMBrokerTransport(cfg)
    session_broker = SyncSHMSessionBroker(transport=transport)

    class FakeConn:
        pass

    conn = FakeConn()
    session_broker.on_checkout(conn)
    assert transport.metrics().active_leases == 1

    session_broker.on_checkin(conn)
    assert transport.metrics().active_leases == 0


def test_broker_plugin_zpages_integration() -> None:
    """Verify BrokerPlugin registers live diagnostic metrics with ZPagesRegistry and Otel."""
    zpages = ZPagesRegistry()
    otel_recorder = DbPoolRecorder()
    cfg = BrokerConfig(pool_size=5, max_overflow=2)
    plugin = BrokerPlugin(config=cfg, zpages=zpages, otel_recorder=otel_recorder, use_shm=False)

    lease = plugin.transport.acquire(worker_id=1, bind_key="test")
    sample = plugin.snapshot_metrics()
    assert sample.active_leases == 1
    assert sample.max_capacity == 7
    assert len(otel_recorder.samples) == 1

    code, payload = zpages.handle_zpage("/debug/zpages/broker")
    assert code == 200

    data = decode_json(payload)
    assert data["status"] == "ok"
    assert data["brokers"]["default"]["active_leases"] == 1
    assert data["brokers"]["default"]["pool_size"] == 5

    plugin.transport.release(lease.lease_id)
    code, payload = zpages.handle_zpage("/debug/zpages/broker")
    data = decode_json(payload)
    assert data["brokers"]["default"]["active_leases"] == 0


@pytest.mark.parametrize(
    "dialect",
    [
        "postgresql",
        "mysql",
        "sqlite",
        "oracle",
        "mssql",
        "duckdb",
        "bigquery",
        "spanner",
        "spangres",
    ],
)
def test_broker_dialect_tags_cover_all_nine_sql_dialects(dialect: str) -> None:
    """Verify SyncSHMBrokerTransport._DIALECT_TAGS maps all 9 SQL dialects and aliases to distinct tags 1..9."""
    tags = SyncSHMBrokerTransport._DIALECT_TAGS
    assert tags["postgresql"] == 1
    assert tags["postgres"] == 1
    assert tags["mysql"] == 2
    assert tags["mariadb"] == 2
    assert tags["sqlite"] == 3
    assert tags["oracle"] == 4
    assert tags["mssql"] == 5
    assert tags["sqlserver"] == 5
    assert tags["duckdb"] == 6
    assert tags["bigquery"] == 7
    assert tags["spanner"] == 8
    assert tags["spangres"] == 9

    transport = SyncSHMBrokerTransport(BrokerConfig())
    sql = "SELECT * FROM events WHERE tenant_id = :tenant_id"
    compiled_count = 0

    def _compile_for(target_dialect: str):
        def _inner(_raw: str) -> tuple[str, tuple[str, ...]]:
            nonlocal compiled_count
            compiled_count += 1
            return (f"compiled_{target_dialect}", ("tenant_id",))

        return _inner

    plan = transport.get_or_compile_statement(dialect, sql, _compile_for(dialect))
    assert plan.compiled_sql == f"compiled_{dialect}"
    assert plan.dialect == dialect
    assert compiled_count == 1

    cached_plan = transport.get_or_compile_statement(dialect, sql, _compile_for("unexpected"))
    assert cached_plan.compiled_sql == f"compiled_{dialect}"
    assert compiled_count == 1
    transport.close()


def test_async_shm_session_broker_int_transport_and_sync_checkout_checkin() -> None:
    """Verify AsyncSHMSessionBroker(int) creates AsyncSHMBrokerTransport and sync checkout/checkin works without unawaited coroutines."""
    broker = AsyncSHMSessionBroker(2)
    assert isinstance(broker.transport, AsyncSHMBrokerTransport)
    assert broker.capacity == 2

    in_mem_async = AsyncInMemoryBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    mem_broker = AsyncSHMSessionBroker(transport=in_mem_async, capacity=1)

    class FakeConn:
        pass

    conn = FakeConn()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        mem_broker.on_checkout(conn)
        assert in_mem_async.metrics().active_leases == 1
        mem_broker.on_checkin(conn)
        assert in_mem_async.metrics().active_leases == 0
        assert in_mem_async.metrics().total_released == 1

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []
    broker.transport._close_sync() if hasattr(broker.transport, "_close_sync") else None


def test_sync_shm_session_broker_multithreaded_contention() -> None:
    """Verify SyncSHMSessionBroker wakes contended threads cleanly via threading.Condition."""
    broker = SyncSHMSessionBroker(capacity=1)
    assert broker.acquire(timeout=1.0) is True
    assert broker.in_use == 1

    ready = threading.Event()
    acquired_in_worker: list[bool] = []

    def _worker() -> None:
        ready.set()
        ok = broker.acquire(timeout=2.0)
        acquired_in_worker.append(ok)
        if ok:
            broker.release()

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()
    assert ready.wait(timeout=1.0) is True
    time.sleep(0.02)
    broker.release()
    worker.join(timeout=2.0)

    assert acquired_in_worker == [True]
    assert broker.in_use == 0
