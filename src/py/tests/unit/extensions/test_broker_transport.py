"""Tests for SyncSHMBrokerTransport, AsyncSHMBrokerTransport, SyncSHMSessionBroker, and DBRPC wire frames."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

from vertebra._vertebra import StatementCache, stable_hash64
from vertebra.extensions.alchemy.broker import AsyncSHMSessionBroker, SyncSHMSessionBroker
from vertebra.extensions.broker import (
    DBRPC_HEADER_SIZE,
    DBRPC_VERSION,
    AsyncSHMBrokerTransport,
    BrokerAttachError,
    BrokerConfig,
    BrokerPlugin,
    DbRpcHeader,
    DbRpcOp,
    DbRpcRequest,
    DbRpcResponse,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker import transport as broker_transport_mod
from vertebra.extensions.broker.dbrpc import (
    decode_dbrpc_request,
    decode_dbrpc_response,
    encode_dbrpc_request,
    encode_dbrpc_response,
)
from vertebra.server.platform import ZPagesRegistry
from vertebra.telemetry.otel import DbPoolRecorder
from vertebra.utils.serializers import decode_json


def test_shm_broker_transport_sync_lifecycle() -> None:
    """Verify synchronous SHM transport acquire, release, and statement caching."""
    cfg = BrokerConfig(pool_size=2, max_overflow=1)
    transport = SyncSHMBrokerTransport(cfg)

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
    assert plan.sql_hash == stable_hash64(b"\x01SELECT 1")

    reclaimed = transport.reclaim_worker(102)
    assert reclaimed == 1
    assert transport.release(l3.lease_id) is True
    assert transport.release(l4.lease_id) is True


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
    """Verify BrokerPlugin registers live diagnostic metrics and partitions with ZPagesRegistry and Otel."""
    zpages = ZPagesRegistry()
    otel_recorder = DbPoolRecorder()
    cfg = BrokerConfig(pool_size=5, max_overflow=2)
    plugin = BrokerPlugin(config=cfg, zpages=zpages, otel_recorder=otel_recorder)
    plugin.on_app_init(SimpleNamespace(state={}, dependencies={}))

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

    shm_async = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    session_broker = AsyncSHMSessionBroker(transport=shm_async, capacity=1)

    class FakeConn:
        pass

    conn = FakeConn()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        session_broker.on_checkout(conn)
        assert shm_async.metrics().active_leases == 1
        session_broker.on_checkin(conn)
        assert shm_async.metrics().active_leases == 0
        assert shm_async.metrics().total_released == 1

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


def test_stable_hash64_cross_process_pythonhashseed_and_statement_cache() -> None:
    """Verify stable_hash64 produces identical unsigned 64-bit digests across distinct PYTHONHASHSEED processes."""
    cache_name = f"/vtb-hash-seed-{os.getpid()}-{int(time.time() * 1000)}"
    sc_owner, created = StatementCache.open_or_create_named(cache_name, capacity=16)
    assert created is True
    assert sc_owner.insert(1, b"SELECT 42", 0, b"SELECT 42", b"") is True

    script = (
        "import json\n"
        "from vertebra._vertebra import StatementCache, stable_hash64\n"
        f"sc, created = StatementCache.open_or_create_named({cache_name!r}, capacity=16)\n"
        "plan = sc.lookup(1, b'SELECT 42')\n"
        "out = {\n"
        "  'empty_b': stable_hash64(b''),\n"
        "  'empty_s': stable_hash64(''),\n"
        "  'key': stable_hash64('tenant:primary:writer'),\n"
        "  'sql': plan.sql_hash if plan is not None else -1,\n"
        "  'created': created,\n"
        "}\n"
        "print(json.dumps(out))\n"
    )
    try:
        outputs = []
        for seed in ("1", "999"):
            env = {**os.environ, "PYTHONHASHSEED": seed}
            res = subprocess.run(
                [sys.executable, "-c", script],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
            outputs.append(decode_json(res.stdout))

        d1, d2 = outputs
        assert d1["empty_b"] == d1["empty_s"]
        assert d1["empty_b"] == d2["empty_b"]
        assert d1["key"] == d2["key"]
        assert d1["key"] >= 0
        assert d1["key"] < (1 << 64)
        assert d1["sql"] == stable_hash64(b"\x01SELECT 42")
        assert d1["sql"] == d2["sql"]
        assert d1["created"] is False
        assert d2["created"] is False
    finally:
        sc_owner.unlink()


@pytest.mark.parametrize("op", list(DbRpcOp))
def test_dbrpc_roundtrip_all_ops(op: DbRpcOp) -> None:
    """Verify DbRpcHeader, DbRpcRequest, and DbRpcResponse round-trip cleanly across all ops."""
    req = DbRpcRequest(
        version=DBRPC_VERSION,
        op=int(op),
        request_id=100 + int(op),
        lease_slot=2,
        lease_generation=7,
        sql="SELECT 1" if op is DbRpcOp.EXECUTE else None,
        params=(1, "alpha") if op is DbRpcOp.EXECUTE else None,
        savepoint="sp1" if op is DbRpcOp.SAVEPOINT else None,
        chunk_rows=64 if op is DbRpcOp.FETCH_CHUNK else 0,
    )
    raw = encode_dbrpc_request(req)
    assert len(raw) > DBRPC_HEADER_SIZE
    hdr, decoded = decode_dbrpc_request(raw)
    assert hdr == DbRpcHeader(
        version=DBRPC_VERSION,
        op=int(op),
        request_id=req.request_id,
        lease_slot=2,
        lease_generation=7,
    )
    assert decoded == req

    resp = DbRpcResponse(
        request_id=req.request_id,
        status=0,
        rowcount=1,
        columns=("id", "name"),
        rows=((1, "alpha"),),
    )
    assert decode_dbrpc_response(encode_dbrpc_response(resp)) == resp


@pytest.mark.parametrize(
    "bad_header",
    [
        b"\x01\x00\x01",
        b"\x02\x00\x01" + b"\x00" * 20,
        b"\x01\x00\xff" + b"\x00" * 20,
    ],
)
def test_dbrpc_rejects_malformed_headers(bad_header: bytes) -> None:
    """Verify DbRpcHeader.decode rejects truncated buffers, version mismatches, and invalid ops."""
    with pytest.raises(ValueError):
        DbRpcHeader.decode(bad_header)


@pytest.mark.parametrize("bind_key", [None, "writer", "default"])
@pytest.mark.anyio
async def test_deterministic_bind_keys_partitions_and_clean_break(bind_key: str | None) -> None:
    """Verify bind_key=None uses 0, partition exhaustion raises BrokerAttachError, and no salted hash() remains."""
    expected_hash = 0 if bind_key is None else stable_hash64(bind_key)
    sync_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    lease = sync_t.acquire(bind_key=bind_key)
    active = sync_t._native.active_leases_for_worker(os.getpid())
    assert len(active) == 1
    assert sync_t.release(lease.lease_id) is True

    raw_grant = sync_t._native.acquire(os.getpid(), ttl_ns=0, timeout_ms=100, bind_key_hash=expected_hash)
    assert raw_grant is not None
    assert raw_grant.bind_key_hash == expected_hash
    assert sync_t._native.release(raw_grant.slot_index, raw_grant.generation) is True

    async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    async_lease = await async_t.acquire(bind_key=bind_key)
    assert async_t._bind_key_hash(bind_key) == expected_hash
    await async_lease.release()

    part_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    for idx in range(64):
        part_t.register_partition(f"pool-{idx}", pool_size=1, max_overflow=0)
    with pytest.raises(BrokerAttachError):
        part_t.register_partition("pool-64", pool_size=1, max_overflow=0)

    source_path = Path(broker_transport_mod.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    hash_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "hash"
    ]
    assert hash_calls == []

    with pytest.raises(ImportError):
        exec("from vertebra.extensions.broker import SyncInMemoryBrokerTransport", {})
