"""Tests for SyncSHMBrokerTransport, AsyncSHMBrokerTransport, SyncSHMSessionBroker, and DBRPC wire frames."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import os
import subprocess
import sys
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from vertebra._vertebra import (
    BrokerEngineRegistry,
    DatabaseError,
    DataError,
    InterfaceError,
    StatementCache,
    stable_hash64,
)
from vertebra.extensions.alchemy.broker import AsyncSHMSessionBroker, SyncSHMSessionBroker
from vertebra.extensions.broker import (
    DBRPC_HEADER_SIZE,
    DBRPC_VERSION,
    AsyncBrokerStream,
    AsyncConnectionLease,
    AsyncSHMBrokerTransport,
    BrokerAttachError,
    BrokerConfig,
    BrokerEngineDiscovery,
    BrokerMetrics,
    BrokerPlugin,
    DbRpcHeader,
    DbRpcOp,
    DbRpcRequest,
    DbRpcResponse,
    LeaseQuarantinedError,
    PoolMetrics,
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
    assert isinstance(plugin.transport, SyncSHMBrokerTransport)

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

    def _compile_for(target_dialect: str) -> Callable[[str], tuple[str, tuple[str, ...]]]:
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


@pytest.mark.anyio
async def test_async_shm_session_broker_int_transport_and_sync_checkout_checkin() -> None:
    """Verify AsyncSHMSessionBroker async lifecycle, InterfaceError on sync hooks, and SyncSHMSessionBroker checkout/checkin."""
    broker = AsyncSHMSessionBroker(2)
    assert isinstance(broker.transport, AsyncSHMBrokerTransport)
    assert broker.capacity == 2

    assert await broker.acquire() is True
    assert broker.in_use == 1
    assert broker.transport.metrics().active_leases == 1
    await broker.release()
    assert broker.in_use == 0
    assert broker.transport.metrics().active_leases == 0

    class FakeAsyncSession:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    created_sessions: list[FakeAsyncSession] = []

    def make_async_session() -> FakeAsyncSession:
        sess = FakeAsyncSession()
        created_sessions.append(sess)
        return sess

    async with broker.provide_session(make_async_session) as sess:
        assert sess is created_sessions[-1]
        assert broker.in_use == 1
        assert broker.transport.metrics().active_leases == 1
    assert created_sessions[-1].closed is True
    assert broker.in_use == 0
    assert broker.transport.metrics().active_leases == 0

    shm_async = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    async_session_broker = AsyncSHMSessionBroker(transport=shm_async, capacity=1)

    class FakeConn:
        pass

    conn = FakeConn()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        with pytest.raises(InterfaceError):
            async_session_broker.on_checkout(conn)
        with pytest.raises(InterfaceError):
            async_session_broker.on_checkin(conn)
        assert shm_async.metrics().active_leases == 0

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []

    shm_sync = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    sync_session_broker = SyncSHMSessionBroker(transport=shm_sync, capacity=1)
    sync_session_broker.on_checkout(conn)
    assert shm_sync.metrics().active_leases == 1
    assert sync_session_broker.in_use == 1
    sync_session_broker.on_checkin(conn)
    assert shm_sync.metrics().active_leases == 0
    assert shm_sync.metrics().total_released == 1
    assert sync_session_broker.in_use == 0


def test_sync_shm_session_broker_multithreaded_contention() -> None:
    """Verify SyncSHMSessionBroker wakes contended threads cleanly via shared-memory transport."""
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
        "from vertebra._vertebra import BrokerEngineRegistry, StatementCache, stable_hash64\n"
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


@pytest.mark.parametrize("secret", [b"top-secret-bytes-999", "top-secret-str-999"])
def test_broker_config_repr_redacts_secrets(secret: bytes | str) -> None:
    """Verify BrokerConfig.__repr__ omits attach_secret plaintext while preserving socket_path."""
    cfg = BrokerConfig(socket_path="/tmp/test-vtb/broker.sock", attach_secret=secret)
    rendered = repr(cfg)
    assert "/tmp/test-vtb/broker.sock" in rendered
    assert "[REDACTED]" in rendered
    secret_text = secret.decode("utf-8") if isinstance(secret, bytes) else secret
    assert secret_text not in rendered


def test_session_pinning_transaction_lifecycle_and_two_partition_pool_metrics() -> None:
    """Verify session/tx affinity pinning and two-partition PoolMetrics summing to BrokerMetrics."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=0, acquire_timeout_ms=50))
    part_a = transport.register_partition("pool_a", pool_size=2, max_overflow=0)
    part_b = transport.register_partition("pool_b", pool_size=2, max_overflow=0)

    lease1 = transport.pin_session("sess-1", bind_key="pool_a", partition=part_a)
    lease2 = transport.pin_session("sess-1", bind_key="pool_a", partition=part_a)
    assert lease2 is lease1
    assert lease1.pinned_session_id == "sess-1"

    tx_lease = transport.begin_transaction("sess-1")
    assert tx_lease.slot_index == lease1.slot_index
    assert tx_lease.in_transaction is True

    transport.acquire(worker_id=77001, bind_key="pool_b", partition=part_b)
    mid_metrics = transport.metrics()
    assert isinstance(mid_metrics, BrokerMetrics)
    assert set(mid_metrics.pools) == {"pool_a", "pool_b"}
    assert all(isinstance(p, PoolMetrics) for p in mid_metrics.pools.values())
    assert sum(p.active_leases for p in mid_metrics.pools.values()) == mid_metrics.active_leases == 2
    assert sum(p.total_acquired for p in mid_metrics.pools.values()) == mid_metrics.total_acquired == 2

    reclaimed = transport.reclaim_worker_leases(77001)
    assert len(reclaimed) == 1
    q_metrics = transport.metrics()
    assert sum(p.quarantined for p in q_metrics.pools.values()) == q_metrics.quarantined == 1
    assert sum(p.total_quarantined for p in q_metrics.pools.values()) == q_metrics.total_quarantined == 1

    transport.dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
    assert transport.commit_transaction("sess-1") is True

    end_metrics = transport.metrics()
    assert sum(p.active_leases for p in end_metrics.pools.values()) == end_metrics.active_leases == 0
    assert sum(p.quarantined for p in end_metrics.pools.values()) == end_metrics.quarantined == 0
    assert sum(p.total_released for p in end_metrics.pools.values()) == end_metrics.total_released == 1
    assert sum(p.total_disposed for p in end_metrics.pools.values()) == end_metrics.total_disposed == 1


def test_stream_dbrpc_vtbarrow_and_cancel_quarantine() -> None:
    """Verify stream_dbrpc delivers DuckDB VTBARROW results and quarantines leases on cancel."""
    BrokerEngineDiscovery.register_installed()

    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    pool_id = BrokerEngineRegistry.open_pool(6, ":memory:", 2)
    try:
        lease_arrow = transport.acquire(worker_id=78001)
        arrow_stream = transport.stream_dbrpc(
            6,
            pool_id,
            "SELECT i, 'row-' || i AS label FROM range(0, 150) AS t(i)",
            lease=lease_arrow,
            chunk_size=128,
        )
        chunks = list(arrow_stream)
        assert len(chunks) == 1
        assert chunks[0].arrow_shm is not None and chunks[0].arrow_shm.startswith("/vtb-dd-")
        assert chunks[0].rowcount == 150
        lease_arrow.release()

        lease_cancel = transport.acquire(worker_id=78002)
        cancel_called = []
        stream = transport.stream_dbrpc(
            6,
            pool_id,
            chunk_source=[
                DbRpcResponse(request_id=1, status=0, rows=((1,),)),
                DbRpcResponse(request_id=1, status=0, rows=((2,),)),
            ],
            lease=lease_cancel,
            on_cancel=lambda: cancel_called.append(True),
        )
        first = next(stream)
        assert first.rows == ((1,),)
        stream.cancel()
        assert cancel_called == [True]
        assert lease_cancel.quarantined is True
        assert transport.quarantined_count() == 1

        with pytest.raises(LeaseQuarantinedError):
            next(stream)
        with pytest.raises(LeaseQuarantinedError):
            transport.release(lease_cancel.lease_id)

        transport.dispose_quarantined(lease_cancel)
        assert transport.quarantined_count() == 0
    finally:
        BrokerEngineRegistry.close_pool(6, pool_id)


def test_execute_dbrpc_and_rollback_raise_and_quarantine_on_nonzero_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify non-zero DBRPC statuses raise BrokerAttachError before altering transaction state."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    lease = transport.acquire(worker_id=79001)
    transport.begin_transaction(lease)

    fail_raw = encode_dbrpc_response(
        DbRpcResponse(
            request_id=1,
            status=1,
            error_class="DatabaseError",
            error_message="simulated rollback failure",
        )
    )
    monkeypatch.setattr(broker_transport_mod, "BrokerEngineRegistry", SimpleNamespace(execute=lambda *_args: fail_raw))

    with pytest.raises(BrokerAttachError, match="simulated rollback failure"):
        transport.execute_dbrpc(6, 1, DbRpcOp.EXECUTE, sql="SELECT 1", lease=lease)

    with pytest.raises(BrokerAttachError, match="simulated rollback failure"):
        transport.commit_transaction(lease, dialect_tag=6, engine_pool_id=1)
    assert lease.in_transaction is True
    assert lease.is_released is False

    with pytest.raises(BrokerAttachError, match="simulated rollback failure"):
        transport.rollback_transaction(lease, dialect_tag=6, engine_pool_id=1)
    assert lease.quarantined is True
    assert transport.quarantined_count() == 1
    transport.dispose_quarantined(lease)
    assert transport.quarantined_count() == 0


@pytest.mark.parametrize("exc_cls", [DatabaseError, RuntimeError])
def test_sync_lease_release_preserves_state_on_failure_and_retries(
    monkeypatch: pytest.MonkeyPatch,
    exc_cls: type[Exception],
) -> None:
    """Verify SyncConnectionLease.release keeps _released False on failure until retry succeeds."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    lease = transport.acquire(worker_id=81001)
    original_release_impl = transport._release_impl
    attempts = 0

    def _flaky_release(lease_id: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise exc_cls("simulated sync release failure")
        return original_release_impl(lease_id)

    monkeypatch.setattr(transport, "_release_impl", _flaky_release)

    with pytest.raises(exc_cls, match="simulated sync release failure"):
        lease.release()
    assert lease.is_released is False
    assert transport.metrics().active_leases == 1

    lease.release()
    assert lease.is_released is True
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_released == 1


@pytest.mark.anyio
@pytest.mark.parametrize("exc_cls", [DatabaseError, RuntimeError])
async def test_async_lease_release_preserves_state_on_failure_and_retries(
    monkeypatch: pytest.MonkeyPatch,
    exc_cls: type[Exception],
) -> None:
    """Verify AsyncConnectionLease.release keeps _released False on failure until retry succeeds."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    lease = await transport.acquire(worker_id=81002)
    original_release_impl = transport._release_impl
    attempts = 0

    def _flaky_release(lease_id: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise exc_cls("simulated async release failure")
        return original_release_impl(lease_id)

    monkeypatch.setattr(transport, "_release_impl", _flaky_release)

    with pytest.raises(exc_cls, match="simulated async release failure"):
        await lease.release()
    assert lease.is_released is False
    assert transport.metrics().active_leases == 1

    await lease.release()
    assert lease.is_released is True
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_released == 1


@pytest.mark.anyio
@pytest.mark.parametrize("exc_cls", [DatabaseError, RuntimeError])
async def test_async_lease_release_sync_preserves_state_on_failure_and_retries(
    monkeypatch: pytest.MonkeyPatch,
    exc_cls: type[Exception],
) -> None:
    """Verify AsyncConnectionLease.release_sync keeps _released False on failure until retry succeeds."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    lease = await transport.acquire(worker_id=81003)
    original_release_impl = transport._release_impl
    attempts = 0

    def _flaky_release(lease_id: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise exc_cls("simulated release_sync failure")
        return original_release_impl(lease_id)

    monkeypatch.setattr(transport, "_release_impl", _flaky_release)

    with pytest.raises(exc_cls, match="simulated release_sync failure"):
        lease.release_sync()
    assert lease.is_released is False
    assert transport.metrics().active_leases == 1

    lease.release_sync()
    assert lease.is_released is True
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_released == 1


@pytest.mark.anyio
async def test_async_lease_release_sync_uses_native_path_and_rejects_async_only_transport() -> None:
    """Verify release_sync runs synchronously on SHM transport and raises InterfaceError on async-only stubs."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    lease = await transport.acquire(worker_id=81004)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        lease.release_sync()

    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
    assert lease.is_released is True
    assert transport.metrics().active_leases == 0

    release_calls = 0

    class _AsyncOnlyTransport:
        async def release(self, _lease_id: str) -> bool:
            nonlocal release_calls
            release_calls += 1
            return True

    async_only_lease = AsyncConnectionLease(
        lease_id="0:1",
        worker_id=81005,
        acquired_at_ns=1,
        ttl_ns=1_000_000,
        bind_key="default",
        is_overflow=False,
        slot_index=0,
        generation=1,
        transport=_AsyncOnlyTransport(),
    )

    with warnings.catch_warnings(record=True) as stub_caught:
        warnings.simplefilter("always", RuntimeWarning)
        with pytest.raises(InterfaceError, match="synchronous"):
            async_only_lease.release_sync()

    assert release_calls == 0
    assert async_only_lease.is_released is False
    assert [w for w in stub_caught if issubclass(w.category, RuntimeWarning)] == []


def test_broker_config_millisecond_timeouts_and_rejected_aliases() -> None:
    """Verify BrokerConfig enforces millisecond timeout fields and rejects removed second-unit kwargs."""
    default_cfg = BrokerConfig()
    assert default_cfg.acquire_timeout_ms == 5_000
    assert default_cfg.lease_ttl_ms == 30_000

    custom_cfg = BrokerConfig(acquire_timeout_ms=250, lease_ttl_ms=1500)
    assert custom_cfg.acquire_timeout_ms == 250
    assert custom_cfg.lease_ttl_ms == 1500

    transport = SyncSHMBrokerTransport(custom_cfg)
    assert transport.config.acquire_timeout_ms == 250
    assert transport.config.lease_ttl_ms == 1500


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"acquire_timeout": 0.5},
        {"lease_ttl": 10.0},
        {"acquire_timeout": 0.5, "lease_ttl": 10.0},
    ],
)
def test_broker_config_rejects_removed_float_timeout_kwargs(bad_kwargs: dict[str, float]) -> None:
    """Verify removed acquire_timeout and lease_ttl fields raise TypeError on BrokerConfig."""
    broker_cls: Any = BrokerConfig
    with pytest.raises(TypeError):
        broker_cls(**bad_kwargs)


@pytest.mark.anyio
async def test_async_broker_stream_multi_frame_order_and_idempotent_aclose() -> None:
    """Verify AsyncSHMBrokerTransport.stream_dbrpc returns AsyncBrokerStream with ordered iteration and idempotent aclose."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=100))
    lease = await transport.acquire(worker_id=82001)
    try:
        frame_1 = encode_dbrpc_response(DbRpcResponse(request_id=10, status=0, rowcount=1, rows=((1, "a"),)))
        frame_2 = encode_dbrpc_response(DbRpcResponse(request_id=10, status=0, rowcount=1, rows=((2, "b"),)))
        frame_3 = DbRpcResponse(request_id=10, status=0, rowcount=1, rows=((3, "c"),))

        stream = transport.stream_dbrpc(
            6,
            None,
            chunk_source=[frame_1, frame_2, frame_3],
            lease=lease,
        )
        assert isinstance(stream, AsyncBrokerStream)
        received = [chunk async for chunk in stream]
        assert [item.rows for item in received] == [((1, "a"),), ((2, "b"),), ((3, "c"),)]

        await stream.aclose()
        await stream.aclose()
        assert lease.quarantined is False

        closed_flags: list[bool] = []

        def _gen_frames() -> Any:
            try:
                yield frame_1
                yield frame_2
                yield frame_3
            finally:
                closed_flags.append(True)

        early_stream = transport.stream_dbrpc(6, None, chunk_source=_gen_frames, lease=lease)
        async with contextlib.aclosing(early_stream):
            first = await anext(early_stream)
            assert first.rows == ((1, "a"),)
        assert closed_flags == [True]
        assert lease.quarantined is False
        with pytest.raises(StopAsyncIteration):
            await anext(early_stream)

        BrokerEngineDiscovery.register_installed()
        pool_id = BrokerEngineRegistry.open_pool(6, ":memory:", 2)
        try:
            arrow_stream = transport.stream_dbrpc(
                6,
                pool_id,
                "SELECT i, 'row-' || i AS label FROM range(0, 150) AS t(i)",
                lease=lease,
                chunk_size=128,
            )
            arrow_chunks = [chunk async for chunk in arrow_stream]
            assert len(arrow_chunks) == 1
            assert arrow_chunks[0].arrow_shm is not None and arrow_chunks[0].arrow_shm.startswith("/vtb-dd-")
            assert arrow_chunks[0].rowcount == 150

            cancel_lease = await transport.acquire(worker_id=82002)
            cancel_called: list[bool] = []
            cancel_stream = transport.stream_dbrpc(
                6,
                pool_id,
                chunk_source=[frame_1, frame_2],
                lease=cancel_lease,
                on_cancel=lambda: cancel_called.append(True),
            )
            first_item = await anext(cancel_stream)
            assert first_item.rows == ((1, "a"),)
            await cancel_stream.cancel()
            assert cancel_called == [True]
            assert cancel_lease.quarantined is True
            with pytest.raises(LeaseQuarantinedError):
                await anext(cancel_stream)
            await transport.dispose_quarantined(cancel_lease)
        finally:
            BrokerEngineRegistry.close_pool(6, pool_id)
    finally:
        await lease.release()
        await transport.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "corrupt_frame",
    [
        b"",
        b"\x99\x01",
        b"corrupt-dbrpc-frame",
        b"\x00",
    ],
)
async def test_broker_streams_raise_data_error_on_malformed_dbrpc_frames(corrupt_frame: bytes) -> None:
    """Verify both BrokerStream and AsyncBrokerStream fail closed with DataError on malformed DBRPC frames."""
    sync_transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    sync_stream = sync_transport.stream_dbrpc(6, None, chunk_source=[corrupt_frame])
    try:
        with pytest.raises(DataError):
            next(sync_stream)
    finally:
        sync_stream.close()
        sync_transport.close()

    async_transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    async_stream = async_transport.stream_dbrpc(6, None, chunk_source=[corrupt_frame])
    try:
        with pytest.raises(DataError):
            await anext(async_stream)
    finally:
        await async_stream.aclose()
        await async_transport.close()


@pytest.mark.parametrize("method_name", ["acquire_sync", "release_sync", "quarantine_sync", "close_sync"])
def test_async_shm_broker_transport_removes_sync_methods(method_name: str) -> None:
    """Verify AsyncSHMBrokerTransport exposes no public *_sync methods."""
    assert hasattr(AsyncSHMBrokerTransport, method_name) is False
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    assert hasattr(transport, method_name) is False


@pytest.mark.anyio
async def test_async_shm_broker_transport_offloads_shm_ops_and_pins_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pinned session APIs and SyncPool offloading do not block an active event-loop heartbeat task."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=100))
    try:
        pinned = await transport.pin_session("sess-async", bind_key="sess-async")
        assert await transport.get_pinned_session("sess-async") is pinned
        unpinned = await transport.unpin_session("sess-async")
        assert unpinned is pinned
        assert await transport.get_pinned_session("sess-async") is None
        assert await transport.unpin_session("sess-async") is None

        entered = threading.Event()
        release_gate = threading.Event()
        original_publish = transport._publish_impl

        def _gated_publish(channel: str, payload: bytes, lease_id: str | None = None) -> Any:
            entered.set()
            assert release_gate.wait(timeout=2.0)
            return original_publish(channel, payload, lease_id)

        monkeypatch.setattr(transport, "_publish_impl", _gated_publish)

        heartbeat_ticks = 0

        async def _heartbeat() -> None:
            nonlocal heartbeat_ticks
            while not release_gate.is_set():
                heartbeat_ticks += 1
                if entered.is_set() and heartbeat_ticks >= 3:
                    release_gate.set()
                await asyncio.sleep(0)

        hb_task = asyncio.create_task(_heartbeat())
        msg = await transport.publish("events", b"payload", lease_id=pinned.lease_id)
        await hb_task
        assert msg.payload == b"payload"
        assert heartbeat_ticks >= 3

        drained = await transport.drain_channel("events")
        assert len(drained) == 1
        assert await transport.drain_all_channels() == []

        assert await transport.release(pinned.lease_id) is True

        q_lease = await transport.acquire(worker_id=83001)
        await transport.quarantine(q_lease)
        assert transport.quarantined_count() == 1
        await transport.dispose_quarantined(q_lease)
        assert transport.quarantined_count() == 0

        r_lease = await transport.acquire(worker_id=83002)
        reclaimed = await transport.reclaim_worker_leases(83002)
        assert len(reclaimed) == 1
        assert r_lease.quarantined is True
        await transport.dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
    finally:
        await transport.close()
