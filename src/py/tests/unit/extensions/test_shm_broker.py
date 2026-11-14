"""Unit and cross-process tests for co-located ShmStatementCache, lease reclamation, and SHM channel rings."""

from __future__ import annotations

import multiprocessing as mp
import os
import struct
import sys
import time
import types
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import ArrowCursor, BrokerEngineRegistry
from vertebra.extensions.broker import (
    DBRPC_VERSION,
    BrokerConfig,
    BrokerEngineDiscovery,
    DbRpcOp,
    DbRpcRequest,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.dbrpc import (
    decode_dbrpc_response,
    encode_dbrpc_request,
)


def _child_publish_and_compile(shm_name: str, pool_size: int, max_overflow: int, stmt_cap: int) -> None:
    """Attach to a named SyncSHMBrokerTransport in a child process, compile a statement, and publish a channel message.

    Args:
        shm_name: Name of the POSIX shared-memory segment.
        pool_size: Primary pool capacity.
        max_overflow: Overflow slot capacity.
        stmt_cap: Statement cache slot capacity.
    """
    cfg = BrokerConfig(
        pool_size=pool_size,
        max_overflow=max_overflow,
        statement_cache_capacity=stmt_cap,
        shm_name=shm_name,
    )
    transport = SyncSHMBrokerTransport(cfg)
    plan = transport.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        lambda _sql: ("SHOULD_NOT_COMPILE", ()),
    )
    assert plan.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan.param_names == ("id",)

    transport.publish("invalidation", b"evict:users:42", lease_id="0:1")


def test_shm_broker_colocated_statement_cache_single_region_and_fd() -> None:
    """Verify SyncSHMBrokerTransport co-locates statement cache in a single ShmRegion and shares across FD attachment."""
    name = f"/vtb-colocated-{os.getpid()}-{int(time.time() * 1000)}"
    cfg = BrokerConfig(
        pool_size=4,
        max_overflow=2,
        statement_cache_capacity=32,
        shm_name=name,
    )
    primary = SyncSHMBrokerTransport(cfg)
    assert primary.stmt_capacity == 32
    assert primary.stmt_offset > 0
    assert primary.shm_size > primary.stmt_offset

    separate_stmt_path = Path(f"/dev/shm/{name.lstrip('/')}-stmt")
    assert not separate_stmt_path.exists()

    compile_count = 0

    def compile_sql(sql: str) -> tuple[str, tuple[str, ...]]:
        nonlocal compile_count
        compile_count += 1
        return (sql.replace(":id", "$1"), ("id",))

    plan1 = primary.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan1.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan1.param_names == ("id",)
    assert compile_count == 1

    assert primary.shm_fd is not None
    fd_attached = SyncSHMBrokerTransport.from_fd(primary.shm_fd, primary.shm_size, cfg)
    plan2 = fd_attached.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan2.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan2.param_names == ("id",)
    assert compile_count == 1
    assert fd_attached.metrics().statement_cache_hits >= 1

    fd_attached.invalidate_statements()
    plan3 = primary.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan3.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert compile_count == 2


def test_shm_broker_active_leases_for_worker_and_reclaim() -> None:
    """Verify active_leases_for_worker and reclaim_worker on SyncSHMBrokerTransport."""
    cfg = BrokerConfig(pool_size=3, max_overflow=1, statement_cache_capacity=16)
    shm_transport = SyncSHMBrokerTransport(cfg)

    l1 = shm_transport.acquire(worker_id=4100, bind_key="writer")
    l2 = shm_transport.acquire(worker_id=4100, bind_key="reader")
    l3 = shm_transport.acquire(worker_id=4200, bind_key="writer")

    worker_4100_leases = shm_transport.active_leases_for_worker(4100)
    assert len(worker_4100_leases) == 2
    slots_4100 = {entry[1] for entry in worker_4100_leases}
    assert slots_4100 == {l1.slot_index, l2.slot_index}

    worker_4200_leases = shm_transport.active_leases_for_worker(4200)
    assert len(worker_4200_leases) == 1
    assert worker_4200_leases[0][1] == l3.slot_index

    reclaimed = shm_transport.reclaim_worker(4100)
    assert reclaimed == 2
    assert shm_transport.active_leases_for_worker(4100) == []
    assert shm_transport.metrics().active_leases == 1
    assert shm_transport.metrics().total_reclaimed == 2

    assert shm_transport.release(l3.lease_id) is True
    assert shm_transport.metrics().active_leases == 0


def test_shm_broker_cross_process_publish_and_drain_channel() -> None:
    """Verify publish and drain_channel communicate across processes via the shared-memory channel ring."""
    name = f"/vtb-chan-{os.getpid()}-{int(time.time() * 1000)}"
    cfg = BrokerConfig(
        pool_size=4,
        max_overflow=2,
        statement_cache_capacity=32,
        shm_name=name,
    )
    parent = SyncSHMBrokerTransport(cfg)
    parent.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        lambda sql: (sql.replace(":id", "$1"), ("id",)),
    )

    ctx = mp.get_context("spawn")
    proc = ctx.Process(
        target=_child_publish_and_compile,
        args=(name, cfg.pool_size, cfg.max_overflow, cfg.statement_cache_capacity),
    )
    proc.start()
    proc.join(timeout=15)
    assert proc.exitcode == 0

    messages = parent.drain_channel("invalidation")
    assert len(messages) == 1
    assert messages[0].channel == "invalidation"
    assert messages[0].payload == b"evict:users:42"
    assert messages[0].lease_id == "0:1"
    assert messages[0].timestamp_ns > 0
    assert parent.drain_channel("invalidation") == []


def test_shm_broker_channel_isolation_and_ring_wraparound() -> None:
    """Verify multi-channel isolation and bounded ring wraparound on SyncSHMBrokerTransport."""
    cfg = BrokerConfig(pool_size=2, max_overflow=1, statement_cache_capacity=16)
    primary = SyncSHMBrokerTransport(cfg)
    assert primary.shm_fd is not None
    secondary = SyncSHMBrokerTransport.from_fd(primary.shm_fd, primary.shm_size, cfg)

    primary.publish("chan.alpha", b"a1", lease_id="0:1")
    secondary.publish("chan.beta", b"b1", lease_id="1:1")
    primary.publish("chan.alpha", b"a2")

    beta_msgs = primary.drain_channel("chan.beta")
    assert [m.payload for m in beta_msgs] == [b"b1"]
    assert beta_msgs[0].lease_id == "1:1"

    alpha_msgs = secondary.drain_channel("chan.alpha")
    assert [m.payload for m in alpha_msgs] == [b"a1", b"a2"]
    assert alpha_msgs[0].lease_id == "0:1"
    assert alpha_msgs[1].lease_id is None

    for idx in range(70):
        primary.publish("chan.wrap", f"msg-{idx}".encode())

    wrapped = secondary.drain_channel("chan.wrap")
    assert len(wrapped) == 64
    assert wrapped[0].payload == b"msg-6"
    assert wrapped[-1].payload == b"msg-69"


def test_broker_engine_registry_rejects_invalid_capsules_and_unknown_tag() -> None:
    """Verify invalid capsules raise TypeError and unregistered tags raise RuntimeError."""
    with pytest.raises(TypeError, match="BrokerEngineCapsule"):
        BrokerEngineRegistry.register(object())

    with pytest.raises(RuntimeError, match="no broker engine registered for dialect_tag 65530"):
        BrokerEngineRegistry.normalize(65530, "SELECT 1")

    with pytest.raises(RuntimeError, match="no broker engine registered for dialect_tag 65530"):
        BrokerEngineRegistry.open_pool(65530, "postgres://localhost/db", 2)


def test_broker_engine_discovery_skips_missing_and_fails_on_broken_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify discovery uses import_optional cleanly, skips missing wheels, and fails loud on broken modules."""
    BrokerEngineDiscovery.register_installed()
    tags = BrokerEngineRegistry.registered_tags()
    assert {1, 2, 4, 5, 6}.issubset(set(tags))

    monkeypatch.setattr(
        "vertebra.extensions.broker.engines.import_optional",
        lambda name: None if name == "vertebra._oracle" else sys.modules.get(name),
    )
    partial_tags = BrokerEngineDiscovery.register_installed()
    assert 4 not in partial_tags
    assert {1, 2, 5, 6}.issubset(set(partial_tags))

    broken_mod = types.ModuleType("vertebra._fake_broken")

    def fake_loader(name: str) -> Any:
        if name == "vertebra._oracle":
            return broken_mod
        return None

    monkeypatch.setattr("vertebra.extensions.broker.engines.import_optional", fake_loader)
    with pytest.raises(AttributeError, match="broker_engine_capsule"):
        BrokerEngineDiscovery.register_installed()


@pytest.mark.parametrize(
    ("module_name", "tag", "raw_sql", "expected_sql", "expected_count", "invalid_dsn"),
    [
        ("vertebra._postgres", 1, "SELECT :id", "SELECT $1", 1, "invalid://localhost/db"),
        ("vertebra._mysql", 2, "SELECT :id", "SELECT ?", 1, "invalid://localhost/db"),
        ("vertebra._oracle", 4, "SELECT :id", "SELECT :1", 1, "invalid://localhost/freepdb1"),
        (
            "vertebra._mssql",
            5,
            "SELECT $1 AND active = ?",
            "SELECT @P1 AND active = @P2",
            2,
            "invalid://localhost/master",
        ),
        ("vertebra._duckdb", 6, "SELECT :id", "SELECT ?", 1, "invalid://localhost/db"),
    ],
)
def test_engine_capsules_register_normalize_and_fail_loud(
    module_name: str,
    tag: int,
    raw_sql: str,
    expected_sql: str,
    expected_count: int,
    invalid_dsn: str,
) -> None:
    """Verify every relational/OLAP engine capsule registers, normalizes SQL, and rejects invalid DSNs."""
    mod = __import__(module_name, fromlist=["broker_engine_capsule"])
    registered_tag = BrokerEngineRegistry.register(mod.broker_engine_capsule())
    assert registered_tag == tag
    assert tag in BrokerEngineRegistry.registered_tags()
    assert BrokerEngineRegistry.normalize(tag, raw_sql) == (expected_sql, expected_count)

    with pytest.raises(RuntimeError):
        BrokerEngineRegistry.open_pool(tag, invalid_dsn, 2)

    unknown_frame = encode_dbrpc_request(
        DbRpcRequest(DBRPC_VERSION, int(DbRpcOp.EXECUTE), 1, 0, 1, "SELECT 1", (), None, 0)
    )
    with pytest.raises(RuntimeError, match="unknown pool id 999999"):
        BrokerEngineRegistry.execute(tag, 999999, unknown_frame)

    BrokerEngineRegistry.close_pool(tag, 999999)


def test_duckdb_broker_capsule_execute_vtbarrow_and_error_handling() -> None:
    """Verify end-to-end DuckDB pool execution, parameterized query, VTBARROW spill, and SQL error mapping."""
    BrokerEngineDiscovery.register_installed()
    pool_id = BrokerEngineRegistry.open_pool(6, "duckdb:///:memory:", 2)
    try:
        ddl_req = encode_dbrpc_request(
            DbRpcRequest(
                DBRPC_VERSION,
                int(DbRpcOp.EXECUTE),
                1,
                0,
                1,
                "CREATE TABLE items (id BIGINT, label VARCHAR)",
                (),
                None,
                0,
            )
        )
        ddl_resp = decode_dbrpc_response(BrokerEngineRegistry.execute(6, pool_id, ddl_req))
        assert ddl_resp.request_id == 1
        assert ddl_resp.status == 0

        ins_req = encode_dbrpc_request(
            DbRpcRequest(
                DBRPC_VERSION,
                int(DbRpcOp.EXECUTE),
                2,
                0,
                1,
                "INSERT INTO items VALUES (?, ?)",
                (42, "alpha"),
                None,
                0,
            )
        )
        ins_resp = decode_dbrpc_response(BrokerEngineRegistry.execute(6, pool_id, ins_req))
        assert ins_resp.status == 0
        assert ins_resp.rowcount == 1

        sel_req = encode_dbrpc_request(
            DbRpcRequest(
                DBRPC_VERSION,
                int(DbRpcOp.EXECUTE),
                3,
                0,
                1,
                "SELECT id, label FROM items WHERE id = ?",
                (42,),
                None,
                0,
            )
        )
        sel_resp = decode_dbrpc_response(BrokerEngineRegistry.execute(6, pool_id, sel_req))
        assert sel_resp.status == 0
        assert sel_resp.columns == ("id", "label")
        assert sel_resp.rows == ((42, "alpha"),)

        err_req = encode_dbrpc_request(
            DbRpcRequest(
                DBRPC_VERSION,
                int(DbRpcOp.EXECUTE),
                4,
                0,
                1,
                "SELECT * FROM missing_table_xyz",
                (),
                None,
                0,
            )
        )
        err_resp = decode_dbrpc_response(BrokerEngineRegistry.execute(6, pool_id, err_req))
        assert err_resp.request_id == 4
        assert err_resp.status != 0
        assert err_resp.error_class == "DuckDBError"
        assert err_resp.error_message is not None and "missing_table_xyz" in err_resp.error_message

        arrow_req = encode_dbrpc_request(
            DbRpcRequest(
                DBRPC_VERSION,
                int(DbRpcOp.EXECUTE),
                5,
                0,
                1,
                "SELECT i AS n FROM range(300) t(i)",
                (),
                None,
                128,
            )
        )
        arrow_resp = decode_dbrpc_response(BrokerEngineRegistry.execute(6, pool_id, arrow_req))
        assert arrow_resp.status == 0
        assert arrow_resp.arrow_shm is not None and arrow_resp.arrow_shm.startswith("/vtb-dd-")
        assert arrow_resp.rowcount == 300
        shm_bytes = Path(f"/dev/shm/{arrow_resp.arrow_shm.lstrip('/')}").read_bytes()
        assert shm_bytes[:8] == b"VTBARROW"
        byte_len = struct.unpack("<Q", shm_bytes[8:16])[0]
        cursor = ArrowCursor(arrow_resp.arrow_shm, byte_len, 300)
        assert cursor.row_count == 300
        assert len(memoryview(cursor)) == byte_len
    finally:
        BrokerEngineRegistry.close_pool(6, pool_id)
        BrokerEngineRegistry.close_pool(6, pool_id)
