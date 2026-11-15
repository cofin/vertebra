"""Unit and cross-process tests for co-located ShmStatementCache, lease reclamation, and SHM channel rings."""

from __future__ import annotations

import multiprocessing as mp
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
import types
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import ArrowCursor, BrokerAttachClient, BrokerAttachServer, BrokerEngineRegistry
from vertebra.extensions.broker import (
    DBRPC_VERSION,
    BrokerAuthenticationError,
    BrokerConfig,
    BrokerEngineDiscovery,
    BrokerHost,
    BrokerPlugin,
    DbRpcOp,
    DbRpcRequest,
    ShutdownReport,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.dbrpc import (
    decode_dbrpc_response,
    encode_dbrpc_request,
)
from vertebra.server.platform import ZPagesRegistry
from vertebra.telemetry.otel import DbPoolMetricSample, DbPoolRecorder
from vertebra.utils.serializers import decode_json


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


def test_uds_broker_attach_wrong_secret_perms_and_worker_transport(tmp_path: Path) -> None:
    """Verify UDS attach enforces 0700 dir permissions, rejects bad secrets, and shares owner leases."""
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="vtb-uds-") as raw_dir:
        sock_dir = Path(raw_dir)
        os.chmod(sock_dir, 0o700)
        sock_path = str(sock_dir / "broker.sock")
        secret = b"correct-broker-attach-secret-key"

        owner_cfg = BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50, socket_path=sock_path)
        host = BrokerHost(owner_cfg, socket_path=sock_path, secret=secret)
        host.start()
        try:
            assert "correct-broker-attach-secret-key" not in repr(host)
            assert "[REDACTED]" in repr(host)

            with pytest.raises(BrokerAuthenticationError) as wrong_exc:
                BrokerAttachClient.attach(sock_path, b"wrong-broker-attach-secret-key", timeout_ms=1000)
            assert "correct-broker-attach-secret-key" not in str(wrong_exc.value)

            os.chmod(sock_dir, 0o755)
            try:
                with pytest.raises(BrokerAuthenticationError):
                    BrokerAttachClient.attach(sock_path, secret, timeout_ms=1000)
            finally:
                os.chmod(sock_dir, 0o700)

            worker = SyncSHMBrokerTransport(
                BrokerConfig(
                    pool_size=2,
                    max_overflow=0,
                    acquire_timeout_ms=50,
                    role="worker",
                    socket_path=sock_path,
                    attach_secret=secret,
                )
            )
            assert "correct-broker-attach-secret-key" not in repr(
                worker.config if hasattr(worker, "config") else owner_cfg
            )
            with host.transport.acquire(worker_id=1), worker.acquire(worker_id=2), pytest.raises(TimeoutError):
                worker.acquire(worker_id=3, timeout_ms=20)
            assert host.transport.metrics().active_leases == 0
        finally:
            host.stop(timeout=2.0)

        with pytest.raises(BrokerAuthenticationError):
            os.chmod(tmp_path, 0o755)
            try:
                BrokerAttachServer(str(tmp_path / "bad.sock"), secret, 0, 0)
            finally:
                os.chmod(tmp_path, 0o700)


def test_broker_host_worker_exit_rollback_success_failure_disposal_and_bounded_stop() -> None:
    """Verify owning-thread recovery handles ROLLBACK success, failure disposal, and bounded stop timeout."""
    host = BrokerHost(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    host.start()
    seen_threads: list[int] = []
    disposed_slots: list[int] = []
    try:
        host_tid = host.owning_thread_id
        assert host_tid is not None

        def _rollback_ok(lease: Any) -> None:
            seen_threads.append(threading.get_ident())

        host.register_session_hooks(
            on_rollback=_rollback_ok, on_dispose=lambda lease: disposed_slots.append(lease.slot_index)
        )
        g_ok = host.transport.acquire(worker_id=81001)
        reclaimed_ok = host.on_worker_exit(81001)
        assert reclaimed_ok == 1
        assert seen_threads == [host_tid]
        assert disposed_slots == []
        assert host.transport.quarantined_count() == 0

        def _rollback_fail(lease: Any) -> None:
            seen_threads.append(threading.get_ident())
            raise RuntimeError("simulated dirty connection reset error")

        host.register_session_hooks(
            on_rollback=_rollback_fail, on_dispose=lambda lease: disposed_slots.append(lease.slot_index)
        )
        g_fail = host.transport.acquire(worker_id=81002)
        reclaimed_fail = host.on_worker_exit(81002)
        assert reclaimed_fail == 1
        assert seen_threads == [host_tid, host_tid]
        assert disposed_slots == [g_fail.slot_index]
        assert host.transport.quarantined_count() == 0

        reused = host.transport.acquire(worker_id=81003)
        reused.release()
        _ = g_ok

        release_blocker = threading.Event()
        blocker_started = threading.Event()

        def _blocking_cleanup(_lease: Any = None) -> None:
            blocker_started.set()
            release_blocker.wait(timeout=1.0)

        blocker_thread = threading.Thread(
            target=lambda: host._executor.submit(_blocking_cleanup, timeout=2.0),
            daemon=True,
        )
        blocker_thread.start()
        assert blocker_started.wait(timeout=2.0)
        report = host.stop(timeout=0.05)
        release_blocker.set()
        blocker_thread.join(timeout=2.0)
        assert isinstance(report, ShutdownReport)
        assert report.completed is False
        assert report.pending_leases >= 1
        assert report.rolled_back_leases == 1
        assert report.rollback_errors == 1
        assert report.disposed_leases == 2
    finally:
        if host.started:
            host.stop(timeout=1.0)


def test_vtb_broker_cli_help_loads_lazily_without_eager_engine_imports() -> None:
    """Verify `vtb broker --help` prints help quickly without importing native engine drivers."""
    script = (
        "import sys\n"
        "from click.testing import CliRunner\n"
        "from vertebra.cli import _build_cli\n"
        "cli = _build_cli()\n"
        "res = CliRunner().invoke(cli, ['broker', '--help'])\n"
        "assert res.exit_code == 0, res.output\n"
        "assert 'serve' in res.output and 'status' in res.output, res.output\n"
        "forbidden = ('vertebra_postgres', 'vertebra_mysql', 'vertebra_oracle', 'vertebra_mssql', 'vertebra_duckdb', 'vertebra.extensions.broker.engines')\n"
        "loaded = [m for m in forbidden if m in sys.modules]\n"
        "assert not loaded, f'eager imports loaded: {loaded}'\n"
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr or proc.stdout


def test_broker_plugin_snapshot_metrics_and_zpages_quarantine_counters() -> None:
    """Verify BrokerPlugin.snapshot_metrics and /brokerz report total_quarantined and total_disposed."""
    zpages = ZPagesRegistry("broker-telemetry-test")
    recorder = DbPoolRecorder()
    plugin = BrokerPlugin(
        BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50),
        zpages=zpages,
        otel_recorder=recorder,
        name="telemetry_pool",
    )
    lease = plugin.transport.acquire(worker_id=91001, bind_key="telemetry_pool")
    reclaimed = plugin.transport.reclaim_worker_leases(91001)
    assert len(reclaimed) == 1
    assert reclaimed[0].slot_index == lease.slot_index

    mid_sample = plugin.snapshot_metrics()
    assert isinstance(mid_sample, DbPoolMetricSample)
    assert mid_sample.quarantined == 1
    assert mid_sample.total_quarantined == 1
    assert mid_sample.total_disposed == 0

    plugin.transport.dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
    end_sample = plugin.snapshot_metrics()
    assert end_sample.quarantined == 0
    assert end_sample.total_quarantined == 1
    assert end_sample.total_disposed == 1
    assert len(recorder.samples) == 2

    status_code, payload = zpages.handle_zpage("/brokerz")
    assert status_code == 200
    body = decode_json(payload)
    assert body["brokers"]["telemetry_pool"]["total_quarantined"] == 1
    assert body["brokers"]["telemetry_pool"]["total_disposed"] == 1
    assert body["brokers"]["telemetry_pool"]["quarantined"] == 0
