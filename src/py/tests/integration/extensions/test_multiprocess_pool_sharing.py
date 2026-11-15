"""Multi-process shared-memory pool sharing, UDS attach, SIGKILL crash recovery, and telemetry tests."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import socket
import struct
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vertebra._vertebra import BrokerEngineRegistry
from vertebra.db.adapters.mysql import MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.extensions.broker import (
    DBRPC_VERSION,
    BrokerConfig,
    BrokerEngineDiscovery,
    BrokerHost,
    BrokerPlugin,
    DbRpcOp,
    DbRpcRequest,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.dbrpc import decode_dbrpc_response, encode_dbrpc_request
from vertebra.server.platform import ZPagesRegistry
from vertebra.telemetry.otel import DbPoolMetricSample, DbPoolRecorder
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

_LENGTH_STRUCT = struct.Struct("!I")
_DIALECT_POSTGRES = 1
_DIALECT_MYSQL = 2
_DIALECT_DUCKDB = 6


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    """Read exactly ``size`` bytes from a stream socket or raise EOFError."""
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("unexpected EOF on UDS RPC stream")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_frame(sock: socket.socket, payload: bytes) -> None:
    """Send a length-prefixed binary RPC payload over a stream socket."""
    sock.sendall(_LENGTH_STRUCT.pack(len(payload)) + payload)


def _recv_frame(sock: socket.socket) -> bytes:
    """Receive a length-prefixed binary RPC payload from a stream socket."""
    header = _recv_exact(sock, _LENGTH_STRUCT.size)
    (length,) = _LENGTH_STRUCT.unpack(header)
    return _recv_exact(sock, length)


def _uds_rpc_call(rpc_socket_path: str, req: DbRpcRequest) -> Any:
    """Send one DbRpcRequest over a UDS stream socket and decode the DbRpcResponse."""
    payload = encode_dbrpc_request(req)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(rpc_socket_path)
        _send_frame(client, payload)
        return decode_dbrpc_response(_recv_frame(client))


class _EngineRpcServer:
    """Thread-safe UDS relay exposing one BrokerHost-owned native engine pool to worker PIDs."""

    def __init__(
        self,
        rpc_socket_path: str,
        dialect_tag: int,
        engine_pool_id: int,
        transport: SyncSHMBrokerTransport,
    ) -> None:
        self._rpc_socket_path = rpc_socket_path
        self._dialect_tag = dialect_tag
        self._engine_pool_id = engine_pool_id
        self._transport = transport
        self._peak_active_leases = 0
        self._peak_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._server_sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    @property
    def peak_active_leases(self) -> int:
        """Return the highest active_leases observed across dispatched worker RPC calls."""
        with self._peak_lock:
            return self._peak_active_leases

    def start(self) -> None:
        """Bind the AF_UNIX RPC listener socket and begin serving worker requests."""
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self._rpc_socket_path)
        os.chmod(self._rpc_socket_path, 0o600)
        server.listen(32)
        server.settimeout(0.05)
        self._server_sock = server
        self._thread = threading.Thread(target=self._serve_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the UDS RPC listener loop and wait for worker handler shutdown."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if self._server_sock is not None:
            self._server_sock.close()
            self._server_sock = None

    def _handle_client(self, conn: socket.socket) -> None:
        """Serve one length-prefixed DBRPC call per connection."""
        with conn:
            try:
                frame = _recv_frame(conn)
            except EOFError:
                return
            active = self._transport.metrics().active_leases
            with self._peak_lock:
                if active > self._peak_active_leases:
                    self._peak_active_leases = active
            resp_bytes = BrokerEngineRegistry.execute(self._dialect_tag, self._engine_pool_id, frame)
            _send_frame(conn, resp_bytes)

    def _serve_loop(self) -> None:
        """Accept incoming UDS RPC connections and handle each on a daemon thread."""
        assert self._server_sock is not None
        while not self._stop_event.is_set():
            try:
                conn, _addr = self._server_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()


def _attach_worker_transport(attach_socket_path: str, secret: bytes) -> SyncSHMBrokerTransport:
    """Attach a spawned child process to the parent's broker SHM exclusively via UDS SCM_RIGHTS."""
    worker_cfg = BrokerConfig(
        role="worker",
        socket_path=attach_socket_path,
        attach_secret=secret,
        acquire_timeout_ms=5000,
    )
    return SyncSHMBrokerTransport(worker_cfg)


def _worker_commit_rows(
    worker_idx: int,
    attach_socket_path: str,
    secret: bytes,
    rpc_socket_path: str,
    insert_sql: str,
    iterations: int,
) -> None:
    """Attach via UDS in a spawned worker process and insert committed rows across shared leases."""
    transport = _attach_worker_transport(attach_socket_path, secret)
    pid = os.getpid()
    try:
        for seq in range(iterations):
            row_id = worker_idx * 100 + seq + 1
            with transport.acquire(worker_id=pid, bind_key="shared") as lease:
                begin_resp = _uds_rpc_call(
                    rpc_socket_path,
                    DbRpcRequest(
                        version=DBRPC_VERSION,
                        op=int(DbRpcOp.BEGIN),
                        request_id=row_id * 10 + 1,
                        lease_slot=lease.slot_index,
                        lease_generation=lease.generation,
                    ),
                )
                assert begin_resp.status == 0, begin_resp.error_message
                ins_resp = _uds_rpc_call(
                    rpc_socket_path,
                    DbRpcRequest(
                        version=DBRPC_VERSION,
                        op=int(DbRpcOp.EXECUTE),
                        request_id=row_id * 10 + 2,
                        lease_slot=lease.slot_index,
                        lease_generation=lease.generation,
                        sql=insert_sql,
                        params=(row_id, pid),
                    ),
                )
                assert ins_resp.status == 0, ins_resp.error_message
                commit_resp = _uds_rpc_call(
                    rpc_socket_path,
                    DbRpcRequest(
                        version=DBRPC_VERSION,
                        op=int(DbRpcOp.COMMIT),
                        request_id=row_id * 10 + 3,
                        lease_slot=lease.slot_index,
                        lease_generation=lease.generation,
                    ),
                )
                assert commit_resp.status == 0, commit_resp.error_message
    finally:
        transport.close()


def _worker_hold_uncommitted_and_pause(
    attach_socket_path: str,
    secret: bytes,
    rpc_socket_path: str,
    insert_sql: str,
    row_id: int,
    ready_conn: Connection,
) -> None:
    """Attach via UDS, write an uncommitted row inside an active transaction, signal parent, and pause."""
    transport = _attach_worker_transport(attach_socket_path, secret)
    pid = os.getpid()
    lease = transport.acquire(worker_id=pid, bind_key="shared")
    begin_resp = _uds_rpc_call(
        rpc_socket_path,
        DbRpcRequest(
            version=DBRPC_VERSION,
            op=int(DbRpcOp.BEGIN),
            request_id=1,
            lease_slot=lease.slot_index,
            lease_generation=lease.generation,
        ),
    )
    assert begin_resp.status == 0, begin_resp.error_message
    ins_resp = _uds_rpc_call(
        rpc_socket_path,
        DbRpcRequest(
            version=DBRPC_VERSION,
            op=int(DbRpcOp.EXECUTE),
            request_id=2,
            lease_slot=lease.slot_index,
            lease_generation=lease.generation,
            sql=insert_sql,
            params=(row_id, pid),
        ),
    )
    assert ins_resp.status == 0, ins_resp.error_message
    ready_conn.send((pid, lease.slot_index, lease.generation))
    ready_conn.close()
    while True:
        time.sleep(1.0)


def _worker_acquire_lease_and_pause(
    attach_socket_path: str,
    secret: bytes,
    ready_conn: Connection,
) -> None:
    """Attach via UDS, acquire one broker lease, signal parent, and sleep for SIGKILL testing."""
    transport = _attach_worker_transport(attach_socket_path, secret)
    pid = os.getpid()
    lease = transport.acquire(worker_id=pid, bind_key="shared")
    ready_conn.send((pid, lease.slot_index, lease.generation))
    ready_conn.close()
    while True:
        time.sleep(1.0)


def _start_host_with_plugin(
    sock_dir: Path,
    *,
    pool_size: int,
    max_overflow: int,
    dialect_tag: int,
    dsn: str,
) -> tuple[BrokerHost, BrokerPlugin, ZPagesRegistry, DbPoolRecorder, int, str, bytes]:
    """Initialize a BrokerHost + BrokerPlugin + ZPagesRegistry owning one native engine pool."""
    attach_sock = str(sock_dir / "attach.sock")
    secret = b"multiprocess-acceptance-secret-32"
    cfg = BrokerConfig(
        pool_size=pool_size,
        max_overflow=max_overflow,
        acquire_timeout_ms=5000,
        socket_path=attach_sock,
        attach_secret=secret,
    )
    zpages = ZPagesRegistry("multiprocess-broker-test")
    recorder = DbPoolRecorder()
    plugin = BrokerPlugin(config=cfg, zpages=zpages, otel_recorder=recorder, name="shared")
    assert isinstance(plugin.transport, SyncSHMBrokerTransport)
    plugin.transport.register_partition("shared", pool_size=pool_size, max_overflow=max_overflow)
    host = BrokerHost(cfg, socket_path=attach_sock, secret=secret, transport=plugin.transport)
    host.start()
    engine_pool_id = host.open_engine_pool(0, dialect_tag, dsn, pool_size + max_overflow)
    return host, plugin, zpages, recorder, engine_pool_id, attach_sock, secret


def test_multiprocess_duckdb_pool_sharing_sigkill_rollback_and_eviction(tmp_path: Path) -> None:
    """Verify 4 spawned workers share one DuckDB BrokerHost pool via UDS and recover cleanly on SIGKILL."""
    BrokerEngineDiscovery.register_installed()
    db_path = tmp_path / "shared_workers.duckdb"
    dsn = f"duckdb:///{db_path}"

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="vtb-mp-duck-") as raw_dir:
        sock_dir = Path(raw_dir)
        os.chmod(sock_dir, 0o700)
        rpc_sock = str(sock_dir / "rpc.sock")
        host, plugin, zpages, recorder, pool_id, attach_sock, secret = _start_host_with_plugin(
            sock_dir,
            pool_size=1,
            max_overflow=0,
            dialect_tag=_DIALECT_DUCKDB,
            dsn=dsn,
        )
        rpc_server = _EngineRpcServer(rpc_sock, _DIALECT_DUCKDB, pool_id, host.transport)
        try:
            ddl_resp = decode_dbrpc_response(
                BrokerEngineRegistry.execute(
                    _DIALECT_DUCKDB,
                    pool_id,
                    encode_dbrpc_request(
                        DbRpcRequest(
                            version=DBRPC_VERSION,
                            op=int(DbRpcOp.EXECUTE),
                            request_id=1,
                            lease_slot=0,
                            lease_generation=1,
                            sql="CREATE TABLE work_log (id BIGINT PRIMARY KEY, worker_pid BIGINT)",
                        )
                    ),
                )
            )
            assert ddl_resp.status == 0, ddl_resp.error_message

            rpc_server.start()
            ctx = mp.get_context("spawn")
            workers = [
                ctx.Process(
                    target=_worker_commit_rows,
                    args=(
                        worker_idx,
                        attach_sock,
                        secret,
                        rpc_sock,
                        "INSERT INTO work_log VALUES (?, ?)",
                        3,
                    ),
                )
                for worker_idx in range(4)
            ]
            for proc in workers:
                proc.start()
            for proc in workers:
                proc.join(timeout=20)
                assert proc.exitcode == 0

            assert rpc_server.peak_active_leases <= 1
            assert host.transport.metrics().active_leases == 0
            assert host.transport.metrics().total_acquired == 12

            count_resp = _uds_rpc_call(
                rpc_sock,
                DbRpcRequest(
                    version=DBRPC_VERSION,
                    op=int(DbRpcOp.EXECUTE),
                    request_id=9001,
                    lease_slot=0,
                    lease_generation=1,
                    sql="SELECT count(*), count(DISTINCT worker_pid) FROM work_log",
                ),
            )
            assert count_resp.status == 0
            assert count_resp.rows == ((12, 4),)

            parent_conn, child_conn = ctx.Pipe(duplex=False)
            killer_proc = ctx.Process(
                target=_worker_hold_uncommitted_and_pause,
                args=(
                    attach_sock,
                    secret,
                    rpc_sock,
                    "INSERT INTO work_log VALUES (?, ?)",
                    999_999,
                    child_conn,
                ),
            )
            killer_proc.start()
            child_conn.close()
            dead_pid, _slot_idx, _gen = parent_conn.recv()
            parent_conn.close()
            assert host.transport.metrics().active_leases == 1

            os.kill(dead_pid, signal.SIGKILL)
            killer_proc.join(timeout=5)
            reclaimed = host.on_worker_exit(dead_pid)
            assert reclaimed == 1
            assert host.transport.quarantined_count() == 0
            assert host.transport.metrics().active_leases == 0

            post_crash_resp = _uds_rpc_call(
                rpc_sock,
                DbRpcRequest(
                    version=DBRPC_VERSION,
                    op=int(DbRpcOp.EXECUTE),
                    request_id=9002,
                    lease_slot=0,
                    lease_generation=1,
                    sql="SELECT count(*) FROM work_log WHERE id = 999999",
                ),
            )
            assert post_crash_resp.status == 0
            assert post_crash_resp.rows == ((0,),)

            sample = plugin.snapshot_metrics()
            assert isinstance(sample, DbPoolMetricSample)
            assert sample.total_quarantined == 1
            assert sample.total_disposed == 1
            assert sample.quarantined == 0
            assert len(recorder.samples) == 1

            status_code, zpage_bytes = zpages.handle_zpage("/brokerz")
            assert status_code == 200
            zpage = decode_json(zpage_bytes)
            assert zpage["brokers"]["shared"]["total_quarantined"] == 1
            assert zpage["brokers"]["shared"]["total_disposed"] == 1

            disposed_on_error: list[int] = []
            host.register_session_hooks(
                on_rollback=lambda _lease: (_ for _ in ()).throw(RuntimeError("simulated rollback failure")),
                on_dispose=lambda lease: disposed_on_error.append(lease.slot_index),
            )
            parent_err_conn, child_err_conn = ctx.Pipe(duplex=False)
            fail_proc = ctx.Process(
                target=_worker_acquire_lease_and_pause,
                args=(attach_sock, secret, child_err_conn),
            )
            fail_proc.start()
            child_err_conn.close()
            fail_pid, fail_slot, _fail_gen = parent_err_conn.recv()
            parent_err_conn.close()

            os.kill(fail_pid, signal.SIGKILL)
            fail_proc.join(timeout=5)
            assert host.on_worker_exit(fail_pid) == 1
            assert disposed_on_error == [fail_slot]
            assert host.transport.quarantined_count() == 0

            after_eviction = plugin.snapshot_metrics()
            assert after_eviction.total_quarantined == 2
            assert after_eviction.total_disposed == 2
            assert after_eviction.quarantined == 0

            healthy_proc = ctx.Process(
                target=_worker_commit_rows,
                args=(
                    8,
                    attach_sock,
                    secret,
                    rpc_sock,
                    "INSERT INTO work_log VALUES (?, ?)",
                    1,
                ),
            )
            healthy_proc.start()
            healthy_proc.join(timeout=15)
            assert healthy_proc.exitcode == 0
        finally:
            rpc_server.stop()
            report = host.stop(timeout=5.0)
            assert report.completed is True


def test_docker_postgres_multiprocess_pool_sharing(vtb_postgres_config: PostgreSQLConfig) -> None:
    """Verify 4 spawned workers share one PostgreSQL BrokerHost pool and SIGKILL triggers ROLLBACK."""
    BrokerEngineDiscovery.register_installed()
    pool_size = 1
    app_tag = f"vtb-mp-pg-{os.getpid()}"
    dsn = f"{vtb_postgres_config.dsn}?application_name={app_tag}"

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="vtb-mp-pg-") as raw_dir:
        sock_dir = Path(raw_dir)
        os.chmod(sock_dir, 0o700)
        rpc_sock = str(sock_dir / "rpc.sock")
        host, plugin, _zpages, _recorder, pool_id, attach_sock, secret = _start_host_with_plugin(
            sock_dir,
            pool_size=pool_size,
            max_overflow=0,
            dialect_tag=_DIALECT_POSTGRES,
            dsn=dsn,
        )
        rpc_server = _EngineRpcServer(rpc_sock, _DIALECT_POSTGRES, pool_id, host.transport)
        inspector = SyncPostgreSQLDriver(vtb_postgres_config)
        try:
            inspector.execute("CREATE TABLE IF NOT EXISTS mp_pg_work_log (id INTEGER PRIMARY KEY, worker_pid INTEGER)")
            inspector.execute("TRUNCATE TABLE mp_pg_work_log")

            rpc_server.start()
            ctx = mp.get_context("spawn")
            workers = [
                ctx.Process(
                    target=_worker_commit_rows,
                    args=(
                        worker_idx,
                        attach_sock,
                        secret,
                        rpc_sock,
                        "INSERT INTO mp_pg_work_log VALUES ($1, $2)",
                        3,
                    ),
                )
                for worker_idx in range(4)
            ]
            for proc in workers:
                proc.start()
            for proc in workers:
                proc.join(timeout=30)
                assert proc.exitcode == 0

            backend_count = inspector.select_value(
                "SELECT count(*)::int4 FROM pg_stat_activity WHERE application_name = $1",
                app_tag,
            )
            assert 1 <= int(backend_count) <= pool_size
            committed = inspector.select_value(
                "SELECT count(*)::int4 FROM mp_pg_work_log",
            )
            assert int(committed) == 12

            parent_conn, child_conn = ctx.Pipe(duplex=False)
            killer_proc = ctx.Process(
                target=_worker_hold_uncommitted_and_pause,
                args=(
                    attach_sock,
                    secret,
                    rpc_sock,
                    "INSERT INTO mp_pg_work_log VALUES ($1, $2)",
                    999_999,
                    child_conn,
                ),
            )
            killer_proc.start()
            child_conn.close()
            dead_pid, _slot_idx, _gen = parent_conn.recv()
            parent_conn.close()

            os.kill(dead_pid, signal.SIGKILL)
            killer_proc.join(timeout=5)
            assert host.on_worker_exit(dead_pid) == 1

            idle_in_tx = inspector.select_value(
                "SELECT count(*)::int4 FROM pg_stat_activity "
                "WHERE application_name = $1 AND state = 'idle in transaction'",
                app_tag,
            )
            assert int(idle_in_tx) == 0
            uncommitted_rows = inspector.select_value(
                "SELECT count(*)::int4 FROM mp_pg_work_log WHERE id = 999999",
            )
            assert int(uncommitted_rows) == 0
            metrics = plugin.snapshot_metrics()
            assert metrics.total_quarantined >= 1
            assert metrics.total_disposed >= 1
        finally:
            inspector.close()
            rpc_server.stop()
            host.stop(timeout=5.0)


def test_docker_mysql_multiprocess_pool_sharing(vtb_mysql_config: MySQLConfig) -> None:
    """Verify 4 spawned workers share one MySQL BrokerHost pool and SIGKILL triggers ROLLBACK."""
    BrokerEngineDiscovery.register_installed()
    pool_size = 1

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="vtb-mp-my-") as raw_dir:
        sock_dir = Path(raw_dir)
        os.chmod(sock_dir, 0o700)
        rpc_sock = str(sock_dir / "rpc.sock")
        host, plugin, _zpages, _recorder, pool_id, attach_sock, secret = _start_host_with_plugin(
            sock_dir,
            pool_size=pool_size,
            max_overflow=0,
            dialect_tag=_DIALECT_MYSQL,
            dsn=vtb_mysql_config.dsn,
        )
        rpc_server = _EngineRpcServer(rpc_sock, _DIALECT_MYSQL, pool_id, host.transport)
        inspector = SyncMySQLDriver(vtb_mysql_config)
        try:
            inspector.execute("CREATE TABLE IF NOT EXISTS mp_mysql_work_log (id INT PRIMARY KEY, worker_pid INT)")
            inspector.execute("TRUNCATE TABLE mp_mysql_work_log")

            rpc_server.start()
            ctx = mp.get_context("spawn")
            workers = [
                ctx.Process(
                    target=_worker_commit_rows,
                    args=(
                        worker_idx,
                        attach_sock,
                        secret,
                        rpc_sock,
                        "INSERT INTO mp_mysql_work_log VALUES (?, ?)",
                        3,
                    ),
                )
                for worker_idx in range(4)
            ]
            for proc in workers:
                proc.start()
            for proc in workers:
                proc.join(timeout=30)
                assert proc.exitcode == 0

            proc_count = inspector.select_value(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = DATABASE() AND ID <> CONNECTION_ID()"
            )
            assert 1 <= int(proc_count) <= pool_size
            committed = inspector.select_value("SELECT COUNT(*) FROM mp_mysql_work_log")
            assert int(committed) == 12

            parent_conn, child_conn = ctx.Pipe(duplex=False)
            killer_proc = ctx.Process(
                target=_worker_hold_uncommitted_and_pause,
                args=(
                    attach_sock,
                    secret,
                    rpc_sock,
                    "INSERT INTO mp_mysql_work_log VALUES (?, ?)",
                    999_999,
                    child_conn,
                ),
            )
            killer_proc.start()
            child_conn.close()
            dead_pid, _slot_idx, _gen = parent_conn.recv()
            parent_conn.close()

            os.kill(dead_pid, signal.SIGKILL)
            killer_proc.join(timeout=5)
            assert host.on_worker_exit(dead_pid) == 1

            uncommitted_rows = inspector.select_value(
                "SELECT COUNT(*) FROM mp_mysql_work_log WHERE id = 999999",
            )
            assert int(uncommitted_rows) == 0
            metrics = plugin.snapshot_metrics()
            assert metrics.total_quarantined >= 1
            assert metrics.total_disposed >= 1
        finally:
            inspector.close()
            rpc_server.stop()
            host.stop(timeout=5.0)
