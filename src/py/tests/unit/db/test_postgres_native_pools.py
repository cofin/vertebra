"""Unit and loopback tests for SyncPostgresPool and AsyncPostgresPool session leases (`ch15:t01`)."""

from __future__ import annotations

import asyncio
import inspect
import socket
import struct
import threading
import time
from collections.abc import Sequence
from typing import Any

import pytest

from tests.unit.db.postgres_data_peers import (
    _bind_frame_stmt_params_and_formats,
    _parse_frame_stmt_and_sql,
    _reply_extended_query,
)
from vertebra._postgres import AsyncPostgresPool, SyncPostgresPool
from vertebra._vertebra import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    OperationalError,
    PoolExhaustedError,
    ProgrammingError,
)
from vertebra.db.adapters.postgres.types import PG_UNCHANGED_TOAST


def _frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _auth_ok_and_ready(pid: int) -> bytes:
    """Build AuthenticationOk + ParameterStatus + BackendKeyData(pid) + ReadyForQuery('I')."""
    return b"".join(
        (
            _frame(b"R", struct.pack("!i", 0)),
            _frame(b"S", b"application_name\x00vertebra\x00"),
            _frame(b"S", b"client_encoding\x00UTF8\x00"),
            _frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _frame(b"S", b"TimeZone\x00UTC\x00"),
            _frame(b"K", struct.pack("!ii", pid, 9000 + pid)),
            _frame(b"Z", b"I"),
        )
    )


def _pid_query_response(pid: int, tx_status: bytes) -> bytes:
    """Build an extended-query SELECT pg_backend_pid() single-row response."""
    col_body = struct.pack("!h", 1) + b"pid\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, 0)
    pid_bytes = str(pid).encode("ascii")
    row_body = struct.pack("!hi", 1, len(pid_bytes)) + pid_bytes
    return b"".join(
        (
            _frame(b"1", b""),
            _frame(b"t", struct.pack("!h", 0)),
            _frame(b"T", col_body),
            _frame(b"2", b""),
            _frame(b"T", col_body),
            _frame(b"D", row_body),
            _frame(b"C", b"SELECT 1\x00"),
            _frame(b"3", b""),
            _frame(b"Z", tx_status),
        )
    )


def _warm_pid_response(pid: int, tx_status: bytes) -> bytes:
    """Build a warm binary-format SELECT pg_backend_pid() single-row response."""
    row_body = struct.pack("!hii", 1, 4, pid)
    return b"".join(
        (
            _frame(b"2", b""),
            _frame(b"D", row_body),
            _frame(b"C", b"SELECT 1\x00"),
            _frame(b"Z", tx_status),
        )
    )


def _read_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a blocking loopback socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("unexpected client EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_startup(conn: socket.socket) -> None:
    """Consume StartupMessage packet."""
    (length,) = struct.unpack("!i", _read_exact(conn, 4))
    _read_exact(conn, length - 4)


def _read_command_group(conn: socket.socket) -> list[tuple[bytes, bytes]]:
    """Read frontend frames until Sync ('S'), Query ('Q'), or Terminate ('X')."""
    frames: list[tuple[bytes, bytes]] = []
    while True:
        hdr = _read_exact(conn, 5)
        tag = hdr[:1]
        (length,) = struct.unpack("!i", hdr[1:5])
        body = _read_exact(conn, length - 4)
        frames.append((tag, body))
        if tag in (b"S", b"Q", b"X"):
            return frames


class _ScriptedPidServer:
    """Multi-connection loopback server assigning sequential backend PIDs and handling pool resets."""

    def __init__(self, *, fail_first_reset: bool = False) -> None:
        """Bind ephemeral loopback listener and launch background accept loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.port: int = self._sock.getsockname()[1]
        self.accept_count = 0
        self._fail_first_reset = fail_first_reset
        self._reset_failed_once = False
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        """Accept incoming pooled sockets until stopped."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with self._lock:
                self.accept_count += 1
                pid = 4000 + self.accept_count
            t = threading.Thread(target=self._serve_conn, args=(conn, pid), daemon=True)
            self._workers.append(t)
            t.start()

    def _serve_conn(self, conn: socket.socket, pid: int) -> None:
        """Respond to BEGIN, COMMIT, ROLLBACK, DISCARD ALL, and SELECT pg_backend_pid()."""
        tx = b"I"
        with conn:
            try:
                _read_startup(conn)
                conn.sendall(_auth_ok_and_ready(pid))
                while not self._stop.is_set():
                    frames = _read_command_group(conn)
                    last_tag, last_body = frames[-1]
                    if last_tag == b"X":
                        return
                    if last_tag == b"Q":
                        sql = last_body.rstrip(b"\x00").decode("utf-8", errors="replace").strip()
                        upper = sql.upper()
                        if upper == "BEGIN":
                            tx = b"T"
                            conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", tx))
                        elif upper == "COMMIT":
                            tx = b"I"
                            conn.sendall(_frame(b"C", b"COMMIT\x00") + _frame(b"Z", tx))
                        elif upper == "ROLLBACK":
                            tx = b"I"
                            conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx))
                        elif upper == "DISCARD ALL":
                            with self._lock:
                                do_fail = self._fail_first_reset and not self._reset_failed_once
                                if do_fail:
                                    self._reset_failed_once = True
                            if do_fail:
                                return
                            tx = b"I"
                            conn.sendall(_frame(b"C", b"DISCARD ALL\x00") + _frame(b"Z", tx))
                        else:
                            conn.sendall(_frame(b"C", b"SET\x00") + _frame(b"Z", tx))
                    elif last_tag == b"S":
                        tags = [t for t, _ in frames]
                        if b"P" in tags:
                            conn.sendall(_pid_query_response(pid, tx))
                        else:
                            conn.sendall(_warm_pid_response(pid, tx))
            except (EOFError, OSError):
                return

    def stop(self) -> None:
        """Stop accepting and close listener."""
        self._stop.set()
        self._sock.close()
        self._accept_thread.join(timeout=2.0)


def test_sync_pool_pid_reuse_transaction_pinning_saturation_and_reset_replacement() -> None:
    """Verify SyncPostgresPool preserves backend PID across tx, enforces max_size=1 saturation, and replaces broken resets."""
    srv = _ScriptedPidServer()
    try:
        pool = SyncPostgresPool(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
            min_size=1,
            max_size=1,
            acquire_timeout=0.06,
            reset_timeout=2.0,
            close_timeout=2.0,
        )
        c1 = pool.acquire()
        assert not c1.closed
        c1.begin()
        p1 = c1.query("SELECT pg_backend_pid() AS pid").rows[0][0]
        assert p1 == 4001
        c1.commit()
        assert c1.query("SELECT pg_backend_pid() AS pid").rows[0][0] == p1

        with pytest.raises(PoolExhaustedError):
            pool.acquire()

        c1.close()
        assert c1.closed
        with pytest.raises(InterfaceError):
            c1.query("SELECT pg_backend_pid() AS pid")

        c2 = pool.acquire()
        p2 = c2.query("SELECT pg_backend_pid() AS pid").rows[0][0]
        assert p2 == p1
        assert srv.accept_count == 1
        c2.close()
        pool.close()
        assert pool.closed
    finally:
        srv.stop()

    fail_srv = _ScriptedPidServer(fail_first_reset=True)
    try:
        pool2 = SyncPostgresPool(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=fail_srv.port,
            sslmode="disable",
            channel_binding="disable",
            min_size=1,
            max_size=1,
        )
        first = pool2.acquire()
        pid_a = first.query("SELECT pg_backend_pid() AS pid").rows[0][0]
        with pytest.raises((OperationalError, Exception)):
            first.close()

        second = pool2.acquire()
        pid_b = second.query("SELECT pg_backend_pid() AS pid").rows[0][0]
        assert pid_b != pid_a
        second.close()
        pool2.close()
    finally:
        fail_srv.stop()


def test_async_pool_pid_pinning_and_cancelled_acquire_recovers_permits() -> None:
    """Verify AsyncPostgresPool recovers permits across both waiting and handoff-race Future cancellations."""
    srv = _ScriptedPidServer()

    async def _run() -> None:
        pool = AsyncPostgresPool(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
            min_size=1,
            max_size=1,
            acquire_timeout=1.5,
            reset_timeout=2.0,
            close_timeout=2.0,
        )
        c1 = await pool.acquire()
        await c1.begin()
        pid1 = (await c1.query("SELECT pg_backend_pid() AS pid")).rows[0][0]
        await c1.commit()
        assert (await c1.query("SELECT pg_backend_pid() AS pid")).rows[0][0] == pid1

        waiter1 = asyncio.ensure_future(pool.acquire())
        await asyncio.sleep(0)
        waiter1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter1

        waiter2 = asyncio.ensure_future(pool.acquire())
        await asyncio.sleep(0)
        close_task = asyncio.ensure_future(c1.close())
        waiter2.cancel()
        await close_task
        try:
            maybe_client = await waiter2
        except asyncio.CancelledError:
            pass
        else:
            await maybe_client.close()

        c3 = await asyncio.wait_for(pool.acquire(), timeout=1.0)
        pid3 = (await c3.query("SELECT pg_backend_pid() AS pid")).rows[0][0]
        assert pid3 == pid1
        await c3.close()
        await pool.close()
        assert pool.closed

    try:
        asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize(
    ("bad_kwargs", "exc_type"),
    [
        ({"min_size": True}, TypeError),
        ({"max_size": False}, TypeError),
        ({"statement_cache_size": True}, TypeError),
        ({"acquire_timeout": True}, TypeError),
        ({"statement_cache_size": -1}, ValueError),
        ({"min_size": 5, "max_size": 2}, ValueError),
        ({"max_size": 0}, ValueError),
        ({"acquire_timeout": 0.0}, ValueError),
        ({"reset_timeout": float("nan")}, ValueError),
        ({"close_timeout": float("inf")}, ValueError),
        ({"pooler_mode": "unknown"}, ValueError),
    ],
)
def test_pool_constructor_rejects_bools_and_invalid_bounds_without_io(
    bad_kwargs: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """Verify SyncPostgresPool and AsyncPostgresPool reject bools, invalid bounds, and bad modes synchronously."""
    base: dict[str, Any] = {
        "user": "alice",
        "database": "appdb",
        "host": "127.0.0.1",
        "port": 1,
        "sslmode": "disable",
    }
    merged = {**base, **bad_kwargs}
    with pytest.raises(exc_type):
        SyncPostgresPool(**merged)
    with pytest.raises(exc_type):
        AsyncPostgresPool(**merged)


@pytest.mark.parametrize(
    "mismatch_override",
    [
        {"password": "other"},
        {"sslmode": "prefer"},
        {"channel_binding": "disable"},
        {"session_gucs": {"TimeZone": "America/New_York"}},
        {"application_name": "other-app"},
        {"pooler_mode": "session"},
        {"protocol_level_pooler_cache": True},
        {"statement_cache_size": 64},
    ],
)
def test_pool_validate_configuration_detects_mismatches_with_zero_network_io(
    mismatch_override: dict[str, Any],
) -> None:
    """Verify validate_configuration performs zero TCP accepts and detects credential/TLS/GUC/preparation drift."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    try:
        base_opts: dict[str, Any] = {
            "user": "alice",
            "database": "appdb",
            "host": "127.0.0.1",
            "port": port,
            "password": "secret",
            "sslmode": "disable",
            "channel_binding": "prefer",
            "application_name": "vertebra-svc",
            "session_gucs": {"TimeZone": "UTC"},
            "pooler_mode": "transaction",
            "protocol_level_pooler_cache": False,
            "statement_cache_size": 128,
        }
        sync_pool = SyncPostgresPool(**base_opts, min_size=1, max_size=4)
        async_pool = AsyncPostgresPool(**base_opts, min_size=1, max_size=4)

        sync_pool.validate_configuration(**base_opts)
        async_pool.validate_configuration(**base_opts)

        bad = {**base_opts, **mismatch_override}
        with pytest.raises(InterfaceError):
            sync_pool.validate_configuration(**bad)
        with pytest.raises(InterfaceError):
            async_pool.validate_configuration(**bad)

        with pytest.raises(BlockingIOError):
            listener.accept()
    finally:
        listener.close()


def _error_response(sqlstate: bytes, message: bytes) -> bytes:
    """Build a PostgreSQL ErrorResponse ('E') frame followed by ReadyForQuery ('I')."""
    body = b"SERROR\x00C" + sqlstate + b"\x00M" + message + b"\x00\x00"
    return _frame(b"E", body) + _frame(b"Z", b"I")


def _select_one_response(cold: bool) -> bytes:
    """Build an extended-query SELECT 1 response for cold or warm execution."""
    if cold:
        col_body = struct.pack("!h", 1) + b"?column?\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, 0)
        row_body = struct.pack("!hi", 1, 1) + b"1"
        return b"".join(
            (
                _frame(b"1", b""),
                _frame(b"t", struct.pack("!h", 0)),
                _frame(b"T", col_body),
                _frame(b"2", b""),
                _frame(b"T", col_body),
                _frame(b"D", row_body),
                _frame(b"C", b"SELECT 1\x00"),
                _frame(b"3", b""),
                _frame(b"Z", b"I"),
            )
        )
    row_body = struct.pack("!hii", 1, 4, 1)
    return b"".join(
        (
            _frame(b"2", b""),
            _frame(b"D", row_body),
            _frame(b"C", b"SELECT 1\x00"),
            _frame(b"Z", b"I"),
        )
    )


def _insert_response(cold: bool, param_count: int) -> bytes:
    """Build an extended-query INSERT 0 1 response for cold or warm execution."""
    if cold:
        param_desc = struct.pack("!h" + "i" * param_count, param_count, *([23] * param_count))
        return b"".join(
            (
                _frame(b"1", b""),
                _frame(b"t", param_desc),
                _frame(b"n", b""),
                _frame(b"2", b""),
                _frame(b"n", b""),
                _frame(b"C", b"INSERT 0 1\x00"),
                _frame(b"3", b""),
                _frame(b"Z", b"I"),
            )
        )
    return b"".join(
        (
            _frame(b"2", b""),
            _frame(b"C", b"INSERT 0 1\x00"),
            _frame(b"Z", b"I"),
        )
    )


class _ScriptedCursorPoolServer:
    """Loopback server handling SELECT 1, parameterized INSERT with backend PID tracking, and injected errors."""

    def __init__(self) -> None:
        """Bind ephemeral loopback listener and launch background accept loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.port: int = self._sock.getsockname()[1]
        self.accept_count = 0
        self.insert_pids: list[int] = []
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        """Accept incoming pooled sockets until stopped."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with self._lock:
                self.accept_count += 1
                pid = 5000 + self.accept_count
            worker = threading.Thread(target=self._serve_conn, args=(conn, pid), daemon=True)
            self._workers.append(worker)
            worker.start()

    def _serve_conn(self, conn: socket.socket, pid: int) -> None:
        """Serve extended queries, prepared-statement bindings, and reset commands on one connection."""
        prepared: dict[str, str] = {}
        with conn:
            try:
                _read_startup(conn)
                conn.sendall(_auth_ok_and_ready(pid))
                while not self._stop.is_set():
                    frames = _read_command_group(conn)
                    last_tag, last_body = frames[-1]
                    if last_tag == b"X":
                        return
                    if last_tag == b"Q":
                        sql = last_body.rstrip(b"\x00").decode("utf-8", errors="replace").strip().upper()
                        if sql == "DISCARD ALL":
                            prepared.clear()
                            conn.sendall(_frame(b"C", b"DISCARD ALL\x00") + _frame(b"Z", b"I"))
                        else:
                            conn.sendall(_frame(b"C", b"OK\x00") + _frame(b"Z", b"I"))
                    elif last_tag == b"S":
                        sql_text = ""
                        cold = False
                        for tag, body in frames:
                            if tag == b"P":
                                cold = True
                                parts = body.split(b"\x00", 2)
                                stmt_name = parts[0].decode("utf-8")
                                sql_text = parts[1].decode("utf-8")
                                prepared[stmt_name] = sql_text
                            elif tag == b"B" and not sql_text:
                                parts = body.split(b"\x00", 2)
                                stmt_name = parts[1].decode("utf-8")
                                sql_text = prepared.get(stmt_name, "")
                        upper = sql_text.upper()
                        if "FAIL_DROP_CONN" in upper:
                            return
                        if "FAIL_SERVER_ERROR" in upper:
                            conn.sendall(_error_response(b"22012", b"division by zero"))
                        elif upper.startswith("INSERT"):
                            with self._lock:
                                self.insert_pids.append(pid)
                            conn.sendall(_insert_response(cold, 1))
                        elif "PG_BACKEND_PID" in upper:
                            if cold:
                                conn.sendall(_pid_query_response(pid, b"I"))
                            else:
                                conn.sendall(_warm_pid_response(pid, b"I"))
                        else:
                            conn.sendall(_select_one_response(cold))
            except (EOFError, OSError):
                return

    def stop(self) -> None:
        """Stop accepting and close listener."""
        self._stop.set()
        self._sock.close()
        self._accept_thread.join(timeout=2.0)


def _make_sync_pool(port: int, *, max_size: int = 1) -> SyncPostgresPool:
    """Create a loopback SyncPostgresPool for pool-cursor tests."""
    return SyncPostgresPool(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=port,
        sslmode="disable",
        channel_binding="disable",
        min_size=1,
        max_size=max_size,
        acquire_timeout=2.0,
        reset_timeout=2.0,
        close_timeout=2.0,
    )


def _make_async_pool(port: int, *, max_size: int = 1) -> AsyncPostgresPool:
    """Create a loopback AsyncPostgresPool for pool-cursor tests."""
    return AsyncPostgresPool(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=port,
        sslmode="disable",
        channel_binding="disable",
        min_size=1,
        max_size=max_size,
        acquire_timeout=2.0,
        reset_timeout=2.0,
        close_timeout=2.0,
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pool_cursor_lazy_begin_rejected_without_checkout(mode: str) -> None:
    """Verify pool.cursor(lazy_begin=True) raises InterfaceError without opening any TCP connection."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    try:
        pool = _make_sync_pool(port) if mode == "sync" else _make_async_pool(port)
        cursor = pool.cursor()
        assert cursor.rowcount == -1
        assert cursor.description is None
        with pytest.raises(InterfaceError, match="a pool cursor has no session; lazy_begin needs a client cursor"):
            pool.cursor(lazy_begin=True)
        with pytest.raises(BlockingIOError):
            listener.accept()
    finally:
        listener.close()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pool_cursor_oneshot_select(mode: str) -> None:
    """Verify pool.cursor().execute('SELECT 1').fetchall() returns [(1,)] for both pool classes."""
    srv = _ScriptedCursorPoolServer()
    try:
        if mode == "sync":
            pool = _make_sync_pool(srv.port)
            cur = pool.cursor()
            assert cur.execute("SELECT 1") is cur
            assert cur.fetchall() == [(1,)]
            assert cur.rowcount == 1
            assert cur.description is not None
            assert len(cur.description) == 1
            cur.close()
            pool.close()
        else:

            async def _run() -> None:
                pool = _make_async_pool(srv.port)
                cur = pool.cursor()
                assert await cur.execute("SELECT 1") is cur
                assert await cur.fetchall() == [(1,)]
                assert cur.rowcount == 1
                assert cur.description is not None
                assert len(cur.description) == 1
                await cur.close()
                await pool.close()

            asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize("pool_cls", [SyncPostgresPool, AsyncPostgresPool])
def test_pool_does_not_expose_query_method(
    pool_cls: type[SyncPostgresPool] | type[AsyncPostgresPool],
) -> None:
    """Verify neither pool class nor its instances expose the removed query() method."""
    assert not hasattr(pool_cls, "query")
    pool = pool_cls(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=1,
        sslmode="disable",
        channel_binding="disable",
        min_size=0,
        max_size=1,
    )
    assert not hasattr(pool, "query")


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pool_cursor_connection_returned_100_sequential(mode: str) -> None:
    """Verify 100 sequential pool cursor executes on a pool of size 1 all succeed and reuse one connection."""
    srv = _ScriptedCursorPoolServer()
    try:
        if mode == "sync":
            pool = _make_sync_pool(srv.port, max_size=1)
            for _ in range(100):
                cur = pool.cursor()
                assert cur.execute("SELECT 1").fetchall() == [(1,)]
                cur.close()
            assert srv.accept_count == 1
            pool.close()
        else:

            async def _run() -> None:
                pool = _make_async_pool(srv.port, max_size=1)
                for _ in range(100):
                    cur = pool.cursor()
                    await cur.execute("SELECT 1")
                    assert await cur.fetchall() == [(1,)]
                    await cur.close()
                assert srv.accept_count == 1
                await pool.close()

            asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pool_cursor_executemany_one_connection(mode: str) -> None:
    """Verify executemany acquires one pooled connection for all parameter sets and returns it."""
    srv = _ScriptedCursorPoolServer()
    try:
        sql = "INSERT INTO pid_log (pid, val) VALUES (pg_backend_pid(), $1)"
        params = [(1,), (2,), (3,)]
        if mode == "sync":
            pool = _make_sync_pool(srv.port, max_size=2)
            cur = pool.cursor()
            assert cur.executemany(sql, params) is cur
            assert cur.rowcount == 3
            assert cur.description is None
            assert len(srv.insert_pids) == 3
            assert len(set(srv.insert_pids)) == 1
            assert pool.cursor().execute("SELECT 1").fetchall() == [(1,)]
            cur.close()
            pool.close()
        else:

            async def _run() -> None:
                pool = _make_async_pool(srv.port, max_size=2)
                cur = pool.cursor()
                assert await cur.executemany(sql, params) is cur
                assert cur.rowcount == 3
                assert cur.description is None
                assert len(srv.insert_pids) == 3
                assert len(set(srv.insert_pids)) == 1
                follow = pool.cursor()
                await follow.execute("SELECT 1")
                assert await follow.fetchall() == [(1,)]
                await cur.close()
                await pool.close()

            asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("error_sql", ["SELECT FAIL_SERVER_ERROR", "SELECT FAIL_DROP_CONN"])
def test_pool_cursor_error_evicts_and_recovers(mode: str, error_sql: str) -> None:
    """Verify a failing statement on a pool cursor leaves the pool healthy for a subsequent SELECT."""
    srv = _ScriptedCursorPoolServer()
    try:
        if mode == "sync":
            pool = _make_sync_pool(srv.port, max_size=1)
            cur = pool.cursor()
            with pytest.raises((DatabaseError, OperationalError)):
                cur.execute(error_sql)
            assert cur.execute("SELECT 1").fetchall() == [(1,)]
            cur.close()
            pool.close()
        else:

            async def _run() -> None:
                pool = _make_async_pool(srv.port, max_size=1)
                cur = pool.cursor()
                with pytest.raises((DatabaseError, OperationalError)):
                    await cur.execute(error_sql)
                await cur.execute("SELECT 1")
                assert await cur.fetchall() == [(1,)]
                await cur.close()
                await pool.close()

            asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pool_cursor_closed_pool_raises_interface_error(mode: str) -> None:
    """Verify execute and executemany on a cursor whose pool is closed raise InterfaceError."""
    srv = _ScriptedCursorPoolServer()
    try:
        if mode == "sync":
            pool = _make_sync_pool(srv.port)
            cur = pool.cursor()
            pool.close()
            with pytest.raises(InterfaceError):
                cur.execute("SELECT 1")
            with pytest.raises(InterfaceError):
                cur.executemany("INSERT INTO t VALUES ($1)", [(1,)])
            cur.close()
        else:

            async def _run() -> None:
                pool = _make_async_pool(srv.port)
                cur = pool.cursor()
                await pool.close()
                with pytest.raises(InterfaceError):
                    await cur.execute("SELECT 1")
                with pytest.raises(InterfaceError):
                    await cur.executemany("INSERT INTO t VALUES ($1)", [(1,)])
                await cur.close()

            asyncio.run(_run())
    finally:
        srv.stop()


def _reply_pool_per_frame_extended(
    frames: Sequence[tuple[bytes, bytes]],
    param_oids: Sequence[int],
    cols: Sequence[tuple[str, int]],
    rows: Sequence[Sequence[str | None]],
    command_tag: str,
    tx_status: bytes = b"I",
) -> bytes:
    """Answer pool extended frames per-frame, matching Bind result format codes for cold and cached runs."""
    result_fmts: list[int] = []
    for tag, body in frames:
        if tag == b"B":
            _, _, result_fmts = _bind_frame_stmt_params_and_formats(body)
            break
    if cols:
        return _reply_extended_query(frames, param_oids, cols, rows, command_tag, tx_status, result_fmts)
    pdesc = _frame(
        b"t",
        struct.pack("!h", len(param_oids)) + b"".join(struct.pack("!I", oid) for oid in param_oids),
    )
    parts: list[bytes] = []
    for tag, body in frames:
        if tag == b"P":
            parts.append(_frame(b"1", b""))
        elif tag == b"D":
            if body[:1] == b"S":
                parts.append(pdesc)
                parts.append(_frame(b"n", b""))
            else:
                parts.append(_frame(b"n", b""))
        elif tag == b"B":
            parts.append(_frame(b"2", b""))
        elif tag == b"E":
            parts.append(_frame(b"C", command_tag.encode("ascii") + b"\x00"))
        elif tag == b"C":
            parts.append(_frame(b"3", b""))
        elif tag == b"S":
            parts.append(_frame(b"Z", tx_status))
    return b"".join(parts)


class _ScriptedSingleCallPoolServer:
    """Loopback server covering the sync single-call matrix on SyncPostgresPool."""

    def __init__(self, *, sleep_delay: float = 0.0) -> None:
        """Bind ephemeral loopback listener and launch background accept loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.port: int = self._sock.getsockname()[1]
        self.accept_count = 0
        self._sleep_delay = sleep_delay
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        """Accept incoming pooled sockets until stopped."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with self._lock:
                self.accept_count += 1
                pid = 7000 + self.accept_count
            worker = threading.Thread(target=self._serve_conn, args=(conn, pid), daemon=True)
            self._workers.append(worker)
            worker.start()

    def _serve_conn(self, conn: socket.socket, pid: int) -> None:
        """Answer single-call matrix queries per-frame on one pooled connection."""
        prepared: dict[str, str] = {}
        with conn:
            try:
                _read_startup(conn)
                conn.sendall(_auth_ok_and_ready(pid))
                while not self._stop.is_set():
                    frames = _read_command_group(conn)
                    last_tag, last_body = frames[-1]
                    if last_tag == b"X":
                        return
                    if last_tag == b"Q":
                        sql = last_body.rstrip(b"\x00").decode("utf-8", errors="replace").strip().upper()
                        if sql == "DISCARD ALL":
                            prepared.clear()
                            conn.sendall(_frame(b"C", b"DISCARD ALL\x00") + _frame(b"Z", b"I"))
                        else:
                            conn.sendall(_frame(b"C", b"OK\x00") + _frame(b"Z", b"I"))
                        continue
                    sql_text = ""
                    for tag, body in frames:
                        if tag == b"P":
                            stmt_name, sql_text = _parse_frame_stmt_and_sql(body)
                            prepared[stmt_name] = sql_text
                        elif tag == b"B" and not sql_text:
                            stmt_name, _, _ = _bind_frame_stmt_params_and_formats(body)
                            sql_text = prepared.get(stmt_name, "")
                    if "PG_SLEEP" in sql_text.upper():
                        time.sleep(self._sleep_delay)
                        return
                    if "FAIL_DROP_CONN" in sql_text.upper():
                        return
                    if "FAIL_SERVER_ERROR" in sql_text.upper():
                        conn.sendall(_error_response(b"22012", b"division by zero"))
                    elif "FAIL_UNIQUE" in sql_text.upper():
                        conn.sendall(_error_response(b"23505", b"duplicate key value violates unique constraint"))
                    elif sql_text == "SELECT 1 AS a":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("a", 23),), [("1",)], "SELECT 1"))
                    elif sql_text == "SELECT 1 AS a, 'x' AS b":
                        conn.sendall(
                            _reply_pool_per_frame_extended(frames, (), (("a", 23), ("b", 25)), [("1", "x")], "SELECT 1")
                        )
                    elif sql_text == "SELECT 1 AS a WHERE false":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("a", 23),), [], "SELECT 0"))
                    elif sql_text == "CREATE TABLE t (id int)":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (), [], "CREATE TABLE"))
                    elif sql_text == "UPDATE t SET v = 1":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (), [], "UPDATE 2"))
                    elif sql_text == "INSERT INTO t VALUES (1) RETURNING id":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("id", 23),), [("42",)], "INSERT 0 1"))
                    elif sql_text == "SELECT NULL":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("val", 25),), [(None,)], "SELECT 1"))
                    elif sql_text == "SELECT $1::int":
                        conn.sendall(_reply_pool_per_frame_extended(frames, (23,), (("n", 23),), [("7",)], "SELECT 1"))
                    elif sql_text.startswith("SELECT ") and sql_text[7:].isdigit():
                        val = sql_text[7:]
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("v", 23),), [(val,)], "SELECT 1"))
                    else:
                        conn.sendall(_reply_pool_per_frame_extended(frames, (), (("v", 23),), [("1",)], "SELECT 1"))
            except (EOFError, OSError):
                return

    def stop(self) -> None:
        """Stop accepting and close listener."""
        self._stop.set()
        self._sock.close()
        self._accept_thread.join(timeout=2.0)


def test_sync_single_call_pool_matrix_shapes_and_parameters() -> None:
    """Verify select equality, dicts, empty set, DDL, DML, returning, NULL, named params, and empty params on SyncPostgresPool."""
    srv = _ScriptedSingleCallPoolServer()
    try:
        pool = _make_sync_pool(srv.port, max_size=1)
        try:
            sql = "SELECT 1 AS a, 'x' AS b"
            cur = pool.cursor()
            cur.execute(sql)
            cur_rows = cur.fetchall()
            assert cur.description is not None
            cur_names = tuple(col[0] for col in cur.description)
            cur.close()

            rows, names = pool.fetch_rows(sql)
            assert rows == cur_rows
            assert names == cur_names
            assert pool.fetch_dicts(sql) == [{"a": 1, "b": "x"}]

            assert pool.fetch_rows("SELECT 1 AS a WHERE false") == ([], ("a",))
            assert pool.fetch_dicts("SELECT 1 AS a WHERE false") == []
            with pytest.raises(NotFoundError, match="Query returned no rows"):
                pool.fetch_value("SELECT 1 AS a WHERE false")

            assert pool.fetch_rows("CREATE TABLE t (id int)") == ([], ())
            assert pool.fetch_dicts("CREATE TABLE t (id int)") == []
            with pytest.raises(NotFoundError, match="Query returned no rows"):
                pool.fetch_value("CREATE TABLE t (id int)")
            assert pool.execute_dml("CREATE TABLE t (id int)") == (0, None)

            assert pool.execute_dml("UPDATE t SET v = 1") == (2, None)
            assert pool.fetch_rows("INSERT INTO t VALUES (1) RETURNING id") == ([(42,)], ("id",))
            assert pool.execute_dml("INSERT INTO t VALUES (1) RETURNING id") == (1, None)

            assert pool.fetch_value("SELECT NULL") is None
            assert pool.fetch_value("SELECT %(n)s::int", {"n": 7}) == 7
            assert pool.fetch_value("SELECT 1", ()) == 1
        finally:
            pool.close()
    finally:
        srv.stop()


def test_sync_single_call_pool_validation_timeout_and_server_errors() -> None:
    """Verify bad parameters, unsupported values, unique violations, and timeouts on SyncPostgresPool."""
    srv = _ScriptedSingleCallPoolServer(sleep_delay=0.3)
    try:
        pool = _make_sync_pool(srv.port, max_size=1)
        try:
            with pytest.raises(ProgrammingError):
                pool.fetch_value("SELECT 1", (1,))
            with pytest.raises(InterfaceError):
                pool.fetch_value("SELECT %s", (PG_UNCHANGED_TOAST,))
            with pytest.raises(IntegrityError) as exc_info:
                pool.execute_dml("INSERT INTO t VALUES (1) /* FAIL_UNIQUE */")
            assert exc_info.value.sqlstate == "23505"

            with pytest.raises(OperationalError) as single_exc:
                pool.fetch_value("SELECT pg_sleep(1)", timeout=0.05)
            cur = pool.cursor()
            with pytest.raises(OperationalError) as cursor_exc:
                cur.execute("SELECT pg_sleep(1)", timeout=0.05)
            cur.close()
            assert type(single_exc.value) is type(cursor_exc.value)
        finally:
            pool.close()
    finally:
        srv.stop()


def test_sync_single_call_pool_returns_connection_100_sequential() -> None:
    """Verify 100 sequential pool.fetch_value calls on a pool of size 1 all succeed and reuse one connection."""
    srv = _ScriptedSingleCallPoolServer()
    try:
        pool = _make_sync_pool(srv.port, max_size=1)
        try:
            for _ in range(100):
                assert pool.fetch_value("SELECT 1") == 1
            assert srv.accept_count == 1
        finally:
            pool.close()
    finally:
        srv.stop()


@pytest.mark.parametrize("error_sql", ["SELECT FAIL_SERVER_ERROR", "SELECT FAIL_DROP_CONN"])
def test_sync_single_call_pool_error_recovers(error_sql: str) -> None:
    """Verify a failing single-call statement leaves the pool healthy for a subsequent fetch_value."""
    srv = _ScriptedSingleCallPoolServer()
    try:
        pool = _make_sync_pool(srv.port, max_size=1)
        try:
            with pytest.raises((DatabaseError, OperationalError)):
                pool.fetch_value(error_sql)
            assert pool.fetch_value("SELECT 1") == 1
        finally:
            pool.close()
    finally:
        srv.stop()


@pytest.mark.parametrize("method_name", ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"])
def test_sync_single_call_pool_closed_raises_interface_error(method_name: str) -> None:
    """Verify calling any single-call method on a closed SyncPostgresPool raises InterfaceError."""
    srv = _ScriptedSingleCallPoolServer()
    try:
        pool = _make_sync_pool(srv.port, max_size=1)
        pool.close()
        method = getattr(pool, method_name)
        with pytest.raises(InterfaceError, match="cannot run a statement on a closed pool"):
            method("SELECT 1")
    finally:
        srv.stop()


class _CountingAwaitable:
    """Wrapper whose __await__ counts how many times an awaitable is awaited."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.await_count = 0

    def __await__(self) -> Any:
        self.await_count += 1
        return self._inner.__await__()


def test_async_pool_single_call_one_awaitable_and_matrix_shapes() -> None:
    """Verify AsyncPostgresPool single-call methods return one non-coroutine awaitable and match the t11 matrix."""
    srv = _ScriptedSingleCallPoolServer()
    try:

        async def _run() -> None:
            pool = _make_async_pool(srv.port, max_size=1)
            try:
                aw = pool.fetch_rows("SELECT 1 AS a")
                assert not inspect.iscoroutine(aw)
                counted = _CountingAwaitable(aw)
                assert await counted == ([(1,)], ("a",))
                assert counted.await_count == 1

                sql = "SELECT 1 AS a, 'x' AS b"
                cur = pool.cursor()
                await cur.execute(sql)
                cur_rows = await cur.fetchall()
                assert cur.description is not None
                cur_names = tuple(col[0] for col in cur.description)
                await cur.close()

                rows, names = await pool.fetch_rows(sql)
                assert rows == cur_rows
                assert names == cur_names
                assert await pool.fetch_dicts(sql) == [{"a": 1, "b": "x"}]

                assert await pool.fetch_rows("SELECT 1 AS a WHERE false") == ([], ("a",))
                assert await pool.fetch_dicts("SELECT 1 AS a WHERE false") == []
                with pytest.raises(NotFoundError, match="Query returned no rows"):
                    await pool.fetch_value("SELECT 1 AS a WHERE false")

                assert await pool.fetch_rows("CREATE TABLE t (id int)") == ([], ())
                assert await pool.fetch_dicts("CREATE TABLE t (id int)") == []
                with pytest.raises(NotFoundError, match="Query returned no rows"):
                    await pool.fetch_value("CREATE TABLE t (id int)")
                assert await pool.execute_dml("CREATE TABLE t (id int)") == (0, None)

                assert await pool.execute_dml("UPDATE t SET v = 1") == (2, None)
                assert await pool.fetch_rows("INSERT INTO t VALUES (1) RETURNING id") == ([(42,)], ("id",))
                assert await pool.execute_dml("INSERT INTO t VALUES (1) RETURNING id") == (1, None)

                assert await pool.fetch_value("SELECT NULL") is None
                assert await pool.fetch_value("SELECT %(n)s::int", {"n": 7}) == 7
                assert await pool.fetch_value("SELECT 1", ()) == 1
            finally:
                await pool.close()

        asyncio.run(_run())
    finally:
        srv.stop()


def test_async_pool_single_call_validation_timeout_and_server_errors() -> None:
    """Verify call-time validation errors, IntegrityError on await, and timeout matching AsyncPostgresCursor on AsyncPostgresPool."""
    srv = _ScriptedSingleCallPoolServer(sleep_delay=0.3)
    try:

        async def _run() -> None:
            pool = _make_async_pool(srv.port, max_size=1)
            try:
                with pytest.raises(ProgrammingError):
                    pool.fetch_rows("SELECT 1", (1,))
                with pytest.raises(InterfaceError):
                    pool.fetch_value("SELECT %s", (PG_UNCHANGED_TOAST,))
                with pytest.raises(IntegrityError) as exc_info:
                    await pool.execute_dml("INSERT INTO t VALUES (1) /* FAIL_UNIQUE */")
                assert exc_info.value.sqlstate == "23505"

                with pytest.raises(OperationalError) as single_exc:
                    await pool.fetch_value("SELECT pg_sleep(1)", timeout=0.05)
                cur = pool.cursor()
                with pytest.raises(OperationalError) as cursor_exc:
                    await cur.execute("SELECT pg_sleep(1)", timeout=0.05)
                await cur.close()
                assert type(single_exc.value) is type(cursor_exc.value)
            finally:
                await pool.close()

        asyncio.run(_run())
    finally:
        srv.stop()


def test_async_pool_single_call_concurrent_gather() -> None:
    """Verify asyncio.gather of five pool.fetch_value calls returns all five values."""
    srv = _ScriptedSingleCallPoolServer()
    try:

        async def _run() -> None:
            pool = _make_async_pool(srv.port, max_size=5)
            try:
                values = await asyncio.gather(*(pool.fetch_value(f"SELECT {i}") for i in range(1, 6)))
                assert values == [1, 2, 3, 4, 5]
            finally:
                await pool.close()

        asyncio.run(_run())
    finally:
        srv.stop()


@pytest.mark.parametrize("method_name", ["fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"])
def test_async_pool_single_call_closed_raises_interface_error(method_name: str) -> None:
    """Verify calling any single-call method on a closed AsyncPostgresPool raises InterfaceError."""
    srv = _ScriptedSingleCallPoolServer()
    try:

        async def _run() -> None:
            pool = _make_async_pool(srv.port, max_size=1)
            await pool.close()
            method = getattr(pool, method_name)
            with pytest.raises(InterfaceError, match="cannot run a statement on a closed pool"):
                method("SELECT 1")

        asyncio.run(_run())
    finally:
        srv.stop()
