"""Unit and loopback tests for SyncPostgresPool and AsyncPostgresPool session leases (`ch15:t01`)."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
from typing import Any

import pytest

from vertebra._postgres import AsyncPostgresPool, SyncPostgresPool
from vertebra._vertebra import InterfaceError, OperationalError, PoolExhaustedError


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
    col_body = struct.pack("!h", 1) + b"pid\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, 1)
    row_body = struct.pack("!hii", 1, 4, pid)
    return b"".join(
        (
            _frame(b"2", b""),
            _frame(b"T", col_body),
            _frame(b"D", row_body),
            _frame(b"C", b"SELECT 1\x00"),
            _frame(b"3", b""),
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
        await asyncio.sleep(0.02)
        waiter1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter1

        waiter2 = asyncio.ensure_future(pool.acquire())
        await asyncio.sleep(0.01)
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
