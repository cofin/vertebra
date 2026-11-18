"""Unit and loopback wire tests for SyncPostgresClient and AsyncPostgresClient cancellation (`ch16:t03`)."""

from __future__ import annotations

import asyncio
import gc
import socket
import struct
import threading
from typing import Any

import pytest

from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
)
from vertebra._vertebra import InterfaceError, QueryCancelledError
from vertebra.db.adapters.postgres.types import PostgresPipelineOperation

_CANCEL_REQUEST_CODE = 80_877_102


def _frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _auth_ok_and_ready(pid: int, secret: int) -> bytes:
    """Build AuthenticationOk + baseline ParameterStatus + BackendKeyData + ReadyForQuery('I')."""
    return b"".join(
        (
            _frame(b"R", struct.pack("!i", 0)),
            _frame(b"S", b"application_name\x00vertebra\x00"),
            _frame(b"S", b"client_encoding\x00UTF8\x00"),
            _frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _frame(b"S", b"TimeZone\x00UTC\x00"),
            _frame(b"K", struct.pack("!ii", pid, secret)),
            _frame(b"Z", b"I"),
        )
    )


def _error_57014(tx_status: bytes) -> bytes:
    """Build ErrorResponse(57014) + ReadyForQuery(tx_status)."""
    body = b"SERROR\x00VERROR\x00C57014\x00Mcanceling statement due to user request\x00\x00"
    return _frame(b"E", body) + _frame(b"Z", tx_status)


def _select_pid_response(pid: int, tx_status: bytes, *, warm: bool) -> bytes:
    """Build single-row int4 response returning pid."""
    fmt = 1 if warm else 0
    col = struct.pack("!h", 1) + b"pid\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, fmt)
    if warm:
        row = struct.pack("!hii", 1, 4, pid)
        return b"".join(
            (
                _frame(b"2", b""),
                _frame(b"T", col),
                _frame(b"D", row),
                _frame(b"C", b"SELECT 1\x00"),
                _frame(b"3", b""),
                _frame(b"Z", tx_status),
            )
        )
    pid_s = str(pid).encode("ascii")
    row = struct.pack("!hi", 1, len(pid_s)) + pid_s
    return b"".join(
        (
            _frame(b"1", b""),
            _frame(b"t", struct.pack("!h", 0)),
            _frame(b"T", col),
            _frame(b"2", b""),
            _frame(b"T", col),
            _frame(b"D", row),
            _frame(b"C", b"SELECT 1\x00"),
            _frame(b"3", b""),
            _frame(b"Z", tx_status),
        )
    )


def _read_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_group(conn: socket.socket) -> list[tuple[bytes, bytes]]:
    """Read frontend frames until Sync ('S'), Query ('Q'), or Terminate ('X')."""
    out: list[tuple[bytes, bytes]] = []
    while True:
        hdr = _read_exact(conn, 5)
        tag = hdr[:1]
        (length,) = struct.unpack("!i", hdr[1:5])
        body = _read_exact(conn, length - 4)
        out.append((tag, body))
        if tag in (b"S", b"Q", b"X"):
            return out


class _CancelAwareServer:
    """Multi-socket loopback PostgreSQL server pairing CancelRequest packets with slow queries."""

    def __init__(self) -> None:
        """Bind ephemeral loopback listener and launch accept loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.1)
        self.port: int = self._sock.getsockname()[1]
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._next_pid = 8000
        self._live_connections = 0
        self._error_57014_count = 0
        self._slow_events: dict[int, threading.Event] = {}
        self._slow_async_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
        self._live_async_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
        self._error_async_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
        self.slow_started = threading.Event()
        self.cancel_packets: list[tuple[int, int, bytes]] = []
        self.simple_queries: list[tuple[int, str]] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _notify_waiters_locked(self) -> None:
        """Wake synchronous Condition waiters and matching event-loop Event waiters."""
        self._cond.notify_all()
        if self.slow_started.is_set() and self._slow_async_waiters:
            for loop, ev in self._slow_async_waiters:
                loop.call_soon_threadsafe(ev.set)
            self._slow_async_waiters.clear()
        if self._live_async_waiters:
            remaining_live: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
            for loop, expected, ev in self._live_async_waiters:
                if self._live_connections == expected:
                    loop.call_soon_threadsafe(ev.set)
                else:
                    remaining_live.append((loop, expected, ev))
            self._live_async_waiters = remaining_live
        if self._error_async_waiters:
            remaining_err: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
            for loop, target_count, ev in self._error_async_waiters:
                if self._error_57014_count >= target_count:
                    loop.call_soon_threadsafe(ev.set)
                else:
                    remaining_err.append((loop, target_count, ev))
            self._error_async_waiters = remaining_err

    async def wait_slow_started_async(self, *, timeout: float = 2.0) -> bool:
        """Await server entry into a slow pg_sleep query without polling."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._lock:
            if self.slow_started.is_set():
                return True
            self._slow_async_waiters.append((loop, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return self.slow_started.is_set()

    async def wait_for_live_connections_async(self, expected: int, *, timeout: float = 2.0) -> bool:
        """Await server live backend connection count reaching expected without polling."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._lock:
            if self._live_connections == expected:
                return True
            self._live_async_waiters.append((loop, expected, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._lock:
                return self._live_connections == expected

    async def wait_for_error_count_async(self, count: int, *, timeout: float = 2.0) -> bool:
        """Await server sending at least count ErrorResponse(57014) frames without polling."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._lock:
            if self._error_57014_count >= count:
                return True
            self._error_async_waiters.append((loop, count, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._lock:
                return self._error_57014_count >= count

    def _accept_loop(self) -> None:
        """Accept both backend sessions and 16-byte CancelRequest connections."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn: socket.socket) -> None:
        """Dispatch CancelRequest packet or interactive backend session."""
        with conn:
            conn.settimeout(3.0)
            try:
                hdr4 = _read_exact(conn, 4)
                (pkt_len,) = struct.unpack("!i", hdr4)
                rest = _read_exact(conn, pkt_len - 4)
                if pkt_len >= 16:
                    (code,) = struct.unpack("!i", rest[:4])
                    if code == _CANCEL_REQUEST_CODE:
                        pid, key = struct.unpack("!ii", rest[4:12])
                        extra = b""
                        try:
                            conn.settimeout(0.05)
                            extra = conn.recv(64)
                        except TimeoutError:
                            extra = b""
                        with self._lock:
                            self.cancel_packets.append((pid, key, hdr4 + rest + extra))
                            ev = self._slow_events.get(pid)
                        if ev is not None:
                            ev.set()
                        return

                with self._lock:
                    self._next_pid += 1
                    pid = self._next_pid
                    secret = 50_000 + pid
                    self._live_connections += 1
                    self._notify_waiters_locked()
                tx = b"I"
                conn.sendall(_auth_ok_and_ready(pid, secret))

                try:
                    while not self._stop.is_set():
                        grp = _read_group(conn)
                        last_tag, last_body = grp[-1]
                        if last_tag == b"X":
                            return
                        if last_tag == b"Q":
                            sql = last_body.rstrip(b"\x00").decode("utf-8", errors="replace").strip()
                            with self._lock:
                                self.simple_queries.append((pid, sql))
                            up = sql.upper()
                            if up == "BEGIN":
                                tx = b"T"
                                conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", tx))
                            elif up == "COMMIT":
                                tx = b"I"
                                conn.sendall(_frame(b"C", b"COMMIT\x00") + _frame(b"Z", tx))
                            elif up == "ROLLBACK":
                                tx = b"I"
                                conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx))
                            elif up.startswith("SAVEPOINT "):
                                tx = b"T"
                                conn.sendall(_frame(b"C", b"SAVEPOINT\x00") + _frame(b"Z", tx))
                            elif up.startswith("ROLLBACK TO SAVEPOINT "):
                                tx = b"T"
                                conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx))
                            elif up.startswith("RELEASE SAVEPOINT "):
                                tx = b"T"
                                conn.sendall(_frame(b"C", b"RELEASE\x00") + _frame(b"Z", tx))
                            elif up == "DISCARD ALL":
                                tx = b"I"
                                conn.sendall(_frame(b"C", b"DISCARD ALL\x00") + _frame(b"Z", tx))
                            else:
                                conn.sendall(_frame(b"C", b"OK\x00") + _frame(b"Z", tx))
                            continue

                        combined = b" ".join(b for t, b in grp if t == b"P")
                        if b"pg_sleep" in combined:
                            cancel_ev = threading.Event()
                            with self._lock:
                                self._slow_events[pid] = cancel_ev
                                self.slow_started.set()
                                self._notify_waiters_locked()
                            cancelled = cancel_ev.wait(timeout=3.0)
                            with self._lock:
                                self._slow_events.pop(pid, None)
                            if cancelled:
                                err_tx = b"E" if tx in (b"T", b"E") else b"I"
                                tx = err_tx
                                conn.sendall(_error_57014(tx))
                                with self._lock:
                                    self._error_57014_count += 1
                                    self._notify_waiters_locked()
                            else:
                                conn.sendall(_select_pid_response(pid, tx, warm=False))
                        else:
                            warm = not any(t == b"P" for t, _ in grp)
                            conn.sendall(_select_pid_response(pid, tx, warm=warm))
                finally:
                    with self._lock:
                        self._live_connections -= 1
                        self._notify_waiters_locked()
            except (EOFError, OSError):
                return

    def stop(self) -> None:
        """Stop accepting sockets and close listener."""
        self._stop.set()
        self._sock.close()
        self._accept_thread.join(timeout=2.0)


def test_sync_client_worker_thread_cancel_sends_16_byte_packet_and_raises_57014() -> None:
    """Verify worker-thread SyncPostgresClient.cancel() sends CancelRequest and raises QueryCancelledError."""
    srv = _CancelAwareServer()
    try:
        client = SyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        client.cancel()
        assert srv.cancel_packets == []

        caught: list[BaseException] = []

        def _run_slow() -> None:
            try:
                client.query("SELECT pg_sleep(10)")
            except BaseException as exc:
                caught.append(exc)

        worker = threading.Thread(target=_run_slow)
        worker.start()
        assert srv.slow_started.wait(timeout=2.0)
        client.cancel()
        worker.join(timeout=2.0)

        assert len(caught) == 1
        err = caught[0]
        assert isinstance(err, QueryCancelledError)
        assert err.sqlstate == "57014"
        assert client.closed

        assert len(srv.cancel_packets) == 1
        pid, key, raw_pkt = srv.cancel_packets[0]
        assert pid == 8001
        assert key == 58001
        assert len(raw_pkt) == 16
    finally:
        srv.stop()


def test_async_client_explicit_cancel_pre_io_cancel_and_post_completion_cancel() -> None:
    """Verify AsyncPostgresClient.cancel(), idle no-op, and pre-I/O / post-result Future.cancel() fencing."""
    srv = _CancelAwareServer()

    async def _run() -> None:
        client = AsyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        await client.cancel()
        await client.cancel()
        assert srv.cancel_packets == []

        unconnected = AsyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        raw_fut: asyncio.Future[Any] = next(unconnected.query("SELECT 1").__await__())
        assert raw_fut.cancel()
        await asyncio.sleep(0)
        assert srv.cancel_packets == []

        ok_task = asyncio.ensure_future(client.query("SELECT pg_backend_pid() AS pid"))
        res = await ok_task
        assert res.rows[0][0] == 8001
        assert not ok_task.cancel()
        await client.cancel()
        assert srv.cancel_packets == []

        srv.slow_started.clear()
        slow_task = asyncio.ensure_future(client.query("SELECT pg_sleep(10)"))
        assert await srv.wait_slow_started_async(timeout=2.0)
        await client.cancel()
        with pytest.raises(QueryCancelledError) as exc_info:
            await slow_task
        assert exc_info.value.sqlstate == "57014"
        assert client.closed
        await client.cancel()
        assert len(srv.cancel_packets) == 1

    try:
        asyncio.run(_run())
    finally:
        srv.stop()


def test_asyncio_wait_for_timeout_on_max_size_1_pool_retires_backend_and_restores_capacity() -> None:
    """Verify asyncio.wait_for timeout cancels query, retires backend PID, and preserves pool capacity on close/GC."""
    srv = _CancelAwareServer()

    async def _run() -> None:
        pool = AsyncPostgresPool(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
            acquire_timeout=2.0,
        )
        try:
            c1 = await pool.acquire()
            pid1 = (await c1.query("SELECT pg_backend_pid() AS pid")).rows[0][0]
            srv.slow_started.clear()
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(c1.query("SELECT pg_sleep(10)"), timeout=0.05)
            await c1.close()

            c2 = await asyncio.wait_for(pool.acquire(), timeout=1.5)
            pid2 = (await c2.query("SELECT pg_backend_pid() AS pid")).rows[0][0]
            assert pid2 != pid1

            srv.slow_started.clear()
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(c2.query("SELECT pg_sleep(10)"), timeout=0.05)
            del c2
            gc.collect()
            assert await srv.wait_for_live_connections_async(0, timeout=2.0)

            c3 = await asyncio.wait_for(pool.acquire(), timeout=1.5)
            pid3 = (await c3.query("SELECT pg_backend_pid() AS pid")).rows[0][0]
            assert pid3 not in (pid1, pid2)
            await c3.close()
        finally:
            await pool.close()

    try:
        asyncio.run(_run())
    finally:
        srv.stop()


def test_ambient_begin_cancel_on_query_and_pipeline_blocks_until_rollback() -> None:
    """Verify ambient tx query and pipeline_results cancellation block ordinary queries until rollback()."""
    srv = _CancelAwareServer()

    async def _async_ambient_query() -> None:
        client = AsyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        await client.begin()
        srv.slow_started.clear()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.query("SELECT pg_sleep(10)"), timeout=0.05)
        assert await srv.wait_for_error_count_async(1, timeout=2.0)
        await asyncio.sleep(0)
        with pytest.raises(InterfaceError):
            await client.query("SELECT pg_backend_pid() AS pid")
        await client.rollback()
        assert client.closed

    try:
        asyncio.run(_async_ambient_query())

        sync_client = SyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        sync_client.connect()
        sync_client.begin()
        scope_id = sync_client.pipeline_begin()
        srv.slow_started.clear()
        caught: list[Any] = []

        def _slow_pipe() -> None:
            try:
                sync_client.pipeline_results(
                    scope_id,
                    (PostgresPipelineOperation(sql="SELECT pg_sleep(10)", parameters=(), kind="select"),),
                )
            except BaseException as exc:
                caught.append(exc)

        t = threading.Thread(target=_slow_pipe)
        t.start()
        assert srv.slow_started.wait(timeout=2.0)
        sync_client.cancel()
        t.join(timeout=2.0)
        assert len(caught) == 1
        assert isinstance(caught[0], QueryCancelledError)

        sync_client.pipeline_finish(scope_id, False)
        with pytest.raises(InterfaceError):
            sync_client.query("SELECT pg_backend_pid() AS pid")
        sync_client.rollback()
        assert sync_client.closed
    finally:
        srv.stop()
