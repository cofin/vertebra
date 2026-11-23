"""Unit and loopback tests for SyncPostgresPortal and AsyncPostgresPortal (`ch17:t03`)."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import socket
import struct
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

from vertebra._postgres import (
    AsyncPostgresPool,
    AsyncPostgresPortal,
    SyncPostgresClient,
    SyncPostgresPool,
    SyncPostgresPortal,
)
from vertebra._vertebra import InterfaceError, PoolExhaustedError
from vertebra.db.adapters.postgres.types import PostgresQueryResult


def _frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _auth_ok_and_ready(pid: int, secret: int) -> bytes:
    """Build AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery('I')."""
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


def _row_desc_int4(name: str = "n") -> bytes:
    """Build a single-column int4 text RowDescription ('T') frame."""
    payload = struct.pack("!h", 1) + name.encode("utf-8") + b"\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, 0)
    return _frame(b"T", payload)


def _data_row_int4(val: int) -> bytes:
    """Build a single-column int4 text DataRow ('D') frame."""
    raw = str(val).encode("ascii")
    return _frame(b"D", struct.pack("!hi", 1, len(raw)) + raw)


def _error_frame(sqlstate: str, msg: str) -> bytes:
    """Build a PostgreSQL ErrorResponse ('E') frame."""
    body = b"".join(
        (
            b"SERROR\x00VERROR\x00C",
            sqlstate.encode("ascii"),
            b"\x00M",
            msg.encode("utf-8"),
            b"\x00\x00",
        )
    )
    return _frame(b"E", body)


def _read_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a blocking socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_startup_or_cancel(conn: socket.socket) -> bytes:
    """Read a startup packet or 16-byte CancelRequest frame."""
    raw_len = _read_exact(conn, 4)
    (length,) = struct.unpack("!i", raw_len)
    return raw_len + _read_exact(conn, length - 4)


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


def _parse_execute_max_rows(body: bytes) -> tuple[str, int]:
    """Decode portal_name and max_rows from a frontend Execute ('E') body."""
    nul = body.index(b"\x00")
    portal_name = body[:nul].decode("utf-8")
    (max_rows,) = struct.unpack("!i", body[nul + 1 : nul + 5])
    return portal_name, max_rows


def _serve_stateful_portal_session(
    conn: socket.socket,
    pid: int,
    secret: int,
    total_rows: int,
    *,
    on_event: Callable[[str], None] | None = None,
) -> list[str]:
    """Serve stateful BEGIN/open_portal/Execute(max_rows)/Close(P)/COMMIT|ROLLBACK/DISCARD ALL."""
    events: list[str] = []
    next_row = 0
    in_tx = False
    with conn:
        _read_startup_or_cancel(conn)
        conn.sendall(_auth_ok_and_ready(pid, secret))
        while True:
            try:
                grp = _read_group(conn)
            except (EOFError, OSError):
                return events
            tags = [t for t, _ in grp]
            if tags == [b"X"]:
                events.append("TERMINATE")
                return events
            if tags[0] == b"Q":
                sql = grp[0][1].rstrip(b"\x00").decode("utf-8")
                events.append(f"Q:{sql}")
                if sql == "BEGIN":
                    in_tx = True
                    conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", b"T"))
                elif sql in ("COMMIT", "ROLLBACK"):
                    in_tx = False
                    conn.sendall(_frame(b"C", sql.encode("ascii") + b"\x00") + _frame(b"Z", b"I"))
                elif sql == "DISCARD ALL":
                    in_tx = False
                    conn.sendall(_frame(b"C", b"DISCARD ALL\x00") + _frame(b"Z", b"I"))
                elif sql.startswith("SELECT "):
                    tx = b"T" if in_tx else b"I"
                    conn.sendall(
                        _row_desc_int4("v") + _data_row_int4(pid) + _frame(b"C", b"SELECT 1\x00") + _frame(b"Z", tx)
                    )
                else:
                    tx = b"T" if in_tx else b"I"
                    conn.sendall(_frame(b"C", b"OK\x00") + _frame(b"Z", tx))
                if on_event is not None:
                    on_event(f"Q:{sql}")
                continue

            if b"P" in tags and b"B" in tags and b"E" not in tags:
                next_row = 0
                events.append("OPEN_PORTAL")
                conn.sendall(
                    b"".join(
                        (
                            _frame(b"1", b""),
                            _frame(b"t", struct.pack("!h", 0)),
                            _row_desc_int4("n"),
                            _frame(b"2", b""),
                            _row_desc_int4("n"),
                            _frame(b"Z", b"T"),
                        )
                    )
                )
            elif tags == [b"E", b"S"]:
                _, max_rows = _parse_execute_max_rows(grp[0][1])
                events.append(f"EXEC:{max_rows}")
                remaining = total_rows - next_row
                take = remaining if max_rows == 0 else min(remaining, max_rows)
                parts = [_data_row_int4(next_row + offset) for offset in range(take)]
                next_row += take
                if next_row < total_rows:
                    parts.append(_frame(b"s", b""))
                else:
                    parts.append(_frame(b"C", f"SELECT {total_rows}\x00".encode("ascii")))
                parts.append(_frame(b"Z", b"T"))
                conn.sendall(b"".join(parts))
            elif tags == [b"C", b"S"]:
                close_kind = grp[0][1][:1]
                events.append(f"CLOSE:{close_kind.decode('ascii')}")
                tx = b"T" if in_tx else b"I"
                conn.sendall(_frame(b"3", b"") + _frame(b"Z", tx))
            elif b"P" in tags and b"E" in tags:
                events.append("EXT_SELECT")
                tx = b"T" if in_tx else b"I"
                conn.sendall(
                    b"".join(
                        (
                            _frame(b"1", b""),
                            _frame(b"t", struct.pack("!h", 0)),
                            _row_desc_int4("v"),
                            _frame(b"2", b""),
                            _row_desc_int4("v"),
                            _data_row_int4(pid),
                            _frame(b"C", b"SELECT 1\x00"),
                            _frame(b"3", b""),
                            _frame(b"Z", tx),
                        )
                    )
                )


def test_sync_and_async_portal_batch_progression_to_exhaustion() -> None:
    """Verify 5-row portal progression in chunks of 2 to exhaustion and immediate session reuse."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(2)
    port = srv.getsockname()[1]
    sync_events: list[str] = []
    async_events: list[str] = []

    def _run_server() -> None:
        c1, _ = srv.accept()
        sync_events.extend(_serve_stateful_portal_session(c1, 3101, 111, 5))
        c2, _ = srv.accept()
        async_events.extend(_serve_stateful_portal_session(c2, 3102, 222, 5))
        srv.close()

    bg = threading.Thread(target=_run_server, daemon=True)
    bg.start()

    pool = SyncPostgresPool(
        host="127.0.0.1",
        port=port,
        user="postgres",
        database="appdb",
        sslmode="disable",
        channel_binding="disable",
        min_size=0,
        max_size=1,
    )
    client = pool.acquire()
    try:
        portal = client.open_portal("SELECT generate_series(0, 4) AS n", batch_size=2)
        assert isinstance(portal, SyncPostgresPortal)
        assert not portal.exhausted
        assert not portal.closed

        b1 = portal.fetch(2)
        assert isinstance(b1, PostgresQueryResult)
        assert b1.rows == ((0,), (1,))
        assert not portal.exhausted
        assert not portal.closed

        b2 = portal.fetch(2)
        assert b2.rows == ((2,), (3,))
        assert not portal.exhausted
        assert not portal.closed

        b3 = portal.fetch(2)
        assert b3.rows == ((4,),)
        assert portal.exhausted
        assert portal.closed

        with pytest.raises(InterfaceError):
            portal.fetch(1)
        portal.close()

        follow = client.query("SELECT 1")
        assert follow.rows == ((3101,),)
    finally:
        client.close()
        pool.close()

    async def _exercise_async() -> None:
        apool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
        )
        aclient = await apool.acquire()
        try:
            aportal = await aclient.open_portal("SELECT generate_series(0, 4) AS n", batch_size=2)
            assert isinstance(aportal, AsyncPostgresPortal)
            assert not aportal.exhausted
            assert not aportal.closed

            c1 = await aportal.fetch(2)
            assert c1.rows == ((0,), (1,))
            assert not aportal.exhausted

            c2 = await aportal.fetch(2)
            assert c2.rows == ((2,), (3,))
            assert not aportal.exhausted

            c3 = await aportal.fetch(2)
            assert c3.rows == ((4,),)
            assert aportal.exhausted
            assert aportal.closed

            with pytest.raises(InterfaceError):
                await aportal.fetch(1)
            await aportal.close()

            afollow = await aclient.query("SELECT 1")
            assert afollow.rows == ((3102,),)
        finally:
            await aclient.close()
            await apool.close()

    asyncio.run(_exercise_async())
    bg.join(timeout=5.0)
    assert "Q:COMMIT" in sync_events
    assert "Q:COMMIT" in async_events


def test_early_close_is_idempotent_rolls_back_and_blocks_competing_operations() -> None:
    """Verify active portal blocks client.query/client.close/pool.acquire and early close rolls back cleanly."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(2)
    port = srv.getsockname()[1]
    recorded: list[str] = []

    def _run_server() -> None:
        c1, _ = srv.accept()
        recorded.extend(_serve_stateful_portal_session(c1, 4101, 333, 5))
        srv.close()

    bg = threading.Thread(target=_run_server, daemon=True)
    bg.start()

    pool = SyncPostgresPool(
        host="127.0.0.1",
        port=port,
        user="postgres",
        database="appdb",
        sslmode="disable",
        channel_binding="disable",
        min_size=0,
        max_size=1,
        acquire_timeout=0.05,
    )
    client = pool.acquire()
    try:
        portal = client.open_portal("SELECT n FROM t", batch_size=2)
        first = portal.fetch(1)
        assert first.rows == ((0,),)
        assert not portal.closed

        with pytest.raises(InterfaceError):
            client.query("SELECT 1")
        with pytest.raises(InterfaceError):
            client.close()
        with pytest.raises(PoolExhaustedError):
            pool.acquire()

        portal.close()
        assert portal.closed
        assert not portal.exhausted
        portal.close()

        with pytest.raises(InterfaceError):
            portal.fetch(1)
    finally:
        client.close()

    reused = pool.acquire()
    try:
        check = reused.query("SELECT 1")
        assert check.rows == ((4101,),)
    finally:
        reused.close()
        pool.close()

    bg.join(timeout=5.0)
    assert "CLOSE:P" in recorded
    assert "Q:ROLLBACK" in recorded
    assert "Q:DISCARD ALL" in recorded


def test_cancelled_async_portal_fetch_and_gc_drop_retire_and_restore_pool_capacity() -> None:
    """Verify asyncio.wait_for cancel on portal.fetch sends CancelRequest and GC drop releases pool slot."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    port = srv.getsockname()[1]
    saw_cancel = threading.Event()
    saw_rollback = threading.Event()
    rollback_waiter: dict[str, tuple[asyncio.AbstractEventLoop, asyncio.Event]] = {}

    def _on_c2_event(event: str) -> None:
        if event == "Q:ROLLBACK":
            saw_rollback.set()
            entry = rollback_waiter.get("ev")
            if entry is not None:
                entry[0].call_soon_threadsafe(entry[1].set)

    def _run_server() -> None:
        c1, _ = srv.accept()
        with c1:
            _read_startup_or_cancel(c1)
            c1.sendall(_auth_ok_and_ready(5201, 0x12345678))
            g_begin = _read_group(c1)
            assert g_begin[0] == (b"Q", b"BEGIN\x00")
            c1.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", b"T"))

            g_open = _read_group(c1)
            assert b"P" in [t for t, _ in g_open]
            c1.sendall(
                _frame(b"1", b"")
                + _frame(b"t", struct.pack("!h", 0))
                + _row_desc_int4("n")
                + _frame(b"2", b"")
                + _row_desc_int4("n")
                + _frame(b"Z", b"T")
            )

            g_exec = _read_group(c1)
            assert [t for t, _ in g_exec] == [b"E", b"S"]

            cancel_conn, _ = srv.accept()
            with cancel_conn:
                pkt = _read_startup_or_cancel(cancel_conn)
                expected = struct.pack("!iiii", 16, 80877102, 5201, 0x12345678)
                assert pkt == expected
                saw_cancel.set()

            c1.sendall(_error_frame("57014", "canceling portal execute") + _frame(b"Z", b"E"))
            with contextlib.suppress(EOFError, OSError):
                _read_group(c1)

        c2, _ = srv.accept()
        _serve_stateful_portal_session(c2, 5202, 0x22334455, 3, on_event=_on_c2_event)
        srv.close()

    bg = threading.Thread(target=_run_server, daemon=True)
    bg.start()

    async def _run() -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=5.0,
            min_size=0,
            max_size=1,
            acquire_timeout=5.0,
        )
        c1 = await pool.acquire()
        p1 = await c1.open_portal("SELECT pg_sleep(10)")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(p1.fetch(10), timeout=0.05)
        assert saw_cancel.wait(timeout=5.0)
        await c1.close()

        c2 = await pool.acquire()
        p2: AsyncPostgresPortal | None = await c2.open_portal("SELECT n FROM t")
        assert p2 is not None
        chunk = await p2.fetch(1)
        assert chunk.rows == ((0,),)
        rollback_ev = asyncio.Event()
        rollback_waiter["ev"] = (asyncio.get_running_loop(), rollback_ev)
        p2 = None
        gc.collect()
        if not saw_rollback.is_set():
            await asyncio.wait_for(rollback_ev.wait(), timeout=5.0)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                await c2.close()
                break
            except InterfaceError:
                await asyncio.sleep(0)
        assert c2.closed

        c3 = await pool.acquire()
        try:
            pid_res = await c3.query("SELECT 1")
            assert pid_res.rows == ((5202,),)
        finally:
            await c3.close()
            await pool.close()

    asyncio.run(_run())
    bg.join(timeout=5.0)


@pytest.mark.parametrize(
    ("bad_value", "expected_exc"),
    [
        (0, InterfaceError),
        (-3, InterfaceError),
        (2**40, InterfaceError),
        (True, TypeError),
        (False, TypeError),
        ("2", TypeError),
    ],
)
def test_invalid_portal_batch_size_and_max_rows_fail_before_wire_emission(
    bad_value: Any,
    expected_exc: type[Exception],
) -> None:
    """Reject non-int, bool, <= 0, and > i32::MAX batch_size and max_rows synchronously before I/O."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def _run_server() -> None:
        conn, _ = srv.accept()
        _serve_stateful_portal_session(conn, 6101, 999, 2)
        srv.close()

    bg = threading.Thread(target=_run_server, daemon=True)
    bg.start()

    client = SyncPostgresClient(
        host="127.0.0.1",
        port=port,
        user="postgres",
        database="appdb",
        sslmode="disable",
        channel_binding="disable",
    )
    client.connect()
    try:
        with pytest.raises(expected_exc):
            client.open_portal("SELECT 1", batch_size=bad_value)

        portal = client.open_portal("SELECT 1", batch_size=2)
        try:
            portal_any: Any = portal
            with pytest.raises(AttributeError):
                portal_any.extra = 1
            with pytest.raises(expected_exc):
                portal.fetch(bad_value)
        finally:
            portal.close()
    finally:
        client.close()
    bg.join(timeout=5.0)
