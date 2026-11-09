"""Unit and loopback tests for native Postgres query, query_multi, and transaction facades (`ch14:t04`)."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading

import pytest

from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient
from vertebra._vertebra import InterfaceError
from vertebra.db.adapters.postgres.types import (
    PostgresQueryResult,
    _project_public_row,
)


def _frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _auth_ok_and_ready() -> bytes:
    """Return AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery('I')."""
    return b"".join(
        (
            _frame(b"R", struct.pack("!i", 0)),
            _frame(b"S", b"client_encoding\x00UTF8\x00"),
            _frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _frame(b"S", b"TimeZone\x00UTC\x00"),
            _frame(b"K", struct.pack("!ii", 101, 202)),
            _frame(b"Z", b"I"),
        )
    )


def _row_desc(cols: tuple[tuple[str, int, int], ...]) -> bytes:
    """Build a RowDescription ('T') frame."""
    parts = [struct.pack("!h", len(cols))]
    parts.extend(
        name.encode("utf-8") + b"\x00" + struct.pack("!ihihih", 0, 0, oid, 4, -1, fmt)
        for name, oid, fmt in cols
    )
    return _frame(b"T", b"".join(parts))


def _data_row_text(cells: tuple[bytes | None, ...]) -> bytes:
    """Build a DataRow ('D') frame."""
    parts = [struct.pack("!h", len(cells))]
    parts.extend(
        struct.pack("!i", -1) if c is None else struct.pack("!i", len(c)) + c for c in cells
    )
    return _frame(b"D", b"".join(parts))


def _read_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_startup(conn: socket.socket) -> None:
    """Drain startup frame."""
    raw_len = _read_exact(conn, 4)
    (length,) = struct.unpack("!i", raw_len)
    _read_exact(conn, length - 4)


def _read_until_sync_or_query(conn: socket.socket) -> list[tuple[bytes, bytes]]:
    """Read frontend frames until Sync ('S') or Query ('Q') or Terminate ('X')."""
    frames: list[tuple[bytes, bytes]] = []
    while True:
        hdr = _read_exact(conn, 5)
        tag = hdr[:1]
        (length,) = struct.unpack("!i", hdr[1:5])
        body = _read_exact(conn, length - 4)
        frames.append((tag, body))
        if tag in (b"S", b"Q", b"X"):
            return frames


def test_sync_and_async_query_and_transaction_facades() -> None:
    """Verify query, query_multi, and transaction/savepoint methods across SyncPostgresClient and AsyncPostgresClient."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(2)
    port = srv.getsockname()[1]

    def _serve_one(conn: socket.socket) -> None:
        with conn:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())

            f1 = _read_until_sync_or_query(conn)
            assert [t for t, _ in f1] == [b"P", b"D", b"B", b"D", b"E", b"C", b"S"]
            conn.sendall(
                b"".join(
                    (
                        _frame(b"1", b""),
                        _frame(b"t", struct.pack("!h", 1) + struct.pack("!I", 23)),
                        _row_desc((("val", 23, 0), ("val", 25, 0))),
                        _frame(b"2", b""),
                        _row_desc((("val", 23, 0), ("val", 25, 0))),
                        _data_row_text((b"42", b"forty-two")),
                        _frame(b"C", b"SELECT 1\x00"),
                        _frame(b"3", b""),
                        _frame(b"Z", b"I"),
                    )
                )
            )

            f2 = _read_until_sync_or_query(conn)
            assert f2[0][0] == b"Q"
            conn.sendall(
                b"".join(
                    (
                        _row_desc((("a", 23, 0),)),
                        _data_row_text((b"1",)),
                        _frame(b"C", b"SELECT 1\x00"),
                        _row_desc((("b", 23, 0),)),
                        _data_row_text((b"2",)),
                        _frame(b"C", b"SELECT 1\x00"),
                        _frame(b"Z", b"I"),
                    )
                )
            )

            f3 = _read_until_sync_or_query(conn)
            assert f3[0] == (b"Q", b"BEGIN\x00")
            conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", b"T"))

            f4 = _read_until_sync_or_query(conn)
            assert f4[0] == (b"Q", b"SAVEPOINT vtb_sp_1\x00")
            conn.sendall(_frame(b"C", b"SAVEPOINT\x00") + _frame(b"Z", b"T"))

            f5 = _read_until_sync_or_query(conn)
            assert f5[0] == (b"Q", b"RELEASE SAVEPOINT vtb_sp_1\x00")
            conn.sendall(_frame(b"C", b"RELEASE\x00") + _frame(b"Z", b"T"))

            f6 = _read_until_sync_or_query(conn)
            assert f6[0] == (b"Q", b"COMMIT\x00")
            conn.sendall(_frame(b"C", b"COMMIT\x00") + _frame(b"Z", b"I"))

            f7 = _read_until_sync_or_query(conn)
            assert f7[0][0] == b"X"

    def _accept_two() -> None:
        conn1, _ = srv.accept()
        _serve_one(conn1)
        conn2, _ = srv.accept()
        _serve_one(conn2)
        srv.close()

    bg = threading.Thread(target=_accept_two, daemon=True)
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
        res = client.query("SELECT $1::int4, 'forty-two'::text", [42])
        assert isinstance(res, PostgresQueryResult)
        assert res.rows == ((42, "forty-two"),)
        assert res.transaction_status == "I"
        assert len(res.columns) == 2
        assert res.columns[0].name == "val"
        assert res.columns[1].name == "val"
        assert _project_public_row(res.rows[0]) == (42, "forty-two")

        multi = client.query_multi("SELECT 1 AS a; SELECT 2 AS b")
        assert len(multi) == 2
        assert multi[0].columns[0].name == "a"
        assert multi[0].rows == ((1,),)
        assert multi[1].columns[0].name == "b"
        assert multi[1].rows == ((2,),)

        client.begin()
        client.savepoint(1)
        client.release_savepoint(1)
        client.commit()
        with pytest.raises(InterfaceError):
            client.commit()
    finally:
        client.close()

    async def _run_async() -> None:
        aclient = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await aclient.connect()
        try:
            ares = await aclient.query("SELECT $1::int4, 'forty-two'::text", [42])
            assert isinstance(ares, PostgresQueryResult)
            assert ares.rows == ((42, "forty-two"),)

            amulti = await aclient.query_multi("SELECT 1 AS a; SELECT 2 AS b")
            assert len(amulti) == 2

            await aclient.begin()
            await aclient.savepoint(1)
            await aclient.release_savepoint(1)
            await aclient.commit()
            with pytest.raises(InterfaceError):
                await aclient.commit()
        finally:
            await aclient.close()

    asyncio.run(_run_async())
    bg.join(timeout=5.0)
