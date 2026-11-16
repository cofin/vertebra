"""Unit and loopback tests for native Postgres query, query_multi, and transaction facades (`ch14:t04`)."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient
from vertebra._vertebra import DataError, InterfaceError
from vertebra.db.adapters.postgres import (
    AsyncPostgresQueueStore,
    SyncPostgresQueueStore,
)
from vertebra.db.adapters.postgres.types import (
    PgTemporal,
    PostgresPipelineOperation,
    PostgresQueryResult,
    _project_public_row,
)
from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, SyncQueueStore


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
        name.encode("utf-8") + b"\x00" + struct.pack("!ihihih", 0, 0, oid, 4, -1, fmt) for name, oid, fmt in cols
    )
    return _frame(b"T", b"".join(parts))


def _data_row_text(cells: tuple[bytes | None, ...]) -> bytes:
    """Build a DataRow ('D') frame."""
    parts = [struct.pack("!h", len(cells))]
    parts.extend(struct.pack("!i", -1) if c is None else struct.pack("!i", len(c)) + c for c in cells)
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


def test_jiff_session_timezone_fold_resolution_and_wire_text_rebind_refusal() -> None:
    """Verify America/New_York EDT/EST fold resolution and DataError refusal on wire_text rebind."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    query_frames_received: list[bytes] = []

    def _serve() -> None:
        conn, _ = srv.accept()
        with conn:
            _read_startup(conn)
            conn.sendall(
                b"".join(
                    (
                        _frame(b"R", struct.pack("!i", 0)),
                        _frame(b"S", b"client_encoding\x00UTF8\x00"),
                        _frame(b"S", b"DateStyle\x00Postgres, MDY\x00"),
                        _frame(b"S", b"TimeZone\x00America/New_York\x00"),
                        _frame(b"S", b"timezone_abbreviations\x00India\x00"),
                        _frame(b"K", struct.pack("!ii", 701, 702)),
                        _frame(b"Z", b"I"),
                    )
                )
            )

            g1 = _read_until_sync_or_query(conn)
            query_frames_received.append(g1[0][0])
            cols = (("edt", 1184, 0), ("est", 1184, 0), ("amb", 1184, 0), ("far", 1184, 0))
            conn.sendall(
                b"".join(
                    (
                        _frame(b"1", b""),
                        _frame(b"t", struct.pack("!h", 0)),
                        _row_desc(cols),
                        _frame(b"2", b""),
                        _row_desc(cols),
                        _data_row_text(
                            (
                                b"Sun Nov 01 01:30:00 2026 EDT",
                                b"Sun Nov 01 01:30:00 2026 EST",
                                b"Wed Jul 09 07:30:00 2026 CST",
                                b"Wed Jul 09 07:30:00 12000 EDT",
                            )
                        ),
                        _frame(b"C", b"SELECT 1\x00"),
                        _frame(b"3", b""),
                        _frame(b"Z", b"I"),
                    )
                )
            )

            g_begin = _read_until_sync_or_query(conn)
            assert g_begin[0] == (b"Q", b"BEGIN\x00")
            conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", b"T"))

            g_rb = _read_until_sync_or_query(conn)
            assert g_rb[0] == (b"Q", b"ROLLBACK\x00")
            conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", b"I"))

            g_end = _read_until_sync_or_query(conn)
            assert g_end[0][0] == b"X"
        srv.close()

    bg = threading.Thread(target=_serve, daemon=True)
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
        res = client.query("SELECT edt, est, amb, far")
        edt_val, est_val, amb_val, far_val = res.rows[0]
        assert edt_val == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
        assert est_val == datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
        assert amb_val == PgTemporal(
            kind="timestamptz",
            value=None,
            special="wire_text",
            wire_text="Wed Jul 09 07:30:00 2026 CST",
            date_style="Postgres, MDY",
            time_zone="America/New_York",
        )
        assert far_val == PgTemporal(
            kind="timestamptz",
            value=None,
            special="wire_text",
            wire_text="Wed Jul 09 07:30:00 12000 EDT",
            date_style="Postgres, MDY",
            time_zone="America/New_York",
        )

        with pytest.raises(DataError):
            client.query("SELECT $1::timestamptz", [amb_val])

        scope_id = client.pipeline_begin()
        try:
            with pytest.raises(DataError):
                client.pipeline_results(
                    scope_id,
                    [PostgresPipelineOperation(sql="SELECT $1::timestamptz", parameters=(amb_val,))],
                )
        finally:
            client.pipeline_finish(scope_id, False)

        assert query_frames_received == [b"P"]
        assert isinstance(amb_val, PgTemporal)
        assert isinstance(amb_val.wire_text, str)
    finally:
        client.close()
    bg.join(timeout=5.0)


class _FakeSyncPostgresDriver:
    """Synchronous PostgreSQL driver fake for queue store testing."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.affected_rows: int = 1

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> int:
        self.executed.append((sql, parameters))
        return self.affected_rows

    def select(self, sql: str, parameters: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.executed.append((sql, parameters))
        return list(self.select_rows)

    def select_one_or_none(self, sql: str, parameters: Sequence[Any] | None = None) -> dict[str, Any] | None:
        self.executed.append((sql, parameters))
        return self.select_rows[0] if self.select_rows else None

    def in_transaction(self) -> bool:
        return False


class _FakeAsyncPostgresDriver:
    """Asynchronous PostgreSQL driver fake for queue store testing."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.affected_rows: int = 1

    async def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> int:
        self.executed.append((sql, parameters))
        return self.affected_rows

    async def select(self, sql: str, parameters: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        self.executed.append((sql, parameters))
        return list(self.select_rows)

    async def select_one_or_none(self, sql: str, parameters: Sequence[Any] | None = None) -> dict[str, Any] | None:
        self.executed.append((sql, parameters))
        return self.select_rows[0] if self.select_rows else None

    def in_transaction(self) -> bool:
        return False


class _FakeSyncPostgresNotifySource:
    """Fake synchronous notification source capturing published channel/payload events."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, channel: str, payload: str) -> None:
        self.published.append((channel, payload))


class _FakeAsyncPostgresNotifySource:
    """Fake asynchronous notification source capturing published channel/payload events."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, channel: str, payload: str) -> None:
        self.published.append((channel, payload))


def test_sync_postgres_queue_store_lifecycle() -> None:
    """Verify SyncPostgresQueueStore protocol conformance, queries, claims, fencing, and DLQ."""
    drv = _FakeSyncPostgresDriver()
    notify = _FakeSyncPostgresNotifySource()
    store = SyncPostgresQueueStore(cast("Any", drv), notification_source=cast("Any", notify))
    assert isinstance(store, SyncQueueStore)

    store.ensure_schema()
    assert any("CREATE TABLE IF NOT EXISTS vertebra_queue_jobs" in call[0] for call in drv.executed)

    entry = QueueEntry(
        job_id="job-1",
        queue="default",
        task_name="tasks.add",
        payload=b'{"a": 1}',
        headers={"x-trace": "trace-1"},
    )
    assigned = store.enqueue(entry)
    assert assigned == "job-1"
    assert notify.published == [("vtb:queue:default", "wakeup:job-1")]

    drv.select_rows = [
        {
            "job_id": "job-1",
            "queue": "default",
            "task_name": "tasks.add",
            "payload": b'{"a": 1}',
            "attempt": 1,
            "max_attempts": 3,
            "lease_until_ms": 1000,
            "scheduled_at_ms": None,
            "headers": '{"x-trace": "trace-1"}',
        }
    ]
    claimed = store.claim(["default"], worker_id="worker-1", lease_seconds=30.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == "job-1"
    assert claimed[0].headers == {"x-trace": "trace-1"}
    assert any("FOR UPDATE OF q SKIP LOCKED" in call[0] for call in drv.executed)

    assert store.ack("job-1", attempt=1) is True
    assert store.heartbeat(["job-1"], worker_id="worker-1", lease_seconds=15.0) == 1

    drv.select_rows = [
        {
            "job_id": "job-1",
            "queue": "default",
            "task_name": "tasks.add",
            "payload": b'{"a": 1}',
            "attempt": 1,
            "max_attempts": 3,
            "lease_until_ms": 1000,
            "scheduled_at_ms": None,
            "headers": "{}",
        }
    ]
    assert store.nack("job-1", attempt=1, error="timeout", retry_delay_s=5.0) is True

    drv.select_rows = [
        {
            "job_id": "job-1",
            "queue": "default",
            "task_name": "tasks.add",
            "payload": b'{"a": 1}',
            "attempt": 3,
            "max_attempts": 3,
            "lease_until_ms": 1000,
            "scheduled_at_ms": None,
            "headers": "{}",
        }
    ]
    assert store.nack("job-1", attempt=3, error="fatal") is True

    assert store.reserve_unique_key("key-1", ttl_seconds=60.0) is True
    store.release_unique_key("key-1")
    assert store.acquire_lease("lease-1", owner_id="worker-1", ttl_seconds=60.0) is True
    store.release_lease("lease-1", owner_id="worker-1")


@pytest.mark.anyio
async def test_async_postgres_queue_store_lifecycle() -> None:
    """Verify AsyncPostgresQueueStore protocol conformance, queries, claims, fencing, and DLQ."""
    drv = _FakeAsyncPostgresDriver()
    notify = _FakeAsyncPostgresNotifySource()
    store = AsyncPostgresQueueStore(cast("Any", drv), notification_source=cast("Any", notify))
    assert isinstance(store, AsyncQueueStore)

    await store.ensure_schema()
    assert any("CREATE TABLE IF NOT EXISTS vertebra_queue_jobs" in call[0] for call in drv.executed)

    entry = QueueEntry(
        job_id="job-async-1",
        queue="high",
        task_name="tasks.multiply",
        payload=b'{"x": 2}',
        headers={"tenant": "corp"},
    )
    assigned = await store.enqueue(entry)
    assert assigned == "job-async-1"
    assert notify.published == [("vtb:queue:high", "wakeup:job-async-1")]

    drv.select_rows = [
        {
            "job_id": "job-async-1",
            "queue": "high",
            "task_name": "tasks.multiply",
            "payload": b'{"x": 2}',
            "attempt": 1,
            "max_attempts": 3,
            "lease_until_ms": 2000,
            "scheduled_at_ms": None,
            "headers": '{"tenant": "corp"}',
        }
    ]
    claimed = await store.claim(["high"], worker_id="worker-async", lease_seconds=10.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == "job-async-1"
    assert any("FOR UPDATE OF q SKIP LOCKED" in call[0] for call in drv.executed)

    assert await store.ack("job-async-1", attempt=1) is True
    assert await store.heartbeat(["job-async-1"], worker_id="worker-async", lease_seconds=10.0) == 1
    assert await store.reserve_unique_key("key-async", ttl_seconds=30.0) is True
    await store.release_unique_key("key-async")
    assert await store.acquire_lease("lease-async", owner_id="w1", ttl_seconds=30.0) is True
    await store.release_lease("lease-async", owner_id="w1")
