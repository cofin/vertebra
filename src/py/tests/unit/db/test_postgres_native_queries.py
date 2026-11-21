"""Unit and loopback tests for native Postgres query, query_multi, and transaction facades."""

from __future__ import annotations

import asyncio
import gc
import socket
import struct
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from tests.unit.db.postgres_data_peers import (
    _bind_frame_stmt_params_and_formats,
    _parse_frame_stmt_and_sql,
    _reply_extended_query,
)
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresCursor,
    SyncPostgresClient,
    SyncPostgresCursor,
)
from vertebra._vertebra import (
    DataError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
)
from vertebra.db import dbapi
from vertebra.db.adapters.postgres import (
    AsyncPostgresQueueStore,
    SyncPostgresQueueStore,
)
from vertebra.db.adapters.postgres.types import (
    PG_UNCHANGED_TOAST,
    PgTemporal,
    PostgresPipelineOperation,
    PostgresQueryResult,
    _project_public_row,
)
from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, QueueReservation, SyncQueueStore
from vertebra.framework.queues.postgres import PostgreSQLQueueBackend


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


@pytest.mark.anyio
async def test_postgres_queue_store_table_validation_reap_stale_and_pop_dead_letter() -> None:
    """Verify Postgres queue stores validate table_name, bump or dead-letter stale claims, and pop dead-letter rows."""
    sync_drv = _FakeSyncPostgresDriver()
    async_drv = _FakeAsyncPostgresDriver()

    with pytest.raises(ValueError, match="table_name"):
        SyncPostgresQueueStore(cast("Any", sync_drv), table_name="jobs; DROP TABLE users")
    with pytest.raises(ValueError, match="table_name"):
        AsyncPostgresQueueStore(cast("Any", async_drv), table_name="jobs; DROP TABLE users")

    sync_store = SyncPostgresQueueStore(cast("Any", sync_drv), table_name="custom_queue_jobs")
    async_store = AsyncPostgresQueueStore(cast("Any", async_drv), table_name="custom_queue_jobs")

    assert sync_store.reap_stale() == 1
    sync_reap_sql = sync_drv.executed[-1][0]
    assert "CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END" in sync_reap_sql
    assert "CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END" in sync_reap_sql

    assert await async_store.reap_stale() == 1
    async_reap_sql = async_drv.executed[-1][0]
    assert "CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'ready' END" in async_reap_sql
    assert "CASE WHEN attempt >= max_attempts THEN attempt ELSE attempt + 1 END" in async_reap_sql

    async_drv.select_rows = [
        {
            "job_id": "dlq-1",
            "queue": "io",
            "task_name": "tasks.fail",
            "payload": b'{"ok": false}',
            "attempt": 3,
            "max_attempts": 3,
            "lease_until_ms": 0,
            "scheduled_at_ms": None,
            "headers": "{}",
        }
    ]
    popped = await async_store.pop_dead_letter("dlq-1")
    assert popped is not None
    assert popped.job_id == "dlq-1"
    assert "DELETE FROM custom_queue_jobs" in async_drv.executed[-1][0]


@pytest.mark.parametrize("bad_limit", [0, -1, -100])
@pytest.mark.anyio
async def test_postgres_queue_store_rejects_non_positive_limits(bad_limit: int) -> None:
    """Sync and async Postgres queue stores raise ValueError for zero or negative limits."""
    sync_store = SyncPostgresQueueStore(cast("Any", _FakeSyncPostgresDriver()))
    async_store = AsyncPostgresQueueStore(cast("Any", _FakeAsyncPostgresDriver()))

    with pytest.raises(ValueError, match="limit"):
        sync_store.list_entries(limit=bad_limit)
    with pytest.raises(ValueError, match="limit"):
        sync_store.cleanup_terminal(before_ms=1000, limit=bad_limit)
    with pytest.raises(ValueError, match="limit"):
        sync_store.expire_overdue(now_ms=1000, limit=bad_limit)

    with pytest.raises(ValueError, match="limit"):
        await async_store.list_entries(limit=bad_limit)
    with pytest.raises(ValueError, match="limit"):
        await async_store.cleanup_terminal(before_ms=1000, limit=bad_limit)
    with pytest.raises(ValueError, match="limit"):
        await async_store.expire_overdue(now_ms=1000, limit=bad_limit)


@pytest.mark.anyio
async def test_postgres_queue_store_extended_capabilities_sync_and_async() -> None:
    """Verify cleanup_terminal, identity reservations, heartbeats, expire_overdue, interrupt_task, and list_entries."""
    sync_drv = _FakeSyncPostgresDriver()
    async_drv = _FakeAsyncPostgresDriver()
    sync_store = SyncPostgresQueueStore(cast("Any", sync_drv), table_name="custom_pg_jobs")
    async_store = AsyncPostgresQueueStore(cast("Any", async_drv), table_name="custom_pg_jobs")

    sync_drv.select_rows = [{"unique_key": "idem-k1"}]
    assert sync_store.reserve_identity("idem-k1", job_id="j-1", task_name="t.run", ttl_seconds=None) is None
    reserve_sql, reserve_params = sync_drv.executed[-1]
    assert "ON CONFLICT (unique_key) DO UPDATE" in reserve_sql
    assert "expires_at_ms IS NOT NULL" in reserve_sql
    assert reserve_params[0] == "idem-k1"
    assert reserve_params[1] == "j-1"
    assert reserve_params[2] == "t.run"
    assert reserve_params[3] is None

    loser_calls: list[tuple[str, Any]] = []

    class _ContendedAsyncDriver(_FakeAsyncPostgresDriver):
        async def select(self, sql: str, parameters: Sequence[Any] | None = None) -> list[dict[str, Any]]:
            loser_calls.append((sql, parameters))
            return []

        async def select_one_or_none(self, sql: str, parameters: Sequence[Any] | None = None) -> dict[str, Any] | None:
            loser_calls.append((sql, parameters))
            return {
                "unique_key": "idem-k1",
                "job_id": "j-1",
                "task_name": "t.run",
                "created_at_ms": 1700000000000,
            }

    contended_store = AsyncPostgresQueueStore(cast("Any", _ContendedAsyncDriver()))
    existing = await contended_store.reserve_identity("idem-k1", job_id="j-2", task_name="t.other", ttl_seconds=30.0)
    assert existing == QueueReservation(
        key="idem-k1",
        job_id="j-1",
        task_name="t.run",
        created_at_ms=1700000000000,
    )
    assert "ON CONFLICT (unique_key) DO UPDATE" in loser_calls[0][0]
    assert "expires_at_ms IS NULL OR expires_at_ms > $2" in loser_calls[1][0]

    assert sync_store.reset_identity("idem-k1", expected_job_id="j-1") is True
    assert sync_drv.executed[-1][0] == "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1 AND job_id = $2"
    assert sync_drv.executed[-1][1] == ("idem-k1", "j-1")

    assert await async_store.reset_identity("idem-k1") is True
    assert async_drv.executed[-1][0] == "DELETE FROM vertebra_queue_unique_keys WHERE unique_key = $1"
    assert async_drv.executed[-1][1] == ("idem-k1",)

    assert sync_store.cleanup_terminal(before_ms=5000, limit=10) == 1
    assert "WITH doomed AS" in sync_drv.executed[-2][0]
    assert sync_drv.executed[-2][1] == (5000, 10)
    assert "expires_at_ms IS NOT NULL AND expires_at_ms <= $1" in sync_drv.executed[-1][0]
    assert sync_drv.executed[-1][1] == (5000,)

    assert await async_store.cleanup_terminal(before_ms=9000) == 1
    assert "DELETE FROM custom_pg_jobs" in async_drv.executed[-2][0]
    assert async_drv.executed[-2][1] == (9000,)
    assert "expires_at_ms IS NOT NULL AND expires_at_ms <= $1" in async_drv.executed[-1][0]

    assert sync_store.assign_worker("j-1", worker_id="w-9", attempt=2) is True
    assert "SET worker_id = $2" in sync_drv.executed[-1][0]
    assert sync_drv.executed[-1][1][:3] == ("j-1", "w-9", 2)

    assert await async_store.touch_heartbeats(["j-1", "j-2"], heartbeat_at_ms=12345) == 1
    assert "SET heartbeat_at_ms = $1" in async_drv.executed[-1][0]
    assert async_drv.executed[-1][1][0] == 12345
    assert async_drv.executed[-1][1][2] == ["j-1", "j-2"]

    assert sync_store.null_heartbeats(["j-1"]) == 1
    assert "SET heartbeat_at_ms = NULL" in sync_drv.executed[-1][0]
    assert sync_drv.executed[-1][1][1] == ["j-1"]

    assert await async_store.interrupt_task("j-1", error="cancelled by operator") is True
    assert "error = $3" in async_drv.executed[-1][0]
    assert async_drv.executed[-1][1][0] == "j-1"
    assert async_drv.executed[-1][1][2] == "cancelled by operator"

    assert sync_store.expire_overdue(now_ms=7777, limit=25) == 1
    assert "SET status = 'expired'" in sync_drv.executed[-1][0]
    assert sync_drv.executed[-1][1] == (7777, 25)

    sample_row = {
        "job_id": "j-key-1",
        "queue": "billing",
        "task_name": "tasks.bill",
        "payload": b"{}",
        "attempt": 1,
        "max_attempts": 3,
        "lease_until_ms": 0,
        "scheduled_at_ms": None,
        "headers": '{"key": "k-bill"}',
        "priority": 5,
        "expires_at_ms": 99999,
        "completed_at_ms": None,
        "worker_id": "w-1",
        "heartbeat_at_ms": 1111,
        "result": None,
        "error": None,
    }
    sync_drv.select_rows = [sample_row]
    async_drv.select_rows = [sample_row]

    by_key = sync_store.get_entry_by_key("k-bill", queue="billing")
    assert by_key is not None
    assert by_key.priority == 5
    assert by_key.expires_at_ms == 99999
    assert "LEFT JOIN vertebra_queue_unique_keys u ON u.job_id = j.job_id" in sync_drv.executed[-1][0]
    assert sync_drv.executed[-1][1] == ("k-bill", "billing")

    cross_queue = await async_store.list_entries(None, status="ready", limit=50)
    assert len(cross_queue) == 1
    assert cross_queue[0].worker_id == "w-1"
    assert "WHERE status = $1" in async_drv.executed[-1][0]
    assert async_drv.executed[-1][1] == ("ready", 50)

    backend = PostgreSQLQueueBackend(store=async_store)
    async_drv.select_rows = [sample_row]
    replayed = await backend.replay_dlq("j-key-1")
    assert replayed is not None
    assert replayed.task_name == "tasks.bill"
    assert any("DELETE FROM custom_pg_jobs" in sql for sql, _ in async_drv.executed)


def _error_response(sqlstate: str, message: str) -> bytes:
    """Build a PostgreSQL ErrorResponse ('E') frame."""
    body = b"SERROR\x00C" + sqlstate.encode("ascii") + b"\x00M" + message.encode("utf-8") + b"\x00\x00"
    return _frame(b"E", body)


def _extract_sql_from_frames(
    frames: list[tuple[bytes, bytes]],
    prepared: dict[bytes, str] | None = None,
) -> str:
    """Extract the SQL text from a Query ('Q'), Parse ('P'), or cached Bind ('B') frame."""
    for tag, body in frames:
        if tag == b"Q":
            return body.rstrip(b"\x00").decode("utf-8")
        if tag == b"P":
            parts = body.split(b"\x00", 2)
            stmt_name = parts[0]
            sql = parts[1].decode("utf-8")
            if prepared is not None:
                prepared[stmt_name] = sql
            return sql
    if prepared is not None:
        for tag, body in frames:
            if tag == b"B":
                parts = body.split(b"\x00", 2)
                return prepared.get(parts[1], "")
    return ""


def _extract_bind_params_from_frames(frames: list[tuple[bytes, bytes]]) -> list[bytes | None]:
    """Decode parameter payloads from the first Bind ('B') frame."""
    for tag, body in frames:
        if tag != b"B":
            continue
        idx = body.index(b"\x00") + 1
        idx = body.index(b"\x00", idx) + 1
        (num_fmt,) = struct.unpack("!h", body[idx : idx + 2])
        idx += 2 + 2 * num_fmt
        (num_params,) = struct.unpack("!h", body[idx : idx + 2])
        idx += 2
        values: list[bytes | None] = []
        for _ in range(num_params):
            (plen,) = struct.unpack("!i", body[idx : idx + 4])
            idx += 4
            if plen < 0:
                values.append(None)
            else:
                values.append(body[idx : idx + plen])
                idx += plen
        return values
    return []


def _extended_ok(
    param_oids: tuple[int, ...],
    cols: tuple[tuple[str, int, int], ...],
    rows: Sequence[tuple[bytes | None, ...]],
    command_tag: bytes,
    tx_status: bytes = b"I",
    *,
    is_simple: bool = False,
    has_parse: bool = True,
    has_describe: bool = False,
) -> bytes:
    """Encode an extended (parsed or cached) or simple query reply sequence ending in ReadyForQuery."""
    parts: list[bytes] = []
    if is_simple:
        if cols:
            parts.append(_row_desc(cols))
            for row in rows:
                parts.append(_data_row_text(row))
        parts.append(_frame(b"C", command_tag + b"\x00"))
        parts.append(_frame(b"Z", tx_status))
        return b"".join(parts)

    if has_parse:
        parts.append(_frame(b"1", b""))
        oid_body = struct.pack("!h", len(param_oids)) + b"".join(struct.pack("!I", oid) for oid in param_oids)
        parts.append(_frame(b"t", oid_body))
        if cols:
            parts.append(_row_desc(cols))
        else:
            parts.append(_frame(b"n", b""))
        parts.append(_frame(b"2", b""))
        if cols:
            parts.append(_row_desc(cols))
        else:
            parts.append(_frame(b"n", b""))
    else:
        parts.append(_frame(b"2", b""))
        if has_describe:
            if cols:
                parts.append(_row_desc(cols))
            else:
                parts.append(_frame(b"n", b""))
    for row in rows:
        parts.append(_data_row_text(row))
    parts.append(_frame(b"C", command_tag + b"\x00"))
    if has_parse:
        parts.append(_frame(b"3", b""))
    parts.append(_frame(b"Z", tx_status))
    return b"".join(parts)


def _run_loopback_pg(
    handler: Callable[[socket.socket], None],
    *,
    backlog: int = 2,
) -> tuple[int, threading.Thread, list[BaseException], socket.socket]:
    """Start a loopback TCP server running handler(conn) for each accepted client."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(backlog)
    port = srv.getsockname()[1]
    errors: list[BaseException] = []

    def _runner() -> None:
        try:
            conn, _ = srv.accept()
            with conn:
                handler(conn)
        except BaseException as exc:
            errors.append(exc)
        finally:
            srv.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    return port, thread, errors, srv


def _make_sync_client(port: int) -> SyncPostgresClient:
    """Create a loopback SyncPostgresClient for tests."""
    return SyncPostgresClient(
        user="postgres",
        database="app",
        host="127.0.0.1",
        port=port,
        sslmode="disable",
    )


def test_sync_cursor_lifecycle_and_constructor_guard() -> None:
    """Verify cursor creation does no I/O, constructor is hidden, and closed/state errors match PEP 249."""
    with pytest.raises(TypeError):
        SyncPostgresCursor()

    client = _make_sync_client(1)
    cur = client.cursor()
    assert cur.description is None
    assert cur.rowcount == -1
    assert cur.lastrowid is None
    assert cur.arraysize == 1

    with pytest.raises(ProgrammingError):
        cur.fetchone()
    with pytest.raises(ProgrammingError):
        cur.fetchmany()
    with pytest.raises(ProgrammingError):
        cur.fetchall()
    with pytest.raises(ProgrammingError):
        cur.arraysize = 0
    with pytest.raises(ProgrammingError):
        cur.arraysize = -5
    assert cur.arraysize == 1

    cur.arraysize = 4
    assert cur.arraysize == 4

    cur.close()
    cur.close()

    with pytest.raises(InterfaceError):
        cur.execute("SELECT 1")
    with pytest.raises(InterfaceError):
        cur.executemany("INSERT INTO t VALUES (%s)", [(1,)])
    with pytest.raises(InterfaceError):
        cur.fetchone()
    with pytest.raises(InterfaceError):
        cur.fetchmany()
    with pytest.raises(InterfaceError):
        cur.fetchall()


def test_sync_cursor_select_and_fetch_modes() -> None:
    """Verify SELECT tuples, 7-item description, fetchone/fetchmany/fetchall, and row replacement."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        f1 = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(f1) == "SELECT 1 AS a, 'x' AS b"
        conn.sendall(
            _extended_ok(
                (),
                (("a", 23, 0), ("b", 25, 0)),
                [(b"1", b"x")],
                b"SELECT 1",
            )
        )

        f2 = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(f2) == "SELECT n FROM seq"
        conn.sendall(
            _extended_ok(
                (),
                (("n", 23, 0),),
                [(b"1",), (b"2",), (b"3",), (b"4",), (b"5",)],
                b"SELECT 5",
            )
        )

        f3 = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(f3) == "SELECT 99 AS z"
        conn.sendall(_extended_ok((), (("z", 23, 0),), [(b"99",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        cur = client.cursor()
        returned = cur.execute("SELECT 1 AS a, 'x' AS b")
        assert returned is cur
        assert cur.description == (
            ("a", 23, None, None, None, None, None),
            ("b", 25, None, None, None, None, None),
        )
        assert cur.rowcount == 1
        assert cur.lastrowid is None
        assert cur.fetchall() == [(1, "x")]
        assert cur.fetchall() == []
        assert cur.fetchone() is None

        cur.execute("SELECT n FROM seq")
        assert cur.rowcount == 5
        assert cur.fetchone() == (1,)
        cur.arraysize = 2
        assert cur.fetchmany(0) == []
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)
        assert cur.fetchmany() == [(2,), (3,)]

        cur.execute("SELECT 99 AS z")
        assert cur.rowcount == 1
        assert cur.fetchall() == [(99,)]
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.parametrize(
    ("sql", "cols", "rows", "tag", "expected_rowcount", "expect_desc", "expected_rows"),
    [
        ("CREATE TEMP TABLE t (id int)", (), [], b"CREATE TABLE", -1, False, None),
        ("INSERT INTO t VALUES (1), (2), (3)", (), [], b"INSERT 0 3", 3, False, None),
        (
            "INSERT INTO t VALUES (10), (20) RETURNING id",
            (("id", 23, 0),),
            [(b"10",), (b"20",)],
            b"INSERT 0 2",
            2,
            True,
            [(10,), (20,)],
        ),
        ("SELECT 1 WHERE false", (("col", 23, 0),), [], b"SELECT 0", 0, True, []),
    ],
)
def test_sync_cursor_command_dml_returning_and_empty_set(
    sql: str,
    cols: tuple[tuple[str, int, int], ...],
    rows: list[tuple[bytes | None, ...]],
    tag: bytes,
    expected_rowcount: int,
    expect_desc: bool,
    expected_rows: list[tuple[Any, ...]] | None,
) -> None:
    """Verify DDL command, DML count, RETURNING, and empty SELECT contracts."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        frames = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(frames) == sql
        conn.sendall(_extended_ok((), cols, rows, tag))
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        cur = client.cursor()
        cur.execute(sql)
        assert cur.rowcount == expected_rowcount
        if expect_desc:
            assert cur.description is not None
            assert cur.fetchall() == expected_rows
        else:
            assert cur.description is None
            with pytest.raises(ProgrammingError):
                cur.fetchall()
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors


def test_sync_cursor_named_format_and_normalized_parameters() -> None:
    """Verify named, format, and non-primitive parameter rewriting, normalization, and error conversion."""
    seen_sql: list[str] = []
    seen_params: list[list[bytes | None]] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        f1 = _read_until_sync_or_query(conn)
        seen_sql.append(_extract_sql_from_frames(f1))
        seen_params.append(_extract_bind_params_from_frames(f1))
        conn.sendall(_extended_ok((23, 23), (("s", 23, 0),), [(b"5",)], b"SELECT 1"))

        f2 = _read_until_sync_or_query(conn)
        seen_sql.append(_extract_sql_from_frames(f2))
        seen_params.append(_extract_bind_params_from_frames(f2))
        conn.sendall(_extended_ok((23, 23), (("s", 23, 0),), [(b"10",)], b"SELECT 1"))

        f3 = _read_until_sync_or_query(conn)
        seen_sql.append(_extract_sql_from_frames(f3))
        seen_params.append(_extract_bind_params_from_frames(f3))
        conn.sendall(_extended_ok((17,), (("b", 25, 0),), [(b"ok",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        cur = client.cursor()
        cur.execute("SELECT %(a)s + %(b)s", {"a": 2, "b": 3})
        assert cur.fetchall() == [(5,)]

        cur.execute("SELECT %s::int + %s::int", (4, 6))
        assert cur.fetchall() == [(10,)]

        cur.execute("SELECT %s", (bytearray(b"abc"),))
        assert cur.fetchall() == [("ok",)]

        with pytest.raises(InterfaceError):
            cur.execute("SELECT %s", (PG_UNCHANGED_TOAST,))
        assert cur.description is None
        assert cur.rowcount == -1
        with pytest.raises(ProgrammingError):
            cur.fetchone()
    finally:
        client.close()
        thread.join(timeout=5.0)

    assert not errors
    assert seen_sql == [
        "SELECT $1 + $2",
        "SELECT $1::int + $2::int",
        "SELECT $1",
    ]
    assert len(seen_params[0]) == 2
    assert len(seen_params[1]) == 2
    assert seen_params[2] == [b"\\x616263"]


def test_sync_cursor_executemany_counts_and_empty() -> None:
    """Verify executemany sums row counts across parameter sets and sends nothing on empty input."""
    exec_count = 0

    def _serve(conn: socket.socket) -> None:
        nonlocal exec_count
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        prepared: dict[bytes, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            has_parse = any(tag == b"P" for tag, _ in frames)
            exec_count += 1
            assert _extract_sql_from_frames(frames, prepared) == "INSERT INTO t VALUES ($1)"
            conn.sendall(_extended_ok((23,), (), [], b"INSERT 0 1", has_parse=has_parse))

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        cur = client.cursor()
        ret = cur.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,), (3,)])
        assert ret is cur
        assert cur.rowcount == 3
        assert cur.description is None
        with pytest.raises(ProgrammingError):
            cur.fetchall()

        cur.executemany("INSERT INTO t VALUES (%s)", [])
        assert cur.rowcount == 0
        assert cur.description is None
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors
    assert exec_count == 3


def test_sync_cursor_server_errors_and_result_clearing() -> None:
    """Verify IntegrityError(23505) and ProgrammingError(42601) match dbapi classes and clear state."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        _read_until_sync_or_query(conn)
        conn.sendall(_extended_ok((), (("a", 23, 0),), [(b"1",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)
        conn.sendall(_error_response("23505", "duplicate key value violates unique constraint") + _frame(b"Z", b"I"))

        _read_until_sync_or_query(conn)
        conn.sendall(_extended_ok((), (("a", 23, 0),), [(b"2",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)
        conn.sendall(_error_response("42601", "syntax error at or near SELEC") + _frame(b"Z", b"I"))

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        cur = client.cursor()
        cur.execute("SELECT 1 AS a")
        assert cur.description is not None

        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO t VALUES (1)")
        assert exc_info.value.sqlstate == "23505"
        assert dbapi.IntegrityError is IntegrityError
        assert type(exc_info.value) is dbapi.IntegrityError
        assert cur.description is None
        assert cur.rowcount == -1
        with pytest.raises(ProgrammingError):
            cur.fetchone()

        cur.execute("SELECT 2 AS a")
        assert cur.description is not None
        with pytest.raises(ProgrammingError):
            cur.execute("SELEC 1")
        assert cur.description is None
        assert cur.rowcount == -1
        with pytest.raises(ProgrammingError):
            cur.fetchone()
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors


def test_sync_cursor_timeout_matches_client_query() -> None:
    """Verify cursor timeout raises the same OperationalError subclass as SyncPostgresClient.query."""

    def _run_timeout(use_cursor: bool) -> type[BaseException]:
        def _serve(conn: socket.socket) -> None:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())
            _read_until_sync_or_query(conn)
            time.sleep(0.3)

        port, thread, _errors, _ = _run_loopback_pg(_serve)
        client = _make_sync_client(port)
        client.connect()
        try:
            if use_cursor:
                cur = client.cursor()
                with pytest.raises(OperationalError) as exc_info:
                    cur.execute("SELECT pg_sleep(1)", timeout=0.05)
                assert cur.description is None
                assert cur.rowcount == -1
                return type(exc_info.value)
            with pytest.raises(OperationalError) as exc_info:
                client.query("SELECT pg_sleep(1)", timeout=0.05)
            return type(exc_info.value)
        finally:
            client.close()
            thread.join(timeout=5.0)

    cursor_exc_type = _run_timeout(use_cursor=True)
    client_exc_type = _run_timeout(use_cursor=False)
    assert cursor_exc_type is client_exc_type
    assert issubclass(cursor_exc_type, OperationalError)


def test_sync_cursor_lazy_begin_and_default_cursor() -> None:
    """Verify lazy_begin opens transactions on demand, reopens after commit, clears on failed begin, and default opens none."""
    seen_commands: list[str] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        tx_status = b"I"
        inserted_rows: list[int] = []
        committed_rows: list[int] = []
        fail_next_begin = False
        prepared: dict[bytes, str] = {}

        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            is_simple = any(tag == b"Q" for tag, _ in frames)
            has_parse = any(tag == b"P" for tag, _ in frames)
            has_describe = any(tag == b"D" for tag, _ in frames)
            sql = _extract_sql_from_frames(frames, prepared)
            seen_commands.append(sql)

            if sql.upper() == "BEGIN":
                if fail_next_begin:
                    fail_next_begin = False
                    conn.sendall(_error_response("40001", "begin failed") + _frame(b"Z", b"I"))
                else:
                    tx_status = b"T"
                    conn.sendall(
                        _extended_ok(
                            (),
                            (),
                            [],
                            b"BEGIN",
                            tx_status,
                            is_simple=is_simple,
                            has_parse=has_parse,
                            has_describe=has_describe,
                        )
                    )
            elif sql.upper() == "ROLLBACK":
                inserted_rows.clear()
                tx_status = b"I"
                conn.sendall(
                    _extended_ok(
                        (),
                        (),
                        [],
                        b"ROLLBACK",
                        tx_status,
                        is_simple=is_simple,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql.upper() == "COMMIT":
                committed_rows.extend(inserted_rows)
                inserted_rows.clear()
                tx_status = b"I"
                conn.sendall(
                    _extended_ok(
                        (),
                        (),
                        [],
                        b"COMMIT",
                        tx_status,
                        is_simple=is_simple,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql == "SELECT arm_fail_begin":
                fail_next_begin = True
                conn.sendall(
                    _extended_ok(
                        (),
                        (("ok", 23, 0),),
                        [(b"1",)],
                        b"SELECT 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql == "SELECT COUNT(*) FROM t":
                total = len(committed_rows) + len(inserted_rows)
                if has_parse:
                    c_cols = (("c", 23, 0),)
                    c_rows = [(str(total).encode(),)]
                else:
                    c_cols = (("c", 23, 1),)
                    c_rows = [(struct.pack("!i", total),)]
                conn.sendall(
                    _extended_ok(
                        (),
                        c_cols,
                        c_rows,
                        b"SELECT 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            else:
                inserted_rows.append(1)
                if tx_status == b"I":
                    committed_rows.extend(inserted_rows)
                    inserted_rows.clear()
                param_oids = (23,) if "$1" in sql else ()
                conn.sendall(
                    _extended_ok(
                        param_oids,
                        (),
                        [],
                        b"INSERT 0 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        default_cur = client.cursor()
        default_cur.execute("INSERT INTO t VALUES (1)")
        assert seen_commands == ["INSERT INTO t VALUES (1)"]

        lazy_cur = client.cursor(lazy_begin=True)
        lazy_cur.execute("INSERT INTO t VALUES (2)")
        lazy_cur.execute("SELECT COUNT(*) FROM t")
        assert lazy_cur.fetchall() == [(2,)]
        client.rollback()

        default_cur.execute("SELECT COUNT(*) FROM t")
        assert default_cur.fetchall() == [(1,)]

        lazy_cur.execute("INSERT INTO t VALUES (3)")
        client.commit()

        lazy_cur.execute("INSERT INTO t VALUES (4)")
        client.commit()

        default_cur.execute("SELECT COUNT(*) FROM t")
        assert default_cur.fetchall() == [(3,)]

        lazy_cur.execute("SELECT COUNT(*) FROM t")
        assert lazy_cur.description is not None
        client.rollback()

        default_cur.execute("SELECT arm_fail_begin")
        assert default_cur.fetchall() == [(1,)]
        with pytest.raises(OperationalError):
            lazy_cur.execute("INSERT INTO t VALUES (5)")
        assert lazy_cur.description is None
        assert lazy_cur.rowcount == -1
    finally:
        client.close()
        thread.join(timeout=5.0)

    assert not errors
    begin_count = sum(1 for cmd in seen_commands if cmd.upper() == "BEGIN")
    assert begin_count == 5


def test_sync_cursor_client_lease_attach_release_close_and_drop() -> None:
    """Verify attach_lease, release_lease, close, and GC drop release semantics on SyncPostgresClient."""
    calls_a = 0

    def _release_a() -> None:
        nonlocal calls_a
        calls_a += 1

    client1 = _make_sync_client(1)
    client1.attach_lease(_release_a)
    client1.release_lease()
    client1.release_lease()
    assert calls_a == 1

    calls_b = 0

    def _release_b() -> None:
        nonlocal calls_b
        calls_b += 1

    client2 = _make_sync_client(1)
    client2.attach_lease(_release_b)
    del client2
    gc.collect()
    assert calls_b == 1

    calls_c = 0
    calls_d = 0

    def _release_c() -> None:
        nonlocal calls_c
        calls_c += 1

    def _release_d() -> None:
        nonlocal calls_d
        calls_d += 1

    client3 = _make_sync_client(1)
    client3.attach_lease(_release_c)
    client3.attach_lease(_release_d)
    assert calls_c == 1
    assert calls_d == 0
    client3.close()
    client3.close()
    assert calls_d == 1


def _make_async_client(port: int) -> AsyncPostgresClient:
    """Create a loopback AsyncPostgresClient for tests."""
    return AsyncPostgresClient(
        user="postgres",
        database="app",
        host="127.0.0.1",
        port=port,
        sslmode="disable",
    )


@pytest.mark.anyio
async def test_async_cursor_lifecycle_and_constructor_guard() -> None:
    """Verify async cursor creation is plain with no I/O, constructor is hidden, and closed/state errors match PEP 249."""
    with pytest.raises(TypeError):
        AsyncPostgresCursor()

    client = _make_async_client(1)
    cur = client.cursor()
    assert isinstance(cur, AsyncPostgresCursor)
    assert cur.description is None
    assert cur.rowcount == -1
    assert cur.lastrowid is None
    assert cur.arraysize == 1

    with pytest.raises(ProgrammingError):
        await cur.fetchone()
    with pytest.raises(ProgrammingError):
        await cur.fetchmany()
    with pytest.raises(ProgrammingError):
        await cur.fetchall()
    with pytest.raises(ProgrammingError):
        cur.arraysize = 0
    with pytest.raises(ProgrammingError):
        cur.arraysize = -5
    assert cur.arraysize == 1

    cur.arraysize = 4
    assert cur.arraysize == 4

    await cur.close()
    await cur.close()

    with pytest.raises(InterfaceError):
        cur.execute("SELECT 1")
    with pytest.raises(InterfaceError):
        cur.executemany("INSERT INTO t VALUES (%s)", [(1,)])
    with pytest.raises(InterfaceError):
        await cur.fetchone()
    with pytest.raises(InterfaceError):
        await cur.fetchmany()
    with pytest.raises(InterfaceError):
        await cur.fetchall()


@pytest.mark.anyio
async def test_async_cursor_select_attributes_and_fetch_modes() -> None:
    """Verify SELECT tuples, plain attributes without await, and fetchone/fetchmany/fetchall modes."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        f1 = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(f1) == "SELECT 1"
        conn.sendall(_extended_ok((), (("col", 23, 0),), [(b"1",)], b"SELECT 1"))

        f2 = _read_until_sync_or_query(conn)
        assert _extract_sql_from_frames(f2) == "SELECT n FROM seq"
        conn.sendall(
            _extended_ok(
                (),
                (("n", 23, 0),),
                [(b"1",), (b"2",), (b"3",), (b"4",), (b"5",)],
                b"SELECT 5",
            )
        )

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        returned = await cur.execute("SELECT 1")
        assert returned is cur
        assert cur.description == (("col", 23, None, None, None, None, None),)
        assert cur.rowcount == 1
        assert cur.lastrowid is None
        assert await cur.fetchall() == [(1,)]
        assert await cur.fetchall() == []
        assert await cur.fetchone() is None

        await cur.execute("SELECT n FROM seq")
        assert cur.rowcount == 5
        assert await cur.fetchone() == (1,)
        cur.arraysize = 2
        assert await cur.fetchmany(0) == []
        with pytest.raises(ProgrammingError):
            await cur.fetchmany(-1)
        assert await cur.fetchmany() == [(2,), (3,)]
        assert await cur.fetchall() == [(4,), (5,)]
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "cols", "rows", "tag", "expected_rowcount", "expect_desc", "expected_rows"),
    [
        ("CREATE TEMP TABLE t (id int)", (), [], b"CREATE TABLE", -1, False, None),
        ("INSERT INTO t VALUES (1), (2), (3)", (), [], b"INSERT 0 3", 3, False, None),
        (
            "INSERT INTO t VALUES (10), (20) RETURNING id",
            (("id", 23, 0),),
            [(b"10",), (b"20",)],
            b"INSERT 0 2",
            2,
            True,
            [(10,), (20,)],
        ),
        ("SELECT 1 WHERE false", (("col", 23, 0),), [], b"SELECT 0", 0, True, []),
    ],
)
async def test_async_cursor_command_ddl_and_empty_set(
    sql: str,
    cols: tuple[tuple[str, int, int], ...],
    rows: list[tuple[bytes | None, ...]],
    tag: bytes,
    expected_rowcount: int,
    expect_desc: bool,
    expected_rows: list[tuple[Any, ...]] | None,
) -> None:
    """Verify DDL, DML, DML RETURNING, and empty SELECT metadata on AsyncPostgresCursor."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        _read_until_sync_or_query(conn)
        conn.sendall(_extended_ok((), cols, rows, tag))
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        await cur.execute(sql)
        assert cur.rowcount == expected_rowcount
        if expect_desc:
            assert cur.description is not None
            assert await cur.fetchall() == expected_rows
        else:
            assert cur.description is None
            with pytest.raises(ProgrammingError):
                await cur.fetchall()
            assert cur.rowcount == expected_rowcount
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.anyio
async def test_async_cursor_executemany_and_call_time_param_errors() -> None:
    """Verify executemany rowcount summation, empty batch handling, and call-time InterfaceError on bad params."""
    seen_binds: list[list[bytes | None]] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        for _ in range(3):
            frames = _read_until_sync_or_query(conn)
            has_parse = any(tag == b"P" for tag, _ in frames)
            has_describe = any(tag == b"D" for tag, _ in frames)
            seen_binds.append(_extract_bind_params_from_frames(frames))
            conn.sendall(
                _extended_ok(
                    (23,),
                    (),
                    [],
                    b"INSERT 0 1",
                    has_parse=has_parse,
                    has_describe=has_describe,
                )
            )
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1", timeout=cast("Any", "bad"))
        with pytest.raises(InterfaceError):
            cur.executemany("INSERT INTO t VALUES (%s)", cast("Any", 123))

        returned = await cur.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,), (3,)])
        assert returned is cur
        assert cur.rowcount == 3
        assert cur.description is None

        returned_empty = await cur.executemany("INSERT INTO t VALUES (%s)", [])
        assert returned_empty is cur
        assert cur.rowcount == 0
        assert cur.description is None
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors
    assert len(seen_binds) == 3


@pytest.mark.anyio
async def test_async_cursor_error_class_integrity_and_clearing() -> None:
    """Verify duplicate key SQLSTATE 23505 raises IntegrityError and clears prior cursor result."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        _read_until_sync_or_query(conn)
        conn.sendall(_extended_ok((), (("id", 23, 0),), [(b"1",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)
        conn.sendall(_error_response("23505", "duplicate key value violates unique constraint") + _frame(b"Z", b"I"))

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        await cur.execute("SELECT 1 AS id")
        assert cur.description is not None
        assert cur.rowcount == 1

        with pytest.raises(IntegrityError) as exc_info:
            await cur.execute("INSERT INTO t VALUES (1)")
        assert getattr(exc_info.value, "sqlstate", None) == "23505"
        assert cur.description is None
        assert cur.rowcount == -1
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.anyio
async def test_async_cursor_concurrent_execute_raises_interface_error() -> None:
    """Verify starting a second execute while one is in flight raises InterfaceError with the expected message."""
    query_received = threading.Event()
    release_response = threading.Event()

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())

        _read_until_sync_or_query(conn)
        query_received.set()
        assert release_response.wait(timeout=5.0)
        conn.sendall(_extended_ok((), (("v", 23, 0),), [(b"7",)], b"SELECT 1"))

        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()

        async def _run_first() -> AsyncPostgresCursor:
            return await cur.execute("SELECT 7 AS v")

        first_task = asyncio.create_task(_run_first())
        assert await asyncio.to_thread(query_received.wait, 5.0)

        with pytest.raises(InterfaceError, match="cursor is already executing"):
            await cur.execute("SELECT 8")

        release_response.set()
        await first_task
        assert await cur.fetchall() == [(7,)]
    finally:
        release_response.set()
        await client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.anyio
async def test_async_cursor_cancellation_cancels_server_and_recovers() -> None:
    """Verify cancelling an awaiting task sends CancelRequest, clears cursor state, and allows the next execute."""
    sleep_started = threading.Event()
    cancel_received = threading.Event()
    cancel_payload: list[bytes] = []
    errors: list[BaseException] = []

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    port = srv.getsockname()[1]

    def _serve_main(conn: socket.socket) -> None:
        with conn:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())

            f1 = _read_until_sync_or_query(conn)
            assert _extract_sql_from_frames(f1) == "SELECT 10"
            conn.sendall(_extended_ok((), (("v", 23, 0),), [(b"10",)], b"SELECT 1"))

            f2 = _read_until_sync_or_query(conn)
            assert "pg_sleep" in _extract_sql_from_frames(f2)
            sleep_started.set()
            assert cancel_received.wait(timeout=5.0)
            conn.sendall(_error_response("57014", "canceling statement due to user request") + _frame(b"Z", b"I"))

            _read_until_sync_or_query(conn)

    def _runner() -> None:
        try:
            main_conn, _ = srv.accept()
            main_thread = threading.Thread(target=_serve_main, args=(main_conn,), daemon=True)
            main_thread.start()

            cancel_conn, _ = srv.accept()
            with cancel_conn:
                data = cancel_conn.recv(16)
                cancel_payload.append(data)
                cancel_received.set()

            main_thread.join(timeout=5.0)
        except BaseException as exc:
            errors.append(exc)
        finally:
            srv.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        await cur.execute("SELECT 10")
        assert cur.description is not None

        async def _run_sleep() -> AsyncPostgresCursor:
            return await cur.execute("SELECT pg_sleep(5)")

        task = asyncio.create_task(_run_sleep())
        assert await asyncio.to_thread(sleep_started.wait, 5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cur.description is None
        assert cur.rowcount == -1

        with pytest.raises(InterfaceError, match="session is not connected"):
            await cur.execute("SELECT 1")
        assert client.closed
    finally:
        await client.close()
        thread.join(timeout=5.0)

    assert not errors
    assert cancel_payload == [struct.pack("!iiii", 16, 80877102, 101, 202)]


@pytest.mark.anyio
async def test_async_cursor_lazy_begin_and_default_cursor() -> None:
    """Verify lazy_begin opens transactions on demand, reopens after commit, clears on failed begin, and default opens none."""
    seen_commands: list[str] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        tx_status = b"I"
        inserted_rows: list[int] = []
        committed_rows: list[int] = []
        fail_next_begin = False
        prepared: dict[bytes, str] = {}

        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            is_simple = any(tag == b"Q" for tag, _ in frames)
            has_parse = any(tag == b"P" for tag, _ in frames)
            has_describe = any(tag == b"D" for tag, _ in frames)
            sql = _extract_sql_from_frames(frames, prepared)
            seen_commands.append(sql)

            if sql.upper() == "BEGIN":
                if fail_next_begin:
                    fail_next_begin = False
                    conn.sendall(_error_response("40001", "begin failed") + _frame(b"Z", b"I"))
                else:
                    tx_status = b"T"
                    conn.sendall(
                        _extended_ok(
                            (),
                            (),
                            [],
                            b"BEGIN",
                            tx_status,
                            is_simple=is_simple,
                            has_parse=has_parse,
                            has_describe=has_describe,
                        )
                    )
            elif sql.upper() == "ROLLBACK":
                inserted_rows.clear()
                tx_status = b"I"
                conn.sendall(
                    _extended_ok(
                        (),
                        (),
                        [],
                        b"ROLLBACK",
                        tx_status,
                        is_simple=is_simple,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql.upper() == "COMMIT":
                committed_rows.extend(inserted_rows)
                inserted_rows.clear()
                tx_status = b"I"
                conn.sendall(
                    _extended_ok(
                        (),
                        (),
                        [],
                        b"COMMIT",
                        tx_status,
                        is_simple=is_simple,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql == "SELECT arm_fail_begin":
                fail_next_begin = True
                conn.sendall(
                    _extended_ok(
                        (),
                        (("ok", 23, 0),),
                        [(b"1",)],
                        b"SELECT 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            elif sql == "SELECT COUNT(*) FROM t":
                total = len(committed_rows) + len(inserted_rows)
                if has_parse:
                    c_cols = (("c", 23, 0),)
                    c_rows = [(str(total).encode(),)]
                else:
                    c_cols = (("c", 23, 1),)
                    c_rows = [(struct.pack("!i", total),)]
                conn.sendall(
                    _extended_ok(
                        (),
                        c_cols,
                        c_rows,
                        b"SELECT 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )
            else:
                inserted_rows.append(1)
                if tx_status == b"I":
                    committed_rows.extend(inserted_rows)
                    inserted_rows.clear()
                param_oids = (23,) if "$1" in sql else ()
                conn.sendall(
                    _extended_ok(
                        param_oids,
                        (),
                        [],
                        b"INSERT 0 1",
                        tx_status,
                        has_parse=has_parse,
                        has_describe=has_describe,
                    )
                )

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        default_cur = client.cursor()
        await default_cur.execute("INSERT INTO t VALUES (1)")
        assert seen_commands == ["INSERT INTO t VALUES (1)"]

        lazy_cur = client.cursor(lazy_begin=True)
        await lazy_cur.execute("INSERT INTO t VALUES (2)")
        await lazy_cur.execute("SELECT COUNT(*) FROM t")
        assert await lazy_cur.fetchall() == [(2,)]
        await client.rollback()

        await default_cur.execute("SELECT COUNT(*) FROM t")
        assert await default_cur.fetchall() == [(1,)]

        await lazy_cur.execute("INSERT INTO t VALUES (3)")
        await client.commit()

        await lazy_cur.executemany("INSERT INTO t VALUES (%s)", [(4,), (5,)])
        assert lazy_cur.rowcount == 2
        await client.commit()

        await default_cur.execute("SELECT COUNT(*) FROM t")
        assert await default_cur.fetchall() == [(4,)]

        await lazy_cur.execute("SELECT COUNT(*) FROM t")
        assert lazy_cur.description is not None
        await client.rollback()

        await default_cur.execute("SELECT arm_fail_begin")
        assert await default_cur.fetchall() == [(1,)]
        with pytest.raises(OperationalError):
            await lazy_cur.execute("INSERT INTO t VALUES (6)")
        assert lazy_cur.description is None
        assert lazy_cur.rowcount == -1
    finally:
        await client.close()
        thread.join(timeout=5.0)

    assert not errors
    begin_count = sum(1 for cmd in seen_commands if cmd.upper() == "BEGIN")
    assert begin_count == 5


@pytest.mark.anyio
async def test_async_cursor_client_lease_attach_release_close_and_drop() -> None:
    """Verify attach_lease, release_lease, close, and GC drop release semantics on AsyncPostgresClient."""
    calls_a = 0

    def _release_a() -> None:
        nonlocal calls_a
        calls_a += 1

    client1 = _make_async_client(1)
    client1.attach_lease(_release_a)
    client1.release_lease()
    client1.release_lease()
    assert calls_a == 1

    calls_b = 0

    def _release_b() -> None:
        nonlocal calls_b
        calls_b += 1

    client2 = _make_async_client(1)
    client2.attach_lease(_release_b)
    del client2
    gc.collect()
    assert calls_b == 1

    calls_c = 0
    calls_d = 0

    def _release_c() -> None:
        nonlocal calls_c
        calls_c += 1

    def _release_d() -> None:
        nonlocal calls_d
        calls_d += 1

    client3 = _make_async_client(1)
    client3.attach_lease(_release_c)
    client3.attach_lease(_release_d)
    assert calls_c == 1
    assert calls_d == 0
    await client3.close()
    await client3.close()
    assert calls_d == 1


def _reply_per_frame_extended(
    frames: Sequence[tuple[bytes, bytes]],
    param_oids: Sequence[int],
    cols: Sequence[tuple[str, int]],
    rows: Sequence[Sequence[str | None]],
    command_tag: str,
    tx_status: bytes = b"I",
) -> bytes:
    """Answer extended frames per-frame, matching Bind result format codes for cold and cached runs."""
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


def _resolve_batch_sql(frames: Sequence[tuple[bytes, bytes]], prepared: dict[str, str]) -> str:
    """Resolve SQL for a cold or cached extended batch, recording newly parsed statement names."""
    for tag, body in frames:
        if tag == b"Q":
            return body.rstrip(b"\x00").decode("utf-8")
        if tag == b"P":
            stmt_name, sql = _parse_frame_stmt_and_sql(body)
            prepared[stmt_name] = sql
            return sql
    for tag, body in frames:
        if tag == b"B":
            stmt_name, _, _ = _bind_frame_stmt_params_and_formats(body)
            return prepared.get(stmt_name, "")
    return ""


def test_sync_single_call_select_equality_dicts_and_null() -> None:
    """Verify fetch_rows matches cursor output on cold and cached runs, fetch_dicts builds dicts, and fetch_value returns NULL as None."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            if sql == "SELECT 1 AS a, 'x' AS b":
                conn.sendall(
                    _reply_per_frame_extended(
                        frames,
                        (),
                        (("a", 23), ("b", 25)),
                        [("1", "x")],
                        "SELECT 1",
                    )
                )
            elif sql == "SELECT 1 AS dup, 2 AS dup":
                conn.sendall(
                    _reply_per_frame_extended(
                        frames,
                        (),
                        (("dup", 23), ("dup", 23)),
                        [("1", "2")],
                        "SELECT 1",
                    )
                )
            elif sql == "SELECT NULL":
                conn.sendall(
                    _reply_per_frame_extended(
                        frames,
                        (),
                        (("val", 25),),
                        [(None,)],
                        "SELECT 1",
                    )
                )
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        sql = "SELECT 1 AS a, 'x' AS b"
        cur = client.cursor()
        cur.execute(sql)
        cur_rows = cur.fetchall()
        assert cur.description is not None
        cur_names = tuple(col[0] for col in cur.description)
        cur.close()

        rows, names = client.fetch_rows(sql)
        assert rows == cur_rows
        assert names == cur_names

        assert client.fetch_dicts(sql) == [{"a": 1, "b": "x"}]
        assert client.fetch_dicts("SELECT 1 AS dup, 2 AS dup") == [{"dup": 2}]
        assert client.fetch_value("SELECT NULL") is None
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors


def test_sync_single_call_empty_result_no_result_set_dml_and_returning() -> None:
    """Verify empty result sets, DDL without result set, DML counts, and INSERT RETURNING on SyncPostgresClient."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            if sql == "SELECT 1 AS a WHERE false":
                conn.sendall(_reply_per_frame_extended(frames, (), (("a", 23),), [], "SELECT 0"))
            elif sql == "CREATE TABLE t (id int)":
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "CREATE TABLE"))
            elif sql == "UPDATE t SET v = 1":
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "UPDATE 2"))
            elif sql == "INSERT INTO t VALUES (1) RETURNING id":
                conn.sendall(_reply_per_frame_extended(frames, (), (("id", 23),), [("42",)], "INSERT 0 1"))
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        assert client.fetch_rows("SELECT 1 AS a WHERE false") == ([], ("a",))
        assert client.fetch_dicts("SELECT 1 AS a WHERE false") == []
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            client.fetch_value("SELECT 1 AS a WHERE false")

        assert client.fetch_rows("CREATE TABLE t (id int)") == ([], ())
        assert client.fetch_dicts("CREATE TABLE t (id int)") == []
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            client.fetch_value("CREATE TABLE t (id int)")
        assert client.execute_dml("CREATE TABLE t (id int)") == (0, None)

        assert client.execute_dml("UPDATE t SET v = 1") == (2, None)

        assert client.fetch_rows("INSERT INTO t VALUES (1) RETURNING id") == ([(42,)], ("id",))
        assert client.execute_dml("INSERT INTO t VALUES (1) RETURNING id") == (1, None)
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors


def test_sync_single_call_parameters_and_validation_errors() -> None:
    """Verify named params, empty params, mismatched positional params, and unsupported parameter errors."""
    seen_sql: list[str] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            seen_sql.append(sql)
            if sql == "SELECT $1::int":
                conn.sendall(_reply_per_frame_extended(frames, (23,), (("n", 23),), [("7",)], "SELECT 1"))
            elif sql == "SELECT 1":
                conn.sendall(_reply_per_frame_extended(frames, (), (("v", 23),), [("1",)], "SELECT 1"))
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        assert client.fetch_value("SELECT %(n)s::int", {"n": 7}) == 7
        assert client.fetch_value("SELECT 1", ()) == 1

        with pytest.raises(ProgrammingError):
            client.fetch_value("SELECT 1", (1,))
        with pytest.raises(InterfaceError):
            client.fetch_value("SELECT %s", (PG_UNCHANGED_TOAST,))
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors
    assert seen_sql == ["SELECT $1::int", "SELECT 1"]


def test_sync_single_call_timeout_and_server_error() -> None:
    """Verify unique violation raises IntegrityError(23505) and timeout matches cursor exception class."""

    def _serve_unique(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        _read_until_sync_or_query(conn)
        conn.sendall(_error_response("23505", "duplicate key value violates unique constraint") + _frame(b"Z", b"I"))
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve_unique)
    client = _make_sync_client(port)
    client.connect()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            client.execute_dml("INSERT INTO t VALUES (1)")
        assert exc_info.value.sqlstate == "23505"
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors

    def _run_timeout(use_single_call: bool) -> type[BaseException]:
        def _serve_delay(conn: socket.socket) -> None:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())
            _read_until_sync_or_query(conn)
            time.sleep(0.3)

        t_port, t_thread, _t_errors, _ = _run_loopback_pg(_serve_delay)
        t_client = _make_sync_client(t_port)
        t_client.connect()
        try:
            if use_single_call:
                with pytest.raises(OperationalError) as exc_info:
                    t_client.fetch_value("SELECT pg_sleep(1)", timeout=0.05)
                return type(exc_info.value)
            cur = t_client.cursor()
            with pytest.raises(OperationalError) as exc_info:
                cur.execute("SELECT pg_sleep(1)", timeout=0.05)
            return type(exc_info.value)
        finally:
            t_client.close()
            t_thread.join(timeout=5.0)

    single_exc = _run_timeout(use_single_call=True)
    cursor_exc = _run_timeout(use_single_call=False)
    assert single_exc is cursor_exc
    assert issubclass(single_exc, OperationalError)


def test_sync_single_call_in_transaction_rollback() -> None:
    """Verify client single-call methods run inside an open transaction without starting one and respect rollback."""
    seen_commands: list[str] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        tx_status = b"I"
        staged: list[int] = []
        committed: list[int] = []
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            seen_commands.append(sql)
            upper = sql.upper()
            if upper == "BEGIN":
                tx_status = b"T"
                staged = list(committed)
                conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", tx_status))
            elif upper == "ROLLBACK":
                tx_status = b"I"
                staged = list(committed)
                conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx_status))
            elif upper.startswith("INSERT"):
                if tx_status == b"T":
                    staged.append(1)
                else:
                    committed.append(1)
                    staged = list(committed)
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "INSERT 0 1", tx_status))
            elif "COUNT(*)" in upper:
                count = len(staged) if tx_status == b"T" else len(committed)
                conn.sendall(
                    _reply_per_frame_extended(frames, (), (("count", 23),), [(str(count),)], "SELECT 1", tx_status)
                )
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_sync_client(port)
    client.connect()
    try:
        client.begin()
        assert client.execute_dml("INSERT INTO t VALUES (1)") == (1, None)
        assert client.fetch_value("SELECT COUNT(*) FROM t") == 1
        client.rollback()
        assert client.fetch_value("SELECT COUNT(*) FROM t") == 0
    finally:
        client.close()
        thread.join(timeout=5.0)
    assert not errors
    assert sum(1 for cmd in seen_commands if cmd.upper() == "BEGIN") == 1


@pytest.mark.anyio
async def test_async_cursor_fetch_ready_does_not_iterate_loop() -> None:
    """Verify fetchone, fetchmany, fetchall, and close return ReadyAwaitable and complete without a loop turn."""

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        _read_until_sync_or_query(conn)
        conn.sendall(
            _extended_ok(
                (),
                (("n", 23, 0),),
                [(b"1",), (b"2",), (b"3",), (b"4",)],
                b"SELECT 4",
            )
        )
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        cur = client.cursor()
        await cur.execute("SELECT n FROM seq")
        loop = asyncio.get_running_loop()

        ran: list[int] = []
        aw_one = cur.fetchone()
        assert type(aw_one).__name__ == "ReadyAwaitable"
        loop.call_soon(ran.append, 1)
        assert await aw_one == (1,)
        assert ran == []
        await asyncio.sleep(0)
        assert ran == [1]

        ran.clear()
        aw_many = cur.fetchmany(2)
        assert type(aw_many).__name__ == "ReadyAwaitable"
        loop.call_soon(ran.append, 1)
        assert await aw_many == [(2,), (3,)]
        assert ran == []
        await asyncio.sleep(0)
        assert ran == [1]

        ran.clear()
        aw_all = cur.fetchall()
        assert type(aw_all).__name__ == "ReadyAwaitable"
        loop.call_soon(ran.append, 1)
        assert await aw_all == [(4,)]
        assert ran == []
        await asyncio.sleep(0)
        assert ran == [1]

        ran.clear()
        aw_close = cur.close()
        assert type(aw_close).__name__ == "ReadyAwaitable"
        loop.call_soon(ran.append, 1)
        await aw_close
        assert ran == []
        await asyncio.sleep(0)
        assert ran == [1]
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors


@pytest.mark.anyio
@pytest.mark.parametrize("method_name", ["fetchone", "fetchmany", "fetchall"])
async def test_async_cursor_fetch_ready_closed_error_on_await(method_name: str) -> None:
    """Verify fetch methods on a closed async cursor return ReadyAwaitable and raise InterfaceError on await."""
    client = _make_async_client(1)
    cur = client.cursor()
    await cur.close()
    aw = getattr(cur, method_name)()
    assert type(aw).__name__ == "ReadyAwaitable"
    with pytest.raises(InterfaceError):
        await aw


@pytest.mark.anyio
async def test_async_cursor_fetch_ready_negative_size_error_on_await() -> None:
    """Verify fetchmany(-1) on an open async cursor returns ReadyAwaitable and raises ProgrammingError on await."""
    client = _make_async_client(1)
    cur = client.cursor()
    aw = cur.fetchmany(-1)
    assert type(aw).__name__ == "ReadyAwaitable"
    with pytest.raises(ProgrammingError):
        await aw


@pytest.mark.anyio
async def test_async_single_call_select_dicts_empty_dml_and_tx() -> None:
    """Verify async client single-call methods match sync semantics across SELECT, dicts, NULL, empty, DDL, DML, RETURNING, params, and transactions."""
    seen_commands: list[str] = []

    def _serve(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        tx_status = b"I"
        staged: list[int] = []
        committed: list[int] = []
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            seen_commands.append(sql)
            upper = sql.upper()
            if upper == "BEGIN":
                tx_status = b"T"
                staged = list(committed)
                conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", tx_status))
            elif upper == "ROLLBACK":
                tx_status = b"I"
                staged = list(committed)
                conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx_status))
            elif sql == "SELECT 1 AS a, 'x' AS b":
                conn.sendall(
                    _reply_per_frame_extended(
                        frames,
                        (),
                        (("a", 23), ("b", 25)),
                        [("1", "x")],
                        "SELECT 1",
                        tx_status,
                    )
                )
            elif sql == "SELECT 1 AS dup, 2 AS dup":
                conn.sendall(
                    _reply_per_frame_extended(
                        frames,
                        (),
                        (("dup", 23), ("dup", 23)),
                        [("1", "2")],
                        "SELECT 1",
                        tx_status,
                    )
                )
            elif sql == "SELECT NULL":
                conn.sendall(_reply_per_frame_extended(frames, (), (("val", 25),), [(None,)], "SELECT 1", tx_status))
            elif sql == "SELECT 1 AS a WHERE false":
                conn.sendall(_reply_per_frame_extended(frames, (), (("a", 23),), [], "SELECT 0", tx_status))
            elif sql == "CREATE TABLE t (id int)":
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "CREATE TABLE", tx_status))
            elif sql == "UPDATE t SET v = 1":
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "UPDATE 2", tx_status))
            elif sql == "INSERT INTO t VALUES (1) RETURNING id":
                conn.sendall(_reply_per_frame_extended(frames, (), (("id", 23),), [("42",)], "INSERT 0 1", tx_status))
            elif sql == "SELECT $1::int":
                conn.sendall(_reply_per_frame_extended(frames, (23,), (("n", 23),), [("7",)], "SELECT 1", tx_status))
            elif sql == "SELECT 1":
                conn.sendall(_reply_per_frame_extended(frames, (), (("v", 23),), [("1",)], "SELECT 1", tx_status))
            elif sql == "INSERT INTO t VALUES (1)":
                if tx_status == b"T":
                    staged.append(1)
                else:
                    committed.append(1)
                    staged = list(committed)
                conn.sendall(_reply_per_frame_extended(frames, (), (), [], "INSERT 0 1", tx_status))
            elif sql == "SELECT COUNT(*) FROM t":
                count = len(staged) if tx_status == b"T" else len(committed)
                conn.sendall(
                    _reply_per_frame_extended(frames, (), (("count", 23),), [(str(count),)], "SELECT 1", tx_status)
                )
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    port, thread, errors, _ = _run_loopback_pg(_serve)
    client = _make_async_client(port)
    await client.connect()
    try:
        sql = "SELECT 1 AS a, 'x' AS b"
        cur = client.cursor()
        await cur.execute(sql)
        cur_rows = await cur.fetchall()
        assert cur.description is not None
        cur_names = tuple(col[0] for col in cur.description)
        await cur.close()

        rows, names = await client.fetch_rows(sql)
        assert rows == cur_rows
        assert names == cur_names
        assert await client.fetch_dicts(sql) == [{"a": 1, "b": "x"}]
        assert await client.fetch_dicts("SELECT 1 AS dup, 2 AS dup") == [{"dup": 2}]
        assert await client.fetch_value("SELECT NULL") is None

        assert await client.fetch_rows("SELECT 1 AS a WHERE false") == ([], ("a",))
        assert await client.fetch_dicts("SELECT 1 AS a WHERE false") == []
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await client.fetch_value("SELECT 1 AS a WHERE false")

        assert await client.fetch_rows("CREATE TABLE t (id int)") == ([], ())
        assert await client.fetch_dicts("CREATE TABLE t (id int)") == []
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await client.fetch_value("CREATE TABLE t (id int)")
        assert await client.execute_dml("CREATE TABLE t (id int)") == (0, None)

        assert await client.execute_dml("UPDATE t SET v = 1") == (2, None)
        assert await client.fetch_rows("INSERT INTO t VALUES (1) RETURNING id") == ([(42,)], ("id",))
        assert await client.execute_dml("INSERT INTO t VALUES (1) RETURNING id") == (1, None)

        assert await client.fetch_value("SELECT %(n)s::int", {"n": 7}) == 7
        assert await client.fetch_value("SELECT 1", ()) == 1

        await client.begin()
        assert await client.execute_dml("INSERT INTO t VALUES (1)") == (1, None)
        assert await client.fetch_value("SELECT COUNT(*) FROM t") == 1
        await client.rollback()
        assert await client.fetch_value("SELECT COUNT(*) FROM t") == 0
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors
    assert sum(1 for cmd in seen_commands if cmd.upper() == "BEGIN") == 1


@pytest.mark.anyio
async def test_async_single_call_errors_and_timeout() -> None:
    """Verify call-time validation errors, server IntegrityError on await, and timeout matching AsyncPostgresCursor."""

    def _serve_unique(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        _read_until_sync_or_query(conn)
        conn.sendall(_error_response("23505", "duplicate key value violates unique constraint") + _frame(b"Z", b"I"))
        _read_until_sync_or_query(conn)

    port, thread, errors, _ = _run_loopback_pg(_serve_unique)
    client = _make_async_client(port)
    await client.connect()
    try:
        with pytest.raises(ProgrammingError):
            client.fetch_rows("SELECT 1", (1,))
        with pytest.raises(InterfaceError):
            client.fetch_value("SELECT %s", (PG_UNCHANGED_TOAST,))
        with pytest.raises(IntegrityError) as exc_info:
            await client.execute_dml("INSERT INTO t VALUES (1)")
        assert exc_info.value.sqlstate == "23505"
    finally:
        await client.close()
        thread.join(timeout=5.0)
    assert not errors

    async def _run_timeout(use_single_call: bool) -> type[BaseException]:
        def _serve_delay(conn: socket.socket) -> None:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())
            _read_until_sync_or_query(conn)
            time.sleep(0.3)

        t_port, t_thread, _t_errors, _ = _run_loopback_pg(_serve_delay)
        t_client = _make_async_client(t_port)
        await t_client.connect()
        try:
            if use_single_call:
                with pytest.raises(OperationalError) as exc_info:
                    await t_client.fetch_value("SELECT pg_sleep(1)", timeout=0.05)
                return type(exc_info.value)
            cur = t_client.cursor()
            with pytest.raises(OperationalError) as exc_info:
                await cur.execute("SELECT pg_sleep(1)", timeout=0.05)
            return type(exc_info.value)
        finally:
            await t_client.close()
            t_thread.join(timeout=5.0)

    single_exc = await _run_timeout(use_single_call=True)
    cursor_exc = await _run_timeout(use_single_call=False)
    assert single_exc is cursor_exc
    assert issubclass(single_exc, OperationalError)


@pytest.mark.anyio
async def test_async_single_call_cancellation() -> None:
    """Verify cancelling an in-flight fetch_rows sends CancelRequest and cancelling a queued call leaves the client usable."""
    first_started = threading.Event()
    release_first = threading.Event()

    def _serve_queued(conn: socket.socket) -> None:
        _read_startup(conn)
        conn.sendall(_auth_ok_and_ready())
        prepared: dict[str, str] = {}
        while True:
            frames = _read_until_sync_or_query(conn)
            if any(tag == b"X" for tag, _ in frames):
                break
            sql = _resolve_batch_sql(frames, prepared)
            if sql == "SELECT 1 AS first":
                first_started.set()
                assert release_first.wait(timeout=5.0)
                conn.sendall(_reply_per_frame_extended(frames, (), (("first", 23),), [("1",)], "SELECT 1"))
            elif sql == "SELECT 3 AS after_cancel":
                conn.sendall(_reply_per_frame_extended(frames, (), (("after_cancel", 23),), [("3",)], "SELECT 1"))
            else:
                raise AssertionError(f"unexpected SQL: {sql!r}")

    q_port, q_thread, q_errors, _ = _run_loopback_pg(_serve_queued)
    q_client = _make_async_client(q_port)
    await q_client.connect()
    try:

        async def _run_first() -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            return await q_client.fetch_rows("SELECT 1 AS first")

        async def _run_queued() -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            return await q_client.fetch_rows("SELECT 2 AS cancelled")

        first_task = asyncio.create_task(_run_first())
        assert await asyncio.to_thread(first_started.wait, 5.0)

        queued_task = asyncio.create_task(_run_queued())
        await asyncio.sleep(0)
        queued_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued_task

        release_first.set()
        assert await first_task == ([(1,)], ("first",))
        assert await q_client.fetch_rows("SELECT 3 AS after_cancel") == ([(3,)], ("after_cancel",))
    finally:
        release_first.set()
        await q_client.close()
        q_thread.join(timeout=5.0)
    assert not q_errors

    sleep_started = threading.Event()
    cancel_received = threading.Event()
    cancel_payload: list[bytes] = []
    errors: list[BaseException] = []

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    port = srv.getsockname()[1]

    def _serve_main(conn: socket.socket) -> None:
        with conn:
            _read_startup(conn)
            conn.sendall(_auth_ok_and_ready())

            f1 = _read_until_sync_or_query(conn)
            assert "pg_sleep" in _extract_sql_from_frames(f1)
            sleep_started.set()
            assert cancel_received.wait(timeout=5.0)
            conn.sendall(_error_response("57014", "canceling statement due to user request") + _frame(b"Z", b"I"))

            _read_until_sync_or_query(conn)

    def _runner() -> None:
        try:
            main_conn, _ = srv.accept()
            main_thread = threading.Thread(target=_serve_main, args=(main_conn,), daemon=True)
            main_thread.start()

            cancel_conn, _ = srv.accept()
            with cancel_conn:
                data = cancel_conn.recv(16)
                cancel_payload.append(data)
                cancel_received.set()

            main_thread.join(timeout=5.0)
        except BaseException as exc:
            errors.append(exc)
        finally:
            srv.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    client = _make_async_client(port)
    await client.connect()
    try:

        async def _run_sleep() -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            return await client.fetch_rows("SELECT pg_sleep(5)")

        task = asyncio.create_task(_run_sleep())
        assert await asyncio.to_thread(sleep_started.wait, 5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert await asyncio.to_thread(cancel_received.wait, 5.0)
        with pytest.raises(InterfaceError, match="session is not connected"):
            await client.fetch_rows("SELECT 1")
        assert client.closed
    finally:
        await client.close()
        thread.join(timeout=5.0)

    assert not errors
    assert cancel_payload == [struct.pack("!iiii", 16, 80877102, 101, 202)]
