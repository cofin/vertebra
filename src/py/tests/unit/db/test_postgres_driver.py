"""Unit tests for SyncPostgreSQLDriver, AsyncPostgreSQLDriver, and Arrow binary COPY streaming."""

from __future__ import annotations

import asyncio
import inspect
import socket
import struct
import sys
import tempfile
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient
from vertebra._vertebra import (
    ConnectionFailure,
    DatabaseError,
    GoogleCloudAuthError,
    InterfaceError,
    LeaseBroker,
)
from vertebra.db import (
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    DatabaseRegistry,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
)
from vertebra.db.adapters.postgres import (
    PGCOPY_BINARY_SIGNATURE,
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.adapters.postgres.options import build_native_client_options
from vertebra.db.dialer import aws_rds_root_ca_pem
from vertebra.db.driver import (
    AZURE_POSTGRES_SCOPE,
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache


@pytest.mark.anyio
async def test_async_postgres_pool_transaction_and_affected_rows() -> None:
    """Async PostgreSQL transactions own the acquired connection and report command status counts."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster)
    driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        assert await driver.execute("CREATE TABLE affected_rows (value INTEGER)") == 0
        async with driver.transaction():
            assert await driver.execute("INSERT INTO affected_rows VALUES ($1)", (1,)) == 1
            assert await driver.execute_many("INSERT INTO affected_rows VALUES ($1)", [(2,), (3,)]) == 2
        assert await driver.execute("UPDATE affected_rows SET value = value + 1 WHERE value >= $1", (0,)) == 3
        assert await driver.execute("DELETE FROM affected_rows WHERE value >= $1", (99,)) == 0
        assert await driver.execute_tuples("SELECT value FROM affected_rows ORDER BY value") == [(2,), (3,), (4,)]
        assert len(pool.active) == 0
        assert len(pool.idle) >= 1
    finally:
        await driver.close()


@pytest.mark.anyio
async def test_async_postgres_pool_rollback_and_cancel_release_connection() -> None:
    """Aborted and cancelled transactions discard writes before releasing their connection."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster)
    driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        await driver.execute("CREATE TABLE affected_rows (value INTEGER)")
        with pytest.raises(ValueError, match="abort"):
            async with driver.transaction():
                await driver.execute("INSERT INTO affected_rows VALUES ($1)", (1,))
                raise ValueError("abort")
        assert await driver.select_value("SELECT COUNT(*) FROM affected_rows") == 0
        inserted = asyncio.Event()

        async def insert_until_cancelled() -> None:
            async with driver.transaction():
                await driver.execute("INSERT INTO affected_rows VALUES ($1)", (2,))
                inserted.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(insert_until_cancelled())
        await inserted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await driver.select_value("SELECT COUNT(*) FROM affected_rows") == 0
        assert len(pool.active) == 0
        async with driver.transaction():
            await driver.execute("INSERT INTO affected_rows VALUES ($1)", (3,))
        assert await driver.execute_tuples("SELECT value FROM affected_rows") == [(3,)]
    finally:
        await driver.close()


@pytest.mark.anyio
async def test_async_postgres_pool_failed_begin_evicts_connection() -> None:
    """A failed transaction start evicts the leased client and permits a retry."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster)
    fail_once = True

    class _FailingBeginPool(FakeAsyncPostgresPool):
        async def acquire(self) -> Any:
            client = await super().acquire()
            nonlocal fail_once
            if fail_once:
                fail_once = False
                orig_begin = client.begin

                async def broken_begin() -> None:
                    raise RuntimeError("begin failed")

                client.__dict__["begin"] = broken_begin
                _ = orig_begin
            return client

    wrapped_pool = _FailingBeginPool(cluster)
    driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=wrapped_pool)
    try:
        with pytest.raises(RuntimeError, match="begin failed"):
            await driver.begin()
        assert len(wrapped_pool.active) == 0
        assert len(wrapped_pool.evicted) == 1
        async with driver.transaction():
            assert await driver.select_value("SELECT 1") == 1
    finally:
        _ = pool
        await driver.close()


def _build_test_pgcopy_payload(
    rows: Sequence[Sequence[bytes | None]],
    flags: int = 0,
    ext_bytes: bytes = b"",
) -> bytes:
    """Build a valid PostgreSQL binary COPY stream payload for test validation."""
    buf = bytearray(PGCOPY_BINARY_SIGNATURE)
    buf.extend(struct.pack(">II", flags, len(ext_bytes)))
    buf.extend(ext_bytes)
    for row in rows:
        buf.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                buf.extend(struct.pack(">i", -1))
            else:
                buf.extend(struct.pack(">i", len(field)))
                buf.extend(field)
    buf.extend(struct.pack(">h", -1))
    return bytes(buf)


def test_sync_postgres_driver_pool_gucs_and_prepared_statements() -> None:
    """Verify worker pool budget calculation and sync driver pool execution."""
    budget = PostgreSQLConfig.compute_worker_pool_budget(
        100,
        replica_count=3,
        workers_per_replica=3,
        reserved_admin_connections=10,
    )
    assert budget == 10

    bad_kwargs: dict[str, Any] = {"reserved_super_connections": 10}
    with pytest.raises(TypeError):
        PostgreSQLConfig.compute_worker_pool_budget(
            100,
            replica_count=3,
            workers_per_replica=3,
            **bad_kwargs,
        )

    cfg = PostgreSQLConfig(
        dsn="postgresql://user:pass@127.0.0.1:5432/appdb",
        statement_timeout_ms=5000,
        idle_in_transaction_session_timeout_ms=10000,
        session_gucs={"work_mem": "64MB"},
    )
    gucs = cfg.build_session_gucs()
    assert gucs["statement_timeout"] == "5000"
    assert gucs["idle_in_transaction_session_timeout"] == "10000"
    assert gucs["work_mem"] == "64MB"
    assert gucs["application_name"] == "vertebra"
    assert cfg.effective_statement_cache_size == 256

    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    cluster._conn.execute("INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta')")
    pool = FakeSyncPostgresPool(cluster)
    driver = SyncPostgreSQLDriver(cfg, pool=pool)
    rows, desc = driver.execute_with_description("SELECT id, name FROM items WHERE id > $1", (0,))
    assert rows == [(1, "alpha"), (2, "beta")]
    assert desc == ("id", "name")

    driver.begin()
    driver.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (3, "gamma"))
    driver.commit()
    driver.begin()
    driver.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (4, "rolled"))
    driver.rollback()
    assert driver.execute_tuples("SELECT id FROM items ORDER BY id") == [(1,), (2,), (3,)]
    driver.close()
    assert pool.closed is True
    assert driver.closed is True


@pytest.mark.anyio
async def test_postgres_sync_and_async_guards_name_native_postgres_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync and async PostgreSQL drivers require vertebra._postgres and name vertebra-core[postgres]."""
    cfg = PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")
    monkeypatch.setitem(sys.modules, "vertebra._postgres", None)
    reset_dependency_cache("vertebra._postgres")

    sync_driver = SyncPostgreSQLDriver(cfg)
    with pytest.raises(MissingDependencyError) as sync_exc:
        sync_driver.execute_tuples("SELECT 1")
    assert sync_exc.value.package == "vertebra-core[postgres]"
    assert sync_exc.value.install_package == "vertebra-core[postgres]"

    async_driver = AsyncPostgreSQLDriver(cfg)
    with pytest.raises(MissingDependencyError) as async_exc:
        await async_driver.execute_tuples("SELECT 1")
    assert async_exc.value.package == "vertebra-core[postgres]"
    assert async_exc.value.install_package == "vertebra-core[postgres]"


def test_pgcopy_binary_parser_success_and_error_handling() -> None:
    """Verify parsing valid and malformed PGCOPY binary stream framing."""
    valid_rows: list[list[bytes | None]] = [
        [b"first", None],
        [b"second", b"val2"],
    ]
    payload = _build_test_pgcopy_payload(valid_rows)
    parsed = SyncPostgreSQLDriver.parse_pgcopy_binary_rows(payload)
    assert parsed == [(b"first", None), (b"second", b"val2")]

    empty_payload = _build_test_pgcopy_payload([])
    assert SyncPostgreSQLDriver.parse_pgcopy_binary_rows(empty_payload) == []

    with pytest.raises(ValueError, match="Invalid PGCOPY binary signature"):
        SyncPostgreSQLDriver.parse_pgcopy_binary_rows(b"BAD_SIGNATURE_EXTRA")

    with pytest.raises(ValueError, match="Truncated PGCOPY header"):
        SyncPostgreSQLDriver.parse_pgcopy_binary_rows(PGCOPY_BINARY_SIGNATURE + b"\x00\x00")

    with pytest.raises(ValueError, match="Unexpected EOF before PGCOPY trailer"):
        bad_trailer = _build_test_pgcopy_payload([[b"v"]]).rstrip(b"\xff\xff")
        SyncPostgreSQLDriver.parse_pgcopy_binary_rows(bad_trailer)


@pytest.mark.anyio
async def test_async_postgres_driver_and_arrow_copy_streaming() -> None:
    """Verify AsyncPostgreSQLDriver and stream_arrow_batches chunking."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE tbl (id INTEGER, name TEXT)")
    cluster._conn.execute("INSERT INTO tbl (id, name) VALUES (1, 'x'), (2, 'y')")
    cluster._conn.execute("CREATE TABLE kv (k TEXT, v TEXT)")
    cluster._conn.execute("INSERT INTO kv (k, v) VALUES ('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')")
    pool = FakeAsyncPostgresPool(cluster)
    async_driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    tuples = await async_driver.execute_tuples("SELECT id, name FROM tbl ORDER BY id")
    assert tuples == [(1, "x"), (2, "y")]

    batches = [
        batch
        async for batch in async_driver.stream_arrow_batches(
            "SELECT k, v FROM kv ORDER BY k",
            batch_size=2,
        )
    ]
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1

    await async_driver.begin()
    await async_driver.commit()
    await async_driver.begin()
    await async_driver.rollback()
    await async_driver.close()
    assert pool.closed is True
    assert async_driver.closed is True


def test_database_registry_attaches_cloudsql_and_alloydb_dialers_to_postgres_drivers() -> None:
    """Verify DatabaseRegistry attaches CloudSQLDialer and AlloyDBDialer to sync and async drivers."""
    registry = DatabaseRegistry()
    registry.register(
        "csql_pg",
        PostgreSQLConfig(
            dsn="postgresql://app@127.0.0.1:5432/db",
            cloud_sql_instance="proj:us-central1:pg1",
            enable_iam_auth=True,
        ),
    )
    registry.register(
        "alloy_pg",
        PostgreSQLConfig(
            dsn="postgresql://app@127.0.0.1:5432/db",
            alloydb_instance_uri="projects/p/locations/l/clusters/c/instances/i",
        ),
    )

    csql_async = registry.get_driver("csql_pg")
    assert isinstance(csql_async, AsyncPostgreSQLDriver)
    assert isinstance(csql_async.dialer, AsyncCloudSQLDialer)

    csql_sync = registry.get_sync_driver("csql_pg")
    assert isinstance(csql_sync, SyncPostgreSQLDriver)
    assert isinstance(csql_sync.dialer, SyncCloudSQLDialer)

    alloy_async = registry.get_driver("alloy_pg")
    assert isinstance(alloy_async, AsyncPostgreSQLDriver)
    assert isinstance(alloy_async.dialer, AsyncAlloyDBDialer)
    assert alloy_async.config.build_session_gucs().get("alloydb.enable_columnar_engine") == "on"

    alloy_sync = registry.get_sync_driver("alloy_pg")
    assert isinstance(alloy_sync, SyncPostgreSQLDriver)
    assert isinstance(alloy_sync.dialer, SyncAlloyDBDialer)
    assert alloy_sync.config.build_session_gucs().get("alloydb.enable_columnar_engine") == "on"


def _pg_frame(tag: bytes, payload: bytes) -> bytes:
    """Build a length-prefixed PostgreSQL wire frame."""
    return tag + struct.pack(">i", len(payload) + 4) + payload


def _pg_trust_greeting_frames(server_version: str = "17.2") -> bytes:
    """Build AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery(Idle) frames."""
    auth_ok = _pg_frame(b"R", struct.pack(">i", 0))
    ps_enc = _pg_frame(b"S", b"client_encoding\x00UTF8\x00")
    ps_ver = _pg_frame(b"S", b"server_version\x00" + server_version.encode("utf-8") + b"\x00")
    key_data = _pg_frame(b"K", struct.pack(">ii", 4242, 987654))
    ready = _pg_frame(b"Z", b"I")
    return auth_ok + ps_enc + ps_ver + key_data + ready


def _pg_error_frame(sqlstate: str, message: str) -> bytes:
    """Build a PostgreSQL ErrorResponse wire frame."""
    body = b"SERROR\x00VERROR\x00C" + sqlstate.encode("ascii") + b"\x00M" + message.encode("utf-8") + b"\x00\x00"
    return _pg_frame(b"E", body)


def _recv_exact(conn: socket.socket, count: int) -> bytes:
    """Read an exact byte count from a blocking socket."""
    chunks = bytearray()
    while len(chunks) < count:
        part = conn.recv(count - len(chunks))
        if not part:
            msg = "unexpected EOF while reading startup packet"
            raise EOFError(msg)
        chunks.extend(part)
    return bytes(chunks)


def _read_startup_map(conn: socket.socket) -> dict[str, str]:
    """Read a PostgreSQL Protocol 3.0 StartupMessage and return its key-value map."""
    total_len = struct.unpack(">i", _recv_exact(conn, 4))[0]
    body = _recv_exact(conn, total_len - 4)
    proto = struct.unpack(">i", body[:4])[0]
    assert proto == 196608
    raw_items = body[4:].rstrip(b"\x00").split(b"\x00")
    keys = [raw_items[idx].decode("utf-8") for idx in range(0, len(raw_items), 2)]
    vals = [raw_items[idx + 1].decode("utf-8") for idx in range(0, len(raw_items), 2)]
    return dict(zip(keys, vals, strict=True))


def test_native_sync_lifecycle_and_parameter_copy_isolation() -> None:
    """Verify SyncPostgresClient pure construction, Ready idempotence, parameter copying, and close."""
    accept_count = 0
    terminate_count = 0
    startup_seen: dict[str, str] = {}

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(4)
        port = srv.getsockname()[1]

        def _server() -> None:
            nonlocal accept_count, terminate_count, startup_seen
            conn, _ = srv.accept()
            accept_count += 1
            with conn:
                startup_seen = _read_startup_map(conn)
                conn.sendall(_pg_trust_greeting_frames("17.2"))
                rest = conn.recv(16)
                if rest == b"X\x00\x00\x00\x04":
                    terminate_count += 1

        worker = threading.Thread(target=_server, daemon=True)
        worker.start()

        client = SyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
        )
        assert client.closed is True
        assert client.parameters == {}
        assert accept_count == 0
        assert not hasattr(client, "backend_key")
        assert not hasattr(client, "secret_key")

        client.connect()
        assert client.closed is False
        assert accept_count == 1
        assert startup_seen["user"] == "alice"
        assert startup_seen["database"] == "appdb"
        assert client.parameters["server_version"] == "17.2"
        assert client.parameters["client_encoding"] == "UTF8"

        mutated = client.parameters
        mutated["server_version"] = "0.0"
        assert client.parameters["server_version"] == "17.2"

        client.connect()
        assert accept_count == 1

        client.close()
        assert client.closed is True
        assert client.parameters["server_version"] == "17.2"
        client.close()
        worker.join(timeout=2.0)
        assert terminate_count == 1

        with pytest.raises(InterfaceError):
            client.connect()


def test_native_sync_releases_gil() -> None:
    """Verify SyncPostgresClient.connect releases the GIL so concurrent Python threads make progress."""
    gate = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(2)
        port = srv.getsockname()[1]
        accepted = threading.Event()

        def _server() -> None:
            conn, _ = srv.accept()
            with conn:
                _ = _read_startup_map(conn)
                accepted.set()
                gate.wait(timeout=5.0)
                conn.sendall(_pg_trust_greeting_frames("16.4"))
                _ = conn.recv(16)

        srv_thread = threading.Thread(target=_server, daemon=True)
        srv_thread.start()

        client = SyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
        )

        def _unblock() -> None:
            assert accepted.wait(timeout=5.0)
            gate.set()

        helper = threading.Thread(target=_unblock, daemon=True)
        helper.start()
        client.connect()
        assert client.closed is False
        client.close()
        helper.join(timeout=2.0)
        srv_thread.join(timeout=2.0)


@pytest.mark.parametrize(
    ("mode", "expected_exc", "expected_sqlstate"),
    [
        ("28P01", DatabaseError, "28P01"),
        ("eof", ConnectionFailure, None),
        ("iam_expired", GoogleCloudAuthError, None),
    ],
)
def test_native_sync_core_exception_identity(
    mode: str,
    expected_exc: type[Exception],
    expected_sqlstate: str | None,
) -> None:
    """Verify SyncPostgresClient maps wire errors and auth failures to exact vertebra._vertebra exceptions."""
    if mode == "iam_expired":
        client = SyncPostgresClient(
            user="iam-user",
            database="appdb",
            host="127.0.0.1",
            sslmode="disable",
            enable_iam_auth=True,
            iam_auth_options={
                "access_token": "tok",
                "expires_at_unix_ms": "1000",
                "scopes": "https://www.googleapis.com/auth/sqlservice.login",
            },
        )
        with pytest.raises(expected_exc) as exc_info:
            client.connect()
        assert getattr(exc_info.value, "sqlstate", None) is None
        assert client.closed is True
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(2)
        port = srv.getsockname()[1]

        def _server() -> None:
            conn, _ = srv.accept()
            with conn:
                _ = _read_startup_map(conn)
                if mode == "28P01":
                    conn.sendall(_pg_error_frame("28P01", "invalid password"))

        worker = threading.Thread(target=_server, daemon=True)
        worker.start()

        client = SyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
        )
        with pytest.raises(expected_exc) as exc_info:
            client.connect()
        assert getattr(exc_info.value, "sqlstate", None) == expected_sqlstate
        assert client.closed is True
        worker.join(timeout=2.0)


@pytest.mark.anyio
async def test_native_async_lifecycle_and_concurrent_progress() -> None:
    """Verify 8 concurrent AsyncPostgresClient connects allow background event-loop progress and reject overlap."""
    gate = asyncio.Event()
    accepted_count = 0
    all_accepted = asyncio.Event()

    async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepted_count
        raw_len = await reader.readexactly(4)
        total_len = struct.unpack(">i", raw_len)[0]
        _ = await reader.readexactly(total_len - 4)
        accepted_count += 1
        if accepted_count == 8:
            all_accepted.set()
        await gate.wait()
        writer.write(_pg_trust_greeting_frames(f"17.{accepted_count}"))
        await writer.drain()
        _ = await reader.read(16)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    ticks = 0

    async def _ticker() -> None:
        nonlocal ticks
        while not gate.is_set():
            ticks += 1
            await asyncio.sleep(0)

    async with server:
        clients = [
            AsyncPostgresClient(
                user=f"u{idx}",
                database="appdb",
                host="127.0.0.1",
                port=port,
                sslmode="disable",
            )
            for idx in range(8)
        ]

        async def _connect_client(c: AsyncPostgresClient) -> None:
            await c.connect()

        ticker_task = asyncio.create_task(_ticker())
        connect_tasks = [asyncio.create_task(_connect_client(c)) for c in clients]
        await asyncio.wait_for(all_accepted.wait(), timeout=5.0)

        with pytest.raises(InterfaceError):
            await clients[0].connect()

        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*connect_tasks)
        await ticker_task

        assert ticks > 0
        assert all(not c.closed for c in clients)
        assert all(c.parameters.get("client_encoding") == "UTF8" for c in clients)
        await asyncio.gather(*(c.close() for c in clients))
        assert all(c.closed for c in clients)


@pytest.mark.anyio
async def test_native_async_ready_noop_cancel_and_callback_setup_failure() -> None:
    """Verify cancelling an already-Ready connect preserves the session and callback setup failure aborts the worker."""
    eof_observed = asyncio.Event()

    async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        raw_len = await reader.readexactly(4)
        total_len = struct.unpack(">i", raw_len)[0]
        _ = await reader.readexactly(total_len - 4)
        writer.write(_pg_trust_greeting_frames("17.2"))
        await writer.drain()
        data = await reader.read(16)
        if not data or data == b"X\x00\x00\x00\x04":
            eof_observed.set()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async with server:
        client = AsyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
        )
        await client.connect()
        assert client.closed is False

        async def _connect_ready() -> None:
            await client.connect()

        noop_task = asyncio.create_task(_connect_ready())
        noop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await noop_task
        assert client.closed is False
        assert not eof_observed.is_set()

        await client.close()
        assert client.closed is True
        await asyncio.wait_for(eof_observed.wait(), timeout=2.0)

        client_fail = AsyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
        )
        loop = asyncio.get_running_loop()
        orig_create_future = loop.create_future

        class _BrokenFuture:
            def add_done_callback(self, _cb: object) -> None:
                msg = "cb-fail"
                raise RuntimeError(msg)

        object.__setattr__(loop, "create_future", _BrokenFuture)
        try:
            with pytest.raises(RuntimeError, match="cb-fail"):
                client_fail.connect()
        finally:
            object.__setattr__(loop, "create_future", orig_create_future)

        assert client_fail.closed is True
        with pytest.raises(InterfaceError, match="closed"):
            await client_fail.connect()


@pytest.mark.anyio
async def test_native_async_cancel_inflight_and_queued_completion_race() -> None:
    """Verify in-flight cancellation and queued-completion cancellation race both leave client closed and close socket."""
    accepted_inflight = asyncio.Event()
    eof_inflight = asyncio.Event()
    hold_inflight = asyncio.Event()

    async def _inflight_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        raw_len = await reader.readexactly(4)
        total_len = struct.unpack(">i", raw_len)[0]
        _ = await reader.readexactly(total_len - 4)
        accepted_inflight.set()
        try:
            await hold_inflight.wait()
        finally:
            _ = await reader.read(16)
            eof_inflight.set()
            writer.close()

    srv1 = await asyncio.start_server(_inflight_server, "127.0.0.1", 0)
    port1 = srv1.sockets[0].getsockname()[1]

    async def _connect_coro(c: AsyncPostgresClient) -> None:
        await c.connect()

    async with srv1:
        c1 = AsyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port1,
            sslmode="disable",
        )
        t1 = asyncio.create_task(_connect_coro(c1))
        await asyncio.wait_for(accepted_inflight.wait(), timeout=5.0)
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1
        hold_inflight.set()
        await asyncio.wait_for(eof_inflight.wait(), timeout=2.0)
        assert c1.closed is True

    eof_queued = asyncio.Event()

    async def _queued_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        raw_len = await reader.readexactly(4)
        total_len = struct.unpack(">i", raw_len)[0]
        _ = await reader.readexactly(total_len - 4)
        writer.write(_pg_trust_greeting_frames("17.2"))
        await writer.drain()
        _ = await reader.read(16)
        eof_queued.set()
        writer.close()

    srv2 = await asyncio.start_server(_queued_server, "127.0.0.1", 0)
    port2 = srv2.sockets[0].getsockname()[1]

    async with srv2:
        loop = asyncio.get_running_loop()
        saved_drainer = getattr(loop, "_vertebra_postgres_completion_drainer", None)
        if saved_drainer is not None:
            delattr(loop, "_vertebra_postgres_completion_drainer")

        orig_add_reader = loop.add_reader
        orig_call_soon_ts = loop.call_soon_threadsafe
        held_callbacks: list[Callable[[], object]] = []
        queued_event = threading.Event()

        def _reject_drainer_reader(fd: Any, callback: Callable[..., Any], *args: Any) -> Any:
            if type(callback).__name__ in ("_DbCompletionDrainer", "_RustCompletionDrainer"):
                raise NotImplementedError
            return orig_add_reader(fd, callback, *args)

        def _hold_drainer_wakeup(callback: Callable[..., Any], *args: Any) -> Any:
            if type(callback).__name__ in ("_DbCompletionDrainer", "_RustCompletionDrainer"):
                held_callbacks.append(lambda: callback(*args))
                queued_event.set()
                return None
            return orig_call_soon_ts(callback, *args)

        object.__setattr__(loop, "add_reader", _reject_drainer_reader)
        object.__setattr__(loop, "call_soon_threadsafe", _hold_drainer_wakeup)
        try:
            c2 = AsyncPostgresClient(
                user="alice",
                database="appdb",
                host="127.0.0.1",
                port=port2,
                sslmode="disable",
            )
            t2 = asyncio.create_task(_connect_coro(c2))
            await asyncio.to_thread(queued_event.wait, 5.0)
            assert queued_event.is_set()
            assert c2.closed is True

            t2.cancel()
            for cb in held_callbacks:
                cb()
            with pytest.raises(asyncio.CancelledError):
                await t2
            await asyncio.wait_for(eof_queued.wait(), timeout=2.0)
            assert c2.closed is True
        finally:
            object.__setattr__(loop, "add_reader", orig_add_reader)
            object.__setattr__(loop, "call_soon_threadsafe", orig_call_soon_ts)
            if hasattr(loop, "_vertebra_postgres_completion_drainer"):
                delattr(loop, "_vertebra_postgres_completion_drainer")
            if saved_drainer is not None:
                object.__setattr__(loop, "_vertebra_postgres_completion_drainer", saved_drainer)


@pytest.mark.parametrize("client_cls", [SyncPostgresClient, AsyncPostgresClient])
def test_native_constructor_signature_parity(client_cls: type[Any]) -> None:
    """Verify SyncPostgresClient and AsyncPostgresClient expose identical 25-parameter keyword-only signatures."""
    sig = inspect.signature(client_cls)
    expected: list[tuple[str, Any]] = [
        ("user", inspect.Parameter.empty),
        ("database", inspect.Parameter.empty),
        ("host", "localhost"),
        ("port", 5432),
        ("password", None),
        ("unix_socket", None),
        ("sslmode", "verify-full"),
        ("ca_pem", None),
        ("client_cert_pem", None),
        ("client_key_pem", None),
        ("server_name", None),
        ("channel_binding", "prefer"),
        ("connect_timeout", 10.0),
        ("application_name", "vertebra"),
        ("session_gucs", None),
        ("keepalives", True),
        ("keepalives_idle", None),
        ("keepalives_interval", None),
        ("keepalives_count", None),
        ("cloud_sql_instance", None),
        ("cloud_sql_ip_type", "PRIVATE"),
        ("alloydb_instance_uri", None),
        ("alloydb_ip_type", "PRIVATE"),
        ("enable_iam_auth", False),
        ("cloud_auth_options", None),
        ("iam_auth_options", None),
    ]
    actual = [(p.name, p.default) for p in sig.parameters.values()]
    assert actual == expected
    assert all(p.kind == inspect.Parameter.KEYWORD_ONLY for p in sig.parameters.values())

    with pytest.raises(TypeError):
        client_cls("alice", "appdb")

    with pytest.raises(TypeError):
        client_cls(user="alice", database="appdb", extra=1)


@pytest.mark.parametrize("client_cls", [SyncPostgresClient, AsyncPostgresClient])
@pytest.mark.parametrize(
    ("bad_kwargs", "expected_exc", "forbidden_secret"),
    [
        ({"session_gucs": {"k": 1}}, TypeError, None),
        ({"iam_auth_options": {1: "v"}}, TypeError, None),
        ({"port": True}, TypeError, None),
        ({"keepalives_idle": False}, TypeError, None),
        ({"connect_timeout": True}, TypeError, None),
        ({"keepalives": 1}, TypeError, None),
        ({"enable_iam_auth": 0}, TypeError, None),
        ({"port": 70000}, ValueError, None),
        ({"sslmode": "bogus"}, ValueError, None),
        ({"password": "s3cr3t\x00bad"}, ValueError, "s3cr3t"),
        ({"client_key_pem": b"PRIVATE-KEY-SECRET"}, ValueError, "PRIVATE-KEY-SECRET"),
    ],
)
def test_native_constructor_type_vs_value_errors_and_redaction(
    client_cls: type[Any],
    bad_kwargs: dict[str, Any],
    expected_exc: type[Exception],
    forbidden_secret: str | None,
) -> None:
    """Verify TypeError vs ValueError boundaries and secret redaction across both constructors."""
    base: dict[str, Any] = {"user": "alice", "database": "appdb"}
    base.update(bad_kwargs)
    with pytest.raises(expected_exc) as exc_info:
        client_cls(**base)
    if forbidden_secret is not None:
        assert forbidden_secret not in str(exc_info.value)


def test_native_constructor_zero_io_and_guc_snapshot_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify constructors perform zero DNS/network I/O and snapshot session_gucs at construction time."""
    dns_calls = 0
    orig_getaddrinfo = socket.getaddrinfo

    def _spy_getaddrinfo(*args: Any, **kwargs: Any) -> Any:
        nonlocal dns_calls
        dns_calls += 1
        return orig_getaddrinfo(*args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", _spy_getaddrinfo)

    _ = SyncPostgresClient(
        user="alice",
        database="appdb",
        cloud_sql_instance="proj:us-central1:inst",
    )
    _ = AsyncPostgresClient(
        user="alice",
        database="appdb",
        alloydb_instance_uri="projects/p/locations/r/clusters/c/instances/i",
    )
    assert dns_calls == 0

    monkeypatch.setattr(socket, "getaddrinfo", orig_getaddrinfo)

    startup_seen: dict[str, str] = {}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(2)
        port = srv.getsockname()[1]

        def _server() -> None:
            nonlocal startup_seen
            conn, _ = srv.accept()
            with conn:
                startup_seen = _read_startup_map(conn)
                conn.sendall(_pg_trust_greeting_frames("17.2"))
                _ = conn.recv(16)

        worker = threading.Thread(target=_server, daemon=True)
        worker.start()

        gucs = {"app.tag": "initial"}
        client = SyncPostgresClient(
            user="alice",
            database="appdb",
            host="127.0.0.1",
            port=port,
            sslmode="disable",
            session_gucs=gucs,
        )
        gucs["app.tag"] = "mutated"
        client.connect()
        assert startup_seen["app.tag"] == "initial"
        client.close()
        worker.join(timeout=2.0)


@pytest.mark.skipif(sys.platform == "win32", reason="Unix domain sockets require POSIX")
def test_native_constructor_unix_socket_reachability() -> None:
    """Verify SyncPostgresClient connects over a Unix domain socket with sslmode='disable' and session_gucs."""
    startup_seen: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="vtb-pg-") as tmp_dir:
        sock_path = Path(tmp_dir) / ".s.PGSQL.5432"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as srv:
            srv.bind(str(sock_path))
            srv.listen(2)

            def _server() -> None:
                nonlocal startup_seen
                conn, _ = srv.accept()
                with conn:
                    startup_seen = _read_startup_map(conn)
                    conn.sendall(_pg_trust_greeting_frames("17.2"))
                    _ = conn.recv(16)

            worker = threading.Thread(target=_server, daemon=True)
            worker.start()

            client = SyncPostgresClient(
                user="alice",
                database="appdb",
                unix_socket=str(sock_path),
                sslmode="disable",
                session_gucs={"application_name": "unix-override", "app.env": "ci"},
            )
            client.connect()
            assert startup_seen["application_name"] == "unix-override"
            assert startup_seen["app.env"] == "ci"
            client.close()
            worker.join(timeout=2.0)


@pytest.mark.anyio
async def test_postgres_drivers_discard_statement_plan_and_release_real_broker_leases() -> None:
    """Sync and Async PostgreSQL drivers use CommonDriverMixin LeaseBroker and bytes StatementCache cleanly."""
    cluster = FakePostgresCluster()
    sync_broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    sync_driver = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=FakeSyncPostgresPool(cluster), broker=sync_broker)
    try:
        assert sync_driver.execute("CREATE TABLE pg_lease_items (id INTEGER)") == 0
        assert sync_driver.execute("INSERT INTO pg_lease_items VALUES ($1)", (1,)) == 1
        assert sync_driver.execute("INSERT INTO pg_lease_items VALUES ($1)", (2,)) == 1
        sc = sync_broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits == 1
        assert sync_broker.stats().active_leases == 0
        assert sync_broker.stats().total_acquired == 3
        assert sync_broker.stats().total_released == 3
    finally:
        sync_driver.close()

    async_broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    async_driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=FakeAsyncPostgresPool(cluster), broker=async_broker)
    try:
        assert await async_driver.execute("INSERT INTO pg_lease_items VALUES ($1)", (3,)) == 1
        assert await async_driver.execute("INSERT INTO pg_lease_items VALUES ($1)", (4,)) == 1
        async_sc = async_broker.statement_cache()
        assert async_sc is not None
        assert async_sc.stats().hits == 1
        assert async_broker.stats().active_leases == 0
        assert async_broker.stats().total_acquired == 2
        assert async_broker.stats().total_released == 2
    finally:
        await async_driver.close()


def _read_pg_message(conn: socket.socket) -> tuple[bytes, bytes]:
    """Read one frontend PostgreSQL wire message `(tag, payload)`."""
    tag = _recv_exact(conn, 1)
    total_len = struct.unpack(">i", _recv_exact(conn, 4))[0]
    payload = _recv_exact(conn, total_len - 4) if total_len > 4 else b""
    return tag, payload


def test_postgres_config_token_provider_redaction_and_dsql_sslmode_rejection() -> None:
    """Verify PostgreSQLConfig redacts credentials in repr, resolves token_provider, and blocks DSQL sslmode=disable."""
    rds_provider = AwsRdsIamTokenProvider(
        endpoint_host="db.example.us-east-1.rds.amazonaws.com",
        region="us-east-1",
        username="iam_user",
        access_key_id="AKIAEXAMPLEKEY1234",
        secret_access_key="test-secret-key-placeholder",
        session_token="test-session-placeholder",
        clock=lambda: 1_794_873_600.0,
    )
    cfg = PostgreSQLConfig(
        dsn="postgresql://iam_user:inline-secret@db.example:5432/appdb",
        iam_auth_options={"service_account_key": "secret-sa-json"},
        token_provider=rds_provider,
    )
    rendered = repr(cfg)
    assert "inline-secret" not in rendered
    assert "secret-sa-json" not in rendered
    assert "AKIAEXAMPLEKEY1234" not in rendered
    assert "test-secret-key-placeholder" not in rendered
    assert "test-session-placeholder" not in rendered
    assert "AwsRdsIamTokenProvider" in rendered

    opts = build_native_client_options(cfg, env={})
    assert opts.password == "inline-secret"

    cfg_no_pw = PostgreSQLConfig(
        dsn="postgresql://iam_user@db.example:5432/appdb",
        token_provider=rds_provider,
    )
    opts_token = build_native_client_options(cfg_no_pw, env={})
    assert opts_token.password == rds_provider.resolve_sync()

    dsql_provider = AwsAuroraDsqlIamTokenProvider(
        hostname="cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
        access_key_id="AKIAEXAMPLEKEY1234",
        secret_access_key="test-secret-key-placeholder",
    )
    with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
        PostgreSQLConfig(
            dsn="postgresql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres?sslmode=disable",
            token_provider=dsql_provider,
        )

    valid_dsql_cfg = PostgreSQLConfig(
        dsn="postgresql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres",
        token_provider=dsql_provider,
    )
    with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
        build_native_client_options(valid_dsql_cfg, env={"PGSSLMODE": "disable"})


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_postgres_drivers_token_provider_refresh_and_auth_retry(mode: str) -> None:
    """Verify Sync and Async PostgreSQL drivers rotate expired IAM tokens and force-refresh on 28P01."""
    now = [1000.0]
    issued: list[str] = []

    def _mint(*, now_ts: float) -> str:
        token = f"iam-token-{len(issued) + 1}-at-{int(now_ts)}"
        issued.append(token)
        return token

    provider = AwsRdsIamTokenProvider(
        endpoint_host="127.0.0.1",
        region="us-east-1",
        username="iam_user",
        refresh_margin_seconds=60.0,
        token_ttl_seconds=900,
        clock=lambda: now[0],
        signer=_mint,
    )

    seen_passwords: list[str] = []
    stop_event = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        srv.settimeout(0.2)
        port = srv.getsockname()[1]

        def _handle_conn(conn: socket.socket, conn_idx: int) -> None:
            with conn:
                _ = _read_startup_map(conn)
                conn.sendall(_pg_frame(b"R", struct.pack(">i", 3)))
                tag, payload = _read_pg_message(conn)
                assert tag == b"p"
                password = payload.rstrip(b"\x00").decode("utf-8")
                seen_passwords.append(password)
                if conn_idx == 2:
                    conn.sendall(_pg_error_frame("28P01", "password authentication failed"))
                    return
                conn.sendall(_pg_trust_greeting_frames("17.2"))
                while True:
                    try:
                        msg_tag, _ = _read_pg_message(conn)
                    except EOFError:
                        break
                    if msg_tag == b"X":
                        break
                    if msg_tag == b"Q":
                        conn.sendall(_pg_frame(b"C", b"SELECT 1\x00") + _pg_frame(b"Z", b"I"))
                    elif msg_tag == b"S":
                        conn.sendall(
                            _pg_frame(b"1", b"")
                            + _pg_frame(b"2", b"")
                            + _pg_frame(b"C", b"SELECT 1\x00")
                            + _pg_frame(b"Z", b"I")
                        )

        def _server() -> None:
            idx = 0
            while not stop_event.is_set():
                try:
                    conn, _ = srv.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                t = threading.Thread(target=_handle_conn, args=(conn, idx), daemon=True)
                t.start()
                idx += 1

        srv_thread = threading.Thread(target=_server, daemon=True)
        srv_thread.start()

        cfg = PostgreSQLConfig(
            dsn=f"postgresql://iam_user@127.0.0.1:{port}/appdb?sslmode=disable",
            min_size=0,
            max_size=2,
            token_provider=provider,
        )
        try:
            if mode == "sync":
                driver = SyncPostgreSQLDriver(cfg)
                try:
                    driver.connect()
                    assert driver.execute("SELECT 1") == 1
                    assert len(issued) == 1
                    assert seen_passwords[-1] == issued[0]

                    now[0] = 1850.0
                    assert driver.execute("SELECT 1") == 1
                    assert len(issued) == 2
                    assert seen_passwords[-1] == issued[1]

                    driver.close()
                    driver.connect()
                    assert driver.execute("SELECT 1") == 1
                    assert len(issued) == 3
                    assert seen_passwords[-2:] == [issued[1], issued[2]]
                finally:
                    driver.close()
            else:
                async_driver = AsyncPostgreSQLDriver(cfg)
                try:
                    await async_driver.connect()
                    assert await async_driver.execute("SELECT 1") == 1
                    assert len(issued) == 1
                    assert seen_passwords[-1] == issued[0]

                    now[0] = 1850.0
                    assert await async_driver.execute("SELECT 1") == 1
                    assert len(issued) == 2
                    assert seen_passwords[-1] == issued[1]

                    await async_driver.close()
                    await async_driver.connect()
                    assert await async_driver.execute("SELECT 1") == 1
                    assert len(issued) == 3
                    assert seen_passwords[-2:] == [issued[1], issued[2]]
                finally:
                    await async_driver.close()
        finally:
            stop_event.set()
            srv_thread.join(timeout=2.0)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("explicit_scope", "expected_scope"),
    [
        (None, AZURE_POSTGRES_SCOPE),
        ("https://custom.postgres.example/.default", "https://custom.postgres.example/.default"),
    ],
)
async def test_postgres_config_azure_entra_default_scope_and_redaction(
    explicit_scope: str | None,
    expected_scope: str,
) -> None:
    """Verify PostgreSQLConfig supplies AZURE_POSTGRES_SCOPE for AzureEntraTokenProvider and redacts secrets."""
    seen_scopes: list[str] = []

    def _fetch(scope: str) -> tuple[str, float]:
        seen_scopes.append(scope)
        return (f"test-token-placeholder-{scope}", 5000.0)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="pg-client",
        client_secret="test-token-placeholder-secret",
        scope=explicit_scope,
        fetcher=_fetch,
        clock=lambda: 1000.0,
    )
    cfg = PostgreSQLConfig(
        dsn="postgresql://entra_user@pg.postgres.database.azure.com:5432/appdb?sslmode=require",
        token_provider=provider,
    )
    assert isinstance(cfg.token_provider, AzureEntraTokenProvider)
    assert cfg.token_provider.scope == expected_scope
    rendered = repr(cfg)
    assert "AzureEntraTokenProvider" in rendered
    assert "test-token-placeholder-secret" not in rendered

    assert cfg.resolve_iam_password_sync() == f"test-token-placeholder-{expected_scope}"
    assert await cfg.resolve_iam_password_async(force_refresh=True) == f"test-token-placeholder-{expected_scope}"

    opts = build_native_client_options(cfg, env={})
    assert opts.password == f"test-token-placeholder-{expected_scope}"
    assert seen_scopes == [expected_scope, expected_scope]
    assert "test-token-placeholder-" not in repr(cfg)


@pytest.mark.parametrize(
    ("ca_source", "explicit_sslmode", "expected_sslmode"),
    [
        ("default_rds", None, "verify-full"),
        ("default_rds", "verify-ca", "verify-ca"),
        ("config_str", None, "verify-full"),
        ("override_str", "require", "require"),
        ("override_bytes", None, "verify-full"),
        ("sslrootcert", None, "verify-full"),
    ],
)
def test_postgres_cloud_auth_rds_ca_bundle_and_ca_pem_normalization(
    tmp_path: Path,
    ca_source: str,
    explicit_sslmode: str | None,
    expected_sslmode: str,
) -> None:
    """Verify remote AwsRdsIamTokenProvider attaches aws_rds_root_ca_pem and normalizes custom ca_pem/sslrootcert."""
    rds_provider = AwsRdsIamTokenProvider(
        endpoint_host="orders.c9akciq32.us-east-1.rds.amazonaws.com",
        region="us-east-1",
        username="iam_user",
        access_key_id="test-token-placeholder-akid",
        secret_access_key="test-token-placeholder-secret",
        clock=lambda: 1_794_873_600.0,
    )
    custom_pem = "-----BEGIN CERTIFICATE-----\nMIIB+TCCAWACCQD=\n-----END CERTIFICATE-----\n"
    dsn_suffix = f"?sslmode={explicit_sslmode}" if explicit_sslmode is not None else ""
    base_dsn = f"postgresql://iam_user@orders.c9akciq32.us-east-1.rds.amazonaws.com:5432/appdb{dsn_suffix}"

    overrides: dict[str, Any] = {}
    config_ca: str | None = None
    expected_ca_bytes = aws_rds_root_ca_pem().encode("utf-8")
    if ca_source == "config_str":
        config_ca = custom_pem
        expected_ca_bytes = custom_pem.encode("utf-8")
    elif ca_source == "override_str":
        overrides["ca_pem"] = custom_pem
        expected_ca_bytes = custom_pem.encode("utf-8")
    elif ca_source == "override_bytes":
        overrides["ca_pem"] = custom_pem.encode("utf-8")
        expected_ca_bytes = custom_pem.encode("utf-8")
    elif ca_source == "sslrootcert":
        ca_file = tmp_path / "custom-rds-ca.pem"
        ca_file.write_text(custom_pem, encoding="utf-8")
        overrides["sslrootcert"] = str(ca_file)
        expected_ca_bytes = custom_pem.encode("utf-8")

    cfg = PostgreSQLConfig(dsn=base_dsn, token_provider=rds_provider, ca_pem=config_ca)
    opts = build_native_client_options(cfg, env={}, **overrides)
    assert opts.sslmode == expected_sslmode
    assert opts.targets[0].sslmode == expected_sslmode
    assert opts.ca_pem == expected_ca_bytes

    for bad_pem in ("", "   ", "bad\x00pem"):
        with pytest.raises(ImproperConfigurationError, match="ca_pem"):
            PostgreSQLConfig(dsn=base_dsn, ca_pem=bad_pem)
        with pytest.raises(ImproperConfigurationError, match="ca_pem"):
            build_native_client_options(cfg, env={}, ca_pem=bad_pem)


@pytest.mark.parametrize("provider_kind", ["aws_rds", "aws_dsql", "azure_entra"])
@pytest.mark.parametrize(
    ("host_literal", "is_loopback"),
    [
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com", False),
        ("10.0.0.42", False),
        ("127.0.0.1", True),
        ("localhost", True),
        ("[::1]", True),
    ],
)
def test_postgres_cloud_auth_remote_tls_enforcement_and_loopback_exemption(
    provider_kind: str,
    host_literal: str,
    is_loopback: bool,
) -> None:
    """Verify AWS and Azure token providers block sslmode='disable' on remote hosts while allowing loopback stands."""
    provider: Any
    if provider_kind == "aws_rds":
        provider = AwsRdsIamTokenProvider(
            endpoint_host="orders.c9akciq32.us-east-1.rds.amazonaws.com",
            region="us-east-1",
            username="iam_user",
            access_key_id="test-token-placeholder-akid",
            secret_access_key="test-token-placeholder-secret",
            clock=lambda: 1_794_873_600.0,
        )
    elif provider_kind == "aws_dsql":
        provider = AwsAuroraDsqlIamTokenProvider(
            hostname="cluster.dsql.us-east-1.on.aws",
            region="us-east-1",
            access_key_id="test-token-placeholder-akid",
            secret_access_key="test-token-placeholder-secret",
            clock=lambda: 1_794_873_600.0,
        )
    else:
        provider = AzureEntraTokenProvider(
            fetcher=lambda scope: (f"test-token-placeholder-{scope}", 5000.0),
            clock=lambda: 1000.0,
        )

    disabled_dsn = f"postgresql://iam_user@{host_literal}:5432/appdb?sslmode=disable"
    plain_dsn = f"postgresql://iam_user@{host_literal}:5432/appdb"

    if not is_loopback:
        with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
            PostgreSQLConfig(dsn=disabled_dsn, token_provider=provider)
        with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
            PostgreSQLConfig(
                dsn="postgresql://iam_user@127.0.0.1:5432/appdb",
                direct_dsn=disabled_dsn,
                token_provider=provider,
            )
        remote_cfg = PostgreSQLConfig(dsn=plain_dsn, token_provider=provider)
        with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
            build_native_client_options(remote_cfg, env={"PGSSLMODE": "disable"})
        with pytest.raises(ImproperConfigurationError, match="sslmode='disable'"):
            build_native_client_options(remote_cfg, env={}, sslmode="disable")
    else:
        local_cfg = PostgreSQLConfig(dsn=disabled_dsn, token_provider=provider)
        opts = build_native_client_options(local_cfg, env={})
        assert opts.sslmode == "disable"
        assert opts.ca_pem is None


@pytest.mark.parametrize(
    ("dsn", "cockroach_cluster", "expected_options"),
    [
        (
            "postgresql://user:test-token-placeholder@crdb.example:26257/appdb",
            "orange-finch-123",
            "--cluster=orange-finch-123",
        ),
        (
            "postgresql://user:test-token-placeholder@crdb.example:26257/appdb?options=--cluster%3Dfrom-dsn",
            None,
            "--cluster=from-dsn",
        ),
        (
            "postgresql://user:test-token-placeholder@crdb.example:26257/appdb?options=-c%20work_mem%3D4MB",
            "orange-finch-123",
            "-c work_mem=4MB --cluster=orange-finch-123",
        ),
        (
            "postgresql://user:test-token-placeholder@crdb.example:26257/appdb?options=--cluster%3Dstale%20-c%20work_mem%3D4MB",
            "orange-finch-123",
            "-c work_mem=4MB --cluster=orange-finch-123",
        ),
    ],
)
def test_postgres_cockroach_cluster_options_propagation_and_redaction(
    dsn: str,
    cockroach_cluster: str | None,
    expected_options: str,
) -> None:
    """Verify CockroachDB cluster routing and DSN options propagate cleanly and redact credentials."""
    cfg = PostgreSQLConfig(dsn=dsn, cockroach_cluster=cockroach_cluster)
    rendered = repr(cfg)
    assert "test-token-placeholder" not in rendered

    opts = build_native_client_options(cfg, env={})
    assert opts.session_gucs["options"] == expected_options
    assert opts.to_client_kwargs()["session_gucs"]["options"] == expected_options


@pytest.mark.parametrize("bad_cluster", ["", "   ", "my cluster", "bad\0cluster"])
def test_postgres_cockroach_cluster_validation_rejects_invalid_values(bad_cluster: str) -> None:
    """Verify empty, whitespace, or NUL-containing cockroach_cluster values are rejected."""
    with pytest.raises(ImproperConfigurationError, match="cockroach_cluster"):
        PostgreSQLConfig(
            dsn="postgresql://user@crdb.example:26257/appdb",
            cockroach_cluster=bad_cluster,
        )


def test_postgres_alloydb_columnar_engine_startup_gucs() -> None:
    """Verify enable_alloydb_columnar_engine populates google_columnar_engine.enabled in startup GUCs."""
    cfg = PostgreSQLConfig(
        dsn="postgresql://user@127.0.0.1:5432/appdb",
        enable_alloydb_columnar_engine=True,
    )
    gucs = cfg.build_session_gucs()
    assert gucs["google_columnar_engine.enabled"] == "on"
    assert gucs["alloydb.enable_columnar_engine"] == "on"
    opts = build_native_client_options(cfg, env={})
    assert opts.session_gucs["google_columnar_engine.enabled"] == "on"
    assert opts.session_gucs["alloydb.enable_columnar_engine"] == "on"


def test_postgres_cockroach_cluster_wire_startup_forwarding() -> None:
    """Verify PostgreSQLConfig(cockroach_cluster=...) forwards --cluster=<id> in native StartupMessage."""
    startup_seen: dict[str, str] = {}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(2)
        port = srv.getsockname()[1]

        def _server() -> None:
            nonlocal startup_seen
            conn, _ = srv.accept()
            with conn:
                startup_seen = _read_startup_map(conn)
                conn.sendall(_pg_trust_greeting_frames("16.4"))
                _ = conn.recv(16)

        worker = threading.Thread(target=_server, daemon=True)
        worker.start()

        cfg = PostgreSQLConfig(
            dsn=f"postgresql://alice:test-token-placeholder@127.0.0.1:{port}/appdb?sslmode=disable&options=-c%20work_mem%3D4MB",
            cockroach_cluster="orange-finch-123",
        )
        assert "test-token-placeholder" not in repr(cfg)
        opts = build_native_client_options(cfg, env={})
        client = SyncPostgresClient(**opts.to_client_kwargs())
        client.connect()
        assert startup_seen["options"] == "-c work_mem=4MB --cluster=orange-finch-123"
        client.close()
        worker.join(timeout=2.0)
