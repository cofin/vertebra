"""Unit and loopback wire tests for SyncMSSQLDriver, AsyncMSSQLDriver, and vertebra._mssql."""

from __future__ import annotations

import contextlib
import socket
import struct
import sys
import threading
import time

import msgspec
import pyarrow as pa
import pytest

import vertebra._mssql as mssql_native
from vertebra._vertebra import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
)
from vertebra.db import (
    ArrowCursor,
    AsyncCloudSQLDialer,
    AsyncPipeline,
    AsyncPipelineCapable,
    CloudSQLConfig,
    DatabaseRegistry,
    SyncCloudSQLDialer,
    SyncPipeline,
    SyncPipelineCapable,
)
from vertebra.db.adapters.mssql import (
    AsyncMSSQLDriver,
    AsyncMSSQLPipeline,
    MSSQLConfig,
    MSSQLCore,
    SyncMSSQLDriver,
    SyncMSSQLPipeline,
    rewrite_mssql_placeholders,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import reset_dependency_cache


class _MssqlItemRow(msgspec.Struct, frozen=True, gc=False):
    """Typed row struct for verifying `schema_type` vectorcall hydration on MSSQL drivers."""

    id: int
    label: str


class _TdsWireBuilder:
    """TDS 7.4 wire frame builder for deterministic loopback server responses."""

    @staticmethod
    def build_packet(pkt_type: int, status: int, payload: bytes) -> bytes:
        """Frame `payload` with an 8-byte TDS packet header."""
        total_len = 8 + len(payload)
        header = struct.pack(">BBHHBB", pkt_type, status, total_len, 0, 1, 0)
        return header + payload

    @classmethod
    def prelogin_not_sup_response(cls) -> bytes:
        """Build a `TabularResult` (`0x04`) Prelogin response with `ENCRYPT_NOT_SUP` (`0x02`)."""
        body = bytes(
            [
                0x00,
                0x00,
                0x10,
                0x00,
                0x06,
                0x01,
                0x00,
                0x16,
                0x00,
                0x01,
                0x02,
                0x00,
                0x17,
                0x00,
                0x01,
                0xFF,
                16,
                0,
                0x03,
                0xE8,
                0,
                0,
                0x02,
                0x00,
            ]
        )
        return cls.build_packet(0x04, 0x01, body)

    @staticmethod
    def _utf16_b_varchar(text: str) -> bytes:
        encoded = text.encode("utf-16-le")
        return struct.pack("B", len(text)) + encoded

    @staticmethod
    def _utf16_us_varchar(text: str) -> bytes:
        encoded = text.encode("utf-16-le")
        return struct.pack("<H", len(text)) + encoded

    @classmethod
    def loginack_token(cls) -> bytes:
        """Build a TDS 7.4 `LOGINACK` (`0xAD`) token."""
        prog = cls._utf16_b_varchar("Microsoft SQL Server")
        val = bytes([1, 0x74, 0x00, 0x00, 0x04]) + prog + bytes([0xE8, 0x03, 0x00, 0x10])
        return bytes([0xAD]) + struct.pack("<H", len(val)) + val

    @staticmethod
    def done_token(status: int, cur_cmd: int, rows: int) -> bytes:
        """Build a TDS `DONE` (`0xFD`) token."""
        return bytes([0xFD]) + struct.pack("<HHQ", status, cur_cmd, rows)

    @staticmethod
    def doneinproc_token(status: int, cur_cmd: int, rows: int) -> bytes:
        """Build a TDS `DONEINPROC` (`0xFF`) token."""
        return bytes([0xFF]) + struct.pack("<HHQ", status, cur_cmd, rows)

    @staticmethod
    def doneproc_token(status: int, cur_cmd: int, rows: int) -> bytes:
        """Build a TDS `DONEPROC` (`0xFE`) token."""
        return bytes([0xFE]) + struct.pack("<HHQ", status, cur_cmd, rows)

    @classmethod
    def error_token(cls, number: int, state: int, class_code: int, message: str) -> bytes:
        """Build a TDS `ERROR` (`0xAA`) token."""
        body = (
            struct.pack("<IBB", number, state, class_code)
            + cls._utf16_us_varchar(message)
            + cls._utf16_b_varchar("loopback-sql")
            + cls._utf16_b_varchar("sp_executesql")
            + struct.pack("<I", 1)
        )
        return bytes([0xAA]) + struct.pack("<H", len(body)) + body

    @classmethod
    def login_success_packet(cls) -> bytes:
        """Build a complete `LOGIN7` acknowledgement packet."""
        tokens = cls.loginack_token() + cls.done_token(0x0000, 0, 0)
        return cls.build_packet(0x04, 0x01, tokens)

    @classmethod
    def select_int_response(cls, col_name: str, value: int) -> bytes:
        """Build a single-column `Int4` result set packet."""
        tokens = (
            bytes([0x81])
            + struct.pack("<H", 1)
            + struct.pack("<IHB", 0, 0x08, 0x38)
            + cls._utf16_b_varchar(col_name)
            + bytes([0xD1])
            + struct.pack("<i", value)
            + cls.done_token(0x0010, 0xC1, 1)
        )
        return cls.build_packet(0x04, 0x01, tokens)

    @classmethod
    def select_items_response(cls, rows: list[tuple[int, str]]) -> bytes:
        """Build a two-column `(id: Int8, label: NVarchar)` result set packet."""
        colmeta = (
            bytes([0x81])
            + struct.pack("<H", 2)
            + struct.pack("<IHB", 0, 0x08, 0x7F)
            + cls._utf16_b_varchar("id")
            + struct.pack("<IHBH", 0, 0x08, 0xE7, 256)
            + bytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            + cls._utf16_b_varchar("label")
        )
        row_tokens = bytearray()
        for item_id, label in rows:
            utf16 = label.encode("utf-16-le")
            row_tokens.append(0xD1)
            row_tokens.extend(struct.pack("<q", item_id))
            row_tokens.append(fd_len := len(utf16) & 0xFF)
            _ = fd_len
            row_tokens[-1:] = struct.pack("<H", len(utf16))
            row_tokens.extend(utf16)
        tokens = (
            colmeta + bytes(row_tokens) + cls.doneinproc_token(0x0010, 0xC1, len(rows)) + cls.doneproc_token(0, 0, 0)
        )
        return cls.build_packet(0x04, 0x01, tokens)


class _LoopbackTdsServer:
    """In-process TCP TDS 7.4 loopback server for exercising `SyncMSSQLDriver` and `AsyncMSSQLDriver`."""

    def __init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port: int = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self.received_packet_types: list[int] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @property
    def dsn(self) -> str:
        """Return the loopback `mssql://` DSN with `encrypt=not_supported`."""
        return f"mssql://sa:Passw0rd@127.0.0.1:{self.port}/master?encrypt=not_supported"

    def close(self) -> None:
        """Stop the accept loop and close the listening socket."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._sock.close()

    @staticmethod
    def _read_packet(conn: socket.socket) -> tuple[int, int, bytes] | None:
        header = bytearray()
        while len(header) < 8:
            chunk = conn.recv(8 - len(header))
            if not chunk:
                return None
            header.extend(chunk)
        pkt_type, status, total_len = struct.unpack(">BBH", header[:4])
        body_len = max(0, total_len - 8)
        body = bytearray()
        while len(body) < body_len:
            chunk = conn.recv(body_len - len(body))
            if not chunk:
                return None
            body.extend(chunk)
        return pkt_type, status, bytes(body)

    @classmethod
    def _read_message(cls, conn: socket.socket) -> tuple[int, bytes] | None:
        first = cls._read_packet(conn)
        if first is None:
            return None
        pkt_type, status, body = first
        full = bytearray(body)
        while (status & 0x01) == 0:
            nxt = cls._read_packet(conn)
            if nxt is None:
                return None
            _, status, body = nxt
            full.extend(body)
        return pkt_type, bytes(full)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()

    def _handle_client(self, conn: socket.socket) -> None:
        with conn:
            pre = self._read_message(conn)
            if pre is None or pre[0] != 0x12:
                return
            conn.sendall(_TdsWireBuilder.prelogin_not_sup_response())
            login = self._read_message(conn)
            if login is None or login[0] != 0x10:
                return
            conn.sendall(_TdsWireBuilder.login_success_packet())

            while not self._stop.is_set():
                msg = self._read_message(conn)
                if msg is None:
                    break
                pkt_type, payload = msg
                self.received_packet_types.append(pkt_type)
                utf16_text = payload.decode("utf-16-le", errors="ignore")

                if pkt_type == 0x06:
                    conn.sendall(
                        _TdsWireBuilder.build_packet(
                            0x04,
                            0x01,
                            _TdsWireBuilder.done_token(0x0020, 0, 0),
                        )
                    )
                    continue

                if pkt_type == 0x07:
                    conn.sendall(
                        _TdsWireBuilder.build_packet(
                            0x04,
                            0x01,
                            _TdsWireBuilder.done_token(0x0010, 0xF0, 2),
                        )
                    )
                    continue

                if pkt_type == 0x0E:
                    req_type = struct.unpack("<H", payload[22:24])[0] if len(payload) >= 24 else 0
                    resp = bytearray()
                    if req_type == 5:
                        resp.extend([0xE3, 11, 0, 8, 8])
                        resp.extend(struct.pack("<Q", 0x1122334455667788))
                        resp.append(0)
                    elif req_type == 7:
                        resp.extend([0xE3, 11, 0, 9, 0, 8])
                        resp.extend(struct.pack("<Q", 0x1122334455667788))
                    elif req_type == 8:
                        resp.extend([0xE3, 11, 0, 10, 0, 8])
                        resp.extend(struct.pack("<Q", 0x1122334455667788))
                    resp.extend(_TdsWireBuilder.done_token(0x0000, 0, 0))
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, bytes(resp)))
                    continue

                if "SELECT TOP 0" in utf16_text or "INSERT BULK" in utf16_text:
                    colmeta = (
                        bytes([0x81])
                        + struct.pack("<H", 2)
                        + struct.pack("<IHB", 0, 0x08, 0x7F)
                        + _TdsWireBuilder._utf16_b_varchar("id")
                        + struct.pack("<IHBH", 0, 0x09, 0xE7, 256)
                        + bytes([0x09, 0x04, 0xD0, 0x00, 0x34])
                        + _TdsWireBuilder._utf16_b_varchar("label")
                        + _TdsWireBuilder.done_token(0x0000, 0, 0)
                    )
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, colmeta))
                elif "SELECT 1" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_response("probe", 1))
                elif "RAISE_DUP" in utf16_text:
                    err = _TdsWireBuilder.error_token(2627, 1, 14, "Violation of PRIMARY KEY constraint")
                    done = _TdsWireBuilder.done_token(0x0002, 0xC1, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                elif "RAISE_DEADLOCK" in utf16_text:
                    err = _TdsWireBuilder.error_token(1205, 51, 13, "Transaction was deadlocked")
                    done = _TdsWireBuilder.done_token(0x0002, 0xC1, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                elif "RAISE_LOCK_TIMEOUT" in utf16_text:
                    err = _TdsWireBuilder.error_token(1222, 56, 16, "Lock request time out period exceeded")
                    done = _TdsWireBuilder.done_token(0x0002, 0xC1, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                elif "RAISE_SYNTAX" in utf16_text:
                    err = _TdsWireBuilder.error_token(208, 1, 16, "Invalid object name 'missing_tbl'")
                    done = _TdsWireBuilder.done_token(0x0002, 0xC1, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                elif "SELECT_EMPTY" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_items_response([]))
                elif "SELECT_ITEMS" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_items_response([(1, "alpha"), (2, "beta")]))
                elif "INSERT BULK" in utf16_text:
                    colmeta = (
                        bytes([0x81])
                        + struct.pack("<H", 2)
                        + struct.pack("<IHB", 0, 0x08, 0x7F)
                        + _TdsWireBuilder._utf16_b_varchar("id")
                        + struct.pack("<IHBH", 0, 0x08, 0xE7, 256)
                        + bytes([0x09, 0x04, 0xD0, 0x00, 0x34])
                        + _TdsWireBuilder._utf16_b_varchar("label")
                        + _TdsWireBuilder.done_token(0x0000, 0, 0)
                    )
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, colmeta))
                elif "WAIT_FOR_CANCEL" in utf16_text:
                    deadline = time.monotonic() + 2.0
                    while time.monotonic() < deadline:
                        nxt = self._read_message(conn)
                        if nxt is not None and nxt[0] == 0x06:
                            self.received_packet_types.append(0x06)
                            conn.sendall(
                                _TdsWireBuilder.build_packet(
                                    0x04,
                                    0x01,
                                    _TdsWireBuilder.done_token(0x0020, 0xC1, 0),
                                )
                            )
                            break
                else:
                    tokens = _TdsWireBuilder.doneinproc_token(0x0010, 0xC1, 1) + _TdsWireBuilder.doneproc_token(0, 0, 1)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, tokens))


def test_mssql_native_module_metadata_and_placeholder_rewriter() -> None:
    """Verify `vertebra._mssql` metadata attributes and `@P1` placeholder rewriting."""
    assert mssql_native.GIL_USED is False
    assert isinstance(mssql_native.FREE_THREADED, bool)
    assert isinstance(mssql_native.__version__, str)

    sql = "SELECT '?', [a?b], \"c$1\" FROM items WHERE id = ? AND status = $2"
    rewritten, count = rewrite_mssql_placeholders(sql)
    assert rewritten == "SELECT '?', [a?b], \"c$1\" FROM items WHERE id = @P1 AND status = @P2"
    assert count == 2
    assert MSSQLCore.rewrite_placeholders(sql) == (rewritten, 2)


def test_mssql_config_url_parsing_and_dialer_forwarding() -> None:
    """Verify `MSSQLConfig.from_url` parses query flags and forwards sync and async Cloud SQL dialers."""
    cfg = MSSQLConfig.from_url(
        "sqlserver://app_user:s3cret@sql.internal:1434/analytics"
        "?encrypt=strict&trust_server_certificate=true&application_intent=ReadOnly"
        "&connect_timeout_ms=5000&query_timeout_ms=15000"
        "&cloud_sql_instance=proj:us-central1:mssql-prod&cloudsql_ip_type=PRIVATE&cloudsql_auth_type=IAM"
    )
    assert cfg.host == "sql.internal"
    assert cfg.port == 1434
    assert cfg.database == "analytics"
    assert cfg.user == "app_user"
    assert cfg.password == "s3cret"
    assert cfg.encryption == "strict"
    assert cfg.trust_server_certificate is True
    assert cfg.read_only_intent is True
    assert cfg.connect_timeout_ms == 5000
    assert cfg.query_timeout_ms == 15000
    assert cfg.cloud_sql_instance == "proj:us-central1:mssql-prod"
    assert cfg.cloudsql_ip_type == "PRIVATE"
    assert cfg.cloudsql_auth_type == "IAM"

    reg = DatabaseRegistry()
    sync_drv = cfg.build_sync_driver(reg)
    async_drv = cfg.build_async_driver(reg)
    try:
        assert isinstance(sync_drv, SyncMSSQLDriver)
        assert isinstance(async_drv, AsyncMSSQLDriver)
        assert isinstance(sync_drv.dialer, SyncCloudSQLDialer)
        assert isinstance(async_drv.dialer, AsyncCloudSQLDialer)
    finally:
        sync_drv.close()


def test_mssql_drivers_fail_loud_when_native_extension_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `SyncMSSQLDriver` and `AsyncMSSQLDriver` raise `MissingDependencyError` when `vertebra._mssql` is blocked."""
    monkeypatch.setitem(sys.modules, "vertebra._mssql", None)
    reset_dependency_cache("vertebra._mssql")
    try:
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            SyncMSSQLDriver()
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            AsyncMSSQLDriver()
    finally:
        reset_dependency_cache("vertebra._mssql")


def test_sync_mssql_driver_loopback_execution_arrow_and_transactions() -> None:
    """Verify `SyncMSSQLDriver` end-to-end TDS execution, transactions, savepoints, VTBARROW, and BulkLoad."""
    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        assert driver.ping() is True
        assert driver.select_value("SELECT 1", value_type=int) == 1

        rows = driver.select("SELECT_ITEMS WHERE id = ?", (1,))
        assert rows == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
        assert driver.last_rewritten_sql == "SELECT_ITEMS WHERE id = @P1"

        typed_rows = driver.select("SELECT_ITEMS", schema_type=_MssqlItemRow)
        assert typed_rows == [_MssqlItemRow(id=1, label="alpha"), _MssqlItemRow(id=2, label="beta")]

        assert driver.select_one("SELECT_ITEMS") == {"id": 1, "label": "alpha"}
        assert driver.select_one_or_none("SELECT_EMPTY") is None
        with pytest.raises(NotFoundError):
            driver.select_one("SELECT_EMPTY")

        items_with_total, total = driver.select_with_total("SELECT_ITEMS")
        assert total == 2
        assert len(items_with_total) == 2

        assert driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "alpha")) == 1
        assert driver.execute_many("INSERT INTO items (id, label) VALUES (?, ?)", [(1, "a"), (2, "b")]) == 2

        with driver.transaction():
            driver.savepoint("sp_nested")
            driver.rollback_to_savepoint("sp_nested")
            driver.release_savepoint("sp_nested")
            driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (3, "c"))

        cursor = driver.select_to_arrow("SELECT_ITEMS")
        assert isinstance(cursor, ArrowCursor)
        assert cursor.used_shm is True
        assert cursor.row_count == 2
        assert cursor.to_pylist() == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
        cursor.close()

        batches = list(driver.stream_arrow_batches("SELECT_ITEMS", batch_size=1))
        assert len(batches) == 2
        assert batches[0].row_count + batches[1].row_count == 2
        batches[0].close()
        batches[1].close()

        ingested = driver.load_from_arrow(
            "dbo.items",
            [{"id": 10, "label": "ten"}, {"id": 20, "label": "twenty"}],
            columns=["id", "label"],
        )
        assert ingested == 2
        assert 0x07 in server.received_packet_types

        stats = driver.lease_stats()
        assert stats["total_acquired"] >= 1
        assert stats["active_leases"] == 0
        cache_stats = driver.statement_cache_stats()
        assert cache_stats is not None
        assert cache_stats["capacity"] >= 1
    finally:
        driver.close()
        server.close()


@pytest.mark.parametrize(
    ("marker_sql", "expected_exc", "expected_sqlstate"),
    [
        ("RAISE_DUP", IntegrityError, "23505"),
        ("RAISE_DEADLOCK", SerializationFailure, "40001"),
        ("RAISE_LOCK_TIMEOUT", QueryCancelledError, "57014"),
        ("RAISE_SYNTAX", ProgrammingError, "42P01"),
    ],
)
def test_sync_mssql_driver_maps_tds_error_numbers_to_database_error_hierarchy(
    marker_sql: str,
    expected_exc: type[DatabaseError],
    expected_sqlstate: str,
) -> None:
    """Verify TDS `TokenError` numbers map to canonical `DatabaseError` subclasses and SQLSTATEs."""
    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        with pytest.raises(expected_exc) as exc_info:
            driver.execute(marker_sql)
        assert isinstance(exc_info.value, DatabaseError)
        assert exc_info.value.sqlstate == expected_sqlstate
    finally:
        driver.close()
        server.close()


@pytest.mark.anyio
async def test_async_mssql_driver_loopback_execution_arrow_and_cancel() -> None:
    """Verify `AsyncMSSQLDriver` executes via the Tokio completion ring and attaches its own loop drainer."""
    server = _LoopbackTdsServer()
    driver = AsyncMSSQLDriver(server.dsn)
    try:
        assert await driver.ping() is True
        assert await driver.select_value("SELECT 1", value_type=int) == 1

        rows = await driver.select("SELECT_ITEMS WHERE id = ?", (1,))
        assert rows == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]

        typed_rows = await driver.select("SELECT_ITEMS", schema_type=_MssqlItemRow)
        assert typed_rows == [_MssqlItemRow(id=1, label="alpha"), _MssqlItemRow(id=2, label="beta")]

        async with driver.transaction():
            await driver.savepoint("sp_async")
            await driver.rollback_to_savepoint("sp_async")
            await driver.release_savepoint("sp_async")
            assert await driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (5, "five")) == 1

        cursor = await driver.select_to_arrow("SELECT_ITEMS")
        assert cursor.used_shm is True
        assert cursor.to_pylist() == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
        cursor.close()

        table = pa.Table.from_pydict({"id": [100, 200], "label": ["one", "two"]})
        loaded = await driver.load_from_arrow("dbo.items", table, columns=["id", "label"])
        assert loaded == 2
    finally:
        await driver.close()
        server.close()


def test_mssql_clean_break_surface_and_zero_row_colmetadata() -> None:
    """Verify PY-07 clean-break method surface (no *_transaction aliases) and 0-row COLMETADATA preservation."""
    assert not hasattr(mssql_native.SyncMSSQLClient, "begin_transaction")
    assert not hasattr(mssql_native.SyncMSSQLClient, "commit_transaction")
    assert not hasattr(mssql_native.SyncMSSQLClient, "rollback_transaction")
    assert not hasattr(mssql_native.AsyncMSSQLClient, "begin_transaction")
    assert not hasattr(mssql_native.AsyncMSSQLClient, "commit_transaction")
    assert not hasattr(mssql_native.AsyncMSSQLClient, "rollback_transaction")

    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        assert "[REDACTED]" in repr(driver._client)
        assert "Passw0rd" not in repr(driver._client)

        rows, cols = driver.execute_with_description("SELECT_EMPTY")
        assert rows == []
        assert cols == ("id", "label")
    finally:
        driver.close()
        server.close()


def test_sync_mssql_pipeline_commit_rollback_and_savepoint_nesting() -> None:
    """Verify `SyncMSSQLPipeline` delayed commit, rollback on error/unflushed queue, and nested savepoints."""
    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        assert isinstance(driver, SyncPipelineCapable)
        with driver.pipeline() as pipe:
            assert isinstance(pipe, SyncPipeline)
            assert isinstance(pipe, SyncMSSQLPipeline)
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "alpha"))
            pipe.select("SELECT_ITEMS")
            out = pipe.results()
        assert out == [1, [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]]
        assert driver.in_transaction is False

        with pytest.raises(IntegrityError), driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "alpha"))
            pipe.execute("RAISE_DUP")
            pipe.results()
        assert driver.in_transaction is False

        with pytest.raises(RuntimeError, match="abort after results"), driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "beta"))
            pipe.results()
            raise RuntimeError("abort after results")
        assert driver.in_transaction is False

        with driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (3, "gamma"))
            pipe.results()
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (4, "unflushed"))
        assert driver.in_transaction is False

        with driver.transaction():
            assert driver.in_transaction is True
            with driver.pipeline() as nested_pipe:
                nested_pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (5, "nested"))
                nested_res = nested_pipe.results()
            assert nested_res == [1]
            assert driver.in_transaction is True
        assert driver.in_transaction is False
    finally:
        driver.close()
        server.close()


@pytest.mark.anyio
async def test_async_mssql_pipeline_commit_rollback_and_savepoint_nesting() -> None:
    """Verify `AsyncMSSQLPipeline` delayed commit, rollback on error/unflushed queue, and nested savepoints."""
    server = _LoopbackTdsServer()
    driver = AsyncMSSQLDriver(server.dsn)
    try:
        assert isinstance(driver, AsyncPipelineCapable)
        rows, cols = await driver.execute_with_description("SELECT_EMPTY")
        assert rows == []
        assert cols == ("id", "label")

        async with driver.pipeline() as pipe:
            assert isinstance(pipe, AsyncPipeline)
            assert isinstance(pipe, AsyncMSSQLPipeline)
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "alpha"))
            pipe.select("SELECT_ITEMS")
            out = await pipe.results()
        assert out == [1, [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]]
        assert driver.in_transaction is False

        with pytest.raises(IntegrityError):
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "alpha"))
                pipe.execute("RAISE_DUP")
                await pipe.results()
        assert driver.in_transaction is False

        with pytest.raises(RuntimeError, match="abort async after results"):
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "beta"))
                await pipe.results()
                raise RuntimeError("abort async after results")
        assert driver.in_transaction is False

        async with driver.transaction():
            assert driver.in_transaction is True
            async with driver.pipeline() as nested_pipe:
                nested_pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (5, "nested"))
                nested_res = await nested_pipe.results()
            assert nested_res == [1]
            assert driver.in_transaction is True
        assert driver.in_transaction is False
    finally:
        await driver.close()
        server.close()


@pytest.mark.parametrize(
    ("kwargs", "match_pattern"),
    [
        ({"port": 0}, "port"),
        ({"port": 70000}, "port"),
        ({"host": ""}, "host"),
        ({"host": "bad\x00host"}, "NUL"),
        ({"user": "bad\x00user"}, "NUL"),
        ({"database": "bad\x00db"}, "NUL"),
        ({"encryption": "bogus-mode"}, "encryption"),
        ({"ca_pem": b"not-a-valid-pem"}, "PEM"),
        ({"client_cert_pem": b"not-a-valid-pem"}, "PEM"),
        ({"client_key_pem": b"not-a-valid-pem"}, "PEM"),
    ],
)
def test_mssql_config_in_memory_validation(
    kwargs: dict[str, object],
    match_pattern: str,
) -> None:
    """Verify `MSSQLConfig` and native `SyncMSSQLClient` reject invalid port, NUL, encryption, and PEM inputs."""
    with pytest.raises(ImproperConfigurationError, match=match_pattern):
        MSSQLConfig(**kwargs)
    with pytest.raises(InterfaceError, match=match_pattern):
        mssql_native.SyncMSSQLClient("mssql://127.0.0.1:1433/master", **kwargs)


def test_mssql_secret_redaction_in_repr() -> None:
    """Verify `MSSQLConfig`, `SyncMSSQLClient`, and `AsyncMSSQLClient` redact passwords in `repr()`."""
    secret = "SuperSecretTdsPassword99"
    cfg = MSSQLConfig.from_url(f"mssql://sa:{secret}@127.0.0.1:1433/appdb?encrypt=optional")
    assert secret not in repr(cfg)
    assert "[REDACTED]" in repr(cfg)

    sync_client = MSSQLCore.create_sync_rust_client(cfg)
    async_client = MSSQLCore.create_async_rust_client(cfg)
    assert secret not in repr(sync_client)
    assert "[REDACTED]" in repr(sync_client)
    assert secret not in repr(async_client)
    assert "[REDACTED]" in repr(async_client)


@pytest.mark.anyio
async def test_mssql_cloudsql_dialer_wiring_to_driver_and_client() -> None:
    """Verify `SyncCloudSQLDialer` and `AsyncCloudSQLDialer` wire instance metadata through to the native client."""
    dialer_cfg = CloudSQLConfig(
        instance_connection_name="proj:us-central1:sqlserver-prod",
        enable_iam_auth=True,
        ip_type="PRIVATE",
    )
    sync_dialer = SyncCloudSQLDialer(dialer_cfg)
    sync_driver = SyncMSSQLDriver("mssql://127.0.0.1:1433/master?encrypt=not_supported", dialer=sync_dialer)
    try:
        assert sync_driver.dialer is sync_dialer
        assert sync_driver.config.cloud_sql_instance == "proj:us-central1:sqlserver-prod"
        assert sync_driver.config.cloudsql_ip_type == "PRIVATE"
        assert sync_driver.config.cloudsql_auth_type == "IAM"
        assert "proj:us-central1:sqlserver-prod" in repr(sync_driver._client)
    finally:
        sync_driver.close()

    async_dialer = AsyncCloudSQLDialer(dialer_cfg)
    async_driver = AsyncMSSQLDriver("mssql://127.0.0.1:1433/master?encrypt=not_supported", dialer=async_dialer)
    try:
        assert async_driver.dialer is async_dialer
        assert async_driver.config.cloud_sql_instance == "proj:us-central1:sqlserver-prod"
        assert async_driver.config.cloudsql_ip_type == "PRIVATE"
        assert async_driver.config.cloudsql_auth_type == "IAM"
        assert "proj:us-central1:sqlserver-prod" in repr(async_driver._client)
    finally:
        await async_driver.close()
