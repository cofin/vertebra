"""Unit and loopback wire tests for SyncMSSQLDriver, AsyncMSSQLDriver, and vertebra._mssql."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import importlib
import socket
import struct
import sys
import threading
import time
from typing import Any

import msgspec
import pyarrow as pa
import pytest

import vertebra._mssql as mssql_native
from vertebra._vertebra import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    OperationalError,
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
from vertebra.db import dbapi as vertebra_dbapi
from vertebra.db.adapters.mssql import (
    AsyncMSSQLDriver,
    AsyncMSSQLPipeline,
    MSSQLConfig,
    MSSQLCore,
    SyncMSSQLDriver,
    SyncMSSQLPipeline,
    rewrite_mssql_placeholders,
)
from vertebra.db.driver import AZURE_MSSQL_SCOPE, AzureEntraTokenProvider
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

    @classmethod
    def select_int_nvarchar_response(cls, col1: str, col2: str, rows: list[tuple[int, str]]) -> bytes:
        """Build a two-column `(col1: Int4, col2: NVarchar)` result set packet."""
        colmeta = (
            bytes([0x81])
            + struct.pack("<H", 2)
            + struct.pack("<IHB", 0, 0x08, 0x38)
            + cls._utf16_b_varchar(col1)
            + struct.pack("<IHBH", 0, 0x08, 0xE7, 256)
            + bytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            + cls._utf16_b_varchar(col2)
        )
        row_tokens = bytearray()
        for int_val, str_val in rows:
            utf16 = str_val.encode("utf-16-le")
            row_tokens.append(0xD1)
            row_tokens.extend(struct.pack("<i", int_val))
            row_tokens.extend(struct.pack("<H", len(utf16)))
            row_tokens.extend(utf16)
        tokens = (
            colmeta + bytes(row_tokens) + cls.doneinproc_token(0x0010, 0xC1, len(rows)) + cls.doneproc_token(0, 0, 0)
        )
        return cls.build_packet(0x04, 0x01, tokens)

    @classmethod
    def select_str_response(cls, col_name: str, value: str) -> bytes:
        """Build a single-column `NVarchar` result set packet."""
        colmeta = (
            bytes([0x81])
            + struct.pack("<H", 1)
            + struct.pack("<IHBH", 0, 0x08, 0xE7, 256)
            + bytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            + cls._utf16_b_varchar(col_name)
        )
        utf16 = value.encode("utf-16-le")
        row_token = bytes([0xD1]) + struct.pack("<H", len(utf16)) + utf16
        tokens = colmeta + row_token + cls.doneinproc_token(0x0010, 0xC1, 1) + cls.doneproc_token(0, 0, 0)
        return cls.build_packet(0x04, 0x01, tokens)

    @classmethod
    def select_int_rows_response(cls, col_name: str, values: list[int]) -> bytes:
        """Build a single-column `Int4` result set packet with zero or more rows."""
        colmeta = (
            bytes([0x81]) + struct.pack("<H", 1) + struct.pack("<IHB", 0, 0x08, 0x38) + cls._utf16_b_varchar(col_name)
        )
        row_tokens = bytearray()
        for val in values:
            row_tokens.append(0xD1)
            row_tokens.extend(struct.pack("<i", val))
        tokens = (
            colmeta + bytes(row_tokens) + cls.doneinproc_token(0x0010, 0xC1, len(values)) + cls.doneproc_token(0, 0, 0)
        )
        return cls.build_packet(0x04, 0x01, tokens)

    @classmethod
    def multi_result_response(cls) -> bytes:
        """Build a packet containing two result sets to verify only the first is stored."""
        first_meta = (
            bytes([0x81]) + struct.pack("<H", 1) + struct.pack("<IHB", 0, 0x08, 0x38) + cls._utf16_b_varchar("first_id")
        )
        first_rows = bytes([0xD1]) + struct.pack("<i", 10) + bytes([0xD1]) + struct.pack("<i", 20)
        second_meta = (
            bytes([0x81])
            + struct.pack("<H", 1)
            + struct.pack("<IHB", 0, 0x08, 0x38)
            + cls._utf16_b_varchar("second_id")
        )
        second_rows = bytes([0xD1]) + struct.pack("<i", 99)
        tokens = (
            first_meta
            + first_rows
            + cls.doneinproc_token(0x0011, 0xC1, 2)
            + second_meta
            + second_rows
            + cls.doneinproc_token(0x0010, 0xC1, 1)
            + cls.doneproc_token(0, 0, 0)
        )
        return cls.build_packet(0x04, 0x01, tokens)


class _LoopbackTdsServer:
    """In-process TCP TDS 7.4 loopback server for exercising `SyncMSSQLDriver` and `AsyncMSSQLDriver`."""

    def __init__(
        self,
        *,
        reject_login_predicate: Any = None,
        close_after_query: bool = False,
        fail_begin: bool = False,
    ) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port: int = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self._reject_login_predicate = reject_login_predicate
        self._close_after_query = close_after_query
        self.fail_begin = fail_begin
        self.release_slow = threading.Event()
        self.slow_started = threading.Event()
        self.committed_rows: list[int] = []
        self.received_logins: list[bytes] = []
        self.received_packet_types: list[int] = []
        self.received_payloads: list[tuple[int, bytes]] = []
        self.received_tx_requests: list[int] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def has_wire_text(self, needle: str) -> bool:
        """Return whether ``needle`` appeared UTF-16-LE encoded in any received TDS payload."""
        raw = needle.encode("utf-16-le")
        return any(raw in payload for _, payload in self.received_payloads)

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
            self.received_logins.append(login[1])
            if self._reject_login_predicate is not None and self._reject_login_predicate(login[1]):
                err = _TdsWireBuilder.error_token(18456, 1, 14, "Login failed for user 'entra_user'")
                done = _TdsWireBuilder.done_token(0x0002, 0, 0)
                conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                return
            conn.sendall(_TdsWireBuilder.login_success_packet())

            in_tx = False
            pending_rows: list[int] = []

            while not self._stop.is_set():
                msg = self._read_message(conn)
                if msg is None:
                    break
                pkt_type, payload = msg
                self.received_packet_types.append(pkt_type)
                self.received_payloads.append((pkt_type, payload))
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
                    self.received_tx_requests.append(req_type)
                    if req_type == 5 and self.fail_begin:
                        err = _TdsWireBuilder.error_token(3902, 1, 16, "BEGIN TRANSACTION failed")
                        done = _TdsWireBuilder.done_token(0x0002, 0, 0)
                        conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                        continue
                    resp = bytearray()
                    if req_type == 5:
                        in_tx = True
                        resp.extend([0xE3, 11, 0, 8, 8])
                        resp.extend(struct.pack("<Q", 0x1122334455667788))
                        resp.append(0)
                    elif req_type == 7:
                        self.committed_rows.extend(pending_rows)
                        pending_rows.clear()
                        in_tx = False
                        resp.extend([0xE3, 11, 0, 9, 0, 8])
                        resp.extend(struct.pack("<Q", 0x1122334455667788))
                    elif req_type == 8:
                        pending_rows.clear()
                        in_tx = False
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
                elif "SELECT 1 AS a, N'x' AS b" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_nvarchar_response("a", "b", [(1, "x")]))
                elif "SELECT 1 AS a, 2 AS b" in utf16_text:
                    colmeta = (
                        bytes([0x81])
                        + struct.pack("<H", 2)
                        + struct.pack("<IHB", 0, 0x08, 0x38)
                        + _TdsWireBuilder._utf16_b_varchar("a")
                        + struct.pack("<IHB", 0, 0x08, 0x38)
                        + _TdsWireBuilder._utf16_b_varchar("b")
                    )
                    row_token = bytes([0xD1]) + struct.pack("<ii", 1, 2)
                    tokens = (
                        colmeta
                        + row_token
                        + _TdsWireBuilder.doneinproc_token(0x0010, 0xC1, 1)
                        + _TdsWireBuilder.doneproc_token(0, 0, 0)
                    )
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, tokens))
                elif "SELECT 1 AS a" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_response("a", 1))
                elif "SELECT @P1 AS a" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_response("a", 5))
                elif "UPDATE two_rows" in utf16_text:
                    tokens = _TdsWireBuilder.doneinproc_token(0x0010, 0xC1, 2) + _TdsWireBuilder.doneproc_token(0, 0, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, tokens))
                elif "SELECT 1 WHERE 1 = 0" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_rows_response("c", []))
                elif "SELECT 'output' AS output" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_str_response("output", "output"))
                elif "SELECT @P1 + @P2" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_response("sum", 3))
                elif "CREATE TABLE #t" in utf16_text:
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, _TdsWireBuilder.done_token(0x0000, 0, 0)))
                elif "INSERT INTO #three" in utf16_text:
                    tokens = _TdsWireBuilder.doneinproc_token(0x0010, 0xC1, 3) + _TdsWireBuilder.doneproc_token(0, 0, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, tokens))
                elif "OUTPUT inserted.id" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_rows_response("id", [101, 102]))
                elif "SELECT_MULTI" in utf16_text:
                    conn.sendall(_TdsWireBuilder.multi_result_response())
                elif "SELEC 1" in utf16_text:
                    err = _TdsWireBuilder.error_token(102, 1, 15, "Incorrect syntax near 'SELEC'")
                    done = _TdsWireBuilder.done_token(0x0002, 0xC1, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, err + done))
                elif "INSERT INTO #lazy_t" in utf16_text:
                    if in_tx:
                        pending_rows.append(1)
                    else:
                        self.committed_rows.append(1)
                    tokens = _TdsWireBuilder.doneinproc_token(0x0010, 0xC1, 1) + _TdsWireBuilder.doneproc_token(0, 0, 0)
                    conn.sendall(_TdsWireBuilder.build_packet(0x04, 0x01, tokens))
                elif "SELECT COUNT(*) AS cnt FROM #lazy_t" in utf16_text:
                    total = len(self.committed_rows) + len(pending_rows)
                    conn.sendall(_TdsWireBuilder.select_int_response("cnt", total))
                elif "SLOW_CURSOR_HOLD" in utf16_text:
                    self.slow_started.set()
                    self.release_slow.wait(timeout=5.0)
                    conn.sendall(_TdsWireBuilder.select_int_response("slow", 1))
                elif "SELECT 1" in utf16_text:
                    conn.sendall(_TdsWireBuilder.select_int_response("probe", 1))
                    if self._close_after_query:
                        break
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

    sql = "SELECT '?', [a?b], \"c$1\" FROM items WHERE id = ? AND status = $2"
    rewritten, count = rewrite_mssql_placeholders(sql)
    assert rewritten == "SELECT '?', [a?b], \"c$1\" FROM items WHERE id = @P1 AND status = @P2"
    assert count == 2
    assert MSSQLCore.rewrite_placeholders(sql) == (rewritten, 2)


@pytest.mark.parametrize(
    ("sql", "expected_sql", "expected_count"),
    [
        (
            "INSERT INTO t (a, b, c, d) VALUES (@P1, @p2, @P1, ?)",
            "INSERT INTO t (a, b, c, d) VALUES (@P1, @P2, @P1, @P3)",
            3,
        ),
        (
            "SELECT '@P9', \"@P8\", [@P7], @@P6, @P1a, @P0, @P1_col, @P1@x FROM t WHERE a = @P2 AND b = @P1",
            "SELECT '@P9', \"@P8\", [@P7], @@P6, @P1a, @P0, @P1_col, @P1@x FROM t WHERE a = @P2 AND b = @P1",
            2,
        ),
        (
            "SELECT a@P1 FROM t -- @P3\n/* @P4 */",
            "SELECT a@P1 FROM t -- @P3\n/* @P4 */",
            0,
        ),
    ],
)
@pytest.mark.parametrize("use_native", [True, False], ids=["native", "python-fallback"])
def test_mssql_placeholder_rewriter_counts_existing_at_p_placeholders(
    monkeypatch: pytest.MonkeyPatch,
    sql: str,
    expected_sql: str,
    expected_count: int,
    use_native: bool,
) -> None:
    """Verify native and fallback MSSQL placeholder rewriters count `@P<n>` placeholders and honor boundaries."""
    if not use_native:
        monkeypatch.setattr(
            "vertebra.db.adapters.mssql.core.import_optional",
            lambda _name: None,
        )
    assert MSSQLCore.rewrite_placeholders(sql) == (expected_sql, expected_count)


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
    """Verify clean-break method surface (no *_transaction aliases) and 0-row COLMETADATA preservation."""
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
            pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (4, "tail_auto_flushed"))
        assert driver.in_transaction is False
        assert server.has_wire_text("tail_auto_flushed")

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
    kwargs: dict[str, Any],
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


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncMSSQLDriver", "vertebra.db.adapters.mssql", "vertebra.db.adapters.mssql._sync"),
        ("SyncMSSQLDriver", "vertebra.db.adapters.mssql.driver", "vertebra.db.adapters.mssql._sync"),
        ("SyncMSSQLDriver", "vertebra.db.adapters.mssql._sync", "vertebra.db.adapters.mssql._sync"),
        ("AsyncMSSQLDriver", "vertebra.db.adapters.mssql", "vertebra.db.adapters.mssql._async"),
        ("AsyncMSSQLDriver", "vertebra.db.adapters.mssql.driver", "vertebra.db.adapters.mssql._async"),
        ("AsyncMSSQLDriver", "vertebra.db.adapters.mssql._async", "vertebra.db.adapters.mssql._async"),
        ("SyncMSSQLPipeline", "vertebra.db.adapters.mssql", "vertebra.db.adapters.mssql.pipeline"),
        ("SyncMSSQLPipeline", "vertebra.db.adapters.mssql.pipeline", "vertebra.db.adapters.mssql.pipeline"),
        ("AsyncMSSQLPipeline", "vertebra.db.adapters.mssql", "vertebra.db.adapters.mssql.pipeline"),
        ("AsyncMSSQLPipeline", "vertebra.db.adapters.mssql.pipeline", "vertebra.db.adapters.mssql.pipeline"),
    ],
)
def test_mssql_driver_and_pipeline_resolution_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify SQL Server sync/async driver and pipeline classes resolve identically across import paths."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "caller_tx_clean", "exception_discard", "caller_tx_exception"],
)
def test_sync_mssql_pipeline_exit_contract(scenario: str) -> None:
    """SyncMSSQLPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        if scenario == "auto_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "auto_sync"))
            assert driver.in_transaction is False
            assert server.has_wire_text("auto_sync")
            assert server.received_tx_requests == [5, 7]
        elif scenario == "caller_tx_clean":
            with driver.transaction():
                assert driver.in_transaction is True
                with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "nested_auto"))
                assert driver.in_transaction is True
                assert server.has_wire_text("nested_auto")
                assert server.has_wire_text("SAVE TRANSACTION [_vtb_pipe_")
                assert server.received_tx_requests == [5]
            assert server.received_tx_requests == [5, 7]
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (3, "flushed_before_err"))
                pipe.results()
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (4, "never_sent"))
                raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert server.has_wire_text("flushed_before_err")
            assert not server.has_wire_text("never_sent")
            assert server.received_tx_requests == [5, 8]
        else:
            with driver.transaction():
                with pytest.raises(RuntimeError, match="abort_inner"), driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (5, "inner_flushed"))
                    pipe.results()
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (6, "inner_never_sent"))
                    raise RuntimeError("abort_inner")
                assert driver.in_transaction is True
                assert server.has_wire_text("inner_flushed")
                assert not server.has_wire_text("inner_never_sent")
                assert server.has_wire_text("SAVE TRANSACTION [_vtb_pipe_")
                assert server.has_wire_text("ROLLBACK TRANSACTION [_vtb_pipe_")
                assert server.received_tx_requests == [5]
            assert server.received_tx_requests == [5, 7]
    finally:
        driver.close()
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "caller_tx_clean", "exception_discard", "caller_tx_exception"],
)
async def test_async_mssql_pipeline_exit_contract(scenario: str) -> None:
    """AsyncMSSQLPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    server = _LoopbackTdsServer()
    driver = AsyncMSSQLDriver(server.dsn)
    try:
        if scenario == "auto_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "auto_async"))
            assert driver.in_transaction is False
            assert server.has_wire_text("auto_async")
            assert server.received_tx_requests == [5, 7]
        elif scenario == "caller_tx_clean":
            async with driver.transaction():
                assert driver.in_transaction is True
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "nested_auto"))
                assert driver.in_transaction is True
                assert server.has_wire_text("nested_auto")
                assert server.has_wire_text("SAVE TRANSACTION [_vtb_pipe_")
                assert server.received_tx_requests == [5]
            assert server.received_tx_requests == [5, 7]
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (3, "flushed_before_err"))
                    await pipe.results()
                    pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (4, "never_sent"))
                    raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert server.has_wire_text("flushed_before_err")
            assert not server.has_wire_text("never_sent")
            assert server.received_tx_requests == [5, 8]
        else:
            async with driver.transaction():
                with pytest.raises(RuntimeError, match="abort_inner"):
                    async with driver.pipeline() as pipe:
                        pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (5, "inner_flushed"))
                        await pipe.results()
                        pipe.execute("INSERT INTO items (id, label) VALUES (?, ?)", (6, "inner_never_sent"))
                        raise RuntimeError("abort_inner")
                assert driver.in_transaction is True
                assert server.has_wire_text("inner_flushed")
                assert not server.has_wire_text("inner_never_sent")
                assert server.has_wire_text("SAVE TRANSACTION [_vtb_pipe_")
                assert server.has_wire_text("ROLLBACK TRANSACTION [_vtb_pipe_")
                assert server.received_tx_requests == [5]
            assert server.received_tx_requests == [5, 7]
    finally:
        await driver.close()
        server.close()


@pytest.mark.parametrize(
    ("explicit_scope", "expected_scope"),
    [
        (None, AZURE_MSSQL_SCOPE),
        ("https://custom.database.windows.net/.default", "https://custom.database.windows.net/.default"),
    ],
)
def test_mssql_config_azure_entra_scope_and_redaction(
    explicit_scope: str | None,
    expected_scope: str,
) -> None:
    """Verify `MSSQLConfig` injects `AZURE_MSSQL_SCOPE`, redacts Entra tokens, and rejects blank tokens."""
    seen_scopes: list[str] = []

    def _fetch(scope: str) -> tuple[str, float]:
        seen_scopes.append(scope)
        return (f"test-token-placeholder-{scope}", 5000.0)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="mssql-client",
        client_secret="test-client-secret-placeholder",
        scope=explicit_scope,
        fetcher=_fetch,
        clock=lambda: 1000.0,
    )
    cfg = MSSQLConfig(
        dsn="mssql://entra_user@127.0.0.1:1433/appdb?encrypt=not_supported",
        token_provider=provider,
    )
    assert cfg.token_provider.scope == expected_scope

    rendered = repr(cfg)
    assert "AzureEntraTokenProvider" in rendered
    assert "test-client-secret-placeholder" not in rendered

    sync_client = MSSQLCore.create_sync_rust_client(cfg)
    async_client = MSSQLCore.create_async_rust_client(cfg)
    assert seen_scopes == [expected_scope]
    for client_repr in (repr(sync_client), repr(async_client)):
        assert "test-token-placeholder-" not in client_repr
        assert "[REDACTED]" in client_repr

    with pytest.raises(InterfaceError, match="aad_token"):
        mssql_native.SyncMSSQLClient("mssql://127.0.0.1:1433/master", aad_token="   ")
    with pytest.raises(InterfaceError, match="aad_token"):
        mssql_native.AsyncMSSQLClient("mssql://127.0.0.1:1433/master", aad_token="")
    with pytest.raises(InterfaceError, match="aad_token"):
        sync_client.set_aad_token("   ")
    with pytest.raises(InterfaceError, match="aad_token"):
        async_client.set_aad_token(" \t\n ")

    blank_provider = AzureEntraTokenProvider(
        scope=AZURE_MSSQL_SCOPE,
        fetcher=lambda _scope: "   ",
        clock=lambda: 1000.0,
    )
    blank_cfg = MSSQLConfig(
        dsn="mssql://entra_user@127.0.0.1:1433/appdb?encrypt=not_supported",
        token_provider=blank_provider,
    )
    with pytest.raises(ImproperConfigurationError, match="empty"):
        blank_cfg.resolve_aad_token_sync()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mssql_driver_azure_entra_token_wire_and_reconnect_refresh(mode: str) -> None:
    """Verify Sync and Async MSSQL drivers pass `FEDAUTH` tokens on `LOGIN7` and refresh via `set_aad_token`."""
    now = [1000.0]
    issued: list[str] = []
    seen_scopes: list[str] = []

    def _sync_fetch(scope: str) -> dict[str, Any]:
        seen_scopes.append(scope)
        tok = f"test-token-placeholder-mssql-{len(issued) + 1}"
        issued.append(tok)
        return {"access_token": tok, "expires_in": 600}

    async def _async_fetch(scope: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return _sync_fetch(scope)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="mssql-client",
        client_secret="test-client-secret-placeholder",
        refresh_margin_seconds=120.0,
        clock=lambda: now[0],
        fetcher=_async_fetch if mode == "async" else _sync_fetch,
    )

    reject_stale = {"active": False}

    def _reject_stale(login_payload: bytes) -> bool:
        if not reject_stale["active"] or not issued:
            return False
        return issued[0].encode("utf-16-le") in login_payload

    server = _LoopbackTdsServer(reject_login_predicate=_reject_stale)
    cfg = MSSQLConfig.from_url(
        f"mssql://entra_user@127.0.0.1:{server.port}/master?encrypt=not_supported",
        token_provider=provider,
    )

    try:
        if mode == "sync":
            driver = SyncMSSQLDriver(cfg)
            try:
                assert driver.config.token_provider.scope == AZURE_MSSQL_SCOPE
                initial_client = driver._client
                assert len(issued) == 1

                now[0] = 1485.0
                assert driver.select_value("SELECT 1", value_type=int) == 1
                assert driver._client is initial_client
                assert len(issued) == 2
                assert issued[1].encode("utf-16-le") in server.received_logins[0]
            finally:
                driver.close()

            issued.clear()
            now[0] = 1000.0
            cfg.token_provider.invalidate()
            driver2 = SyncMSSQLDriver(cfg)
            try:
                initial_client2 = driver2._client
                reject_stale["active"] = True
                assert driver2.select_value("SELECT 1", value_type=int) == 1
                assert driver2._client is initial_client2
                assert len(issued) == 2
                assert issued[0].encode("utf-16-le") in server.received_logins[1]
                assert issued[1].encode("utf-16-le") in server.received_logins[2]
            finally:
                driver2.close()
        else:
            async_driver = AsyncMSSQLDriver(cfg)
            try:
                assert async_driver.config.token_provider.scope == AZURE_MSSQL_SCOPE
                initial_client = async_driver._client
                await async_driver.connect()
                assert len(issued) == 1

                now[0] = 1485.0
                assert await async_driver.select_value("SELECT 1", value_type=int) == 1
                assert async_driver._client is initial_client
                assert len(issued) == 2
                assert issued[1].encode("utf-16-le") in server.received_logins[0]
            finally:
                await async_driver.close()

            issued.clear()
            now[0] = 1000.0
            cfg.token_provider.invalidate()
            async_driver2 = AsyncMSSQLDriver(cfg)
            try:
                initial_client2 = async_driver2._client
                reject_stale["active"] = True
                assert await async_driver2.select_value("SELECT 1", value_type=int) == 1
                assert async_driver2._client is initial_client2
                assert len(issued) == 2
                assert issued[0].encode("utf-16-le") in server.received_logins[1]
                assert issued[1].encode("utf-16-le") in server.received_logins[2]
            finally:
                await async_driver2.close()
    finally:
        server.close()
    assert all(s == AZURE_MSSQL_SCOPE for s in seen_scopes)


def _make_mssql_entra_provider() -> AzureEntraTokenProvider:
    """Build an offline Azure Entra token provider for SQL Server transport-security policy tests."""
    return AzureEntraTokenProvider(
        fetcher=lambda scope: (f"test-token-placeholder-{scope}", 5000.0),
        clock=lambda: 1000.0,
    )


_MSSQL_PLAINTEXT_LOGIN_MODES = [
    "not_supported",
    "disabled",
    "disable",
    "false",
    "off",
    "no",
    "NOT_SUPPORTED",
    "login_only",
    "login-only",
    "prefer",
    "optional",
]
_MSSQL_TLS_LOGIN_MODES = [
    "required",
    "require",
    "true",
    "on",
    "yes",
    "mandatory",
    "strict",
    "verify-full",
    "verify_full",
    "verify-ca",
    "verify_ca",
]


@pytest.mark.parametrize("encryption", _MSSQL_PLAINTEXT_LOGIN_MODES)
@pytest.mark.parametrize(
    "source", ["field", "url_encryption", "url_encrypt", "sqlserver_url", "replace", "dbapi_vtb_url"]
)
def test_mssql_entra_remote_host_rejects_unencrypted_login_modes(encryption: str, source: str) -> None:
    """Verify an Entra token provider on a remote host rejects encryption modes that allow a plaintext login."""
    provider = _make_mssql_entra_provider()
    remote = "entra_user@sql.remote.example:1433/appdb"
    if source == "dbapi_vtb_url":
        with pytest.raises(vertebra_dbapi.InterfaceError, match="encryption") as dbapi_exc:
            vertebra_dbapi.connect(f"mssql+vtb://{remote}?encryption={encryption}", token_provider=provider)
        assert isinstance(dbapi_exc.value.__cause__, ImproperConfigurationError)
        return
    with pytest.raises(ImproperConfigurationError, match="encryption") as exc_info:
        if source == "field":
            MSSQLConfig(host="sql.remote.example", encryption=encryption, token_provider=provider)
        elif source == "url_encryption":
            MSSQLConfig.from_url(f"mssql://{remote}?encryption={encryption}", token_provider=provider)
        elif source == "url_encrypt":
            MSSQLConfig.from_url(f"mssql://{remote}?encrypt={encryption}", token_provider=provider)
        elif source == "sqlserver_url":
            MSSQLConfig.from_url(f"sqlserver://{remote}?encryption={encryption}", token_provider=provider)
        else:
            msgspec.structs.replace(
                MSSQLConfig(host="sql.remote.example", token_provider=provider),
                encryption=encryption,
            )
    assert "test-token-placeholder" not in str(exc_info.value)


@pytest.mark.parametrize("encryption", ["required", "strict"])
@pytest.mark.parametrize("source", ["field", "url_snake", "url_pascal", "dbapi_vtb_url"])
def test_mssql_entra_remote_host_rejects_trust_server_certificate(encryption: str, source: str) -> None:
    """Verify an Entra token provider on a remote host rejects skipping server certificate verification."""
    provider = _make_mssql_entra_provider()
    remote = f"entra_user@sql.remote.example:1433/appdb?encryption={encryption}"
    if source == "dbapi_vtb_url":
        with pytest.raises(vertebra_dbapi.InterfaceError, match="trust_server_certificate") as dbapi_exc:
            vertebra_dbapi.connect(f"mssql+vtb://{remote}&trust_server_certificate=true", token_provider=provider)
        assert isinstance(dbapi_exc.value.__cause__, ImproperConfigurationError)
        return
    with pytest.raises(ImproperConfigurationError, match="trust_server_certificate"):
        if source == "field":
            MSSQLConfig(
                host="sql.remote.example",
                encryption=encryption,
                trust_server_certificate=True,
                token_provider=provider,
            )
        elif source == "url_snake":
            MSSQLConfig.from_url(f"mssql://{remote}&trust_server_certificate=true", token_provider=provider)
        else:
            MSSQLConfig.from_url(f"mssql://{remote}&TrustServerCertificate=yes", token_provider=provider)


@pytest.mark.parametrize("encryption", _MSSQL_TLS_LOGIN_MODES)
@pytest.mark.parametrize("host", ["sql.remote.example", "10.0.0.42", "localhost%evil.example.com"])
def test_mssql_entra_remote_host_accepts_verified_tls_modes(encryption: str, host: str) -> None:
    """Verify an Entra token provider on a remote host keeps accepting verified full-session TLS modes."""
    cfg = MSSQLConfig(host=host, encryption=encryption, token_provider=_make_mssql_entra_provider())
    assert cfg.encryption == encryption
    assert cfg.trust_server_certificate is False
    assert cfg.token_provider.scope == AZURE_MSSQL_SCOPE


@pytest.mark.parametrize("encryption", ["not_supported", "off", "login_only", "required"])
@pytest.mark.parametrize("trust_server_certificate", [False, True])
@pytest.mark.parametrize(
    ("host", "with_provider"),
    [
        ("127.0.0.1", True),
        ("localhost", True),
        ("::1", True),
        ("sql.remote.example", False),
    ],
)
def test_mssql_transport_security_unchanged_for_loopback_or_password_auth(
    encryption: str,
    trust_server_certificate: bool,
    host: str,
    with_provider: bool,
) -> None:
    """Verify loopback Entra targets and remote password logins keep every encryption and trust setting."""
    cfg = MSSQLConfig(
        host=host,
        encryption=encryption,
        trust_server_certificate=trust_server_certificate,
        token_provider=_make_mssql_entra_provider() if with_provider else None,
    )
    assert cfg.encryption == encryption
    assert cfg.trust_server_certificate is trust_server_certificate


class TypedParameter:
    """Minimal TypedParameter stand-in for testing Rust slow-path `.value` unwrapping."""

    def __init__(self, value: Any) -> None:
        self.value = value


def test_sync_cursor_matrix_and_fetch_semantics() -> None:
    """Verify `SyncMSSQLCursor` across select, command, DML, OUTPUT, empty, positional, executemany, multi-set, and errors."""
    server = _LoopbackTdsServer()
    client = mssql_native.SyncMSSQLClient(server.dsn)
    try:
        cur = client.cursor()
        assert isinstance(cur, mssql_native.SyncMSSQLCursor)
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.arraysize == 1
        assert cur.fetchone() is None
        assert cur.fetchmany() == []
        assert cur.fetchall() == []

        assert cur.execute("SELECT 1 AS a, N'x' AS b") is cur
        assert cur.description == (
            ("a", "int", None, None, None, None, None),
            ("b", "nvarchar", None, None, None, None, None),
        )
        assert cur.rowcount == 1
        assert cur.fetchall() == [(1, "x")]
        assert cur.fetchone() is None

        cur.execute("CREATE TABLE #t (id INT)")
        assert cur.description is None
        assert cur.fetchone() is None
        assert cur.fetchall() == []

        cur.execute("INSERT INTO #three (v) VALUES (1), (2), (3)")
        assert cur.description is None
        assert cur.rowcount == 3

        cur.execute("INSERT INTO #t (id) OUTPUT inserted.id VALUES (101), (102)")
        assert cur.description == (("id", "int", None, None, None, None, None),)
        assert cur.rowcount == 2
        assert cur.fetchall() == [(101,), (102,)]

        cur.execute("SELECT 'output' AS output")
        assert cur.description == (("output", "nvarchar", None, None, None, None, None),)
        assert cur.fetchall() == [("output",)]

        cur.execute("SELECT 1 WHERE 1 = 0")
        assert cur.description is not None
        assert cur.rowcount == 0
        assert cur.fetchall() == []

        cur.execute("SELECT ? + ?", (1, 2))
        assert server.has_wire_text("SELECT @P1 + @P2")
        assert cur.fetchall() == [(3,)]

        cur.execute("SELECT ? + ?", (TypedParameter(1), TypedParameter(2)))
        assert cur.fetchall() == [(3,)]

        cur.execute("SELECT @a + @b", {"a": 1, "b": 2})
        assert cur.fetchall() == [(3,)]

        cur.execute("SELECT_MULTI")
        assert cur.description == (("first_id", "int", None, None, None, None, None),)
        assert cur.rowcount == 2
        cur.arraysize = 1
        assert cur.fetchmany(0) == []
        assert cur.fetchmany() == [(10,)]
        assert cur.fetchall() == [(20,)]

        with pytest.raises(ProgrammingError):
            cur.arraysize = 0
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)

        assert cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(1,), (2,), (TypedParameter(3),)]) is cur
        assert cur.description is None
        assert cur.rowcount == 3

        cur.execute("SELECT 1 AS a, N'x' AS b")
        assert cur.description is not None
        with pytest.raises(IntegrityError):
            cur.execute("RAISE_DUP")
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None

        cur.execute("SELECT 1 AS a, N'x' AS b")
        with pytest.raises(ProgrammingError):
            cur.execute("SELEC 1")
        assert cur.description is None
        assert cur.rowcount == -1

        cur.close()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1")
        with pytest.raises(InterfaceError):
            cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(1,)])
        with pytest.raises(InterfaceError):
            cur.fetchone()
        with pytest.raises(InterfaceError):
            cur.fetchmany()
        with pytest.raises(InterfaceError):
            cur.fetchall()

        with client.cursor() as ctx_cur:
            assert ctx_cur.execute("SELECT 1 AS a, N'x' AS b").fetchall() == [(1, "x")]
        with pytest.raises(InterfaceError):
            ctx_cur.fetchone()
    finally:
        client.close()
        server.close()


def test_sync_cursor_lazy_begin_and_rollback_commit() -> None:
    """Verify `lazy_begin=True` opens transactions on demand, rolls back rows, re-opens after commit, and clears on begin failure."""
    server = _LoopbackTdsServer()
    client = mssql_native.SyncMSSQLClient(server.dsn)
    try:
        default_cur = client.cursor()
        default_cur.execute("INSERT INTO #lazy_t VALUES (1)")
        assert client.in_transaction is False
        assert server.received_tx_requests == []
        default_cur.close()

        lazy_cur = client.cursor(lazy_begin=True)
        lazy_cur.execute("INSERT INTO #lazy_t VALUES (2)")
        assert client.in_transaction is True
        assert server.received_tx_requests == [5]
        assert lazy_cur.execute("SELECT COUNT(*) AS cnt FROM #lazy_t").fetchone() == (2,)
        client.rollback()
        assert client.in_transaction is False
        assert client.cursor().execute("SELECT COUNT(*) AS cnt FROM #lazy_t").fetchone() == (1,)

        lazy_cur.execute("INSERT INTO #lazy_t VALUES (3)")
        assert client.in_transaction is True
        client.commit()
        assert client.in_transaction is False

        lazy_cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(4,), (5,)])
        assert client.in_transaction is True
        client.commit()
        assert client.in_transaction is False
        assert server.received_tx_requests.count(5) == 3

        lazy_cur.execute("SELECT 1 AS a, N'x' AS b")
        assert lazy_cur.description is not None
        client.commit()
        server.fail_begin = True
        with pytest.raises(DatabaseError):
            lazy_cur.execute("INSERT INTO #lazy_t VALUES (6)")
        assert lazy_cur.description is None
        assert lazy_cur.rowcount == -1
        lazy_cur.close()
    finally:
        client.close()
        server.close()


def test_sync_cursor_client_lease_lifecycle() -> None:
    """Verify `SyncMSSQLClient` lease attach, idempotent release, replacement, close, and GC drop."""
    server = _LoopbackTdsServer()
    try:
        calls: list[str] = []
        client = mssql_native.SyncMSSQLClient(server.dsn)
        client.attach_lease(lambda: calls.append("first"))
        client.release_lease()
        client.release_lease()
        assert calls == ["first"]

        client.attach_lease(lambda: calls.append("second"))
        client.attach_lease(lambda: calls.append("third"))
        assert calls == ["first", "second"]
        client.close()
        assert calls == ["first", "second", "third"]

        gc_calls: list[int] = []
        gc_client: Any = mssql_native.SyncMSSQLClient(server.dsn)
        gc_client.attach_lease(lambda: gc_calls.append(1))
        del gc_client
        gc.collect()
        assert gc_calls == [1]
    finally:
        server.close()


def test_async_cursor_matrix_and_concurrent_guard() -> None:
    """Verify `AsyncMSSQLCursor` across the full matrix, pagination, errors, closed checks, and in-flight guard."""

    async def _run() -> None:
        server = _LoopbackTdsServer()
        client = mssql_native.AsyncMSSQLClient(server.dsn)
        try:
            cur = client.cursor()
            assert isinstance(cur, mssql_native.AsyncMSSQLCursor)
            assert cur.description is None
            assert cur.rowcount == -1
            assert cur.lastrowid is None
            assert cur.arraysize == 1
            assert await cur.fetchone() is None
            assert await cur.fetchmany() == []
            assert await cur.fetchall() == []

            assert await cur.execute("SELECT 1 AS a, N'x' AS b") is cur
            assert cur.description == (
                ("a", "int", None, None, None, None, None),
                ("b", "nvarchar", None, None, None, None, None),
            )
            assert cur.rowcount == 1
            assert await cur.fetchall() == [(1, "x")]
            assert await cur.fetchone() is None

            await cur.execute("CREATE TABLE #t (id INT)")
            assert cur.description is None

            await cur.execute("INSERT INTO #three (v) VALUES (1), (2), (3)")
            assert cur.description is None
            assert cur.rowcount == 3

            await cur.execute("INSERT INTO #t (id) OUTPUT inserted.id VALUES (101), (102)")
            assert cur.description == (("id", "int", None, None, None, None, None),)
            assert cur.rowcount == 2
            assert await cur.fetchall() == [(101,), (102,)]

            await cur.execute("SELECT 'output' AS output")
            assert cur.description == (("output", "nvarchar", None, None, None, None, None),)
            assert await cur.fetchall() == [("output",)]

            await cur.execute("SELECT 1 WHERE 1 = 0")
            assert cur.description is not None
            assert cur.rowcount == 0
            assert await cur.fetchall() == []

            await cur.execute("SELECT ? + ?", (TypedParameter(1), TypedParameter(2)))
            assert await cur.fetchall() == [(3,)]

            await cur.execute("SELECT @a + @b", {"a": 1, "b": 2})
            assert await cur.fetchall() == [(3,)]

            await cur.execute("SELECT_MULTI")
            assert cur.description == (("first_id", "int", None, None, None, None, None),)
            assert cur.rowcount == 2
            assert await cur.fetchmany(0) == []
            assert await cur.fetchmany(1) == [(10,)]
            assert await cur.fetchall() == [(20,)]

            with pytest.raises(ProgrammingError):
                cur.arraysize = 0
            with pytest.raises(ProgrammingError):
                await cur.fetchmany(-1)

            assert await cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(1,), (2,), (3,)]) is cur
            assert cur.description is None
            assert cur.rowcount == 3

            await cur.execute("SELECT 1 AS a, N'x' AS b")
            with pytest.raises(IntegrityError):
                await cur.execute("RAISE_DUP")
            assert cur.description is None
            assert cur.rowcount == -1

            await cur.execute("SELECT 1 AS a, N'x' AS b")
            with pytest.raises(ProgrammingError):
                await cur.execute("SELEC 1")
            assert cur.description is None
            assert cur.rowcount == -1

            inflight_task = cur.execute("SLOW_CURSOR_HOLD")
            assert await asyncio.to_thread(server.slow_started.wait, 5.0)
            with pytest.raises(InterfaceError, match="cursor is already executing"):
                await cur.execute("SELECT 1")
            with pytest.raises(InterfaceError, match="cursor is already executing"):
                await cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(1,)])
            server.release_slow.set()
            await inflight_task
            assert await cur.fetchall() == [(1,)]

            await cur.close()
            await cur.close()
            with pytest.raises(InterfaceError):
                await cur.execute("SELECT 1")
            with pytest.raises(InterfaceError):
                await cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(1,)])
            with pytest.raises(InterfaceError):
                await cur.fetchone()
            with pytest.raises(InterfaceError):
                await cur.fetchmany()
            with pytest.raises(InterfaceError):
                await cur.fetchall()
        finally:
            server.release_slow.set()
            await client.close()
            server.close()

    asyncio.run(_run())


def test_async_cursor_fetch_returns_completed_awaitable() -> None:
    """Verify `AsyncMSSQLCursor.fetchall` returns a completed awaitable without yielding."""

    async def _run() -> None:
        server = _LoopbackTdsServer()
        client = mssql_native.AsyncMSSQLClient(server.dsn)
        try:
            cur = client.cursor()
            await cur.execute("SELECT 1 AS a, N'x' AS b")
            step = cur.fetchall().__await__()
            with pytest.raises(StopIteration) as exc_info:
                next(step)
            assert exc_info.value.value == [(1, "x")]
        finally:
            await client.close()
            server.close()

    asyncio.run(_run())


def test_async_cursor_close_takes_effect_at_call() -> None:
    """Verify `AsyncMSSQLCursor.close` closes state at call time and `fetchone` raises on await."""

    async def _run() -> None:
        server = _LoopbackTdsServer()
        client = mssql_native.AsyncMSSQLClient(server.dsn)
        try:
            cur = client.cursor()
            cur.close()
            pending = cur.fetchone()
            with pytest.raises(InterfaceError):
                await pending
        finally:
            await client.close()
            server.close()

    asyncio.run(_run())


def test_async_cursor_fetchmany_errors_raise_at_call() -> None:
    """Verify `AsyncMSSQLCursor.fetchmany` raises negative-size and closed-cursor errors at call time."""

    async def _run() -> None:
        server = _LoopbackTdsServer()
        client = mssql_native.AsyncMSSQLClient(server.dsn)
        try:
            cur = client.cursor()
            with pytest.raises(ProgrammingError):
                cur.fetchmany(-1)
            cur.close()
            with pytest.raises(InterfaceError):
                cur.fetchmany()
        finally:
            await client.close()
            server.close()

    asyncio.run(_run())


def test_async_cursor_lazy_begin_and_lease_lifecycle() -> None:
    """Verify `AsyncMSSQLCursor` `lazy_begin` transitions and `AsyncMSSQLClient` lease release semantics."""

    async def _run() -> None:
        server = _LoopbackTdsServer()
        client = mssql_native.AsyncMSSQLClient(server.dsn)
        try:
            default_cur = client.cursor()
            await default_cur.execute("INSERT INTO #lazy_t VALUES (1)")
            assert client.in_transaction is False
            await default_cur.close()

            lazy_cur = client.cursor(lazy_begin=True)
            await lazy_cur.execute("INSERT INTO #lazy_t VALUES (2)")
            assert client.in_transaction is True
            await client.rollback()
            assert client.in_transaction is False
            check_cur = client.cursor()
            await check_cur.execute("SELECT COUNT(*) AS cnt FROM #lazy_t")
            assert await check_cur.fetchone() == (1,)
            await check_cur.close()

            await lazy_cur.execute("INSERT INTO #lazy_t VALUES (3)")
            assert client.in_transaction is True
            await client.commit()
            assert client.in_transaction is False

            await lazy_cur.executemany("INSERT INTO #lazy_t VALUES (?)", [(4,), (5,)])
            assert client.in_transaction is True
            await client.commit()
            assert client.in_transaction is False

            await lazy_cur.execute("SELECT 1 AS a, N'x' AS b")
            await client.commit()
            server.fail_begin = True
            with pytest.raises(DatabaseError):
                await lazy_cur.execute("INSERT INTO #lazy_t VALUES (6)")
            assert lazy_cur.description is None
            assert lazy_cur.rowcount == -1
            await lazy_cur.close()

            calls: list[str] = []
            client.attach_lease(lambda: calls.append("a"))
            client.release_lease()
            client.release_lease()
            assert calls == ["a"]
            client.attach_lease(lambda: calls.append("b"))
            client.attach_lease(lambda: calls.append("c"))
            assert calls == ["a", "b"]
            await client.close()
            assert calls == ["a", "b", "c"]

            gc_calls: list[int] = []
            gc_client: Any = mssql_native.AsyncMSSQLClient(server.dsn)
            gc_client.attach_lease(lambda: gc_calls.append(1))
            del gc_client
            gc.collect()
            assert gc_calls == [1]
        finally:
            server.close()

    asyncio.run(_run())


class _RecordingLeaseGrant:
    """Minimal lease grant returned by `_RecordingBroker`."""

    def __init__(self, slot_index: int, generation: int) -> None:
        self.slot_index = slot_index
        self.generation = generation


class _RecordingBroker:
    """Recording broker double asserting exact acquire and release counts."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.acquire_calls: int = 0
        self.release_calls: int = 0
        self._generation: int = 0

    def acquire(self, pid: int, *, timeout_ms: int = 5000, bind_key_hash: int = 0) -> _RecordingLeaseGrant:
        """Record an acquire attempt and either raise `RuntimeError` or return a grant."""
        _ = (pid, timeout_ms, bind_key_hash)
        self.acquire_calls += 1
        if self.refuse:
            msg = "broker slot pool exhausted"
            raise RuntimeError(msg)
        self._generation += 1
        return _RecordingLeaseGrant(slot_index=0, generation=self._generation)

    def release(self, slot_index: int, generation: int) -> None:
        """Record a release call."""
        _ = (slot_index, generation)
        self.release_calls += 1


class _AsyncRecordingBroker:
    """Async recording broker double whose `acquire` is a coroutine."""

    def __init__(self, *, refuse: bool = False) -> None:
        """Initialize the inner recording broker."""
        self._inner = _RecordingBroker(refuse=refuse)

    @property
    def acquire_calls(self) -> int:
        """Return the number of recorded acquire attempts."""
        return self._inner.acquire_calls

    @property
    def release_calls(self) -> int:
        """Return the number of recorded release calls."""
        return self._inner.release_calls

    async def acquire(self, pid: int, *, timeout_ms: int = 5000, bind_key_hash: int = 0) -> _RecordingLeaseGrant:
        """Record an async acquire attempt and either raise `RuntimeError` or return a grant."""
        await asyncio.sleep(0)
        return self._inner.acquire(pid, timeout_ms=timeout_ms, bind_key_hash=bind_key_hash)

    def release(self, slot_index: int, generation: int) -> None:
        """Record a release call."""
        self._inner.release(slot_index, generation)


def test_sync_mssql_driver_native_cursor_query_matrix() -> None:
    """Verify `SyncMSSQLDriver` query methods, `cursor()`, `lazy_begin`, and closed checks run on the native cursor."""
    server = _LoopbackTdsServer()
    driver = SyncMSSQLDriver(server.dsn)
    try:
        driver.connect()
        assert not hasattr(driver, "last_rewritten_sql")

        cur = driver.cursor()
        assert isinstance(cur, mssql_native.SyncMSSQLCursor)
        cur.close()

        lazy_cur = driver.cursor(lazy_begin=True)
        lazy_cur.execute("INSERT INTO #lazy_t VALUES (1)")
        assert driver.in_transaction is True
        driver.rollback()
        assert driver.in_transaction is False
        lazy_cur.close()

        assert driver.select("SELECT 1 AS a") == [{"a": 1}]
        assert driver.execute_with_description("SELECT 1 AS a, 2 AS b") == ([(1, 2)], ("a", "b"))
        assert driver.execute_tuples("SELECT 1 AS a, 2 AS b") == [(1, 2)]
        assert driver.execute("UPDATE two_rows SET v = 1") == 2
        assert driver.select("SELECT ? AS a", (5,)) == [{"a": 5}]
        assert driver.execute_many("INSERT INTO #lazy_t VALUES (?)", [(1,), (2,), (3,)]) == 3

        assert driver.select_one("SELECT 1 AS a") == {"a": 1}
        assert driver.select_one_or_none("SELECT_EMPTY") is None
        assert driver.select_value("SELECT 1 AS a", value_type=int) == 1
        assert driver.select_with_total("SELECT 1 AS a") == ([{"a": 1}], 1)

        with pytest.raises(NotFoundError):
            driver.select_value("SELECT_EMPTY")
        with pytest.raises(IntegrityError):
            driver.execute("RAISE_DUP")

        driver.close()
        with pytest.raises(OperationalError, match="closed"):
            driver.cursor()
        with pytest.raises(OperationalError, match="closed"):
            driver.execute("SELECT 1")
    finally:
        if not driver.closed:
            driver.close()
        server.close()


@pytest.mark.anyio
async def test_async_mssql_driver_native_cursor_query_matrix() -> None:
    """Verify `AsyncMSSQLDriver` query methods, `cursor()`, `lazy_begin`, and closed checks run on the native cursor."""
    server = _LoopbackTdsServer()
    driver = AsyncMSSQLDriver(server.dsn)
    try:
        await driver.connect()
        assert not hasattr(driver, "last_rewritten_sql")

        cur = driver.cursor()
        assert isinstance(cur, mssql_native.AsyncMSSQLCursor)
        await cur.close()

        lazy_cur = driver.cursor(lazy_begin=True)
        await lazy_cur.execute("INSERT INTO #lazy_t VALUES (1)")
        assert driver.in_transaction is True
        await driver.rollback()
        assert driver.in_transaction is False
        await lazy_cur.close()

        assert await driver.select("SELECT 1 AS a") == [{"a": 1}]
        assert await driver.execute_with_description("SELECT 1 AS a, 2 AS b") == ([(1, 2)], ("a", "b"))
        assert await driver.execute_tuples("SELECT 1 AS a, 2 AS b") == [(1, 2)]
        assert await driver.execute("UPDATE two_rows SET v = 1") == 2
        assert await driver.select("SELECT ? AS a", (5,)) == [{"a": 5}]
        assert await driver.execute_many("INSERT INTO #lazy_t VALUES (?)", [(1,), (2,), (3,)]) == 3

        assert await driver.select_one("SELECT 1 AS a") == {"a": 1}
        assert await driver.select_one_or_none("SELECT_EMPTY") is None
        assert await driver.select_value("SELECT 1 AS a", value_type=int) == 1
        assert await driver.select_with_total("SELECT 1 AS a") == ([{"a": 1}], 1)

        with pytest.raises(NotFoundError):
            await driver.select_value("SELECT_EMPTY")
        with pytest.raises(IntegrityError):
            await driver.execute("RAISE_DUP")

        await driver.close()
        with pytest.raises(OperationalError, match="closed"):
            driver.cursor()
        with pytest.raises(OperationalError, match="closed"):
            await driver.execute("SELECT 1")
    finally:
        if not driver.closed:
            await driver.close()
        server.close()


@pytest.mark.parametrize("scenario", ["no_broker", "one_lease", "refused", "dropped"])
def test_sync_mssql_driver_checkout_lease_lifecycle(scenario: str) -> None:
    """Verify `SyncMSSQLDriver` acquires one broker lease at connect and releases at close or GC drop."""
    server = _LoopbackTdsServer()
    try:
        if scenario == "no_broker":
            unused_broker = _RecordingBroker()
            driver = SyncMSSQLDriver(server.dsn, broker=None)
            driver.connect()
            for _ in range(10):
                assert driver.select("SELECT 1 AS a") == [{"a": 1}]
            driver.close()
            assert unused_broker.acquire_calls == 0
            assert unused_broker.release_calls == 0
        elif scenario == "one_lease":
            broker = _RecordingBroker()
            driver = SyncMSSQLDriver(server.dsn, broker=broker)
            driver.connect()
            assert broker.acquire_calls == 1
            assert broker.release_calls == 0
            for _ in range(10):
                assert driver.select("SELECT 1 AS a") == [{"a": 1}]
            assert broker.acquire_calls == 1
            assert broker.release_calls == 0
            driver.close()
            assert broker.acquire_calls == 1
            assert broker.release_calls == 1
        elif scenario == "refused":
            broker = _RecordingBroker(refuse=True)
            driver = SyncMSSQLDriver(server.dsn, broker=broker)
            driver.connect()
            assert broker.acquire_calls == 1
            assert driver.select("SELECT 1 AS a") == [{"a": 1}]
            driver.close()
            assert broker.release_calls == 0
        else:
            broker = _RecordingBroker()
            dropped_driver: Any = SyncMSSQLDriver(server.dsn, broker=broker)
            dropped_driver.connect()
            assert broker.acquire_calls == 1
            assert dropped_driver.select("SELECT 1 AS a") == [{"a": 1}]
            del dropped_driver
            gc.collect()
            assert broker.release_calls == 1
    finally:
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", ["no_broker", "one_lease", "refused", "dropped"])
@pytest.mark.parametrize("broker_cls", [_RecordingBroker, _AsyncRecordingBroker])
async def test_async_mssql_driver_checkout_lease_lifecycle(
    scenario: str,
    broker_cls: type[_RecordingBroker] | type[_AsyncRecordingBroker],
) -> None:
    """Verify `AsyncMSSQLDriver` acquires one broker lease at connect and releases at close or GC drop."""
    server = _LoopbackTdsServer()
    try:
        if scenario == "no_broker":
            unused_broker = broker_cls()
            driver = AsyncMSSQLDriver(server.dsn, broker=None)
            await driver.connect()
            for _ in range(10):
                assert await driver.select("SELECT 1 AS a") == [{"a": 1}]
            await driver.close()
            assert unused_broker.acquire_calls == 0
            assert unused_broker.release_calls == 0
        elif scenario == "one_lease":
            broker = broker_cls()
            driver = AsyncMSSQLDriver(server.dsn, broker=broker)
            await driver.connect()
            assert broker.acquire_calls == 1
            assert broker.release_calls == 0
            for _ in range(10):
                assert await driver.select("SELECT 1 AS a") == [{"a": 1}]
            assert broker.acquire_calls == 1
            assert broker.release_calls == 0
            await driver.close()
            assert broker.acquire_calls == 1
            assert broker.release_calls == 1
        elif scenario == "refused":
            broker = broker_cls(refuse=True)
            driver = AsyncMSSQLDriver(server.dsn, broker=broker)
            await driver.connect()
            assert broker.acquire_calls == 1
            assert await driver.select("SELECT 1 AS a") == [{"a": 1}]
            await driver.close()
            assert broker.release_calls == 0
        else:
            broker = broker_cls()
            dropped_driver: Any = AsyncMSSQLDriver(server.dsn, broker=broker)
            await dropped_driver.connect()
            assert broker.acquire_calls == 1
            assert await dropped_driver.select("SELECT 1 AS a") == [{"a": 1}]
            del dropped_driver
            gc.collect()
            assert broker.release_calls == 1
    finally:
        server.close()
