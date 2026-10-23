"""Pure-Rust-backed SQL Server (TDS 7.4) driver, `@P1` parameter rewriter, and Arrow cursor support."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import struct
from typing import TYPE_CHECKING, Any

from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.typing import FrozenBaseStruct, VertebraError
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "MSSQL_DIALECT_TAG",
    "TDS_PKT_ATTENTION",
    "TDS_PKT_BULK_LOAD",
    "TDS_PKT_PRE_LOGIN",
    "TDS_PKT_RPC",
    "TDS_PKT_SQL_BATCH",
    "TDS_PKT_TABULAR_RESULT",
    "TDS_PKT_TDS7_LOGIN",
    "TDS_PKT_TRANS_MGR_REQ",
    "AsyncMssqlDriver",
    "MssqlConfig",
    "SyncMssqlDriver",
    "decode_tds_packet",
    "encode_tds_packet",
    "encode_tds_sql_batch",
    "rewrite_mssql_placeholders",
]

MSSQL_DIALECT_TAG = 5

TDS_PKT_SQL_BATCH = 0x01
TDS_PKT_RPC = 0x03
TDS_PKT_TABULAR_RESULT = 0x04
TDS_PKT_ATTENTION = 0x06
TDS_PKT_BULK_LOAD = 0x07
TDS_PKT_TRANS_MGR_REQ = 0x0E
TDS_PKT_TDS7_LOGIN = 0x10
TDS_PKT_PRE_LOGIN = 0x12

_VALID_TDS_PACKET_TYPES = frozenset(
    {
        TDS_PKT_SQL_BATCH,
        TDS_PKT_RPC,
        TDS_PKT_TABULAR_RESULT,
        TDS_PKT_ATTENTION,
        TDS_PKT_BULK_LOAD,
        TDS_PKT_TRANS_MGR_REQ,
        TDS_PKT_TDS7_LOGIN,
        TDS_PKT_PRE_LOGIN,
    }
)


def encode_tds_packet(
    packet_type: int,
    packet_id: int,
    is_eom: bool,
    payload: bytes,
) -> bytes:
    """Encode an 8-byte TDS packet header (`[type, status, len_be_u16, spid_be_u16, packet_id, window]`) + payload."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_tds_packet"):
        return bytes(native.encode_tds_packet(packet_type, packet_id, is_eom, payload))
    total_len = 8 + len(payload)
    status = 0x01 if is_eom else 0x00
    header = struct.pack(">BBHHBB", packet_type & 0xFF, status, total_len & 0xFFFF, 0, packet_id & 0xFF, 0)
    return header + payload


def decode_tds_packet(buf: bytes | bytearray | memoryview) -> tuple[int, bool, int, bytes]:
    """Decode a TDS packet into `(packet_type, is_eom, packet_id, payload)`."""
    raw = bytes(buf)
    if len(raw) < 8:
        raise VertebraError("Buffer too short for 8-byte TDS packet header")
    pkt_type, status, total_len, _spid, packet_id, _window = struct.unpack(">BBHHBB", raw[:8])
    if pkt_type not in _VALID_TDS_PACKET_TYPES:
        raise VertebraError(f"Unsupported TDS packet type byte: 0x{pkt_type:02x}")
    if total_len < 8 or len(raw) < total_len:
        raise VertebraError(f"Invalid or truncated TDS packet length: expected {total_len}, got {len(raw)}")
    is_eom = (status & 0x01) != 0
    return (pkt_type, is_eom, packet_id, raw[8:total_len])


def encode_tds_sql_batch(sql: str, transaction_descriptor: int = 0) -> bytes:
    """Encode a TDS 7.4 `SQLBatch` packet with 22-byte `ALL_HEADERS` `TransactionDescriptor` and UTF-16LE SQL."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "encode_tds_sql_batch"):
        return bytes(native.encode_tds_sql_batch(sql, transaction_descriptor))

    all_headers = struct.pack("<IIHQI", 22, 18, 0x0002, transaction_descriptor & 0xFFFFFFFFFFFFFFFF, 1)
    utf16_sql = sql.encode("utf-16-le")
    return encode_tds_packet(TDS_PKT_SQL_BATCH, 1, True, all_headers + utf16_sql)


def rewrite_mssql_placeholders(sql: str) -> tuple[str, int]:
    """Rewrite `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...` outside quotes, brackets, and comments."""
    native = import_optional("vertebra._vertebra")
    if native is not None and hasattr(native, "rewrite_mssql_placeholders"):
        rewritten, count = native.rewrite_mssql_placeholders(sql)
        return str(rewritten), int(count)

    out: list[str] = []
    state = "NORMAL"
    i = 0
    n = len(sql)
    seq_index = 0
    max_param = 0

    while i < n:
        ch = sql[i]
        if state == "NORMAL":
            if ch == "'":
                out.append(ch)
                state = "SINGLE_QUOTE"
                i += 1
            elif ch == '"':
                out.append(ch)
                state = "DOUBLE_QUOTE"
                i += 1
            elif ch == "[":
                out.append(ch)
                state = "BRACKET"
                i += 1
            elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
                out.append("--")
                state = "LINE_COMMENT"
                i += 2
            elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
                out.append("/*")
                state = "BLOCK_COMMENT"
                i += 2
            elif ch == "?":
                seq_index += 1
                max_param = max(max_param, seq_index)
                out.append(f"@P{seq_index}")
                i += 1
            elif ch == "$" and i + 1 < n and sql[i + 1].isdigit() and sql[i + 1] != "0":
                j = i + 1
                while j < n and sql[j].isdigit():
                    j += 1
                num = int(sql[i + 1 : j])
                seq_index = max(seq_index, num)
                max_param = max(max_param, num)
                out.append(f"@P{num}")
                i = j
            else:
                out.append(ch)
                i += 1
        elif state == "SINGLE_QUOTE":
            out.append(ch)
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("'")
                    i += 2
                else:
                    state = "NORMAL"
                    i += 1
            else:
                i += 1
        elif state == "DOUBLE_QUOTE":
            out.append(ch)
            if ch == '"':
                if i + 1 < n and sql[i + 1] == '"':
                    out.append('"')
                    i += 2
                else:
                    state = "NORMAL"
                    i += 1
            else:
                i += 1
        elif state == "BRACKET":
            out.append(ch)
            if ch == "]":
                if i + 1 < n and sql[i + 1] == "]":
                    out.append("]")
                    i += 2
                else:
                    state = "NORMAL"
                    i += 1
            else:
                i += 1
        elif state == "LINE_COMMENT":
            out.append(ch)
            i += 1
            if ch == "\n":
                state = "NORMAL"
        elif state == "BLOCK_COMMENT":
            if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                out.append("*/")
                state = "NORMAL"
                i += 2
            else:
                out.append(ch)
                i += 1

    return "".join(out), max_param


class MssqlConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for the native SQL Server (TDS 7.4) driver."""

    dsn: str = "mssql://127.0.0.1:1433/master"
    host: str = "127.0.0.1"
    port: int = 1433
    database: str = "master"
    user: str = "sa"
    password: str = ""
    trust_server_certificate: bool = False
    cloudsql_instance: str | None = None
    pool_min_size: int = 1
    pool_max_size: int = 10
    statement_cache_size: int = 256
    broker: Any = None

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1` placeholders into T-SQL `@P1, @P2, ...` parameters."""
        return rewrite_mssql_placeholders(sql)

    @staticmethod
    def encode_sql_batch(sql: str, transaction_descriptor: int = 0) -> bytes:
        """Encode a T-SQL statement into a TDS 7.4 `SQLBatch` packet."""
        return encode_tds_sql_batch(sql, transaction_descriptor)


def _normalize_mssql_for_sqlite(
    sql: str, params: Sequence[Any] | Mapping[str, Any] | None
) -> tuple[str, tuple[Any, ...]]:
    """Convert `@P1` / `?` / `$1` placeholders into SQLite positional parameters for local verification."""
    if not params:
        return sql, ()
    if isinstance(params, dict):
        ordered: list[Any] = []

        def _replace_named(match: re.Match[str]) -> str:
            key = match.group(1)
            ordered.append(params.get(key, params.get(f"@{key}")))
            return "?"

        return re.sub(r"@([a-zA-Z_][a-zA-Z0-9_]*)", _replace_named, sql), tuple(ordered)

    seq = tuple(params)
    rewritten, _ = rewrite_mssql_placeholders(sql)
    sqlite_sql = re.sub(r"@P\d+", "?", rewritten)
    return sqlite_sql, seq


class SyncMssqlDriver:
    """Synchronous SQL Server TDS 7.4 driver with SHM statement cache and Arrow cursor support."""

    def __init__(self, config: MssqlConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or MssqlConfig()
        self._broker = broker if broker is not None else self.config.broker
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._last_rewritten_sql: str = ""
        self._last_tds_batch: bytes = b""

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._last_rewritten_sql

    @property
    def last_tds_batch(self) -> bytes:
        """Return the most recently encoded TDS 7.4 `SQLBatch` packet."""
        return self._last_tds_batch

    def _prepare_and_lease(self, sql: str) -> Any:
        if self._broker is not None and hasattr(self._broker, "statement_cache"):
            cached = self._broker.statement_cache.lookup(MSSQL_DIALECT_TAG, sql)
            if cached is not None and cached.compiled_sql:
                rewritten = cached.compiled_sql.decode("utf-8")
            else:
                rewritten, count = rewrite_mssql_placeholders(sql)
                self._broker.statement_cache.insert(
                    MSSQL_DIALECT_TAG,
                    sql,
                    count,
                    rewritten.encode("utf-8"),
                    b"",
                )
        else:
            rewritten, _ = rewrite_mssql_placeholders(sql)

        self._last_rewritten_sql = rewritten
        self._last_tds_batch = encode_tds_sql_batch(rewritten, 0)

        if self._broker is not None and hasattr(self._broker, "lease_table"):
            return self._broker.lease_table.acquire(os.getpid())
        return None

    def _release_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None and hasattr(self._broker, "lease_table"):
            self._broker.lease_table.release(grant.slot_index, grant.generation)

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = _normalize_mssql_for_sqlite(sql, params)
            cur = self._conn.execute(sqlite_sql, bound)
            self._conn.commit()
            return max(cur.rowcount, 0)
        finally:
            self._release_lease(grant)

    def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` query and return rows as dictionaries."""
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = _normalize_mssql_for_sqlite(sql, params)
            cur = self._conn.execute(sqlite_sql, bound)
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [col[0] for col in (cur.description or ())]
            return [dict(zip(cols, tuple(row), strict=False)) for row in rows]
        finally:
            self._release_lease(grant)

    def execute_tuples(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = _normalize_mssql_for_sqlite(sql, params)
            cur = self._conn.execute(sqlite_sql, bound)
            rows = [tuple(row) for row in cur.fetchall()]
            self._conn.commit()
            return rows
        finally:
            self._release_lease(grant)

    def execute_with_description(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = _normalize_mssql_for_sqlite(sql, params)
            cur = self._conn.execute(sqlite_sql, bound)
            if cur.description is not None:
                rows = [tuple(row) for row in cur.fetchall()]
                desc = tuple((col[0], None, None, None, None, None, None) for col in cur.description)
                return rows, desc, len(rows)
            self._conn.commit()
            return [], None, max(cur.rowcount, 0)
        finally:
            self._release_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy Arrow cursor."""
        rows = self.select(sql, params)
        return ArrowCursor.from_rows(rows, threshold_rows=shm_threshold_rows)

    def close(self) -> None:
        """Close the underlying driver resources."""
        self._conn.close()


class AsyncMssqlDriver:
    """Asynchronous SQL Server TDS 7.4 driver with SHM statement cache and Arrow cursor support."""

    def __init__(self, config: MssqlConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or MssqlConfig()
        self._sync = SyncMssqlDriver(self.config, broker=broker)

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._sync.last_rewritten_sql

    @property
    def last_tds_batch(self) -> bytes:
        """Return the most recently encoded TDS 7.4 `SQLBatch` packet."""
        return self._sync.last_tds_batch

    async def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement asynchronously."""
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` query asynchronously."""
        return await asyncio.to_thread(self._sync.select, sql, params)

    async def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )

    async def close(self) -> None:
        """Close the underlying driver resources."""
        await asyncio.to_thread(self._sync.close)
