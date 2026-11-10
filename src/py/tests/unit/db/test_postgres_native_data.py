"""Unit tests for native PostgreSQL Arrow results, COPY handles, and streaming Arrow import."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import os
import select
import socket
import struct
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

import vertebra._postgres as native_pg
import vertebra._vertebra as native_vertebra
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db import arrow as arrow_module
from vertebra.db.adapters.postgres.arrow import load_arrow_async, load_arrow_sync
from vertebra.db.adapters.postgres.core import PGCOPY_BINARY_SIGNATURE, PostgreSQLCore
from vertebra.db.adapters.postgres.types import PostgresArrowResult
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import DataError, InterfaceError, OperationalError
from vertebra.typing import MissingDependencyError, reset_dependency_cache

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence


def _serialize_ipc_rows(rows: list[tuple[int, str]]) -> bytes:
    """Serialize `(id, label)` rows into an Arrow IPC stream with explicit schema."""
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    ids = [r[0] for r in rows]
    labels = [r[1] for r in rows]
    table = pa.Table.from_arrays([pa.array(ids, type=pa.int64()), pa.array(labels, type=pa.string())], schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        if rows:
            writer.write_table(table)
    return bytes(sink.getvalue())


@pytest.mark.parametrize(
    ("rows", "columns"),
    [
        ([], ("id", "label")),
        ([(1, "a"), (2, "bc"), (3, "def")], ("id", "id")),
    ],
)
def test_arrow_count_postgres_arrow_result_inline_and_frozen(
    rows: list[tuple[int, str]],
    columns: tuple[str, ...],
) -> None:
    """Verify frozen PostgresArrowResult preserves inline bytes, duplicate columns, and row_count."""
    ipc_bytes = _serialize_ipc_rows(rows)
    res = PostgresArrowResult(
        buffer=ipc_bytes,
        row_count=len(rows),
        columns=columns,
        used_shm=False,
    )
    assert res.buffer == ipc_bytes
    assert res.row_count == len(rows)
    assert res.columns == columns
    assert res.used_shm is False
    mutable: Any = res
    with pytest.raises(AttributeError):
        mutable.row_count = 99


def test_arrow_count_postgres_arrow_result_accepts_native_shm_cursor() -> None:
    """Verify PostgresArrowResult accepts a native SHM ArrowCursor when used_shm is True."""
    ipc_bytes = _serialize_ipc_rows([(10, "x"), (20, "yz")])
    native_cur = native_vertebra.ArrowCursor.write_ipc_to_shm(
        ArrowCursor._next_shm_segment_name(),
        ipc_bytes,
        2,
    )
    try:
        res = PostgresArrowResult(
            buffer=native_cur,
            row_count=2,
            columns=("id", "label"),
            used_shm=True,
        )
        assert res.used_shm is True
        assert res.row_count == 2
        assert res.columns == ("id", "label")
        assert res.buffer is native_cur
    finally:
        native_cur.release()


@pytest.mark.parametrize(
    ("kwargs", "exc_type"),
    [
        ({"buffer": b"", "row_count": True, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": False, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": "2", "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": -1, "columns": ("a",), "used_shm": False}, ValueError),
        ({"buffer": b"", "row_count": 0, "columns": ["a"], "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": 0, "columns": (1,), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": 0, "columns": ("a",), "used_shm": 1}, TypeError),
        ({"buffer": bytearray(b"x"), "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": memoryview(b"x"), "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": None, "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"ipc", "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
        ({"buffer": None, "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
        ({"buffer": object(), "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
    ],
)
def test_arrow_count_postgres_arrow_result_rejects_invalid_fields(
    kwargs: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """Verify PostgresArrowResult rejects bool/negative row_count, bad columns, and buffer/used_shm mismatches."""
    with pytest.raises(exc_type):
        PostgresArrowResult(**kwargs)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
    ],
)
def test_arrow_count_inline_ipc_returns_exact_count_without_decoding_rows(
    rows: list[tuple[int, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify inline ArrowCursor(row_count=N) reports len/num_rows/row_count without decoding IPC rows."""
    ipc_bytes = _serialize_ipc_rows(rows)
    expected = len(rows)
    cursor = ArrowCursor(
        ipc_bytes,
        columns=("id", "label"),
        used_shm=False,
        row_count=expected,
    )
    assert cursor._ipc_table_cache is None
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    reset_dependency_cache("pyarrow")
    try:
        assert len(cursor) == expected
        assert cursor.num_rows == expected
        assert cursor.row_count == expected
        assert cursor._ipc_table_cache is None
    finally:
        monkeypatch.undo()
        reset_dependency_cache("pyarrow")

    tbl = cursor.to_arrow_table()
    assert tbl.num_rows == expected
    assert tbl.column_names == ["id", "label"]


@pytest.mark.parametrize(
    ("kwargs", "exc_type"),
    [
        ({"row_count": -1}, ValueError),
        ({"row_count": True}, TypeError),
        ({"row_count": False}, TypeError),
        ({"row_count": "3"}, TypeError),
        ({"rows": [(1,)], "row_count": 2}, ValueError),
        ({"rows": [(1,)], "row_count": 0}, ValueError),
    ],
)
def test_arrow_count_rejects_negative_bool_and_conflicting_row_metadata(
    kwargs: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """Verify ArrowCursor rejects negative, bool, non-int, and conflicting explicit row counts."""
    with pytest.raises(exc_type):
        ArrowCursor(b"", **kwargs)


@pytest.mark.parametrize("bad_count", [0, 4, 6])
def test_arrow_count_rejects_conflicting_native_shm_cursor_count(bad_count: int) -> None:
    """Verify ArrowCursor rejects explicit row_count values (including 0) that disagree with native SHM row_count."""
    ipc_bytes = _serialize_ipc_rows([(i, f"v{i}") for i in range(5)])
    native_cur = native_vertebra.ArrowCursor.write_ipc_to_shm(
        ArrowCursor._next_shm_segment_name(),
        ipc_bytes,
        5,
    )
    try:
        with pytest.raises(ValueError, match="row_count"):
            ArrowCursor(native_cur, used_shm=True, row_count=bad_count)
        matching = ArrowCursor(native_cur, used_shm=True, row_count=5)
        assert len(matching) == 5
        assert matching.num_rows == 5
        assert matching.row_count == 5
        assert matching._ipc_table_cache is None
    finally:
        native_cur.release()


def test_arrow_count_keeps_arrow_module_engine_agnostic() -> None:
    """Verify vertebra.db.arrow imports zero database adapter or PostgreSQL modules."""
    source = Path(arrow_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ] + [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    assert not any("postgres" in mod or "adapters" in mod for mod in imported)


_CANCEL_REQUEST_CODE = 80877102
_NAME_OID = 19
_INT2_OID = 21
_INT4_OID = 23
_TEXT_OID = 25
_OID_OID = 26
_PGCOPY_HEADER = PGCOPY_BINARY_SIGNATURE + struct.pack("!II", 0, 0)
_PGCOPY_TRAILER = b"\xff\xff"


def _pg_frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL v3 wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _pg_auth_ready(pid: int, secret: int) -> bytes:
    """Build AuthenticationOk, ParameterStatus, BackendKeyData, and ReadyForQuery('I')."""
    return b"".join(
        (
            _pg_frame(b"R", struct.pack("!i", 0)),
            _pg_frame(b"S", b"application_name\x00vertebra\x00"),
            _pg_frame(b"S", b"client_encoding\x00UTF8\x00"),
            _pg_frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _pg_frame(b"S", b"TimeZone\x00UTC\x00"),
            _pg_frame(b"K", struct.pack("!ii", pid, secret)),
            _pg_frame(b"Z", b"I"),
        )
    )


def _pg_error(sqlstate: str, msg: str) -> bytes:
    """Encode a PostgreSQL ErrorResponse ('E') frame."""
    body = b"".join(
        (
            b"SERROR\x00VERROR\x00C",
            sqlstate.encode("ascii"),
            b"\x00M",
            msg.encode("utf-8"),
            b"\x00\x00",
        )
    )
    return _pg_frame(b"E", body)


def _pg_row_desc_with_formats(cols: Sequence[tuple[str, int]], fmts: Sequence[int]) -> bytes:
    """Encode a RowDescription ('T') frame with per-column format codes."""
    parts = [struct.pack("!h", len(cols))]
    for (name, oid), fmt in zip(cols, fmts, strict=True):
        parts.append(name.encode("utf-8") + b"\x00" + struct.pack("!ihIhih", 0, 0, oid, -1, -1, fmt))
    return _pg_frame(b"T", b"".join(parts))


def _encode_wire_cell(val: str | None, oid: int, fmt: int) -> bytes:
    """Encode one text or binary PostgreSQL DataRow cell."""
    if val is None:
        return struct.pack("!i", -1)
    if fmt == 1:
        if oid == _OID_OID:
            raw = struct.pack("!I", int(val))
        elif oid == _INT4_OID:
            raw = struct.pack("!i", int(val))
        elif oid == _INT2_OID:
            raw = struct.pack("!h", int(val))
        else:
            raw = val.encode("utf-8")
    else:
        raw = val.encode("utf-8")
    return struct.pack("!i", len(raw)) + raw


def _pg_data_row_formatted(
    values: Sequence[str | None],
    cols: Sequence[tuple[str, int]],
    fmts: Sequence[int],
) -> bytes:
    """Encode a DataRow ('D') frame matching per-column text/binary format codes."""
    parts = [struct.pack("!h", len(values))]
    for val, (_, oid), fmt in zip(values, cols, fmts, strict=True):
        parts.append(_encode_wire_cell(val, oid, fmt))
    return _pg_frame(b"D", b"".join(parts))


def _pg_copy_in_binary(ncols: int) -> bytes:
    """Encode a binary CopyInResponse ('G') frame for ncols columns."""
    return _pg_frame(b"G", b"\x01" + struct.pack("!h", ncols) + (struct.pack("!h", 1) * ncols))


def _pg_copy_out_binary(ncols: int) -> bytes:
    """Encode a binary CopyOutResponse ('H') frame for ncols columns."""
    return _pg_frame(b"H", b"\x01" + struct.pack("!h", ncols) + (struct.pack("!h", 1) * ncols))


def _encode_binary_tuple(fields: Sequence[bytes | None]) -> bytes:
    """Encode a single PGCOPY binary row tuple without header or trailer."""
    parts = [struct.pack("!h", len(fields))]
    for cell in fields:
        if cell is None:
            parts.append(struct.pack("!i", -1))
        else:
            parts.append(struct.pack("!i", len(cell)) + cell)
    return b"".join(parts)


def _read_exact(conn: socket.socket, size: int) -> bytes:
    """Read exactly size bytes from conn or raise EOFError."""
    buf = bytearray()
    while len(buf) < size:
        chunk = conn.recv(size - len(buf))
        if not chunk:
            raise EOFError("unexpected socket EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_initial_packet(conn: socket.socket) -> tuple[str, tuple[int, int] | None]:
    """Read a StartupMessage or CancelRequest packet."""
    raw_len = _read_exact(conn, 4)
    (length,) = struct.unpack("!i", raw_len)
    payload = _read_exact(conn, length - 4)
    if length == 16:
        code, pid, secret = struct.unpack("!iii", payload)
        if code == _CANCEL_REQUEST_CODE:
            return "cancel", (pid, secret)
    return "startup", None


def _read_frame(conn: socket.socket) -> tuple[bytes, bytes]:
    """Read a single frontend frame from conn."""
    hdr = _read_exact(conn, 5)
    tag = hdr[:1]
    (length,) = struct.unpack("!i", hdr[1:5])
    return tag, _read_exact(conn, length - 4)


def _read_extended_batch(first: tuple[bytes, bytes], conn: socket.socket) -> list[tuple[bytes, bytes]]:
    """Read remaining frames of an extended query batch through Sync ('S')."""
    out = [first]
    if first[0] == b"S":
        return out
    while True:
        item = _read_frame(conn)
        out.append(item)
        if item[0] == b"S":
            return out


def _parse_frame_stmt_and_sql(body: bytes) -> tuple[str, str]:
    """Extract (statement_name, sql) from a Parse ('P') payload."""
    first_nul = body.index(b"\x00")
    stmt_name = body[:first_nul].decode("utf-8")
    rest = body[first_nul + 1 :]
    second_nul = rest.index(b"\x00")
    sql = rest[:second_nul].decode("utf-8")
    return stmt_name, sql


def _bind_frame_stmt_params_and_formats(body: bytes) -> tuple[str, list[str], list[int]]:
    """Extract (statement_name, text_params, result_format_codes) from a Bind ('B') payload."""
    portal_nul = body.index(b"\x00")
    rest = body[portal_nul + 1 :]
    stmt_nul = rest.index(b"\x00")
    stmt_name = rest[:stmt_nul].decode("utf-8")
    cursor = memoryview(rest)[stmt_nul + 1 :]
    (fmt_count,) = struct.unpack("!h", cursor[:2])
    cursor = cursor[2 + fmt_count * 2 :]
    (val_count,) = struct.unpack("!h", cursor[:2])
    cursor = cursor[2:]
    params: list[str] = []
    for _ in range(val_count):
        (plen,) = struct.unpack("!i", cursor[:4])
        cursor = cursor[4:]
        if plen >= 0:
            params.append(bytes(cursor[:plen]).decode("utf-8"))
            cursor = cursor[plen:]
    res_fmts: list[int] = []
    if len(cursor) >= 2:
        (res_count,) = struct.unpack("!h", cursor[:2])
        cursor = cursor[2:]
        for idx in range(res_count):
            (rfmt,) = struct.unpack("!h", cursor[idx * 2 : (idx + 1) * 2])
            res_fmts.append(rfmt)
    return stmt_name, params, res_fmts


def _resolve_portal_formats(ncols: int, requested: Sequence[int]) -> list[int]:
    """Expand Bind result format codes for ncols columns per PostgreSQL Protocol 3.0 rules."""
    if not requested:
        return [0] * ncols
    if len(requested) == 1:
        return [requested[0]] * ncols
    return list(requested[:ncols])


def _reply_extended_query(
    grp: Sequence[tuple[bytes, bytes]],
    param_oids: Sequence[int],
    cols: Sequence[tuple[str, int]],
    rows: Sequence[Sequence[str | None]],
    command_tag: str,
    tx_status: bytes,
    result_fmts: Sequence[int],
) -> bytes:
    """Build exact extended-protocol reply frames matching cached or uncached batches."""
    stmt_desc = _pg_row_desc_with_formats(cols, [0] * len(cols))
    portal_fmts = _resolve_portal_formats(len(cols), result_fmts)
    portal_desc = _pg_row_desc_with_formats(cols, portal_fmts)
    pdesc = _pg_frame(
        b"t",
        struct.pack("!h", len(param_oids)) + b"".join(struct.pack("!I", oid) for oid in param_oids),
    )
    parts: list[bytes] = []
    for ftag, fbody in grp:
        if ftag == b"P":
            parts.append(_pg_frame(b"1", b""))
        elif ftag == b"D":
            if fbody[:1] == b"S":
                parts.append(pdesc)
                parts.append(stmt_desc)
            else:
                parts.append(portal_desc)
        elif ftag == b"B":
            parts.append(_pg_frame(b"2", b""))
        elif ftag == b"E":
            parts.extend(_pg_data_row_formatted(r, cols, portal_fmts) for r in rows)
            parts.append(_pg_frame(b"C", command_tag.encode("ascii") + b"\x00"))
        elif ftag == b"C":
            parts.append(_pg_frame(b"3", b""))
        elif ftag == b"S":
            parts.append(_pg_frame(b"Z", tx_status))
    return b"".join(parts)


def _count_quoted_copy_cols(sql: str) -> int:
    """Count target columns inside `COPY ... ("c1", "c2") FROM STDIN`."""
    from_idx = sql.upper().rfind(" FROM STDIN")
    if from_idx == -1:
        return 1
    prefix = sql[:from_idx]
    close_paren = prefix.rfind(")")
    open_paren = prefix.rfind("(", 0, close_paren + 1)
    if open_paren == -1 or close_paren == -1:
        return 1
    inner = prefix[open_paren + 1 : close_paren]
    return inner.count(",") + 1


@dataclass
class _CopyPeerState:
    """Thread-safe record and hooks for scripted PostgreSQL COPY wire peers."""

    catalog_rows: list[tuple[str, str, str, int, int, int]] = field(
        default_factory=lambda: [
            ("public", "41001", "id", _INT4_OID, -1, 1),
            ("public", "41001", "label", _TEXT_OID, -1, 2),
        ]
    )
    copy_out_rows: list[tuple[bytes | None, ...]] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    catalog_binds: list[tuple[str, ...]] = field(default_factory=list)
    completed_copies: list[tuple[str, bytes, bool]] = field(default_factory=list)
    aborted_copies: list[tuple[str, bytes, str]] = field(default_factory=list)
    cancels: list[tuple[int, int]] = field(default_factory=list)
    startup_connections: int = 0
    post_ready_frames: int = 0
    first_tuple_on_wire: threading.Event = field(default_factory=threading.Event)
    cancel_received: threading.Event = field(default_factory=threading.Event)
    preflight_entered: threading.Event = field(default_factory=threading.Event)
    copy_in_entered: threading.Event = field(default_factory=threading.Event)
    copy_out_row1_sent: threading.Event = field(default_factory=threading.Event)
    copy_in_ready: threading.Event = field(default_factory=threading.Event)
    finish_entered: threading.Event = field(default_factory=threading.Event)
    release_finish: threading.Event = field(default_factory=threading.Event)
    stall_first_preflight: bool = False
    stall_first_copy_in: bool = False
    stall_copy_out_after_row1: bool = False
    hold_finish_until_released: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


def _serve_copy_connection(
    conn: socket.socket,
    pid: int,
    secret: int,
    state: _CopyPeerState,
) -> None:
    """Serve one startup or CancelRequest connection against state."""
    stmt_sql_map: dict[str, str] = {}
    in_tx = False
    tx_failed = False
    with conn:
        conn.settimeout(5.0)
        try:
            kind, cancel_key = _read_initial_packet(conn)
        except (EOFError, OSError):
            return
        if kind == "cancel" and cancel_key is not None:
            with state.lock:
                state.cancels.append(cancel_key)
                state.events.append(f"CANCEL:{cancel_key[0]}")
            state.cancel_received.set()
            return

        with state.lock:
            state.startup_connections += 1
        conn.sendall(_pg_auth_ready(pid, secret))

        while True:
            try:
                tag, body = _read_frame(conn)
            except (EOFError, OSError):
                return
            with state.lock:
                state.post_ready_frames += 1

            if tag == b"X":
                with state.lock:
                    state.events.append("TERMINATE")
                return

            if tag == b"Q":
                sql = body.rstrip(b"\x00").decode("utf-8")
                with state.lock:
                    state.events.append(f"Q:{sql}")
                if sql == "BEGIN":
                    in_tx = True
                    tx_failed = False
                    conn.sendall(_pg_frame(b"C", b"BEGIN\x00") + _pg_frame(b"Z", b"T"))
                    continue
                if sql in ("COMMIT", "ROLLBACK", "DISCARD ALL"):
                    in_tx = False
                    tx_failed = False
                    conn.sendall(_pg_frame(b"C", sql.encode("ascii") + b"\x00") + _pg_frame(b"Z", b"I"))
                    continue
                if sql.startswith("COPY ") and "FROM STDIN" in sql.upper():
                    was_ambient = in_tx
                    ncols = _count_quoted_copy_cols(sql)
                    should_stall_start = False
                    with state.lock:
                        if state.stall_first_copy_in:
                            state.stall_first_copy_in = False
                            should_stall_start = True
                    if should_stall_start:
                        state.copy_in_entered.set()
                        state.cancel_received.wait(timeout=3.0)
                    conn.sendall(_pg_copy_in_binary(ncols))
                    state.copy_in_ready.set()
                    copy_buf = bytearray()
                    while True:
                        ctag, cbody = _read_frame(conn)
                        if ctag == b"d":
                            copy_buf.extend(cbody)
                            if len(copy_buf) > len(_PGCOPY_HEADER):
                                state.first_tuple_on_wire.set()
                        elif ctag == b"c":
                            if state.hold_finish_until_released:
                                state.finish_entered.set()
                                while not state.release_finish.is_set() and not state.cancel_received.is_set():
                                    state.release_finish.wait(timeout=0.02)
                                if state.cancel_received.is_set() and not state.release_finish.is_set():
                                    if in_tx:
                                        tx_failed = True
                                    err_tx = b"E" if tx_failed else b"I"
                                    with contextlib.suppress(OSError):
                                        conn.sendall(
                                            _pg_error("57014", "canceling statement due to user request")
                                            + _pg_frame(b"Z", err_tx)
                                        )
                                    break
                            decoded = PostgreSQLCore.decode_pgcopy_rows(bytes(copy_buf))
                            with state.lock:
                                state.completed_copies.append((sql, bytes(copy_buf), was_ambient))
                                state.events.append(f"COPY_DONE:{len(decoded)}")
                            tx_byte = b"T" if in_tx else b"I"
                            conn.sendall(
                                _pg_frame(b"C", f"COPY {len(decoded)}\x00".encode("ascii")) + _pg_frame(b"Z", tx_byte)
                            )
                            break
                        elif ctag == b"f":
                            reason = cbody.rstrip(b"\x00").decode("utf-8", errors="replace")
                            with state.lock:
                                state.aborted_copies.append((sql, bytes(copy_buf), reason))
                                state.events.append(f"COPY_FAIL:{reason}")
                            if in_tx:
                                tx_failed = True
                            tx_byte = b"E" if tx_failed else (b"T" if in_tx else b"I")
                            conn.sendall(_pg_error("57014", reason) + _pg_frame(b"Z", tx_byte))
                            break
                    continue

                if sql.startswith("COPY ") and "TO STDOUT" in sql.upper():
                    rows_out = list(state.copy_out_rows)
                    ncols = len(rows_out[0]) if rows_out else 1
                    conn.sendall(_pg_copy_out_binary(ncols))
                    if state.stall_copy_out_after_row1 and rows_out:
                        first_chunk = _PGCOPY_HEADER + _encode_binary_tuple(rows_out[0])
                        conn.sendall(_pg_frame(b"d", first_chunk))
                        state.copy_out_row1_sent.set()
                        cancelled = state.cancel_received.wait(timeout=3.0)
                        tx_byte = b"E" if (cancelled and in_tx) else (b"T" if in_tx else b"I")
                        if cancelled:
                            with contextlib.suppress(OSError):
                                conn.sendall(
                                    _pg_error("57014", "canceling statement due to user request")
                                    + _pg_frame(b"Z", tx_byte)
                                )
                        else:
                            rest = b"".join(_encode_binary_tuple(r) for r in rows_out[1:]) + _PGCOPY_TRAILER
                            with contextlib.suppress(OSError):
                                conn.sendall(
                                    _pg_frame(b"d", rest)
                                    + _pg_frame(b"c", b"")
                                    + _pg_frame(b"C", f"COPY {len(rows_out)}\x00".encode("ascii"))
                                    + _pg_frame(b"Z", tx_byte)
                                )
                    else:
                        payload = _PGCOPY_HEADER + b"".join(_encode_binary_tuple(r) for r in rows_out) + _PGCOPY_TRAILER
                        tx_byte = b"T" if in_tx else b"I"
                        conn.sendall(
                            _pg_frame(b"d", payload)
                            + _pg_frame(b"c", b"")
                            + _pg_frame(b"C", f"COPY {len(rows_out)}\x00".encode("ascii"))
                            + _pg_frame(b"Z", tx_byte)
                        )
                    continue

                tx_byte = b"E" if tx_failed else (b"T" if in_tx else b"I")
                conn.sendall(_pg_frame(b"C", b"OK\x00") + _pg_frame(b"Z", tx_byte))
                continue

            grp = _read_extended_batch((tag, body), conn)
            sql_text = ""
            bind_params: list[str] = []
            bind_res_fmts: list[int] = []
            for ftag, fbody in grp:
                if ftag == b"P":
                    sname, parsed_sql = _parse_frame_stmt_and_sql(fbody)
                    stmt_sql_map[sname] = parsed_sql
                    sql_text = parsed_sql
                elif ftag == b"B":
                    sname, bind_params, bind_res_fmts = _bind_frame_stmt_params_and_formats(fbody)
                    if not sql_text:
                        sql_text = stmt_sql_map.get(sname, "")

            tx_byte = b"E" if tx_failed else (b"T" if in_tx else b"I")
            if "pg_class" in sql_text:
                with state.lock:
                    state.catalog_binds.append(tuple(bind_params))
                    state.events.append(f"CATALOG:{','.join(bind_params)}")
                    should_stall = state.stall_first_preflight
                    if should_stall:
                        state.stall_first_preflight = False
                if should_stall:
                    state.preflight_entered.set()
                    state.cancel_received.wait(timeout=3.0)
                    if in_tx:
                        tx_failed = True
                    err_tx = b"E" if tx_failed else b"I"
                    with contextlib.suppress(OSError):
                        conn.sendall(
                            _pg_error("57014", "canceling statement due to user request") + _pg_frame(b"Z", err_tx)
                        )
                    continue

                if len(bind_params) == 2:
                    want_schema, want_table = bind_params[0], bind_params[1]
                    matched = [
                        (nsp, oid_s, att, str(toid), str(tmod), str(attnum))
                        for (nsp, oid_s, att, toid, tmod, attnum) in state.catalog_rows
                        if nsp == want_schema and want_table in ("items", "odd.table")
                    ]
                    param_oids = [_TEXT_OID, _TEXT_OID]
                else:
                    matched = [
                        (nsp, oid_s, att, str(toid), str(tmod), str(attnum))
                        for (nsp, oid_s, att, toid, tmod, attnum) in state.catalog_rows
                    ]
                    param_oids = [_TEXT_OID]
                cat_cols = [
                    ("nspname", _NAME_OID),
                    ("oid", _OID_OID),
                    ("attname", _NAME_OID),
                    ("atttypid", _OID_OID),
                    ("atttypmod", _INT4_OID),
                    ("attnum", _INT2_OID),
                ]
                conn.sendall(
                    _reply_extended_query(
                        grp,
                        param_oids,
                        cat_cols,
                        matched,
                        f"SELECT {len(matched)}",
                        tx_byte,
                        bind_res_fmts,
                    )
                )
            else:
                with state.lock:
                    state.events.append(f"EXT:{sql_text or 'SELECT'}")
                conn.sendall(
                    _reply_extended_query(
                        grp,
                        [],
                        [("v", _INT4_OID)],
                        [("1",)],
                        "SELECT 1",
                        tx_byte,
                        bind_res_fmts,
                    )
                )


@contextlib.contextmanager
def _running_copy_peer(state: _CopyPeerState | None = None) -> Iterator[tuple[int, _CopyPeerState]]:
    """Run a multi-connection loopback PostgreSQL v3 peer returning (port, state)."""
    peer_state = state if state is not None else _CopyPeerState()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.1)
    port = srv.getsockname()[1]
    stop_evt = threading.Event()
    workers: list[threading.Thread] = []

    def _accept_loop() -> None:
        seq = 0
        while not stop_evt.is_set():
            try:
                conn, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            seq += 1
            t = threading.Thread(
                target=_serve_copy_connection,
                args=(conn, 7000 + seq, 90000 + seq, peer_state),
                daemon=True,
            )
            t.start()
            workers.append(t)

    accept_thread = threading.Thread(target=_accept_loop, daemon=True)
    accept_thread.start()
    try:
        yield port, peer_state
    finally:
        stop_evt.set()
        peer_state.cancel_received.set()
        peer_state.release_finish.set()
        with contextlib.suppress(OSError):
            srv.close()
        accept_thread.join(timeout=3.0)
        for worker in workers:
            worker.join(timeout=3.0)


async def _as_coro(awaitable: Awaitable[Any]) -> Any:
    """Wrap a native RustDbTaskAwaitable inside a Python coroutine for asyncio.create_task."""
    return await awaitable


class _CStreamProvider:
    """Minimal Arrow PyCapsule stream wrapper forwarding __arrow_c_stream__ and tracking closure."""

    def __init__(self, reader_factory: Callable[[], pa.RecordBatchReader]) -> None:
        self._reader_factory = reader_factory

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        reader = self._reader_factory()
        return reader.__arrow_c_stream__(requested_schema)


class _AsyncBatchStream:
    """Async RecordBatch stream recording aclose calls."""

    def __init__(self, schema: pa.Schema, batches: Sequence[pa.RecordBatch]) -> None:
        self.schema = schema
        self._batches = list(batches)
        self.closed = False

    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        async def _gen() -> AsyncIterator[pa.RecordBatch]:
            try:
                for batch in self._batches:
                    yield batch
            finally:
                self.closed = True

        return _gen()


def _three_arrow_batches() -> tuple[pa.Schema, list[pa.RecordBatch]]:
    """Build a 2-column schema and 3 RecordBatches totaling 6 rows including a NULL cell."""
    schema = pa.schema([("id", pa.int32()), ("label", pa.string())])
    b1 = pa.record_batch([pa.array([1, 2], type=pa.int32()), pa.array(["a", "bb"], type=pa.string())], schema=schema)
    b2 = pa.record_batch([pa.array([3, 4], type=pa.int32()), pa.array([None, "dd"], type=pa.string())], schema=schema)
    b3 = pa.record_batch([pa.array([5, 6], type=pa.int32()), pa.array(["eee", ""], type=pa.string())], schema=schema)
    return schema, [b1, b2, b3]


def _build_arrow_source(shape: str) -> Any:
    """Build one of the supported Arrow input sources over 6 rows."""
    schema, batches = _three_arrow_batches()
    if shape == "table":
        return pa.Table.from_batches(batches, schema=schema)
    if shape == "record_batch":
        combined = pa.Table.from_batches(batches, schema=schema).combine_chunks()
        return combined.to_batches()[0]
    if shape == "reader":
        return pa.RecordBatchReader.from_batches(schema, batches)
    if shape == "c_stream":
        return _CStreamProvider(lambda: pa.RecordBatchReader.from_batches(schema, batches))
    if shape == "arrow_cursor":
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
        return ArrowCursor(bytes(sink.getvalue()), columns=("id", "label"), used_shm=False, row_count=6)
    if shape == "async_batches":
        return _AsyncBatchStream(schema, batches)
    msg = f"unknown shape {shape!r}"
    raise AssertionError(msg)


@contextlib.contextmanager
def _forbid_row_materialization(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Intercept pyarrow C-extension and ArrowCursor row/dict conversion methods and fail if called."""

    def _boom(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("Python row/dict materialization must not run during Arrow COPY")

    def _profile_guard(frame: Any, event: str, arg: Any) -> None:
        if event == "c_call" and getattr(arg, "__name__", None) in ("to_pylist", "to_pydict"):
            raise AssertionError(f"forbidden C-extension row materialization: {arg!r}")
        if event == "call" and frame.f_code.co_name in ("to_pylist", "to_pydict"):
            raise AssertionError(f"forbidden Python row materialization: {frame.f_code.co_name}")

    monkeypatch.setattr(ArrowCursor, "to_pylist", _boom)
    monkeypatch.setattr(ArrowCursor, "to_arrow_table", _boom)
    prev_profile = sys.getprofile()
    sys.setprofile(_profile_guard)
    try:
        yield
    finally:
        sys.setprofile(prev_profile)


def _expected_six_binary_tuples() -> list[tuple[bytes | None, ...]]:
    """Return the exact binary COPY tuple payloads expected from _three_arrow_batches()."""
    return [
        (struct.pack("!i", 1), b"a"),
        (struct.pack("!i", 2), b"bb"),
        (struct.pack("!i", 3), None),
        (struct.pack("!i", 4), b"dd"),
        (struct.pack("!i", 5), b"eee"),
        (struct.pack("!i", 6), b""),
    ]


@pytest.mark.parametrize("shape", ("table", "record_batch", "reader", "c_stream", "arrow_cursor"))
def test_load_arrow_sync_all_five_shapes_zero_row_materialization(
    shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify load_arrow_sync streams all 5 input shapes with 1 PGCOPY header and zero row materialization."""
    source = _build_arrow_source(shape)
    with _forbid_row_materialization(monkeypatch), _running_copy_peer() as (port, state):
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
            copied = load_arrow_sync(client, "items", source)
            assert copied == 6
        finally:
            client.close()

    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, was_ambient = state.completed_copies[0]
    assert copy_sql == 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    assert was_ambient is True
    assert wire_bytes.startswith(_PGCOPY_HEADER)
    assert wire_bytes.endswith(_PGCOPY_TRAILER)
    assert wire_bytes.count(PGCOPY_BINARY_SIGNATURE) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == _expected_six_binary_tuples()
    assert "Q:BEGIN" in state.events
    assert "Q:COMMIT" in state.events


@pytest.mark.parametrize("shape", ("table", "record_batch", "reader", "c_stream", "arrow_cursor", "async_batches"))
def test_load_arrow_async_all_shapes_zero_row_materialization(
    shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify load_arrow_async streams all Arrow shapes with 1 PGCOPY header and zero row materialization."""
    source = _build_arrow_source(shape)

    async def _run(port: int) -> int:
        client = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        try:
            return await load_arrow_async(client, "items", source)
        finally:
            await client.close()

    with _forbid_row_materialization(monkeypatch), _running_copy_peer() as (port, state):
        copied = asyncio.run(_run(port))

    assert copied == 6
    if isinstance(source, _AsyncBatchStream):
        assert source.closed is True
    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, _ = state.completed_copies[0]
    assert copy_sql == 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    assert wire_bytes.count(PGCOPY_BINARY_SIGNATURE) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == _expected_six_binary_tuples()
    assert "Q:BEGIN" in state.events
    assert "Q:COMMIT" in state.events


@pytest.mark.parametrize("shape", ("reader", "c_stream"))
def test_load_arrow_second_batch_streaming_barrier_and_reader_close(shape: str) -> None:
    """Verify batch 1 arrives on TCP wire before batch 2 is pulled and readers close on completion."""
    schema, batches = _three_arrow_batches()

    def _make_barrier_source(wire_evt: threading.Event, closed_evt: threading.Event) -> Any:
        def _gen() -> Iterator[pa.RecordBatch]:
            try:
                yield batches[0]
                assert wire_evt.wait(timeout=3.0), "batch 1 did not arrive over TCP before batch 2 pull"
                yield batches[1]
                yield batches[2]
            finally:
                closed_evt.set()

        if shape == "reader":
            return pa.RecordBatchReader.from_batches(schema, _gen())
        return _CStreamProvider(lambda: pa.RecordBatchReader.from_batches(schema, _gen()))

    sync_closed = threading.Event()
    with _running_copy_peer() as (port, sync_state):
        sync_src = _make_barrier_source(sync_state.first_tuple_on_wire, sync_closed)
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
            assert load_arrow_sync(client, "items", sync_src) == 6
        finally:
            client.close()
    assert sync_closed.is_set()
    assert PostgreSQLCore.decode_pgcopy_rows(sync_state.completed_copies[0][1]) == _expected_six_binary_tuples()

    async_closed = threading.Event()

    async def _run_async(port: int, astate: _CopyPeerState) -> int:
        async_src = _make_barrier_source(astate.first_tuple_on_wire, async_closed)
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
            return await load_arrow_async(aclient, "items", async_src)
        finally:
            await aclient.close()

    with _running_copy_peer() as (port, async_state):
        assert asyncio.run(_run_async(port, async_state)) == 6
    assert async_closed.is_set()
    assert PostgreSQLCore.decode_pgcopy_rows(async_state.completed_copies[0][1]) == _expected_six_binary_tuples()


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_empty_schema_columns_schema_name_and_ambient_transaction(is_async: bool) -> None:
    """Verify 0-row Table forwards columns + schema_name on dotted table and joins ambient transactions."""
    state = _CopyPeerState(
        catalog_rows=[
            ("analytics", "51001", "a", _INT4_OID, -1, 1),
            ("analytics", "51001", "b", _TEXT_OID, -1, 2),
            ("analytics", "51001", "ignored", _TEXT_OID, -1, 3),
        ]
    )
    empty_table = pa.table(
        {"b": pa.array([], type=pa.string()), "a": pa.array([], type=pa.int32())},
    )

    with _running_copy_peer(state) as (port, peer):
        if not is_async:
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
                client.begin()
                count = load_arrow_sync(
                    client,
                    "odd.table",
                    empty_table,
                    columns=["b", "a"],
                    schema_name="analytics",
                )
                assert count == 0
                assert peer.events.count("Q:COMMIT") == 0
                assert peer.events.count("Q:BEGIN") == 1
                client.commit()
            finally:
                client.close()
        else:

            async def _run_async() -> int:
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
                    await aclient.begin()
                    res = await load_arrow_async(
                        aclient,
                        "odd.table",
                        empty_table,
                        columns=["b", "a"],
                        schema_name="analytics",
                    )
                    assert peer.events.count("Q:COMMIT") == 0
                    assert peer.events.count("Q:BEGIN") == 1
                    await aclient.commit()
                    return res
                finally:
                    await aclient.close()

            assert asyncio.run(_run_async()) == 0

    assert state.catalog_binds == [("analytics", "odd.table")]
    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, _ = state.completed_copies[0]
    assert copy_sql == 'COPY "analytics"."odd.table" ("b", "a") FROM STDIN (FORMAT binary)'
    assert wire_bytes == _PGCOPY_HEADER + _PGCOPY_TRAILER
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == []
    assert state.events.count("Q:BEGIN") == 1
    assert state.events.count("Q:COMMIT") == 1


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_missing_pyarrow_fails_before_socket_traffic(
    is_async: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify blocked pyarrow raises MissingDependencyError before any PostgreSQL wire traffic."""
    schema, batches = _three_arrow_batches()
    tbl = pa.Table.from_batches(batches[:1], schema=schema)

    with _running_copy_peer() as (port, state):
        if not is_async:
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
                with state.lock:
                    baseline = state.post_ready_frames
                monkeypatch.setitem(sys.modules, "pyarrow", None)
                reset_dependency_cache("pyarrow")
                try:
                    with pytest.raises(MissingDependencyError):
                        load_arrow_sync(client, "items", tbl)
                finally:
                    monkeypatch.undo()
                    reset_dependency_cache("pyarrow")
                with state.lock:
                    assert state.post_ready_frames == baseline
            finally:
                client.close()
        else:

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
                    with state.lock:
                        baseline = state.post_ready_frames
                    monkeypatch.setitem(sys.modules, "pyarrow", None)
                    reset_dependency_cache("pyarrow")
                    try:
                        with pytest.raises(MissingDependencyError):
                            await load_arrow_async(aclient, "items", tbl)
                    finally:
                        monkeypatch.undo()
                        reset_dependency_cache("pyarrow")
                    with state.lock:
                        assert state.post_ready_frames == baseline
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert state.catalog_binds == []
    assert state.completed_copies == []


@pytest.mark.parametrize("is_async", (False, True))
def test_native_copy_arrow_ipc_rejects_malformed_oversize_and_accepts_buffers(is_async: bool) -> None:
    """Verify malformed and 17 MiB IPC raise DataError while memoryview/bytearray/pa.Buffer succeed."""
    schema, batches = _three_arrow_batches()
    sink1 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink1, schema) as w1:
        w1.write_batch(batches[0])
    ipc1 = bytes(sink1.getvalue())
    sink2 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink2, schema) as w2:
        w2.write_batch(batches[1])
    ipc2 = bytes(sink2.getvalue())

    oversize_buf = bytearray(17 * 1024 * 1024)
    oversize_view = memoryview(oversize_buf)

    with _running_copy_peer() as (port, state):
        if not is_async:
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
                with pytest.raises(DataError):
                    client.copy_from_arrow("items", b"not-arrow")
                with pytest.raises(DataError):
                    client.copy_from_arrow("items", oversize_view)
                with pytest.raises((BufferError, TypeError)):
                    client.copy_from_arrow("items", memoryview(bytearray(32))[::2])
                assert state.catalog_binds == []

                with (
                    client.copy_from_arrow("items", memoryview(ipc1)) as bad_malformed,
                    pytest.raises(DataError),
                ):
                    bad_malformed.write_arrow_ipc(b"not-arrow")

                with (
                    client.copy_from_arrow("items", bytearray(ipc1)) as bad_oversize,
                    pytest.raises(DataError),
                ):
                    bad_oversize.write_arrow_ipc(oversize_view)

                with client.copy_from_arrow("items", pa.py_buffer(ipc1)) as good:
                    assert isinstance(good, native_pg.SyncPostgresCopyIn)
                    good.write_arrow_ipc(memoryview(bytearray(ipc2)))
                    inserted = good.finish()
                    assert inserted == 4
            finally:
                client.close()
        else:

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
                    with pytest.raises(DataError):
                        await aclient.copy_from_arrow("items", b"not-arrow")
                    with pytest.raises(DataError):
                        await aclient.copy_from_arrow("items", oversize_view)
                    with pytest.raises((BufferError, TypeError)):
                        await aclient.copy_from_arrow("items", memoryview(bytearray(32))[::2])
                    assert state.catalog_binds == []

                    async with await aclient.copy_from_arrow("items", memoryview(ipc1)) as bad_malformed:
                        with pytest.raises(DataError):
                            await bad_malformed.write_arrow_ipc(b"not-arrow")

                    async with await aclient.copy_from_arrow("items", bytearray(ipc1)) as bad_oversize:
                        with pytest.raises(DataError):
                            await bad_oversize.write_arrow_ipc(oversize_view)

                    async with await aclient.copy_from_arrow("items", pa.py_buffer(ipc1)) as good:
                        assert isinstance(good, native_pg.AsyncPostgresCopyIn)
                        await good.write_arrow_ipc(memoryview(bytearray(ipc2)))
                        inserted = await good.finish()
                        assert inserted == 4
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert len(state.aborted_copies) == 2
    assert len(state.completed_copies) == 1
    assert len(PostgreSQLCore.decode_pgcopy_rows(state.completed_copies[0][1])) == 4


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_producer_exception_aborts_and_recovers_pool(is_async: bool) -> None:
    """Verify producer exception after batch 1 sends CopyFail + ROLLBACK and recovers max_size=1 pool."""
    schema, batches = _three_arrow_batches()
    closed_evt = threading.Event()

    def _failing_gen() -> Iterator[pa.RecordBatch]:
        try:
            yield batches[0]
            msg = "boom"
            raise RuntimeError(msg)
        finally:
            closed_evt.set()

    reader = pa.RecordBatchReader.from_batches(schema, _failing_gen())

    with _running_copy_peer() as (port, state):
        if not is_async:
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
            try:
                c1 = pool.acquire()
                try:
                    with pytest.raises((RuntimeError, OSError), match="boom"):
                        load_arrow_sync(c1, "items", reader)
                finally:
                    c1.close()

                c2 = pool.acquire()
                try:
                    assert c2.query("SELECT 1").rows == ((1,),)
                finally:
                    c2.close()
            finally:
                pool.close()
        else:

            async def _failing_async_gen() -> AsyncIterator[pa.RecordBatch]:
                try:
                    yield batches[0]
                    msg = "boom"
                    raise RuntimeError(msg)
                finally:
                    closed_evt.set()

            class _FailingAsyncSource:
                def __init__(self) -> None:
                    self.schema = schema

                def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
                    return _failing_async_gen()

            async def _run_async() -> None:
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
                try:
                    ac1 = await apool.acquire()
                    try:
                        with pytest.raises(RuntimeError, match="boom"):
                            await load_arrow_async(ac1, "items", _FailingAsyncSource())
                    finally:
                        await ac1.close()

                    ac2 = await apool.acquire()
                    try:
                        res = await ac2.query("SELECT 1")
                        assert res.rows == ((1,),)
                    finally:
                        await ac2.close()
                finally:
                    await apool.close()

            asyncio.run(_run_async())

    assert closed_evt.is_set()
    assert state.completed_copies == []
    assert len(state.aborted_copies) == 1
    assert "Q:ROLLBACK" in state.events
    assert "Q:COMMIT" not in state.events


def test_native_copy_to_binary_rows_early_break_and_cancel_recover_pool() -> None:
    """Verify binary copy_to yields tuple[bytes | None, ...] and early break/cancel recovers max_size=1 pool."""
    expected_rows: list[tuple[bytes | None, ...]] = [
        (struct.pack("!i", 10), b"alpha"),
        (struct.pack("!i", 20), None),
        (struct.pack("!i", 30), b""),
    ]

    full_state = _CopyPeerState(copy_out_rows=expected_rows)
    with _running_copy_peer(full_state) as (port, _):
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
            with client.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                assert isinstance(copy_out, native_pg.SyncPostgresCopyOut)
                rows = list(copy_out)
            copy_out.close()
            assert rows == expected_rows
            assert client.query("SELECT 1").rows == ((1,),)
        finally:
            client.close()

    break_state = _CopyPeerState(copy_out_rows=expected_rows, stall_copy_out_after_row1=True)
    with _running_copy_peer(break_state) as (port, peer):
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
        try:
            c1 = pool.acquire()
            try:
                with c1.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                    first = next(copy_out)
                    assert first == expected_rows[0]
            finally:
                c1.close()
            assert len(peer.cancels) == 1

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            pool.close()

    async_cancel_state = _CopyPeerState(copy_out_rows=expected_rows, stall_copy_out_after_row1=True)

    async def _exercise_async_copy_to(port: int) -> None:
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
        try:
            ac1 = await apool.acquire()
            try:
                async with await ac1.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                    assert isinstance(copy_out, native_pg.AsyncPostgresCopyOut)
                    row1 = await anext(copy_out)
                    assert row1 == expected_rows[0]
                    row2_task = asyncio.create_task(_as_coro(anext(copy_out)))
                    await asyncio.sleep(0)
                    row2_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await row2_task
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(async_cancel_state) as (port, apeer):
        asyncio.run(_exercise_async_copy_to(port))
    assert len(apeer.cancels) >= 1


@pytest.mark.parametrize("stage", ("preflight", "start_copy_in"))
def test_native_copy_from_arrow_async_cancel_mid_preflight_and_mid_start_recovers_pool(stage: str) -> None:
    """Verify cancelling copy_from_arrow(..., schema_name='analytics') mid-preflight or mid-start recovers pool."""
    schema, batches = _three_arrow_batches()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batches[0])
    first_ipc = bytes(sink.getvalue())

    state = _CopyPeerState(
        catalog_rows=[
            ("analytics", "61001", "id", _INT4_OID, -1, 1),
            ("analytics", "61001", "label", _TEXT_OID, -1, 2),
        ],
        stall_first_preflight=(stage == "preflight"),
        stall_first_copy_in=(stage == "start_copy_in"),
    )
    trigger_evt = state.preflight_entered if stage == "preflight" else state.copy_in_entered

    async def _run_cancel(port: int) -> None:
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
        try:
            ac1 = await apool.acquire()
            try:
                task = asyncio.create_task(
                    _as_coro(ac1.copy_from_arrow("items", first_ipc, schema_name="analytics")),
                )
                entered = await asyncio.to_thread(trigger_evt.wait, 3.0)
                assert entered
                task.cancel()
                with pytest.raises((asyncio.CancelledError, OperationalError)):
                    await task
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(state) as (port, peer):
        asyncio.run(_run_cancel(port))

    assert peer.completed_copies == []
    assert len(peer.cancels) >= 1
    if stage == "preflight":
        assert not any(ev.startswith("Q:COPY ") for ev in peer.events)


@pytest.mark.parametrize("is_async", (False, True))
def test_native_copy_from_binary_write_rows_and_unhandled_exit_abort(is_async: bool) -> None:
    """Verify SyncPostgresCopyIn/AsyncPostgresCopyIn write(rows, oids), finish(), and abort on unhandled exit."""
    copy_sql = 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    rows: list[tuple[int, str | None]] = [(1, "one"), (2, None), (3, "three")]
    oids = [_INT4_OID, _TEXT_OID]

    with _running_copy_peer() as (port, state):
        if not is_async:
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
                with client.copy_from(copy_sql) as copy_in:
                    assert isinstance(copy_in, native_pg.SyncPostgresCopyIn)
                    copy_in.write(rows[:2], oids)
                    copy_in.write(rows[2:], oids)
                    assert copy_in.finish() == 3
                copy_in.close()

                with pytest.raises(RuntimeError, match="abort-sync"), client.copy_from(copy_sql) as aborted:
                    aborted.write([(99, "discard")], oids)
                    raise RuntimeError("abort-sync")

                assert client.query("SELECT 1").rows == ((1,),)
            finally:
                client.close()
        else:

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
                    async with await aclient.copy_from(copy_sql) as copy_in:
                        assert isinstance(copy_in, native_pg.AsyncPostgresCopyIn)
                        await copy_in.write(rows[:2], oids)
                        await copy_in.write(rows[2:], oids)
                        assert await copy_in.finish() == 3
                    await copy_in.aclose()

                    with pytest.raises(RuntimeError, match="abort-async"):
                        async with await aclient.copy_from(copy_sql) as aborted:
                            await aborted.write([(99, "discard")], oids)
                            raise RuntimeError("abort-async")

                    res = await aclient.query("SELECT 1")
                    assert res.rows == ((1,),)
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert len(state.completed_copies) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(state.completed_copies[0][1]) == [
        (struct.pack("!i", 1), b"one"),
        (struct.pack("!i", 2), None),
        (struct.pack("!i", 3), b"three"),
    ]
    assert len(state.aborted_copies) == 1
    assert "Q:ROLLBACK" in state.events


def test_native_copy_overlapping_and_post_close_raise_interface_error() -> None:
    """Verify overlapping async handle calls and post-close/finish calls raise InterfaceError immediately."""
    state = _CopyPeerState(
        copy_out_rows=[(struct.pack("!i", 1), b"a"), (struct.pack("!i", 2), b"b")],
        stall_copy_out_after_row1=True,
        hold_finish_until_released=True,
    )
    copy_in_sql = 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'

    async def _exercise(port: int) -> None:
        c_in = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c_in.connect()
        try:
            copy_in = await c_in.copy_from(copy_in_sql)
            await copy_in.write([(1, "ok")], [_INT4_OID, _TEXT_OID])
            finish_fut = asyncio.create_task(_as_coro(copy_in.finish()))
            assert await asyncio.to_thread(state.finish_entered.wait, 3.0)
            with pytest.raises(InterfaceError):
                await copy_in.write([(2, "overlap")], [_INT4_OID, _TEXT_OID])
            with pytest.raises(InterfaceError):
                await copy_in.write_arrow_ipc(b"overlap")
            with pytest.raises(InterfaceError):
                await copy_in.finish()
            state.release_finish.set()
            assert await finish_fut == 1

            await copy_in.aclose()
            with pytest.raises(InterfaceError):
                await copy_in.write([(3, "closed")], [_INT4_OID, _TEXT_OID])
            with pytest.raises(InterfaceError):
                await copy_in.write_arrow_ipc(b"closed")
            with pytest.raises(InterfaceError):
                await copy_in.finish()
        finally:
            await c_in.close()

        c_out = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c_out.connect()
        try:
            copy_out = await c_out.copy_to("COPY items TO STDOUT (FORMAT binary)")
            assert await anext(copy_out) == (struct.pack("!i", 1), b"a")
            next_task = asyncio.create_task(_as_coro(anext(copy_out)))
            await asyncio.sleep(0)
            with pytest.raises(InterfaceError):
                await anext(copy_out)
            next_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await next_task
            await copy_out.aclose()
            await copy_out.aclose()
            with pytest.raises(InterfaceError):
                await anext(copy_out)
        finally:
            await c_out.close()

    with _running_copy_peer(state) as (port, _):
        asyncio.run(_exercise(port))


@pytest.mark.parametrize("op_kind", ("finish", "write_arrow_ipc"))
def test_native_copy_async_cancel_finish_and_write_recovers_pool(op_kind: str) -> None:
    """Verify cancelling copy_in.finish() or write_arrow_ipc() recovers a max_size=1 pool without aclose."""
    schema, batches = _three_arrow_batches()
    sink1 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink1, schema) as w1:
        w1.write_batch(batches[0])
    ipc1 = bytes(sink1.getvalue())
    sink2 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink2, schema) as w2:
        w2.write_batch(batches[1])
    ipc2 = bytes(sink2.getvalue())

    state = _CopyPeerState(hold_finish_until_released=(op_kind == "finish"))

    async def _exercise(port: int) -> None:
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
        try:
            ac1 = await apool.acquire()
            try:
                copy_in = await ac1.copy_from_arrow("items", ipc1)
                if op_kind == "finish":
                    fut = asyncio.create_task(_as_coro(copy_in.finish()))
                    assert await asyncio.to_thread(state.finish_entered.wait, 3.0)
                    fut.cancel()
                    with pytest.raises((asyncio.CancelledError, OperationalError)):
                        await fut
                else:
                    fut = asyncio.create_task(_as_coro(copy_in.write_arrow_ipc(ipc2)))
                    await asyncio.sleep(0)
                    fut.cancel()
                    with pytest.raises((asyncio.CancelledError, OperationalError)):
                        await fut
                    del copy_in
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(state) as (port, peer):
        asyncio.run(_exercise(port))

    assert peer.completed_copies == []


_REGPROC_OID = 24
_TIMESTAMPTZ_OID = 1184
_CUSTOM_VECTOR_OID = 42001
_CUSTOM_ENUM_OID = 42002
_CUSTOM_DOMAIN_OID = 42003
_CUSTOM_RANGE_OID = 42004
_CUSTOM_ENUM_ARRAY_OID = 42005


def _pg_row_desc_typed(
    cols: Sequence[tuple[str, int, int]],
    fmts: Sequence[int],
) -> bytes:
    """Encode a RowDescription ('T') frame with explicit `(name, oid, typmod)` and format codes."""
    parts = [struct.pack("!h", len(cols))]
    for (name, oid, typmod), fmt in zip(cols, fmts, strict=True):
        parts.append(name.encode("utf-8") + b"\x00" + struct.pack("!ihIhih", 0, 0, oid, -1, typmod, fmt))
    return _pg_frame(b"T", b"".join(parts))


def _pg_raw_data_row(cells: Sequence[bytes | None]) -> bytes:
    """Encode a DataRow ('D') frame from pre-encoded raw byte cells."""
    parts = [struct.pack("!h", len(cells))]
    for cell in cells:
        if cell is None:
            parts.append(struct.pack("!i", -1))
        else:
            parts.append(struct.pack("!i", len(cell)) + cell)
    return _pg_frame(b"D", b"".join(parts))


def _encode_pgvector_binary(vals: Sequence[float]) -> bytes:
    """Encode a binary pgvector (`uint16 dim + uint16 unused=0 + big-endian float32[]`)."""
    return struct.pack("!HH", len(vals), 0) + b"".join(struct.pack("!f", v) for v in vals)


def _catalog_discovery_reply(sql_q: str, tx_status: bytes) -> bytes:
    """Encode a deterministic `pg_catalog.pg_type` discovery reply matching requested OIDs."""
    cat_cols = (
        ("oid", _OID_OID, -1),
        ("nspname", _NAME_OID, -1),
        ("typname", _NAME_OID, -1),
        ("typtype", _TEXT_OID, -1),
        ("typdelim", _TEXT_OID, -1),
        ("typelem", _OID_OID, -1),
        ("typbasetype", _OID_OID, -1),
        ("extname", _NAME_OID, -1),
        ("attname", _NAME_OID, -1),
        ("atttypid", _OID_OID, -1),
        ("atttypmod", _INT4_OID, -1),
        ("attnum", _INT2_OID, -1),
        ("attisdropped", _TEXT_OID, -1),
    )
    cat_rows: tuple[tuple[str | None, ...], ...] = (
        ("42001", "public", "vector", "b", ",", "0", "0", "vector", None, None, None, None, None),
        ("42002", "public", "mood", "e", ",", "0", "0", None, None, None, None, None, None),
        ("42003", "public", "int4_dom", "d", ",", "0", "23", None, None, None, None, None, None),
        ("42004", "public", "numrange_custom", "r", ",", "0", "0", None, None, None, None, None, None),
        ("42005", "public", "_mood", "b", ",", "42002", "0", None, None, None, None, None, None),
    )
    where_tail = sql_q.partition("WHERE ")[2]
    matched_rows = [row for row in cat_rows if row[0] is not None and row[0] in where_tail]
    selected_rows = matched_rows if matched_rows else list(cat_rows)
    parts = [_pg_row_desc_typed(cat_cols, [0] * len(cat_cols))]
    parts.extend(_pg_raw_data_row([None if c is None else c.encode("utf-8") for c in row]) for row in selected_rows)
    parts.append(_pg_frame(b"C", f"SELECT {len(selected_rows)}\x00".encode("ascii")))
    parts.append(_pg_frame(b"Z", tx_status))
    return b"".join(parts)


def _decode_ipc_table(payload: bytes | memoryview) -> tuple[pa.Schema, pa.Table]:
    """Read an Arrow IPC stream payload into `(schema, table)`."""
    raw = bytes(payload)
    reader = pa.ipc.open_stream(pa.py_buffer(raw))
    return reader.schema, reader.read_all()


@dataclass
class _SyncArrowPeerState:
    """Mutable wire event log and programmable barriers for SyncPostgresClient Arrow tests."""

    returning_row_count: int = 0
    stall_sleep_query: bool = False
    hold_portal_bind: bool = False
    stall_execute_until_cancel: bool = False
    portal_mode: str = "none"
    custom_date_style: tuple[str, str] | None = None
    post_startup_frames: int = 0
    returning_describes: int = 0
    returning_executes: int = 0
    catalog_queries: int = 0
    execute_frames: list[tuple[str, int]] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    cancels: list[tuple[int, int]] = field(default_factory=list)
    bind_waiting: threading.Event = field(default_factory=threading.Event)
    batch1_sent: threading.Event = field(default_factory=threading.Event)
    allow_batch2: threading.Event = field(default_factory=threading.Event)
    batch2_waiting: threading.Event = field(default_factory=threading.Event)
    allow_tail_complete: threading.Event = field(default_factory=threading.Event)
    cancel_arrived: threading.Event = field(default_factory=threading.Event)
    sleep_started: threading.Event = field(default_factory=threading.Event)
    paused_mid_execute: threading.Event = field(default_factory=threading.Event)


def _pg_auth_ready_with_params(pid: int, secret: int, date_tz: tuple[str, str] | None) -> bytes:
    """Build startup ready frames with optional custom DateStyle/TimeZone session GUCs."""
    date_style, tz_name = date_tz if date_tz is not None else ("ISO, MDY", "UTC")
    return b"".join(
        (
            _pg_frame(b"R", struct.pack("!i", 0)),
            _pg_frame(b"S", b"application_name\x00vertebra\x00"),
            _pg_frame(b"S", b"client_encoding\x00UTF8\x00"),
            _pg_frame(b"S", f"DateStyle\x00{date_style}\x00".encode()),
            _pg_frame(b"S", f"TimeZone\x00{tz_name}\x00".encode()),
            _pg_frame(b"K", struct.pack("!ii", pid, secret)),
            _pg_frame(b"Z", b"I"),
        )
    )


def _encode_returning_row(idx: int, fmts: Sequence[int]) -> list[bytes | None]:
    """Encode one mixed binary/text custom-OID RETURNING row matching negotiated formats."""
    row_id = 100 + idx
    id_cell = struct.pack("!i", row_id) if fmts[0] == 1 else str(row_id).encode("ascii")
    vec_vals = (float(idx + 1), float(idx + 2), 0.5)
    vec_cell = (
        _encode_pgvector_binary(vec_vals)
        if fmts[1] == 1
        else f"[{vec_vals[0]},{vec_vals[1]},{vec_vals[2]}]".encode("ascii")
    )
    mood_cell = b"happy" if idx % 2 == 0 else b"calm"
    dom_val = 1000 + idx
    dom_cell = struct.pack("!i", dom_val) if fmts[3] == 1 else str(dom_val).encode("ascii")
    rng_cell = f"[{idx},{idx + 10})".encode("ascii")
    arr_cell = b"{happy,calm}" if idx % 2 == 0 else b"{calm}"
    proc_cell = struct.pack("!I", 1299) if fmts[6] == 1 else b"now"
    return [id_cell, vec_cell, mood_cell, dom_cell, rng_cell, arr_cell, proc_cell]


def _parse_frame_oids(body: bytes) -> list[int]:
    """Extract parameter OID hints encoded in a Parse ('P') payload."""
    first_nul = body.index(b"\x00")
    rest = body[first_nul + 1 :]
    second_nul = rest.index(b"\x00")
    tail = memoryview(rest)[second_nul + 1 :]
    (count,) = struct.unpack("!h", tail[:2])
    return [struct.unpack("!I", tail[2 + i * 4 : 6 + i * 4])[0] for i in range(count)]


def _execute_frame_portal_and_max(body: bytes) -> tuple[str, int]:
    """Extract `(portal_name, max_rows)` from an Execute ('E') payload."""
    nul = body.index(b"\x00")
    portal_name = body[:nul].decode("utf-8")
    (max_rows,) = struct.unpack("!i", body[nul + 1 : nul + 5])
    return portal_name, max_rows


def _close_frame_target(body: bytes) -> tuple[str, str]:
    """Extract `(kind, name)` from a Close ('C') payload."""
    kind = body[:1].decode("ascii")
    nul = body.index(b"\x00", 1)
    return kind, body[1:nul].decode("utf-8")


def _serve_sync_arrow_conn(conn: socket.socket, conn_id: int, state: _SyncArrowPeerState) -> None:
    """Drive one backend PostgreSQL v3 connection for SyncPostgresClient Arrow tests."""
    pid = 9000 + conn_id
    secret = 77000 + conn_id
    tx_status = b"I"
    returning_cols = (
        ("id", _INT4_OID, -1),
        ("vec", _CUSTOM_VECTOR_OID, 3),
        ("mood", _CUSTOM_ENUM_OID, -1),
        ("dom_id", _CUSTOM_DOMAIN_OID, -1),
        ("rng", _CUSTOM_RANGE_OID, -1),
        ("moods", _CUSTOM_ENUM_ARRAY_OID, -1),
        ("fn_proc", _REGPROC_OID, -1),
    )
    portal_step = 0
    current_sql = ""
    current_fmts: list[int] = []

    try:
        conn.settimeout(5.0)
        kind, cancel_key = _read_initial_packet(conn)
        if kind == "cancel":
            if cancel_key is not None:
                state.cancels.append(cancel_key)
                state.events.append("CANCEL")
                state.cancel_arrived.set()
            return

        conn.sendall(_pg_auth_ready_with_params(pid, secret, state.custom_date_style))

        while True:
            try:
                tag, body = _read_frame(conn)
            except (EOFError, OSError):
                return
            if tag == b"X":
                return

            state.post_startup_frames += 1

            if tag == b"Q":
                sql_q = body[:-1].decode("utf-8").strip()
                if "FROM pg_catalog.pg_type" in sql_q:
                    state.catalog_queries += 1
                    state.events.append("Q:CATALOG")
                    conn.sendall(_catalog_discovery_reply(sql_q, tx_status))
                    continue
                state.events.append(f"Q:{sql_q}")
                upper = sql_q.upper()
                if upper == "BEGIN":
                    tx_status = b"T"
                    conn.sendall(_pg_frame(b"C", b"BEGIN\x00") + _pg_frame(b"Z", tx_status))
                elif upper == "COMMIT":
                    tx_status = b"I"
                    conn.sendall(_pg_frame(b"C", b"COMMIT\x00") + _pg_frame(b"Z", tx_status))
                elif upper == "ROLLBACK":
                    tx_status = b"I"
                    conn.sendall(_pg_frame(b"C", b"ROLLBACK\x00") + _pg_frame(b"Z", tx_status))
                elif upper.startswith("SAVEPOINT "):
                    conn.sendall(_pg_frame(b"C", b"SAVEPOINT\x00") + _pg_frame(b"Z", tx_status))
                elif upper.startswith("RELEASE SAVEPOINT "):
                    conn.sendall(_pg_frame(b"C", b"RELEASE\x00") + _pg_frame(b"Z", tx_status))
                elif upper.startswith("ROLLBACK TO SAVEPOINT "):
                    if tx_status == b"E":
                        tx_status = b"T"
                    conn.sendall(_pg_frame(b"C", b"ROLLBACK\x00") + _pg_frame(b"Z", tx_status))
                else:
                    conn.sendall(_pg_frame(b"C", b"SELECT 0\x00") + _pg_frame(b"Z", tx_status))
                continue

            grp = _read_extended_batch((tag, body), conn)
            state.post_startup_frames += len(grp) - 1
            has_execute = any(ft == b"E" for ft, _ in grp)

            for ft, fb in grp:
                if ft == b"P":
                    _, parsed_sql = _parse_frame_stmt_and_sql(fb)
                    oids = _parse_frame_oids(fb)
                    current_sql = parsed_sql
                    state.events.append(f"P:{parsed_sql}:oids={oids}")
                elif ft == b"B":
                    _, _, rfmts = _bind_frame_stmt_params_and_formats(fb)
                    current_fmts = rfmts
                elif ft == b"C":
                    ck, cname = _close_frame_target(fb)
                    state.events.append(f"Close({ck}:{cname})")

            if "pg_sleep" in current_sql and has_execute and state.stall_sleep_query:
                state.sleep_started.set()
                if state.cancel_arrived.wait(timeout=2.0):
                    parts_cancel: list[bytes] = []
                    for ft, fb in grp:
                        if ft == b"P":
                            parts_cancel.append(_pg_frame(b"1", b""))
                        elif ft == b"D":
                            if fb[:1] == b"S":
                                parts_cancel.append(_pg_frame(b"t", struct.pack("!h", 0)))
                            parts_cancel.append(_pg_row_desc_typed([("s", _INT4_OID, -1)], [0]))
                        elif ft == b"B":
                            parts_cancel.append(_pg_frame(b"2", b""))
                        elif ft == b"E":
                            parts_cancel.append(_pg_error("57014", "canceling statement due to user request"))
                        elif ft == b"S":
                            parts_cancel.append(_pg_frame(b"Z", tx_status))
                    with contextlib.suppress(OSError):
                        conn.sendall(b"".join(parts_cancel))
                return

            if "pg_sleep" in current_sql and not has_execute:
                out_desc: list[bytes] = []
                for ft, fb in grp:
                    if ft == b"P":
                        out_desc.append(_pg_frame(b"1", b""))
                    elif ft == b"D" and fb[:1] == b"S":
                        out_desc.append(_pg_frame(b"t", struct.pack("!h", 0)))
                        out_desc.append(_pg_row_desc_typed([("s", _INT4_OID, -1)], [0]))
                    elif ft == b"S":
                        out_desc.append(_pg_frame(b"Z", tx_status))
                conn.sendall(b"".join(out_desc))
                continue

            if "INSERT INTO audit" in current_sql:
                out_ins: list[bytes] = []
                for ft, fb in grp:
                    if ft == b"P":
                        out_ins.append(_pg_frame(b"1", b""))
                    elif ft == b"D":
                        if fb[:1] == b"S":
                            out_ins.append(_pg_frame(b"t", struct.pack("!h", 0)))
                        out_ins.append(_pg_frame(b"n", b""))
                    elif ft == b"B":
                        out_ins.append(_pg_frame(b"2", b""))
                    elif ft == b"E":
                        state.events.append("EXEC:INSERT_AUDIT")
                        out_ins.append(_pg_frame(b"C", b"INSERT 0 1\x00"))
                    elif ft == b"C":
                        out_ins.append(_pg_frame(b"3", b""))
                    elif ft == b"S":
                        out_ins.append(_pg_frame(b"Z", tx_status))
                conn.sendall(b"".join(out_ins))
                continue

            if "SELECT 42 AS answer" in current_sql or "SELECT 1" in current_sql:
                col_name = "answer" if "answer" in current_sql else "?column?"
                val_num = 42 if "answer" in current_sql else 1
                col_spec = [(col_name, _INT4_OID, -1)]
                eff_fmts = _resolve_portal_formats(1, current_fmts) if has_execute else [0]
                out_simple: list[bytes] = []
                for ft, fb in grp:
                    if ft == b"P":
                        out_simple.append(_pg_frame(b"1", b""))
                    elif ft == b"D":
                        if fb[:1] == b"S":
                            out_simple.append(_pg_frame(b"t", struct.pack("!h", 0)))
                            out_simple.append(_pg_row_desc_typed(col_spec, [0]))
                        else:
                            out_simple.append(_pg_row_desc_typed(col_spec, eff_fmts))
                    elif ft == b"B":
                        out_simple.append(_pg_frame(b"2", b""))
                    elif ft == b"E":
                        cell = struct.pack("!i", val_num) if eff_fmts[0] == 1 else str(val_num).encode("ascii")
                        out_simple.append(_pg_raw_data_row([cell]))
                        out_simple.append(_pg_frame(b"C", b"SELECT 1\x00"))
                    elif ft == b"C":
                        out_simple.append(_pg_frame(b"3", b""))
                    elif ft == b"S":
                        out_simple.append(_pg_frame(b"Z", tx_status))
                conn.sendall(b"".join(out_simple))
                continue

            if "RETURNING id, vec" in current_sql:
                if not has_execute:
                    state.returning_describes += 1
                    state.events.append("DESCRIBE_ONLY:RETURNING")
                out_ret: list[bytes] = []
                eff_ret_fmts = _resolve_portal_formats(len(returning_cols), current_fmts)
                for ft, fb in grp:
                    if ft == b"P":
                        out_ret.append(_pg_frame(b"1", b""))
                    elif ft == b"D":
                        if fb[:1] == b"S":
                            out_ret.append(
                                _pg_frame(
                                    b"t",
                                    struct.pack("!hII", 2, _TEXT_OID, _CUSTOM_ENUM_OID),
                                )
                            )
                            out_ret.append(_pg_row_desc_typed(returning_cols, [0] * len(returning_cols)))
                        else:
                            out_ret.append(_pg_row_desc_typed(returning_cols, eff_ret_fmts))
                    elif ft == b"B":
                        out_ret.append(_pg_frame(b"2", b""))
                    elif ft == b"E":
                        state.returning_executes += 1
                        state.events.append(f"EXEC:RETURNING:fmts={eff_ret_fmts}")
                        for r_idx in range(state.returning_row_count):
                            out_ret.append(_pg_raw_data_row(_encode_returning_row(r_idx, eff_ret_fmts)))
                        out_ret.append(_pg_frame(b"C", f"INSERT 0 {state.returning_row_count}\x00".encode("ascii")))
                    elif ft == b"C":
                        out_ret.append(_pg_frame(b"3", b""))
                    elif ft == b"S":
                        out_ret.append(_pg_frame(b"Z", tx_status))
                conn.sendall(b"".join(out_ret))
                continue

            if state.portal_mode in (
                "suspended_barrier",
                "big_resume",
                "big_stall_cancel",
                "oversized_row",
                "scope_tracking",
            ):
                if state.portal_mode == "suspended_barrier":
                    p_cols: Sequence[tuple[str, int, int]] = (
                        ("fn_col", _REGPROC_OID, -1),
                        ("ts_col", _TIMESTAMPTZ_OID, -1),
                    )
                elif state.portal_mode in ("big_resume", "big_stall_cancel"):
                    p_cols = (("dup", _TEXT_OID, -1), ("dup", _TEXT_OID, -1))
                else:
                    p_cols = (("payload", _TEXT_OID, -1),)

                has_bind = any(ft == b"B" for ft, _ in grp)
                has_close = any(ft == b"C" for ft, _ in grp)
                if has_bind and not has_execute:
                    if state.hold_portal_bind:
                        state.bind_waiting.set()
                        if state.cancel_arrived.wait(timeout=2.0):
                            tx_status = b"E"
                            out_cancel_bind: list[bytes] = []
                            for ft, fb in grp:
                                if ft == b"P":
                                    out_cancel_bind.append(_pg_frame(b"1", b""))
                                elif ft == b"D" and fb[:1] == b"S":
                                    out_cancel_bind.append(_pg_frame(b"t", struct.pack("!h", 0)))
                                    out_cancel_bind.append(_pg_row_desc_typed(p_cols, [0] * len(p_cols)))
                                elif ft == b"B":
                                    out_cancel_bind.append(
                                        _pg_error("57014", "canceling portal open due to user request")
                                    )
                                elif ft == b"S":
                                    out_cancel_bind.append(_pg_frame(b"Z", tx_status))
                            with contextlib.suppress(OSError):
                                conn.sendall(b"".join(out_cancel_bind))
                        return
                    out_open: list[bytes] = []
                    for ft, fb in grp:
                        if ft == b"P":
                            out_open.append(_pg_frame(b"1", b""))
                        elif ft == b"D":
                            if fb[:1] == b"S":
                                out_open.append(_pg_frame(b"t", struct.pack("!h", 0)))
                            out_open.append(_pg_row_desc_typed(p_cols, [0] * len(p_cols)))
                        elif ft == b"B":
                            out_open.append(_pg_frame(b"2", b""))
                        elif ft == b"S":
                            out_open.append(_pg_frame(b"Z", tx_status))
                    conn.sendall(b"".join(out_open))
                    continue

                if has_close and not has_execute:
                    out_cls: list[bytes] = []
                    for ft, _ in grp:
                        if ft == b"C":
                            out_cls.append(_pg_frame(b"3", b""))
                        elif ft == b"S":
                            out_cls.append(_pg_frame(b"Z", tx_status))
                    conn.sendall(b"".join(out_cls))
                    continue

                if has_execute:
                    exec_body = next(fb for ft, fb in grp if ft == b"E")
                    pname, max_rows = _execute_frame_portal_and_max(exec_body)
                    state.execute_frames.append((pname, max_rows))
                    state.events.append(f"E:{pname}:{max_rows}")
                    portal_step += 1

                    if state.portal_mode == "suspended_barrier":
                        if portal_step == 1:
                            conn.sendall(
                                _pg_raw_data_row([b"now", b"Sun Nov 18 11:59:59 1883 LMT"])
                                + _pg_frame(b"s", b"")
                                + _pg_frame(b"Z", tx_status)
                            )
                            state.batch1_sent.set()
                        elif portal_step == 2:
                            state.batch2_waiting.set()
                            if state.stall_execute_until_cancel:
                                if state.cancel_arrived.wait(timeout=2.0):
                                    tx_status = b"E"
                                    with contextlib.suppress(OSError):
                                        conn.sendall(
                                            _pg_error("57014", "canceling portal fetch due to user request")
                                            + _pg_frame(b"Z", tx_status)
                                        )
                                return
                            if not state.allow_batch2.wait(timeout=3.0):
                                return
                            conn.sendall(
                                _pg_raw_data_row([b"pg_typeof", b"Wed Dec 17 07:37:16 1997 EST"])
                                + _pg_frame(b"s", b"")
                                + _pg_frame(b"Z", tx_status)
                            )
                        else:
                            conn.sendall(_pg_frame(b"C", b"SELECT 0\x00") + _pg_frame(b"Z", tx_status))
                        continue

                    if state.portal_mode == "big_resume":
                        chunk_a = b"a" * (1792 * 1024)
                        chunk_b = b"b" * (1792 * 1024)
                        chunk_c = b"c" * (1792 * 1024)
                        conn.sendall(
                            _pg_raw_data_row([chunk_a, chunk_b])
                            + _pg_raw_data_row([chunk_b, chunk_a])
                            + _pg_raw_data_row([chunk_c, chunk_c])
                        )
                        state.paused_mid_execute.set()
                        if not state.allow_tail_complete.wait(timeout=3.0):
                            return
                        conn.sendall(_pg_frame(b"C", b"SELECT 3\x00") + _pg_frame(b"Z", tx_status))
                        continue

                    if state.portal_mode == "big_stall_cancel":
                        chunk_a = b"x" * (1792 * 1024)
                        chunk_b = b"y" * (1792 * 1024)
                        conn.sendall(
                            _pg_raw_data_row([chunk_a, chunk_b])
                            + _pg_raw_data_row([chunk_b, chunk_a])
                            + _pg_raw_data_row([chunk_a, chunk_a])
                        )
                        state.paused_mid_execute.set()
                        if state.cancel_arrived.wait(timeout=2.0):
                            tx_status = b"E"
                            with contextlib.suppress(OSError):
                                conn.sendall(
                                    _pg_error("57014", "canceling portal due to user request")
                                    + _pg_frame(b"Z", tx_status)
                                )
                        return

                    if state.portal_mode == "oversized_row":
                        huge = b"z" * (8 * 1024 * 1024 + 512 * 1024)
                        conn.sendall(
                            _pg_raw_data_row([huge]) + _pg_frame(b"C", b"SELECT 1\x00") + _pg_frame(b"Z", tx_status)
                        )
                        continue

                    if state.portal_mode == "scope_tracking":
                        if portal_step == 1:
                            conn.sendall(
                                _pg_raw_data_row([b"row1"]) + _pg_frame(b"s", b"") + _pg_frame(b"Z", tx_status)
                            )
                        else:
                            conn.sendall(_pg_frame(b"C", b"SELECT 0\x00") + _pg_frame(b"Z", tx_status))
                        continue
    finally:
        with contextlib.suppress(OSError):
            conn.close()


@contextlib.contextmanager
def _running_sync_arrow_peer(
    state: _SyncArrowPeerState | None = None,
) -> Iterator[tuple[int, _SyncArrowPeerState]]:
    """Run a multi-connection loopback PostgreSQL v3 peer for SyncPostgresClient Arrow tests."""
    peer = state or _SyncArrowPeerState()
    stop = threading.Event()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.05)
    port = int(srv.getsockname()[1])
    workers: list[threading.Thread] = []

    def _accept_loop() -> None:
        seq = 0
        while not stop.is_set():
            try:
                c, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            seq += 1
            t = threading.Thread(target=_serve_sync_arrow_conn, args=(c, seq, peer), daemon=True)
            t.start()
            workers.append(t)

    accept_t = threading.Thread(target=_accept_loop, daemon=True)
    accept_t.start()
    try:
        yield port, peer
    finally:
        stop.set()
        with contextlib.suppress(OSError):
            srv.close()
        accept_t.join(timeout=2.0)
        for w in workers:
            w.join(timeout=2.0)


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"threshold_rows": -1}, (ValueError, DataError)),
        ("SELECT 1", None, {"threshold_rows": True}, (TypeError,)),
        ("SELECT 1", None, {"threshold_rows": False}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": -8}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": True}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": False}, (TypeError,)),
        ("SELECT 1", None, {"timeout": 0.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": -1.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("nan")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("inf")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": True}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1", [1], {"parameter_oids": [-1]}, (ValueError, DataError, OverflowError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_sync_arrow_select_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify select_to_arrow rejects invalid bounds, bools, timeouts, and parameter_oids before sending wire frames."""
    with _running_sync_arrow_peer() as (port, state):
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.select_to_arrow(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            client.close()


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"batch_size": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": -4}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": True}, (TypeError,)),
        ("SELECT 1", None, {"batch_size": False}, (TypeError,)),
        ("SELECT 1", None, {"columns": "not_seq"}, (TypeError,)),
        ("SELECT 1", None, {"columns": [123]}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_sync_arrow_stream_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify stream_arrow_batches rejects invalid batch_size, bools, columns, and parameter_oids with zero wire frames."""
    with _running_sync_arrow_peer() as (port, state):
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.stream_arrow_batches(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            client.close()


@pytest.mark.parametrize(
    ("row_count", "expect_shm"),
    [
        (0, False),
        (3, False),
        (4, True),
    ],
)
def test_sync_arrow_select_cold_custom_oids_volatile_returning_and_thresholds(
    row_count: int,
    expect_shm: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cold custom OIDs run Parse+Describe preflight + discovery before single Execute and promote at threshold_rows."""
    sql = "INSERT INTO items (tag, mood) VALUES ($1, $2) RETURNING id, vec, mood, dom_id, rng, moods, fn_proc"
    peer_state = _SyncArrowPeerState(returning_row_count=row_count)

    with _forbid_row_materialization(monkeypatch), _running_sync_arrow_peer(peer_state) as (port, state):
        client = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        res: PostgresArrowResult | None = None
        try:
            res = client.select_to_arrow(
                sql,
                ["alpha", "happy"],
                parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                threshold_rows=3,
            )
            assert isinstance(res, PostgresArrowResult)
            assert res.row_count == row_count
            assert res.columns == ("id", "vec", "mood", "dom_id", "rng", "moods", "fn_proc")
            assert res.used_shm is expect_shm

            if expect_shm:
                assert isinstance(res.buffer, native_vertebra.ArrowCursor)
                assert res.buffer.row_count == row_count
                schema, table = _decode_ipc_table(res.buffer.buffer_view())
            else:
                assert isinstance(res.buffer, bytes)
                schema, table = _decode_ipc_table(res.buffer)

            assert schema.metadata is not None
            assert schema.metadata.get(b"postgres.arrow.version") == b"1"
            assert schema.field("vec").type == pa.list_(schema.field("vec").type.value_field, 3)
            assert schema.field("vec").metadata[b"postgres.encoding"] == b"pgvector"
            assert schema.field("mood").metadata[b"postgres.encoding"] == b"pg-enum"
            assert schema.field("dom_id").metadata[b"postgres.oid"] == b"42003"
            assert schema.field("dom_id").metadata[b"postgres.base_oid"] == b"23"
            assert schema.field("dom_id").metadata[b"postgres.type_name"] == b"int4_dom"
            assert schema.field("rng").metadata[b"postgres.encoding"] == b"pg-range"
            assert schema.field("moods").metadata[b"postgres.encoding"] == b"pg-array"
            assert schema.field("fn_proc").metadata[b"postgres.encoding"] == b"regproc-text"
            assert table.num_rows == row_count

            rows_list = table.to_pylist()
            expected_ids = [100 + i for i in range(row_count)]
            assert [r["id"] for r in rows_list] == expected_ids
            if row_count > 0:
                assert rows_list[0]["vec"] == [1.0, 2.0, 0.5]
                assert rows_list[0]["mood"] == "happy"
                assert rows_list[0]["dom_id"] == 1000
                assert rows_list[0]["rng"] == "[0,10)"
                assert rows_list[0]["moods"]["values"] == ["happy", "calm"]
                assert rows_list[0]["fn_proc"] == "now"

            with pytest.raises(DataError):
                client.select_to_arrow("SELECT 42 AS answer", max_result_bytes=8)
            warm_check = client.select_to_arrow("SELECT 42 AS answer")
            assert isinstance(warm_check.buffer, bytes)
            _, warm_tbl = _decode_ipc_table(warm_check.buffer)
            assert warm_tbl.to_pylist() == [{"answer": 42}]
        finally:
            if res is not None and res.used_shm and isinstance(res.buffer, native_vertebra.ArrowCursor):
                res.buffer.release()
            client.close()

    assert state.returning_describes == 1
    assert state.returning_executes == 1
    assert state.catalog_queries >= 2
    describe_pos = state.events.index("DESCRIBE_ONLY:RETURNING")
    exec_pos = next(i for i, ev in enumerate(state.events) if ev.startswith("EXEC:RETURNING:"))
    cat_between = [i for i, ev in enumerate(state.events) if ev == "Q:CATALOG" and describe_pos < i < exec_pos]
    assert len(cat_between) == 1
    assert state.events[exec_pos] == "EXEC:RETURNING:fmts=[1, 1, 0, 1, 0, 0, 0]"


def test_sync_arrow_select_timeout_cancels_and_recovers_pool() -> None:
    """Verify select_to_arrow command timeout emits CancelRequest, cleans up socket, and recovers pool."""
    peer_state = _SyncArrowPeerState(stall_sleep_query=True)
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            try:
                with pytest.raises(OperationalError) as exc_info:
                    c1.select_to_arrow("SELECT pg_sleep(5)", timeout=0.02)
                assert exc_info.value.sqlstate is None
            finally:
                c1.close()

            assert state.cancels == [(9001, 77001)]

            c2 = pool.acquire()
            try:
                res = c2.select_to_arrow("SELECT 42 AS answer", threshold_rows=10)
                assert res.row_count == 1
                assert res.columns == ("answer",)
                assert res.used_shm is False
                assert isinstance(res.buffer, bytes)
                _, tbl = _decode_ipc_table(res.buffer)
                assert tbl.to_pylist() == [{"answer": 42}]
            finally:
                c2.close()
        finally:
            pool.close()


def test_sync_arrow_stream_portal_suspended_barrier_text_fidelity_and_pool_pinning() -> None:
    """Verify SyncPostgresArrowStream yields at PortalSuspended, preserves regproc-text and TemporalText, pins pool, and retains cursors after close."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        custom_date_style=("Postgres, MDY", "America/New_York"),
    )
    with _running_sync_arrow_peer(peer_state) as (port, state):
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
        cur1: Any = None
        cur2: Any = None
        mv1: memoryview | None = None
        try:
            c1 = pool.acquire()
            stream: native_pg.SyncPostgresArrowStream | None = None
            try:
                stream = c1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                assert isinstance(stream, native_pg.SyncPostgresArrowStream)
                assert state.events.count("Q:BEGIN") == 1

                cur1 = next(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 1
                assert not state.allow_batch2.is_set()
                assert len(state.execute_frames) == 1
                assert state.execute_frames[0][1] == 1

                mv1 = cur1.buffer_view()
                schema1, tbl1 = _decode_ipc_table(mv1)
                assert schema1.metadata[b"postgres.arrow.version"] == b"1"
                assert schema1.field("fn_col").metadata[b"postgres.encoding"] == b"regproc-text"
                assert schema1.field("ts_col").metadata[b"postgres.encoding"] == b"pg-timestamptz"
                assert tbl1.to_pylist() == [
                    {
                        "fn_col": "now",
                        "ts_col": {
                            "kind": 2,
                            "value": None,
                            "wire_text": "Sun Nov 18 11:59:59 1883 LMT",
                            "date_style": "Postgres, MDY",
                            "time_zone": "America/New_York",
                        },
                    }
                ]

                with pytest.raises(InterfaceError):
                    c1.query("SELECT 1")
                with pytest.raises(InterfaceError):
                    c1.select_to_arrow("SELECT 1")
                with pytest.raises(OperationalError):
                    pool.acquire()

                worker_out: list[Any] = []
                worker_err: list[BaseException] = []
                active_stream = stream

                def _fetch_batch2() -> None:
                    try:
                        worker_out.append(next(active_stream))
                    except BaseException as exc:
                        worker_err.append(exc)

                t_batch2 = threading.Thread(target=_fetch_batch2)
                t_batch2.start()
                assert state.batch2_waiting.wait(timeout=2.0)

                with pytest.raises(InterfaceError):
                    next(stream)
                with pytest.raises(InterfaceError):
                    stream.close()

                state.allow_batch2.set()
                t_batch2.join(timeout=2.0)
                assert not worker_err
                cur2 = worker_out[0]
                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1

                with pytest.raises(StopIteration):
                    next(stream)
                with pytest.raises(StopIteration):
                    next(stream)

                stream.close()
                stream.close()
            finally:
                if stream is not None:
                    with contextlib.suppress(Exception):
                        stream.close()
                c1.close()

            assert state.events.count("Q:BEGIN") == 1
            assert state.events.count("Q:COMMIT") == 1

            _, tbl1_after = _decode_ipc_table(mv1)
            assert tbl1_after.to_pylist()[0]["fn_col"] == "now"
            _, tbl2_after = _decode_ipc_table(cur2.buffer_view())
            row2 = tbl2_after.to_pylist()[0]
            assert row2["fn_col"] == "pg_typeof"
            assert row2["ts_col"]["kind"] == 0
            assert isinstance(row2["ts_col"]["value"], int)

            c_check = pool.acquire()
            try:
                assert c_check.query("SELECT 1").rows == ((1,),)
            finally:
                c_check.close()
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            pool.close()


def test_sync_arrow_stream_mid_execute_8mib_byte_cap_resume_and_duplicate_columns() -> None:
    """Verify 3 x 3.5 MiB rows inside one Execute(10) flush batch 1 at 8 MiB without a second Execute and resume row 3 with duplicate column names."""
    peer_state = _SyncArrowPeerState(portal_mode="big_resume")
    with _running_sync_arrow_peer(peer_state) as (port, state):
        client = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        cur1: Any = None
        cur2: Any = None
        try:
            with client.stream_arrow_batches(
                "SELECT a, b FROM big_rows",
                batch_size=10,
                columns=["x", "x"],
            ) as stream:
                cur1 = next(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 2
                assert len(state.execute_frames) == 1
                assert not state.allow_tail_complete.is_set()

                schema1, tbl1 = _decode_ipc_table(cur1.buffer_view())
                assert [f.name for f in schema1] == ["x", "x"]
                assert tbl1.column(0)[0].as_py() == "a" * (1792 * 1024)
                assert tbl1.column(1)[0].as_py() == "b" * (1792 * 1024)
                assert tbl1.column(0)[1].as_py() == "b" * (1792 * 1024)
                assert tbl1.column(1)[1].as_py() == "a" * (1792 * 1024)

                state.allow_tail_complete.set()
                cur2 = next(stream)
                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1
                assert len(state.execute_frames) == 1

                schema2, tbl2 = _decode_ipc_table(cur2.buffer_view())
                assert [f.name for f in schema2] == ["x", "x"]
                assert tbl2.column(0)[0].as_py() == "c" * (1792 * 1024)
                assert tbl2.column(1)[0].as_py() == "c" * (1792 * 1024)

                with pytest.raises(StopIteration):
                    next(stream)
        finally:
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            client.close()


def test_sync_arrow_stream_mid_execute_close_cancels_retains_cursor_and_recovers_pool() -> None:
    """Verify stream.close() while paused mid-Execute at 8 MiB sends CancelRequest, keeps retained SHM cursor readable, and recovers max_size=1 pool."""
    peer_state = _SyncArrowPeerState(portal_mode="big_stall_cancel")
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        cur1: Any = None
        mv1: memoryview | None = None
        try:
            c1 = pool.acquire()
            try:
                stream = c1.stream_arrow_batches(
                    "SELECT a, b FROM stalled_big_rows",
                    batch_size=10,
                    columns=["x", "x"],
                )
                cur1 = next(stream)
                assert cur1.row_count == 2
                mv1 = cur1.buffer_view()
                assert len(state.execute_frames) == 1

                stream.close()
                with pytest.raises(InterfaceError):
                    next(stream)
            finally:
                c1.close()

            assert state.cancels == [(9001, 77001)]
            schema1, tbl1 = _decode_ipc_table(mv1)
            assert [f.name for f in schema1] == ["x", "x"]
            assert tbl1.num_rows == 2
            assert len(tbl1.column(0)[0].as_py()) == 1792 * 1024

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            pool.close()


@pytest.mark.parametrize("failure_mode", ("columns_mismatch", "oversized_row"))
def test_sync_arrow_stream_columns_mismatch_and_oversized_row_rollback_and_recover(
    failure_mode: str,
) -> None:
    """Verify columns width mismatch and single row > 8 MiB raise DataError, clean up portal scope, and keep pool healthy."""
    portal_mode = "big_resume" if failure_mode == "columns_mismatch" else "oversized_row"
    peer_state = _SyncArrowPeerState(portal_mode=portal_mode)
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            try:
                if failure_mode == "columns_mismatch":
                    with pytest.raises(DataError):
                        c1.stream_arrow_batches(
                            "SELECT a, b FROM big_rows",
                            batch_size=4,
                            columns=["only_one"],
                        )
                    assert "Q:ROLLBACK" in state.events
                else:
                    with (
                        pytest.raises(DataError),
                        c1.stream_arrow_batches("SELECT payload FROM huge_rows", batch_size=4) as stream,
                    ):
                        next(stream)
            finally:
                c1.close()

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            pool.close()


def test_sync_arrow_stream_single_scope_begin_rollback_and_ambient_savepoint() -> None:
    """Verify idle early-closed stream emits single BEGIN+ROLLBACK while ambient client.begin() uses SAVEPOINT+ROLLBACK TO SAVEPOINT+RELEASE SAVEPOINT."""
    idle_state = _SyncArrowPeerState(portal_mode="scope_tracking")
    with _running_sync_arrow_peer(idle_state) as (port, s_idle):
        c_idle = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        c_idle.connect()
        cur_idle: Any = None
        try:
            with c_idle.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_idle = next(stream)
                assert cur_idle.row_count == 1
            assert c_idle.query("SELECT 1").rows == ((1,),)
        finally:
            if cur_idle is not None:
                cur_idle.release()
            c_idle.close()

    assert s_idle.events.count("Q:BEGIN") == 1
    assert s_idle.events.count("Q:ROLLBACK") == 1
    assert any(ev.startswith("Close(P:") for ev in s_idle.events)

    amb_state = _SyncArrowPeerState(portal_mode="scope_tracking")
    with _running_sync_arrow_peer(amb_state) as (port, s_amb):
        c_amb = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        c_amb.connect()
        cur_amb: Any = None
        try:
            c_amb.begin()
            c_amb.query("INSERT INTO audit VALUES (1)")
            with c_amb.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_amb = next(stream)
                assert cur_amb.row_count == 1
            assert c_amb.query("SELECT 1").rows == ((1,),)
            c_amb.commit()
            assert c_amb.query("SELECT 1").rows == ((1,),)
        finally:
            if cur_amb is not None:
                cur_amb.release()
            c_amb.close()

    assert s_amb.events.count("Q:BEGIN") == 1
    assert "EXEC:INSERT_AUDIT" in s_amb.events
    savepoints = [ev for ev in s_amb.events if ev.startswith("Q:SAVEPOINT vtb_sp_")]
    rollbacks_sp = [ev for ev in s_amb.events if ev.startswith("Q:ROLLBACK TO SAVEPOINT vtb_sp_")]
    releases_sp = [ev for ev in s_amb.events if ev.startswith("Q:RELEASE SAVEPOINT vtb_sp_")]
    assert len(savepoints) == 1
    assert len(rollbacks_sp) == 1
    assert len(releases_sp) == 1
    assert "Q:ROLLBACK" not in s_amb.events
    assert s_amb.events.count("Q:COMMIT") == 1
    assert s_amb.events.index(releases_sp[0]) < s_amb.events.index("Q:COMMIT")


def _process_arrow_shm_files() -> set[str]:
    """Return active `/dev/shm` VTBARROW segment filenames created by this OS process."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return set()
    prefix = f"vtb-pg-{os.getpid():x}-"
    return {entry.name for entry in shm_dir.iterdir() if entry.name.startswith(prefix)}


def _find_loop_drainer_fd(loop: asyncio.AbstractEventLoop, drainer: Any) -> int:
    """Locate the file descriptor registered with `loop.add_reader` for `drainer`."""
    selector = getattr(loop, "_selector", None)
    assert selector is not None
    for key in selector.get_map().values():
        data = key.data
        if isinstance(data, tuple) and data and getattr(data[0], "_callback", None) is drainer:
            return int(key.fd)
    msg = "failed to find _vertebra_postgres_completion_drainer reader fd on running loop"
    raise AssertionError(msg)


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"threshold_rows": -1}, (ValueError, DataError)),
        ("SELECT 1", None, {"threshold_rows": True}, (TypeError,)),
        ("SELECT 1", None, {"threshold_rows": False}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": -8}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": True}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": False}, (TypeError,)),
        ("SELECT 1", None, {"timeout": 0.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": -1.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("nan")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("inf")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": True}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1", [1], {"parameter_oids": [-1]}, (ValueError, DataError, OverflowError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_async_arrow_select_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify AsyncPostgresClient.select_to_arrow rejects invalid bounds, bools, timeouts, and parameter_oids before sending wire frames."""

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        client = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        try:
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                await client.select_to_arrow(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            await client.close()

    with _running_sync_arrow_peer() as (port, state):
        asyncio.run(_run(port, state))


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"batch_size": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": -4}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": True}, (TypeError,)),
        ("SELECT 1", None, {"batch_size": False}, (TypeError,)),
        ("SELECT 1", None, {"columns": "not_seq"}, (TypeError,)),
        ("SELECT 1", None, {"columns": [123]}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_async_arrow_stream_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify synchronous AsyncPostgresClient.stream_arrow_batches rejects invalid batch_size, bools, columns, and parameter_oids with zero frames."""

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        client = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        try:
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.stream_arrow_batches(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            await client.close()

    with _running_sync_arrow_peer() as (port, state):
        asyncio.run(_run(port, state))


@pytest.mark.parametrize(
    ("row_count", "expect_shm"),
    [
        (0, False),
        (3, False),
        (4, True),
    ],
)
def test_async_arrow_select_cold_custom_oids_volatile_returning_and_thresholds(
    row_count: int,
    expect_shm: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async select_to_arrow discovers cold custom OIDs before single volatile execution and matches sync IPC parity."""
    sql = "INSERT INTO items (tag, mood) VALUES ($1, $2) RETURNING id, vec, mood, dom_id, rng, moods, fn_proc"
    async_peer = _SyncArrowPeerState(returning_row_count=row_count)
    sync_peer = _SyncArrowPeerState(returning_row_count=row_count)

    with _forbid_row_materialization(monkeypatch):
        with _running_sync_arrow_peer(sync_peer) as (sport, _):
            sclient = SyncPostgresClient(
                host="127.0.0.1",
                port=sport,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            sclient.connect()
            sres: PostgresArrowResult | None = None
            try:
                sres = sclient.select_to_arrow(
                    sql,
                    ["alpha", "happy"],
                    parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                    threshold_rows=3,
                )
                sync_payload = (
                    bytes(sres.buffer.buffer_view())
                    if isinstance(sres.buffer, native_vertebra.ArrowCursor)
                    else sres.buffer
                )
                sync_schema, sync_tbl = _decode_ipc_table(sync_payload)
                sync_rows = sync_tbl.to_pylist()
            finally:
                if sres is not None and isinstance(sres.buffer, native_vertebra.ArrowCursor):
                    sres.buffer.release()
                sclient.close()

        async def _run_async(aport: int) -> tuple[pa.Schema, list[dict[str, Any]]]:
            aclient = AsyncPostgresClient(
                host="127.0.0.1",
                port=aport,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            await aclient.connect()
            ares: PostgresArrowResult | None = None
            try:
                ares = await aclient.select_to_arrow(
                    sql,
                    ["alpha", "happy"],
                    parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                    timeout=5.0,
                    threshold_rows=3,
                )
                assert isinstance(ares, PostgresArrowResult)
                assert ares.row_count == row_count
                assert ares.columns == ("id", "vec", "mood", "dom_id", "rng", "moods", "fn_proc")
                assert ares.used_shm is expect_shm
                if expect_shm:
                    assert isinstance(ares.buffer, native_vertebra.ArrowCursor)
                    assert ares.buffer.row_count == row_count
                    schema, tbl = _decode_ipc_table(ares.buffer.buffer_view())
                else:
                    assert isinstance(ares.buffer, bytes)
                    schema, tbl = _decode_ipc_table(ares.buffer)

                with pytest.raises(DataError):
                    await aclient.select_to_arrow("SELECT 42 AS answer", max_result_bytes=8)
                warm_check = await aclient.select_to_arrow("SELECT 42 AS answer")
                assert isinstance(warm_check.buffer, bytes)
                _, warm_tbl = _decode_ipc_table(warm_check.buffer)
                assert warm_tbl.to_pylist() == [{"answer": 42}]
                return schema, tbl.to_pylist()
            finally:
                if ares is not None and isinstance(ares.buffer, native_vertebra.ArrowCursor):
                    ares.buffer.release()
                await aclient.close()

        with _running_sync_arrow_peer(async_peer) as (aport, state):
            async_schema, async_rows = asyncio.run(_run_async(aport))

    assert async_schema == sync_schema
    assert async_rows == sync_rows
    assert state.returning_describes == 1
    assert state.returning_executes == 1
    assert state.catalog_queries >= 2
    describe_pos = state.events.index("DESCRIBE_ONLY:RETURNING")
    exec_pos = next(i for i, ev in enumerate(state.events) if ev.startswith("EXEC:RETURNING:"))
    cat_between = [i for i, ev in enumerate(state.events) if ev == "Q:CATALOG" and describe_pos < i < exec_pos]
    assert len(cat_between) == 1
    assert state.events[exec_pos] == "EXEC:RETURNING:fmts=[1, 1, 0, 1, 0, 0, 0]"


def test_async_arrow_select_timeout_and_loop_heartbeat_recovers_pool() -> None:
    """Verify async select_to_arrow timeout keeps asyncio loop responsive, sends CancelRequest, and recovers max_size=1 pool."""
    peer_state = _SyncArrowPeerState(stall_sleep_query=True)

    async def _run(port: int, state: _SyncArrowPeerState) -> int:
        ticks = 0
        stop_hb = False

        async def _heartbeat() -> None:
            nonlocal ticks
            while not stop_hb:
                await asyncio.sleep(0.005)
                ticks += 1

        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        hb_task = asyncio.create_task(_heartbeat())
        try:
            c1 = await pool.acquire()
            try:
                with pytest.raises(OperationalError) as exc_info:
                    await c1.select_to_arrow("SELECT pg_sleep(5)", timeout=0.05)
                assert exc_info.value.sqlstate is None
            finally:
                await c1.close()

            assert state.cancels == [(9001, 77001)]

            c2 = await pool.acquire()
            try:
                res = await c2.select_to_arrow("SELECT 42 AS answer", threshold_rows=10)
                assert res.row_count == 1
                assert res.columns == ("answer",)
                assert res.used_shm is False
                assert isinstance(res.buffer, bytes)
                _, tbl = _decode_ipc_table(res.buffer)
                assert tbl.to_pylist() == [{"answer": 42}]
            finally:
                await c2.close()
            return ticks
        finally:
            stop_hb = True
            await hb_task
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        observed_ticks = asyncio.run(_run(port, state))
    assert observed_ticks >= 3


def test_async_arrow_stream_heartbeat_overlapping_anext_and_exhausted_aclose() -> None:
    """Verify AsyncPostgresArrowStream synchronous factory, non-blocking loop heartbeat, overlapping __anext__ InterfaceError, and exhausted aclose idempotence."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        custom_date_style=("Postgres, MDY", "America/New_York"),
    )

    async def _run(port: int, state: _SyncArrowPeerState) -> int:
        pool = AsyncPostgresPool(
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
        cur1: Any = None
        cur2: Any = None
        mv1: memoryview | None = None
        ticks = 0
        try:
            c1 = await pool.acquire()
            stream: Any = None
            try:
                pre_frames = state.post_startup_frames
                stream = c1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                assert not inspect.isawaitable(stream)
                assert isinstance(stream, native_pg.AsyncPostgresArrowStream)
                assert stream.__aiter__() is stream
                assert state.post_startup_frames == pre_frames

                cur1 = await anext(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 1
                assert state.events.count("Q:BEGIN") == 1
                assert len(state.execute_frames) == 1
                assert state.execute_frames[0][1] == 1

                mv1 = cur1.buffer_view()
                schema1, tbl1 = _decode_ipc_table(mv1)
                assert schema1.metadata[b"postgres.arrow.version"] == b"1"
                assert schema1.field("fn_col").metadata[b"postgres.encoding"] == b"regproc-text"
                assert schema1.field("ts_col").metadata[b"postgres.encoding"] == b"pg-timestamptz"
                assert tbl1.to_pylist() == [
                    {
                        "fn_col": "now",
                        "ts_col": {
                            "kind": 2,
                            "value": None,
                            "wire_text": "Sun Nov 18 11:59:59 1883 LMT",
                            "date_style": "Postgres, MDY",
                            "time_zone": "America/New_York",
                        },
                    }
                ]

                with pytest.raises(InterfaceError):
                    await c1.query("SELECT 1")
                with pytest.raises(InterfaceError):
                    await c1.select_to_arrow("SELECT 1")
                with pytest.raises(OperationalError):
                    await pool.acquire()

                stop_hb = False

                async def _heartbeat() -> None:
                    nonlocal ticks
                    while not stop_hb:
                        await asyncio.sleep(0.005)
                        ticks += 1

                hb_task = asyncio.create_task(_heartbeat())
                fetch2_task = asyncio.create_task(_as_coro(anext(stream)))
                try:
                    assert await asyncio.to_thread(state.batch2_waiting.wait, 2.0)
                    while ticks < 4:
                        await asyncio.sleep(0.005)
                    with pytest.raises(InterfaceError):
                        stream.__anext__()
                    state.allow_batch2.set()
                    cur2 = await fetch2_task
                finally:
                    stop_hb = True
                    await hb_task

                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1

                with pytest.raises(StopAsyncIteration):
                    await anext(stream)
                with pytest.raises(StopAsyncIteration):
                    await anext(stream)

                await stream.aclose()
                await stream.aclose()
            finally:
                if stream is not None:
                    with contextlib.suppress(Exception):
                        await stream.aclose()
                await c1.close()

            assert state.events.count("Q:BEGIN") == 1
            assert state.events.count("Q:COMMIT") == 1

            assert mv1 is not None
            _, tbl1_after = _decode_ipc_table(mv1)
            assert tbl1_after.to_pylist()[0]["fn_col"] == "now"
            _, tbl2_after = _decode_ipc_table(cur2.buffer_view())
            row2 = tbl2_after.to_pylist()[0]
            assert row2["fn_col"] == "pg_typeof"
            assert row2["ts_col"]["kind"] == 0
            assert isinstance(row2["ts_col"]["value"], int)

            c_check = await pool.acquire()
            try:
                res_check = await c_check.query("SELECT 1")
                assert res_check.rows == ((1,),)
            finally:
                await c_check.close()
            return ticks
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        assert asyncio.run(_run(port, state)) >= 4


def test_async_arrow_stream_aclose_lifecycle_mid_execute_8mib_and_ambient_savepoint() -> None:
    """Verify aclose() before first __anext__, during mid-Execute 8 MiB pause with retained memoryview/Table, and inside ambient savepoint scopes."""
    unopened_peer = _SyncArrowPeerState(portal_mode="scope_tracking")

    async def _exercise_unopened_and_resume(uport: int, ustate: _SyncArrowPeerState) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=uport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        try:
            baseline = ustate.post_startup_frames
            s_unopened = c.stream_arrow_batches("SELECT payload FROM items", batch_size=2)
            await s_unopened.aclose()
            await s_unopened.aclose()
            assert ustate.post_startup_frames == baseline
            with pytest.raises(InterfaceError):
                await anext(s_unopened)
            assert (await c.query("SELECT 1")).rows == ((1,),)
        finally:
            await c.close()

    with _running_sync_arrow_peer(unopened_peer) as (uport, ustate):
        asyncio.run(_exercise_unopened_and_resume(uport, ustate))

    resume_peer = _SyncArrowPeerState(portal_mode="big_resume")

    async def _exercise_resume(rport: int, rstate: _SyncArrowPeerState) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=rport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        b1: Any = None
        b2: Any = None
        try:
            async with c.stream_arrow_batches(
                "SELECT a, b FROM big_rows",
                batch_size=10,
                columns=["x", "x"],
            ) as stream:
                b1 = await anext(stream)
                assert b1.row_count == 2
                assert len(rstate.execute_frames) == 1
                rstate.allow_tail_complete.set()
                b2 = await anext(stream)
                assert b2.row_count == 1
                assert len(rstate.execute_frames) == 1
                sc2, tb2 = _decode_ipc_table(b2.buffer_view())
                assert [f.name for f in sc2] == ["x", "x"]
                assert tb2.column(0)[0].as_py() == "c" * (1792 * 1024)
                with pytest.raises(StopAsyncIteration):
                    await anext(stream)
        finally:
            if b1 is not None:
                b1.release()
            if b2 is not None:
                b2.release()
            await c.close()

    with _running_sync_arrow_peer(resume_peer) as (rport, rstate):
        asyncio.run(_exercise_resume(rport, rstate))
    assert rstate.events.count("Q:BEGIN") == 1
    assert rstate.events.count("Q:COMMIT") == 1

    mid_peer = _SyncArrowPeerState(portal_mode="big_stall_cancel")

    async def _exercise_mid_aclose(mport: int, mstate: _SyncArrowPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=mport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        cur1: Any = None
        mv1: memoryview | None = None
        retained_tbl: pa.Table | None = None
        tbl_after: pa.Table | None = None
        try:
            c1 = await pool.acquire()
            try:
                stream: Any = c1.stream_arrow_batches(
                    "SELECT a, b FROM stalled_big_rows",
                    batch_size=10,
                    columns=["x", "x"],
                )
                cur1 = await anext(stream)
                assert cur1.row_count == 2
                assert len(mstate.execute_frames) == 1
                mv1 = cur1.buffer_view()
                retained_tbl = pa.ipc.open_stream(pa.py_buffer(mv1)).read_all()

                close_aw = stream.aclose()
                with pytest.raises(InterfaceError):
                    stream.__anext__()
                await close_aw
                await stream.aclose()
                with pytest.raises(InterfaceError):
                    await anext(stream)
                del stream
            finally:
                await c1.close()

            assert mstate.events.count("Q:BEGIN") == 1
            assert mstate.cancels == [(9001, 77001)]
            assert mv1 is not None
            assert retained_tbl is not None
            assert bytes(mv1[:4]) != b""
            sc_after, tbl_after = _decode_ipc_table(mv1)
            assert [f.name for f in sc_after] == ["x", "x"]
            assert tbl_after.num_rows == 2
            assert retained_tbl.num_rows == 2
            assert len(retained_tbl.column(0)[0].as_py()) == 1792 * 1024
            assert retained_tbl.column(1)[0].as_py()[:4] == "yyyy"

            c2 = await pool.acquire()
            try:
                assert (await c2.query("SELECT 1")).rows == ((1,),)
            finally:
                await c2.close()
        finally:
            retained_tbl = None
            tbl_after = None
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            await pool.close()

    with _running_sync_arrow_peer(mid_peer) as (mport, mstate):
        asyncio.run(_exercise_mid_aclose(mport, mstate))

    amb_peer = _SyncArrowPeerState(portal_mode="scope_tracking")

    async def _exercise_ambient(aport: int) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=aport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        cur_amb: Any = None
        try:
            await c.begin()
            await c.query("INSERT INTO audit VALUES (1)")
            async with c.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_amb = await anext(stream)
                assert cur_amb.row_count == 1
            assert (await c.query("SELECT 1")).rows == ((1,),)
            await c.commit()
            assert (await c.query("SELECT 1")).rows == ((1,),)
        finally:
            if cur_amb is not None:
                cur_amb.release()
            await c.close()

    with _running_sync_arrow_peer(amb_peer) as (aport, astate):
        asyncio.run(_exercise_ambient(aport))

    assert astate.events.count("Q:BEGIN") == 1
    assert "EXEC:INSERT_AUDIT" in astate.events
    savepoints = [ev for ev in astate.events if ev.startswith("Q:SAVEPOINT vtb_sp_")]
    rollbacks_sp = [ev for ev in astate.events if ev.startswith("Q:ROLLBACK TO SAVEPOINT vtb_sp_")]
    releases_sp = [ev for ev in astate.events if ev.startswith("Q:RELEASE SAVEPOINT vtb_sp_")]
    assert len(savepoints) == 1
    assert len(rollbacks_sp) == 1
    assert len(releases_sp) == 1
    assert "Q:ROLLBACK" not in astate.events
    assert astate.events.count("Q:COMMIT") == 1


@pytest.mark.parametrize("failure_mode", ("columns_mismatch", "oversized_row"))
def test_async_arrow_stream_columns_mismatch_and_oversized_row_recover_pool(
    failure_mode: str,
) -> None:
    """Verify async stream_arrow_batches columns mismatch and >8 MiB row raise DataError, preserve zero leaked SHM, and recover pool."""
    portal_mode = "big_resume" if failure_mode == "columns_mismatch" else "oversized_row"
    peer_state = _SyncArrowPeerState(portal_mode=portal_mode)
    baseline_shm = _process_arrow_shm_files()

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = await pool.acquire()
            try:
                cols = ["only_one"] if failure_mode == "columns_mismatch" else None
                async with c1.stream_arrow_batches("SELECT a FROM rows", batch_size=4, columns=cols) as stream:
                    with pytest.raises(DataError):
                        await anext(stream)
                    with pytest.raises(InterfaceError):
                        await anext(stream)
                if failure_mode == "columns_mismatch":
                    assert "Q:ROLLBACK" in state.events
            finally:
                await c1.close()

            c2 = await pool.acquire()
            try:
                assert (await c2.query("SELECT 1")).rows == ((1,),)
            finally:
                await c2.close()
        finally:
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))
    assert _process_arrow_shm_files() - baseline_shm == set()


@pytest.mark.parametrize(
    "cancel_stage",
    ("open_bind", "execute_in_flight", "queued_task_cancel", "queued_future_drop"),
)
def test_async_arrow_stream_cancellation_open_execute_and_queued_drainer_zero_shm_leak(
    cancel_stage: str,
) -> None:
    """Verify task cancellation during Bind, in-flight Execute, and paused completion-drainer publication leaks 0 SHM files or pool leases."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        hold_portal_bind=(cancel_stage == "open_bind"),
        stall_execute_until_cancel=(cancel_stage == "execute_in_flight"),
    )
    baseline_shm = _process_arrow_shm_files()

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        loop = asyncio.get_running_loop()
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await pool.acquire()
            try:
                stream = ac1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                if cancel_stage == "open_bind":
                    t_open = asyncio.create_task(_as_coro(anext(stream)))
                    assert await asyncio.to_thread(state.bind_waiting.wait, 2.0)
                    t_open.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_open
                elif cancel_stage == "execute_in_flight":
                    cur1 = await anext(stream)
                    cur1.release()
                    t_exec = asyncio.create_task(_as_coro(anext(stream)))
                    assert await asyncio.to_thread(state.batch2_waiting.wait, 2.0)
                    t_exec.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_exec
                elif cancel_stage == "queued_task_cancel":
                    drainer = getattr(loop, "_vertebra_postgres_completion_drainer", None)
                    assert drainer is not None
                    drainer_fd = _find_loop_drainer_fd(loop, drainer)
                    loop.remove_reader(drainer_fd)
                    reader_restored = False
                    try:
                        t_queued = asyncio.create_task(_as_coro(anext(stream)))
                        await asyncio.sleep(0)
                        assert await asyncio.to_thread(state.batch1_sent.wait, 2.0)
                        ready, _, _ = await asyncio.to_thread(select.select, [drainer_fd], [], [], 2.0)
                        assert ready == [drainer_fd]
                        assert len(_process_arrow_shm_files() - baseline_shm) == 1

                        t_queued.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await t_queued
                        assert _process_arrow_shm_files() - baseline_shm == set()

                        loop.add_reader(drainer_fd, drainer)
                        reader_restored = True
                        await asyncio.sleep(0.01)
                    finally:
                        if not reader_restored:
                            loop.add_reader(drainer_fd, drainer)
                else:
                    drainer = getattr(loop, "_vertebra_postgres_completion_drainer", None)
                    assert drainer is not None
                    drainer_fd = _find_loop_drainer_fd(loop, drainer)
                    loop.remove_reader(drainer_fd)
                    reader_restored = False
                    aw = stream.__anext__()
                    await_iter = aw.__await__()
                    try:
                        py_fut = next(await_iter)
                        assert await asyncio.to_thread(state.batch1_sent.wait, 2.0)
                        ready, _, _ = await asyncio.to_thread(select.select, [drainer_fd], [], [], 2.0)
                        assert ready == [drainer_fd]
                        assert len(_process_arrow_shm_files() - baseline_shm) == 1

                        py_fut.cancel()
                        drainer()
                        assert _process_arrow_shm_files() - baseline_shm == set()

                        loop.add_reader(drainer_fd, drainer)
                        reader_restored = True
                    finally:
                        await_iter.close()
                        if not reader_restored:
                            loop.add_reader(drainer_fd, drainer)

                with pytest.raises(InterfaceError):
                    await anext(stream)
            finally:
                await ac1.close()

            assert _process_arrow_shm_files() - baseline_shm == set()

            ac2 = await pool.acquire()
            try:
                assert (await ac2.query("SELECT 1")).rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))

    assert _process_arrow_shm_files() - baseline_shm == set()
    if cancel_stage in ("open_bind", "execute_in_flight"):
        assert len(state.cancels) >= 1
    else:
        assert "Q:ROLLBACK" in state.events
        assert any(ev.startswith("Close(P:") for ev in state.events)
