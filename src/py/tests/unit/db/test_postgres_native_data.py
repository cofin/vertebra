"""Unit tests for native PostgreSQL Arrow results, COPY handles, and streaming Arrow import."""

from __future__ import annotations

import ast
import asyncio
import contextlib
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
