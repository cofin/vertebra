"""Shared scripted PostgreSQL Protocol 3.0 wire peers and Arrow/COPY helpers for unit tests."""

from __future__ import annotations

import contextlib
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

from vertebra.db.adapters.postgres.core import PGCOPY_BINARY_SIGNATURE, PostgreSQLCore
from vertebra.db.arrow import ArrowCursor

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence

    import pytest

_CANCEL_REQUEST_CODE = 80877102
_NAME_OID = 19
_INT2_OID = 21
_REGPROC_OID = 24
_INT4_OID = 23
_TEXT_OID = 25
_OID_OID = 26
_TIMESTAMPTZ_OID = 1184
_CUSTOM_VECTOR_OID = 42001
_CUSTOM_ENUM_OID = 42002
_CUSTOM_DOMAIN_OID = 42003
_CUSTOM_RANGE_OID = 42004
_CUSTOM_ENUM_ARRAY_OID = 42005
_PGCOPY_HEADER = PGCOPY_BINARY_SIGNATURE + struct.pack("!II", 0, 0)
_PGCOPY_TRAILER = b"\xff\xff"


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
    copy_data_stalled: threading.Event = field(default_factory=threading.Event)
    copy_fail_entered: threading.Event = field(default_factory=threading.Event)
    finish_entered: threading.Event = field(default_factory=threading.Event)
    commit_entered: threading.Event = field(default_factory=threading.Event)
    release_finish: threading.Event = field(default_factory=threading.Event)
    abort_sync: threading.Event = field(default_factory=threading.Event)
    allow_cancel_reply: threading.Event = field(default_factory=threading.Event)
    client_disconnected: threading.Event = field(default_factory=threading.Event)
    stall_first_preflight: bool = False
    stall_first_copy_in: bool = False
    stall_copy_out_after_row1: bool = False
    stall_copy_data_after_frames: int | None = None
    hold_finish_until_released: bool = False
    hold_commit_until_released: bool = False
    hold_copy_fail_until_cancel: bool = False
    wait_for_abort_sync: bool = False
    hold_copy_out_cancel_reply: bool = False
    ignore_cancel_request: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    finish_cond: threading.Condition = field(init=False)
    _release_pipe: tuple[int, int] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Initialize condition variable bound to state.lock."""
        self.finish_cond = threading.Condition(self.lock)

    def signal_release_finish(self) -> None:
        """Set release_finish and wake both Condition and poll() pipe waiters."""
        with self.finish_cond:
            self.release_finish.set()
            pipe = self._release_pipe
            self.finish_cond.notify_all()
        if pipe is not None:
            with contextlib.suppress(OSError):
                os.write(pipe[1], b"1")


def _wait_release_or_disconnect(conn: socket.socket, state: _CopyPeerState, timeout: float = 5.0) -> None:
    """Wait until release_finish is signaled or the client closes the TCP socket."""
    if state.release_finish.is_set():
        return
    pipe = state._release_pipe
    if pipe is None:
        state.release_finish.wait(timeout=timeout)
        return
    poller = select.poll()
    poll_events = getattr(select, "POLLRDHUP", 0) | select.POLLHUP | select.POLLERR
    poller.register(conn.fileno(), poll_events)
    poller.register(pipe[0], select.POLLIN)
    events = poller.poll(int(timeout * 1000))
    for fd, mask in events:
        if fd == conn.fileno() and (mask & poll_events):
            state.client_disconnected.set()
            return


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
            with state.finish_cond:
                state.cancels.append(cancel_key)
                state.events.append(f"CANCEL:{cancel_key[0]}")
                state.cancel_received.set()
                state.finish_cond.notify_all()
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
                    if sql == "COMMIT":
                        should_hold_commit = False
                        with state.lock:
                            if state.hold_commit_until_released:
                                state.hold_commit_until_released = False
                                should_hold_commit = True
                        if should_hold_commit:
                            state.commit_entered.set()
                            if state.ignore_cancel_request:
                                _wait_release_or_disconnect(conn, state, timeout=5.0)
                                return
                    in_tx = False
                    tx_failed = False
                    conn.sendall(_pg_frame(b"C", sql.encode("ascii") + b"\x00") + _pg_frame(b"Z", b"I"))
                    continue
                if sql.startswith("COPY ") and "FROM STDIN" in sql.upper():
                    was_ambient = in_tx
                    ncols = _count_quoted_copy_cols(sql)
                    should_stall_start = False
                    stall_data_limit: int | None = None
                    with state.lock:
                        if state.stall_first_copy_in:
                            state.stall_first_copy_in = False
                            should_stall_start = True
                        if state.stall_copy_data_after_frames is not None:
                            stall_data_limit = state.stall_copy_data_after_frames
                            state.stall_copy_data_after_frames = None
                    if should_stall_start:
                        state.copy_in_entered.set()
                        state.cancel_received.wait(timeout=3.0)
                    conn.sendall(_pg_copy_in_binary(ncols))
                    state.copy_in_ready.set()
                    if stall_data_limit == 0:
                        state.copy_data_stalled.set()
                        _wait_release_or_disconnect(conn, state, timeout=5.0)
                        return
                    copy_buf = bytearray()
                    copy_frames_seen = 0
                    while True:
                        try:
                            ctag, cbody = _read_frame(conn)
                        except (EOFError, OSError):
                            return
                        if ctag == b"d":
                            copy_buf.extend(cbody)
                            copy_frames_seen += 1
                            if len(copy_buf) > len(_PGCOPY_HEADER):
                                state.first_tuple_on_wire.set()
                            if stall_data_limit is not None and copy_frames_seen >= stall_data_limit:
                                state.copy_data_stalled.set()
                                _wait_release_or_disconnect(conn, state, timeout=5.0)
                                return
                        elif ctag == b"c":
                            if state.hold_finish_until_released:
                                state.finish_entered.set()
                                if state.ignore_cancel_request:
                                    with state.lock:
                                        state.hold_finish_until_released = False
                                    _wait_release_or_disconnect(conn, state, timeout=5.0)
                                    return
                                with state.finish_cond:
                                    state.finish_cond.wait_for(
                                        lambda: state.release_finish.is_set() or state.cancel_received.is_set(),
                                        timeout=5.0,
                                    )
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
                            hold_fail = False
                            with state.lock:
                                state.aborted_copies.append((sql, bytes(copy_buf), reason))
                                state.events.append(f"COPY_FAIL:{reason}")
                                if state.hold_copy_fail_until_cancel:
                                    state.hold_copy_fail_until_cancel = False
                                    hold_fail = True
                            state.copy_fail_entered.set()
                            if hold_fail:
                                state.cancel_received.wait(timeout=2.0)
                                if state.wait_for_abort_sync:
                                    state.abort_sync.wait(timeout=2.0)
                            if in_tx:
                                tx_failed = True
                            tx_byte = b"E" if tx_failed else (b"T" if in_tx else b"I")
                            with contextlib.suppress(OSError):
                                conn.sendall(_pg_error("57014", reason) + _pg_frame(b"Z", tx_byte))
                            break
                    continue

                if sql.startswith("COPY ") and "TO STDOUT" in sql.upper():
                    rows_out = list(state.copy_out_rows)
                    ncols = len(rows_out[0]) if rows_out else 1
                    conn.sendall(_pg_copy_out_binary(ncols))
                    should_stall_out = False
                    with state.lock:
                        if state.stall_copy_out_after_row1 and rows_out:
                            should_stall_out = True
                    if should_stall_out:
                        first_chunk = _PGCOPY_HEADER + _encode_binary_tuple(rows_out[0])
                        conn.sendall(_pg_frame(b"d", first_chunk))
                        state.copy_out_row1_sent.set()
                        cancelled = state.cancel_received.wait(timeout=3.0)
                        if cancelled and state.hold_copy_out_cancel_reply:
                            state.allow_cancel_reply.wait(timeout=2.0)
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
    pipe = os.pipe()
    peer_state._release_pipe = pipe
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if peer_state.stall_copy_data_after_frames is not None:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
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
        with peer_state.finish_cond:
            peer_state.cancel_received.set()
            peer_state.abort_sync.set()
            peer_state.allow_cancel_reply.set()
            peer_state.finish_cond.notify_all()
        peer_state.signal_release_finish()
        with contextlib.suppress(OSError):
            srv.close()
        accept_thread.join(timeout=3.0)
        for worker in workers:
            worker.join(timeout=3.0)
        peer_state._release_pipe = None
        for fd in pipe:
            with contextlib.suppress(OSError):
                os.close(fd)


async def _as_coro(awaitable: Awaitable[Any], started: asyncio.Event | None = None) -> Any:
    """Wrap a native TaskAwaitable inside a Python coroutine for asyncio.create_task."""
    if started is not None:
        started.set()
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


def _single_row_arrow_ipc(row_id: int = 1, label: str = "seed") -> bytes:
    """Serialize a 1-row `(id: int32, label: utf8)` RecordBatch into an Arrow IPC stream."""
    schema = pa.schema([("id", pa.int32()), ("label", pa.string())])
    batch = pa.record_batch(
        [pa.array([row_id], type=pa.int32()), pa.array([label], type=pa.string())],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    return bytes(sink.getvalue())


def _large_arrow_ipc_batch() -> bytes:
    """Build a 12 MiB (`<= 16 MiB`) multi-batch Arrow IPC stream with each batch `<= 8 MiB` to stall TCP send buffers."""
    row_text = "w" * 32768
    row_count = 64
    schema = pa.schema([("id", pa.int32()), ("label", pa.string())])
    unit_batch = pa.record_batch(
        [
            pa.array(list(range(row_count)), type=pa.int32()),
            pa.array([row_text] * row_count, type=pa.string()),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(unit_batch)
        writer.write_batch(unit_batch)
        writer.write_batch(unit_batch)
        writer.write_batch(unit_batch)
        writer.write_batch(unit_batch)
        writer.write_batch(unit_batch)
    return bytes(sink.getvalue())


def _wrap_ipc_buffer(raw: bytes, kind: str) -> tuple[Any, Any]:
    """Wrap raw Arrow IPC bytes into a read-only or writable buffer exporter returning (anchor, buf)."""
    if kind == "bytes":
        return raw, raw
    if kind == "bytearray":
        ba = bytearray(raw)
        return ba, ba
    if kind == "writable_mv":
        ba = bytearray(raw)
        return ba, memoryview(ba)
    if kind == "sliced_writable_mv":
        padded = bytearray(b"HEAD" + raw + b"TAIL")
        full_mv = memoryview(padded)
        return (padded, full_mv), full_mv[4 : 4 + len(raw)]
    if kind == "readonly_mv":
        return raw, memoryview(raw)
    if kind == "writable_pa_buffer":
        ba = bytearray(raw)
        return ba, pa.py_buffer(ba)
    msg = f"unexpected buffer kind {kind!r}"
    raise AssertionError(msg)
