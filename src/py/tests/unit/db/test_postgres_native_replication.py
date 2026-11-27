"""Unit and scripted wire-protocol tests for AsyncPostgresReplication and client replication entrypoints."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import socket
import struct
import threading
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    AsyncPostgresReplication,
)
from vertebra.db import (
    DataError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
)
from vertebra.db.adapters.postgres.types import (
    PG_UNCHANGED_TOAST,
    PostgresReplicationIdentity,
    PostgresReplicationRecord,
)


async def _as_coro(awaitable: Awaitable[Any], started: asyncio.Event | None = None) -> Any:
    """Wrap a native TaskAwaitable inside a Python coroutine for asyncio.create_task."""
    if started is not None:
        started.set()
    return await awaitable


def _pg_frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 backend wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _pg_auth_ready(pid: int) -> bytes:
    """Encode AuthenticationOk, ParameterStatus headers, BackendKeyData, and ReadyForQuery('I')."""
    return b"".join(
        (
            _pg_frame(b"R", struct.pack("!i", 0)),
            _pg_frame(b"S", b"server_version\x0016.4\x00"),
            _pg_frame(b"S", b"client_encoding\x00UTF8\x00"),
            _pg_frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _pg_frame(b"S", b"TimeZone\x00UTC\x00"),
            _pg_frame(b"K", struct.pack("!ii", pid, 60_000 + pid)),
            _pg_frame(b"Z", b"I"),
        )
    )


def _pg_error(severity: str, sqlstate: str, message: str) -> bytes:
    """Encode a PostgreSQL ErrorResponse ('E') backend frame."""
    body = (
        b"S"
        + severity.encode("ascii")
        + b"\x00V"
        + severity.encode("ascii")
        + b"\x00C"
        + sqlstate.encode("ascii")
        + b"\x00M"
        + message.encode("utf-8")
        + b"\x00\x00"
    )
    return _pg_frame(b"E", body)


def _pg_text_row_description(columns: list[tuple[str, int]]) -> bytes:
    """Encode a PostgreSQL RowDescription ('T') frame with text-format columns."""
    out = bytearray(struct.pack("!h", len(columns)))
    for name, oid in columns:
        out.extend(name.encode("utf-8") + b"\x00")
        out.extend(struct.pack("!ihihih", 0, 0, oid, -1, -1, 0))
    return _pg_frame(b"T", bytes(out))


def _pg_data_row(cells: list[str | None]) -> bytes:
    """Encode a PostgreSQL DataRow ('D') frame using text cells and -1 for NULL."""
    out = bytearray(struct.pack("!h", len(cells)))
    for cell in cells:
        if cell is None:
            out.extend(struct.pack("!i", -1))
        else:
            raw = cell.encode("utf-8")
            out.extend(struct.pack("!i", len(raw)) + raw)
    return _pg_frame(b"D", bytes(out))


def _pg_extended_select(
    columns: list[tuple[str, int]],
    rows: list[list[str | None]],
    *,
    tag: str = "SELECT 1",
    has_parse: bool = True,
    has_describe_stmt: bool = True,
    has_describe_portal: bool = False,
    has_close_portal: bool = False,
) -> bytes:
    """Encode ParseComplete/ParameterDescription/RowDescription/BindComplete/DataRow/CommandComplete/CloseComplete/ReadyForQuery."""
    out = bytearray()
    if has_parse:
        out.extend(_pg_frame(b"1", b""))
    if has_describe_stmt:
        out.extend(_pg_frame(b"t", struct.pack("!hi", 1, 25)))
        out.extend(_pg_text_row_description(columns))
    out.extend(_pg_frame(b"2", b""))
    if has_describe_portal:
        out.extend(_pg_text_row_description(columns))
    for row in rows:
        out.extend(_pg_data_row(row))
    out.extend(_pg_frame(b"C", tag.encode("utf-8") + b"\x00"))
    if has_close_portal:
        out.extend(_pg_frame(b"3", b""))
    out.extend(_pg_frame(b"Z", b"I"))
    return bytes(out)


def _pg_identify_system(
    systemid: str,
    timeline: str,
    xlogpos: str,
    dbname: str | None,
) -> bytes:
    """Encode a full IDENTIFY_SYSTEM RowDescription + DataRow + CommandComplete + ReadyForQuery reply."""
    cols = [("systemid", 25), ("timeline", 23), ("xlogpos", 25), ("dbname", 25)]
    return b"".join(
        (
            _pg_text_row_description(cols),
            _pg_data_row([systemid, timeline, xlogpos, dbname]),
            _pg_frame(b"C", b"IDENTIFY_SYSTEM\x00"),
            _pg_frame(b"Z", b"I"),
        )
    )


def _pg_xlog_data(lsn: int, payload: bytes) -> bytes:
    """Wrap one pgoutput message payload inside a CopyData('d') XLogData('w') wire frame."""
    header = struct.pack("!BQQq", ord("w"), lsn, lsn, 0)
    return _pg_frame(b"d", header + payload)


def _pgoutput_relation(
    relation_oid: int,
    namespace: str,
    name: str,
    replica_identity: bytes,
    columns: list[tuple[bool, str, int, int]],
) -> bytes:
    """Encode a pgoutput v1 Relation ('R') payload."""
    out = bytearray()
    out.extend(b"R")
    out.extend(struct.pack("!I", relation_oid))
    out.extend(namespace.encode("utf-8") + b"\x00")
    out.extend(name.encode("utf-8") + b"\x00")
    out.extend(replica_identity)
    out.extend(struct.pack("!H", len(columns)))
    for is_key, col_name, type_oid, type_mod in columns:
        out.extend(struct.pack("!B", 1 if is_key else 0))
        out.extend(col_name.encode("utf-8") + b"\x00")
        out.extend(struct.pack("!Ii", type_oid, type_mod))
    return bytes(out)


def _pgoutput_tuple(cells: list[tuple[str, bytes | None]]) -> bytes:
    """Encode pgoutput TupleData cells tagged with 'n', 'u', 't', or 'b'."""
    out = bytearray(struct.pack("!H", len(cells)))
    for kind, data in cells:
        if kind == "n":
            out.extend(b"n")
        elif kind == "u":
            out.extend(b"u")
        else:
            assert data is not None
            out.extend(kind.encode("ascii"))
            out.extend(struct.pack("!i", len(data)) + data)
    return bytes(out)


def _pgoutput_begin(final_lsn: int, commit_ts_us: int, xid: int) -> bytes:
    """Encode a pgoutput Begin ('B') payload."""
    return b"B" + struct.pack("!QqI", final_lsn, commit_ts_us, xid)


def _pgoutput_insert(relation_oid: int, cells: list[tuple[str, bytes | None]]) -> bytes:
    """Encode a pgoutput Insert ('I') payload."""
    return b"I" + struct.pack("!IB", relation_oid, ord("N")) + _pgoutput_tuple(cells)


def _pgoutput_update(
    relation_oid: int,
    old_kind: bytes | None,
    old_cells: list[tuple[str, bytes | None]] | None,
    new_cells: list[tuple[str, bytes | None]],
) -> bytes:
    """Encode a pgoutput Update ('U') payload with optional key/full old tuple."""
    out = bytearray(b"U" + struct.pack("!I", relation_oid))
    if old_kind is not None and old_cells is not None:
        out.extend(old_kind)
        out.extend(_pgoutput_tuple(old_cells))
    out.extend(b"N")
    out.extend(_pgoutput_tuple(new_cells))
    return bytes(out)


def _pgoutput_delete(
    relation_oid: int,
    old_kind: bytes,
    old_cells: list[tuple[str, bytes | None]],
) -> bytes:
    """Encode a pgoutput Delete ('D') payload."""
    return b"D" + struct.pack("!I", relation_oid) + old_kind + _pgoutput_tuple(old_cells)


def _pgoutput_truncate(relation_oids: list[int], *, cascade: bool, restart_identity: bool) -> bytes:
    """Encode a pgoutput Truncate ('T') payload."""
    options = (1 if cascade else 0) | (2 if restart_identity else 0)
    out = bytearray(b"T" + struct.pack("!IB", len(relation_oids), options))
    for oid in relation_oids:
        out.extend(struct.pack("!I", oid))
    return bytes(out)


def _pgoutput_message(
    *,
    transactional: bool,
    lsn: int,
    prefix: str,
    content: bytes,
) -> bytes:
    """Encode a pgoutput Message ('M') payload."""
    return (
        b"M"
        + struct.pack("!BQ", 1 if transactional else 0, lsn)
        + prefix.encode("utf-8")
        + b"\x00"
        + struct.pack("!i", len(content))
        + content
    )


def _pgoutput_commit(flags: int, commit_lsn: int, commit_end_lsn: int, commit_ts_us: int) -> bytes:
    """Encode a pgoutput Commit ('C') payload."""
    return b"C" + struct.pack("!BQQq", flags, commit_lsn, commit_end_lsn, commit_ts_us)


class _DrainerController:
    """Deterministic pause/inspect/resume controller for loop._vertebra_postgres_completion_drainer."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Install add_reader wrapper on loop to capture the Postgres completion drainer fd."""
        self._loop = loop
        self._fd: int | None = None
        self._callback: Callable[..., Any] | None = None
        self._args: tuple[Any, ...] = ()
        self._paused = False
        orig_add_reader = loop.add_reader

        def _wrapped_add_reader(fd: Any, callback: Callable[..., Any], *args: Any) -> None:
            if type(callback).__name__ in ("_DbCompletionDrainer", "_RustCompletionDrainer") and self._fd is None:
                self._fd = int(fd)
                self._callback = callback
                self._args = args
            orig_add_reader(fd, callback, *args)

        self._orig_add_reader = orig_add_reader
        loop_any: Any = loop
        loop_any.add_reader = _wrapped_add_reader

    def pause(self) -> None:
        """Detach the completion drainer fd from the event loop selector while keeping the ring live."""
        assert self._fd is not None
        if not self._paused:
            self._loop.remove_reader(self._fd)
            self._paused = True

    def resume(self) -> None:
        """Re-register the completion drainer fd on the event loop selector if paused."""
        if self._paused and self._fd is not None and self._callback is not None:
            self._orig_add_reader(self._fd, self._callback, *self._args)
            self._paused = False

    def drain_once(self) -> None:
        """Synchronously invoke the captured _DbCompletionDrainer callback once."""
        assert self._callback is not None
        self._callback(*self._args)

    async def wait_until_signaled(self, *, timeout: float) -> bool:
        """Await readability of the paused completion eventfd without consuming it."""
        if self._fd is None:
            return False
        ready = asyncio.Event()
        fd = self._fd

        def _on_readable() -> None:
            self._loop.remove_reader(fd)
            ready.set()

        self._orig_add_reader(fd, _on_readable)
        try:
            await asyncio.wait_for(ready.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False
        finally:
            self._loop.remove_reader(fd)


@dataclass
class _ReplicationPeerSpec:
    """Configurable scripted backend state for logical replication protocol sessions."""

    system_id: str = "7399000000000001"
    timeline: str = "1"
    xlogpos: str = "2/500"
    database: str = "appdb"
    slot_name: str = "vtb_slot"
    publication_name: str = "vtb_pub"
    restart_lsn: str = "1/100"
    confirmed_flush_lsn: str = "1/200"
    active: str = "f"
    pub_rows: list[list[str | None]] = field(
        default_factory=lambda: [["vtb_pub", "f", "t", "t", "t", "f", "f", "public", "items", None, None]]
    )
    wal_chunks: list[bytes] = field(default_factory=list)
    start_error: tuple[str, str] | None = None
    hang_before_identify_reply: threading.Event | None = None


class _ReplicationSession:
    """Single accepted backend TCP connection on _ScriptedReplicationServer."""

    def __init__(
        self,
        conn: socket.socket,
        startup_params: dict[str, str],
        on_close: Callable[[], None] | None = None,
    ) -> None:
        """Store startup parameters and initialize synchronization primitives."""
        self._conn = conn
        self.startup_params = startup_params
        self._on_close = on_close
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._closed = False
        self._ack_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
        self._close_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
        self.simple_queries: list[str] = []
        self.standby_acks: list[int] = []
        self.saw_copy_done = False

    @property
    def is_closed(self) -> bool:
        """Return True once the session socket handler has exited."""
        with self._state_lock:
            return self._closed

    def send_raw(self, data: bytes) -> None:
        """Write raw backend frames synchronously under the write lock."""
        with self._write_lock, contextlib.suppress(OSError):
            self._conn.sendall(data)

    def push_xlog(self, lsn: int, payload: bytes) -> None:
        """Push one CopyData XLogData('w') frame over the active CopyBoth stream."""
        self.send_raw(_pg_xlog_data(lsn, payload))

    def close_from_peer(self) -> None:
        """Forcefully shut down and close the backend TCP socket."""
        with self._write_lock:
            with contextlib.suppress(OSError):
                self._conn.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                self._conn.close()

    def _record_standby_ack(self, flush_lsn: int) -> None:
        """Record one StandbyStatusUpdate ack and wake matching async waiters."""
        with self._state_lock:
            self.standby_acks.append(flush_lsn)
            ack_count = len(self.standby_acks)
            if self._ack_waiters:
                remaining: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
                for loop, target, ev in self._ack_waiters:
                    if ack_count >= target:
                        loop.call_soon_threadsafe(ev.set)
                    else:
                        remaining.append((loop, target, ev))
                self._ack_waiters = remaining

    async def wait_for_ack_count_async(self, count: int, *, timeout: float) -> bool:
        """Await at least count StandbyStatusUpdate acks on this session."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._state_lock:
            if len(self.standby_acks) >= count:
                return True
            self._ack_waiters.append((loop, count, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._state_lock:
                self._ack_waiters = [item for item in self._ack_waiters if item[2] is not ev]
                return len(self.standby_acks) >= count

    async def wait_closed_async(self, *, timeout: float) -> bool:
        """Await session closure on the server without polling."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._state_lock:
            if self._closed:
                return True
            self._close_waiters.append((loop, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._state_lock:
                self._close_waiters = [item for item in self._close_waiters if item[1] is not ev]
                return self._closed

    def _mark_closed(self) -> None:
        """Mark session closed after reader exit."""
        with self._state_lock:
            self._closed = True
            for loop, ev in self._close_waiters:
                loop.call_soon_threadsafe(ev.set)
            self._close_waiters.clear()
        if self._on_close is not None:
            self._on_close()


class _ScriptedReplicationServer:
    """Multi-generation scripted PostgreSQL Protocol 3.0 replication server."""

    def __init__(
        self,
        spec_factory: Callable[[int, dict[str, str]], _ReplicationPeerSpec],
    ) -> None:
        """Bind loopback socket and spawn background accept thread."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.05)
        self.port: int = self._sock.getsockname()[1]
        self._spec_factory = spec_factory
        self._cond = threading.Condition()
        self._stop = threading.Event()
        self._accept_count = 0
        self._sessions: list[_ReplicationSession] = []
        self._workers: list[threading.Thread] = []
        self._accept_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    @property
    def accept_count(self) -> int:
        """Return total accepted TCP connections."""
        with self._cond:
            return self._accept_count

    def session(self, index: int) -> _ReplicationSession:
        """Return accepted session by index."""
        with self._cond:
            return self._sessions[index]

    def session_or_none(self, index: int) -> _ReplicationSession | None:
        """Return accepted session by index when startup completed, or None if dropped early."""
        with self._cond:
            return self._sessions[index] if index < len(self._sessions) else None

    def _notify_state_locked(self) -> None:
        """Notify Condition and async waiters under self._cond."""
        self._cond.notify_all()
        if self._accept_waiters:
            remaining: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
            for loop, count, ev in self._accept_waiters:
                if self._accept_count >= count:
                    loop.call_soon_threadsafe(ev.set)
                else:
                    remaining.append((loop, count, ev))
            self._accept_waiters = remaining

    def _on_session_closed(self) -> None:
        """Wake live-connection waiters when a backend session terminates."""
        with self._cond:
            self._notify_state_locked()

    async def wait_for_accept_count_async(self, count: int, *, timeout: float) -> bool:
        """Wait asynchronously until at least count TCP sockets have been accepted."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._cond:
            if self._accept_count >= count:
                return True
            self._accept_waiters.append((loop, count, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._cond:
                self._accept_waiters = [item for item in self._accept_waiters if item[2] is not ev]
                return self._accept_count >= count

    def wait_for_live_connections(self, expected: int, *, timeout: float) -> bool:
        """Wait synchronously until live connection count matches expected."""
        with self._cond:
            return self._cond.wait_for(
                lambda: sum(0 if s.is_closed else 1 for s in self._sessions) == expected,
                timeout=timeout,
            )

    def stop(self) -> None:
        """Stop listener socket and close active backend connections."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._sock.close()
        with self._cond:
            sessions = list(self._sessions)
            workers = list(self._workers)
        for sess in sessions:
            sess.close_from_peer()
        self._accept_thread.join(timeout=2.0)
        for worker in workers:
            worker.join(timeout=2.0)

    def _accept_loop(self) -> None:
        """Accept connections until stopped."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            conn.settimeout(0.2)
            with self._cond:
                self._accept_count += 1
                idx = self._accept_count
                self._notify_state_locked()
            t = threading.Thread(target=self._serve, args=(conn, idx), daemon=True)
            with self._cond:
                self._workers.append(t)
            t.start()

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> bytes | None:
        """Read exact byte count or return None on EOF."""
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = conn.recv(n - len(buf))
            except TimeoutError:
                return None
            except OSError:
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    @classmethod
    def _recv_exact_poll(cls, conn: socket.socket, n: int, stop: threading.Event) -> bytes | None:
        """Read exact byte count polling socket timeouts until stop or EOF."""
        buf = bytearray()
        while len(buf) < n and not stop.is_set():
            try:
                chunk = conn.recv(n - len(buf))
            except TimeoutError:
                continue
            except OSError:
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf) if len(buf) == n else None

    @classmethod
    def _read_startup(cls, conn: socket.socket) -> dict[str, str] | None:
        """Read optional SSLRequest ('N') and PostgreSQL 3.0 StartupMessage."""
        hdr = cls._recv_exact(conn, 8)
        if hdr is None:
            return None
        length, code = struct.unpack("!ii", hdr)
        if length == 8 and code == 80877103:
            conn.sendall(b"N")
            hdr = cls._recv_exact(conn, 8)
            if hdr is None:
                return None
            length, code = struct.unpack("!ii", hdr)
        if code != 196608 or length < 8:
            return None
        body = cls._recv_exact(conn, length - 8)
        if body is None:
            return None
        parts = body.rstrip(b"\x00").split(b"\x00")
        params: dict[str, str] = {}
        for i in range(0, len(parts) - 1, 2):
            params[parts[i].decode("utf-8", errors="replace")] = parts[i + 1].decode("utf-8", errors="replace")
        return params

    def _serve(self, conn: socket.socket, idx: int) -> None:
        """Serve catalog preflight queries or CopyBoth replication on a single TCP connection."""
        params = self._read_startup(conn)
        if params is None:
            with contextlib.suppress(OSError):
                conn.close()
            return
        spec = self._spec_factory(idx, params)
        sess = _ReplicationSession(conn, params, on_close=self._on_session_closed)
        with self._cond:
            self._sessions.append(sess)
            self._notify_state_locked()
        try:
            sess.send_raw(_pg_auth_ready(9000 + idx))
            pending_ext: list[tuple[bytes, bytes]] = []
            while not self._stop.is_set():
                hdr = self._recv_exact_poll(conn, 5, self._stop)
                if hdr is None:
                    break
                tag = hdr[:1]
                (length,) = struct.unpack("!i", hdr[1:])
                body = self._recv_exact_poll(conn, length - 4, self._stop) if length > 4 else b""
                if body is None:
                    break
                if tag == b"X":
                    break
                if tag in (b"P", b"B", b"D", b"E", b"C"):
                    pending_ext.append((tag, body))
                    continue
                if tag == b"S":
                    self._handle_extended(sess, spec, pending_ext)
                    pending_ext.clear()
                    continue
                if tag == b"Q":
                    sql = body[:-1].decode("utf-8", errors="replace")
                    with sess._state_lock:
                        sess.simple_queries.append(sql)
                    self._handle_simple(sess, spec, sql)
                    continue
                if tag == b"d" and body[:1] == b"r" and len(body) >= 34:
                    (_, _, flush_lsn, _) = struct.unpack_from("!BQQQ", body, 0)
                    sess._record_standby_ack(flush_lsn)
                    continue
                if tag == b"c":
                    with sess._state_lock:
                        sess.saw_copy_done = True
                    sess.send_raw(_pg_frame(b"c", b"") + _pg_frame(b"C", b"COPY 0\x00") + _pg_frame(b"Z", b"I"))
                    continue
        finally:
            sess._mark_closed()
            with contextlib.suppress(OSError):
                conn.close()

    def _handle_extended(
        self,
        sess: _ReplicationSession,
        spec: _ReplicationPeerSpec,
        frames: list[tuple[bytes, bytes]],
    ) -> None:
        """Respond to pg_replication_slots, pg_publication, and custom-type preload extended queries."""
        sql = ""
        has_parse = False
        has_describe_stmt = False
        has_describe_portal = False
        has_close_portal = False
        for tag, body in frames:
            if tag == b"P":
                has_parse = True
                stmt_end = body.index(b"\x00")
                sql_end = body.index(b"\x00", stmt_end + 1)
                sql = body[stmt_end + 1 : sql_end].decode("utf-8", errors="replace")
            elif tag == b"D" and body[:1] == b"S":
                has_describe_stmt = True
            elif tag == b"D" and body[:1] == b"P":
                has_describe_portal = True
            elif tag == b"C" and body[:1] == b"P":
                has_close_portal = True

        rows: list[list[str | None]]
        if "pg_replication_slots" in sql:
            cols = [
                ("current_database", 19),
                ("database", 19),
                ("slot_type", 25),
                ("plugin", 19),
                ("active", 16),
                ("restart_lsn", 25),
                ("confirmed_flush_lsn", 25),
                ("wal_status", 25),
            ]
            rows = [
                [
                    spec.database,
                    spec.database,
                    "logical",
                    "pgoutput",
                    spec.active,
                    spec.restart_lsn,
                    spec.confirmed_flush_lsn,
                    "reserved",
                ]
            ]
        elif "puballtables" in sql:
            cols = [
                ("pubname", 19),
                ("puballtables", 16),
                ("pubinsert", 16),
                ("pubupdate", 16),
                ("pubdelete", 16),
                ("pubtruncate", 16),
                ("pubviaroot", 16),
                ("schemaname", 19),
                ("tablename", 19),
                ("attnames", 1003),
                ("rowfilter", 25),
            ]
            rows = spec.pub_rows
        else:
            cols = [
                ("oid", 26),
                ("nspname", 19),
                ("typname", 19),
                ("typtype", 25),
                ("typcategory", 25),
                ("typdelim", 25),
                ("typelem", 26),
                ("typbasetype", 26),
                ("rngsubtype", 26),
                ("mrngtypid", 26),
                ("rngsubtype", 26),
                ("extname", 19),
                ("attname", 19),
                ("atttypid", 26),
                ("atttypmod", 23),
                ("attnum", 21),
            ]
            rows = []

        sess.send_raw(
            _pg_extended_select(
                cols,
                rows,
                has_parse=has_parse,
                has_describe_stmt=has_describe_stmt,
                has_describe_portal=has_describe_portal,
                has_close_portal=has_close_portal,
            )
        )

    def _handle_simple(
        self,
        sess: _ReplicationSession,
        spec: _ReplicationPeerSpec,
        sql: str,
    ) -> None:
        """Respond to IDENTIFY_SYSTEM or START_REPLICATION simple queries."""
        if sql == "IDENTIFY_SYSTEM":
            if spec.hang_before_identify_reply is not None:
                spec.hang_before_identify_reply.wait(timeout=3.0)
            sess.send_raw(
                _pg_identify_system(
                    spec.system_id,
                    spec.timeline,
                    spec.xlogpos,
                    spec.database,
                )
            )
            return
        if sql.startswith("START_REPLICATION"):
            if spec.start_error is not None:
                code, msg = spec.start_error
                sess.send_raw(_pg_error("ERROR", code, msg) + _pg_frame(b"Z", b"I"))
                return
            out = bytearray()
            out.extend(_pg_frame(b"S", b"wal_sender_timeout\x0030s\x00"))
            out.extend(_pg_frame(b"W", struct.pack("!Bh", 0, 0)))
            for chunk in spec.wal_chunks:
                out.extend(chunk)
            sess.send_raw(bytes(out))


@contextlib.contextmanager
def _running_repl_server(
    spec_factory: Callable[[int, dict[str, str]], _ReplicationPeerSpec] | None = None,
) -> Iterator[_ScriptedReplicationServer]:
    """Context manager guaranteeing scripted server shutdown even on assertion failures."""
    factory = spec_factory or (lambda _idx, _p: _ReplicationPeerSpec())
    srv = _ScriptedReplicationServer(factory)
    try:
        yield srv
    finally:
        srv.stop()


def test_replication_pre_dial_validations_and_no_loop() -> None:
    """Reject missing loop, closed clients, unsupported pooler modes, and invalid args before dialing."""
    with _running_repl_server() as srv:
        no_loop_client = AsyncPostgresClient(
            host="127.0.0.1",
            port=srv.port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        with pytest.raises(RuntimeError):
            no_loop_client.replication_identity("vtb_slot", "vtb_pub")
        with pytest.raises(RuntimeError):
            no_loop_client.open_replication("vtb_slot", "vtb_pub")
        assert srv.accept_count == 0

        async def _check_closed_and_args() -> None:
            closed_client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            await closed_client.close()
            with pytest.raises(InterfaceError):
                await closed_client.replication_identity("vtb_slot", "vtb_pub")
            with pytest.raises(InterfaceError):
                await closed_client.open_replication("vtb_slot", "vtb_pub")

            direct = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            try:
                for bad_cap in (0, -1, 1025, 2**70):
                    with pytest.raises(ValueError):
                        await direct.open_replication("vtb_slot", "vtb_pub", queue_capacity=bad_cap)
                for bool_cap in (True, False):
                    with pytest.raises(TypeError):
                        await direct.open_replication("vtb_slot", "vtb_pub", queue_capacity=bool_cap)
                for bool_lsn in (True, False):
                    with pytest.raises(TypeError):
                        await direct.open_replication("vtb_slot", "vtb_pub", start_lsn=bool_lsn)
                for bad_lsn in (-1, 2**64):
                    with pytest.raises(ValueError):
                        await direct.open_replication("vtb_slot", "vtb_pub", start_lsn=bad_lsn)
                with pytest.raises(TypeError):
                    await direct.open_replication("vtb_slot", "vtb_pub", reconnect=cast(Any, 1))
                for bad_ident in ("", "a" * 64, "bad\x00ident", "bad\nident"):
                    with pytest.raises(DataError):
                        await direct.replication_identity(bad_ident, "vtb_pub")
                    with pytest.raises(DataError):
                        await direct.open_replication("vtb_slot", bad_ident)
            finally:
                await direct.close()

        asyncio.run(_check_closed_and_args())
        assert srv.accept_count == 0


@pytest.mark.parametrize(
    ("pooler_mode", "expected_exc"),
    (
        ("transaction", NotSupportedError),
        ("statement", NotSupportedError),
        ("auto", InterfaceError),
    ),
)
def test_replication_pooler_mode_rejected_before_dial(
    pooler_mode: str,
    expected_exc: type[BaseException],
) -> None:
    """Reject Transaction/Statement with NotSupportedError and Auto with InterfaceError before opening a socket."""
    with _running_repl_server() as srv:

        async def _run() -> None:
            pool = AsyncPostgresPool(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                pooler_mode=pooler_mode,
                min_size=1,
                max_size=1,
            )
            client = await pool.acquire()
            try:
                assert srv.accept_count == 1
                with pytest.raises(expected_exc):
                    await client.replication_identity("vtb_slot", "vtb_pub")
                with pytest.raises(expected_exc):
                    await client.open_replication("vtb_slot", "vtb_pub")
                assert srv.accept_count == 1
            finally:
                await client.close()
                await pool.close()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_replication_identity_and_unawaited_open_drop() -> None:
    """Verify replication_identity returns verified PostgresReplicationIdentity and unawaited open cancels cleanly."""
    gate = threading.Event()

    def _factory(idx: int, params: dict[str, str]) -> _ReplicationPeerSpec:
        if "replication" in params and idx >= 3:
            return _ReplicationPeerSpec(hang_before_identify_reply=gate)
        return _ReplicationPeerSpec()

    with _running_repl_server(_factory) as srv:

        async def _run() -> None:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            try:
                ident = await client.replication_identity("vtb_slot", "vtb_pub")
                assert isinstance(ident, PostgresReplicationIdentity)
                assert ident.system_identifier == "7399000000000001"
                assert ident.database == "appdb"
                assert ident.slot == "vtb_slot"
                assert len(ident.publication_fingerprint) == 64

                pending = client.open_replication("vtb_slot", "vtb_pub")
                del pending
                gc.collect()
                gate.set()
                await asyncio.sleep(0)
                dropped_session = srv.session_or_none(2)
                if dropped_session is not None:
                    assert await dropped_session.wait_closed_async(timeout=2.0)
            finally:
                await client.close()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_replication_stream_records_toast_bytes_async_iter_and_acknowledge() -> None:
    """Verify lossless streaming of all record kinds, PG_UNCHANGED_TOAST, binary bytes, async with, and fenced acks."""
    rel_bytes = _pgoutput_relation(
        16384,
        "public",
        "items",
        b"f",
        [(True, "id", 23, -1), (False, "blob", 17, -1), (False, "note", 25, -1)],
    )
    wal_chunks = [
        _pg_xlog_data(0x1_0000_0210, rel_bytes),
        _pg_xlog_data(0x1_0000_0220, _pgoutput_begin(0x1_0000_0300, 123_456, 51)),
        _pg_xlog_data(
            0x1_0000_0230,
            _pgoutput_insert(
                16384,
                [("t", b"1"), ("b", b"\x00\xffraw"), ("t", b"hello")],
            ),
        ),
        _pg_xlog_data(
            0x1_0000_0240,
            _pgoutput_update(
                16384,
                b"K",
                [("t", b"1"), ("u", None), ("n", None)],
                [("t", b"1"), ("u", None), ("t", b"updated")],
            ),
        ),
        _pg_xlog_data(
            0x1_0000_0250,
            _pgoutput_delete(16384, b"O", [("t", b"1"), ("n", None), ("t", b"updated")]),
        ),
        _pg_xlog_data(
            0x1_0000_0260,
            _pgoutput_truncate([16384], cascade=True, restart_identity=False),
        ),
        _pg_xlog_data(
            0x1_0000_0270,
            _pgoutput_message(
                transactional=True,
                lsn=0x1_0000_0270,
                prefix="cdc.event",
                content=b"\x01\x02\xfe\xff",
            ),
        ),
        _pg_xlog_data(0x1_0000_0300, _pgoutput_commit(0, 0x1_0000_0300, 0x1_0000_0300, 123_999)),
    ]

    with _running_repl_server(lambda _i, _p: _ReplicationPeerSpec(wal_chunks=wal_chunks)) as srv:

        async def _run() -> None:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            try:
                async with await client.open_replication(
                    "vtb_slot",
                    "vtb_pub",
                    start_lsn=0x1_0000_0200,
                    reconnect=False,
                ) as stream:
                    assert isinstance(stream, AsyncPostgresReplication)
                    assert stream.closed is False

                    with pytest.raises(TypeError):
                        stream.acknowledge(True)
                    with pytest.raises(ValueError):
                        stream.acknowledge(-1)
                    with pytest.raises(ValueError):
                        stream.acknowledge(2**64)
                    with pytest.raises(InterfaceError):
                        stream.acknowledge(0x1_0000_0200)

                    collected: list[PostgresReplicationRecord] = []
                    async for rec in stream:
                        assert isinstance(rec, PostgresReplicationRecord)
                        collected.append(rec)
                        if rec.kind == "commit":
                            assert rec.commit_end_lsn == 0x1_0000_0300
                            acked = stream.acknowledge(rec.commit_end_lsn)
                            assert acked == 0x1_0000_0300
                            break

                    assert [r.kind for r in collected] == [
                        "control",
                        "begin",
                        "insert",
                        "update",
                        "delete",
                        "truncate",
                        "message",
                        "commit",
                    ]
                    assert collected[0].payload["identity"].slot == "vtb_slot"
                    assert collected[0].generation == 1
                    assert collected[2].payload["new_tuple"]["blob"] == b"\x00\xffraw"
                    assert collected[2].payload["relation"]["columns"][0]["is_key"] is True
                    assert collected[3].payload["old_kind"] == "key"
                    assert collected[3].payload["old_tuple"]["blob"] is PG_UNCHANGED_TOAST
                    assert collected[3].payload["new_tuple"]["blob"] is PG_UNCHANGED_TOAST
                    assert collected[4].payload["old_kind"] == "full"
                    assert collected[5].payload["cascade"] is True
                    assert collected[6].payload["content"] == b"\x01\x02\xfe\xff"

                    repl_sess = srv.session(1)
                    assert await repl_sess.wait_for_ack_count_async(1, timeout=2.0)
                    assert repl_sess.standby_acks[0] == 0x1_0000_0300

                    with pytest.raises(InterfaceError):
                        stream.acknowledge(0x1_0000_0250)

                assert stream.closed is True
                await stream.aclose()
                assert await stream.next() is None
                with pytest.raises(StopAsyncIteration):
                    await stream.__anext__()
                with pytest.raises(InterfaceError):
                    stream.acknowledge(0x1_0000_0300)
            finally:
                await client.close()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize(
    "cancel_mode",
    ("drainer_drop_first", "task_cancel_while_paused", "resume_after_next_created"),
)
def test_replication_cancelled_next_and_overlap_fencing(cancel_mode: str) -> None:
    """Verify paused-drainer cancellation preserves ordered records and concurrent next() fails synchronously."""
    rel_bytes = _pgoutput_relation(
        16384,
        "public",
        "items",
        b"d",
        [(True, "id", 23, -1), (False, "note", 25, -1)],
    )
    tx_bytes = b"".join(
        (
            _pg_xlog_data(0x1_0000_0210, rel_bytes),
            _pg_xlog_data(0x1_0000_0220, _pgoutput_begin(0x1_0000_0260, 100, 12)),
            _pg_xlog_data(0x1_0000_0230, _pgoutput_insert(16384, [("t", b"9"), ("u", None)])),
            _pg_xlog_data(0x1_0000_0260, _pgoutput_commit(0, 0x1_0000_0260, 0x1_0000_0260, 101)),
        )
    )

    with _running_repl_server() as srv:
        loop = asyncio.new_event_loop()
        drainer_ctl = _DrainerController(loop)
        try:

            async def _run() -> None:
                client = AsyncPostgresClient(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                )
                stream = await client.open_replication("vtb_slot", "vtb_pub", reconnect=False)
                try:
                    ctrl = await stream.next()
                    assert ctrl is not None and ctrl.kind == "control"

                    pending = stream.next()
                    with pytest.raises(InterfaceError):
                        stream.next()
                    del pending
                    gc.collect()

                    repl_sess = srv.session(1)
                    drainer_ctl.pause()
                    first_started = asyncio.Event()
                    first_next = asyncio.create_task(_as_coro(stream.next(), first_started))
                    await asyncio.wait_for(first_started.wait(), timeout=2.0)

                    repl_sess.send_raw(tx_bytes)
                    assert await drainer_ctl.wait_until_signaled(timeout=2.0)

                    first_next.cancel()
                    if cancel_mode == "drainer_drop_first":
                        drainer_ctl.drain_once()
                        drainer_ctl.resume()
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        r_begin = await stream.next()
                    elif cancel_mode == "task_cancel_while_paused":
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        drainer_ctl.resume()
                        await asyncio.sleep(0)
                        r_begin = await stream.next()
                    else:
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        second_started = asyncio.Event()
                        second_next = asyncio.create_task(_as_coro(stream.next(), second_started))
                        await asyncio.wait_for(second_started.wait(), timeout=2.0)
                        drainer_ctl.resume()
                        r_begin = await second_next

                    r_insert = await stream.next()
                    r_commit = await stream.next()
                    assert r_begin is not None and r_begin.kind == "begin"
                    assert r_insert is not None and r_insert.kind == "insert"
                    assert r_insert.payload["new_tuple"]["note"] is PG_UNCHANGED_TOAST
                    assert r_commit is not None and r_commit.kind == "commit"
                finally:
                    drainer_ctl.resume()
                    await stream.aclose()
                    await client.close()

            loop.run_until_complete(_run())
        finally:
            loop.close()

        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_replication_reconnect_fences_stale_generation_ack() -> None:
    """Verify reconnect transitions surface generation-2 control records and reject un-advanced generation-1 acks."""

    def _factory(idx: int, _params: dict[str, str]) -> _ReplicationPeerSpec:
        if idx <= 2:
            commit_bytes = b"".join(
                (
                    _pg_xlog_data(0x1_0000_0210, _pgoutput_begin(0x1_0000_0240, 200, 77)),
                    _pg_xlog_data(
                        0x1_0000_0240,
                        _pgoutput_commit(0, 0x1_0000_0240, 0x1_0000_0240, 201),
                    ),
                )
            )
            return _ReplicationPeerSpec(wal_chunks=[commit_bytes])
        commit2_bytes = b"".join(
            (
                _pg_xlog_data(0x1_0000_0250, _pgoutput_begin(0x1_0000_0280, 300, 78)),
                _pg_xlog_data(
                    0x1_0000_0280,
                    _pgoutput_commit(0, 0x1_0000_0280, 0x1_0000_0280, 301),
                ),
            )
        )
        return _ReplicationPeerSpec(confirmed_flush_lsn="1/200", wal_chunks=[commit2_bytes])

    with _running_repl_server(_factory) as srv:

        async def _run() -> None:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            try:
                stream = await client.open_replication("vtb_slot", "vtb_pub", reconnect=True)
                try:
                    c1 = await stream.next()
                    b1 = await stream.next()
                    cm1 = await stream.next()
                    assert c1 is not None and c1.generation == 1
                    assert b1 is not None and b1.kind == "begin"
                    assert cm1 is not None and cm1.commit_end_lsn == 0x1_0000_0240

                    srv.session(1).close_from_peer()
                    assert await srv.wait_for_accept_count_async(4, timeout=3.0)

                    with pytest.raises(InterfaceError):
                        stream.acknowledge(0x1_0000_0240)

                    c2 = await stream.next()
                    b2 = await stream.next()
                    cm2 = await stream.next()
                    assert c2 is not None and c2.kind == "control" and c2.generation == 2
                    assert b2 is not None and b2.kind == "begin" and b2.generation == 2
                    assert cm2 is not None and cm2.commit_end_lsn == 0x1_0000_0280
                    assert stream.acknowledge(0x1_0000_0280) == 0x1_0000_0280
                finally:
                    await stream.aclose()
            finally:
                await client.close()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_replication_error_propagates_as_operational_error() -> None:
    """Verify server START_REPLICATION ErrorResponse propagates cleanly and closes the stream."""

    def _factory(_idx: int, _params: dict[str, str]) -> _ReplicationPeerSpec:
        return _ReplicationPeerSpec(start_error=("57P01", "terminating replication due to administrator command"))

    with _running_repl_server(_factory) as srv:

        async def _run() -> None:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            try:
                with pytest.raises(OperationalError) as exc_info:
                    await client.open_replication("vtb_slot", "vtb_pub", reconnect=False)
                assert exc_info.value.sqlstate == "57P01"
            finally:
                await client.close()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)
