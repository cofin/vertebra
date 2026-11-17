"""Shared scripted PostgreSQL notification server, OAuth server, and drainer harness."""

from __future__ import annotations

import asyncio
import contextlib
import socket
import struct
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient
from vertebra.utils.serializers import encode_json_bytes


def _pg_client_kwargs(port: int, *, host: str = "127.0.0.1", database: str = "appdb") -> dict[str, Any]:
    """Return baseline direct loopback kwargs for Sync/Async PostgreSQL clients and pools."""
    return {
        "host": host,
        "port": port,
        "user": "postgres",
        "database": database,
        "sslmode": "disable",
        "channel_binding": "disable",
    }


def _make_direct_client(client_kind: str, port: int) -> SyncPostgresClient | AsyncPostgresClient:
    """Construct an unconnected direct-mode native PostgreSQL client targeting port."""
    cls = SyncPostgresClient if client_kind == "sync" else AsyncPostgresClient
    return cls(**_pg_client_kwargs(port))


async def _as_coro(awaitable: Awaitable[Any], started: asyncio.Event | None = None) -> Any:
    """Wrap a native RustDbTaskAwaitable inside a Python coroutine for asyncio.create_task."""
    if started is not None:
        started.set()
    return await awaitable


async def _wait_threading_event(evt: threading.Event, *, timeout: float) -> bool:
    """Await a threading.Event without blocking the running asyncio event loop."""
    if evt.is_set():
        return True
    return await asyncio.to_thread(evt.wait, timeout)


def _pg_frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 backend wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _pg_notification(pid: int, channel: str, payload: bytes) -> bytes:
    """Encode a PostgreSQL NotificationResponse ('A') backend frame."""
    body = struct.pack("!i", pid) + channel.encode("utf-8") + b"\x00" + payload + b"\x00"
    return _pg_frame(b"A", body)


def _pg_error(severity: str, sqlstate: str, message: str) -> bytes:
    """Encode a PostgreSQL ErrorResponse ('E') backend frame."""
    sev = severity.encode("ascii")
    body = (
        b"S"
        + sev
        + b"\x00V"
        + sev
        + b"\x00C"
        + sqlstate.encode("ascii")
        + b"\x00M"
        + message.encode("utf-8")
        + b"\x00\x00"
    )
    return _pg_frame(b"E", body)


def _pg_auth_ready(pid: int) -> bytes:
    """Encode AuthenticationOk, ParameterStatus headers, BackendKeyData, and ReadyForQuery('I')."""
    return b"".join(
        (
            _pg_frame(b"R", struct.pack("!i", 0)),
            _pg_frame(b"S", b"application_name\x00vertebra\x00"),
            _pg_frame(b"S", b"client_encoding\x00UTF8\x00"),
            _pg_frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _pg_frame(b"S", b"TimeZone\x00UTC\x00"),
            _pg_frame(b"K", struct.pack("!ii", pid, 50_000 + pid)),
            _pg_frame(b"Z", b"I"),
        )
    )


def _unquote_pg_channel(token: str) -> str:
    """Unquote a double-quoted or bare PostgreSQL channel identifier from LISTEN/UNLISTEN SQL."""
    stripped = token.strip()
    if len(stripped) >= 2 and stripped[0] == '"' and stripped[-1] == '"':
        return stripped[1:-1].replace('""', '"')
    return stripped


def _pg_extended_response(
    sess: _ServerSession,
    frames: list[tuple[bytes, bytes]],
) -> tuple[bytes, tuple[str, bytes] | None]:
    """Encode an extended-query SELECT 1 or SELECT pg_notify($1, $2) wire response."""
    col_fmt = 0
    bound_sql = ""
    bound_params: list[bytes | None] = []
    for tag, body in frames:
        if tag == b"P":
            stmt_end = body.index(b"\x00")
            stmt_name = body[:stmt_end].decode("utf-8", errors="replace")
            sql_end = body.index(b"\x00", stmt_end + 1)
            parsed_sql = body[stmt_end + 1 : sql_end].decode("utf-8", errors="replace")
            sess.record_prepared(stmt_name, parsed_sql)
        elif tag == b"B":
            portal_end = body.index(b"\x00") + 1
            stmt_nul = body.index(b"\x00", portal_end)
            stmt_name = body[portal_end:stmt_nul].decode("utf-8", errors="replace")
            bound_sql = sess.lookup_prepared(stmt_name)
            stmt_end = stmt_nul + 1
            (num_pfmt,) = struct.unpack_from("!h", body, stmt_end)
            offset = stmt_end + 2 + 2 * num_pfmt
            (num_params,) = struct.unpack_from("!h", body, offset)
            offset += 2
            bound_params = []
            for _ in range(num_params):
                (plen,) = struct.unpack_from("!i", body, offset)
                offset += 4
                if plen < 0:
                    bound_params.append(None)
                else:
                    bound_params.append(body[offset : offset + plen])
                    offset += plen
            (num_rfmt,) = struct.unpack_from("!h", body, offset)
            if num_rfmt >= 1:
                (col_fmt,) = struct.unpack_from("!h", body, offset + 2)
        elif tag == b"C" and body[:1] == b"S":
            closed_name = body[1:].rstrip(b"\x00").decode("utf-8", errors="replace")
            sess.drop_prepared(closed_name)

    is_pg_notify = "pg_notify" in bound_sql.casefold()
    notify_out: tuple[str, bytes] | None = None
    if is_pg_notify:
        stmt_col_desc = struct.pack("!h", 1) + b"pg_notify\x00" + struct.pack("!ihihih", 0, 0, 25, -1, -1, 0)
        portal_col_desc = struct.pack("!h", 1) + b"pg_notify\x00" + struct.pack("!ihihih", 0, 0, 25, -1, -1, col_fmt)
        row_frame = _pg_frame(b"D", struct.pack("!hi", 1, 0))
        param_desc = _pg_frame(b"t", struct.pack("!hii", 2, 25, 25))
        if (
            any(t == b"E" for t, _ in frames)
            and len(bound_params) == 2
            and bound_params[0] is not None
            and bound_params[1] is not None
        ):
            notify_out = (bound_params[0].decode("utf-8"), bound_params[1])
    else:
        stmt_col_desc = struct.pack("!h", 1) + b"one\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, 0)
        portal_col_desc = struct.pack("!h", 1) + b"one\x00" + struct.pack("!ihihih", 0, 0, 23, 4, -1, col_fmt)
        row_val = struct.pack("!i", 1) if col_fmt == 1 else b"1"
        row_frame = _pg_frame(b"D", struct.pack("!hi", 1, len(row_val)) + row_val)
        param_desc = _pg_frame(b"t", struct.pack("!h", 0))

    out = bytearray()
    tags = {t for t, _ in frames}
    if b"P" in tags:
        out.extend(_pg_frame(b"1", b""))
    if b"D" in tags and any(t == b"D" and b[:1] == b"S" for t, b in frames):
        out.extend(param_desc)
        out.extend(_pg_frame(b"T", stmt_col_desc))
    if b"B" in tags:
        out.extend(_pg_frame(b"2", b""))
    if b"D" in tags and any(t == b"D" and b[:1] == b"P" for t, b in frames):
        out.extend(_pg_frame(b"T", portal_col_desc))
    if b"E" in tags:
        out.extend(row_frame)
        out.extend(_pg_frame(b"C", b"SELECT 1\x00"))
    if any(t == b"C" for t, _ in frames):
        out.extend(_pg_frame(b"3", b""))
    out.extend(_pg_frame(b"Z", b"I"))
    return bytes(out), notify_out


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
        if self._fd is None:
            drainer = getattr(self._loop, "_vertebra_postgres_completion_drainer", None)
            assert drainer is not None, "Postgres completion drainer has not been registered yet"
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
        """Synchronously invoke the captured completion drainer callback once."""
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


class _ServerSession:
    """Single accepted backend TCP session on _ScriptedNotificationServer."""

    def __init__(
        self,
        conn: socket.socket,
        pid: int,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        """Initialize socket locks and recorded frontend command history."""
        self._conn = conn
        self.pid = pid
        self._on_close = on_close
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._queries: list[str] = []
        self._prepared: dict[str, str] = {}
        self._subscribed: set[str] = set()
        self._password: str | None = None
        self._closed = False
        self._query_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []

    @property
    def queries(self) -> list[str]:
        """Return a snapshot copy of SQL simple queries received on this session."""
        with self._state_lock:
            return list(self._queries)

    @property
    def password(self) -> str | None:
        """Return the cleartext password / IAM token received during startup, if any."""
        with self._state_lock:
            return self._password

    @property
    def is_closed(self) -> bool:
        """Return True once the session socket loop has exited and closed."""
        with self._state_lock:
            return self._closed

    def is_subscribed_to(self, channel: str) -> bool:
        """Return True when this live session currently holds LISTEN for channel."""
        with self._state_lock:
            return not self._closed and channel in self._subscribed

    def record_prepared(self, name: str, sql: str) -> None:
        """Store an extended-query Parse statement definition on this session."""
        with self._state_lock:
            self._prepared[name] = sql

    def lookup_prepared(self, name: str) -> str:
        """Return the SQL associated with prepared statement name, or empty string."""
        with self._state_lock:
            return self._prepared.get(name, "")

    def drop_prepared(self, name: str) -> None:
        """Remove a closed extended-query prepared statement entry."""
        with self._state_lock:
            self._prepared.pop(name, None)

    def send_raw(self, data: bytes) -> None:
        """Write raw backend frames synchronously under the session write lock."""
        with self._write_lock, contextlib.suppress(OSError):
            self._conn.sendall(data)

    def push_notification(self, pid: int, channel: str, payload: bytes) -> None:
        """Push a PostgreSQL NotificationResponse ('A') frame to the connected client."""
        self.send_raw(_pg_notification(pid, channel, payload))

    def close_from_peer(self) -> None:
        """Forcefully shut down and close the backend TCP socket to trigger client EOF."""
        with self._write_lock:
            with contextlib.suppress(OSError):
                self._conn.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                self._conn.close()

    async def wait_for_query_count_async(self, count: int, *, timeout: float) -> bool:
        """Await at least count recorded simple Query frames without blocking the event loop."""
        loop = asyncio.get_running_loop()
        ev = asyncio.Event()
        with self._state_lock:
            if len(self._queries) >= count:
                return True
            self._query_waiters.append((loop, count, ev))
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except TimeoutError:
            with self._state_lock:
                self._query_waiters = [item for item in self._query_waiters if item[2] is not ev]
                return len(self._queries) >= count

    def _record_password(self, value: str) -> None:
        """Store the decoded startup password value."""
        with self._state_lock:
            self._password = value

    def _record_query(self, sql: str) -> None:
        """Append a decoded simple query string and update session LISTEN/prepared state."""
        upper = sql.upper()
        with self._state_lock:
            self._queries.append(sql)
            if upper.startswith("LISTEN "):
                self._subscribed.add(_unquote_pg_channel(sql[7:]))
            elif upper == "UNLISTEN *":
                self._subscribed.clear()
            elif upper.startswith("UNLISTEN "):
                self._subscribed.discard(_unquote_pg_channel(sql[9:]))
            elif upper in ("DISCARD ALL", "DEALLOCATE ALL"):
                if upper == "DISCARD ALL":
                    self._subscribed.clear()
                self._prepared = {k: v for k, v in self._prepared.items() if not k}
            qcount = len(self._queries)
            if self._query_waiters:
                self._query_waiters = [
                    (loop, target, ev)
                    for loop, target, ev in self._query_waiters
                    if not (qcount >= target and (loop.call_soon_threadsafe(ev.set) or True))
                ]

    def _mark_closed(self) -> None:
        """Mark the session socket terminated."""
        with self._state_lock:
            self._closed = True
            self._subscribed.clear()
        if self._on_close is not None:
            self._on_close()


class _ScriptedNotificationServer:
    """Multi-connection scripted PostgreSQL Protocol 3.0 loopback server for listener tests."""

    def __init__(
        self,
        *,
        startup_gate: tuple[threading.Event, threading.Event] | None = None,
        require_cleartext_password: bool = False,
        startup_hook: Callable[[int, socket.socket], bytes | None] | None = None,
        query_hook: Callable[[_ServerSession, str], bool] | None = None,
    ) -> None:
        """Bind an ephemeral loopback listener and launch the background accept thread."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.05)
        self.port: int = self._sock.getsockname()[1]
        self._startup_gate = startup_gate
        self._require_cleartext_password = require_cleartext_password
        self._startup_hook = startup_hook
        self._query_hook = query_hook
        self._cond = threading.Condition()
        self._stop = threading.Event()
        self._accept_count = 0
        self._sessions: list[_ServerSession] = []
        self._workers: list[threading.Thread] = []
        self._accept_waiters: list[tuple[asyncio.AbstractEventLoop, int, asyncio.Event]] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    @property
    def accept_count(self) -> int:
        """Return total accepted TCP connections so far."""
        with self._cond:
            return self._accept_count

    @property
    def live_connections(self) -> int:
        """Return currently open backend session count."""
        with self._cond:
            sessions = list(self._sessions)
        return sum(0 if s.is_closed else 1 for s in sessions)

    def session(self, index: int) -> _ServerSession:
        """Return the accepted _ServerSession at index."""
        with self._cond:
            return self._sessions[index]

    def _notify_state_locked(self) -> None:
        """Notify Condition and async waiters under self._cond."""
        self._cond.notify_all()
        if self._accept_waiters:
            self._accept_waiters = [
                (loop, target, ev)
                for loop, target, ev in self._accept_waiters
                if not (self._accept_count >= target and (loop.call_soon_threadsafe(ev.set) or True))
            ]

    def _on_session_closed(self) -> None:
        """Wake Condition waiters when a session closes."""
        with self._cond:
            self._cond.notify_all()

    async def wait_for_accept_count_async(self, count: int, *, timeout: float) -> bool:
        """Await accept_count >= count without blocking the asyncio loop."""
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
        """Wait until live_connections equals expected."""
        with self._cond:
            return self._cond.wait_for(
                lambda: sum(0 if s.is_closed else 1 for s in self._sessions) == expected,
                timeout=timeout,
            )

    def stop_accepting_and_drop_all(self) -> None:
        """Close the listening socket and drop all currently open sessions."""
        self._stop.set()
        if self._startup_gate is not None:
            self._startup_gate[1].set()
        with contextlib.suppress(OSError):
            self._sock.close()
        with self._cond:
            sessions = list(self._sessions)
        for sess in sessions:
            sess.close_from_peer()

    def stop(self) -> None:
        """Shut down listener, close active sockets, and join worker threads."""
        self.stop_accepting_and_drop_all()
        self._accept_thread.join(timeout=2.0)
        with self._cond:
            workers = list(self._workers)
        for worker in workers:
            worker.join(timeout=2.0)

    def _accept_loop(self) -> None:
        """Accept incoming frontend connections and dispatch per-socket threads."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with self._cond:
                self._accept_count += 1
                idx = self._accept_count
                sess = _ServerSession(conn, 5_000 + idx, on_close=self._on_session_closed)
                self._sessions.append(sess)
                self._notify_state_locked()
            worker = threading.Thread(target=self._serve_session, args=(idx, conn, sess), daemon=True)
            with self._cond:
                self._workers.append(worker)
            worker.start()

    def _serve_session(self, idx: int, conn: socket.socket, sess: _ServerSession) -> None:
        """Handle PostgreSQL Protocol 3.0 startup, LISTEN/UNLISTEN, and extended SELECT 1."""
        try:
            self._read_startup(conn)
            if self._startup_gate is not None:
                entered_evt, release_evt = self._startup_gate
                entered_evt.set()
                release_evt.wait(timeout=3.0)
            if self._startup_hook is not None:
                override = self._startup_hook(idx, conn)
                if override is not None:
                    sess.send_raw(override)
                    return
            if self._require_cleartext_password:
                sess.send_raw(_pg_frame(b"R", struct.pack("!i", 3)))
                ptag, pbody = self._read_frame(conn)
                if ptag != b"p":
                    return
                sess._record_password(pbody.rstrip(b"\x00").decode("utf-8", errors="replace"))
            sess.send_raw(_pg_auth_ready(sess.pid))

            while not self._stop.is_set():
                tag, body = self._read_frame(conn)
                if tag == b"X":
                    return
                if tag == b"Q":
                    sql = body.rstrip(b"\x00").decode("utf-8", errors="replace").strip()
                    sess._record_query(sql)
                    if self._query_hook is not None and self._query_hook(sess, sql):
                        continue
                    upper = sql.upper()
                    if upper.startswith("LISTEN "):
                        sess.send_raw(_pg_frame(b"C", b"LISTEN\x00") + _pg_frame(b"Z", b"I"))
                    elif upper.startswith("UNLISTEN"):
                        sess.send_raw(_pg_frame(b"C", b"UNLISTEN\x00") + _pg_frame(b"Z", b"I"))
                    elif upper == "DISCARD ALL":
                        sess.send_raw(_pg_frame(b"C", b"DISCARD ALL\x00") + _pg_frame(b"Z", b"I"))
                    elif upper == "DEALLOCATE ALL":
                        sess.send_raw(_pg_frame(b"C", b"DEALLOCATE ALL\x00") + _pg_frame(b"Z", b"I"))
                    elif upper == "RESET ALL":
                        sess.send_raw(_pg_frame(b"C", b"RESET\x00") + _pg_frame(b"Z", b"I"))
                    else:
                        sess.send_raw(_pg_frame(b"C", b"OK\x00") + _pg_frame(b"Z", b"I"))
                else:
                    group = [(tag, body)]
                    while group[-1][0] != b"S":
                        ftag, fbody = self._read_frame(conn)
                        if ftag == b"X":
                            return
                        group.append((ftag, fbody))
                    reply, notify_out = _pg_extended_response(sess, group)
                    if notify_out is not None:
                        self._broadcast_notification(sess.pid, notify_out[0], notify_out[1])
                    sess.send_raw(reply)
        except (EOFError, OSError):
            return
        finally:
            sess.close_from_peer()
            sess._mark_closed()

    def _broadcast_notification(self, sender_pid: int, channel: str, payload: bytes) -> None:
        """Deliver a NotificationResponse frame to every live session subscribed to channel."""
        with self._cond:
            sessions = list(self._sessions)
        for target in sessions:
            if target.is_subscribed_to(channel):
                target.push_notification(sender_pid, channel, payload)

    @staticmethod
    def _read_exact(conn: socket.socket, size: int) -> bytes:
        """Read exactly size bytes from conn or raise EOFError."""
        buf = bytearray()
        while len(buf) < size:
            chunk = conn.recv(size - len(buf))
            if not chunk:
                raise EOFError("client closed socket")
            buf.extend(chunk)
        return bytes(buf)

    @classmethod
    def _read_startup(cls, conn: socket.socket) -> None:
        """Consume StartupMessage handling an optional SSLRequest probe first."""
        raw_len = cls._read_exact(conn, 4)
        (pkt_len,) = struct.unpack("!i", raw_len)
        payload = cls._read_exact(conn, pkt_len - 4)
        if pkt_len == 8 and payload == struct.pack("!i", 80877103):
            conn.sendall(b"N")
            raw_len2 = cls._read_exact(conn, 4)
            (pkt_len2,) = struct.unpack("!i", raw_len2)
            cls._read_exact(conn, pkt_len2 - 4)

    @classmethod
    def _read_frame(cls, conn: socket.socket) -> tuple[bytes, bytes]:
        """Read one 5-byte tagged frontend PostgreSQL protocol frame."""
        hdr = cls._read_exact(conn, 5)
        tag = hdr[:1]
        (length,) = struct.unpack("!i", hdr[1:5])
        body = cls._read_exact(conn, length - 4)
        return tag, body


class _LoopbackOAuthServer:
    """Loopback HTTP/1.1 OAuth2 token endpoint issuing sequential IAM tokens."""

    def __init__(self, *, expires_in: int = 1, refresh_window_s: float = 0.2) -> None:
        """Bind an ephemeral loopback HTTP socket and start serving token requests."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.05)
        self.port: int = self._sock.getsockname()[1]
        self._expires_in = expires_in
        self._refresh_window_s = refresh_window_s
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._issue_count = 0
        self._last_issued_monotonic = 0.0
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @property
    def issue_count(self) -> int:
        """Return total access tokens issued so far."""
        with self._lock:
            return self._issue_count

    async def wait_until_current_token_expired(self) -> None:
        """Wait until the active token enters its refresh-skew window."""
        with self._lock:
            issued_at = self._last_issued_monotonic
        if issued_at <= 0.0:
            return
        remaining = (issued_at + self._refresh_window_s) - time.monotonic()
        if remaining <= 0.0:
            return
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        handle = loop.call_later(remaining, lambda: None if fut.done() else fut.set_result(None))
        try:
            await fut
        finally:
            handle.cancel()

    def stop(self) -> None:
        """Stop the HTTP accept thread and close the socket."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._sock.close()
        self._thread.join(timeout=2.0)

    def _serve(self) -> None:
        """Accept HTTP POST /token requests and reply with sequential iam-token-vN payloads."""
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with conn, contextlib.suppress(OSError):
                conn.settimeout(1.0)
                req = bytearray()
                while b"\r\n\r\n" not in req:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    req.extend(chunk)
                header_end = req.find(b"\r\n\r\n")
                if header_end >= 0:
                    headers_text = req[:header_end].decode("latin1", errors="replace")
                    content_len = 0
                    for line in headers_text.split("\r\n")[1:]:
                        if line.lower().startswith("content-length:"):
                            content_len = int(line.split(":", 1)[1].strip())
                    body_have = len(req) - (header_end + 4)
                    while body_have < content_len:
                        more = conn.recv(content_len - body_have)
                        if not more:
                            break
                        body_have += len(more)
                with self._lock:
                    self._issue_count += 1
                    ver = self._issue_count
                    self._last_issued_monotonic = time.monotonic()
                body_bytes = encode_json_bytes(
                    {"access_token": f"iam-token-v{ver}", "expires_in": self._expires_in, "token_type": "Bearer"}
                )
                resp = (
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Connection: close\r\n" + f"Content-Length: {len(body_bytes)}\r\n\r\n".encode("ascii") + body_bytes
                )
                conn.sendall(resp)


@contextlib.contextmanager
def _running_pg_server(
    *,
    startup_gate: tuple[threading.Event, threading.Event] | None = None,
    require_cleartext_password: bool = False,
    startup_hook: Callable[[int, socket.socket], bytes | None] | None = None,
    query_hook: Callable[[_ServerSession, str], bool] | None = None,
) -> Iterator[_ScriptedNotificationServer]:
    """Context manager yielding a running _ScriptedNotificationServer and ensuring shutdown."""
    srv = _ScriptedNotificationServer(
        startup_gate=startup_gate,
        require_cleartext_password=require_cleartext_password,
        startup_hook=startup_hook,
        query_hook=query_hook,
    )
    try:
        yield srv
    finally:
        srv.stop()


@contextlib.contextmanager
def _running_oauth_server(*, expires_in: int = 1, refresh_window_s: float = 0.2) -> Iterator[_LoopbackOAuthServer]:
    """Context manager yielding a running _LoopbackOAuthServer and ensuring shutdown."""
    srv = _LoopbackOAuthServer(expires_in=expires_in, refresh_window_s=refresh_window_s)
    try:
        yield srv
    finally:
        srv.stop()
