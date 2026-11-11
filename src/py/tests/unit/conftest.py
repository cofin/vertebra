"""Shared unit-test fixtures including the threaded loopback RESP3 server."""

import contextlib
import fnmatch
import hashlib
import socket
import socketserver
import threading
import time
from collections.abc import Iterator, Sequence

import pytest

from vertebra.db.adapters.redis.core import Resp3Parser


class Resp3FakeServer:
    """Threaded loopback RESP3 server holding keys, expiries, scripts, and subscriptions in process."""

    def __init__(self) -> None:
        """Start a daemon ThreadingTCPServer bound to an ephemeral loopback port."""
        self._lock = threading.Lock()
        self._sockets: set[socket.socket] = set()
        self._values: dict[bytes, bytes] = {}
        self._expiries: dict[bytes, float] = {}
        self._scripts: dict[str, str] = {}
        self._subscriptions: dict[socket.socket, set[tuple[str, bytes]]] = {}
        self._commands: list[list[bytes]] = []
        self._server = _Server(("127.0.0.1", 0), _Handler)
        self._server.owner = self
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self._thread.start()

    @property
    def port(self) -> int:
        """Return the bound TCP port."""
        return int(self._server.server_address[1])

    @property
    def redis_url(self) -> str:
        """Return the redis:// URL for this server."""
        return f"redis://127.0.0.1:{self.port}/0"

    @property
    def valkey_url(self) -> str:
        """Return the valkey:// URL for this server."""
        return f"valkey://127.0.0.1:{self.port}/0"

    @property
    def commands(self) -> list[list[bytes]]:
        """Return a snapshot of received command frames."""
        with self._lock:
            return list(self._commands)

    def close(self) -> None:
        """Shut down client sockets, stop the TCP server, and join the server thread."""
        with self._lock:
            sockets = list(self._sockets)
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2.0)

    def _purge_expired(self, key: bytes) -> None:
        """Evict key if its monotonic deadline has passed."""
        exp = self._expiries.get(key)
        if exp is not None and time.monotonic() >= exp:
            self._values.pop(key, None)
            self._expiries.pop(key, None)

    def _dispatch(self, conn: socket.socket, args: list[bytes]) -> bytes:
        """Execute a parsed RESP3 command under the server lock and return wire reply bytes."""
        if not args:
            return _encode(_ErrorReply("ERR empty command"))
        cmd = args[0].decode("ascii", errors="replace").upper()
        match cmd:
            case "HELLO":
                return b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n"
            case "CLIENT":
                return _encode("OK")
            case "PING":
                return _encode(args[1] if len(args) > 1 else "PONG")
            case "GET":
                key = args[1]
                self._purge_expired(key)
                return _encode(self._values.get(key))
            case "SET":
                key, value = args[1], args[2]
                self._values[key] = value
                self._expiries.pop(key, None)
                idx = 3
                while idx + 1 < len(args):
                    opt = args[idx].decode("ascii", errors="replace").upper()
                    if opt == "EX":
                        self._expiries[key] = time.monotonic() + float(int(args[idx + 1]))
                    elif opt == "PX":
                        self._expiries[key] = time.monotonic() + int(args[idx + 1]) / 1000.0
                    idx += 2
                return _encode("OK")
            case "DEL" | "UNLINK":
                removed = 0
                for key in args[1:]:
                    self._purge_expired(key)
                    self._expiries.pop(key, None)
                    if self._values.pop(key, None) is not None:
                        removed += 1
                return _encode(removed)
            case "PTTL":
                key = args[1]
                self._purge_expired(key)
                if key not in self._values:
                    return _encode(-2)
                exp = self._expiries.get(key)
                if exp is None:
                    return _encode(-1)
                return _encode(max(1, int((exp - time.monotonic()) * 1000)))
            case "SCAN":
                pattern: str | None = None
                idx = 2
                while idx + 1 < len(args):
                    if args[idx].decode("ascii", errors="replace").upper() == "MATCH":
                        pattern = args[idx + 1].decode("utf-8")
                    idx += 2
                for key in list(self._values):
                    self._purge_expired(key)
                matched = [
                    key for key in self._values if pattern is None or fnmatch.fnmatchcase(key.decode("utf-8"), pattern)
                ]
                return _encode([b"0", matched])
            case "FLUSHDB":
                self._values.clear()
                self._expiries.clear()
                return _encode("OK")
            case "EVALSHA":
                sha = args[1].decode("ascii")
                script = self._scripts.get(sha)
                if script is None:
                    return _encode(_ErrorReply("NOSCRIPT No matching script. Please use EVAL."))
                numkeys = int(args[2])
                keys = args[3 : 3 + numkeys]
                script_args = args[3 + numkeys :]
                for key in keys:
                    self._purge_expired(key)
                return _encode(_script_result(script, keys, script_args, self._values))
            case "EVAL":
                script = args[1].decode("utf-8")
                sha = hashlib.sha1(args[1]).hexdigest()
                self._scripts[sha] = script
                numkeys = int(args[2])
                keys = args[3 : 3 + numkeys]
                script_args = args[3 + numkeys :]
                for key in keys:
                    self._purge_expired(key)
                return _encode(_script_result(script, keys, script_args, self._values))
            case "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE":
                ack_tag = cmd.lower().encode("ascii")
                sub_kind = {"SUBSCRIBE": "message", "PSUBSCRIBE": "pmessage", "SSUBSCRIBE": "smessage"}[cmd]
                subs = self._subscriptions.setdefault(conn, set())
                out = bytearray()
                for name in args[1:]:
                    subs.add((sub_kind, name))
                    out.extend(_encode_push([ack_tag, name, len(subs)]))
                return bytes(out)
            case "UNSUBSCRIBE":
                subs = self._subscriptions.setdefault(conn, set())
                targets = args[1:] if len(args) > 1 else [name for kind, name in list(subs) if kind == "message"]
                if not targets:
                    return _encode_push([b"unsubscribe", None, len(subs)])
                out = bytearray()
                for name in targets:
                    subs.discard(("message", name))
                    out.extend(_encode_push([b"unsubscribe", name, len(subs)]))
                return bytes(out)
            case "PUBLISH":
                channel, payload = args[1], args[2]
                ch_str = channel.decode("utf-8")
                delivered = 0
                for sub_conn, subs in list(self._subscriptions.items()):
                    for kind, target in list(subs):
                        if kind == "message" and target == channel:
                            with contextlib.suppress(OSError):
                                sub_conn.sendall(_encode_push([b"message", channel, payload]))
                                delivered += 1
                        elif kind == "pmessage" and fnmatch.fnmatchcase(ch_str, target.decode("utf-8")):
                            with contextlib.suppress(OSError):
                                sub_conn.sendall(_encode_push([b"pmessage", target, channel, payload]))
                                delivered += 1
                return _encode(delivered)
            case "SPUBLISH":
                channel, payload = args[1], args[2]
                delivered = 0
                for sub_conn, subs in list(self._subscriptions.items()):
                    if ("smessage", channel) in subs:
                        with contextlib.suppress(OSError):
                            sub_conn.sendall(_encode_push([b"smessage", channel, payload]))
                            delivered += 1
                return _encode(delivered)
            case _:
                return _encode(_ErrorReply(f"ERR unknown command '{cmd}'"))


@pytest.fixture
def resp3_server() -> Iterator[Resp3FakeServer]:
    """Yield a running loopback RESP3 server and stop it after the test."""
    server = Resp3FakeServer()
    try:
        yield server
    finally:
        server.close()


class _ErrorReply(str):
    """RESP3 simple error payload."""


class _Handler(socketserver.BaseRequestHandler):
    """Socket handler reading RESP3 command frames and writing replies under the server lock."""

    def handle(self) -> None:
        """Read client frames until EOF and dispatch each under the owning server lock."""
        assert isinstance(self.server, _Server)
        owner = self.server.owner
        conn = self.request
        with owner._lock:
            owner._sockets.add(conn)
        buf = bytearray()
        try:
            while True:
                try:
                    chunk = conn.recv(4096)
                except OSError:
                    break
                if not chunk:
                    break
                buf.extend(chunk)
                while buf:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    if isinstance(frame, list):
                        raw_args = [item if isinstance(item, bytes) else str(item).encode("utf-8") for item in frame]
                        with owner._lock:
                            owner._commands.append(raw_args)
                            reply = owner._dispatch(conn, raw_args)
                            with contextlib.suppress(OSError):
                                conn.sendall(reply)
        finally:
            with owner._lock:
                owner._sockets.discard(conn)
                owner._subscriptions.pop(conn, None)


class _Server(socketserver.ThreadingTCPServer):
    """Threaded TCP server carrying a back-reference to its owning Resp3FakeServer."""

    allow_reuse_address = True
    daemon_threads = True
    owner: Resp3FakeServer


def _encode(value: object) -> bytes:
    """Encode a Python value into a RESP3 wire frame."""
    if value is None:
        return b"_\r\n"
    if isinstance(value, _ErrorReply):
        return f"-{value}\r\n".encode()
    if isinstance(value, int):
        return f":{value}\r\n".encode("ascii")
    if isinstance(value, str):
        return f"+{value}\r\n".encode()
    if isinstance(value, bytes):
        return f"${len(value)}\r\n".encode("ascii") + value + b"\r\n"
    if isinstance(value, list):
        return f"*{len(value)}\r\n".encode("ascii") + b"".join(_encode(item) for item in value)
    msg = f"Unsupported RESP3 reply type: {type(value)!r}"
    raise TypeError(msg)


def _encode_push(items: Sequence[bytes | int | None]) -> bytes:
    """Encode a RESP3 push frame (>N) over bulk strings, integers, and nulls."""
    return f">{len(items)}\r\n".encode("ascii") + b"".join(_encode(item) for item in items)


def _script_result(
    script: str,
    keys: list[bytes],
    args: list[bytes],
    values: dict[bytes, bytes],
) -> object:
    """Evaluate a test Lua script stub against in-memory keys and arguments."""
    if "KEYS[1]" in script and keys:
        return values.get(keys[0])
    if "ARGV[1]" in script and args:
        return args[0]
    return "OK"
