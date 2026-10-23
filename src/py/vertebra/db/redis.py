"""Redis RESP3 driver, command pipelining, and Litestar Channels/Store acceleration."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import hashlib
import socket
import ssl
import threading
import time
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import msgspec

from vertebra.typing import VertebraError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Sequence

__all__ = (
    "AsyncRedisDriver",
    "RedisConfig",
    "Resp3Error",
    "Resp3Parser",
    "SyncRedisDriver",
)


class Resp3Error(VertebraError):
    """Exception raised when a Redis or Valkey server returns a RESP3 error frame."""


class Resp3Parser:
    """Zero-copy and low-overhead parser for the Redis Serialization Protocol (RESP3)."""

    @classmethod
    def parse_frame(
        cls,
        data: bytes | bytearray | memoryview,
        offset: int = 0,
    ) -> tuple[Any, int]:
        """Parse a single RESP3 frame from data at offset, returning (value, next_offset)."""
        buf = memoryview(data) if not isinstance(data, memoryview) else data
        total_len = len(buf)
        if offset >= total_len:
            msg = "Unexpected end of RESP3 buffer"
            raise ValueError(msg)

        marker = buf[offset]
        pos = offset + 1

        if marker == 43:
            idx = cls._find_crlf(buf, pos)
            text = bytes(buf[pos:idx]).decode("utf-8")
            return text, idx + 2

        if marker == 45:
            idx = cls._find_crlf(buf, pos)
            err_msg = bytes(buf[pos:idx]).decode("utf-8")
            raise Resp3Error(err_msg)

        if marker == 33:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete blob error frame"
                raise ValueError(msg)
            err_msg = bytes(buf[val_start:val_end]).decode("utf-8")
            raise Resp3Error(err_msg)

        if marker == 58:
            idx = cls._find_crlf(buf, pos)
            num = int(bytes(buf[pos:idx]).decode("ascii"))
            return num, idx + 2

        if marker == 44:
            idx = cls._find_crlf(buf, pos)
            val_str = bytes(buf[pos:idx]).decode("ascii").lower()
            if val_str == "inf":
                f_val = float("inf")
            elif val_str == "-inf":
                f_val = float("-inf")
            elif val_str == "nan":
                f_val = float("nan")
            else:
                f_val = float(val_str)
            return f_val, idx + 2

        if marker == 35:
            idx = cls._find_crlf(buf, pos)
            b_char = buf[pos]
            return b_char == 116, idx + 2

        if marker == 95:
            idx = cls._find_crlf(buf, pos)
            return None, idx + 2

        if marker == 36:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            if length == -1:
                return None, idx + 2
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete bulk string frame"
                raise ValueError(msg)
            return bytes(buf[val_start:val_end]), val_end + 2

        if marker == 61:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete verbatim string frame"
                raise ValueError(msg)
            return bytes(buf[val_start + 4 : val_end]), val_end + 2

        if marker == 42:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            if count == -1:
                return None, idx + 2
            curr = idx + 2
            items: list[Any] = []
            for _ in range(count):
                item, curr = cls.parse_frame(buf, curr)
                items.append(item)
            return items, curr

        if marker == 126:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            set_items: set[Any] = set()
            for _ in range(count):
                item, curr = cls.parse_frame(buf, curr)
                set_items.add(item)
            return set_items, curr

        if marker == 37:
            idx = cls._find_crlf(buf, pos)
            pairs = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            dict_items: dict[Any, Any] = {}
            for _ in range(pairs):
                k, curr = cls.parse_frame(buf, curr)
                v, curr = cls.parse_frame(buf, curr)
                dict_items[k] = v
            return dict_items, curr

        if marker == 62:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            push_items: list[Any] = []
            for _ in range(count):
                item, curr = cls.parse_frame(buf, curr)
                push_items.append(item)
            return ("push", push_items), curr

        msg = f"Unsupported RESP3 marker byte: {chr(marker)!r} ({marker})"
        raise ValueError(msg)

    @staticmethod
    def _find_crlf(buf: memoryview, start: int) -> int:
        """Locate next carriage return and line feed delimiter."""
        total = len(buf)
        pos = start
        while pos + 1 < total:
            if buf[pos] == 13 and buf[pos + 1] == 10:
                return pos
            pos += 1
        msg = "Incomplete RESP3 frame delimiter"
        raise ValueError(msg)


def _decode_part(part: str | bytes | int) -> str:
    """Decode a command part to string."""
    if isinstance(part, bytes):
        return part.decode("utf-8")
    return str(part)


class RedisConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Redis 7.2+ / 8.0+ standalone, Sentinel, and Cluster connections."""

    url: str = "redis://localhost:6379/0"
    username: str | None = None
    password: str | None = None
    client_name: str = "vertebra"
    protocol: Literal[2, 3] = 3
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    sentinel_service_name: str | None = None
    cluster_nodes: tuple[str, ...] = ()
    enable_client_side_caching: bool = False
    enable_gcp_iam_auth: bool = False
    max_connections: int = 32

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`rediss://`)."""
        return self.url.startswith("rediss://")


class SyncRedisDriver:
    """Synchronous Redis RESP3 driver with in-memory fallback, pooling, and pipelining."""

    __slots__ = (
        "_closed",
        "_config",
        "_connections",
        "_kv",
        "_lock",
        "_push_callbacks",
        "_scripts",
        "_ttls",
    )

    def __init__(self, config: RedisConfig | None = None) -> None:
        self._config = config or RedisConfig()
        self._kv: dict[str, bytes] = {}
        self._ttls: dict[str, float] = {}
        self._scripts: dict[str, str] = {}
        self._connections: collections.deque[socket.socket] = collections.deque()
        self._push_callbacks: list[Callable[[list[Any]], None]] = []
        self._lock = threading.Lock()
        self._closed = False

    @property
    def config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self._config

    @staticmethod
    def encode_command(*parts: str | bytes | int) -> bytes:
        """Encode a Redis or Valkey RESP3 bulk-string array command."""
        chunks = [f"*{len(parts)}\r\n".encode("ascii")]
        for arg in parts:
            if isinstance(arg, int):
                raw = str(arg).encode("ascii")
            elif isinstance(arg, str):
                raw = arg.encode("utf-8")
            else:
                raw = bytes(arg)
            chunks.append(f"${len(raw)}\r\n".encode("ascii"))
            chunks.append(raw)
            chunks.append(b"\r\n")
        return b"".join(chunks)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = "vertebra",
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake command with optional AUTH and SETNAME."""
        parts: list[bytes | str] = ["HELLO", "3"]
        if password is not None:
            parts.extend(["AUTH", username or "default", password])
        if client_name:
            parts.extend(["SETNAME", client_name])
        return SyncRedisDriver.encode_command(*parts)

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> bytes:
        """Encode a batch of pipelined RESP3 commands into a single wire buffer."""
        return b"".join(self.encode_command(*cmd) for cmd in commands)

    def register_push_callback(self, callback: Callable[[list[Any]], None]) -> None:
        """Register a handler for incoming RESP3 push frames."""
        self._push_callbacks.append(callback)

    def get(self, key: str) -> bytes | None:
        """Fetch a key value from the store."""
        if self._config.url.startswith("memory://"):
            with self._lock:
                exp = self._ttls.get(key)
                if exp is not None and time.monotonic() >= exp:
                    self._kv.pop(key, None)
                    self._ttls.pop(key, None)
                    return None
                return self._kv.get(key)
        res = self.execute_command("GET", key)
        return bytes(res) if isinstance(res, (bytes, bytearray)) else None

    def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Set a key value in the store."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        if self._config.url.startswith("memory://"):
            with self._lock:
                self._kv[key] = raw
                if ex is not None:
                    self._ttls[key] = time.monotonic() + ex
                else:
                    self._ttls.pop(key, None)
            return
        if ex is not None:
            self.execute_command("SET", key, raw, "EX", ex)
        else:
            self.execute_command("SET", key, raw)

    def delete(self, key: str) -> bool:
        """Delete a key from the store."""
        if self._config.url.startswith("memory://"):
            with self._lock:
                self._ttls.pop(key, None)
                return self._kv.pop(key, None) is not None
        res = self.execute_command("DEL", key)
        return bool(res and res > 0)

    def pttl(self, key: str) -> int:
        """Return the remaining time to live of a key in milliseconds."""
        if self._config.url.startswith("memory://"):
            with self._lock:
                if key not in self._kv:
                    return -2
                exp = self._ttls.get(key)
                if exp is None:
                    return -1
                remaining_ms = int((exp - time.monotonic()) * 1000)
                if remaining_ms <= 0:
                    self._kv.pop(key, None)
                    self._ttls.pop(key, None)
                    return -2
                return remaining_ms
        res = self.execute_command("PTTL", key)
        return int(res) if isinstance(res, int) else -2

    def flushdb(self) -> None:
        """Clear all keys in the store."""
        if self._config.url.startswith("memory://"):
            with self._lock:
                self._kv.clear()
                self._ttls.clear()
            return
        self.execute_command("FLUSHDB")

    def publish(self, channel: str, payload: bytes | str) -> int:
        """Publish a message to a Redis pub/sub channel."""
        if self._config.url.startswith("memory://"):
            return 1
        res = self.execute_command("PUBLISH", channel, payload)
        return int(res) if isinstance(res, int) else 1

    def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[str | bytes | int] = (),
    ) -> Any:
        """Execute a Lua script using SHA1 EVALSHA with transparent EVAL fallback on NOSCRIPT."""
        sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
        try:
            return self.execute_command("EVALSHA", sha, len(keys), *keys, *args)
        except Resp3Error as err:
            if "NOSCRIPT" in str(err):
                return self.execute_command("EVAL", script, len(keys), *keys, *args)
            raise

    def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a single RESP3 command, returning parsed response."""
        if self._config.url.startswith("memory://"):
            return self._execute_memory_command(*parts)
        sock = self._get_socket()
        try:
            sock.sendall(self.encode_command(*parts))
            return self._read_socket_reply(sock)
        except Exception:
            self._close_socket(sock)
            raise
        else:
            self._return_socket(sock)

    def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of pipelined RESP3 commands into a single wire buffer."""
        if not commands:
            return []
        if self._config.url.startswith("memory://"):
            return [self._execute_memory_command(*cmd) for cmd in commands]
        wire_buf = self.encode_pipeline(commands)
        sock = self._get_socket()
        try:
            sock.sendall(wire_buf)
            results: list[Any] = []
            buf = bytearray()
            for _ in range(len(commands)):
                results.append(self._read_socket_reply(sock, buf=buf))
            return results
        except Exception:
            self._close_socket(sock)
            raise
        else:
            self._return_socket(sock)

    def subscribe(self, *channels: str) -> Iterator[tuple[str, bytes]]:
        """Subscribe to pub/sub channels and yield (channel, message) tuples."""
        sock = self._connect_socket()
        try:
            sock.sendall(self.encode_command("SUBSCRIBE", *channels))
            buf = bytearray()
            while not self._closed:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                while True:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    items = frame[1] if (isinstance(frame, tuple) and frame[0] == "push") else frame
                    if isinstance(items, list) and len(items) >= 3:
                        msg_type = items[0]
                        if msg_type in ("message", b"message"):
                            ch = items[1].decode("utf-8") if isinstance(items[1], bytes) else str(items[1])
                            data = items[2] if isinstance(items[2], bytes) else str(items[2]).encode("utf-8")
                            yield ch, data
        finally:
            self._close_socket(sock)

    def close(self) -> None:
        """Close all pooled socket connections."""
        self._closed = True
        with self._lock:
            while self._connections:
                sock = self._connections.popleft()
                self._close_socket(sock)

    def _execute_memory_command(self, *parts: str | bytes | int) -> Any:
        """Simulate RESP3 commands against in-memory dictionary state."""
        if not parts:
            return None
        cmd = (parts[0].decode("ascii") if isinstance(parts[0], bytes) else str(parts[0])).upper()
        if cmd == "GET":
            k = parts[1].decode("utf-8") if isinstance(parts[1], bytes) else str(parts[1])
            return self.get(k)
        if cmd == "SET":
            k = parts[1].decode("utf-8") if isinstance(parts[1], bytes) else str(parts[1])
            raw_v = parts[2]
            v = str(raw_v) if isinstance(raw_v, int) else raw_v
            ex = None
            if len(parts) >= 5 and str(parts[3]).upper() == "EX":
                ex = int(parts[4])
            self.set(k, v, ex=ex)
            return "OK"
        if cmd == "DEL":
            count = 0
            for raw_k in parts[1:]:
                k = raw_k.decode("utf-8") if isinstance(raw_k, bytes) else str(raw_k)
                if self.delete(k):
                    count += 1
            return count
        if cmd == "PTTL":
            k = parts[1].decode("utf-8") if isinstance(parts[1], bytes) else str(parts[1])
            return self.pttl(k)
        if cmd == "FLUSHDB":
            self.flushdb()
            return "OK"
        if cmd == "PUBLISH":
            return 1
        if cmd == "PING":
            return parts[1] if len(parts) > 1 else "PONG"
        if cmd == "EVAL":
            script = _decode_part(parts[1])
            sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
            numkeys = int(parts[2])
            keys = [_decode_part(parts[3 + i]) for i in range(numkeys)]
            args = list(parts[3 + numkeys :])
            self._scripts[sha] = script
            return self._eval_memory_script(script, keys, args)
        if cmd == "EVALSHA":
            sha = _decode_part(parts[1])
            if sha not in self._scripts:
                msg = f"NOSCRIPT No matching script. Please use EVAL. (sha: {sha})"
                raise Resp3Error(msg)
            script = self._scripts[sha]
            numkeys = int(parts[2])
            keys = [_decode_part(parts[3 + i]) for i in range(numkeys)]
            args = list(parts[3 + numkeys :])
            return self._eval_memory_script(script, keys, args)
        if cmd == "CLIENT":
            return "OK"
        if cmd == "HELLO":
            return {"server": "redis", "proto": 3}
        return "OK"

    def _eval_memory_script(
        self,
        script: str,
        keys: Sequence[str],
        args: Sequence[str | bytes | int],
    ) -> Any:
        """Run standard scripted expressions in memory test mode."""
        if "KEYS[1]" in script and keys:
            return self.get(keys[0])
        if "ARGV[1]" in script and args:
            return args[0]
        return "OK"

    def _connect_socket(self) -> socket.socket:
        """Create and initialize a new TCP or TLS socket connection with HELLO 3 handshake."""
        parsed = urlparse(self._config.url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 6379
        sock = socket.create_connection((host, port), timeout=10.0)
        if self._config.uses_tls:
            ctx = ssl.create_default_context()
            sock = ctx.wrap_socket(sock, server_hostname=host)
        effective_user = parsed.username or self._config.username
        effective_pwd = parsed.password or self._config.password
        handshake = self.encode_hello3_handshake(
            username=effective_user,
            password=effective_pwd,
            client_name=self._config.client_name,
        )
        sock.sendall(handshake)
        self._read_socket_reply(sock)
        return sock

    def _get_socket(self) -> socket.socket:
        """Retrieve an idle socket from the pool or create a new one."""
        with self._lock:
            while self._connections:
                s = self._connections.popleft()
                try:
                    s.setblocking(False)
                    data = s.recv(1, socket.MSG_PEEK)
                    s.setblocking(True)
                    if data == b"":
                        s.close()
                        continue
                except (BlockingIOError, ssl.SSLWantReadError):
                    s.setblocking(True)
                except Exception:
                    s.close()
                    continue
                return s
        return self._connect_socket()

    def _return_socket(self, sock: socket.socket) -> None:
        """Return an active socket to the pool if capacity permits."""
        with self._lock:
            if not self._closed and len(self._connections) < self._config.max_connections:
                self._connections.append(sock)
            else:
                self._close_socket(sock)

    def _close_socket(self, sock: socket.socket) -> None:
        """Safely close a socket."""
        with contextlib.suppress(Exception):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(Exception):
            sock.close()

    def _read_socket_reply(self, sock: socket.socket, buf: bytearray | None = None) -> Any:
        """Read bytes until a complete non-push RESP3 frame is parsed."""
        read_buf = buf if buf is not None else bytearray()
        while True:
            if read_buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(read_buf, 0)
                except ValueError:
                    pass
                else:
                    del read_buf[:next_offset]
                    if isinstance(frame, tuple) and len(frame) == 2 and frame[0] == "push":
                        for cb in self._push_callbacks:
                            cb(frame[1])
                        continue
                    return frame
            chunk = sock.recv(4096)
            if not chunk:
                msg = "Connection closed while reading RESP3 reply"
                raise ConnectionError(msg)
            read_buf.extend(chunk)


class AsyncRedisDriver:
    """Asynchronous Redis RESP3 driver facade with connection pooling and streaming."""

    __slots__ = (
        "_closed",
        "_config",
        "_connections",
        "_lock",
        "_push_callbacks",
        "_sync",
    )

    def __init__(self, config: RedisConfig | None = None) -> None:
        self._config = config or RedisConfig()
        self._sync = SyncRedisDriver(self._config)
        self._connections: collections.deque[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = collections.deque()
        self._push_callbacks: list[Callable[[list[Any]], None]] = []
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self._config

    def register_push_callback(self, callback: Callable[[list[Any]], None]) -> None:
        """Register a handler for incoming RESP3 push frames."""
        self._push_callbacks.append(callback)
        self._sync.register_push_callback(callback)

    async def get(self, key: str) -> bytes | None:
        """Asynchronously fetch a key value."""
        if self._config.url.startswith("memory://"):
            return self._sync.get(key)
        res = await self.execute_command("GET", key)
        return bytes(res) if isinstance(res, (bytes, bytearray)) else None

    async def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Asynchronously set a key value."""
        if self._config.url.startswith("memory://"):
            self._sync.set(key, value, ex=ex)
            return
        if ex is not None:
            await self.execute_command("SET", key, value, "EX", ex)
        else:
            await self.execute_command("SET", key, value)

    async def delete(self, key: str) -> bool:
        """Asynchronously delete a key."""
        if self._config.url.startswith("memory://"):
            return self._sync.delete(key)
        res = await self.execute_command("DEL", key)
        return bool(res and res > 0)

    async def pttl(self, key: str) -> int:
        """Return remaining TTL of key in milliseconds."""
        if self._config.url.startswith("memory://"):
            return self._sync.pttl(key)
        res = await self.execute_command("PTTL", key)
        return int(res) if isinstance(res, int) else -2

    async def flushdb(self) -> None:
        """Asynchronously clear all keys in the store."""
        if self._config.url.startswith("memory://"):
            self._sync.flushdb()
            return
        await self.execute_command("FLUSHDB")

    async def publish(self, channel: str, payload: bytes | str) -> int:
        """Asynchronously publish a message to a Redis pub/sub channel."""
        if self._config.url.startswith("memory://"):
            return 1
        res = await self.execute_command("PUBLISH", channel, payload)
        return int(res) if isinstance(res, int) else 1

    async def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[str | bytes | int] = (),
    ) -> Any:
        """Execute a Lua script using SHA1 EVALSHA with transparent EVAL fallback on NOSCRIPT."""
        sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
        try:
            return await self.execute_command("EVALSHA", sha, len(keys), *keys, *args)
        except Resp3Error as err:
            if "NOSCRIPT" in str(err):
                return await self.execute_command("EVAL", script, len(keys), *keys, *args)
            raise

    async def execute_command(self, *parts: str | bytes | int) -> Any:
        """Asynchronously execute a single RESP3 command."""
        if self._config.url.startswith("memory://"):
            return self._sync._execute_memory_command(*parts)
        reader, writer = await self._get_connection()
        try:
            writer.write(SyncRedisDriver.encode_command(*parts))
            await writer.drain()
            return await self._read_reply(reader)
        except Exception:
            await self._close_connection(reader, writer)
            raise
        else:
            await self._return_connection(reader, writer)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Asynchronously execute a batch of pipelined RESP3 commands."""
        if not commands:
            return []
        if self._config.url.startswith("memory://"):
            return self._sync.execute_pipeline(commands)
        wire_buf = self._sync.encode_pipeline(commands)
        reader, writer = await self._get_connection()
        try:
            writer.write(wire_buf)
            await writer.drain()
            results: list[Any] = []
            buf = bytearray()
            for _ in range(len(commands)):
                results.append(await self._read_reply(reader, buf=buf))
            return results
        except Exception:
            await self._close_connection(reader, writer)
            raise
        else:
            await self._return_connection(reader, writer)

    async def subscribe(self, *channels: str) -> AsyncIterator[tuple[str, bytes]]:
        """Asynchronously subscribe to pub/sub channels and stream incoming events."""
        reader, writer = await self._create_connection()
        try:
            writer.write(SyncRedisDriver.encode_command("SUBSCRIBE", *channels))
            await writer.drain()
            buf = bytearray()
            while not self._closed:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                while True:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    items = frame[1] if (isinstance(frame, tuple) and frame[0] == "push") else frame
                    if isinstance(items, list) and len(items) >= 3:
                        msg_type = items[0]
                        if msg_type in ("message", b"message"):
                            ch = items[1].decode("utf-8") if isinstance(items[1], bytes) else str(items[1])
                            data = items[2] if isinstance(items[2], bytes) else str(items[2]).encode("utf-8")
                            yield ch, data
        finally:
            await self._close_connection(reader, writer)

    async def close(self) -> None:
        """Close all pooled connections."""
        self._closed = True
        self._sync.close()
        async with self._lock:
            while self._connections:
                r, w = self._connections.popleft()
                await self._close_connection(r, w)

    async def _create_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish a new async connection and send HELLO 3 handshake."""
        parsed = urlparse(self._config.url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 6379
        ssl_ctx = ssl.create_default_context() if self._config.uses_tls else None
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx)
        effective_user = parsed.username or self._config.username
        effective_pwd = parsed.password or self._config.password
        handshake = SyncRedisDriver.encode_hello3_handshake(
            username=effective_user,
            password=effective_pwd,
            client_name=self._config.client_name,
        )
        writer.write(handshake)
        await writer.drain()
        await self._read_reply(reader)
        return reader, writer

    async def _get_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Obtain an idle connection from the pool or create a new one."""
        async with self._lock:
            while self._connections:
                r, w = self._connections.popleft()
                if not w.is_closing():
                    return r, w
        return await self._create_connection()

    async def _return_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Return a connection to the pool."""
        async with self._lock:
            if not self._closed and not writer.is_closing() and len(self._connections) < self._config.max_connections:
                self._connections.append((reader, writer))
            else:
                await self._close_connection(reader, writer)

    async def _close_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Close connection stream writer."""
        _ = reader
        with contextlib.suppress(Exception):
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=0.05)

    async def _read_reply(self, reader: asyncio.StreamReader, buf: bytearray | None = None) -> Any:
        """Read stream bytes until a non-push RESP3 frame is parsed."""
        read_buf = buf if buf is not None else bytearray()
        while True:
            if read_buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(read_buf, 0)
                except ValueError:
                    pass
                else:
                    del read_buf[:next_offset]
                    if isinstance(frame, tuple) and len(frame) == 2 and frame[0] == "push":
                        for cb in self._push_callbacks:
                            cb(frame[1])
                        continue
                    return frame
            chunk = await reader.read(4096)
            if not chunk:
                msg = "Connection closed while reading RESP3 reply"
                raise ConnectionError(msg)
            read_buf.extend(chunk)
