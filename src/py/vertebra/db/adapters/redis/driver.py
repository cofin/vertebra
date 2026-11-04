"""Synchronous and asynchronous Redis RESP3 drivers."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import fnmatch
import hashlib
import inspect
import socket
import ssl
import threading
import time
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from vertebra.db.adapters.redis.config import RedisConfig
from vertebra.db.adapters.redis.core import (
    NON_KEY_COMMANDS,
    PubSubMessage,
    RedisCore,
    Resp3Error,
    Resp3Parser,
    encode_hello3_handshake,
    encode_resp_command,
    hash_slot,
    parse_cluster_redirect,
)
from vertebra.db.driver import REDIS_DIALECT_TAG, CommonDriverMixin
from vertebra.typing import ImproperConfigurationError, VertebraError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Generator, Iterator, Sequence

__all__ = (
    "AsyncRedisDriver",
    "SyncRedisDriver",
)


class SyncRedisDriver(CommonDriverMixin[RedisConfig]):
    """Synchronous Redis RESP3 driver with topology routing, client cache, and pub/sub."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG

    __slots__ = (
        "_client_cache",
        "_connections",
        "_invalidation_listeners",
        "_kv",
        "_lock",
        "_master_address",
        "_node_connections",
        "_pubsub_queue",
        "_push_callbacks",
        "_scripts",
        "_slot_map",
        "_socket_buffers",
        "_socket_factory",
        "_socket_nodes",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shards",
        "_ttls",
    )

    def __init__(
        self,
        config: RedisConfig | None = None,
        *,
        broker: Any = None,
        socket_factory: Callable[[tuple[str, int]], socket.socket] | None = None,
    ) -> None:
        resolved = config or RedisConfig()
        super().__init__(resolved, broker=broker)
        self._socket_factory = socket_factory
        self._kv: dict[str, bytes] = {}
        self._ttls: dict[str, float] = {}
        self._scripts: dict[str, str] = {}
        self._connections: collections.deque[socket.socket] = collections.deque()
        self._node_connections: dict[tuple[str, int], collections.deque[socket.socket]] = {}
        self._socket_buffers: dict[int, bytearray] = {}
        self._socket_nodes: dict[int, tuple[str, int]] = {}
        self._slot_map: dict[int, tuple[str, int]] = {}
        self._master_address: tuple[str, int] | None = None
        self._client_cache: collections.OrderedDict[str, tuple[bytes, float | None]] = collections.OrderedDict()
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._push_callbacks: list[Callable[[list[Any]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._subscribed_channels: set[str] = set()
        self._subscribed_patterns: set[str] = set()
        self._subscribed_shards: set[str] = set()
        self._lock = threading.Lock()
        self.register_push_callback(self._on_push_frame)

    @property
    def _config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        if not self.config.url.startswith("memory://"):
            self._drain_idle_sockets()
        now = time.monotonic()
        expired: list[str] = []
        for k, entry in self._client_cache.items():
            if isinstance(entry, tuple) and entry[1] is not None and now >= entry[1]:
                expired.append(k)
        for k in expired:
            self._client_cache.pop(k, None)
        return tuple(self._client_cache.keys())

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return self._slot_map

    def attach_broker(self, broker: Any) -> None:
        """Attach a `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, or `DatabaseBroker` for push event forwarding."""
        self._broker = broker

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    @staticmethod
    def encode_command(*parts: str | bytes | int) -> bytes:
        """Encode a Redis or Valkey RESP3 bulk-string array command."""
        return encode_resp_command(*parts)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = "vertebra",
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake command with optional AUTH and SETNAME."""
        return encode_hello3_handshake(
            username=username,
            password=password,
            client_name=client_name,
        )

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> bytes:
        """Encode a batch of pipelined RESP3 commands into a single wire buffer."""
        return b"".join(self.encode_command(*cmd) for cmd in commands)

    def register_push_callback(self, callback: Callable[[list[Any]], None]) -> None:
        """Register a handler for incoming RESP3 push frames."""
        if callback not in self._push_callbacks:
            self._push_callbacks.append(callback)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process a RESP3 invalidate push frame, evicting local entries and invoking listeners."""
        if keys is None:
            normalized = list(self._client_cache.keys())
            evicted = len(normalized)
            self._client_cache.clear()
        else:
            normalized = [RedisCore.decode_part(k) for k in keys]
            evicted = 0
            for k in normalized:
                if self._client_cache.pop(k, None) is not None:
                    evicted += 1
        for listener in self._invalidation_listeners:
            listener(normalized)
        self._notify_broker_invalidation(normalized)
        return evicted

    def discover_sentinel_master(self) -> tuple[str, int]:
        """Query configured Sentinel nodes for the active primary (host, port)."""
        master_name = self.config.effective_sentinel_master_name
        if not master_name:
            msg = "sentinel_master_name or sentinel_service_name must be set for Sentinel discovery"
            raise ImproperConfigurationError(msg)
        candidates = self._get_sentinel_candidates()
        last_err: Exception | None = None
        for s_host, s_port in candidates:
            sock: socket.socket | None = None
            try:
                sock = self._create_raw_socket(s_host, s_port)
                cmd = self.encode_command("SENTINEL", "get-master-addr-by-name", master_name)
                sock.sendall(cmd)
                buf = bytearray()
                reply = self._read_socket_reply(sock, buf=buf)
                if isinstance(reply, dict):
                    reply = self._read_socket_reply(sock, buf=buf)
                if isinstance(reply, (list, tuple)) and len(reply) >= 2:
                    m_host = RedisCore.decode_part(reply[0])
                    m_port = int(RedisCore.decode_part(reply[1]))
                    self._master_address = (m_host, m_port)
                    return self._master_address
            except Exception as err:
                last_err = err
            finally:
                if sock is not None:
                    self._close_socket(sock)
        msg = f"Unable to resolve Sentinel master {master_name!r} from {candidates}"
        if last_err is not None:
            raise VertebraError(msg) from last_err
        raise VertebraError(msg)

    def get(self, key: str) -> bytes | None:
        """Fetch a key value, consulting the bounded LRU client-side cache when enabled."""
        if not self.config.url.startswith("memory://"):
            self._drain_idle_sockets()
        if self.config.effective_tracking_enabled:
            cached = self._cache_get(key)
            if cached is not None:
                return cached
        if self.config.url.startswith("memory://"):
            with self._lock:
                exp = self._ttls.get(key)
                if exp is not None and time.monotonic() >= exp:
                    self._kv.pop(key, None)
                    self._ttls.pop(key, None)
                    return None
                val = self._kv.get(key)
            if val is not None and self.config.effective_tracking_enabled:
                self._cache_put(key, val)
            return val
        res = self.execute_command("GET", key)
        out = bytes(res) if isinstance(res, (bytes, bytearray)) else None
        if out is not None and self.config.effective_tracking_enabled:
            self._cache_put(key, out)
        return out

    def set(self, key: str, value: bytes | str, *, ex: int | None = None, px: int | None = None) -> None:
        """Set a key value with an optional TTL and update the client-side cache.

        Args:
            key: Key to write.
            value: Value bytes or text.
            ex: Expiry in whole seconds.
            px: Expiry in milliseconds.

        Raises:
            ValueError: When both ``ex`` and ``px`` are given.
        """
        if ex is not None and px is not None:
            raise ValueError("set() accepts ex or px, not both")
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        ttl_seconds = float(ex) if ex is not None else (px / 1000.0 if px is not None else None)
        if self.config.effective_tracking_enabled:
            self._cache_put(key, raw, ttl_seconds=ttl_seconds)
        if self.config.url.startswith("memory://"):
            with self._lock:
                self._kv[key] = raw
                if ttl_seconds is not None:
                    self._ttls[key] = time.monotonic() + ttl_seconds
                else:
                    self._ttls.pop(key, None)
            return
        if ex is not None:
            self.execute_command("SET", key, raw, "EX", ex)
        elif px is not None:
            self.execute_command("SET", key, raw, "PX", px)
        else:
            self.execute_command("SET", key, raw)

    def delete(self, key: str) -> bool:
        """Delete a key and evict it from the client-side cache."""
        self._client_cache.pop(key, None)
        if self.config.url.startswith("memory://"):
            with self._lock:
                self._ttls.pop(key, None)
                return self._kv.pop(key, None) is not None
        res = self.execute_command("DEL", key)
        return bool(res and res > 0)

    def scan(self, cursor: int = 0, *, match: str | None = None, count: int = 500) -> tuple[int, list[str]]:
        """Run one ``SCAN`` step and return the next cursor and the matching keys.

        In ``memory://`` mode all matching keys are returned with cursor ``0``.
        """
        if self.config.url.startswith("memory://"):
            with self._lock:
                keys = [k for k in self._kv if match is None or fnmatch.fnmatchcase(k, match)]
            return 0, keys
        parts: list[str | int] = ["SCAN", cursor]
        if match is not None:
            parts.extend(["MATCH", match])
        parts.extend(["COUNT", count])
        reply = self.execute_command(*parts)
        return int(RedisCore.decode_part(reply[0])), [RedisCore.decode_part(k) for k in reply[1]]

    def unlink(self, *keys: str) -> int:
        """Remove ``keys`` with ``UNLINK`` and evict them from the client-side cache."""
        if not keys:
            return 0
        for key in keys:
            self._client_cache.pop(key, None)
        if self.config.url.startswith("memory://"):
            removed = 0
            with self._lock:
                for key in keys:
                    self._ttls.pop(key, None)
                    if self._kv.pop(key, None) is not None:
                        removed += 1
            return removed
        res = self.execute_command("UNLINK", *keys)
        return int(res) if isinstance(res, int) else 0

    def pttl(self, key: str) -> int:
        """Return the remaining time to live of a key in milliseconds."""
        if self.config.url.startswith("memory://"):
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
        """Clear all keys in the store and local client-side cache."""
        self._client_cache.clear()
        if self.config.url.startswith("memory://"):
            with self._lock:
                self._kv.clear()
                self._ttls.clear()
            return
        self.execute_command("FLUSHDB")

    def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to a Redis or Valkey pub/sub channel."""
        raw = message.encode("utf-8") if isinstance(message, str) else bytes(message)
        if self.config.url.startswith("memory://"):
            return self._publish_memory(channel, raw)
        res = self.execute_command("PUBLISH", channel, raw)
        return int(res) if isinstance(res, int) else 1

    def subscribe(self, *channels: str) -> _SyncSubscriptionStream:
        """Subscribe to channels on the multiplexed RESP3 connection and return a message stream."""
        for ch in channels:
            self._subscribed_channels.add(ch)
        if channels and not self.config.url.startswith("memory://"):
            self._send_pubsub_command("SUBSCRIBE", *channels)
        return _SyncSubscriptionStream(self, channels)

    def psubscribe(self, *patterns: str) -> _SyncSubscriptionStream:
        """Subscribe to glob patterns on the multiplexed RESP3 connection."""
        for pat in patterns:
            self._subscribed_patterns.add(pat)
        if patterns and not self.config.url.startswith("memory://"):
            self._send_pubsub_command("PSUBSCRIBE", *patterns)
        return _SyncSubscriptionStream(self, patterns)

    def ssubscribe(self, *shard_channels: str) -> _SyncSubscriptionStream:
        """Subscribe to cluster shard channels (SSUBSCRIBE) on the multiplexed RESP3 connection."""
        for sch in shard_channels:
            self._subscribed_shards.add(sch)
        if shard_channels and not self.config.url.startswith("memory://"):
            self._send_pubsub_command("SSUBSCRIBE", *shard_channels)
        return _SyncSubscriptionStream(self, shard_channels)

    def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from the specified channels (or all channels when empty)."""
        if channels:
            for ch in channels:
                self._subscribed_channels.discard(ch)
        else:
            self._subscribed_channels.clear()
        if not self.config.url.startswith("memory://"):
            self._send_pubsub_command("UNSUBSCRIBE", *channels)

    def drain_messages(self) -> list[PubSubMessage]:
        """Drain and return all pending PubSubMessage items from the multiplexed subscription queue."""
        if not self.config.url.startswith("memory://"):
            self._drain_idle_sockets()
        messages: list[PubSubMessage] = []
        while self._pubsub_queue:
            messages.append(self._pubsub_queue.popleft())
        return messages

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
        """Execute a single RESP3 command with Cluster MOVED/ASK redirection and Sentinel support."""
        if self.config.url.startswith("memory://"):
            return self._execute_memory_command(*parts)

        target_node = self._select_target_node(parts)
        asking = False
        max_redirects = max(0, self.config.max_redirections)

        for attempt in range(max_redirects + 1):
            sock, from_pool = self._get_socket_for_node(target_node)
            try:
                buf = self._socket_buffers.setdefault(id(sock), bytearray())
                if asking:
                    sock.sendall(self.encode_command("ASKING") + self.encode_command(*parts))
                    self._read_socket_reply(sock, buf=buf)
                    reply = self._read_socket_reply(sock, buf=buf)
                else:
                    sock.sendall(self.encode_command(*parts))
                    reply = self._read_socket_reply(sock, buf=buf)
                self._consume_buffered_push_frames(buf)
                self._return_socket(sock, target_node)
                return reply
            except Resp3Error as err:
                self._return_socket(sock, target_node)
                redirect = parse_cluster_redirect(str(err))
                if redirect is None or attempt >= max_redirects:
                    raise
                if redirect.kind == "MOVED":
                    self._slot_map[redirect.slot] = (redirect.host, redirect.port)
                    target_node = (redirect.host, redirect.port)
                    asking = False
                else:
                    target_node = (redirect.host, redirect.port)
                    asking = True
            except ConnectionError:
                self._close_socket(sock)
                if from_pool and not asking:
                    sock2, _ = self._get_socket_for_node(target_node, force_new=True)
                    try:
                        buf2 = self._socket_buffers.setdefault(id(sock2), bytearray())
                        sock2.sendall(self.encode_command(*parts))
                        reply2 = self._read_socket_reply(sock2, buf=buf2)
                        self._consume_buffered_push_frames(buf2)
                        self._return_socket(sock2, target_node)
                        return reply2
                    except Resp3Error as err2:
                        self._return_socket(sock2, target_node)
                        redirect = parse_cluster_redirect(str(err2))
                        if redirect is None or attempt >= max_redirects:
                            raise
                        if redirect.kind == "MOVED":
                            self._slot_map[redirect.slot] = (redirect.host, redirect.port)
                            target_node = (redirect.host, redirect.port)
                            asking = False
                        else:
                            target_node = (redirect.host, redirect.port)
                            asking = True
                    except Exception:
                        self._close_socket(sock2)
                        raise
                else:
                    raise
            except Exception:
                self._close_socket(sock)
                raise

        msg = f"Exceeded maximum cluster redirections ({max_redirects})"
        raise Resp3Error(msg)

    def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of pipelined RESP3 commands into a single wire buffer."""
        if not commands:
            return []
        if self.config.url.startswith("memory://"):
            return [self._execute_memory_command(*cmd) for cmd in commands]
        wire_buf = self.encode_pipeline(commands)
        target_node = self._select_target_node(commands[0])
        sock, _ = self._get_socket_for_node(target_node)
        try:
            sock.sendall(wire_buf)
            results: list[Any] = []
            buf = self._socket_buffers.setdefault(id(sock), bytearray())
            for _ in range(len(commands)):
                results.append(self._read_socket_reply(sock, buf=buf))
            self._consume_buffered_push_frames(buf)
            return results
        except Exception:
            self._close_socket(sock)
            raise
        else:
            self._return_socket(sock, target_node)

    def reconnect(self) -> socket.socket:
        """Close all pooled connections and establish a fresh socket with updated handshake/IAM token."""
        self.close()
        self._closed = False
        sock = self._open_socket()
        self._return_socket(sock, self._default_node())
        return sock

    def close(self) -> None:
        """Close all pooled socket connections."""
        self._closed = True
        with self._lock:
            while self._connections:
                sock = self._connections.popleft()
                self._close_socket(sock)
            for pool in self._node_connections.values():
                while pool:
                    self._close_socket(pool.popleft())
            self._node_connections.clear()

    def _cache_get(self, key: str) -> bytes | None:
        """Retrieve a value from the bounded LRU client cache if present and unexpired."""
        entry = self._client_cache.get(key)
        if entry is None:
            return None
        if isinstance(entry, tuple):
            value, expires_at = entry
            if expires_at is not None and time.monotonic() >= expires_at:
                self._client_cache.pop(key, None)
                return None
            self._client_cache.move_to_end(key)
            return value
        self._client_cache.move_to_end(key)
        return bytes(entry)

    def _cache_put(self, key: str, value: bytes, *, ttl_seconds: float | None = None) -> None:
        """Insert or update a key in the bounded LRU client cache, evicting oldest entries on overflow."""
        capacity = self.config.effective_cache_capacity
        if capacity <= 0:
            self._client_cache.clear()
            return
        effective_ttl = ttl_seconds if ttl_seconds is not None else self.config.client_cache_ttl_seconds
        expires_at = (time.monotonic() + effective_ttl) if effective_ttl is not None else None
        self._client_cache[key] = (value, expires_at)
        self._client_cache.move_to_end(key)
        while len(self._client_cache) > capacity:
            self._client_cache.popitem(last=False)

    def _on_push_frame(self, items: list[Any]) -> None:
        """Handle RESP3 push frames for client cache invalidation and multiplexed Pub/Sub."""
        if not items:
            return
        unwrapped = items
        first = RedisCore.decode_part(unwrapped[0]).lower()
        if first == "pubsub" and len(unwrapped) > 1:
            unwrapped = unwrapped[1:]
            first = RedisCore.decode_part(unwrapped[0]).lower()

        if first == "invalidate":
            raw_keys = unwrapped[1] if len(unwrapped) > 1 else None
            if raw_keys is None or isinstance(raw_keys, (list, tuple, set)):
                self.handle_invalidation_push(list(raw_keys) if raw_keys is not None else None)
            return

        if first in ("message", "smessage") and len(unwrapped) >= 3:
            ch = RedisCore.decode_part(unwrapped[1])
            payload = RedisCore.ensure_bytes(unwrapped[2])
            msg = PubSubMessage(kind=first, channel=ch, payload=payload, pattern=None)
            self._pubsub_queue.append(msg)
            self._notify_broker_pubsub(ch, payload)
            return

        if first == "pmessage" and len(unwrapped) >= 4:
            pat = RedisCore.decode_part(unwrapped[1])
            ch = RedisCore.decode_part(unwrapped[2])
            payload = RedisCore.ensure_bytes(unwrapped[3])
            msg = PubSubMessage(kind="pmessage", channel=ch, payload=payload, pattern=pat)
            self._pubsub_queue.append(msg)
            self._notify_broker_pubsub(ch, payload)

    def _notify_broker_invalidation(self, keys: Sequence[str]) -> None:
        """Forward cache invalidation events to an attached broker transport when present."""
        if self._broker is None:
            return
        pub_inv = getattr(self._broker, "publish_invalidation", None)
        if callable(pub_inv):
            res = pub_inv(list(keys))
            self._schedule_if_awaitable(res)
            return
        pub_fn = getattr(self._broker, "publish", None)
        if callable(pub_fn):
            payload = ",".join(keys).encode("utf-8")
            res = pub_fn("invalidate", payload)
            self._schedule_if_awaitable(res)

    def _notify_broker_pubsub(self, channel: str, payload: bytes) -> None:
        """Forward pub/sub push messages to an attached broker transport when present."""
        if self._broker is None:
            return
        pub_fn = getattr(self._broker, "publish", None)
        if callable(pub_fn):
            res = pub_fn(channel, payload)
            self._schedule_if_awaitable(res)

    @staticmethod
    def _schedule_if_awaitable(value: Any) -> None:
        """Schedule a coroutine on the active event loop if one is running."""
        if inspect.iscoroutine(value):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(value)
            except RuntimeError:
                with contextlib.suppress(Exception):
                    value.close()

    def _publish_memory(self, channel: str, payload: bytes) -> int:
        """Deliver published message to matching in-memory subscriptions and attached broker."""
        delivered = 0
        if channel in self._subscribed_channels:
            self._pubsub_queue.append(PubSubMessage(kind="message", channel=channel, payload=payload, pattern=None))
            delivered += 1
        for pat in self._subscribed_patterns:
            if fnmatch.fnmatchcase(channel, pat):
                self._pubsub_queue.append(PubSubMessage(kind="pmessage", channel=channel, payload=payload, pattern=pat))
                delivered += 1
        if channel in self._subscribed_shards:
            self._pubsub_queue.append(PubSubMessage(kind="smessage", channel=channel, payload=payload, pattern=None))
            delivered += 1
        self._notify_broker_pubsub(channel, payload)
        return max(1, delivered)

    def _send_pubsub_command(self, cmd: str, *targets: str) -> None:
        """Send a SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE, or UNSUBSCRIBE command and process initial frames."""
        node = self._default_node()
        sock, _ = self._get_socket_for_node(node)
        try:
            buf = self._socket_buffers.setdefault(id(sock), bytearray())
            sock.sendall(self.encode_command(cmd, *targets))
            self._read_pubsub_ack_or_push(sock, buf)
        except Exception:
            self._close_socket(sock)
            raise
        else:
            self._return_socket(sock, node)

    def _read_pubsub_ack_or_push(self, sock: socket.socket, buf: bytearray) -> None:
        """Read initial pub/sub confirmation and any piggybacked push frames from sock."""
        received_any = False
        while True:
            while buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                except ValueError:
                    break
                del buf[:next_offset]
                received_any = True
                self._dispatch_parsed_frame(frame)
            if received_any:
                try:
                    sock.setblocking(False)
                    extra = sock.recv(4096)
                    sock.setblocking(True)
                    if extra:
                        buf.extend(extra)
                        continue
                except (BlockingIOError, ssl.SSLWantReadError):
                    sock.setblocking(True)
                except Exception:
                    sock.setblocking(True)
                break
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf.extend(chunk)

    def _dispatch_parsed_frame(self, frame: Any) -> None:
        """Route a parsed push or pub/sub array frame to registered callbacks."""
        if isinstance(frame, tuple) and len(frame) == 2 and frame[0] == "push":
            for cb in self._push_callbacks:
                cb(frame[1])
            return
        if isinstance(frame, list) and frame:
            tag = RedisCore.decode_part(frame[0]).lower()
            if tag in ("message", "pmessage", "smessage", "invalidate", "pubsub"):
                for cb in self._push_callbacks:
                    cb(frame)

    def _consume_buffered_push_frames(self, buf: bytearray) -> None:
        """Extract and dispatch any complete push frames sitting at the head of buf."""
        while buf and buf[0] in (62, 42):
            try:
                frame, next_offset = Resp3Parser.parse_frame(buf, 0)
            except (ValueError, Resp3Error):
                break
            if isinstance(frame, tuple) and len(frame) == 2 and frame[0] == "push":
                del buf[:next_offset]
                for cb in self._push_callbacks:
                    cb(frame[1])
                continue
            if isinstance(frame, list) and frame:
                tag = RedisCore.decode_part(frame[0]).lower()
                if tag in (
                    "message",
                    "pmessage",
                    "smessage",
                    "invalidate",
                    "pubsub",
                    "subscribe",
                    "psubscribe",
                    "ssubscribe",
                    "unsubscribe",
                    "punsubscribe",
                    "sunsubscribe",
                ):
                    del buf[:next_offset]
                    if tag in ("message", "pmessage", "smessage", "invalidate", "pubsub"):
                        for cb in self._push_callbacks:
                            cb(frame)
                    continue
            break

    def _drain_idle_sockets(self) -> None:
        """Non-blockingly read and process pending push frames across all pooled sockets."""
        with self._lock:
            all_sockets = list(self._connections)
            for pool in self._node_connections.values():
                for s in pool:
                    if s not in all_sockets:
                        all_sockets.append(s)
        for sock in all_sockets:
            buf = self._socket_buffers.setdefault(id(sock), bytearray())
            try:
                sock.setblocking(False)
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    buf.extend(chunk)
            except (BlockingIOError, ssl.SSLWantReadError):
                pass
            except Exception:
                continue
            finally:
                with contextlib.suppress(Exception):
                    sock.setblocking(True)
            self._consume_buffered_push_frames(buf)

    def _get_sentinel_candidates(self) -> list[tuple[str, int]]:
        """Return the ordered list of Sentinel (host, port) endpoints."""
        if self.config.sentinel_nodes:
            return [RedisCore.normalize_node(n, default_port=26379) for n in self.config.sentinel_nodes]
        parsed = urlparse(self.config.url)
        return [(parsed.hostname or "127.0.0.1", parsed.port or 26379)]

    def _default_node(self) -> tuple[str, int]:
        """Return the default (host, port) target node."""
        if self._master_address is not None:
            return self._master_address
        if self.config.effective_sentinel_enabled:
            return self.discover_sentinel_master()
        if self.config.cluster_nodes:
            return RedisCore.normalize_node(self.config.cluster_nodes[0], default_port=6379)
        parsed = urlparse(self.config.url)
        return (parsed.hostname or "127.0.0.1", parsed.port or 6379)

    def _select_target_node(self, parts: Sequence[str | bytes | int]) -> tuple[str, int]:
        """Select the target (host, port) for a command based on Cluster slot map or default node."""
        if not parts:
            return self._default_node()
        if self.config.effective_cluster_enabled or self._slot_map:
            cmd_name = RedisCore.decode_part(parts[0]).upper()
            if len(parts) >= 2 and cmd_name not in NON_KEY_COMMANDS:
                key_arg = parts[1]
                key_bytes = RedisCore.encode_part_bytes(key_arg)
                slot = hash_slot(key_bytes)
                mapped = self._slot_map.get(slot)
                if mapped is not None:
                    return mapped
        return self._default_node()

    def _resolve_iam_token(self) -> str | None:
        """Fetch a fresh GCP Memorystore IAM token synchronously when configured."""
        if not self.config.effective_iam_enabled:
            return None
        provider = self.config.effective_token_provider
        if provider is None:
            return None
        token = provider()
        if inspect.isawaitable(token):
            close_fn = getattr(token, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()
            msg = "Async token_provider cannot be used with SyncRedisDriver; use AsyncRedisDriver"
            raise ImproperConfigurationError(msg)
        return str(token)

    def _build_handshake_frames(self, *, iam_token: str | None = None) -> list[bytes]:
        """Build the sequence of handshake command frames for a newly opened connection."""
        return self.config.build_handshake_commands(iam_token=iam_token)

    def _create_raw_socket(self, host: str, port: int) -> socket.socket:
        """Create a raw TCP or TLS socket to (host, port)."""
        if self._socket_factory is not None:
            return self._socket_factory((host, port))
        sock = socket.create_connection((host, port), timeout=10.0)
        if self.config.uses_tls:
            ctx = ssl.create_default_context()
            sock = ctx.wrap_socket(sock, server_hostname=host)
        return sock

    def _open_socket(self, host: str | None = None, port: int | None = None) -> socket.socket:
        """Create and initialize a new TCP or TLS socket with HELLO 3, IAM auth, and CLIENT TRACKING."""
        if host is None or port is None:
            default_host, default_port = self._default_node()
            target_host = host or default_host
            target_port = port or default_port
        else:
            target_host, target_port = host, port
        sock = self._create_raw_socket(target_host, target_port)
        buf = bytearray()
        self._socket_buffers[id(sock)] = buf
        self._socket_nodes[id(sock)] = (target_host, target_port)
        try:
            iam_token = self._resolve_iam_token()
            frames = self._build_handshake_frames(iam_token=iam_token)
            for cmd_bytes in frames:
                sock.sendall(cmd_bytes)
                self._read_socket_reply(sock, buf=buf)
            self._consume_buffered_push_frames(buf)
        except Exception:
            self._close_socket(sock)
            raise
        return sock

    def _connect_socket(self, host: str | None = None, port: int | None = None) -> socket.socket:
        """Create and initialize a new TCP or TLS socket connection with handshake."""
        return self._open_socket(host=host, port=port)

    def _get_socket(self) -> socket.socket:
        """Retrieve an idle socket from the default pool or create a new one."""
        sock, _ = self._get_socket_for_node(self._default_node())
        return sock

    def _get_socket_for_node(
        self,
        node: tuple[str, int],
        *,
        force_new: bool = False,
    ) -> tuple[socket.socket, bool]:
        """Retrieve an idle socket for node from the pool or open a new connection."""
        default_node = self._default_node()
        if not force_new:
            with self._lock:
                pool = self._connections if node == default_node else self._node_connections.get(node)
                while pool:
                    s = pool.popleft()
                    if node == default_node and node in self._node_connections:
                        with contextlib.suppress(ValueError):
                            self._node_connections[node].remove(s)
                    try:
                        s.setblocking(False)
                        data = s.recv(1, socket.MSG_PEEK)
                        s.setblocking(True)
                        if data == b"":
                            self._close_socket(s)
                            continue
                    except (BlockingIOError, ssl.SSLWantReadError):
                        s.setblocking(True)
                    except Exception:
                        self._close_socket(s)
                        continue
                    buf = self._socket_buffers.setdefault(id(s), bytearray())
                    self._consume_buffered_push_frames(buf)
                    return s, True
        return self._open_socket(host=node[0], port=node[1]), False

    def _return_socket(self, sock: socket.socket, node: tuple[str, int] | None = None) -> None:
        """Return an active socket to the pool if capacity permits."""
        target = node or self._socket_nodes.get(id(sock)) or self._default_node()
        default_node = self._default_node()
        with self._lock:
            if self._closed:
                self._close_socket(sock)
                return
            if target == default_node:
                if len(self._connections) < self.config.max_connections:
                    self._connections.append(sock)
                else:
                    self._close_socket(sock)
            else:
                pool = self._node_connections.setdefault(target, collections.deque())
                if len(pool) < self.config.max_connections:
                    pool.append(sock)
                else:
                    self._close_socket(sock)

    def _close_socket(self, sock: socket.socket) -> None:
        """Safely close a socket and clean up its buffer metadata."""
        self._socket_buffers.pop(id(sock), None)
        self._socket_nodes.pop(id(sock), None)
        with contextlib.suppress(Exception):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(Exception):
            sock.close()

    def _read_socket_reply(self, sock: socket.socket, buf: bytearray | None = None) -> Any:
        """Read bytes until a complete non-push RESP3 frame is parsed."""
        read_buf = buf if buf is not None else self._socket_buffers.setdefault(id(sock), bytearray())
        while True:
            if read_buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(read_buf, 0)
                except Resp3Error as err:
                    if err.next_offset > 0:
                        del read_buf[: err.next_offset]
                    raise
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

    @staticmethod
    async def _read_stream_reply_once(reader: asyncio.StreamReader, buf: bytearray) -> Any:
        """Read a single RESP3 reply from an async StreamReader."""
        while True:
            if buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                except Resp3Error as err:
                    if err.next_offset > 0:
                        del buf[: err.next_offset]
                    raise
                except ValueError:
                    pass
                else:
                    del buf[:next_offset]
                    if isinstance(frame, tuple) and len(frame) == 2 and frame[0] == "push":
                        continue
                    return frame
            chunk = await reader.read(4096)
            if not chunk:
                msg = "Connection closed while reading RESP3 reply"
                raise ConnectionError(msg)
            buf.extend(chunk)

    def _execute_memory_command(self, *parts: str | bytes | int) -> Any:
        """Simulate RESP3 commands against in-memory dictionary state."""
        if not parts:
            return None
        cmd = RedisCore.decode_part(parts[0]).upper()
        if cmd == "GET":
            k = RedisCore.decode_part(parts[1])
            return self.get(k)
        if cmd == "SET":
            k = RedisCore.decode_part(parts[1])
            raw_v = parts[2]
            v = str(raw_v) if isinstance(raw_v, int) else raw_v
            ex = None
            if len(parts) >= 5 and RedisCore.decode_part(parts[3]).upper() == "EX":
                ex = int(parts[4])
            px = int(parts[4]) if len(parts) >= 5 and RedisCore.decode_part(parts[3]).upper() == "PX" else None
            self.set(k, v, ex=ex, px=px)
            return "OK"
        if cmd == "DEL":
            count = 0
            for raw_k in parts[1:]:
                k = RedisCore.decode_part(raw_k)
                if self.delete(k):
                    count += 1
            return count
        if cmd == "PTTL":
            k = RedisCore.decode_part(parts[1])
            return self.pttl(k)
        if cmd == "FLUSHDB":
            self.flushdb()
            return "OK"
        if cmd == "PUBLISH":
            ch = RedisCore.decode_part(parts[1])
            payload = RedisCore.ensure_bytes(parts[2]) if len(parts) > 2 else b""
            return self._publish_memory(ch, payload)
        if cmd == "PING":
            return parts[1] if len(parts) > 1 else "PONG"
        if cmd == "EVAL":
            script = RedisCore.decode_part(parts[1])
            sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
            numkeys = int(parts[2])
            keys = [RedisCore.decode_part(parts[3 + i]) for i in range(numkeys)]
            args = list(parts[3 + numkeys :])
            self._scripts[sha] = script
            return self._eval_memory_script(script, keys, args)
        if cmd == "EVALSHA":
            sha = RedisCore.decode_part(parts[1])
            if sha not in self._scripts:
                msg = f"NOSCRIPT No matching script. Please use EVAL. (sha: {sha})"
                raise Resp3Error(msg)
            script = self._scripts[sha]
            numkeys = int(parts[2])
            keys = [RedisCore.decode_part(parts[3 + i]) for i in range(numkeys)]
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


class AsyncRedisDriver(CommonDriverMixin[RedisConfig]):
    """Asynchronous Redis RESP3 driver with topology routing, client cache, and pub/sub."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG

    __slots__ = (
        "_client_cache",
        "_connections",
        "_idle_readers",
        "_invalidation_listeners",
        "_lock",
        "_master_address",
        "_node_connections",
        "_pubsub_queue",
        "_push_callbacks",
        "_reader_buffers",
        "_reader_nodes",
        "_slot_map",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shards",
        "_sync",
    )

    def __init__(self, config: RedisConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or RedisConfig()
        super().__init__(resolved, broker=broker)
        self._sync = SyncRedisDriver(self.config, broker=self._broker)
        self._client_cache = self._sync._client_cache
        self._invalidation_listeners = self._sync._invalidation_listeners
        self._slot_map = self._sync._slot_map
        self._pubsub_queue = self._sync._pubsub_queue
        self._subscribed_channels = self._sync._subscribed_channels
        self._subscribed_patterns = self._sync._subscribed_patterns
        self._subscribed_shards = self._sync._subscribed_shards
        self._connections: collections.deque[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = collections.deque()
        self._node_connections: dict[
            tuple[str, int],
            collections.deque[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
        ] = {}
        self._reader_buffers: dict[int, bytearray] = {}
        self._reader_nodes: dict[int, tuple[str, int]] = {}
        self._idle_readers: set[int] = set()
        self._master_address: tuple[str, int] | None = None
        self._push_callbacks: list[Callable[[list[Any]], None]] = [self._sync._on_push_frame]
        self._lock = asyncio.Lock()

    @property
    def _config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        self._drain_idle_readers()
        return self._sync.cached_keys

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return self._slot_map

    def attach_broker(self, broker: Any) -> None:
        """Attach a `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, or `DatabaseBroker` for push event forwarding."""
        self._broker = broker
        self._sync.attach_broker(broker)

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    def register_push_callback(self, callback: Callable[[list[Any]], None]) -> None:
        """Register a handler for incoming RESP3 push frames."""
        if callback not in self._push_callbacks:
            self._push_callbacks.append(callback)
        self._sync.register_push_callback(callback)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._sync.add_invalidation_listener(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process a RESP3 invalidate push frame, evicting local entries and invoking listeners."""
        return self._sync.handle_invalidation_push(keys)

    async def discover_sentinel_master(self) -> tuple[str, int]:
        """Query configured Sentinel nodes asynchronously for the active primary (host, port)."""
        master_name = self.config.effective_sentinel_master_name
        if not master_name:
            msg = "sentinel_master_name or sentinel_service_name must be set for Sentinel discovery"
            raise ImproperConfigurationError(msg)
        candidates = self._sync._get_sentinel_candidates()
        last_err: Exception | None = None
        for s_host, s_port in candidates:
            writer: asyncio.StreamWriter | None = None
            try:
                ssl_ctx = ssl.create_default_context() if self.config.uses_tls else None
                reader, writer = await asyncio.open_connection(s_host, s_port, ssl=ssl_ctx)
                writer.write(SyncRedisDriver.encode_command("SENTINEL", "get-master-addr-by-name", master_name))
                await writer.drain()
                buf = bytearray()
                reply = await SyncRedisDriver._read_stream_reply_once(reader, buf)
                if isinstance(reply, dict):
                    reply = await SyncRedisDriver._read_stream_reply_once(reader, buf)
                if isinstance(reply, (list, tuple)) and len(reply) >= 2:
                    m_host = RedisCore.decode_part(reply[0])
                    m_port = int(RedisCore.decode_part(reply[1]))
                    self._master_address = (m_host, m_port)
                    return self._master_address
            except Exception as err:
                last_err = err
            finally:
                if writer is not None:
                    with contextlib.suppress(Exception):
                        writer.close()
                        await asyncio.wait_for(writer.wait_closed(), timeout=0.05)
        msg = f"Unable to resolve Sentinel master {master_name!r} from {candidates}"
        if last_err is not None:
            raise VertebraError(msg) from last_err
        raise VertebraError(msg)

    async def get(self, key: str) -> bytes | None:
        """Asynchronously fetch a key value, consulting the bounded LRU client cache when enabled."""
        self._drain_idle_readers()
        if self.config.effective_tracking_enabled:
            cached = self._sync._cache_get(key)
            if cached is not None:
                return cached
        if self.config.url.startswith("memory://"):
            return self._sync.get(key)
        res = await self.execute_command("GET", key)
        out = bytes(res) if isinstance(res, (bytes, bytearray)) else None
        if out is not None and self.config.effective_tracking_enabled:
            self._sync._cache_put(key, out)
        return out

    async def set(self, key: str, value: bytes | str, *, ex: int | None = None, px: int | None = None) -> None:
        """Asynchronously set a key value with an optional TTL and update the client-side cache.

        Args:
            key: Key to write.
            value: Value bytes or text.
            ex: Expiry in whole seconds.
            px: Expiry in milliseconds.

        Raises:
            ValueError: When both ``ex`` and ``px`` are given.
        """
        if ex is not None and px is not None:
            raise ValueError("set() accepts ex or px, not both")
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        ttl_seconds = float(ex) if ex is not None else (px / 1000.0 if px is not None else None)
        if self.config.effective_tracking_enabled:
            self._sync._cache_put(key, raw, ttl_seconds=ttl_seconds)
        if self.config.url.startswith("memory://"):
            self._sync.set(key, raw, ex=ex, px=px)
            return
        if ex is not None:
            await self.execute_command("SET", key, raw, "EX", ex)
        elif px is not None:
            await self.execute_command("SET", key, raw, "PX", px)
        else:
            await self.execute_command("SET", key, raw)

    async def delete(self, key: str) -> bool:
        """Asynchronously delete a key and evict from client cache."""
        self._client_cache.pop(key, None)
        if self.config.url.startswith("memory://"):
            return self._sync.delete(key)
        res = await self.execute_command("DEL", key)
        return bool(res and res > 0)

    async def scan(self, cursor: int = 0, *, match: str | None = None, count: int = 500) -> tuple[int, list[str]]:
        """Run one ``SCAN`` step and return the next cursor and the matching keys.

        In ``memory://`` mode all matching keys are returned with cursor ``0``.
        """
        if self.config.url.startswith("memory://"):
            return self._sync.scan(cursor, match=match, count=count)
        parts: list[str | int] = ["SCAN", cursor]
        if match is not None:
            parts.extend(["MATCH", match])
        parts.extend(["COUNT", count])
        reply = await self.execute_command(*parts)
        return int(RedisCore.decode_part(reply[0])), [RedisCore.decode_part(k) for k in reply[1]]

    async def unlink(self, *keys: str) -> int:
        """Remove ``keys`` with ``UNLINK`` and evict them from the client-side cache."""
        if not keys:
            return 0
        for key in keys:
            self._client_cache.pop(key, None)
        if self.config.url.startswith("memory://"):
            return self._sync.unlink(*keys)
        res = await self.execute_command("UNLINK", *keys)
        return int(res) if isinstance(res, int) else 0

    async def pttl(self, key: str) -> int:
        """Return remaining TTL of key in milliseconds."""
        if self.config.url.startswith("memory://"):
            return self._sync.pttl(key)
        res = await self.execute_command("PTTL", key)
        return int(res) if isinstance(res, int) else -2

    async def flushdb(self) -> None:
        """Asynchronously clear all keys in the store and client cache."""
        self._client_cache.clear()
        if self.config.url.startswith("memory://"):
            self._sync.flushdb()
            return
        await self.execute_command("FLUSHDB")

    async def publish(self, channel: str, message: bytes | str) -> int:
        """Asynchronously publish a message to a Redis or Valkey pub/sub channel."""
        raw = message.encode("utf-8") if isinstance(message, str) else bytes(message)
        if self.config.url.startswith("memory://"):
            return self._sync._publish_memory(channel, raw)
        res = await self.execute_command("PUBLISH", channel, raw)
        return int(res) if isinstance(res, int) else 1

    def subscribe(self, *channels: str) -> _AsyncSubscriptionStream:
        """Subscribe to pub/sub channels (supports both `await driver.subscribe(...)` and `async for`)."""
        for ch in channels:
            self._subscribed_channels.add(ch)
        return _AsyncSubscriptionStream(self, "SUBSCRIBE", channels)

    def psubscribe(self, *patterns: str) -> _AsyncSubscriptionStream:
        """Subscribe to glob patterns on the multiplexed RESP3 connection."""
        for pat in patterns:
            self._subscribed_patterns.add(pat)
        return _AsyncSubscriptionStream(self, "PSUBSCRIBE", patterns)

    def ssubscribe(self, *shard_channels: str) -> _AsyncSubscriptionStream:
        """Subscribe to cluster shard channels (SSUBSCRIBE) on the multiplexed RESP3 connection."""
        for sch in shard_channels:
            self._subscribed_shards.add(sch)
        return _AsyncSubscriptionStream(self, "SSUBSCRIBE", shard_channels)

    async def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from the specified channels (or all channels when empty)."""
        if channels:
            for ch in channels:
                self._subscribed_channels.discard(ch)
        else:
            self._subscribed_channels.clear()
        if not self.config.url.startswith("memory://"):
            await self._send_pubsub_command_async("UNSUBSCRIBE", *channels)

    def drain_messages(self) -> _AwaitableMessageList:
        """Drain and return all pending PubSubMessage items (usable synchronously or via await)."""
        self._drain_idle_readers()
        messages: list[PubSubMessage] = []
        while self._pubsub_queue:
            messages.append(self._pubsub_queue.popleft())
        return _AwaitableMessageList(messages, driver=self)

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
        """Asynchronously execute a single RESP3 command with Cluster MOVED/ASK redirection."""
        if self.config.url.startswith("memory://"):
            return self._sync._execute_memory_command(*parts)

        target_node = await self._select_target_node_async(parts)
        asking = False
        max_redirects = max(0, self.config.max_redirections)

        for attempt in range(max_redirects + 1):
            reader, writer, from_pool = await self._get_connection_for_node(target_node)
            try:
                buf = self._reader_buffers.setdefault(id(reader), bytearray())
                if asking:
                    writer.write(SyncRedisDriver.encode_command("ASKING") + SyncRedisDriver.encode_command(*parts))
                    await writer.drain()
                    await self._read_reply(reader, buf=buf)
                    reply = await self._read_reply(reader, buf=buf)
                else:
                    writer.write(SyncRedisDriver.encode_command(*parts))
                    await writer.drain()
                    reply = await self._read_reply(reader, buf=buf)
                self._sync._consume_buffered_push_frames(buf)
                await self._return_connection(reader, writer, target_node)
                return reply
            except Resp3Error as err:
                await self._return_connection(reader, writer, target_node)
                redirect = parse_cluster_redirect(str(err))
                if redirect is None or attempt >= max_redirects:
                    raise
                if redirect.kind == "MOVED":
                    self._slot_map[redirect.slot] = (redirect.host, redirect.port)
                    target_node = (redirect.host, redirect.port)
                    asking = False
                else:
                    target_node = (redirect.host, redirect.port)
                    asking = True
            except ConnectionError:
                await self._close_connection(reader, writer)
                if from_pool and not asking:
                    r2, w2, _ = await self._get_connection_for_node(target_node, force_new=True)
                    try:
                        buf2 = self._reader_buffers.setdefault(id(r2), bytearray())
                        w2.write(SyncRedisDriver.encode_command(*parts))
                        await w2.drain()
                        reply2 = await self._read_reply(r2, buf=buf2)
                        self._sync._consume_buffered_push_frames(buf2)
                        await self._return_connection(r2, w2, target_node)
                        return reply2
                    except Resp3Error as err2:
                        await self._return_connection(r2, w2, target_node)
                        redirect = parse_cluster_redirect(str(err2))
                        if redirect is None or attempt >= max_redirects:
                            raise
                        if redirect.kind == "MOVED":
                            self._slot_map[redirect.slot] = (redirect.host, redirect.port)
                            target_node = (redirect.host, redirect.port)
                            asking = False
                        else:
                            target_node = (redirect.host, redirect.port)
                            asking = True
                    except Exception:
                        await self._close_connection(r2, w2)
                        raise
                else:
                    raise
            except Exception:
                await self._close_connection(reader, writer)
                raise

        msg = f"Exceeded maximum cluster redirections ({max_redirects})"
        raise Resp3Error(msg)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Asynchronously execute a batch of pipelined RESP3 commands."""
        if not commands:
            return []
        if self.config.url.startswith("memory://"):
            return self._sync.execute_pipeline(commands)
        wire_buf = self._sync.encode_pipeline(commands)
        target_node = await self._select_target_node_async(commands[0])
        reader, writer, _ = await self._get_connection_for_node(target_node)
        try:
            writer.write(wire_buf)
            await writer.drain()
            results: list[Any] = []
            buf = self._reader_buffers.setdefault(id(reader), bytearray())
            for _ in range(len(commands)):
                results.append(await self._read_reply(reader, buf=buf))
            self._sync._consume_buffered_push_frames(buf)
            return results
        except Exception:
            await self._close_connection(reader, writer)
            raise
        else:
            await self._return_connection(reader, writer, target_node)

    async def reconnect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Close all pooled connections and open a fresh connection with updated IAM token."""
        await self.close()
        self._closed = False
        self._sync._closed = False
        reader, writer = await self._open_stream()
        await self._return_connection(reader, writer, await self._default_node_async())
        return reader, writer

    async def close(self) -> None:
        """Close all pooled connections."""
        self._closed = True
        self._sync.close()
        async with self._lock:
            while self._connections:
                r, w = self._connections.popleft()
                await self._close_connection(r, w)
            for pool in self._node_connections.values():
                while pool:
                    r, w = pool.popleft()
                    await self._close_connection(r, w)
            self._node_connections.clear()

    async def _send_pubsub_command_async(self, cmd: str, *targets: str) -> None:
        """Send a pub/sub command over the pooled async connection and process initial frames."""
        node = await self._default_node_async()
        reader, writer, _ = await self._get_connection_for_node(node)
        try:
            buf = self._reader_buffers.setdefault(id(reader), bytearray())
            writer.write(SyncRedisDriver.encode_command(cmd, *targets))
            await writer.drain()
            await self._read_pubsub_ack_or_push_async(reader, buf)
        except Exception:
            await self._close_connection(reader, writer)
            raise
        else:
            await self._return_connection(reader, writer, node)

    async def _read_pubsub_ack_or_push_async(
        self,
        reader: asyncio.StreamReader,
        buf: bytearray,
    ) -> None:
        """Read initial pub/sub confirmation and any piggybacked push frames from reader."""
        self._pull_transport_buffer(reader, buf)
        received_any = False
        while True:
            while buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                except ValueError:
                    break
                del buf[:next_offset]
                received_any = True
                self._sync._dispatch_parsed_frame(frame)
            if received_any:
                self._pull_transport_buffer(reader, buf)
                if buf:
                    continue
                break
            chunk = await reader.read(4096)
            if not chunk:
                break
            buf.extend(chunk)

    def _pull_transport_buffer(self, reader: asyncio.StreamReader, buf: bytearray) -> None:
        """Move any already-received bytes from StreamReader's internal buffer into buf."""
        internal = getattr(reader, "_buffer", None)
        if isinstance(internal, bytearray) and internal:
            buf.extend(internal)
            internal.clear()

    def _drain_idle_readers(self) -> None:
        """Extract and dispatch any push frames received on idle pooled StreamReaders."""
        all_readers: list[asyncio.StreamReader] = [r for r, _ in self._connections]
        for pool in self._node_connections.values():
            for r, _ in pool:
                if r not in all_readers:
                    all_readers.append(r)
        for reader in all_readers:
            buf = self._reader_buffers.setdefault(id(reader), bytearray())
            self._pull_transport_buffer(reader, buf)
            self._sync._consume_buffered_push_frames(buf)

    async def _poll_idle_readers_async(self) -> None:
        """Poll idle readers briefly when awaiting drain_messages()."""
        self._drain_idle_readers()
        if self._pubsub_queue:
            return
        await asyncio.sleep(0)
        self._drain_idle_readers()

    async def _default_node_async(self) -> tuple[str, int]:
        """Resolve the default (host, port) target node asynchronously."""
        if self._master_address is not None:
            return self._master_address
        if self._sync._master_address is not None:
            self._master_address = self._sync._master_address
            return self._master_address
        if self.config.effective_sentinel_enabled:
            return await self.discover_sentinel_master()
        if self.config.cluster_nodes:
            return RedisCore.normalize_node(self.config.cluster_nodes[0], default_port=6379)
        parsed = urlparse(self.config.url)
        return (parsed.hostname or "127.0.0.1", parsed.port or 6379)

    async def _select_target_node_async(self, parts: Sequence[str | bytes | int]) -> tuple[str, int]:
        """Select the target (host, port) for an async command using the Cluster slot map."""
        if not parts:
            return await self._default_node_async()
        if self.config.effective_cluster_enabled or self._slot_map:
            cmd_name = RedisCore.decode_part(parts[0]).upper()
            if len(parts) >= 2 and cmd_name not in NON_KEY_COMMANDS:
                key_bytes = RedisCore.encode_part_bytes(parts[1])
                slot = hash_slot(key_bytes)
                mapped = self._slot_map.get(slot)
                if mapped is not None:
                    return mapped
        return await self._default_node_async()

    async def _resolve_iam_token(self) -> str | None:
        """Fetch a fresh GCP Memorystore IAM token (sync or async callable) when configured."""
        if not self.config.effective_iam_enabled:
            return None
        provider = self.config.effective_token_provider
        if provider is None:
            return None
        res = provider()
        if inspect.isawaitable(res):
            res = await res
        return str(res)

    def _build_handshake_frames(self, *, iam_token: str | None = None) -> list[bytes]:
        """Build the sequence of handshake command frames for a newly opened async stream."""
        return self.config.build_handshake_commands(iam_token=iam_token)

    def _install_reader_hook(self, reader: asyncio.StreamReader) -> None:
        """Install a feed_data hook on reader to process push frames while idle in the pool."""
        orig_feed_data = reader.feed_data
        reader_id = id(reader)

        def _hooked_feed_data(data: bytes) -> None:
            orig_feed_data(data)
            if reader_id in self._idle_readers:
                buf = self._reader_buffers.setdefault(reader_id, bytearray())
                self._pull_transport_buffer(reader, buf)
                self._sync._consume_buffered_push_frames(buf)

        reader.__dict__["feed_data"] = _hooked_feed_data

    async def _open_stream(
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish a new async TCP/TLS stream and execute HELLO 3, IAM auth, and CLIENT TRACKING."""
        if host is None or port is None:
            default_host, default_port = await self._default_node_async()
            target_host = host or default_host
            target_port = port or default_port
        else:
            target_host, target_port = host, port
        ssl_ctx = ssl.create_default_context() if self.config.uses_tls else None
        reader, writer = await asyncio.open_connection(target_host, target_port, ssl=ssl_ctx)
        buf = bytearray()
        self._reader_buffers[id(reader)] = buf
        self._reader_nodes[id(reader)] = (target_host, target_port)
        self._install_reader_hook(reader)
        try:
            iam_token = await self._resolve_iam_token()
            frames = self._build_handshake_frames(iam_token=iam_token)
            for cmd_bytes in frames:
                writer.write(cmd_bytes)
                await writer.drain()
                await self._read_reply(reader, buf=buf)
            self._sync._consume_buffered_push_frames(buf)
        except Exception:
            await self._close_connection(reader, writer)
            raise
        return reader, writer

    async def _create_connection(
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish a new async connection and execute handshake commands."""
        return await self._open_stream(host=host, port=port)

    async def _get_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Obtain an idle connection from the default pool or create a new one."""
        reader, writer, _ = await self._get_connection_for_node(await self._default_node_async())
        return reader, writer

    async def _get_connection_for_node(
        self,
        node: tuple[str, int],
        *,
        force_new: bool = False,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bool]:
        """Obtain an idle connection for node from the pool or create a new one."""
        default_node = await self._default_node_async()
        if not force_new:
            async with self._lock:
                pool = self._connections if node == default_node else self._node_connections.get(node)
                while pool:
                    r, w = pool.popleft()
                    self._idle_readers.discard(id(r))
                    if not w.is_closing() and not r.at_eof():
                        buf = self._reader_buffers.setdefault(id(r), bytearray())
                        self._pull_transport_buffer(r, buf)
                        self._sync._consume_buffered_push_frames(buf)
                        return r, w, True
                    await self._close_connection(r, w)
        r_new, w_new = await self._open_stream(host=node[0], port=node[1])
        return r_new, w_new, False

    async def _return_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        node: tuple[str, int] | None = None,
    ) -> None:
        """Return a connection to the pool and mark its reader idle for push frame monitoring."""
        target = node or self._reader_nodes.get(id(reader)) or await self._default_node_async()
        default_node = await self._default_node_async()
        async with self._lock:
            if self._closed or writer.is_closing():
                await self._close_connection(reader, writer)
                return
            buf = self._reader_buffers.setdefault(id(reader), bytearray())
            self._pull_transport_buffer(reader, buf)
            self._sync._consume_buffered_push_frames(buf)
            if target == default_node:
                if len(self._connections) < self.config.max_connections:
                    self._idle_readers.add(id(reader))
                    self._connections.append((reader, writer))
                else:
                    await self._close_connection(reader, writer)
            else:
                pool = self._node_connections.setdefault(target, collections.deque())
                if len(pool) < self.config.max_connections:
                    self._idle_readers.add(id(reader))
                    pool.append((reader, writer))
                else:
                    await self._close_connection(reader, writer)

    async def _close_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Close connection stream writer and clean up reader buffers."""
        self._idle_readers.discard(id(reader))
        self._reader_buffers.pop(id(reader), None)
        self._reader_nodes.pop(id(reader), None)
        with contextlib.suppress(Exception):
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=0.05)

    async def _read_reply(self, reader: asyncio.StreamReader, buf: bytearray | None = None) -> Any:
        """Read stream bytes until a non-push RESP3 frame is parsed."""
        read_buf = buf if buf is not None else self._reader_buffers.setdefault(id(reader), bytearray())
        self._pull_transport_buffer(reader, read_buf)
        while True:
            if read_buf:
                try:
                    frame, next_offset = Resp3Parser.parse_frame(read_buf, 0)
                except Resp3Error as err:
                    if err.next_offset > 0:
                        del read_buf[: err.next_offset]
                    raise
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


class _AwaitableMessageList(list[PubSubMessage]):
    """List of PubSubMessage items usable both synchronously as a list and via await."""

    def __init__(self, items: Sequence[PubSubMessage], *, driver: AsyncRedisDriver) -> None:
        super().__init__(items)
        self._driver = driver

    def __await__(self) -> Generator[Any, None, list[PubSubMessage]]:
        async def _coro() -> list[PubSubMessage]:
            await self._driver._poll_idle_readers_async()
            while self._driver._pubsub_queue:
                self.append(self._driver._pubsub_queue.popleft())
            return list(self)

        return _coro().__await__()


class _SyncSubscriptionStream:
    """Synchronous subscription handle supporting iteration over (channel, payload) tuples."""

    __slots__ = ("_channels", "_driver")

    def __init__(self, driver: SyncRedisDriver, channels: Sequence[str]) -> None:
        self._driver = driver
        self._channels = tuple(channels)

    def __iter__(self) -> Iterator[tuple[str, bytes]]:
        return self._iter_messages()

    def _iter_messages(self) -> Iterator[tuple[str, bytes]]:
        while self._driver._pubsub_queue:
            msg = self._driver._pubsub_queue.popleft()
            yield msg.channel, msg.payload
        if self._driver.config.url.startswith("memory://"):
            return
        sock = self._driver._get_socket()
        buf = self._driver._socket_buffers.setdefault(id(sock), bytearray())
        try:
            while not self._driver._closed:
                while buf:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    self._driver._dispatch_parsed_frame(frame)
                while self._driver._pubsub_queue:
                    msg = self._driver._pubsub_queue.popleft()
                    yield msg.channel, msg.payload
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
        finally:
            self._driver._close_socket(sock)


class _AsyncSubscriptionStream:
    """Dual awaitable and async-iterable subscription handle for AsyncRedisDriver."""

    __slots__ = ("_channels", "_command", "_driver")

    def __init__(self, driver: AsyncRedisDriver, command: str, channels: Sequence[str]) -> None:
        self._driver = driver
        self._command = command
        self._channels = tuple(channels)

    def __await__(self) -> Generator[Any, None, _AsyncSubscriptionStream]:
        return self._execute_subscribe().__await__()

    async def _execute_subscribe(self) -> _AsyncSubscriptionStream:
        if self._channels and not self._driver.config.url.startswith("memory://"):
            await self._driver._send_pubsub_command_async(self._command, *self._channels)
        return self

    def __aiter__(self) -> AsyncIterator[tuple[str, bytes]]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[tuple[str, bytes]]:
        while self._driver._pubsub_queue:
            queued = self._driver._pubsub_queue.popleft()
            yield queued.channel, queued.payload
        if self._driver.config.url.startswith("memory://"):
            return
        reader, writer = await self._driver._open_stream()
        buf = self._driver._reader_buffers.setdefault(id(reader), bytearray())
        try:
            if self._channels:
                writer.write(SyncRedisDriver.encode_command(self._command, *self._channels))
                await writer.drain()
            while not self._driver._closed:
                self._driver._pull_transport_buffer(reader, buf)
                while buf:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    self._driver._sync._dispatch_parsed_frame(frame)
                while self._driver._pubsub_queue:
                    msg = self._driver._pubsub_queue.popleft()
                    yield msg.channel, msg.payload
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                while buf:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    self._driver._sync._dispatch_parsed_frame(frame)
                while self._driver._pubsub_queue:
                    msg = self._driver._pubsub_queue.popleft()
                    yield msg.channel, msg.payload
        finally:
            await self._driver._close_connection(reader, writer)
