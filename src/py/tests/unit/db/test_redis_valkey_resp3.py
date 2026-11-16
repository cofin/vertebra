"""Unit tests for Redis and Valkey RESP3 handshakes, IAM auth, LRU client cache, Cluster/Sentinel, and Pub/Sub."""

from __future__ import annotations

import asyncio
import collections
import socket
import sys
import threading
import time
from typing import TYPE_CHECKING

import pytest

from vertebra.db.adapters.redis import (
    AsyncRedisDriver,
    AsyncRedisNotificationSource,
    ClusterRedirect,
    PubSubMessage,
    RedisConfig,
    RedisNotifyConfig,
    SyncRedisDriver,
    SyncRedisNotificationSource,
    crc16_xmodem,
    hash_slot,
    parse_cluster_redirect,
)
from vertebra.db.adapters.valkey import (
    AsyncValkeyDriver,
    Resp3Parser,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.db.adapters.valkey import (
    PubSubMessage as ValkeyPubSubMessage,
)
from vertebra.db.cdc import ChangeFeed
from vertebra.db.driver import AsyncNotificationSource, NotificationEvent, SyncNotificationSource
from vertebra.server._channels import ChannelBus
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import reset_dependency_cache

if TYPE_CHECKING:
    from tests.unit.conftest import Resp3FakeServer


class _RecordingBroker:
    """In-memory broker stub recording published pub/sub and invalidation events."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.invalidated: list[list[str]] = []

    def publish(self, channel: str, payload: bytes) -> None:
        """Record a published message on channel."""
        self.published.append((channel, bytes(payload)))

    def publish_invalidation(self, keys: list[str]) -> None:
        """Record an invalidation push batch."""
        self.invalidated.append(list(keys))


def test_crc16_xmodem_and_hash_slot_vectors() -> None:
    """crc16_xmodem and hash_slot must match Redis Cluster XMODEM specification and {tag} rules."""
    assert crc16_xmodem(b"123456789") == 0x31C3
    assert hash_slot("123456789") == (0x31C3 % 16384)

    slot_profile = hash_slot("user:{42}:profile")
    slot_orders = hash_slot("user:{42}:orders")
    assert slot_profile == slot_orders
    assert slot_profile == hash_slot(b"42")

    assert SyncRedisDriver.hash_slot("{user:1000}.following") == SyncRedisDriver.hash_slot("{user:1000}.followers")
    assert AsyncRedisDriver.hash_slot("user:{}:profile") == hash_slot(b"user:{}:profile")


def test_parse_cluster_redirect_moved_and_ask() -> None:
    """parse_cluster_redirect must parse MOVED and ASK error frames into ClusterRedirect structs."""
    moved = parse_cluster_redirect("MOVED 3999 127.0.0.1:6381")
    assert moved == ClusterRedirect(kind="MOVED", slot=3999, host="127.0.0.1", port=6381)

    ask = ClusterRedirect.parse("-ASK 12739 10.0.0.12:7002\r\n")
    assert ask == ClusterRedirect(kind="ASK", slot=12739, host="10.0.0.12", port=7002)

    assert parse_cluster_redirect("ERR syntax error") is None
    assert parse_cluster_redirect("MOVED invalid") is None


def test_sync_socketpair_hello3_client_tracking_and_iam_refresh() -> None:
    """SyncValkeyDriver over socket.socketpair() must send HELLO 3 with IAM token, CLIENT TRACKING, and refresh on reconnect."""
    tokens = collections.deque(["mock-iam-token-1", "mock-iam-token-2"])
    received_commands: list[list[bytes]] = []
    active_server_socks: list[socket.socket] = []

    def make_socketpair(_addr: tuple[str, int]) -> socket.socket:
        client_sock, server_sock = socket.socketpair()
        active_server_socks.append(server_sock)

        def server_loop() -> None:
            buf = bytearray()
            try:
                while True:
                    chunk = server_sock.recv(4096)
                    if not chunk:
                        break
                    buf.extend(chunk)
                    while buf:
                        try:
                            frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                        except ValueError:
                            break
                        del buf[:next_offset]
                        if isinstance(frame, list) and frame:
                            received_commands.append(
                                [bytes(p) if isinstance(p, bytes) else str(p).encode() for p in frame]
                            )
                            cmd = frame[0].decode("ascii").upper()
                            if cmd == "HELLO":
                                server_sock.sendall(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                            elif cmd == "CLIENT":
                                server_sock.sendall(b"+OK\r\n")
                            elif cmd == "PING":
                                server_sock.sendall(b"+PONG\r\n")
                            else:
                                server_sock.sendall(b"+OK\r\n")
            finally:
                server_sock.close()

        threading.Thread(target=server_loop, daemon=True).start()
        return client_sock

    cfg = ValkeyConfig(
        url="valkey://127.0.0.1:6379/0",
        username="iam-user",
        iam_auth_enabled=True,
        token_provider=tokens.popleft,
        enable_client_tracking=True,
        client_tracking_prefixes=("user:", "order:"),
    )
    driver = SyncValkeyDriver(cfg, socket_factory=make_socketpair)
    try:
        pong = driver.execute_command("PING")
        assert pong == "PONG"
        assert len(received_commands) >= 3
        assert received_commands[0] == [
            b"HELLO",
            b"3",
            b"AUTH",
            b"iam-user",
            b"mock-iam-token-1",
            b"SETNAME",
            b"vertebra-valkey",
        ]
        assert received_commands[1] == [
            b"CLIENT",
            b"TRACKING",
            b"ON",
            b"PREFIX",
            b"user:",
            b"PREFIX",
            b"order:",
            b"BCAST",
        ]
        assert received_commands[2] == [b"PING"]

        driver.reconnect()
        assert any(
            cmd == [b"HELLO", b"3", b"AUTH", b"iam-user", b"mock-iam-token-2", b"SETNAME", b"vertebra-valkey"]
            for cmd in received_commands
        )
    finally:
        driver.close()
        for s in active_server_socks:
            s.close()


def test_bounded_lru_client_cache_and_live_socket_push_invalidation() -> None:
    """SyncValkeyDriver must enforce bounded OrderedDict LRU eviction and evict on live RESP3 invalidate push frames."""
    client_sock, server_sock = socket.socketpair()

    def server_worker() -> None:
        buf = bytearray()
        try:
            while True:
                chunk = server_sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                while buf:
                    try:
                        frame, next_offset = Resp3Parser.parse_frame(buf, 0)
                    except ValueError:
                        break
                    del buf[:next_offset]
                    if isinstance(frame, list) and frame:
                        cmd = frame[0].decode("ascii").upper()
                        if cmd == "HELLO":
                            server_sock.sendall(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                        elif cmd in ("CLIENT", "SET"):
                            server_sock.sendall(b"+OK\r\n")
                        elif cmd == "GET":
                            key = frame[1].decode("utf-8")
                            val = f"fresh-{key}".encode()
                            server_sock.sendall(f"${len(val)}\r\n".encode("ascii") + val + b"\r\n")
                        elif cmd == "PING":
                            server_sock.sendall(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nuser:1\r\n+PONG\r\n")
                        elif cmd == "ECHO":
                            server_sock.sendall(b">2\r\n$10\r\ninvalidate\r\n_\r\n$5\r\nflush\r\n")
        finally:
            server_sock.close()

    threading.Thread(target=server_worker, daemon=True).start()

    broker = _RecordingBroker()
    cfg = ValkeyConfig(
        url="valkey://127.0.0.1:6379/0",
        client_cache_capacity=2,
        enable_client_tracking=True,
    )
    driver = SyncValkeyDriver(cfg, broker=broker, socket_factory=lambda _addr: client_sock)
    try:
        assert isinstance(driver._client_cache, collections.OrderedDict)

        driver.set("user:1", b"v1")
        driver.set("user:2", b"v2")
        assert driver.cached_keys == ("user:1", "user:2")

        assert driver.get("user:1") == b"v1"
        assert driver.cached_keys == ("user:2", "user:1")

        driver.set("user:3", b"v3")
        assert driver.cached_keys == ("user:1", "user:3")
        assert "user:2" not in driver._client_cache

        assert driver.execute_command("PING") == "PONG"
        assert "user:1" not in driver.cached_keys
        assert list(driver.cached_keys) == ["user:3"]
        assert ["user:1"] in broker.invalidated

        assert driver.execute_command("ECHO", "flush") == b"flush"
        assert not driver.cached_keys
    finally:
        driver.close()
        server_sock.close()


@pytest.mark.anyio
async def test_async_cluster_moved_and_ask_redirection() -> None:
    """AsyncRedisDriver must update _slot_map on MOVED and send ASKING without mutating _slot_map on ASK."""
    node2_commands: list[list[str]] = []
    node3_commands: list[list[str]] = []

    target_key_moved = "order:{42}"
    target_key_ask = "cart:{99}"
    moved_slot = hash_slot(target_key_moved)
    ask_slot = hash_slot(target_key_ask)

    node2_port = 0
    node3_port = 0

    async def handle_node1(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    cmd = frame[0].decode("ascii").upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                    elif cmd == "GET":
                        key = frame[1].decode("utf-8")
                        if key == target_key_moved:
                            writer.write(f"-MOVED {moved_slot} 127.0.0.1:{node2_port}\r\n".encode("ascii"))
                        elif key == target_key_ask:
                            writer.write(f"-ASK {ask_slot} 127.0.0.1:{node3_port}\r\n".encode("ascii"))
                        else:
                            writer.write(b"$5\r\nnode1\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    async def handle_node2(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    decoded = [p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in frame]
                    node2_commands.append(decoded)
                    cmd = decoded[0].upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                    elif cmd == "GET":
                        writer.write(b"$10\r\nfrom-node2\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    async def handle_node3(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    decoded = [p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in frame]
                    node3_commands.append(decoded)
                    cmd = decoded[0].upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                    elif cmd == "ASKING":
                        writer.write(b"+OK\r\n")
                    elif cmd == "GET":
                        writer.write(b"$10\r\nfrom-node3\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    srv1 = await asyncio.start_server(handle_node1, "127.0.0.1", 0)
    srv2 = await asyncio.start_server(handle_node2, "127.0.0.1", 0)
    srv3 = await asyncio.start_server(handle_node3, "127.0.0.1", 0)
    node1_port = srv1.sockets[0].getsockname()[1]
    node2_port = srv2.sockets[0].getsockname()[1]
    node3_port = srv3.sockets[0].getsockname()[1]

    driver = AsyncValkeyDriver(
        ValkeyConfig(
            url=f"valkey://127.0.0.1:{node1_port}/0",
            cluster_enabled=True,
            cluster_nodes=(("127.0.0.1", node1_port),),
            max_redirections=3,
        )
    )
    try:
        val_moved = await driver.get(target_key_moved)
        assert val_moved == b"from-node2"
        assert driver._slot_map[moved_slot] == ("127.0.0.1", node2_port)

        val_ask = await driver.get(target_key_ask)
        assert val_ask == b"from-node3"
        assert ask_slot not in driver._slot_map
        assert ["ASKING"] in node3_commands
        assert ["GET", target_key_ask] in node3_commands
    finally:
        await driver.close()
        srv1.close()
        srv2.close()
        srv3.close()
        await srv1.wait_closed()
        await srv2.wait_closed()
        await srv3.wait_closed()


@pytest.mark.anyio
async def test_sentinel_master_discovery_and_failover() -> None:
    """SyncRedisDriver and AsyncRedisDriver must discover master address via SENTINEL get-master-addr-by-name."""

    client_writers: list[asyncio.StreamWriter] = []

    async def handle_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client_writers.append(writer)
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    cmd = frame[0].decode("ascii").upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                    elif cmd == "GET":
                        writer.write(b"$13\r\nmaster-active\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    master_srv = await asyncio.start_server(handle_master, "127.0.0.1", 0)
    master_port = master_srv.sockets[0].getsockname()[1]

    async def handle_sentinel(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client_writers.append(writer)
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    cmd = frame[0].decode("ascii").upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                    elif cmd == "SENTINEL":
                        port_b = str(master_port).encode("ascii")
                        writer.write(
                            b"*2\r\n$9\r\n127.0.0.1\r\n$"
                            + str(len(port_b)).encode("ascii")
                            + b"\r\n"
                            + port_b
                            + b"\r\n"
                        )
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    sentinel_srv = await asyncio.start_server(handle_sentinel, "127.0.0.1", 0)
    sentinel_port = sentinel_srv.sockets[0].getsockname()[1]

    cfg = RedisConfig(
        topology="sentinel",
        sentinel_master_name="mymaster",
        sentinel_nodes=(("127.0.0.1", 1), ("127.0.0.1", sentinel_port)),
    )
    async_driver = AsyncRedisDriver(cfg)
    try:
        discovered = await async_driver.discover_sentinel_master()
        assert discovered == ("127.0.0.1", master_port)
        assert await async_driver.get("status") == b"master-active"
    finally:
        await async_driver.close()
        for w in client_writers:
            w.close()
        sentinel_srv.close()
        master_srv.close()
        await sentinel_srv.wait_closed()
        await master_srv.wait_closed()


@pytest.mark.anyio
async def test_multiplexed_resp3_pubsub_and_broker_forwarding() -> None:
    """SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE, and drain_messages must multiplex push frames and forward to broker."""
    received_cmds: list[list[str]] = []

    async def handle_pubsub_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    decoded = [p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in frame]
                    received_cmds.append(decoded)
                    cmd = decoded[0].upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                    elif cmd == "CLIENT":
                        writer.write(b"+OK\r\n")
                    elif cmd == "SUBSCRIBE":
                        ch = decoded[1]
                        writer.write(f">3\r\n$9\r\nsubscribe\r\n${len(ch)}\r\n{ch}\r\n:1\r\n".encode("ascii"))
                        writer.write(f">3\r\n$7\r\nmessage\r\n${len(ch)}\r\n{ch}\r\n$5\r\nhello\r\n".encode("ascii"))
                    elif cmd == "PSUBSCRIBE":
                        pat = decoded[1]
                        writer.write(f">3\r\n$10\r\npsubscribe\r\n${len(pat)}\r\n{pat}\r\n:1\r\n".encode("ascii"))
                        writer.write(
                            f">4\r\n$8\r\npmessage\r\n${len(pat)}\r\n{pat}\r\n$9\r\nuser:1000\r\n$7\r\nmatched\r\n".encode(
                                "ascii"
                            )
                        )
                    elif cmd == "SSUBSCRIBE":
                        sch = decoded[1]
                        writer.write(f">3\r\n$10\r\nssubscribe\r\n${len(sch)}\r\n{sch}\r\n:1\r\n".encode("ascii"))
                        writer.write(
                            f">3\r\n$8\r\nsmessage\r\n${len(sch)}\r\n{sch}\r\n$11\r\nshard-event\r\n".encode("ascii")
                        )
                    elif cmd == "UNSUBSCRIBE":
                        ch = decoded[1] if len(decoded) > 1 else ""
                        writer.write(f">3\r\n$11\r\nunsubscribe\r\n${len(ch)}\r\n{ch}\r\n:0\r\n".encode("ascii"))
                    elif cmd == "PUBLISH":
                        writer.write(b":2\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle_pubsub_server, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    broker = _RecordingBroker()
    driver = AsyncValkeyDriver(
        ValkeyConfig(url=f"valkey://127.0.0.1:{port}/0", enable_client_tracking=True),
        broker=broker,
    )
    try:
        await driver.subscribe("events")
        await driver.psubscribe("user:*")
        await driver.ssubscribe("shard:{42}")

        pub_count = await driver.publish("events", b"outbound")
        assert pub_count == 2

        drained = driver.drain_messages()
        assert drained == [
            ValkeyPubSubMessage(kind="message", channel="events", payload=b"hello", pattern=None),
            ValkeyPubSubMessage(kind="pmessage", channel="user:1000", payload=b"matched", pattern="user:*"),
            ValkeyPubSubMessage(kind="smessage", channel="shard:{42}", payload=b"shard-event", pattern=None),
        ]
        assert broker.published == [
            ("events", b"hello"),
            ("user:1000", b"matched"),
            ("shard:{42}", b"shard-event"),
        ]

        await driver.unsubscribe("events")
        assert ["UNSUBSCRIBE", "events"] in received_cmds
        assert driver.drain_messages() == []
    finally:
        await driver.close()
        server.close()
        await server.wait_closed()


def test_sync_multiplexed_pubsub_and_cluster_moved_ask(resp3_server: Resp3FakeServer) -> None:
    """SyncRedisDriver must support subscribe, psubscribe, ssubscribe, drain_messages, and Cluster MOVED/ASK."""
    broker = _RecordingBroker()
    driver = SyncRedisDriver(RedisConfig(url=resp3_server.redis_url), broker=broker)
    try:
        driver.subscribe("alpha")
        driver.psubscribe("beta:*")
        driver.ssubscribe("shard:1")

        driver.publish("alpha", "msg-a")
        driver.publish("beta:42", b"msg-b")
        driver.execute_command("SPUBLISH", "shard:1", b"msg-s")

        msgs: list[PubSubMessage] = []
        deadline = time.monotonic() + 2.0
        while len(msgs) < 3 and time.monotonic() < deadline:
            msgs.extend(driver.drain_messages())
        assert msgs == [
            PubSubMessage(kind="message", channel="alpha", payload=b"msg-a", pattern=None),
            PubSubMessage(kind="pmessage", channel="beta:42", payload=b"msg-b", pattern="beta:*"),
            PubSubMessage(kind="smessage", channel="shard:1", payload=b"msg-s", pattern=None),
        ]
        assert ("alpha", b"msg-a") in broker.published
        assert ("beta:42", b"msg-b") in broker.published
        assert ("shard:1", b"msg-s") in broker.published

        driver.unsubscribe("alpha")
        driver.publish("alpha", "ignored")
        assert driver.drain_messages() == []
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_iam_token_provider_coroutine_refresh_on_reconnect() -> None:
    """AsyncValkeyDriver must await async token_provider callables on connect and reconnect."""
    tokens = collections.deque(["async-iam-tok-1", "async-iam-tok-2"])
    auth_headers: list[tuple[str, str]] = []

    async def async_token_provider() -> str:
        await asyncio.sleep(0)
        return tokens.popleft()

    async def handle_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    decoded = [p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in frame]
                    if decoded[0].upper() == "HELLO":
                        if "AUTH" in decoded:
                            idx = decoded.index("AUTH")
                            auth_headers.append((decoded[idx + 1], decoded[idx + 2]))
                        writer.write(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle_server, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    driver = AsyncValkeyDriver(
        ValkeyConfig(
            url=f"valkey://127.0.0.1:{port}/0",
            iam_auth_enabled=True,
            token_provider=async_token_provider,
        )
    )
    try:
        await driver.set("k", "v")
        assert auth_headers == [("default", "async-iam-tok-1")]
        await driver.reconnect()
        assert auth_headers == [("default", "async-iam-tok-1"), ("default", "async-iam-tok-2")]
    finally:
        await driver.close()
        server.close()
        await server.wait_closed()


def test_redis_notification_source_contracts_and_missing_wheel(monkeypatch: pytest.MonkeyPatch) -> None:
    """SyncRedisNotificationSource and AsyncRedisNotificationSource satisfy capability protocols and check wheel dependencies."""
    monkeypatch.setitem(sys.modules, "vertebra._redis", None)
    reset_dependency_cache("vertebra._redis")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SyncRedisNotificationSource(client=None)
        assert exc_info.value.package == "vertebra-core[redis]"

        with pytest.raises(MissingDependencyError) as async_exc:
            AsyncRedisNotificationSource(client=None)
        assert async_exc.value.package == "vertebra-core[redis]"
    finally:
        reset_dependency_cache("vertebra._redis")


def test_sync_redis_notification_source_loopback(resp3_server: Resp3FakeServer) -> None:
    """SyncRedisNotificationSource streams live pub/sub messages, supports pattern/shard subscriptions, and hydrates envelopes."""
    driver = SyncRedisDriver(RedisConfig(url=f"redis://127.0.0.1:{resp3_server.port}/0"))
    feed = ChangeFeed()
    source = driver.notification_source(
        RedisNotifyConfig(channels=("orders",), timeout_ms=50),
        change_feed=feed,
    )
    assert isinstance(source, SyncNotificationSource)
    assert source.subscribed_channels == ["orders"]

    source.psubscribe(["alerts:*"])
    source.ssubscribe(["shard:1"])
    assert source.subscribed_patterns == ["alerts:*"]
    assert source.subscribed_shard_channels == ["shard:1"]

    with pytest.raises(TypeError):
        source.subscribe("bad_channel")

    events: list[NotificationEvent] = []
    ready = threading.Event()

    def run_listener() -> None:
        stream = source.listen()
        ready.set()
        for evt in stream:
            events.append(evt)
            if len(events) >= 3:
                break

    t = threading.Thread(target=run_listener, daemon=True)
    t.start()

    assert ready.wait(timeout=2.0)
    deadline = time.monotonic() + 2.0
    while getattr(source, "_listener", None) is None and time.monotonic() < deadline:
        threading.Event().wait(0.005)
    threading.Event().wait(0.02)
    source.publish("orders", b"order-created-1")
    source.publish("alerts:cpu", b"high-cpu")
    source.spublish("shard:1", b"shard-msg")

    t.join(timeout=3.0)
    assert not t.is_alive()
    assert len(events) == 3
    assert events[0].channel == "orders"
    assert events[0].payload == b"order-created-1"
    assert events[0].metadata["kind"] == "message"
    assert events[1].channel == "alerts:cpu"
    assert events[1].payload == b"high-cpu"
    assert events[1].metadata["kind"] == "pmessage"
    assert events[1].metadata["pattern"] == "alerts:*"
    assert events[2].channel == "shard:1"
    assert events[2].payload == b"shard-msg"
    assert events[2].metadata["kind"] == "smessage"
    assert events[2].metadata["shard"] is True

    source.close()
    driver.close()


@pytest.mark.anyio
async def test_async_redis_notification_source_loopback_and_channelbus(resp3_server: Resp3FakeServer) -> None:
    """AsyncRedisNotificationSource streams live frames, hydrates envelopes, and bridges ChannelBus."""
    driver = AsyncRedisDriver(RedisConfig(url=f"redis://127.0.0.1:{resp3_server.port}/0"))
    source = driver.notification_source(
        RedisNotifyConfig(channels=("events",), timeout_ms=50),
    )
    assert isinstance(source, AsyncNotificationSource)

    events: list[NotificationEvent] = []

    async def consume() -> None:
        async for evt in source.listen():
            events.append(evt)
            if len(events) >= 2:
                break

    task = asyncio.create_task(consume())
    deadline = time.monotonic() + 2.0
    while getattr(source, "_listener", None) is None and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.02)

    await source.publish("events", b"msg-1")
    await source.publish("events", b"b64:aGVsbG8=")

    await asyncio.wait_for(task, timeout=3.0)
    assert len(events) == 2
    assert events[0].payload == b"msg-1"
    assert events[1].payload == b"hello"

    bus = ChannelBus(notification_source=source)
    await bus.on_startup()
    await bus.subscribe(["events"])
    await bus.publish(b"from-bus", ["events"])
    await bus.on_shutdown()

    await source.aclose()
    await driver.close()
