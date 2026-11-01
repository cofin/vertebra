"""Unit tests for native RESP3 wire parser, pooled Redis/Valkey drivers, and Litestar adapters."""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from vertebra.db.redis import (
    AsyncRedisDriver,
    RedisConfig,
    Resp3Error,
    Resp3Parser,
    SyncRedisDriver,
)
from vertebra.db.valkey import (
    AsyncValkeyDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.extensions.litestar.channels import ChannelsBackend
from vertebra.extensions.litestar.store import MemoryStore
from vertebra.framework.channels import ChannelBus
from vertebra.framework.stores import KeyValueStore


def test_resp3_wire_parser_decodes_primitives_and_collections() -> None:
    """Resp3Parser must decode simple strings, integers, doubles, booleans, nulls, arrays, and maps."""
    val, offset = Resp3Parser.parse_frame(b"+OK\r\n")
    assert val == "OK"
    assert offset == 5

    val, offset = Resp3Parser.parse_frame(b":42\r\n")
    assert val == 42
    assert offset == 5

    val, offset = Resp3Parser.parse_frame(b",3.14\r\n")
    assert abs(val - 3.14) < 1e-6
    assert offset == 7

    val, offset = Resp3Parser.parse_frame(b"#t\r\n")
    assert val is True
    assert offset == 4

    val, offset = Resp3Parser.parse_frame(b"#f\r\n")
    assert val is False
    assert offset == 4

    val, offset = Resp3Parser.parse_frame(b"_\r\n")
    assert val is None
    assert offset == 3

    val, offset = Resp3Parser.parse_frame(b"$5\r\nhello\r\n")
    assert val == b"hello"
    assert offset == 11

    val, offset = Resp3Parser.parse_frame(b"$-1\r\n")
    assert val is None
    assert offset == 5

    val, offset = Resp3Parser.parse_frame(b"*2\r\n$4\r\necho\r\n$5\r\nworld\r\n")
    assert val == [b"echo", b"world"]
    assert offset == 25

    val, offset = Resp3Parser.parse_frame(b"%1\r\n+status\r\n:200\r\n")
    assert val == {"status": 200}
    assert offset == 19


def test_resp3_wire_parser_decodes_push_and_raises_errors() -> None:
    """Resp3Parser must decode push frames as ('push', items) and raise Resp3Error on error frames."""
    raw_push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nuser:1\r\n"
    val, offset = Resp3Parser.parse_frame(raw_push)
    assert val == ("push", [b"invalidate", [b"user:1"]])
    assert offset == len(raw_push)

    with pytest.raises(Resp3Error, match="ERR unknown command"):
        Resp3Parser.parse_frame(b"-ERR unknown command\r\n")

    with pytest.raises(Resp3Error, match="NOSCRIPT No matching script"):
        Resp3Parser.parse_frame(b"-NOSCRIPT No matching script\r\n")


def test_encode_hello3_handshake_parameters() -> None:
    """SyncRedisDriver.encode_hello3_handshake must accept username, password, and client_name."""
    raw = SyncRedisDriver.encode_hello3_handshake(
        username="alice",
        password="secret",
        client_name="test-client",
    )
    assert b"HELLO" in raw
    assert b"3" in raw
    assert b"AUTH" in raw
    assert b"alice" in raw
    assert b"secret" in raw
    assert b"SETNAME" in raw
    assert b"test-client" in raw


def test_sync_redis_driver_memory_store_operations() -> None:
    """SyncRedisDriver over memory:// must support get, set, delete, pttl, flushdb, eval_script, and pipeline."""
    driver = SyncRedisDriver(RedisConfig(url="memory://"))
    driver.set("k1", "v1", ex=60)
    assert driver.get("k1") == b"v1"
    ttl = driver.pttl("k1")
    assert 0 < ttl <= 60000

    pipe_results = driver.execute_pipeline(
        [
            ("SET", "k2", "v2"),
            ("GET", "k2"),
            ("DEL", "k1"),
        ]
    )
    assert pipe_results == ["OK", b"v2", 1]
    assert driver.get("k1") is None
    assert driver.get("k2") == b"v2"

    script = "return redis.call('get', KEYS[1])"
    res = driver.eval_script(script, keys=["k2"])
    assert res == b"v2"

    driver.flushdb()
    assert driver.get("k2") is None
    assert driver.pttl("k2") == -2
    driver.close()


@pytest.mark.anyio
async def test_async_redis_driver_memory_store_operations() -> None:
    """AsyncRedisDriver over memory:// must support async get, set, delete, pttl, flushdb, and eval_script."""
    driver = AsyncRedisDriver(RedisConfig(url="memory://"))
    await driver.set("k1", "v1", ex=10)
    assert await driver.get("k1") == b"v1"
    assert (await driver.pttl("k1")) > 0

    assert (await driver.publish("news", "headline")) >= 1

    res = await driver.eval_script("return ARGV[1]", args=["hello"])
    assert res == "hello" or res == b"hello"

    await driver.flushdb()
    assert await driver.get("k1") is None
    await driver.close()


@pytest.mark.anyio
async def test_resp3_wire_loopback_server_execution_and_invalidation() -> None:
    """AsyncValkeyDriver over real loopback socket executes commands and handles push invalidations."""
    stored_kv: dict[str, bytes] = {}
    cached_scripts: set[str] = set()

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    cmd = (frame[0].decode("ascii") if isinstance(frame[0], bytes) else str(frame[0])).upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                    elif cmd == "CLIENT":
                        writer.write(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nwire\r\n")
                        writer.write(b"+OK\r\n")
                    elif cmd == "SET":
                        k = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        v = frame[2] if isinstance(frame[2], bytes) else str(frame[2]).encode("utf-8")
                        stored_kv[k] = v
                        writer.write(b"+OK\r\n")
                    elif cmd == "GET":
                        k = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        v = stored_kv.get(k)
                        if v is None:
                            writer.write(b"_\r\n")
                        else:
                            writer.write(f"${len(v)}\r\n".encode("ascii") + v + b"\r\n")
                    elif cmd == "PTTL":
                        writer.write(b":5000\r\n")
                    elif cmd == "DEL":
                        writer.write(b":1\r\n")
                    elif cmd == "FLUSHDB":
                        stored_kv.clear()
                        writer.write(b"+OK\r\n")
                    elif cmd == "EVALSHA":
                        sha = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        if sha not in cached_scripts:
                            writer.write(b"-NOSCRIPT No matching script\r\n")
                        else:
                            writer.write(b"+script_ok\r\n")
                    elif cmd == "EVAL":
                        script = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
                        cached_scripts.add(sha)
                        writer.write(b"+script_ok\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    driver = AsyncValkeyDriver(ValkeyConfig(url=f"valkey://127.0.0.1:{port}/0", enable_client_side_caching=True))
    invalidated_keys: list[list[str]] = []
    driver.add_invalidation_listener(lambda keys: invalidated_keys.append(list(keys)))

    await driver.set("wire", "testval")
    assert await driver.get("wire") == b"testval"

    assert await driver.pttl("wire") == 5000

    eval_res = await driver.eval_script("return 1")
    assert eval_res == "script_ok"

    await driver.execute_command("CLIENT", "TRACKING", "ON")
    await asyncio.sleep(0.05)
    assert ["wire"] in invalidated_keys
    assert "wire" not in driver.cached_keys

    await driver.close()
    server.close()
    await server.wait_closed()


def test_valkey_subclasses_redis_and_handles_invalidation_push() -> None:
    """SyncValkeyDriver must inherit from SyncRedisDriver and evict keys on invalidation push frames."""
    assert issubclass(SyncValkeyDriver, SyncRedisDriver)
    assert issubclass(AsyncValkeyDriver, AsyncRedisDriver)

    driver = SyncValkeyDriver(ValkeyConfig(url="memory://", enable_client_side_caching=True))
    driver.set("user:1", "alice")
    assert driver.get("user:1") == b"alice"
    assert "user:1" in driver.cached_keys

    invalidated: list[list[str]] = []
    driver.add_invalidation_listener(lambda keys: invalidated.append(list(keys)))

    evicted = driver.handle_invalidation_push(["user:1", "user:2"])
    assert evicted == 1
    assert "user:1" not in driver.cached_keys
    assert invalidated == [["user:1", "user:2"]]
    driver.close()


@pytest.mark.anyio
async def test_valkey_resp3_client_tracking_and_litestar_integration() -> None:
    """MemoryStore must propagate delete_all, expires_in, and Valkey push invalidations."""
    store = MemoryStore.from_valkey("memory://")
    await store.set("k1", "val1", expires_in=30)
    assert await store.get("k1") == b"val1"
    ttl = await store.expires_in("k1")
    assert ttl is not None and 0 < ttl <= 30

    vdriver = store.driver
    assert isinstance(vdriver, AsyncValkeyDriver)
    vdriver.handle_invalidation_push(["k1"])
    assert "k1" not in store._entries

    await store.delete_all()
    assert await store.get("k1") is None
    assert await store.expires_in("k1") is None
    if store.driver is not None:
        await store.driver.close()


@pytest.mark.anyio
async def test_key_value_backend_and_channel_bus() -> None:
    """KeyValueStore and ChannelBus must provide unified caching and pub/sub streaming."""
    kv = KeyValueStore.from_valkey("memory://")
    await kv.set("a", "1")
    await kv.set_many({"b": "2", "c": "3"})

    batch = await kv.get_many(["a", "b", "c", "d"])
    assert batch["a"] == b"1"
    assert batch["b"] == b"2"
    assert batch["c"] == b"3"
    assert batch["d"] is None

    ns = kv.namespace("tenant:1")
    await ns.set("user", "bob")
    assert await ns.get("user") == b"bob"

    bus = ChannelBus.from_redis("memory://")
    assert isinstance(bus.backend, ChannelBus)
    await bus.on_startup()
    await bus.subscribe(["events"])

    stream = bus.stream_events()

    async def _publish_soon() -> None:
        await asyncio.sleep(0.02)
        await bus.publish(b"msg1", ["events"])

    pub_task = asyncio.create_task(_publish_soon())
    channel, data = await anext(stream)
    assert channel == "events"
    assert data == b"msg1"
    await pub_task

    await bus.on_shutdown()
    await kv.close()


@pytest.mark.anyio
async def test_resp3_loopback_server_pipeline_evalsha_and_valkey_push_invalidation() -> None:
    """AsyncValkeyDriver, KeyValueBackend, and ChannelBus operate over live loopback RESP3 server."""
    stored_kv: dict[str, bytes] = {}
    cached_scripts: set[str] = set()

    subscriber_writers: list[asyncio.StreamWriter] = []

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buf = bytearray()
        while True:
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
                if isinstance(frame, list) and frame:
                    cmd = (frame[0].decode("ascii") if isinstance(frame[0], bytes) else str(frame[0])).upper()
                    if cmd == "HELLO":
                        writer.write(b"%2\r\n+server\r\n+valkey\r\n+proto\r\n:3\r\n")
                    elif cmd == "CLIENT":
                        writer.write(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nuser:1\r\n")
                        writer.write(b"+OK\r\n")
                    elif cmd == "SET":
                        k = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        v = frame[2] if isinstance(frame[2], bytes) else str(frame[2]).encode("utf-8")
                        stored_kv[k] = v
                        writer.write(b"+OK\r\n")
                    elif cmd == "GET":
                        k = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        v = stored_kv.get(k)
                        if v is None:
                            writer.write(b"_\r\n")
                        else:
                            writer.write(f"${len(v)}\r\n".encode("ascii") + v + b"\r\n")
                    elif cmd == "PTTL":
                        writer.write(b":5000\r\n")
                    elif cmd == "DEL":
                        writer.write(b":1\r\n")
                    elif cmd == "FLUSHDB":
                        stored_kv.clear()
                        writer.write(b"+OK\r\n")
                    elif cmd == "EVALSHA":
                        sha = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        if sha not in cached_scripts:
                            writer.write(b"-NOSCRIPT No matching script\r\n")
                        else:
                            writer.write(b"+script_ok\r\n")
                    elif cmd == "EVAL":
                        script = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        sha = hashlib.sha1(script.encode("utf-8")).hexdigest()
                        cached_scripts.add(sha)
                        writer.write(b"+script_ok\r\n")
                    elif cmd == "SUBSCRIBE":
                        subscriber_writers.append(writer)
                        writer.write(b"*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n")
                    elif cmd == "PUBLISH":
                        ch = frame[1].decode("utf-8") if isinstance(frame[1], bytes) else str(frame[1])
                        msg = frame[2] if isinstance(frame[2], bytes) else str(frame[2]).encode("utf-8")
                        for sub_w in subscriber_writers:
                            sub_w.write(
                                f"*3\r\n$7\r\nmessage\r\n${len(ch)}\r\n{ch}\r\n${len(msg)}\r\n".encode("ascii")
                                + msg
                                + b"\r\n"
                            )
                            await sub_w.drain()
                        writer.write(b":1\r\n")
                    elif cmd == "UNKNOWN":
                        writer.write(b"-ERR unknown command\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    valkey_url = f"valkey://127.0.0.1:{port}/0"

    driver = AsyncValkeyDriver(ValkeyConfig(url=valkey_url, enable_client_side_caching=True))
    invalidated_keys: list[list[str]] = []
    driver.add_invalidation_listener(lambda keys: invalidated_keys.append(list(keys)))

    kv = KeyValueStore(driver=driver)
    await kv.set("user:1", "alice")
    await kv.set_many({"user:2": "bob", "user:3": "carol"})

    batch = await kv.get_many(["user:1", "user:2", "user:3", "missing"])
    assert batch["user:1"] == b"alice"
    assert batch["user:2"] == b"bob"
    assert batch["user:3"] == b"carol"
    assert batch["missing"] is None

    eval_result = await driver.eval_script("return 42")
    assert eval_result == "script_ok"

    await driver.execute_command("CLIENT", "TRACKING", "ON")
    assert ["user:1"] in invalidated_keys

    with pytest.raises(Resp3Error, match="ERR unknown command"):
        await driver.execute_command("UNKNOWN")

    bus = ChannelBus(ChannelsBackend(driver=driver))
    await bus.on_startup()
    await bus.subscribe(["events"])

    stream = bus.stream_events()

    async def _publish_remote() -> None:
        await asyncio.sleep(0.02)
        await bus.publish("remote_data", ["events"])

    pub_task = asyncio.create_task(_publish_remote())
    channel, payload = await anext(stream)
    assert channel == "events"
    assert payload == b"remote_data"
    await pub_task

    await bus.on_shutdown()
    await kv.close()
    await driver.close()
    for w in subscriber_writers:
        w.close()
    server.close()
    await server.wait_closed()
