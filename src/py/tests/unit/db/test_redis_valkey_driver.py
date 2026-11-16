"""Unit tests for native RESP3 wire parser, pooled Redis/Valkey drivers, and Litestar adapters."""

from __future__ import annotations

import asyncio
import collections
import hashlib
import os
import time
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

import vertebra._vertebra as core_mod
import vertebra.db.adapters.redis.notify as redis_notify_mod
import vertebra.db.adapters.valkey.driver as valkey_driver_mod
from vertebra.db import DatabaseRegistry, ProgrammingError
from vertebra.db.adapters.redis import (
    AsyncRedisDriver,
    RedisConfig,
    SyncRedisDriver,
)
from vertebra.db.adapters.redis.notify import (
    AsyncRedisNotificationSource,
    RedisNotifyConfig,
)
from vertebra.db.adapters.valkey import (
    AsyncValkeyDriver,
    Resp3Error,
    Resp3Parser,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.extensions.alchemy.cache import SQLAlchemyCacheBackend
from vertebra.extensions.litestar.store import MemoryStore
from vertebra.extensions.starlette.store import CacheStore
from vertebra.framework.stores import KeyValueStore
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from tests.unit.conftest import Resp3FakeServer


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
    """SyncValkeyDriver.encode_hello3_handshake must accept username, password, and client_name."""
    raw = SyncValkeyDriver.encode_hello3_handshake(
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


def test_sync_redis_driver_store_operations(resp3_server: Resp3FakeServer) -> None:
    """SyncRedisDriver over the RESP3 wire must support get, set, delete, pttl, flushdb, eval_script, and pipeline."""
    driver = SyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
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
async def test_async_redis_driver_store_operations(resp3_server: Resp3FakeServer) -> None:
    """AsyncRedisDriver over the RESP3 wire must support async get, set, delete, pttl, publish, flushdb, and eval_script."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    await driver.set("k1", "v1", ex=10)
    assert await driver.get("k1") == b"v1"
    assert (await driver.pttl("k1")) > 0

    assert await driver.publish("news", "headline") == 0

    res = await driver.eval_script("return ARGV[1]", args=["hello"])
    assert res == b"hello"
    assert [c[0] for c in resp3_server.commands if c[0] in (b"EVALSHA", b"EVAL")] == [b"EVALSHA", b"EVAL"]

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
                        got_v = stored_kv.get(k)
                        if got_v is None:
                            writer.write(b"_\r\n")
                        else:
                            writer.write(f"${len(got_v)}\r\n".encode("ascii") + got_v + b"\r\n")
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


def test_valkey_subclasses_redis_and_handles_invalidation_push(resp3_server: Resp3FakeServer) -> None:
    """SyncValkeyDriver must evict keys on invalidation push frames."""
    driver = SyncValkeyDriver(ValkeyConfig(url=resp3_server.valkey_url, enable_client_side_caching=True))
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
async def test_valkey_resp3_client_tracking_and_litestar_integration(resp3_server: Resp3FakeServer) -> None:
    """MemoryStore must propagate delete_all, expires_in, and Valkey push invalidations."""
    store = MemoryStore.from_valkey(resp3_server.valkey_url)
    await store.set("k1", "val1", expires_in=30)
    assert await store.get("k1") == b"val1"
    ttl = await store.expires_in("k1")
    assert ttl is not None and 0 < ttl <= 30

    vdriver = store.driver
    assert isinstance(vdriver, AsyncValkeyDriver)
    vdriver.handle_invalidation_push(["vertebra:k1"])
    assert "vertebra:k1" not in store._entries

    await store.delete_all()
    assert await store.get("k1") is None
    assert await store.expires_in("k1") is None
    if store.driver is not None:
        await store.driver.close()


@pytest.mark.anyio
async def test_key_value_store_batch_and_namespace(resp3_server: Resp3FakeServer) -> None:
    """Key/value caching preserves batch operations and namespaced key isolation over Valkey."""
    kv = KeyValueStore.from_valkey(resp3_server.valkey_url)
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
    await kv.close()


@pytest.mark.anyio
async def test_resp3_loopback_server_pipeline_evalsha_and_valkey_push_invalidation() -> None:
    """Valkey caching and pub/sub execute against a synchronized loopback RESP3 server."""
    stored_kv: dict[str, bytes] = {}
    cached_scripts: set[str] = set()

    subscriber_writers: list[asyncio.StreamWriter] = []
    subscription_ready = asyncio.Event()
    published_frames: list[tuple[str, bytes]] = []

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
                        got_v = stored_kv.get(k)
                        if got_v is None:
                            writer.write(b"_\r\n")
                        else:
                            writer.write(f"${len(got_v)}\r\n".encode("ascii") + got_v + b"\r\n")
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
                        published_frames.append((ch, msg))
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
                    if cmd == "SUBSCRIBE":
                        subscription_ready.set()
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

    with pytest.raises((Resp3Error, ProgrammingError), match="ERR unknown command"):
        await driver.execute_command("UNKNOWN")

    stream = driver.subscribe("events").__aiter__()
    next_message: asyncio.Task[tuple[str, bytes]] = asyncio.create_task(stream.__anext__())
    await asyncio.wait_for(subscription_ready.wait(), timeout=2)
    assert len(subscriber_writers) == 1
    assert await driver.publish("events", b"remote_data\x00\xff") == 1
    assert await asyncio.wait_for(next_message, timeout=2) == ("events", b"remote_data\x00\xff")
    assert published_frames == [("events", b"remote_data\x00\xff")]
    await stream.aclose()

    await kv.close()
    await driver.close()
    for w in subscriber_writers:
        w.close()
    server.close()
    await server.wait_closed()


_STORE_CLASSES: list[type[Any]] = [KeyValueStore, MemoryStore]
_STORE_IDS = ["framework", "litestar"]


@pytest.mark.anyio
async def test_redis_driver_scan_unlink_and_px(resp3_server: Resp3FakeServer) -> None:
    """The RESP3 driver supports PX TTLs, prefix SCAN, and UNLINK."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    await driver.set("cache:a", b"1", px=250)
    await driver.set("cache:b", b"2")
    await driver.set("other:c", b"3")
    assert 0 < await driver.pttl("cache:a") <= 250
    cursor, keys = await driver.scan(0, match="cache:*")
    assert cursor == 0
    assert sorted(keys) == ["cache:a", "cache:b"]
    assert await driver.unlink("cache:a", "cache:b") == 2
    assert await driver.get("cache:a") is None
    assert await driver.get("other:c") == b"3"
    with pytest.raises(ValueError, match="ex or px"):
        await driver.set("k", b"v", ex=1, px=1000)
    await driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
async def test_store_delete_all_removes_only_its_prefix(
    store_cls: type[Any],
    resp3_server: Resp3FakeServer,
) -> None:
    """delete_all removes the store's prefixed keys and leaves other keys in the database."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    await driver.set("other:keep", b"x")
    store = store_cls(driver=driver)
    await store.set("a", "1")
    await store.set("b", "2", expires_in=60)
    await store.delete_all()
    assert await driver.get("other:keep") == b"x"
    assert await driver.get("vertebra:a") is None
    assert await store.get("a") is None
    assert await store.get("b") is None
    await driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
async def test_store_sends_sub_second_ttl_as_milliseconds(
    store_cls: type[Any],
    resp3_server: Resp3FakeServer,
) -> None:
    """A 250 ms expiry reaches the driver as a PX TTL instead of EX 0."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    store = store_cls(driver=driver)
    await store.set("short", "v", expires_in=timedelta(milliseconds=250))
    assert 0 < await driver.pttl("vertebra:short") <= 250
    assert await store.get("short") == b"v"
    await driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
@pytest.mark.parametrize("backed", [False, True], ids=["local", "driver"])
async def test_store_expires_in_returns_none_without_expiry(
    store_cls: type[Any],
    backed: bool,
    resp3_server: Resp3FakeServer,
) -> None:
    """expires_in returns None for keys without expiry and for missing keys."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url)) if backed else None
    writer = store_cls(driver=driver)
    await writer.set("forever", "v")
    reader = store_cls(driver=driver) if backed else writer
    assert await reader.expires_in("forever") is None
    assert await reader.expires_in("missing") is None
    if driver is not None:
        await driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
@pytest.mark.parametrize("expires_in", [0, -5, timedelta(0)], ids=["zero", "negative", "zero-delta"])
async def test_store_rejects_non_positive_ttl(store_cls: type[Any], expires_in: int | timedelta) -> None:
    """A zero or negative expiry raises before the key is written."""
    store = store_cls()
    with pytest.raises(ValueError, match="expires_in must be a positive duration"):
        await store.set("k", "v", expires_in=expires_in)
    assert await store.get("k") is None


@pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
def test_store_rejects_invalid_key_prefix(store_cls: type[Any]) -> None:
    """Key prefixes containing glob characters are rejected."""
    with pytest.raises(ValueError, match="key_prefix"):
        store_cls(key_prefix="cache*")


@pytest.mark.parametrize(
    ("cfg", "expected_len", "expected_prefix"),
    [
        (RedisConfig(url="redis://localhost:6379/0"), 2, ("redis", "redis://localhost:6379/0")),
        (ValkeyConfig(url="valkey://localhost:6379/0"), 2, ("valkey", "valkey://localhost:6379/0")),
        (
            RedisConfig(url="redis://localhost:6379/0", username="alice"),
            13,
            ("redis", "redis://localhost:6379/0", "alice", 3),
        ),
        (
            RedisConfig(url="redis://localhost:6379/0", protocol=2),
            13,
            ("redis", "redis://localhost:6379/0", "", 2),
        ),
        (
            RedisConfig(url="redis://localhost:6379/0", topology="sentinel", sentinel_master_name="mymaster"),
            13,
            ("redis", "redis://localhost:6379/0", "", 3, "sentinel", "mymaster"),
        ),
        (
            RedisConfig(url="redis://localhost:6379/0", enable_client_tracking=True),
            13,
            ("redis", "redis://localhost:6379/0"),
        ),
        (
            ValkeyConfig(url="valkey://localhost:6379/0", enable_client_side_caching=False),
            13,
            ("valkey", "valkey://localhost:6379/0"),
        ),
        (
            ValkeyConfig(url="valkey://localhost:6379/0", client_tracking_prefixes=("tenant:",)),
            13,
            ("valkey", "valkey://localhost:6379/0"),
        ),
        (
            RedisConfig(url="redis://localhost:6379/0", enable_gcp_iam_auth=True),
            13,
            ("redis", "redis://localhost:6379/0"),
        ),
        (
            ValkeyConfig(
                url="valkey://localhost:6379/0",
                memorystore_instance_id="projects/p/locations/r/instances/i",
            ),
            13,
            ("valkey", "valkey://localhost:6379/0"),
        ),
    ],
    ids=[
        "redis-default",
        "valkey-default",
        "redis-username",
        "redis-resp2",
        "redis-sentinel",
        "redis-tracking-on",
        "valkey-tracking-off",
        "valkey-tracking-prefixes",
        "redis-gcp-iam",
        "valkey-memorystore-id",
    ],
)
def test_redis_and_valkey_pool_key_compact_defaults_and_option_expansion(
    cfg: RedisConfig | ValkeyConfig,
    expected_len: int,
    expected_prefix: tuple[Any, ...],
) -> None:
    """Default RedisConfig and ValkeyConfig emit compact 2-tuples while non-default options expand to 13-tuples."""
    key = cfg.pool_key()
    assert len(key) == expected_len
    assert key[: len(expected_prefix)] == expected_prefix


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("cfg_cls", "url", "sync_cls", "async_cls"),
    [
        (RedisConfig, "redis://127.0.0.1:6379/0", SyncRedisDriver, AsyncRedisDriver),
        (ValkeyConfig, "valkey://127.0.0.1:6379/0", SyncValkeyDriver, AsyncValkeyDriver),
    ],
    ids=["redis", "valkey"],
)
async def test_redis_and_valkey_build_drivers_propagate_broker(
    cfg_cls: type[RedisConfig] | type[ValkeyConfig],
    url: str,
    sync_cls: type[SyncRedisDriver],
    async_cls: type[AsyncRedisDriver],
) -> None:
    """build_sync_driver and build_async_driver propagate explicit config.broker or registry._broker."""
    reg_broker = object()
    registry = DatabaseRegistry(broker=reg_broker)
    cfg_from_reg = cfg_cls(url=url)
    sync_drv = cfg_from_reg.build_sync_driver(registry)
    async_drv = cfg_from_reg.build_async_driver(registry)
    try:
        assert isinstance(sync_drv, sync_cls)
        assert isinstance(async_drv, async_cls)
        assert sync_drv.broker is reg_broker
        assert async_drv.broker is reg_broker
    finally:
        sync_drv.close()
        await async_drv.close()

    explicit_broker = object()
    cfg_explicit = cfg_cls(url=url, broker=explicit_broker)
    sync_exp = cfg_explicit.build_sync_driver(registry)
    async_exp = cfg_explicit.build_async_driver(registry)
    try:
        assert sync_exp.broker is explicit_broker
        assert async_exp.broker is explicit_broker
    finally:
        sync_exp.close()
        await async_exp.close()


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda: SyncRedisDriver(RedisConfig(url="memory://")), id="sync-redis"),
        pytest.param(lambda: AsyncRedisDriver(RedisConfig(url="MEMORY://")), id="async-redis"),
        pytest.param(lambda: SyncValkeyDriver(ValkeyConfig(url="memory://")), id="sync-valkey"),
        pytest.param(lambda: AsyncValkeyDriver(ValkeyConfig(url="memory://")), id="async-valkey"),
        pytest.param(lambda: KeyValueStore.from_valkey("memory://"), id="kv-valkey"),
        pytest.param(lambda: KeyValueStore.from_redis("memory://"), id="kv-redis"),
        pytest.param(lambda: MemoryStore.from_valkey("memory://"), id="litestar-valkey"),
        pytest.param(lambda: MemoryStore.from_redis("memory://"), id="litestar-redis"),
        pytest.param(lambda: CacheStore.from_valkey("memory://"), id="starlette-valkey"),
        pytest.param(lambda: CacheStore.from_redis("memory://"), id="starlette-redis"),
        pytest.param(lambda: SQLAlchemyCacheBackend.from_valkey("memory://"), id="alchemy-valkey"),
        pytest.param(lambda: SQLAlchemyCacheBackend.from_redis("memory://"), id="alchemy-redis"),
    ],
)
def test_redis_drivers_and_store_factories_reject_memory_url(factory: Callable[[], object]) -> None:
    """Redis and Valkey drivers and store/cache factories reject memory:// URLs with ImproperConfigurationError."""
    with pytest.raises(ImproperConfigurationError, match="memory://"):
        factory()


def test_legacy_resp3_bindings_removed_from_vertebra_core() -> None:
    """Legacy RESP3 functions must not be exposed by vertebra._vertebra."""
    assert not hasattr(core_mod, "encode_resp_command")
    assert not hasattr(core_mod, "encode_hello3_handshake")
    assert not hasattr(core_mod, "crc16_xmodem")
    assert not hasattr(core_mod, "redis_hash_slot")
    assert not hasattr(core_mod, "parse_cluster_redirect")


def test_sync_valkey_9_hash_field_expiration(resp3_server: Resp3FakeServer) -> None:
    """SyncValkeyDriver executes Valkey 9.0 hash-field expiration commands."""
    driver = SyncValkeyDriver(ValkeyConfig(url=resp3_server.valkey_url))
    try:
        driver.execute_command("HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3")

        res = driver.hexpire("myhash", 60, "f1")
        assert res == [1]
        assert driver.httl("myhash", "f1")[0] > 0

        res = driver.hpexpire("myhash", 60000, "f2")
        assert res == [1]
        assert driver.hpttl("myhash", "f2")[0] > 0

        target_time = int(time.time()) + 120
        res = driver.hexpireat("myhash", target_time, "f1")
        assert res == [1]

        target_time_ms = int(time.time() * 1000) + 120000
        res = driver.hpexpireat("myhash", target_time_ms, "f2")
        assert res == [1]

        persist_res = driver.hpersist("myhash", "f1", "f2", "f3")
        assert persist_res == [1, 1, -1]
        assert driver.httl("myhash", "f1") == [-1]
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_valkey_9_hash_field_expiration(resp3_server: Resp3FakeServer) -> None:
    """AsyncValkeyDriver executes Valkey 9.0 hash-field expiration commands."""
    driver = AsyncValkeyDriver(ValkeyConfig(url=resp3_server.valkey_url))
    try:
        await driver.execute_command("HSET", "myhash_async", "f1", "v1", "f2", "v2")

        res = await driver.hexpire("myhash_async", 100, "f1")
        assert res == [1]
        ttl = await driver.httl("myhash_async", "f1")
        assert ttl[0] > 0

        res_p = await driver.hpexpire("myhash_async", 100000, "f2")
        assert res_p == [1]
        pttl = await driver.hpttl("myhash_async", "f2")
        assert pttl[0] > 0

        now_sec = int(time.time()) + 300
        res_at = await driver.hexpireat("myhash_async", now_sec, "f1")
        assert res_at == [1]

        now_ms = int(time.time() * 1000) + 300000
        res_pat = await driver.hpexpireat("myhash_async", now_ms, "f2")
        assert res_pat == [1]

        persist_res = await driver.hpersist("myhash_async", "f1", "f2")
        assert persist_res == [1, 1]
    finally:
        await driver.close()


def test_valkey_client_capa_negotiation(resp3_server: Resp3FakeServer) -> None:
    """Valkey driver negotiates CLIENT CAPA redirect during connection establishment."""
    cfg = ValkeyConfig(url=resp3_server.valkey_url)
    driver = SyncValkeyDriver(cfg)
    try:
        driver.set("capa_key", "capa_val")
        assert driver.get("capa_key") == b"capa_val"
        commands = [cmd[0].decode("ascii", errors="replace").upper() for cmd in resp3_server.commands]
        assert "CLIENT" in commands
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_redis_driver_reconnect_awaits_native_client() -> None:
    """AsyncRedisDriver.reconnect awaits the native AsyncRedisClient.reconnect coroutine."""
    reconnected_passwords: list[str | None] = []

    class _FakeAsyncRedisClient:
        async def reconnect(self, password: str | None = None) -> None:
            await asyncio.sleep(0)
            reconnected_passwords.append(password)

        def close(self) -> None:
            return None

    fake_client: Any = _FakeAsyncRedisClient()
    driver = AsyncRedisDriver.__new__(AsyncRedisDriver)
    driver._active_listener = None
    driver._client = fake_client
    await driver.reconnect(password="rotated-token")
    assert reconnected_passwords == ["rotated-token"]
    await driver.close()


@pytest.mark.anyio
async def test_async_redis_notification_source_nonblocking_notifier_fd(monkeypatch: pytest.MonkeyPatch) -> None:
    """AsyncRedisNotificationSource uses submit_*subscribe, notifier_fd, and drain_* without blocking calls."""
    monkeypatch.setattr(redis_notify_mod, "ensure_module", lambda *_args, **_kwargs: object())
    r_fd, w_fd = os.pipe()
    os.set_blocking(r_fd, False)

    class _FakeNonBlockingListener:
        def __init__(self, *, fail_subscribe: bool = False) -> None:
            self.closed = False
            self.submitted_channels: list[list[str]] = []
            self._fail_subscribe = fail_subscribe
            self._next_op_id = 0
            self._completions: collections.deque[tuple[int, bool, str | None]] = collections.deque()
            self._messages: collections.deque[tuple[str, str, bytes, str | None]] = collections.deque()

        def notifier_fd(self) -> int:
            return r_fd

        def submit_subscribe(self, channels: list[str]) -> int:
            self._next_op_id += 1
            op_id = self._next_op_id
            self.submitted_channels.append(list(channels))
            if self._fail_subscribe:
                self._completions.append((op_id, False, "ERR acl denied"))
            else:
                self._completions.append((op_id, True, None))
            os.write(w_fd, b"\x01")
            return op_id

        def subscribe(self, _channels: list[str]) -> None:
            raise AssertionError("Blocking subscribe must not be called when notifier_fd is active")

        def next_message(self, _timeout_ms: int) -> Any:
            raise AssertionError("Blocking next_message must not be called when notifier_fd is active")

        def drain_completions(self) -> list[tuple[int, bool, str | None]]:
            items = list(self._completions)
            self._completions.clear()
            return items

        def drain_messages(self) -> list[tuple[str, str, bytes, str | None]]:
            items = list(self._messages)
            self._messages.clear()
            return items

        def push_message(self, channel: str, payload: bytes) -> None:
            self._messages.append(("message", channel, payload, None))
            os.write(w_fd, b"\x01")

        def close(self) -> None:
            self.closed = True

    class _FakeClient:
        def __init__(self, listener: _FakeNonBlockingListener) -> None:
            self._listener = listener

        def listener(self) -> _FakeNonBlockingListener:
            return self._listener

    try:
        ok_listener = _FakeNonBlockingListener()
        client_arg: Any = _FakeClient(ok_listener)
        source = AsyncRedisNotificationSource(
            config=RedisNotifyConfig(channels=("jobs",), timeout_ms=25),
            client=client_arg,
        )

        async def _consume_one() -> tuple[str, bytes]:
            async for item in source.listen():
                return item.channel, item.payload
            msg = "Stream ended unexpectedly"
            raise AssertionError(msg)

        first_event_task: asyncio.Task[tuple[str, bytes]] = asyncio.create_task(_consume_one())
        await asyncio.sleep(0.01)
        assert source._reader_registered is True
        assert ok_listener.submitted_channels == [["jobs"]]
        ok_listener.push_message("jobs", b"wakeup-1")
        ch_name, payload_bytes = await asyncio.wait_for(first_event_task, timeout=1.0)
        assert ch_name == "jobs"
        assert payload_bytes == b"wakeup-1"
        await source.aclose()
        assert source._reader_registered is False
        assert ok_listener.closed is True

        err_listener = _FakeNonBlockingListener(fail_subscribe=True)
        err_client_arg: Any = _FakeClient(err_listener)
        err_source = AsyncRedisNotificationSource(
            config=RedisNotifyConfig(channels=("restricted",), timeout_ms=25),
            client=err_client_arg,
        )
        with pytest.raises(ProgrammingError, match="ERR acl denied"):
            await err_source.listen().__anext__()
        await err_source.aclose()
    finally:
        os.close(r_fd)
        os.close(w_fd)


@pytest.mark.anyio
async def test_valkey_discover_sentinel_master_passes_valkey_client_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SyncValkeyDriver and AsyncValkeyDriver discover_sentinel_master pass ValkeyClientConfig instances."""
    constructed_sync_configs: list[Any] = []
    constructed_async_configs: list[Any] = []

    class _FakeValkeyClientConfig:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.url = kwargs["url"]

    class _FakeSyncValkeyClient:
        def __init__(self, cfg: Any) -> None:
            if not isinstance(cfg, _FakeValkeyClientConfig):
                msg = f"Expected ValkeyClientConfig, got {type(cfg).__name__}"
                raise TypeError(msg)
            self.cfg = cfg
            self.closed = False
            constructed_sync_configs.append(cfg)

        def execute_command(self, *parts: Any) -> Any:
            assert parts[:2] == ("SENTINEL", "get-master-addr-by-name")
            if "26379" in self.cfg.url:
                raise ConnectionError("seed 1 down")
            return [b"10.0.0.9", b"6380"]

        def close(self) -> None:
            self.closed = True

    class _FakeAsyncValkeyClient:
        def __init__(self, cfg: Any) -> None:
            if not isinstance(cfg, _FakeValkeyClientConfig):
                msg = f"Expected ValkeyClientConfig, got {type(cfg).__name__}"
                raise TypeError(msg)
            self.cfg = cfg
            self.closed = False
            constructed_async_configs.append(cfg)

        async def execute_command(self, *parts: Any) -> Any:
            assert parts[:2] == ("SENTINEL", "get-master-addr-by-name")
            if "26379" in self.cfg.url:
                raise ConnectionError("seed 1 down")
            return [b"10.0.0.11", b"6381"]

        async def close(self) -> None:
            self.closed = True

    class _FakeValkeyModule:
        ValkeyClientConfig = _FakeValkeyClientConfig
        SyncValkeyClient = _FakeSyncValkeyClient
        AsyncValkeyClient = _FakeAsyncValkeyClient

    monkeypatch.setattr(valkey_driver_mod, "ensure_module", lambda *_args, **_kwargs: _FakeValkeyModule)

    valkey_cfg = ValkeyConfig(
        url="valkey://127.0.0.1:6379/0",
        topology="sentinel",
        sentinel_master_name="mymaster",
        sentinel_nodes=(("127.0.0.1", 26379), ("127.0.0.1", 26380)),
        username="svc",
        password="secret",
        client_name="vtb-sentinel",
    )

    sync_driver = SyncValkeyDriver(valkey_cfg)
    initial_sync_client: Any = sync_driver._client
    assert sync_driver.discover_sentinel_master() == ("10.0.0.9", 6380)
    assert initial_sync_client.closed is True
    assert constructed_sync_configs[-1].url == "valkey://10.0.0.9:6380/0"
    assert constructed_sync_configs[-1].kwargs["username"] == "svc"
    assert constructed_sync_configs[-1].kwargs["password"] == "secret"
    sync_driver.close()

    async_driver = AsyncValkeyDriver(valkey_cfg)
    initial_async_client: Any = async_driver._client
    assert await async_driver.discover_sentinel_master() == ("10.0.0.11", 6381)
    assert initial_async_client.closed is True
    active_async_client: Any = async_driver._client
    assert active_async_client.cfg.url == "valkey://10.0.0.11:6381/0"
    assert active_async_client.cfg.kwargs["username"] == "svc"
    assert active_async_client.cfg.kwargs["password"] == "secret"
    await async_driver.close()
