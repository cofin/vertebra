"""Unit tests for native RESP3 wire parser, pooled Redis/Valkey drivers, and Litestar adapters."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from vertebra.db import DatabaseRegistry
from vertebra.db.adapters.redis import (
    AsyncRedisDriver,
    RedisConfig,
    Resp3Error,
    Resp3Parser,
    SyncRedisDriver,
)
from vertebra.db.adapters.valkey import (
    AsyncValkeyDriver,
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


def test_valkey_subclasses_redis_and_handles_invalidation_push(resp3_server: Resp3FakeServer) -> None:
    """SyncValkeyDriver must inherit from SyncRedisDriver and evict keys on invalidation push frames."""
    assert issubclass(SyncValkeyDriver, SyncRedisDriver)
    assert issubclass(AsyncValkeyDriver, AsyncRedisDriver)

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

    with pytest.raises(Resp3Error, match="ERR unknown command"):
        await driver.execute_command("UNKNOWN")

    stream = driver.subscribe("events").__aiter__()
    next_message = asyncio.create_task(anext(stream))
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
