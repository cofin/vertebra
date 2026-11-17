"""Unit tests for native RESP3 wire parser, pooled Redis/Valkey drivers, and Litestar adapters."""

from __future__ import annotations

import asyncio
import collections
import hashlib
import importlib
import os
import socket
import sys
import threading
import time
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

import vertebra._redis as native_redis
import vertebra._valkey as native_valkey
import vertebra._vertebra as core_mod
import vertebra.db.adapters.redis as redis_pkg
import vertebra.db.adapters.redis._async as redis_async_mod
import vertebra.db.adapters.redis._sync as redis_sync_mod
import vertebra.db.adapters.redis.driver as redis_driver_barrel
import vertebra.db.adapters.redis.notify as redis_notify_mod
import vertebra.db.adapters.valkey as valkey_pkg
import vertebra.db.adapters.valkey._async as valkey_async_mod
import vertebra.db.adapters.valkey._sync as valkey_sync_mod
import vertebra.db.adapters.valkey.core as valkey_core_mod
import vertebra.db.adapters.valkey.driver as valkey_driver_mod
import vertebra.db.driver._resp_queue as resp_store_mod
import vertebra.framework.queues._resp as resp_queue_mod
from vertebra.db import DatabaseError, DatabaseRegistry, ProgrammingError
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
    AsyncValkeyQueueStore,
    Resp3Error,
    Resp3Parser,
    SyncValkeyDriver,
    SyncValkeyQueueStore,
    ValkeyConfig,
)
from vertebra.db.adapters.valkey.queue import (
    AsyncValkeyQueueStore as DirectAsyncValkeyQueueStore,
)
from vertebra.db.adapters.valkey.queue import (
    SyncValkeyQueueStore as DirectSyncValkeyQueueStore,
)
from vertebra.db.driver import (
    AZURE_REDIS_SCOPE,
    AsyncQueueStore,
    AzureEntraTokenProvider,
    QueueEntry,
    QueueReservation,
    SyncQueueStore,
)
from vertebra.db.driver import Resp3Error as DriverResp3Error
from vertebra.db.driver._resp import ClusterRedirect as SharedClusterRedirect
from vertebra.db.driver._resp import Resp3Error as SharedResp3Error
from vertebra.extensions.litestar.store import MemoryStore
from vertebra.extensions.starlette.store import CacheStore
from vertebra.framework.queues import QueueConfig, ValkeyQueueBackend
from vertebra.framework.stores import KeyValueStore
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    VertebraError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json, encode_json

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

    monkeypatch.setattr(valkey_sync_mod, "ensure_module", lambda *_args, **_kwargs: _FakeValkeyModule)
    monkeypatch.setattr(valkey_async_mod, "ensure_module", lambda *_args, **_kwargs: _FakeValkeyModule)

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


def test_shared_resp3_error_subclass_and_offset() -> None:
    """Shared Resp3Error subclasses DatabaseError and VertebraError while preserving next_offset."""
    assert DriverResp3Error is SharedResp3Error
    assert valkey_core_mod.Resp3Error is DriverResp3Error
    assert valkey_core_mod.ClusterRedirect is SharedClusterRedirect
    assert issubclass(SharedResp3Error, DatabaseError)
    assert issubclass(SharedResp3Error, VertebraError)
    assert issubclass(valkey_core_mod.Resp3Error, DatabaseError)

    err = SharedResp3Error("ERR boom", next_offset=17)
    assert isinstance(err, DatabaseError)
    assert isinstance(err, VertebraError)
    assert str(err) == "ERR boom"
    assert err.next_offset == 17
    assert SharedResp3Error("ERR default").next_offset == 0


@pytest.mark.parametrize(
    ("pkg", "barrel", "symbol_name", "module_path"),
    [
        pytest.param(
            redis_pkg,
            redis_driver_barrel,
            "SyncRedisDriver",
            "vertebra.db.adapters.redis._sync",
            id="sync-redis-driver",
        ),
        pytest.param(
            redis_pkg,
            redis_driver_barrel,
            "AsyncRedisDriver",
            "vertebra.db.adapters.redis._async",
            id="async-redis-driver",
        ),
        pytest.param(
            valkey_pkg,
            valkey_driver_mod,
            "SyncValkeyDriver",
            "vertebra.db.adapters.valkey._sync",
            id="sync-valkey-driver",
        ),
        pytest.param(
            valkey_pkg,
            valkey_driver_mod,
            "AsyncValkeyDriver",
            "vertebra.db.adapters.valkey._async",
            id="async-valkey-driver",
        ),
    ],
)
def test_driver_split_preserves_import_identity(
    pkg: Any,
    barrel: Any,
    symbol_name: str,
    module_path: str,
) -> None:
    """Sync and Async Redis/Valkey drivers maintain object identity across package, barrel, and split modules."""
    direct_mod = importlib.import_module(module_path)
    pkg_obj = getattr(pkg, symbol_name)
    barrel_obj = getattr(barrel, symbol_name)
    direct_obj = getattr(direct_mod, symbol_name)
    assert pkg_obj is barrel_obj
    assert barrel_obj is direct_obj


@pytest.mark.parametrize(
    ("backend", "topology"),
    [
        pytest.param("redis", "standalone", id="redis-standalone"),
        pytest.param("redis", "sentinel", id="redis-sentinel"),
        pytest.param("valkey", "standalone", id="valkey-standalone"),
        pytest.param("valkey", "sentinel", id="valkey-sentinel"),
    ],
)
@pytest.mark.anyio
async def test_native_non_cluster_clients_return_empty_slot_map(
    backend: str,
    topology: str,
) -> None:
    """Standalone and Sentinel native Redis/Valkey clients return empty slot maps without blocking."""
    if backend == "redis":
        sentinel_name = "mymaster" if topology == "sentinel" else None
        sentinel_nodes = [("127.0.0.1", 26379)] if topology == "sentinel" else []
        sync_client = native_redis.SyncRedisClient(
            "redis://127.0.0.1:6379/0",
            topology=topology,
            sentinel_master_name=sentinel_name,
            sentinel_nodes=sentinel_nodes,
        )
        async_client = native_redis.AsyncRedisClient(
            "redis://127.0.0.1:6379/0",
            topology=topology,
            sentinel_master_name=sentinel_name,
            sentinel_nodes=sentinel_nodes,
        )
        try:
            assert sync_client.slot_map() == {}
            assert async_client.slot_map() == {}
        finally:
            sync_client.close()
            async_client.close()
        return

    cfg = native_valkey.ValkeyClientConfig(
        url="valkey://127.0.0.1:6379/0",
        sentinel_service_name="mymaster" if topology == "sentinel" else None,
        sentinel_nodes=["127.0.0.1:26379"] if topology == "sentinel" else None,
    )
    sync_valkey = native_valkey.SyncValkeyClient(cfg)
    async_valkey = native_valkey.AsyncValkeyClient(cfg)
    try:
        assert sync_valkey.slot_map() == {}
        assert async_valkey.slot_map() == {}
    finally:
        sync_valkey.close()
        await async_valkey.close()


@pytest.mark.parametrize(
    "driver_cls",
    [
        pytest.param(SyncRedisDriver, id="sync-redis"),
        pytest.param(AsyncRedisDriver, id="async-redis"),
        pytest.param(SyncValkeyDriver, id="sync-valkey"),
        pytest.param(AsyncValkeyDriver, id="async-valkey"),
    ],
)
def test_redis_and_valkey_drivers_reject_removed_socket_factory_and_client_cache(driver_cls: type[Any]) -> None:
    """Removed socket_factory kwarg and _client_cache property must fail cleanly."""
    assert hasattr(driver_cls, "_client_cache") is False
    with pytest.raises(TypeError, match="socket_factory"):
        driver_cls(socket_factory=lambda *_args, **_kwargs: None)


@pytest.mark.parametrize(
    ("backend", "is_async"),
    [
        pytest.param("redis", False, id="redis-sync"),
        pytest.param("redis", True, id="redis-async"),
        pytest.param("valkey", False, id="valkey-sync"),
        pytest.param("valkey", True, id="valkey-async"),
    ],
)
@pytest.mark.parametrize("use_tls", [False, True], ids=["plain", "tls"])
@pytest.mark.anyio
async def test_discover_sentinel_master_preserves_tls_tracking_and_iam(
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    is_async: bool,
    use_tls: bool,
) -> None:
    """discover_sentinel_master preserves TLS scheme, tracking, IAM, protocol, and pool size across reconnects."""
    constructed_clients: list[Any] = []
    probe_clients: list[Any] = []

    def token_provider() -> str:
        return "test-token-placeholder"

    class _FakeRedisClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.url = kwargs["url"]
            self.closed = False
            constructed_clients.append(self)

        def execute_command(self, cmd: Any) -> Any:
            probe_clients.append(self)
            assert cmd[:2] == ["SENTINEL", "get-master-addr-by-name"]
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            return [b"10.0.0.21", b"6382"]

        def close(self) -> None:
            self.closed = True

    class _FakeAsyncRedisClient(_FakeRedisClient):
        async def execute_command(self, cmd: Any) -> Any:
            return super().execute_command(cmd)

    class _FakeRedisModule:
        SyncRedisClient = _FakeRedisClient
        AsyncRedisClient = _FakeAsyncRedisClient

    class _FakeValkeyClientConfig:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.url = kwargs["url"]

    class _FakeValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.cfg = cfg
            self.url = cfg.url
            self.closed = False
            constructed_clients.append(self)

        def execute_command(self, *parts: Any) -> Any:
            probe_clients.append(self)
            assert parts[:2] == ("SENTINEL", "get-master-addr-by-name")
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            return [b"10.0.0.22", b"6383"]

        def close(self) -> None:
            self.closed = True

    class _FakeAsyncValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.cfg = cfg
            self.url = cfg.url
            self.closed = False
            constructed_clients.append(self)

        async def execute_command(self, *parts: Any) -> Any:
            probe_clients.append(self)
            assert parts[:2] == ("SENTINEL", "get-master-addr-by-name")
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            return [b"10.0.0.22", b"6383"]

        async def close(self) -> None:
            self.closed = True

    class _FakeValkeyModule:
        ValkeyClientConfig = _FakeValkeyClientConfig
        SyncValkeyClient = _FakeValkeyClient
        AsyncValkeyClient = _FakeAsyncValkeyClient

    if backend == "redis":
        monkeypatch.setattr(redis_sync_mod, "ensure_module", lambda *_a, **_kw: _FakeRedisModule)
        monkeypatch.setattr(redis_async_mod, "ensure_module", lambda *_a, **_kw: _FakeRedisModule)
        scheme = "rediss" if use_tls else "redis"
        redis_cfg = RedisConfig(
            url=f"{scheme}://127.0.0.1:6379/2",
            username="svc",
            password="test-token-placeholder",
            client_name="vtb-sentinel-tls",
            protocol=3,
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=(("127.0.0.1", 26379), ("127.0.0.1", 26380)),
            enable_client_side_caching=True,
            client_tracking_prefixes=("user:", "order:"),
            client_cache_capacity=2048,
            client_cache_ttl_seconds=12.5,
            enable_gcp_iam_auth=True,
            iam_token_provider=token_provider,
            max_connections=64,
        )
        driver: Any = AsyncRedisDriver(redis_cfg) if is_async else SyncRedisDriver(redis_cfg)
    else:
        monkeypatch.setattr(valkey_sync_mod, "ensure_module", lambda *_a, **_kw: _FakeValkeyModule)
        monkeypatch.setattr(valkey_async_mod, "ensure_module", lambda *_a, **_kw: _FakeValkeyModule)
        scheme = "valkeys" if use_tls else "valkey"
        valkey_cfg = ValkeyConfig(
            url=f"{scheme}://127.0.0.1:6379/2",
            username="svc",
            password="test-token-placeholder",
            client_name="vtb-sentinel-tls",
            protocol=3,
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=(("127.0.0.1", 26379), ("127.0.0.1", 26380)),
            enable_client_side_caching=True,
            client_tracking_prefixes=("user:", "order:"),
            client_cache_capacity=2048,
            client_cache_ttl_seconds=12.5,
            enable_gcp_iam_auth=True,
            iam_token_provider=token_provider,
            max_connections=64,
        )
        driver = AsyncValkeyDriver(valkey_cfg) if is_async else SyncValkeyDriver(valkey_cfg)

    initial_client = driver._client
    discovered = await driver.discover_sentinel_master() if is_async else driver.discover_sentinel_master()

    expected_addr = ("10.0.0.21", 6382) if backend == "redis" else ("10.0.0.22", 6383)
    assert discovered == expected_addr
    assert initial_client.closed is True
    assert all(probe.url.startswith(f"{scheme}://") for probe in probe_clients)
    assert all(probe.closed is True for probe in probe_clients)
    assert driver.config.url == f"{scheme}://{expected_addr[0]}:{expected_addr[1]}/2"
    assert driver.config.uses_tls is use_tls

    active_client = constructed_clients[-1]
    assert driver._client is active_client
    if backend == "redis":
        kwargs = active_client.kwargs
        assert kwargs["url"] == f"{scheme}://10.0.0.21:6382/2"
        assert kwargs["username"] == "svc"
        assert kwargs["password"] == "test-token-placeholder"
        assert kwargs["protocol"] == 3
        assert kwargs["enable_client_side_caching"] is True
        assert kwargs["client_tracking_prefixes"] == ["user:", "order:"]
        assert kwargs["client_cache_capacity"] == 2048
        assert kwargs["client_cache_ttl_seconds"] == 12.5
        assert kwargs["enable_gcp_iam_auth"] is True
        assert kwargs["max_connections"] == 64
        assert driver.config.iam_token_provider is token_provider
    else:
        kwargs = active_client.cfg.kwargs
        assert kwargs["url"] == f"{scheme}://10.0.0.22:6383/2"
        assert kwargs["username"] == "svc"
        assert kwargs["password"] == "test-token-placeholder"
        assert kwargs["protocol_resp3"] is True
        assert kwargs["client_tracking_enabled"] is True
        assert kwargs["client_tracking_prefixes"] == ["user:", "order:"]
        assert kwargs["client_tracking_capacity"] == 2048
        assert kwargs["client_tracking_ttl_ms"] == 12500
        assert kwargs["enable_gcp_memorystore_iam"] is True
        assert kwargs["token_provider"] is token_provider
        assert kwargs["pool_max_size"] == 64

    rediscovered = await driver.discover_sentinel_master() if is_async else driver.discover_sentinel_master()
    assert rediscovered == expected_addr
    if is_async:
        await driver.close()
    else:
        driver.close()


@pytest.mark.parametrize(
    ("backend", "is_async"),
    [
        pytest.param("redis", False, id="redis-sync"),
        pytest.param("redis", True, id="redis-async"),
        pytest.param("valkey", False, id="valkey-sync"),
        pytest.param("valkey", True, id="valkey-async"),
    ],
)
@pytest.mark.parametrize("empty_nodes", [False, True], ids=["all-fail", "empty-nodes"])
@pytest.mark.anyio
async def test_discover_sentinel_master_raises_last_failure_and_preserves_client(
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    is_async: bool,
    empty_nodes: bool,
) -> None:
    """discover_sentinel_master chains ImproperConfigurationError from the last failure without closing _client."""
    probe_clients: list[Any] = []

    class _FailingRedisClient:
        def __init__(self, **kwargs: Any) -> None:
            self.url = kwargs["url"]
            self.closed = False

        def execute_command(self, _cmd: Any) -> Any:
            probe_clients.append(self)
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            raise TimeoutError("seed 2 timed out")

        def close(self) -> None:
            self.closed = True

    class _FailingAsyncRedisClient(_FailingRedisClient):
        async def execute_command(self, cmd: Any) -> Any:
            return super().execute_command(cmd)

    class _FailingRedisModule:
        SyncRedisClient = _FailingRedisClient
        AsyncRedisClient = _FailingAsyncRedisClient

    class _FakeValkeyClientConfig:
        def __init__(self, **kwargs: Any) -> None:
            self.url = kwargs["url"]

    class _FailingValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.url = cfg.url
            self.closed = False

        def execute_command(self, *_parts: Any) -> Any:
            probe_clients.append(self)
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            raise TimeoutError("seed 2 timed out")

        def close(self) -> None:
            self.closed = True

    class _FailingAsyncValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.url = cfg.url
            self.closed = False

        async def execute_command(self, *_parts: Any) -> Any:
            probe_clients.append(self)
            if "26379" in self.url:
                raise ConnectionError("seed 1 down")
            raise TimeoutError("seed 2 timed out")

        async def close(self) -> None:
            self.closed = True

    class _FailingValkeyModule:
        ValkeyClientConfig = _FakeValkeyClientConfig
        SyncValkeyClient = _FailingValkeyClient
        AsyncValkeyClient = _FailingAsyncValkeyClient

    nodes: tuple[tuple[str, int], ...] = () if empty_nodes else (("127.0.0.1", 26379), ("127.0.0.1", 26380))
    if backend == "redis":
        monkeypatch.setattr(redis_sync_mod, "ensure_module", lambda *_a, **_kw: _FailingRedisModule)
        monkeypatch.setattr(redis_async_mod, "ensure_module", lambda *_a, **_kw: _FailingRedisModule)
        redis_cfg = RedisConfig(
            url="rediss://127.0.0.1:6379/0",
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=nodes,
        )
        driver: Any = AsyncRedisDriver(redis_cfg) if is_async else SyncRedisDriver(redis_cfg)
    else:
        monkeypatch.setattr(valkey_sync_mod, "ensure_module", lambda *_a, **_kw: _FailingValkeyModule)
        monkeypatch.setattr(valkey_async_mod, "ensure_module", lambda *_a, **_kw: _FailingValkeyModule)
        valkey_cfg = ValkeyConfig(
            url="valkeys://127.0.0.1:6379/0",
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=nodes,
        )
        driver = AsyncValkeyDriver(valkey_cfg) if is_async else SyncValkeyDriver(valkey_cfg)

    initial_client = driver._client
    with pytest.raises(ImproperConfigurationError, match="mymaster") as exc_info:
        if is_async:
            await driver.discover_sentinel_master()
        else:
            driver.discover_sentinel_master()

    if empty_nodes:
        assert exc_info.value.__cause__ is None
        assert not probe_clients
    else:
        assert isinstance(exc_info.value.__cause__, TimeoutError)
        assert "seed 2 timed out" in str(exc_info.value.__cause__)
        assert len(probe_clients) == 2
        assert all(probe.closed is True for probe in probe_clients)

    assert initial_client.closed is False
    assert driver._client is initial_client
    if is_async:
        await driver.close()
    else:
        driver.close()


@pytest.mark.parametrize(
    ("cfg_cls", "scheme"),
    [
        pytest.param(RedisConfig, "redis", id="redis"),
        pytest.param(ValkeyConfig, "valkey", id="valkey"),
    ],
)
@pytest.mark.parametrize(
    ("explicit_scope", "expected_scope"),
    [
        (None, AZURE_REDIS_SCOPE),
        ("https://custom.redis.example/.default", "https://custom.redis.example/.default"),
    ],
)
def test_redis_and_valkey_azure_entra_config_scope_redaction_and_conflict(
    cfg_cls: type[RedisConfig] | type[ValkeyConfig],
    scheme: str,
    explicit_scope: str | None,
    expected_scope: str,
) -> None:
    """Configs auto-scope AzureEntraTokenProvider to AZURE_REDIS_SCOPE, redact secrets, and reject dual IAM setups."""
    seen_scopes: list[str] = []

    def _fetch(scope: str) -> tuple[str, float]:
        seen_scopes.append(scope)
        return ("test-token-placeholder", 5000.0)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="cache-client",
        client_secret="test-client-secret-placeholder",
        scope=explicit_scope,
        fetcher=_fetch,
        clock=lambda: 1000.0,
    )
    cfg = cfg_cls(
        url=f"{scheme}://oid-123:test-url-secret-placeholder@cache.redis.azure.net:6380/0",
        username="oid-123",
        password="test-password-placeholder",
        token_provider=provider,
    )
    assert cfg.token_provider is not None
    assert cfg.token_provider.scope == expected_scope

    rendered = repr(cfg)
    assert "AzureEntraTokenProvider" in rendered
    assert "test-client-secret-placeholder" not in rendered
    assert "test-url-secret-placeholder" not in rendered
    assert "test-password-placeholder" not in rendered

    other_provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="other-cache-client",
        client_secret="test-client-secret-placeholder",
        fetcher=_fetch,
        clock=lambda: 1000.0,
    )
    other_cfg = cfg_cls(url=f"{scheme}://localhost:6379/0", username="oid-123", token_provider=other_provider)
    assert cfg.pool_key() != other_cfg.pool_key()

    token = cfg.token_provider.resolve_sync()
    assert token == "test-token-placeholder"
    assert seen_scopes == [expected_scope]
    assert "test-token-placeholder" not in repr(cfg)

    with pytest.raises(ImproperConfigurationError, match="Cannot combine GCP IAM authentication"):
        cfg_cls(
            url=f"{scheme}://localhost:6379/0",
            username="oid-123",
            enable_gcp_iam_auth=True,
            token_provider=provider,
        )

    with pytest.raises(ImproperConfigurationError, match="Cannot combine GCP IAM authentication"):
        cfg_cls(
            url=f"{scheme}://localhost:6379/0",
            username="oid-123",
            iam_auth_enabled=True,
            token_provider=provider,
        )

    with pytest.raises(ImproperConfigurationError, match="Cannot configure both token_provider and iam_token_provider"):
        cfg_cls(
            url=f"{scheme}://localhost:6379/0",
            username="oid-123",
            token_provider=provider,
            iam_token_provider=other_provider,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["redis", "valkey"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_redis_and_valkey_drivers_azure_entra_wire_auth_refresh_and_auth_retry(
    backend: str,
    mode: str,
) -> None:
    """Sync and Async Redis/Valkey drivers pass Entra bearer tokens on wire, refresh pre-expiry, and retry WRONGPASS."""
    now = [1000.0]
    seen_scopes: list[str] = []
    issued: list[str] = []
    revoked_tokens: set[str] = set()
    auth_headers: list[tuple[str, str]] = []
    store: dict[bytes, bytes] = {}
    stop_event = threading.Event()
    server_socks: list[socket.socket] = []

    def _sync_fetch(scope: str) -> dict[str, Any]:
        seen_scopes.append(scope)
        tok = f"test-token-placeholder-{len(issued) + 1}"
        issued.append(tok)
        return {"access_token": tok, "expires_in": 600}

    async def _async_fetch(scope: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return _sync_fetch(scope)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="cache-client",
        client_secret="test-client-secret-placeholder",
        refresh_margin_seconds=120.0,
        clock=lambda: now[0],
        fetcher=_async_fetch if mode == "async" else _sync_fetch,
    )

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.2)
    port = srv.getsockname()[1]

    def _handle_conn(conn: socket.socket) -> None:
        server_socks.append(conn)
        buf = bytearray()
        authed_token: str | None = None
        try:
            while not stop_event.is_set():
                try:
                    chunk = conn.recv(4096)
                except TimeoutError:
                    continue
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
                    if not isinstance(frame, list) or not frame:
                        continue
                    parts = [p.decode("utf-8", errors="replace") if isinstance(p, bytes) else str(p) for p in frame]
                    cmd = parts[0].upper()
                    if cmd == "HELLO":
                        if "AUTH" in parts:
                            idx = parts.index("AUTH")
                            auth_user = parts[idx + 1]
                            auth_pass = parts[idx + 2]
                            auth_headers.append((auth_user, auth_pass))
                            authed_token = auth_pass
                            if auth_pass in revoked_tokens:
                                conn.sendall(b"-WRONGPASS invalid or revoked Entra token\r\n")
                                continue
                        conn.sendall(f"%2\r\n+server\r\n+{backend}\r\n+proto\r\n:3\r\n".encode("ascii"))
                    elif cmd == "AUTH":
                        auth_user = parts[1] if len(parts) > 2 else "default"
                        auth_pass = parts[2] if len(parts) > 2 else parts[1]
                        auth_headers.append((auth_user, auth_pass))
                        authed_token = auth_pass
                        if auth_pass in revoked_tokens:
                            conn.sendall(b"-WRONGPASS invalid or revoked Entra token\r\n")
                        else:
                            conn.sendall(b"+OK\r\n")
                    elif authed_token in revoked_tokens:
                        conn.sendall(b"-WRONGPASS invalid or revoked Entra token\r\n")
                    elif cmd == "CLIENT":
                        conn.sendall(b"+OK\r\n")
                    elif cmd == "PING":
                        conn.sendall(b"+PONG\r\n")
                    elif cmd == "SET":
                        raw_k = frame[1] if isinstance(frame[1], bytes) else str(frame[1]).encode("utf-8")
                        raw_v = frame[2] if isinstance(frame[2], bytes) else str(frame[2]).encode("utf-8")
                        store[raw_k] = raw_v
                        conn.sendall(b"+OK\r\n")
                    elif cmd == "GET":
                        raw_k = frame[1] if isinstance(frame[1], bytes) else str(frame[1]).encode("utf-8")
                        val = store.get(raw_k)
                        if val is None:
                            conn.sendall(b"$-1\r\n")
                        else:
                            conn.sendall(f"${len(val)}\r\n".encode("ascii") + val + b"\r\n")
                    else:
                        conn.sendall(b"+OK\r\n")
        finally:
            conn.close()

    def _accept_loop() -> None:
        while not stop_event.is_set():
            try:
                conn, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            conn.settimeout(0.2)
            threading.Thread(target=_handle_conn, args=(conn,), daemon=True).start()

    accept_thread = threading.Thread(target=_accept_loop, daemon=True)
    accept_thread.start()

    try:
        if backend == "redis":
            cfg: Any = RedisConfig(
                url=f"redis://127.0.0.1:{port}/0",
                username="oid-123",
                token_provider=provider,
            )
            driver: Any = AsyncRedisDriver(cfg) if mode == "async" else SyncRedisDriver(cfg)
        else:
            cfg = ValkeyConfig(
                url=f"valkey://127.0.0.1:{port}/0",
                username="oid-123",
                token_provider=provider,
                enable_client_side_caching=False,
                enable_client_tracking=False,
            )
            driver = AsyncValkeyDriver(cfg) if mode == "async" else SyncValkeyDriver(cfg)

        try:
            assert driver.config.token_provider is not None
            assert driver.config.token_provider.scope == AZURE_REDIS_SCOPE

            if mode == "sync":
                driver.set("k1", "v1")
                assert driver.get("k1") == b"v1"
                assert issued == ["test-token-placeholder-1"]
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-1")

                now[0] = 1480.0
                assert driver.get("k1") == b"v1"
                assert len(issued) == 2
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-2")

                now[0] = 1970.0
                driver.reconnect()
                assert len(issued) == 3
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-3")

                revoked_tokens.add("test-token-placeholder-3")
                assert driver.get("k1") == b"v1"
                assert len(issued) == 4
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-4")
            else:
                await driver.set("k1", "v1")
                assert await driver.get("k1") == b"v1"
                assert issued == ["test-token-placeholder-1"]
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-1")

                now[0] = 1480.0
                assert await driver.get("k1") == b"v1"
                assert len(issued) == 2
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-2")

                now[0] = 1970.0
                await driver.reconnect()
                assert len(issued) == 3
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-3")

                revoked_tokens.add("test-token-placeholder-3")
                assert await driver.get("k1") == b"v1"
                assert len(issued) == 4
                assert auth_headers[-1] == ("oid-123", "test-token-placeholder-4")

            assert seen_scopes == [AZURE_REDIS_SCOPE] * 4
            for tok in issued:
                assert tok not in repr(driver.config)
            assert "test-client-secret-placeholder" not in repr(driver.config)
        finally:
            if mode == "async":
                await driver.close()
            else:
                driver.close()
    finally:
        stop_event.set()
        srv.close()
        for sock in server_socks:
            sock.close()
        accept_thread.join(timeout=2.0)


@pytest.mark.parametrize(
    ("backend", "is_async"),
    [
        pytest.param("redis", False, id="redis-sync"),
        pytest.param("redis", True, id="redis-async"),
        pytest.param("valkey", False, id="valkey-sync"),
        pytest.param("valkey", True, id="valkey-async"),
    ],
)
@pytest.mark.parametrize("use_tls", [False, True], ids=["plain", "tls"])
@pytest.mark.anyio
async def test_discover_sentinel_master_preserves_azure_entra_token_provider_and_refreshes(
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    is_async: bool,
    use_tls: bool,
) -> None:
    """discover_sentinel_master preserves Entra token_provider on config and passes refreshed bearer passwords."""
    now = [1000.0]
    issued: list[str] = []
    constructed_clients: list[Any] = []

    def _sync_fetch(scope: str) -> dict[str, Any]:
        assert scope == AZURE_REDIS_SCOPE
        tok = f"test-token-placeholder-{len(issued) + 1}"
        issued.append(tok)
        return {"access_token": tok, "expires_in": 600}

    async def _async_fetch(scope: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return _sync_fetch(scope)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="sentinel-client",
        client_secret="test-client-secret-placeholder",
        refresh_margin_seconds=120.0,
        clock=lambda: now[0],
        fetcher=_async_fetch if is_async else _sync_fetch,
    )

    class _FakeRedisClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.url = kwargs["url"]
            self.closed = False
            constructed_clients.append(self)

        def execute_command(self, cmd: Any) -> Any:
            assert cmd[:2] == ["SENTINEL", "get-master-addr-by-name"]
            return [b"10.0.0.41", b"6384"]

        def close(self) -> None:
            self.closed = True

    class _FakeAsyncRedisClient(_FakeRedisClient):
        async def execute_command(self, cmd: Any) -> Any:
            return super().execute_command(cmd)

    class _FakeRedisModule:
        SyncRedisClient = _FakeRedisClient
        AsyncRedisClient = _FakeAsyncRedisClient

    class _FakeValkeyClientConfig:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.url = kwargs["url"]

    class _FakeValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.cfg = cfg
            self.url = cfg.url
            self.closed = False
            constructed_clients.append(self)

        def execute_command(self, *_parts: Any) -> Any:
            return [b"10.0.0.42", b"6385"]

        def close(self) -> None:
            self.closed = True

    class _FakeAsyncValkeyClient:
        def __init__(self, cfg: Any) -> None:
            self.cfg = cfg
            self.url = cfg.url
            self.closed = False
            constructed_clients.append(self)

        async def execute_command(self, *_parts: Any) -> Any:
            return [b"10.0.0.42", b"6385"]

        async def close(self) -> None:
            self.closed = True

    class _FakeValkeyModule:
        ValkeyClientConfig = _FakeValkeyClientConfig
        SyncValkeyClient = _FakeValkeyClient
        AsyncValkeyClient = _FakeAsyncValkeyClient

    if backend == "redis":
        monkeypatch.setattr(redis_sync_mod, "ensure_module", lambda *_a, **_kw: _FakeRedisModule)
        monkeypatch.setattr(redis_async_mod, "ensure_module", lambda *_a, **_kw: _FakeRedisModule)
        scheme = "rediss" if use_tls else "redis"
        cfg: Any = RedisConfig(
            url=f"{scheme}://127.0.0.1:6379/2",
            username="oid-123",
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=(("127.0.0.1", 26379),),
            token_provider=provider,
        )
        driver: Any = AsyncRedisDriver(cfg) if is_async else SyncRedisDriver(cfg)
    else:
        monkeypatch.setattr(valkey_sync_mod, "ensure_module", lambda *_a, **_kw: _FakeValkeyModule)
        monkeypatch.setattr(valkey_async_mod, "ensure_module", lambda *_a, **_kw: _FakeValkeyModule)
        scheme = "valkeys" if use_tls else "valkey"
        cfg = ValkeyConfig(
            url=f"{scheme}://127.0.0.1:6379/2",
            username="oid-123",
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=(("127.0.0.1", 26379),),
            token_provider=provider,
        )
        driver = AsyncValkeyDriver(cfg) if is_async else SyncValkeyDriver(cfg)

    discovered = await driver.discover_sentinel_master() if is_async else driver.discover_sentinel_master()
    expected_addr = ("10.0.0.41", 6384) if backend == "redis" else ("10.0.0.42", 6385)
    assert discovered == expected_addr
    assert isinstance(driver.config.token_provider, AzureEntraTokenProvider)
    assert driver.config.token_provider.scope == AZURE_REDIS_SCOPE

    master_client = constructed_clients[-1]
    if backend == "redis":
        assert master_client.kwargs["username"] == "oid-123"
        assert master_client.kwargs["password"] == "test-token-placeholder-1"
        assert master_client.kwargs["enable_gcp_iam_auth"] is False
    else:
        assert master_client.cfg.kwargs["username"] == "oid-123"
        assert master_client.cfg.kwargs["password"] == "test-token-placeholder-1"
        assert master_client.cfg.kwargs["enable_gcp_memorystore_iam"] is False

    now[0] = 1480.0
    rediscovered = await driver.discover_sentinel_master() if is_async else driver.discover_sentinel_master()
    assert rediscovered == expected_addr
    refreshed_client = constructed_clients[-1]
    if backend == "redis":
        assert refreshed_client.kwargs["username"] == "oid-123"
        assert refreshed_client.kwargs["password"] == "test-token-placeholder-2"
    else:
        assert refreshed_client.cfg.kwargs["username"] == "oid-123"
        assert refreshed_client.cfg.kwargs["password"] == "test-token-placeholder-2"

    if is_async:
        await driver.close()
    else:
        driver.close()


class _ValkeyInMemoryRESPDriver:
    """In-memory RESP command and Lua test double for ValkeyQueueStore tests."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = collections.defaultdict(dict)
        self.zsets: dict[str, dict[str, float]] = collections.defaultdict(dict)
        self.kv: dict[str, tuple[str, float | None]] = {}
        self.seq: int = 0
        self.closed: bool = False

    def _get_kv(self, key: str) -> str | None:
        item = self.kv.get(key)
        if item is None:
            return None
        val, exp = item
        if exp is not None and exp <= time.time():
            del self.kv[key]
            return None
        return val

    def execute_command(self, *parts: Any) -> Any:
        """Execute a simulated synchronous RESP command."""
        cmd = str(parts[0]).upper()
        if cmd == "HGET":
            return self.hashes[str(parts[1])].get(str(parts[2]))
        if cmd == "HMGET":
            h = self.hashes[str(parts[1])]
            return [h.get(str(f)) for f in parts[2:]]
        if cmd == "GET":
            return self._get_kv(str(parts[1]))
        if cmd == "SET":
            key, val = str(parts[1]), str(parts[2])
            nx = "NX" in parts[3:]
            px: int | None = None
            for idx, token in enumerate(parts[3:], start=3):
                if str(token).upper() == "PX" and idx + 1 < len(parts):
                    px = int(parts[idx + 1])
            if nx and self._get_kv(key) is not None:
                return False
            self.kv[key] = (val, time.time() + (px / 1000.0) if px is not None else None)
            return True
        if cmd == "DEL":
            return 1 if self.kv.pop(str(parts[1]), None) is not None else 0
        if cmd == "ZRANGE":
            zset = self.zsets[str(parts[1])]
            ordered = sorted(zset.items(), key=lambda pair: (pair[1], pair[0]))
            if "WITHSCORES" in parts:
                out: list[Any] = []
                for member, score in ordered:
                    out.extend((member, score))
                return out
            return [member for member, _ in ordered]
        msg = f"Unsupported command {cmd}"
        raise NotImplementedError(msg)

    def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[Any] = (),
    ) -> Any:
        """Execute a simulated Lua queue-store script atomically."""
        if script is resp_store_mod._STORE_ENQUEUE_LUA:
            jobs_key, order_key, _seq_key, sched_key, exp_key, by_key_hash, ready_key = keys
            jid, queue, record_json, now_ms_s, sched_s, prio_s, exp_s, idem_key = (str(a) for a in args)
            self.seq += 1
            data = decode_json(record_json.encode("utf-8"))
            data["seq"] = self.seq
            self.hashes[jobs_key][jid] = encode_json(data)
            self.zsets[order_key][jid] = float(self.seq)
            if sched_s and int(sched_s) > int(now_ms_s):
                self.zsets[sched_key][jid] = float(sched_s)
            else:
                self.zsets[ready_key][jid] = float((-int(prio_s) * 1_000_000) + self.seq)
            if exp_s:
                self.zsets[exp_key][jid] = float(exp_s)
            if idem_key:
                self.hashes[by_key_hash][f"any:{idem_key}"] = jid
                self.hashes[by_key_hash][f"q:{queue}:{idem_key}"] = jid
            return jid

        if script is resp_store_mod._STORE_CLAIM_LUA:
            jobs_key, claimed_key, *ready_keys = keys
            limit, worker_id, lease_until_s, now_ms_val = (
                int(args[0]),
                str(args[1]),
                int(args[2]),
                int(args[3]),
            )
            claimed: list[str] = []
            for rkey in ready_keys:
                if len(claimed) >= limit:
                    break
                ordered = sorted(self.zsets[rkey].items(), key=lambda p: (p[1], p[0]))
                for jid, _ in ordered[: limit - len(claimed)]:
                    self.zsets[rkey].pop(jid, None)
                    raw = self.hashes[jobs_key].get(jid)
                    if not raw:
                        continue
                    data = decode_json(raw.encode("utf-8"))
                    if data.get("status") == "ready":
                        data["status"] = "claimed"
                        data["worker_id"] = worker_id
                        data["lease_until_ms"] = lease_until_s
                        data["updated_at_ms"] = now_ms_val
                        encoded = encode_json(data)
                        self.hashes[jobs_key][jid] = encoded
                        self.zsets[claimed_key][jid] = float(lease_until_s)
                        claimed.append(encoded)
            return claimed

        if script is resp_store_mod._STORE_ACK_LUA:
            jobs_key, claimed_key, terminal_key, exp_key = keys
            jid, attempt, now_ms = str(args[0]), int(args[1]), int(args[2])
            raw = self.hashes[jobs_key].get(jid)
            if not raw:
                return 0
            data = decode_json(raw.encode("utf-8"))
            if data.get("status") != "claimed" or int(data["attempt"]) != attempt:
                return 0
            comp_ms = data.get("completed_at_ms")
            if comp_ms is None:
                comp_ms = now_ms
            data["status"] = "completed"
            data["lease_until_ms"] = 0
            data["completed_at_ms"] = comp_ms
            data["updated_at_ms"] = now_ms
            self.hashes[jobs_key][jid] = encode_json(data)
            self.zsets[claimed_key].pop(jid, None)
            self.zsets[exp_key].pop(jid, None)
            self.zsets[terminal_key][jid] = float(comp_ms)
            return 1

        if script is resp_store_mod._STORE_NACK_LUA:
            jobs_key, claimed_key, sched_key, terminal_key, exp_key = keys
            jid, attempt, error_msg, delay_ms_s, now_ms, tag = (
                str(args[0]),
                int(args[1]),
                str(args[2]),
                str(args[3]),
                int(args[4]),
                str(args[5]),
            )
            raw = self.hashes[jobs_key].get(jid)
            if not raw:
                return 0
            data = decode_json(raw.encode("utf-8"))
            if data.get("status") != "claimed" or int(data["attempt"]) != attempt:
                return 0
            self.zsets[claimed_key].pop(jid, None)
            data["error"] = error_msg
            data["lease_until_ms"] = 0
            data["worker_id"] = None
            data["updated_at_ms"] = now_ms
            if int(data["attempt"]) >= int(data["max_attempts"]) or delay_ms_s == "":
                comp_ms = data.get("completed_at_ms") or now_ms
                data["status"] = "dead_letter"
                data["completed_at_ms"] = comp_ms
                self.zsets[exp_key].pop(jid, None)
                self.zsets[terminal_key][jid] = float(comp_ms)
            else:
                data["attempt"] = int(data["attempt"]) + 1
                data["status"] = "ready"
                delay_val = int(delay_ms_s)
                if delay_val > 0:
                    sched = now_ms + delay_val
                    data["scheduled_at_ms"] = sched
                    self.zsets[sched_key][jid] = float(sched)
                else:
                    data["scheduled_at_ms"] = None
                    prio = int(data.get("priority") or 0)
                    seq = int(data.get("seq") or 0)
                    self.zsets[f"{tag}:ready:{data['queue']}"][jid] = float((-prio * 1_000_000) + seq)
            self.hashes[jobs_key][jid] = encode_json(data)
            return 1

        if script is resp_store_mod._STORE_CLEANUP_TERMINAL_LUA:
            jobs_key, order_key, terminal_key, exp_key, claimed_key, by_key_hash = keys
            before_ms, limit = int(args[0]), int(args[1])
            eligible = [
                jid for jid, score in sorted(self.zsets[terminal_key].items(), key=lambda p: p[1]) if score < before_ms
            ]
            if limit >= 0:
                eligible = eligible[:limit]
            for jid in eligible:
                raw = self.hashes[jobs_key].pop(jid, None)
                if raw:
                    data = decode_json(raw.encode("utf-8"))
                    headers = data.get("headers") if isinstance(data.get("headers"), dict) else {}
                    for hk in (headers.get("key"), headers.get("idempotency_key")):
                        if hk:
                            self.hashes[by_key_hash].pop(f"any:{hk}", None)
                            self.hashes[by_key_hash].pop(f"q:{data['queue']}:{hk}", None)
                self.zsets[order_key].pop(jid, None)
                self.zsets[terminal_key].pop(jid, None)
                self.zsets[exp_key].pop(jid, None)
                self.zsets[claimed_key].pop(jid, None)
            return len(eligible)

        if script is resp_store_mod._STORE_RESERVE_IDENTITY_LUA:
            res_key = keys[0]
            key_name, jid, task_name, now_ms, ttl_ms = (
                str(args[0]),
                str(args[1]),
                str(args[2]),
                int(args[3]),
                int(args[4]),
            )
            existing = self._get_kv(res_key)
            if existing is not None:
                data = decode_json(existing.encode("utf-8"))
                exp = data.get("expires_at_ms")
                if exp is None or int(exp) > now_ms:
                    return existing
            expires_at_ms = now_ms + ttl_ms if ttl_ms >= 0 else None
            payload = encode_json(
                {
                    "key": key_name,
                    "job_id": jid,
                    "task_name": task_name,
                    "created_at_ms": now_ms,
                    "expires_at_ms": expires_at_ms,
                }
            )
            if ttl_ms > 0:
                self.kv[res_key] = (payload, time.time() + (ttl_ms / 1000.0))
            elif ttl_ms < 0:
                self.kv[res_key] = (payload, None)
            return None

        if script is resp_store_mod._STORE_RESET_IDENTITY_LUA:
            res_key = keys[0]
            expected_job_id = str(args[0])
            existing = self._get_kv(res_key)
            if existing is None:
                return 0
            if expected_job_id:
                data = decode_json(existing.encode("utf-8"))
                if data.get("job_id") != expected_job_id:
                    return 0
            self.kv.pop(res_key, None)
            return 1

        if script is resp_store_mod._STORE_EXPIRE_OVERDUE_LUA:
            jobs_key, exp_key, sched_key, terminal_key = keys
            now_ms, limit, tag = int(args[0]), int(args[1]), str(args[2])
            eligible = [
                jid for jid, score in sorted(self.zsets[exp_key].items(), key=lambda p: p[1]) if score <= now_ms
            ]
            if limit >= 0:
                eligible = eligible[:limit]
            expired = 0
            for jid in eligible:
                raw = self.hashes[jobs_key].get(jid)
                if not raw:
                    continue
                data = decode_json(raw.encode("utf-8"))
                if (
                    data.get("status") == "ready"
                    and data.get("expires_at_ms") is not None
                    and int(data["expires_at_ms"]) <= now_ms
                ):
                    comp_ms = data.get("completed_at_ms") or now_ms
                    data["status"] = "expired"
                    data["lease_until_ms"] = 0
                    data["worker_id"] = None
                    data["completed_at_ms"] = comp_ms
                    data["updated_at_ms"] = now_ms
                    self.hashes[jobs_key][jid] = encode_json(data)
                    self.zsets[f"{tag}:ready:{data['queue']}"].pop(jid, None)
                    self.zsets[sched_key].pop(jid, None)
                    self.zsets[exp_key].pop(jid, None)
                    self.zsets[terminal_key][jid] = float(comp_ms)
                    expired += 1
            return expired

        if script is resp_store_mod._STORE_ASSIGN_WORKER_LUA:
            jobs_key = keys[0]
            jid, worker_id, attempt, now_ms = str(args[0]), str(args[1]), int(args[2]), int(args[3])
            raw = self.hashes[jobs_key].get(jid)
            if not raw:
                return 0
            data = decode_json(raw.encode("utf-8"))
            if data.get("status") != "claimed" or int(data["attempt"]) != attempt:
                return 0
            data["worker_id"] = worker_id
            data["updated_at_ms"] = now_ms
            self.hashes[jobs_key][jid] = encode_json(data)
            return 1

        if script is resp_store_mod._STORE_TOUCH_HEARTBEATS_LUA:
            jobs_key = keys[0]
            hb_ms, now_ms = int(args[0]), int(args[1])
            touched = 0
            for jid in args[2:]:
                raw = self.hashes[jobs_key].get(str(jid))
                if not raw:
                    continue
                data = decode_json(raw.encode("utf-8"))
                if data.get("status") == "claimed":
                    data["heartbeat_at_ms"] = hb_ms
                    data["updated_at_ms"] = now_ms
                    self.hashes[jobs_key][str(jid)] = encode_json(data)
                    touched += 1
            return touched

        if script is resp_store_mod._STORE_NULL_HEARTBEATS_LUA:
            jobs_key = keys[0]
            now_ms = int(args[0])
            cleared = 0
            for jid in args[1:]:
                raw = self.hashes[jobs_key].get(str(jid))
                if not raw:
                    continue
                data = decode_json(raw.encode("utf-8"))
                if data.get("heartbeat_at_ms") is not None:
                    data["heartbeat_at_ms"] = None
                    data["updated_at_ms"] = now_ms
                    self.hashes[jobs_key][str(jid)] = encode_json(data)
                    cleared += 1
            return cleared

        if script is resp_store_mod._STORE_INTERRUPT_TASK_LUA:
            jobs_key, claimed_key, sched_key, terminal_key, exp_key = keys
            jid, has_err, err_msg, now_ms, tag = (
                str(args[0]),
                str(args[1]),
                str(args[2]),
                int(args[3]),
                str(args[4]),
            )
            raw = self.hashes[jobs_key].get(jid)
            if not raw:
                return 0
            data = decode_json(raw.encode("utf-8"))
            if data.get("status") not in ("ready", "claimed"):
                return 0
            comp_ms = data.get("completed_at_ms") or now_ms
            data["status"] = "interrupted"
            data["lease_until_ms"] = 0
            data["worker_id"] = None
            data["completed_at_ms"] = comp_ms
            data["updated_at_ms"] = now_ms
            if has_err == "1":
                data["error"] = err_msg
            self.hashes[jobs_key][jid] = encode_json(data)
            self.zsets[f"{tag}:ready:{data['queue']}"].pop(jid, None)
            self.zsets[claimed_key].pop(jid, None)
            self.zsets[sched_key].pop(jid, None)
            self.zsets[exp_key].pop(jid, None)
            self.zsets[terminal_key][jid] = float(comp_ms)
            return 1

        if script is resp_store_mod._STORE_ACQUIRE_LEASE_LUA:
            lease_key = keys[0]
            owner_id, ttl_ms = str(args[0]), int(args[1])
            cur = self._get_kv(lease_key)
            if cur is None or cur == owner_id:
                self.kv[lease_key] = (owner_id, time.time() + (ttl_ms / 1000.0))
                return 1
            return 0

        if script is resp_store_mod._STORE_RELEASE_LEASE_LUA:
            lease_key = keys[0]
            owner_id = str(args[0])
            if self._get_kv(lease_key) == owner_id:
                self.kv.pop(lease_key, None)
                return 1
            return 0

        msg = "Unknown Lua script"
        raise NotImplementedError(msg)

    def close(self) -> None:
        """Mark driver closed."""
        self.closed = True


class _ValkeyInMemoryAsyncRESPDriver:
    """Async adapter over _ValkeyInMemoryRESPDriver."""

    def __init__(self) -> None:
        self._sync = _ValkeyInMemoryRESPDriver()

    async def execute_command(self, *parts: Any) -> Any:
        """Execute a simulated async RESP command."""
        return self._sync.execute_command(*parts)

    async def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[Any] = (),
    ) -> Any:
        """Execute a simulated async Lua script."""
        return self._sync.eval_script(script, keys=keys, args=args)

    async def close(self) -> None:
        """Close the async driver."""
        self._sync.close()


@pytest.mark.parametrize(
    ("url", "expected_err"),
    [
        ("memory://", "does not support memory://"),
        ("redis://localhost:6379/0", "redis:// URLs are not supported by ValkeyQueueBackend"),
        ("rediss://localhost:6380/0", "redis:// URLs are not supported by ValkeyQueueBackend"),
        ("valkey:///0", "has no host"),
        ("unix:///", "has no socket path"),
    ],
)
def test_valkey_queue_backend_scheme_validation_and_missing_dep(
    url: str,
    expected_err: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify ValkeyQueueBackend delegates to RESPStreamQueueBackend and enforces scheme/missing-dep rules."""
    assert issubclass(ValkeyQueueBackend, resp_queue_mod.RESPStreamQueueBackend)
    with pytest.raises(ImproperConfigurationError, match=expected_err):
        ValkeyQueueBackend(QueueConfig(backend_url=url))

    monkeypatch.setitem(sys.modules, "vertebra._valkey", None)
    reset_dependency_cache("vertebra._valkey")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            ValkeyQueueBackend(QueueConfig(backend_url="valkey://127.0.0.1:6379/0"))
        assert exc_info.value.package == "vertebra-core[valkey]"
    finally:
        monkeypatch.delitem(sys.modules, "vertebra._valkey", raising=False)
        reset_dependency_cache("vertebra._valkey")


def test_sync_valkey_queue_store_full_lifecycle() -> None:
    """Verify SyncValkeyQueueStore exports and full SyncQueueStore lifecycle."""
    assert SyncValkeyQueueStore is DirectSyncValkeyQueueStore
    assert issubclass(SyncValkeyQueueStore, resp_store_mod.RESPQueueStoreBase)
    fake_driver = _ValkeyInMemoryRESPDriver()
    store = SyncValkeyQueueStore(cast("Any", fake_driver), key_prefix="test:valkey")
    assert isinstance(store, SyncQueueStore)
    store.ensure_schema()

    assert store.reserve_identity("idem-v1", job_id="vj-1", task_name="sync_job") is None
    contender = store.reserve_identity("idem-v1", job_id="vj-2", task_name="other")
    assert isinstance(contender, QueueReservation)
    assert contender.job_id == "vj-1"
    assert store.reset_identity("idem-v1", expected_job_id="wrong") is False

    store.enqueue_many(
        [
            QueueEntry(
                job_id="vj-done",
                queue="v-alpha",
                task_name="t1",
                payload=b"{}",
                completed_at_ms=100,
                headers={"key": "vk-done"},
            ),
            QueueEntry(
                job_id="vj-overdue",
                queue="v-beta",
                task_name="t2",
                payload=b"{}",
                expires_at_ms=500,
            ),
            QueueEntry(
                job_id="vj-active",
                queue="v-beta",
                task_name="t3",
                payload=b"{}",
            ),
        ]
    )
    assert [e.job_id for e in store.list_entries(queue=None)] == ["vj-done", "vj-overdue", "vj-active"]
    assert store.expire_overdue(now_ms=600) == 1
    assert store.claim(["v-alpha"], limit=1, worker_id="vw-0")[0].job_id == "vj-done"
    assert store.ack("vj-done", attempt=1) is True
    assert store.cleanup_terminal(before_ms=200) == 1
    assert store.get_entry("vj-done") is None
    assert store.get_reservation("idem-v1") == contender

    claimed = store.claim(["v-beta"], limit=1, worker_id="vw-1")
    assert len(claimed) == 1
    assert store.assign_worker("vj-active", worker_id="vw-2", attempt=1) is True
    assert store.touch_heartbeats(["vj-active"], heartbeat_at_ms=777) == 1
    assert store.null_heartbeats(["vj-active"]) == 1
    assert store.interrupt_task("vj-active", error="stop") is True
    assert store.reset_identity("idem-v1", expected_job_id="vj-1") is True
    assert store.acquire_lease("vcron", owner_id="vowner", ttl_seconds=10.0) is True
    assert store.release_lease("vcron", owner_id="vowner") is True
    store.close()
    assert fake_driver.closed is True


@pytest.mark.anyio
async def test_async_valkey_queue_store_full_lifecycle() -> None:
    """Verify AsyncValkeyQueueStore exports and full AsyncQueueStore lifecycle."""
    assert AsyncValkeyQueueStore is DirectAsyncValkeyQueueStore
    assert issubclass(AsyncValkeyQueueStore, resp_store_mod.RESPQueueStoreBase)
    fake_driver = _ValkeyInMemoryAsyncRESPDriver()
    store = AsyncValkeyQueueStore(cast("Any", fake_driver), key_prefix="test:avalkey")
    assert isinstance(store, AsyncQueueStore)
    await store.ensure_schema()

    assert await store.reserve_identity("idem-av1", job_id="avj-1", task_name="async_job") is None
    contender = await store.reserve_identity("idem-av1", job_id="avj-2", task_name="other")
    assert isinstance(contender, QueueReservation)
    assert contender.job_id == "avj-1"

    await store.enqueue(
        QueueEntry(
            job_id="avj-done",
            queue="av-alpha",
            task_name="t1",
            payload=b"{}",
            completed_at_ms=100,
        )
    )
    await store.enqueue(
        QueueEntry(
            job_id="avj-overdue",
            queue="av-beta",
            task_name="t2",
            payload=b"{}",
            expires_at_ms=500,
        )
    )
    assert [e.job_id for e in await store.list_entries(queue=None)] == ["avj-done", "avj-overdue"]
    assert await store.expire_overdue(now_ms=600) == 1
    claimed = await store.claim(["av-alpha"], limit=1, worker_id="vw-1")
    assert len(claimed) == 1
    assert await store.ack("avj-done", attempt=1) is True
    assert await store.cleanup_terminal(before_ms=200) == 1
    assert await store.get_entry("avj-done") is None
    assert await store.get_reservation("idem-av1") == contender
    assert await store.reset_identity("idem-av1", expected_job_id="avj-1") is True
    await store.aclose()
    assert fake_driver._sync.closed is True
