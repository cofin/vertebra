"""Unit and scripted wire-protocol tests for AsyncPostgresNotificationSource."""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import subprocess
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

from tests.unit.db.postgres_notification_harness import (
    _as_coro,
    _DrainerController,
    _pg_client_kwargs,
    _running_pg_server,
    _ServerSession,
    _wait_threading_event,
)
from vertebra._postgres import AsyncPostgresClient, AsyncPostgresListener, SyncPostgresPool
from vertebra.db import CDCConfig, ChangeFeed, InterfaceError
from vertebra.db.adapters.postgres.config import PoolerMode, PostgreSQLConfig
from vertebra.db.adapters.postgres.events import PostgresNotificationDriver
from vertebra.db.adapters.postgres.notify import (
    AsyncPostgresNotificationSource,
    PostgresNotifyConfig,
)
from vertebra.db.driver import AsyncNotificationSource, NotificationEvent
from vertebra.typing import ImproperConfigurationError


def _expected_evt(
    channel: str, payload: bytes, sender_id: str | int, *, reconnected: bool = False
) -> NotificationEvent:
    """Build an expected PostgreSQL NotificationEvent for at_most_once delivery assertions."""
    return NotificationEvent(
        channel=channel,
        payload=payload,
        sender_id=str(sender_id),
        metadata={"reconnected": reconnected, "delivery": "at_most_once"},
    )


class NativeNotificationDriverPort:
    """Typed notification driver port backed by real SyncPostgresPool and AsyncPostgresClient sessions."""

    __slots__ = ("_config", "_listener_clients", "_publish_calls", "_query_pool")

    def __init__(self, config: PostgreSQLConfig) -> None:
        """Initialize a native SyncPostgresPool for pg_notify and standalone listener dial parameters."""
        self._config = config
        parsed = urlparse(config.dsn)
        mode = "transaction" if (config.pgbouncer or config.pgbouncer_mode) else config.pooler_mode
        kwargs = _pg_client_kwargs(
            parsed.port or 5432,
            host=parsed.hostname or "127.0.0.1",
            database=parsed.path.lstrip("/") or "appdb",
        )
        self._query_pool = SyncPostgresPool(**kwargs, pooler_mode=mode, min_size=0, max_size=config.max_size)
        self._listener_clients: list[AsyncPostgresClient] = []
        self._publish_calls = 0

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig inspected by AsyncPostgresNotificationSource."""
        return self._config

    @property
    def publish_calls(self) -> int:
        """Return the total count of _publish_notification executions."""
        return self._publish_calls

    def select_one(self) -> int:
        """Execute SELECT 1 on the max_size-bounded query pool to verify non-starvation."""
        leased = self._query_pool.acquire()
        try:
            val = leased.query("SELECT 1", timeout=5.0).rows[0][0]
            assert isinstance(val, int)
            return val
        finally:
            leased.close()

    async def aclose(self) -> None:
        """Close all standalone listener clients and drain the synchronous query pool."""
        clients = list(self._listener_clients)
        self._listener_clients.clear()
        for client in clients:
            await client.close()
        await asyncio.to_thread(self._query_pool.close)

    async def _open_notification_listener(
        self,
        channels: Sequence[str],
        *,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> AsyncPostgresListener:
        """Derive direct connection coordinates and open a standalone AsyncPostgresListener."""
        parsed = urlparse(self._config.derive_direct_config().dsn)
        client = AsyncPostgresClient(
            **_pg_client_kwargs(
                parsed.port or 5432,
                host=parsed.hostname or "127.0.0.1",
                database=parsed.path.lstrip("/") or "appdb",
            )
        )
        self._listener_clients.append(client)
        return await client.open_listener(channels, queue_capacity=queue_capacity, reconnect=reconnect)

    async def _publish_notification(self, channel: str, payload: str) -> None:
        """Offload parameterized SELECT pg_notify($1, $2) execution onto SyncPostgresPool."""
        self._publish_calls += 1
        await asyncio.to_thread(self._publish_sync, channel, payload)

    def _publish_sync(self, channel: str, payload: str) -> None:
        """Acquire a transient SyncPostgresClient lease and emit parameterized pg_notify."""
        leased = self._query_pool.acquire()
        try:
            leased.query("SELECT pg_notify($1, $2)", (channel, payload), timeout=10.0)
        finally:
            leased.close()


def test_notification_source_end_to_end_publish_subscribe_reuse_and_reconnect() -> None:
    """Verify protocol-typed publish, dynamic subscribe/unsubscribe, max_size=1 non-starvation, reuse, and reconnect."""
    with _running_pg_server() as srv:

        async def _run() -> None:
            drainer_ctl = _DrainerController(asyncio.get_running_loop())
            cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb", max_size=1)
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with AsyncPostgresNotificationSource(
                    port,
                    PostgresNotifyConfig(channel="default_events"),
                    reconnect=True,
                ) as source:
                    assert isinstance(source, AsyncNotificationSource)
                    assert source.subscribed_channels == []
                    assert source.change_feed is None

                    received: list[NotificationEvent] = []
                    stream_opened = asyncio.Event()
                    got_event = asyncio.Event()
                    stop_after = 3

                    async def _consume_three() -> None:
                        stream = source.listen()
                        stream_opened.set()
                        async for evt in stream:
                            received.append(evt)
                            got_event.set()
                            if len(received) >= stop_after:
                                break

                    consumer = asyncio.create_task(_consume_three())
                    await stream_opened.wait()
                    assert await srv.wait_for_accept_count_async(1, timeout=3.0)
                    listener_sess = srv.session(0)
                    assert listener_sess.queries == []

                    assert await asyncio.to_thread(concrete_port.select_one) == 1
                    publisher_pid = srv.session(1).pid

                    await source.publish("default_events", "unsubscribed-default")
                    await source.publish("orders", "unsubscribed-orders")

                    await source.subscribe(["orders", "orders", "audit"])
                    assert source.subscribed_channels == ["orders", "audit"]
                    assert listener_sess.queries == ['LISTEN "orders"', 'LISTEN "audit"']

                    got_event.clear()
                    await source.publish("orders", "hello-utf8")
                    await asyncio.wait_for(got_event.wait(), timeout=3.0)
                    assert received[-1] == _expected_evt("orders", b"hello-utf8", publisher_pid)

                    got_event.clear()
                    await source.publish("audit", b"\x00\xff\xfe\x80")
                    await asyncio.wait_for(got_event.wait(), timeout=3.0)
                    assert received[-1] == _expected_evt("audit", b"\x00\xff\xfe\x80", publisher_pid)

                    await source.unsubscribe(["orders", "never_seen"])
                    assert source.subscribed_channels == ["audit"]
                    assert listener_sess.queries[-1] == 'UNLISTEN "orders"'

                    await source.publish("orders", "dropped-after-unlisten")
                    await source.publish("audit", "audit-after-unlisten")
                    await asyncio.wait_for(consumer, timeout=3.0)
                    assert len(received) == 3
                    assert received[-1] == _expected_evt("audit", b"audit-after-unlisten", publisher_pid)
                    assert concrete_port.publish_calls == 6
                    assert srv.wait_for_live_connections(1, timeout=2.0)

                    reuse_events: list[NotificationEvent] = []
                    reconnect_ready = asyncio.Event()

                    async def _consume_reused_with_reconnect() -> None:
                        reconnect_ready.set()
                        async for evt in source.listen():
                            reuse_events.append(evt)
                            if len(reuse_events) >= 2:
                                raise RuntimeError("consumer-body-abort")

                    reuse_task = asyncio.create_task(_consume_reused_with_reconnect())
                    await reconnect_ready.wait()
                    assert await srv.wait_for_accept_count_async(3, timeout=3.0)
                    second_listener = srv.session(2)
                    assert await second_listener.wait_for_query_count_async(1, timeout=3.0)
                    assert second_listener.queries == ['LISTEN "audit"']

                    second_listener.close_from_peer()
                    assert await srv.wait_for_accept_count_async(4, timeout=3.0)
                    third_listener = srv.session(3)
                    assert await third_listener.wait_for_query_count_async(1, timeout=3.0)
                    assert third_listener.queries == ['LISTEN "audit"']

                    await source.publish("audit", "gap-recovered")
                    await source.publish("audit", "post-gap-steady")
                    with pytest.raises(RuntimeError, match="consumer-body-abort"):
                        await asyncio.wait_for(reuse_task, timeout=3.0)
                    assert reuse_events == [
                        NotificationEvent(
                            channel="audit",
                            payload=b"gap-recovered",
                            sender_id=str(publisher_pid),
                            metadata={"reconnected": True, "delivery": "at_most_once"},
                        ),
                        NotificationEvent(
                            channel="audit",
                            payload=b"post-gap-steady",
                            sender_id=str(publisher_pid),
                            metadata={"reconnected": False, "delivery": "at_most_once"},
                        ),
                    ]
                    assert srv.wait_for_live_connections(1, timeout=2.0)

                    async def _consume_until_cancelled() -> None:
                        async for _ in source.listen():
                            pass

                    await source.aclose()
                    drainer_ctl.pause()
                    cancel_task = asyncio.create_task(_consume_until_cancelled())
                    try:
                        assert await srv.wait_for_accept_count_async(5, timeout=3.0)
                        assert await drainer_ctl.wait_until_signaled(timeout=2.0)
                        cancel_task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await cancel_task
                    finally:
                        drainer_ctl.resume()
                    assert await asyncio.to_thread(srv.wait_for_live_connections, 1, timeout=2.0)

                    drained_on_aclose: list[NotificationEvent] = []
                    got_before_aclose = asyncio.Event()

                    async def _consume_until_source_aclose() -> None:
                        async for evt in source.listen():
                            drained_on_aclose.append(evt)
                            got_before_aclose.set()

                    aclose_waiter = asyncio.create_task(_consume_until_source_aclose())
                    assert await srv.wait_for_accept_count_async(6, timeout=3.0)
                    final_listener = srv.session(5)
                    assert await final_listener.wait_for_query_count_async(1, timeout=3.0)
                    await source.publish("audit", "before-aclose")
                    await asyncio.wait_for(got_before_aclose.wait(), timeout=2.0)
                    assert len(drained_on_aclose) == 1
                    await source.aclose()
                    await source.aclose()
                    await asyncio.wait_for(aclose_waiter, timeout=3.0)
                    assert srv.wait_for_live_connections(1, timeout=2.0)
            finally:
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_notification_source_change_feed_spill_hydration_and_missing_spill_error() -> None:
    """Verify inline and spilled ChangeFeed hydration plus direct propagation of evicted spill KeyError."""
    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb", max_size=1)
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            feed = ChangeFeed(
                CDCConfig(
                    channel="cdc_bus",
                    spill_threshold_bytes=48,
                    max_memory_spills=1,
                )
            )
            try:
                async with AsyncPostgresNotificationSource(port, change_feed=feed) as source:
                    assert source.change_feed is feed
                    await source.subscribe(["orders"])

                    hydrated: list[NotificationEvent] = []
                    ready = asyncio.Event()

                    async def _read_two() -> None:
                        ready.set()
                        async for evt in source.listen():
                            hydrated.append(evt)
                            if len(hydrated) >= 2:
                                break

                    reader = asyncio.create_task(_read_two())
                    await ready.wait()
                    assert await srv.wait_for_accept_count_async(1, timeout=3.0)
                    assert await srv.session(0).wait_for_query_count_async(1, timeout=3.0)

                    await source.publish("orders", b"small")
                    spilled_blob = b"\x00\xfe" + b"payload-" * 32
                    await source.publish("orders", spilled_blob)
                    await asyncio.wait_for(reader, timeout=3.0)

                    publisher_pid = str(srv.session(1).pid)
                    assert hydrated == [
                        _expected_evt("orders", b"small", publisher_pid),
                        _expected_evt("orders", spilled_blob, publisher_pid),
                    ]
                    assert feed.replay_since(0)[-1].spilled is True
                    assert srv.wait_for_live_connections(1, timeout=2.0)

                    evict_stream = source.listen()
                    next_task = asyncio.create_task(_as_coro(anext(evict_stream)))
                    assert await srv.wait_for_accept_count_async(3, timeout=3.0)
                    evict_listener = srv.session(2)
                    assert await evict_listener.wait_for_query_count_async(1, timeout=3.0)

                    stale_envelope, stale_evt = feed.publish_event(
                        "PUBLISH",
                        {"channel": "orders", "data_b64": "c3RhbGU=" + ("A" * 80)},
                    )
                    assert stale_evt.spilled is True
                    _, newer_evt = feed.publish_event(
                        "PUBLISH",
                        {"channel": "orders", "data_b64": "bmV3ZXI=" + ("B" * 80)},
                    )
                    assert newer_evt.spilled is True

                    evict_listener.push_notification(7777, "orders", stale_envelope.encode("utf-8"))
                    with pytest.raises(KeyError, match=str(stale_evt.event_id)):
                        await asyncio.wait_for(next_task, timeout=3.0)
                    assert srv.wait_for_live_connections(1, timeout=2.0)

                    recovery_task = asyncio.create_task(_as_coro(anext(source.listen())))
                    assert await srv.wait_for_accept_count_async(4, timeout=3.0)
                    assert await srv.session(3).wait_for_query_count_async(1, timeout=3.0)
                    await source.publish("orders", "recovered-after-evict")
                    recovered_evt = await asyncio.wait_for(recovery_task, timeout=3.0)
                    assert recovered_evt.payload == b"recovered-after-evict"
            finally:
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize(
    ("pooler_mode", "pgbouncer", "use_direct_dsn", "expect_ok"),
    (
        ("direct", False, False, True),
        ("session", False, False, True),
        ("transaction", False, False, False),
        ("statement", False, False, False),
        ("auto", False, False, False),
        ("direct", True, False, False),
        ("transaction", False, True, True),
        ("statement", True, True, True),
    ),
)
def test_notification_source_pooler_mode_and_direct_dsn_validation(
    pooler_mode: PoolerMode,
    pgbouncer: bool,
    use_direct_dsn: bool,
    expect_ok: bool,
) -> None:
    """Reject multiplexed pooler configs lacking direct_dsn before dialing while allowing derived direct_dsn."""
    with _running_pg_server() as srv:

        async def _run() -> None:
            direct_uri = f"postgresql://postgres@127.0.0.1:{srv.port}/appdb"
            cfg = PostgreSQLConfig(
                dsn=direct_uri,
                direct_dsn=direct_uri if use_direct_dsn else None,
                pooler_mode=pooler_mode,
                pgbouncer=pgbouncer,
                max_size=1,
            )
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with AsyncPostgresNotificationSource(port) as source:
                    await source.subscribe(["orders"])
                    if not expect_ok:
                        with pytest.raises(ImproperConfigurationError, match="LISTEN/NOTIFY"):
                            await anext(source.listen())
                        assert srv.accept_count == 0
                        return

                    waiter = asyncio.create_task(_as_coro(anext(source.listen())))
                    assert await srv.wait_for_accept_count_async(1, timeout=3.0)
                    assert await srv.session(0).wait_for_query_count_async(1, timeout=3.0)
                    await source.publish("orders", "pooled-ok")
                    evt = await asyncio.wait_for(waiter, timeout=3.0)
                    assert evt.channel == "orders"
                    assert evt.payload == b"pooled-ok"
            finally:
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize(
    "bad_channel",
    (
        "",
        "bad;drop",
        "bad.dot",
        "has space",
        "a" * 64,
        "\u00e9" * 32,
    ),
)
def test_notification_source_channel_bounds_are_atomic_without_io(bad_channel: str) -> None:
    """Reject malformed and >63-UTF-8-byte channels across config, publish, subscribe, and unsubscribe with zero I/O."""
    with pytest.raises(ValueError, match="channel"):
        PostgresNotifyConfig(channel=bad_channel)
    with pytest.raises(ValueError, match="channel"):
        AsyncPostgresNotificationSource.build_notify_sql(bad_channel, "payload")

    max_channel = "a" * 63
    assert len(max_channel.encode("utf-8")) == 63
    assert PostgresNotifyConfig(channel=max_channel).channel == max_channel

    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb", max_size=1)
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with AsyncPostgresNotificationSource(port) as source:
                    assert source.subscribed_channels == []

                    with pytest.raises(ValueError, match="channel"):
                        await source.publish(bad_channel, "payload")
                    assert concrete_port.publish_calls == 0
                    assert srv.accept_count == 0

                    with pytest.raises(ValueError, match="channel"):
                        await source.subscribe(["orders", bad_channel])
                    assert source.subscribed_channels == []
                    assert srv.accept_count == 0

                    await source.subscribe(["existing", max_channel])
                    assert source.subscribed_channels == ["existing", max_channel]

                    waiter = asyncio.create_task(_as_coro(anext(source.listen())))
                    assert await srv.wait_for_accept_count_async(1, timeout=3.0)
                    listener_sess = srv.session(0)
                    assert await listener_sess.wait_for_query_count_async(2, timeout=3.0)
                    baseline_queries = list(listener_sess.queries)

                    with pytest.raises(ValueError, match="channel"):
                        await source.subscribe(["orders", bad_channel])
                    assert source.subscribed_channels == ["existing", max_channel]
                    assert listener_sess.queries == baseline_queries

                    with pytest.raises(ValueError, match="channel"):
                        await source.unsubscribe(["existing", bad_channel])
                    assert source.subscribed_channels == ["existing", max_channel]
                    assert listener_sess.queries == baseline_queries

                    await source.publish(max_channel, "boundary-63")
                    evt = await asyncio.wait_for(waiter, timeout=3.0)
                    assert evt.channel == max_channel
                    assert evt.payload == b"boundary-63"
            finally:
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize(
    ("queue_capacity", "reconnect", "expected_exc"),
    (
        (True, False, TypeError),
        (False, False, TypeError),
        ("16", False, TypeError),
        (0, False, ValueError),
        (-5, False, ValueError),
        (16, 1, TypeError),
        (16, "yes", TypeError),
    ),
)
def test_notification_source_rejects_invalid_constructor_options(
    queue_capacity: Any,
    reconnect: Any,
    expected_exc: type[BaseException],
) -> None:
    """Validate non-bool positive queue_capacity and bool reconnect in AsyncPostgresNotificationSource.__init__."""
    with _running_pg_server() as srv:
        cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb")
        concrete_port = NativeNotificationDriverPort(cfg)
        port: PostgresNotificationDriver = concrete_port
        try:
            with pytest.raises(expected_exc, match="queue_capacity|reconnect"):
                AsyncPostgresNotificationSource(
                    port,
                    queue_capacity=queue_capacity,
                    reconnect=reconnect,
                )
            assert srv.accept_count == 0
        finally:
            asyncio.run(concrete_port.aclose())


def test_notification_source_concurrent_listen_guard_and_lazy_extension_imports() -> None:
    """Reject concurrent source.listen() with InterfaceError and keep events/notify free of eager _postgres imports."""
    src_py = str(Path(__file__).resolve().parents[3])
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.db.adapters.postgres.events; "
                "import vertebra.db.adapters.postgres.notify; "
                "assert 'vertebra._postgres' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": os.environ.get("PYTHONPATH", src_py), "PATH": os.environ.get("PATH", "")},
    )
    assert probe.returncode == 0, probe.stderr

    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb", max_size=1)
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with AsyncPostgresNotificationSource(port, queue_capacity=16) as source:
                    bad_source: Any = source
                    with pytest.raises(TypeError):
                        await bad_source.subscribe("orders")
                    with pytest.raises(TypeError):
                        await bad_source.unsubscribe(b"orders")

                    await source.subscribe(["orders"])
                    active_stream = source.listen()
                    first_waiter = asyncio.create_task(_as_coro(anext(active_stream)))
                    assert await srv.wait_for_accept_count_async(1, timeout=3.0)
                    assert await srv.session(0).wait_for_query_count_async(1, timeout=3.0)

                    with pytest.raises(InterfaceError, match="already active"):
                        await anext(source.listen())
                    assert srv.accept_count == 1
                    assert not first_waiter.done()

                    await source.publish("orders", "primary-stream-intact")
                    evt = await asyncio.wait_for(first_waiter, timeout=3.0)
                    assert evt.payload == b"primary-stream-intact"

                    with pytest.raises(InterfaceError, match="already active"):
                        await anext(source.listen())
            finally:
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_notification_source_startup_reconciliation_and_concurrent_subscriptions() -> None:
    """Converge subscribe/unsubscribe mutations, channel reorderings, and source.aclose() across startup and LISTEN."""
    gate_lock = threading.Lock()
    gated_startup_idx = 1
    entered_startup = threading.Event()
    release_startup = threading.Event()
    entered_listen_b = threading.Event()
    release_listen_b = threading.Event()
    entered_listen_c = threading.Event()
    release_listen_c = threading.Event()
    entered_listen_aclose = threading.Event()
    release_listen_aclose = threading.Event()

    def _startup_hook(idx: int, _conn: socket.socket) -> bytes | None:
        with gate_lock:
            should_gate = idx == gated_startup_idx
        if should_gate:
            entered_startup.set()
            release_startup.wait(timeout=3.0)
        return None

    def _query_hook(_sess: _ServerSession, sql: str) -> bool:
        if sql == 'LISTEN "b"' and not entered_listen_b.is_set():
            entered_listen_b.set()
            release_listen_b.wait(timeout=3.0)
        elif sql == 'LISTEN "c"' and not entered_listen_c.is_set():
            entered_listen_c.set()
            release_listen_c.wait(timeout=3.0)
        elif sql == 'LISTEN "closing_ch"' and not entered_listen_aclose.is_set():
            entered_listen_aclose.set()
            release_listen_aclose.wait(timeout=3.0)
        return False

    with _running_pg_server(startup_hook=_startup_hook, query_hook=_query_hook) as srv:

        async def _run() -> None:
            nonlocal gated_startup_idx
            cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb", max_size=1)
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with AsyncPostgresNotificationSource(port, queue_capacity=16) as source:
                    await source.subscribe(["a"])
                    stream = source.listen()
                    first_waiter = asyncio.create_task(_as_coro(anext(stream)))
                    try:
                        assert await _wait_threading_event(entered_startup, timeout=3.0)
                        await source.subscribe(["b"])
                        await source.unsubscribe(["a"])
                        assert source.subscribed_channels == ["b"]

                        release_startup.set()
                        assert await _wait_threading_event(entered_listen_b, timeout=3.0)

                        resub_a = asyncio.create_task(source.subscribe(["a"]))
                        await asyncio.sleep(0)

                        with pytest.raises(ValueError, match="channel"):
                            await source.subscribe(["bad;drop"])

                        release_listen_b.set()
                        await asyncio.wait_for(resub_a, timeout=3.0)

                        listener_sess = srv.session(0)
                        assert await listener_sess.wait_for_query_count_async(4, timeout=3.0)
                        assert listener_sess.queries == [
                            'LISTEN "a"',
                            'LISTEN "b"',
                            'UNLISTEN "a"',
                            'LISTEN "a"',
                        ]
                        assert source.subscribed_channels == ["b", "a"]
                        assert listener_sess.is_subscribed_to("a") is True
                        assert listener_sess.is_subscribed_to("b") is True

                        await source.publish("a", "resubscribed-a")
                        evt_a = await asyncio.wait_for(first_waiter, timeout=3.0)
                        assert evt_a.channel == "a"
                        assert evt_a.payload == b"resubscribed-a"

                        await source.publish("b", "subscribed-b")
                        evt_b = await asyncio.wait_for(_as_coro(anext(stream)), timeout=3.0)
                        assert evt_b.channel == "b"
                        assert evt_b.payload == b"subscribed-b"

                        sub_c = asyncio.create_task(source.subscribe(["c"]))
                        assert await _wait_threading_event(entered_listen_c, timeout=3.0)
                        unsub_c = asyncio.create_task(source.unsubscribe(["c"]))
                        await asyncio.sleep(0)
                        release_listen_c.set()
                        await asyncio.wait_for(asyncio.gather(sub_c, unsub_c), timeout=3.0)

                        assert listener_sess.queries[-2:] == ['LISTEN "c"', 'UNLISTEN "c"']
                        assert "c" not in source.subscribed_channels
                        assert listener_sess.is_subscribed_to("c") is False
                    finally:
                        release_startup.set()
                        release_listen_b.set()
                        release_listen_c.set()
                        if not first_waiter.done():
                            first_waiter.cancel()
                            with contextlib.suppress(BaseException):
                                await first_waiter

                async with AsyncPostgresNotificationSource(port, queue_capacity=16) as reorder_source:
                    await reorder_source.subscribe(["x", "y"])
                    entered_startup.clear()
                    release_startup.clear()
                    with gate_lock:
                        gated_startup_idx = srv.accept_count + 1
                    reorder_stream = reorder_source.listen()
                    reorder_waiter = asyncio.create_task(_as_coro(anext(reorder_stream)))
                    try:
                        assert await _wait_threading_event(entered_startup, timeout=3.0)
                        await reorder_source.unsubscribe(["x"])
                        await reorder_source.subscribe(["z", "x"])
                        assert reorder_source.subscribed_channels == ["y", "z", "x"]
                        release_startup.set()

                        reorder_sess = srv.session(gated_startup_idx - 1)
                        assert await reorder_sess.wait_for_query_count_async(3, timeout=3.0)
                        assert reorder_sess.queries == ['LISTEN "x"', 'LISTEN "y"', 'LISTEN "z"']
                        await reorder_source.publish("x", "reordered-x")
                        evt_x = await asyncio.wait_for(reorder_waiter, timeout=1.0)
                        assert evt_x.channel == "x"
                        assert evt_x.payload == b"reordered-x"

                        await reorder_source.publish("z", "added-z")
                        evt_z = await asyncio.wait_for(_as_coro(anext(reorder_stream)), timeout=1.0)
                        assert evt_z.channel == "z"
                        assert evt_z.payload == b"added-z"
                    finally:
                        release_startup.set()
                        if not reorder_waiter.done():
                            reorder_waiter.cancel()
                            with contextlib.suppress(BaseException):
                                await reorder_waiter

                async with AsyncPostgresNotificationSource(port, queue_capacity=16) as same_set_source:
                    await same_set_source.subscribe(["m", "n"])
                    entered_startup.clear()
                    release_startup.clear()
                    with gate_lock:
                        gated_startup_idx = srv.accept_count + 1
                    same_set_stream = same_set_source.listen()
                    same_set_waiter = asyncio.create_task(_as_coro(anext(same_set_stream)))
                    try:
                        assert await _wait_threading_event(entered_startup, timeout=3.0)
                        await same_set_source.unsubscribe(["m"])
                        await same_set_source.subscribe(["m"])
                        assert same_set_source.subscribed_channels == ["n", "m"]
                        release_startup.set()

                        same_set_sess = srv.session(gated_startup_idx - 1)
                        assert await same_set_sess.wait_for_query_count_async(2, timeout=3.0)
                        await same_set_source.publish("m", "same-set-m")
                        evt_m = await asyncio.wait_for(same_set_waiter, timeout=1.0)
                        assert evt_m.channel == "m"
                        assert evt_m.payload == b"same-set-m"
                        assert same_set_sess.queries == ['LISTEN "m"', 'LISTEN "n"']
                    finally:
                        release_startup.set()
                        if not same_set_waiter.done():
                            same_set_waiter.cancel()
                            with contextlib.suppress(BaseException):
                                await same_set_waiter

                closing_source = AsyncPostgresNotificationSource(port, queue_capacity=16)
                await closing_source.subscribe(["m"])
                entered_startup.clear()
                release_startup.clear()
                with gate_lock:
                    gated_startup_idx = srv.accept_count + 1
                sentinel = object()
                closing_stream = closing_source.listen()
                closing_waiter = asyncio.create_task(_as_coro(anext(closing_stream, sentinel)))
                try:
                    assert await _wait_threading_event(entered_startup, timeout=3.0)
                    await closing_source.subscribe(["closing_ch"])
                    release_startup.set()
                    assert await _wait_threading_event(entered_listen_aclose, timeout=3.0)

                    await asyncio.wait_for(closing_source.aclose(), timeout=1.0)
                    assert await asyncio.wait_for(closing_waiter, timeout=1.0) is sentinel
                finally:
                    release_startup.set()
                    release_listen_aclose.set()
                    await closing_source.aclose()
                    if not closing_waiter.done():
                        closing_waiter.cancel()
                        with contextlib.suppress(BaseException):
                            await closing_waiter
            finally:
                release_startup.set()
                release_listen_b.set()
                release_listen_c.set()
                release_listen_aclose.set()
                await concrete_port.aclose()

        asyncio.run(_run())
        assert srv.wait_for_live_connections(0, timeout=2.0)
