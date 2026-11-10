"""Unit and scripted wire-protocol tests for PostgreSQL listeners and notification sources."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import select
import socket
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Iterator, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresListener,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db import (
    CDCConfig,
    ChangeFeed,
    ConnectionFailure,
    DatabaseError,
    DataError,
    InterfaceError,
    OperationalError,
)
from vertebra.db.adapters.postgres.config import PoolerMode, PostgreSQLConfig
from vertebra.db.adapters.postgres.events import PostgresNotificationDriver
from vertebra.db.adapters.postgres.notify import PostgresNotificationSource, PostgresNotifyConfig
from vertebra.db.driver import NotificationEvent, NotificationSource
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import encode_json_bytes


class NativeNotificationDriverPort:
    """Typed notification driver port backed by real SyncPostgresPool and AsyncPostgresClient sessions."""

    __slots__ = ("_config", "_listener_clients", "_publish_calls", "_query_pool")

    def __init__(self, config: PostgreSQLConfig) -> None:
        """Initialize a native SyncPostgresPool for pg_notify and standalone listener dial parameters."""
        self._config = config
        parsed = urlparse(config.dsn)
        self._query_pool = SyncPostgresPool(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 5432,
            user=parsed.username or "postgres",
            database=parsed.path.lstrip("/") or "appdb",
            sslmode="disable",
            channel_binding="disable",
            pooler_mode="transaction" if (config.pgbouncer or config.pgbouncer_mode) else config.pooler_mode,
            min_size=0,
            max_size=config.max_size,
        )
        self._listener_clients: list[AsyncPostgresClient] = []
        self._publish_calls = 0

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig inspected by PostgresNotificationSource."""
        return self._config

    @property
    def publish_calls(self) -> int:
        """Return the total count of _publish_notification executions."""
        return self._publish_calls

    def select_one(self) -> int:
        """Execute SELECT 1 on the max_size-bounded query pool to verify non-starvation."""
        leased = self._query_pool.acquire()
        try:
            res = leased.query("SELECT 1", timeout=5.0)
            val = res.rows[0][0]
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
        direct_cfg = self._config.derive_direct_config()
        parsed = urlparse(direct_cfg.dsn)
        client = AsyncPostgresClient(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 5432,
            user=parsed.username or "postgres",
            database=parsed.path.lstrip("/") or "appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        self._listener_clients.append(client)
        return await client.open_listener(
            channels,
            queue_capacity=queue_capacity,
            reconnect=reconnect,
        )

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


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("pooler_mode", ("transaction", "statement", "auto"))
def test_native_listener_pooler_mode_rejected_before_dial(
    client_kind: str,
    pooler_mode: str,
) -> None:
    """Reject non-direct/session pooler modes with InterfaceError and zero standalone socket accepts."""
    with _running_pg_server() as srv:
        if client_kind == "sync":
            pool = SyncPostgresPool(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                pooler_mode=pooler_mode,
                min_size=1,
                max_size=1,
            )
            client = pool.acquire()
            try:
                assert srv.accept_count == 1

                async def _attempt_sync() -> None:
                    with pytest.raises(InterfaceError, match=pooler_mode):
                        await client.open_listener(["orders"])

                asyncio.run(_attempt_sync())
                assert srv.accept_count == 1
            finally:
                client.close()
                pool.close()
        else:

            async def _attempt_async() -> None:
                apool = AsyncPostgresPool(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                    pooler_mode=pooler_mode,
                    min_size=1,
                    max_size=1,
                )
                aclient = await apool.acquire()
                try:
                    assert srv.accept_count == 1
                    with pytest.raises(InterfaceError, match=pooler_mode):
                        await aclient.open_listener(["orders"])
                    assert srv.accept_count == 1
                finally:
                    await aclient.close()
                    await apool.close()

            asyncio.run(_attempt_async())


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize(
    ("channels", "queue_capacity", "reconnect", "expected_exc"),
    (
        (["orders"], True, False, TypeError),
        (["orders"], False, False, TypeError),
        (["orders"], "16", False, TypeError),
        (["orders"], 0, False, InterfaceError),
        (["orders"], -4, False, InterfaceError),
        (["orders"], 2**80, False, InterfaceError),
        (["orders"], 16, 1, TypeError),
        ("orders", 16, False, TypeError),
        (b"orders", 16, False, TypeError),
        ({"orders": 1}, 16, False, TypeError),
        ([123], 16, False, TypeError),
        ([""], 16, False, DataError),
        (["a" * 64], 16, False, DataError),
        (["\u00e9" * 32], 16, False, DataError),
        (["orders", "bad;drop"], 16, False, DataError),
    ),
)
def test_native_listener_rejects_invalid_arguments_before_dial(
    client_kind: str,
    channels: Any,
    queue_capacity: Any,
    reconnect: Any,
    expected_exc: type[BaseException],
) -> None:
    """Validate queue_capacity, reconnect, and channel identifiers synchronously before dialing."""
    with _running_pg_server() as srv:
        client: Any = _make_direct_client(client_kind, srv.port)

        async def _run() -> None:
            with pytest.raises(expected_exc):
                await client.open_listener(
                    channels,
                    queue_capacity=queue_capacity,
                    reconnect=reconnect,
                )

        asyncio.run(_run())
        assert srv.accept_count == 0


@pytest.mark.parametrize("client_kind", ("sync", "async"))
def test_native_listener_requires_running_loop_closed_client_check_and_standalone_lifetime(
    client_kind: str,
) -> None:
    """Verify no-loop RuntimeError, closed-client InterfaceError, and standalone listener lifetime."""
    with _running_pg_server() as srv:
        no_loop_client = _make_direct_client(client_kind, srv.port)
        with pytest.raises(RuntimeError):
            no_loop_client.open_listener(["orders"])
        assert srv.accept_count == 0

        if client_kind == "sync":
            raw_sync = SyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            raw_sync.close()

            async def _closed_sync() -> None:
                with pytest.raises(InterfaceError):
                    await raw_sync.open_listener(["orders"])

            asyncio.run(_closed_sync())
            assert srv.accept_count == 0

            pool = SyncPostgresPool(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                min_size=0,
                max_size=1,
            )
            leased = pool.acquire()

            async def _standalone_sync() -> None:
                listener = await leased.open_listener(["orders"])
                assert isinstance(listener, AsyncPostgresListener)
                assert srv.accept_count == 2
                res = await asyncio.to_thread(leased.query, "SELECT 1")
                assert res.rows == ((1,),)
                await asyncio.to_thread(leased.close)
                await asyncio.to_thread(pool.close)
                assert listener.closed is False
                srv.session(-1).push_notification(6101, "orders", b"independent-sync")
                assert await listener.next() == (6101, "orders", b"independent-sync", False)
                await listener.aclose()
                assert listener.closed is True

            asyncio.run(_standalone_sync())
            assert srv.wait_for_live_connections(0, timeout=2.0)
        else:

            async def _closed_and_standalone_async() -> None:
                raw_async = AsyncPostgresClient(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                )
                await raw_async.close()
                with pytest.raises(InterfaceError):
                    await raw_async.open_listener(["orders"])
                assert srv.accept_count == 0

                apool = AsyncPostgresPool(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                    min_size=0,
                    max_size=1,
                )
                aleased = await apool.acquire()
                listener = await aleased.open_listener(["orders"])
                assert isinstance(listener, AsyncPostgresListener)
                assert srv.accept_count == 2
                qres = await aleased.query("SELECT 1")
                assert qres.rows == ((1,),)
                await aleased.close()
                await apool.close()
                assert listener.closed is False
                srv.session(-1).push_notification(6102, "orders", b"independent-async")
                assert await listener.next() == (6102, "orders", b"independent-async", False)
                await listener.aclose()
                assert listener.closed is True

            asyncio.run(_closed_and_standalone_async())
            assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("pooler_mode", ("direct", "session"))
def test_native_listener_delivery_subscribe_unsubscribe_duplicate_next_and_context(
    client_kind: str,
    pooler_mode: str,
) -> None:
    """Verify push delivery, ReadyForQuery subscribe/unsubscribe gating, quoted names, and single-waiter next()."""
    startup_delay = 0.04 if client_kind == "sync" and pooler_mode == "direct" else 0.0
    hold_audit_ready = threading.Event()
    entered_audit_listen = threading.Event()

    def _on_query(sess: _ServerSession, sql: str) -> bool:
        if sql == 'LISTEN "audit"':
            sess.send_raw(_pg_notification(7102, "orders", b"during-listen"))
            entered_audit_listen.set()
            assert hold_audit_ready.wait(timeout=3.0)
            sess.send_raw(_pg_frame(b"C", b"LISTEN\x00") + _pg_frame(b"Z", b"I"))
            return True
        return False

    with _running_pg_server(startup_delay_s=startup_delay, query_hook=_on_query) as srv:
        if client_kind == "sync":
            pool = SyncPostgresPool(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                pooler_mode=pooler_mode,
                min_size=1,
                max_size=1,
            )
            sync_client = pool.acquire()
            try:
                asyncio.run(
                    _exercise_active_listener(
                        sync_client,
                        srv,
                        expect_loop_ticks=startup_delay > 0.0,
                        entered_audit_listen=entered_audit_listen,
                        hold_audit_ready=hold_audit_ready,
                    )
                )
            finally:
                sync_client.close()
                pool.close()
        else:

            async def _run_async() -> None:
                apool = AsyncPostgresPool(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                    pooler_mode=pooler_mode,
                    min_size=1,
                    max_size=1,
                )
                aclient = await apool.acquire()
                try:
                    await _exercise_active_listener(
                        aclient,
                        srv,
                        expect_loop_ticks=False,
                        entered_audit_listen=entered_audit_listen,
                        hold_audit_ready=hold_audit_ready,
                    )
                finally:
                    await aclient.close()
                    await apool.close()

            asyncio.run(_run_async())

        assert srv.wait_for_live_connections(0, timeout=2.0)


@pytest.mark.parametrize(
    "cancel_mode",
    ("drainer_drop_first", "task_cancel_while_paused", "resume_after_next_created"),
)
def test_native_listener_cancelled_next_retains_frame_order_and_gap(cancel_mode: str) -> None:
    """Verify Tokio-wait and paused-drainer cancelled next() preserve exact f1, f2 order and reconnected_gap."""
    with _running_pg_server() as srv:
        loop = asyncio.new_event_loop()
        drainer_ctl = _DrainerController(loop)
        try:

            async def _run() -> None:
                client = AsyncPostgresClient(
                    host="127.0.0.1",
                    port=srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                )
                listener = await client.open_listener(["orders"], reconnect=True)
                try:
                    idle_task = asyncio.create_task(_as_coro(listener.next()))
                    await asyncio.sleep(0.02)
                    idle_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await idle_task

                    unawaited: Awaitable[tuple[int, str, bytes, bool] | None] | None = listener.next()
                    assert unawaited is not None
                    unawaited = None
                    gc.collect()

                    srv.session(0).push_notification(8100, "orders", b"after-idle-cancel")
                    assert await listener.next() == (8100, "orders", b"after-idle-cancel", False)

                    srv.session(0).close_from_peer()
                    assert await srv.wait_for_accept_count_async(2, timeout=3.0)
                    reconnected_sess = srv.session(1)
                    assert await reconnected_sess.wait_for_query_count_async(1, timeout=3.0)
                    assert reconnected_sess.queries == ['LISTEN "orders"']

                    drainer_ctl.pause()
                    first_next = asyncio.create_task(_as_coro(listener.next()))
                    await asyncio.sleep(0.02)

                    reconnected_sess.send_raw(
                        _pg_notification(8101, "orders", b"gap-f1") + _pg_notification(8102, "orders", b"steady-f2")
                    )
                    assert await drainer_ctl.wait_until_signaled(timeout=2.0)

                    first_next.cancel()
                    if cancel_mode == "drainer_drop_first":
                        drainer_ctl.drain_once()
                        drainer_ctl.resume()
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        assert listener.closed is False
                        f1 = await listener.next()
                        f2 = await listener.next()
                    elif cancel_mode == "task_cancel_while_paused":
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        drainer_ctl.resume()
                        await asyncio.sleep(0)
                        assert listener.closed is False
                        f1 = await listener.next()
                        f2 = await listener.next()
                    else:
                        with pytest.raises(asyncio.CancelledError):
                            await first_next
                        second_next = asyncio.create_task(_as_coro(listener.next()))
                        await asyncio.sleep(0.01)
                        drainer_ctl.resume()
                        f1 = await second_next
                        f2 = await listener.next()

                    assert listener.closed is False
                    assert f1 == (8101, "orders", b"gap-f1", True)
                    assert f2 == (8102, "orders", b"steady-f2", False)
                finally:
                    drainer_ctl.resume()
                    await listener.aclose()
                    await client.close()

            loop.run_until_complete(_run())
        finally:
            loop.close()

        assert srv.wait_for_live_connections(0, timeout=2.0)


def test_native_listener_closed_state() -> None:
    """Verify read-only closed snapshot across active, reconnect backoff, EOF, overflow, 28P01, and aclose()."""

    async def _run_active_backoff_and_aclose() -> None:
        allow_reconnect = threading.Event()
        entered_second_startup = threading.Event()

        def _startup_gate(idx: int, _conn: socket.socket) -> bytes | None:
            if idx == 2:
                entered_second_startup.set()
                allow_reconnect.wait(timeout=3.0)
            return None

        with _running_pg_server(startup_hook=_startup_gate) as srv:
            client = SyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            listener = await client.open_listener(["orders"], reconnect=True)
            listener_any: Any = listener
            with pytest.raises(AttributeError):
                listener_any.closed = False
            with pytest.raises(AttributeError):
                listener_any.extra_attr = 1

            baseline_queries = list(srv.session(0).queries)
            assert listener.closed is False
            assert listener.closed is False
            assert srv.session(0).queries == baseline_queries

            srv.session(0).close_from_peer()
            assert await _wait_threading_event(entered_second_startup, timeout=3.0)
            assert listener.closed is False
            allow_reconnect.set()

            reconnected = srv.session(1)
            assert await reconnected.wait_for_query_count_async(1, timeout=3.0)
            reconnected.push_notification(9101, "orders", b"recovered")
            assert await listener.next() == (9101, "orders", b"recovered", True)
            assert listener.closed is False

            close_task = asyncio.create_task(_as_coro(listener.aclose()))
            close_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await close_task

            deadline = time.monotonic() + 2.0
            while not listener.closed and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert listener.closed is True
            assert srv.wait_for_live_connections(0, timeout=2.0)

            await listener.aclose()
            assert listener.closed is True
            await listener.aclose()
            assert listener.closed is True
            assert await listener.next() is None
            client.close()

    async def _run_clean_eof() -> None:
        with _running_pg_server() as srv:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            listener = await client.open_listener(["orders"], reconnect=False)
            assert listener.closed is False
            srv.session(0).close_from_peer()
            with pytest.raises(ConnectionFailure):
                await listener.next()
            assert listener.closed is True
            assert srv.accept_count == 1
            await listener.aclose()
            assert listener.closed is True
            await client.close()

    async def _run_overflow_with_reconnect_true() -> None:
        with _running_pg_server() as srv:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            listener = await client.open_listener(
                ["orders"],
                queue_capacity=2,
                reconnect=True,
            )
            assert listener.closed is False
            srv.session(0).send_raw(
                _pg_notification(9201, "orders", b"one")
                + _pg_notification(9202, "orders", b"two")
                + _pg_notification(9203, "orders", b"three")
            )
            deadline = time.monotonic() + 2.0
            while not listener.closed and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert listener.closed is True
            with pytest.raises(OperationalError, match="overflow"):
                await listener.next()
            await asyncio.sleep(0.16)
            assert srv.accept_count == 1
            await listener.aclose()
            assert listener.closed is True
            await client.close()

    async def _run_auth_28p01_terminal() -> None:
        def _reject_second_auth(idx: int, _conn: socket.socket) -> bytes | None:
            if idx >= 2:
                return _pg_error("FATAL", "28P01", "password authentication failed")
            return None

        with _running_pg_server(startup_hook=_reject_second_auth) as srv:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            listener = await client.open_listener(["orders"], reconnect=True)
            assert listener.closed is False
            srv.session(0).close_from_peer()
            with pytest.raises(DatabaseError) as exc_info:
                await listener.next()
            err_any: Any = exc_info.value
            assert err_any.sqlstate == "28P01"
            assert listener.closed is True
            await asyncio.sleep(0.16)
            assert srv.accept_count == 2
            await listener.aclose()
            assert listener.closed is True
            await client.close()

    asyncio.run(_run_active_backoff_and_aclose())
    asyncio.run(_run_clean_eof())
    asyncio.run(_run_overflow_with_reconnect_true())
    asyncio.run(_run_auth_28p01_terminal())


def test_native_listener_reconnect_token_refresh_mid_subscribe_and_cancel_cleanup() -> None:
    """Verify OAuth2 token refresh across idle/mid-subscribe drops and zero leaked sockets on cancellation."""
    drop_audit_once = {"done": False}

    def _query_hook(sess: _ServerSession, sql: str) -> bool:
        if sql == 'LISTEN "audit"' and not drop_audit_once["done"]:
            drop_audit_once["done"] = True
            sess.close_from_peer()
            return True
        return False

    with (
        _running_oauth_server() as oauth,
        _running_pg_server(
            require_cleartext_password=True,
            query_hook=_query_hook,
        ) as srv,
    ):

        async def _run_token_refresh_and_mid_subscribe() -> None:
            client = AsyncPostgresClient(
                host="127.0.0.1",
                port=srv.port,
                user="iam-user@example.com",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                enable_iam_auth=True,
                iam_auth_options={
                    "client_id": "vtb-test-client",
                    "client_secret": "vtb-test-secret",
                    "refresh_token": "vtb-refresh-token",
                    "token_uri": f"http://127.0.0.1:{oauth.port}/token",
                    "refresh_skew_ms": "0",
                    "proxy_policy": "direct",
                },
            )
            listener = await client.open_listener(["orders", "stale"], reconnect=True)
            try:
                assert srv.session(0).password == "iam-token-v1"
                assert srv.session(0).queries == ['LISTEN "orders"', 'LISTEN "stale"']

                await listener.unsubscribe(["stale"])
                assert srv.session(0).queries[-1] == 'UNLISTEN "stale"'

                await oauth.wait_until_current_token_expired()
                srv.session(0).close_from_peer()
                assert await srv.wait_for_accept_count_async(2, timeout=3.0)
                sess2 = srv.session(1)
                assert await sess2.wait_for_query_count_async(1, timeout=3.0)
                assert sess2.password == "iam-token-v2"
                assert sess2.queries == ['LISTEN "orders"']

                sess2.send_raw(
                    _pg_notification(9501, "orders", b"after-idle-drop")
                    + _pg_notification(9502, "orders", b"steady-after-idle")
                )
                assert await listener.next() == (9501, "orders", b"after-idle-drop", True)
                assert await listener.next() == (9502, "orders", b"steady-after-idle", False)

                await oauth.wait_until_current_token_expired()
                await listener.subscribe(["audit"])
                assert listener.closed is False
                assert srv.accept_count == 3
                sess3 = srv.session(2)
                assert sess3.password == "iam-token-v3"
                assert sess3.queries == ['LISTEN "orders"', 'LISTEN "audit"']

                sess3.push_notification(9503, "audit", b"after-mid-sub-drop")
                assert await listener.next() == (9503, "audit", b"after-mid-sub-drop", True)
            finally:
                await listener.aclose()
                await client.close()

        asyncio.run(_run_token_refresh_and_mid_subscribe())
        assert oauth.issue_count == 3
        assert srv.wait_for_live_connections(0, timeout=2.0)

    with _running_pg_server() as cancel_srv:
        loop = asyncio.new_event_loop()
        drainer_ctl = _DrainerController(loop)
        try:

            async def _run_cancelled_open_and_backoff() -> None:
                client = SyncPostgresClient(
                    host="127.0.0.1",
                    port=cancel_srv.port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                )
                warm = await client.open_listener([])
                await warm.aclose()
                assert cancel_srv.wait_for_live_connections(0, timeout=2.0)

                drainer_ctl.pause()
                open_task = asyncio.create_task(_as_coro(client.open_listener(["orders"])))
                assert await cancel_srv.wait_for_accept_count_async(2, timeout=3.0)
                assert await drainer_ctl.wait_until_signaled(timeout=2.0)

                open_task.cancel()
                drainer_ctl.resume()
                with pytest.raises(asyncio.CancelledError):
                    await open_task
                assert cancel_srv.wait_for_live_connections(0, timeout=2.0)

                backoff_listener = await client.open_listener(["orders"], reconnect=True)
                cancel_srv.stop_accepting_and_drop_all()
                await asyncio.sleep(0.03)
                assert backoff_listener.closed is False

                backoff_wait = asyncio.create_task(_as_coro(backoff_listener.next()))
                await asyncio.sleep(0.02)
                backoff_wait.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await backoff_wait
                assert backoff_listener.closed is False

                started = time.monotonic()
                await backoff_listener.aclose()
                elapsed = time.monotonic() - started
                assert elapsed < 0.25
                assert backoff_listener.closed is True
                client.close()

            loop.run_until_complete(_run_cancelled_open_and_backoff())
        finally:
            loop.close()

        assert cancel_srv.wait_for_live_connections(0, timeout=2.0)


def test_notification_source_end_to_end_publish_subscribe_reuse_and_reconnect() -> None:
    """Verify protocol-typed publish, dynamic subscribe/unsubscribe, max_size=1 non-starvation, reuse, and reconnect."""
    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(
                dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb",
                max_size=1,
            )
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with PostgresNotificationSource(
                    port,
                    PostgresNotifyConfig(channel="default_events"),
                    reconnect=True,
                ) as source:
                    assert isinstance(source, NotificationSource)
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
                    assert received[-1] == NotificationEvent(
                        channel="orders",
                        payload=b"hello-utf8",
                        sender_id=str(publisher_pid),
                        metadata={"reconnected": False, "delivery": "at_most_once"},
                    )

                    got_event.clear()
                    await source.publish("audit", b"\x00\xff\xfe\x80")
                    await asyncio.wait_for(got_event.wait(), timeout=3.0)
                    assert received[-1] == NotificationEvent(
                        channel="audit",
                        payload=b"\x00\xff\xfe\x80",
                        sender_id=str(publisher_pid),
                        metadata={"reconnected": False, "delivery": "at_most_once"},
                    )

                    await source.unsubscribe(["orders", "never_seen"])
                    assert source.subscribed_channels == ["audit"]
                    assert listener_sess.queries[-1] == 'UNLISTEN "orders"'

                    await source.publish("orders", "dropped-after-unlisten")
                    await source.publish("audit", "audit-after-unlisten")
                    await asyncio.wait_for(consumer, timeout=3.0)
                    assert len(received) == 3
                    assert received[-1] == NotificationEvent(
                        channel="audit",
                        payload=b"audit-after-unlisten",
                        sender_id=str(publisher_pid),
                        metadata={"reconnected": False, "delivery": "at_most_once"},
                    )
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

                    cancel_task = asyncio.create_task(_consume_until_cancelled())
                    assert await srv.wait_for_accept_count_async(5, timeout=3.0)
                    cancel_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await cancel_task
                    assert srv.wait_for_live_connections(1, timeout=2.0)

                    drained_on_aclose: list[NotificationEvent] = []

                    async def _consume_until_source_aclose() -> None:
                        async for evt in source.listen():
                            drained_on_aclose.append(evt)

                    aclose_waiter = asyncio.create_task(_consume_until_source_aclose())
                    assert await srv.wait_for_accept_count_async(6, timeout=3.0)
                    final_listener = srv.session(5)
                    assert await final_listener.wait_for_query_count_async(1, timeout=3.0)
                    await source.publish("audit", "before-aclose")
                    deadline = time.monotonic() + 2.0
                    while not drained_on_aclose and time.monotonic() < deadline:
                        await asyncio.sleep(0.01)
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
            cfg = PostgreSQLConfig(
                dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb",
                max_size=1,
            )
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
                async with PostgresNotificationSource(port, change_feed=feed) as source:
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
                        NotificationEvent(
                            channel="orders",
                            payload=b"small",
                            sender_id=publisher_pid,
                            metadata={"reconnected": False, "delivery": "at_most_once"},
                        ),
                        NotificationEvent(
                            channel="orders",
                            payload=spilled_blob,
                            sender_id=publisher_pid,
                            metadata={"reconnected": False, "delivery": "at_most_once"},
                        ),
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
                async with PostgresNotificationSource(port) as source:
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
        PostgresNotificationSource.build_notify_sql(bad_channel, "payload")

    max_channel = "a" * 63
    assert len(max_channel.encode("utf-8")) == 63
    assert PostgresNotifyConfig(channel=max_channel).channel == max_channel

    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(
                dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb",
                max_size=1,
            )
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with PostgresNotificationSource(port) as source:
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
    """Validate non-bool positive queue_capacity and bool reconnect in PostgresNotificationSource.__init__."""
    with _running_pg_server() as srv:
        cfg = PostgreSQLConfig(dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb")
        concrete_port = NativeNotificationDriverPort(cfg)
        port: PostgresNotificationDriver = concrete_port
        try:
            with pytest.raises(expected_exc, match="queue_capacity|reconnect"):
                PostgresNotificationSource(
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
        env={"PYTHONPATH": src_py, "PATH": ""},
    )
    assert probe.returncode == 0, probe.stderr

    with _running_pg_server() as srv:

        async def _run() -> None:
            cfg = PostgreSQLConfig(
                dsn=f"postgresql://postgres@127.0.0.1:{srv.port}/appdb",
                max_size=1,
            )
            concrete_port = NativeNotificationDriverPort(cfg)
            port: PostgresNotificationDriver = concrete_port
            try:
                async with PostgresNotificationSource(port, queue_capacity=16) as source:
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


async def _exercise_active_listener(
    client: SyncPostgresClient | AsyncPostgresClient,
    srv: _ScriptedNotificationServer,
    *,
    expect_loop_ticks: bool,
    entered_audit_listen: threading.Event,
    hold_audit_ready: threading.Event,
) -> None:
    """Drive open_listener, delivery, subscribe/unsubscribe gating, duplicate next(), and aclose()."""
    ticks = 0
    ticking = True

    async def _ticker() -> None:
        nonlocal ticks
        while ticking:
            ticks += 1
            await asyncio.sleep(0.005)

    ticker_task = asyncio.create_task(_ticker())
    try:
        open_coro = client.open_listener(["orders", "orders"])
        async with await open_coro as listener:
            ticking = False
            await ticker_task
            if expect_loop_ticks:
                assert ticks >= 2
            assert isinstance(listener, AsyncPostgresListener)
            assert "AsyncPostgresListener" in repr(listener)
            assert listener.closed is False

            lsess = srv.session(-1)
            assert lsess.queries == ['LISTEN "orders"']

            lsess.push_notification(7101, "orders", b"order-created")
            assert await listener.next() == (7101, "orders", b"order-created", False)

            sub_task = asyncio.create_task(_as_coro(listener.subscribe(["audit", "audit"])))
            assert await _wait_threading_event(entered_audit_listen, timeout=3.0)
            await asyncio.sleep(0.02)
            assert not sub_task.done()
            hold_audit_ready.set()
            assert await sub_task is None

            lsess.push_notification(7103, "audit", b"audit-recorded")
            assert await listener.next() == (7102, "orders", b"during-listen", False)
            assert await listener.next() == (7103, "audit", b"audit-recorded", False)

            await listener.subscribe(["Order$Items"])
            assert lsess.queries[-1] == 'LISTEN "Order$Items"'

            bad_listener: Any = listener
            with pytest.raises(DataError):
                await listener.subscribe([""])
            with pytest.raises(DataError):
                await listener.subscribe(['odd"chan'])
            with pytest.raises(TypeError):
                await bad_listener.subscribe("orders")
            with pytest.raises(DataError):
                await listener.unsubscribe(["bad;drop"])
            with pytest.raises(TypeError):
                await bad_listener.unsubscribe(123)
            assert listener.closed is False

            await listener.unsubscribe(["orders", "Order$Items", "never_seen"])
            assert 'UNLISTEN "orders"' in lsess.queries
            assert 'UNLISTEN "Order$Items"' in lsess.queries

            primary_wait = asyncio.create_task(_as_coro(listener.next()))
            await asyncio.sleep(0.02)
            with pytest.raises(InterfaceError):
                await listener.next()
            assert not primary_wait.done()

            lsess.push_notification(7104, "audit", b"still-waiting")
            assert await primary_wait == (7104, "audit", b"still-waiting", False)

            closing_wait = asyncio.create_task(_as_coro(listener.next()))
            await asyncio.sleep(0.02)
            await listener.aclose()
            assert listener.closed is True
            assert await closing_wait is None

        assert listener.closed is True
    finally:
        ticking = False
        if not ticker_task.done():
            await ticker_task


def _make_direct_client(client_kind: str, port: int) -> SyncPostgresClient | AsyncPostgresClient:
    """Construct an unconnected direct-mode native PostgreSQL client targeting port."""
    if client_kind == "sync":
        return SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
    return AsyncPostgresClient(
        host="127.0.0.1",
        port=port,
        user="postgres",
        database="appdb",
        sslmode="disable",
        channel_binding="disable",
    )


async def _as_coro(awaitable: Awaitable[Any]) -> Any:
    """Wrap a native RustDbTaskAwaitable inside a Python coroutine for asyncio.create_task."""
    return await awaitable


async def _wait_threading_event(evt: threading.Event, *, timeout: float) -> bool:
    """Await a threading.Event without blocking the running asyncio event loop."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if evt.is_set():
            return True
        await asyncio.sleep(0.005)
    return evt.is_set()


def _pg_frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 backend wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _pg_notification(pid: int, channel: str, payload: bytes) -> bytes:
    """Encode a PostgreSQL NotificationResponse ('A') backend frame."""
    body = struct.pack("!i", pid) + channel.encode("utf-8") + b"\x00" + payload + b"\x00"
    return _pg_frame(b"A", body)


def _pg_error(severity: str, sqlstate: str, message: str) -> bytes:
    """Encode a PostgreSQL ErrorResponse ('E') backend frame."""
    body = (
        b"S"
        + severity.encode("ascii")
        + b"\x00V"
        + severity.encode("ascii")
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
            if type(callback).__name__ == "_DbCompletionDrainer" and self._fd is None:
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
        """Synchronously invoke the captured _DbCompletionDrainer callback once."""
        assert self._callback is not None
        self._callback(*self._args)

    async def wait_until_signaled(self, *, timeout: float) -> bool:
        """Poll readability of the paused completion eventfd without consuming it."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._fd is not None:
                readable, _, _ = select.select([self._fd], [], [], 0.0)
                if readable:
                    return True
            await asyncio.sleep(0.005)
        return False


class _ServerSession:
    """Single accepted backend TCP session on _ScriptedNotificationServer."""

    def __init__(self, conn: socket.socket, pid: int) -> None:
        """Initialize socket locks and recorded frontend command history."""
        self._conn = conn
        self.pid = pid
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._queries: list[str] = []
        self._prepared: dict[str, str] = {}
        self._subscribed: set[str] = set()
        self._password: str | None = None
        self._closed = False

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
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.queries) >= count:
                return True
            await asyncio.sleep(0.005)
        return len(self.queries) >= count

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
            elif upper == "DISCARD ALL":
                self._subscribed.clear()
                self._prepared = {k: v for k, v in self._prepared.items() if not k}
            elif upper == "DEALLOCATE ALL":
                self._prepared = {k: v for k, v in self._prepared.items() if not k}

    def _mark_closed(self) -> None:
        """Mark the session socket terminated."""
        with self._state_lock:
            self._closed = True
            self._subscribed.clear()


class _ScriptedNotificationServer:
    """Multi-connection scripted PostgreSQL Protocol 3.0 loopback server for listener tests."""

    def __init__(
        self,
        *,
        startup_delay_s: float = 0.0,
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
        self._startup_delay_s = startup_delay_s
        self._require_cleartext_password = require_cleartext_password
        self._startup_hook = startup_hook
        self._query_hook = query_hook
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._accept_count = 0
        self._sessions: list[_ServerSession] = []
        self._workers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    @property
    def accept_count(self) -> int:
        """Return total accepted TCP connections so far."""
        with self._lock:
            return self._accept_count

    @property
    def live_connections(self) -> int:
        """Return currently open backend session count."""
        with self._lock:
            sessions = list(self._sessions)
        return sum(0 if s.is_closed else 1 for s in sessions)

    def session(self, index: int) -> _ServerSession:
        """Return the accepted _ServerSession at index."""
        with self._lock:
            return self._sessions[index]

    async def wait_for_accept_count_async(self, count: int, *, timeout: float) -> bool:
        """Await accept_count >= count without blocking the asyncio loop."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.accept_count >= count:
                return True
            await asyncio.sleep(0.005)
        return self.accept_count >= count

    def wait_for_live_connections(self, expected: int, *, timeout: float) -> bool:
        """Wait until live_connections equals expected."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.live_connections == expected:
                return True
            time.sleep(0.005)
        return self.live_connections == expected

    def stop_accepting_and_drop_all(self) -> None:
        """Close the listening socket and drop all currently open sessions."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._sock.close()
        with self._lock:
            sessions = list(self._sessions)
        for sess in sessions:
            sess.close_from_peer()

    def stop(self) -> None:
        """Shut down listener, close active sockets, and join worker threads."""
        self.stop_accepting_and_drop_all()
        self._accept_thread.join(timeout=2.0)
        with self._lock:
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
            with self._lock:
                self._accept_count += 1
                idx = self._accept_count
                sess = _ServerSession(conn, 5_000 + idx)
                self._sessions.append(sess)
            worker = threading.Thread(
                target=self._serve_session,
                args=(idx, conn, sess),
                daemon=True,
            )
            with self._lock:
                self._workers.append(worker)
            worker.start()

    def _serve_session(self, idx: int, conn: socket.socket, sess: _ServerSession) -> None:
        """Handle PostgreSQL Protocol 3.0 startup, LISTEN/UNLISTEN, and extended SELECT 1."""
        try:
            self._read_startup(conn)
            if self._startup_delay_s > 0.0:
                time.sleep(self._startup_delay_s)
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
        with self._lock:
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
    """Loopback HTTP/1.1 OAuth2 token endpoint issuing short-lived sequential IAM tokens."""

    def __init__(self) -> None:
        """Bind an ephemeral loopback HTTP socket and start serving token requests."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.05)
        self.port: int = self._sock.getsockname()[1]
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
        """Sleep asynchronously until the 1-second TTL of the most recent token has elapsed."""
        with self._lock:
            issued_at = self._last_issued_monotonic
        if issued_at <= 0.0:
            return
        remaining = (issued_at + 1.05) - time.monotonic()
        if remaining > 0.0:
            await asyncio.sleep(remaining)

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
                    {
                        "access_token": f"iam-token-v{ver}",
                        "expires_in": 1,
                        "token_type": "Bearer",
                    }
                )
                resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Connection: close\r\n" + f"Content-Length: {len(body_bytes)}\r\n\r\n".encode("ascii") + body_bytes
                )
                conn.sendall(resp)


@contextlib.contextmanager
def _running_pg_server(
    *,
    startup_delay_s: float = 0.0,
    require_cleartext_password: bool = False,
    startup_hook: Callable[[int, socket.socket], bytes | None] | None = None,
    query_hook: Callable[[_ServerSession, str], bool] | None = None,
) -> Iterator[_ScriptedNotificationServer]:
    """Context manager yielding a running _ScriptedNotificationServer and ensuring shutdown."""
    srv = _ScriptedNotificationServer(
        startup_delay_s=startup_delay_s,
        require_cleartext_password=require_cleartext_password,
        startup_hook=startup_hook,
        query_hook=query_hook,
    )
    try:
        yield srv
    finally:
        srv.stop()


@contextlib.contextmanager
def _running_oauth_server() -> Iterator[_LoopbackOAuthServer]:
    """Context manager yielding a running _LoopbackOAuthServer and ensuring shutdown."""
    srv = _LoopbackOAuthServer()
    try:
        yield srv
    finally:
        srv.stop()
