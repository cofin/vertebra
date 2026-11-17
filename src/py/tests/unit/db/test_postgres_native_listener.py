"""Unit and scripted wire-protocol tests for AsyncPostgresListener."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import socket
import threading
import time
from collections.abc import Awaitable
from typing import Any

import pytest

from tests.unit.db.postgres_notification_harness import (
    _as_coro,
    _DrainerController,
    _make_direct_client,
    _pg_client_kwargs,
    _pg_error,
    _pg_frame,
    _pg_notification,
    _running_oauth_server,
    _running_pg_server,
    _ScriptedNotificationServer,
    _ServerSession,
    _wait_threading_event,
)
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresListener,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db import ConnectionFailure, DatabaseError, DataError, InterfaceError, OperationalError


async def _exercise_active_listener(
    client: SyncPostgresClient | AsyncPostgresClient,
    srv: _ScriptedNotificationServer,
    *,
    startup_gate: tuple[threading.Event, threading.Event] | None,
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
            await asyncio.sleep(0)

    ticker_task = asyncio.create_task(_ticker())
    try:
        open_coro = client.open_listener(["orders", "orders"])
        if startup_gate is not None:
            entered_evt, release_evt = startup_gate
            open_task = asyncio.create_task(_as_coro(open_coro))
            assert await _wait_threading_event(entered_evt, timeout=3.0)
            while ticks < 2:
                await asyncio.sleep(0)
            release_evt.set()
            listener = await open_task
        else:
            listener = await open_coro
        async with listener:
            ticking = False
            await ticker_task
            if startup_gate is not None:
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
            await asyncio.sleep(0)
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

            primary_started = asyncio.Event()
            primary_wait = asyncio.create_task(_as_coro(listener.next(), primary_started))
            await asyncio.wait_for(primary_started.wait(), timeout=2.0)
            with pytest.raises(InterfaceError):
                await listener.next()
            assert not primary_wait.done()

            lsess.push_notification(7104, "audit", b"still-waiting")
            assert await primary_wait == (7104, "audit", b"still-waiting", False)

            closing_started = asyncio.Event()
            closing_wait = asyncio.create_task(_as_coro(listener.next(), closing_started))
            await asyncio.wait_for(closing_started.wait(), timeout=2.0)
            await listener.aclose()
            assert listener.closed is True
            assert await closing_wait is None

        assert listener.closed is True
    finally:
        ticking = False
        if startup_gate is not None:
            startup_gate[1].set()
        if not ticker_task.done():
            await ticker_task


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("pooler_mode", ("transaction", "statement", "auto"))
def test_native_listener_pooler_mode_rejected_before_dial(
    client_kind: str,
    pooler_mode: str,
) -> None:
    """Reject non-direct/session pooler modes with InterfaceError and zero standalone socket accepts."""
    with _running_pg_server() as srv:
        if client_kind == "sync":
            pool = SyncPostgresPool(**_pg_client_kwargs(srv.port), pooler_mode=pooler_mode, min_size=1, max_size=1)
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
                    **_pg_client_kwargs(srv.port), pooler_mode=pooler_mode, min_size=1, max_size=1
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
            raw_sync = SyncPostgresClient(**_pg_client_kwargs(srv.port))
            raw_sync.close()

            async def _closed_sync() -> None:
                with pytest.raises(InterfaceError):
                    await raw_sync.open_listener(["orders"])

            asyncio.run(_closed_sync())
            assert srv.accept_count == 0

            pool = SyncPostgresPool(**_pg_client_kwargs(srv.port), min_size=0, max_size=1)
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
                raw_async = AsyncPostgresClient(**_pg_client_kwargs(srv.port))
                await raw_async.close()
                with pytest.raises(InterfaceError):
                    await raw_async.open_listener(["orders"])
                assert srv.accept_count == 0

                apool = AsyncPostgresPool(**_pg_client_kwargs(srv.port), min_size=0, max_size=1)
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
    use_gate = client_kind == "sync" and pooler_mode == "direct"
    startup_gate = (threading.Event(), threading.Event()) if use_gate else None
    hold_audit_ready = threading.Event()
    entered_audit_listen = threading.Event()
    pool_released = threading.Event()

    def _on_query(sess: _ServerSession, sql: str) -> bool:
        if sql == 'LISTEN "audit"':
            sess.send_raw(_pg_notification(7102, "orders", b"during-listen"))
            entered_audit_listen.set()
            assert hold_audit_ready.wait(timeout=3.0)
            sess.send_raw(_pg_frame(b"C", b"LISTEN\x00") + _pg_frame(b"Z", b"I"))
            return True
        return False

    def _startup_hook(idx: int, _conn: socket.socket) -> bytes | None:
        if use_gate and idx == 1 and startup_gate is not None:
            startup_gate[1].set()
        return None

    with _running_pg_server(
        startup_gate=startup_gate,
        startup_hook=_startup_hook,
        query_hook=_on_query,
    ) as srv:
        if client_kind == "sync":
            pool = SyncPostgresPool(**_pg_client_kwargs(srv.port), pooler_mode=pooler_mode, min_size=1, max_size=1)
            sync_client = pool.acquire()
            if startup_gate is not None:
                startup_gate[0].clear()
                startup_gate[1].clear()
            _ = pool_released
            try:
                asyncio.run(
                    _exercise_active_listener(
                        sync_client,
                        srv,
                        startup_gate=startup_gate,
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
                    **_pg_client_kwargs(srv.port), pooler_mode=pooler_mode, min_size=1, max_size=1
                )
                aclient = await apool.acquire()
                try:
                    await _exercise_active_listener(
                        aclient,
                        srv,
                        startup_gate=None,
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
                client = AsyncPostgresClient(**_pg_client_kwargs(srv.port))
                listener = await client.open_listener(["orders"], reconnect=True)
                try:
                    idle_started = asyncio.Event()
                    idle_task = asyncio.create_task(_as_coro(listener.next(), idle_started))
                    await asyncio.wait_for(idle_started.wait(), timeout=2.0)
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
                    first_started = asyncio.Event()
                    first_next = asyncio.create_task(_as_coro(listener.next(), first_started))
                    await asyncio.wait_for(first_started.wait(), timeout=2.0)

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
                        second_started = asyncio.Event()
                        second_next = asyncio.create_task(_as_coro(listener.next(), second_started))
                        await asyncio.wait_for(second_started.wait(), timeout=2.0)
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
            client = SyncPostgresClient(**_pg_client_kwargs(srv.port))
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

            assert await asyncio.to_thread(srv.wait_for_live_connections, 0, timeout=2.0)
            deadline = time.monotonic() + 2.0
            while not listener.closed and time.monotonic() < deadline:
                await asyncio.sleep(0)
            assert listener.closed is True

            await listener.aclose()
            assert listener.closed is True
            await listener.aclose()
            assert listener.closed is True
            assert await listener.next() is None
            client.close()

    async def _run_clean_eof() -> None:
        with _running_pg_server() as srv:
            client = AsyncPostgresClient(**_pg_client_kwargs(srv.port))
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
            client = AsyncPostgresClient(**_pg_client_kwargs(srv.port))
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
            assert await asyncio.to_thread(srv.wait_for_live_connections, 0, timeout=2.0)
            deadline = time.monotonic() + 2.0
            while not listener.closed and time.monotonic() < deadline:
                await asyncio.sleep(0)
            assert listener.closed is True
            with pytest.raises(OperationalError, match="overflow"):
                await listener.next()
            await asyncio.sleep(0)
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
            client = AsyncPostgresClient(**_pg_client_kwargs(srv.port))
            listener = await client.open_listener(["orders"], reconnect=True)
            assert listener.closed is False
            srv.session(0).close_from_peer()
            with pytest.raises(DatabaseError) as exc_info:
                await listener.next()
            err_any: Any = exc_info.value
            assert err_any.sqlstate == "28P01"
            assert listener.closed is True
            assert await asyncio.to_thread(srv.wait_for_live_connections, 0, timeout=2.0)
            await asyncio.sleep(0)
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
        _running_oauth_server(expires_in=1, refresh_window_s=0.2) as oauth,
        _running_pg_server(require_cleartext_password=True, query_hook=_query_hook) as srv,
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
                    "refresh_skew_ms": "850",
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
                client = SyncPostgresClient(**_pg_client_kwargs(cancel_srv.port))
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
                assert await asyncio.to_thread(cancel_srv.wait_for_live_connections, 0, timeout=2.0)
                await asyncio.sleep(0)
                assert backoff_listener.closed is False

                backoff_started = asyncio.Event()
                backoff_wait = asyncio.create_task(_as_coro(backoff_listener.next(), backoff_started))
                await asyncio.wait_for(backoff_started.wait(), timeout=2.0)
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
