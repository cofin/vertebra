"""Live PostgreSQL integration tests for Python query and query_multi command deadlines (`ch17:t02`)."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import pytest

from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra._vertebra import InterfaceError, OperationalError, QueryCancelledError

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


def _client_kwargs(svc: PostgresService) -> dict[str, Any]:
    """Build native client connection kwargs from PostgresService primitives."""
    return {
        "host": svc.host,
        "port": svc.port,
        "user": svc.user,
        "password": svc.password,
        "database": svc.database,
        "sslmode": "disable",
        "channel_binding": "disable",
        "connect_timeout": 5.0,
    }


@pytest.mark.parametrize(
    ("bad_timeout", "expected_exc"),
    [
        (0, InterfaceError),
        (0.0, InterfaceError),
        (-1.0, InterfaceError),
        (float("nan"), InterfaceError),
        (float("inf"), InterfaceError),
        (float("-inf"), InterfaceError),
        (True, TypeError),
        (False, TypeError),
    ],
)
def test_invalid_timeout_rejected_before_io_sync_and_async(
    postgres_service: PostgresService,
    bad_timeout: Any,
    expected_exc: type[Exception],
) -> None:
    """Verify invalid timeout values fail synchronously before emission and keep sessions clean."""
    kwargs = _client_kwargs(postgres_service)
    client = SyncPostgresClient(**kwargs)
    client.connect()
    try:
        with pytest.raises(expected_exc):
            client.query("SELECT 1", timeout=bad_timeout)
        with pytest.raises(expected_exc):
            client.query_multi("SELECT 1", timeout=bad_timeout)
        res = client.query("SELECT $1::int4", [7])
        assert res.rows == ((7,),)
    finally:
        client.close()

    async def _run_async() -> None:
        aclient = AsyncPostgresClient(**kwargs)
        await aclient.connect()
        try:
            with pytest.raises(expected_exc):
                aclient.query("SELECT 1", timeout=bad_timeout)
            with pytest.raises(expected_exc):
                aclient.query_multi("SELECT 1", timeout=bad_timeout)
            ares = await aclient.query("SELECT 1", timeout=5.0)
            assert ares.rows == ((1,),)
        finally:
            await aclient.close()

    asyncio.run(_run_async())


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_live_client_deadline_cancels_pg_sleep_and_replaces_pooled_backend_pid(
    postgres_service: PostgresService,
    mode: str,
) -> None:
    """Verify timeout=0.1 raises OperationalError(sqlstate=None), cancels pg_sleep, and replaces PID."""
    kwargs = _client_kwargs(postgres_service)
    marker = f"vtb_dl_probe_{mode}"

    if mode == "sync":
        pool = SyncPostgresPool(min_size=0, max_size=1, **kwargs)
        try:
            c1 = pool.acquire()
            try:
                default_res = c1.query("SELECT $1::int4", [7])
                assert default_res.rows == ((7,),)
                timed_ok = c1.query("SELECT 1", timeout=5.0)
                assert timed_ok.rows == ((1,),)
                pid1 = c1.query("SELECT pg_backend_pid()").rows[0][0]
                c1.begin()
                c1.query("SELECT pg_advisory_xact_lock($1::int8)", [7391001])
                with pytest.raises(OperationalError) as exc_info:
                    c1.query(f"SELECT pg_sleep(5), '{marker}'", timeout=0.1)
                assert exc_info.value.sqlstate is None
                assert exc_info.value.vendor_code is None
            finally:
                c1.close()

            c2 = pool.acquire()
            try:
                pid2 = c2.query("SELECT pg_backend_pid()").rows[0][0]
                assert pid2 != pid1
                c2.begin()
                try:
                    c2.query("SET LOCAL lock_timeout = '2s'")
                    c2.query("SELECT pg_advisory_xact_lock($1::int8)", [7391001])
                finally:
                    c2.rollback()
                deadline = time.monotonic() + 2.0
                lingering = 1
                while time.monotonic() < deadline:
                    act = c2.query(
                        "SELECT count(*)::int4 FROM pg_stat_activity WHERE pid = $1::int4 AND state = 'active'",
                        [pid1],
                    )
                    cell = act.rows[0][0]
                    assert isinstance(cell, int)
                    lingering = cell
                    if lingering == 0:
                        break
                assert lingering == 0
            finally:
                c2.close()
        finally:
            pool.close()
        return

    async def _run_async() -> None:
        apool = AsyncPostgresPool(min_size=0, max_size=1, **kwargs)
        try:
            ac1 = await apool.acquire()
            try:
                adefault = await ac1.query("SELECT $1::int4", [7])
                assert adefault.rows == ((7,),)
                atimed = await ac1.query("SELECT 1", timeout=5.0)
                assert atimed.rows == ((1,),)
                apid1 = (await ac1.query("SELECT pg_backend_pid()")).rows[0][0]
                await ac1.begin()
                await ac1.query("SELECT pg_advisory_xact_lock($1::int8)", [7391002])
                with pytest.raises(OperationalError) as exc_info:
                    await ac1.query(f"SELECT pg_sleep(5), '{marker}'", timeout=0.1)
                assert exc_info.value.sqlstate is None
                assert exc_info.value.vendor_code is None
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                apid2 = (await ac2.query("SELECT pg_backend_pid()")).rows[0][0]
                assert apid2 != apid1
                await ac2.begin()
                try:
                    await ac2.query("SET LOCAL lock_timeout = '2s'")
                    await ac2.query("SELECT pg_advisory_xact_lock($1::int8)", [7391002])
                finally:
                    await ac2.rollback()
                deadline = time.monotonic() + 2.0
                lingering = 1
                while time.monotonic() < deadline:
                    act = await ac2.query(
                        "SELECT count(*)::int4 FROM pg_stat_activity WHERE pid = $1::int4 AND state = 'active'",
                        [apid1],
                    )
                    cell = act.rows[0][0]
                    assert isinstance(cell, int)
                    lingering = cell
                    if lingering == 0:
                        break
                    await asyncio.sleep(0)
                assert lingering == 0
            finally:
                await ac2.close()
        finally:
            await apool.close()

    asyncio.run(_run_async())


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_live_shared_query_multi_budget_and_statement_timeout_precedence(
    postgres_service: PostgresService,
    mode: str,
) -> None:
    """Verify query_multi shares one non-renewing budget and statement_timeout surfaces 57014."""
    kwargs = _client_kwargs(postgres_service)

    if mode == "sync":
        c_multi = SyncPostgresClient(**kwargs)
        c_multi.connect()
        try:
            with pytest.raises(OperationalError) as multi_exc:
                c_multi.query_multi("SELECT pg_sleep(0.08); SELECT pg_sleep(0.08)", timeout=0.10)
            assert multi_exc.value.sqlstate is None
            assert multi_exc.value.vendor_code is None
        finally:
            c_multi.close()

        c_stmt = SyncPostgresClient(**kwargs)
        c_stmt.connect()
        try:
            c_stmt.query("SET statement_timeout = '50ms'")
            with pytest.raises(QueryCancelledError) as stmt_exc:
                c_stmt.query("SELECT pg_sleep(5)", timeout=5.0)
            assert stmt_exc.value.sqlstate == "57014"
        finally:
            c_stmt.close()
        return

    async def _run_async() -> None:
        ac_multi = AsyncPostgresClient(**kwargs)
        await ac_multi.connect()
        try:
            with pytest.raises(OperationalError) as multi_exc:
                await ac_multi.query_multi("SELECT pg_sleep(0.08); SELECT pg_sleep(0.08)", timeout=0.10)
            assert multi_exc.value.sqlstate is None
            assert multi_exc.value.vendor_code is None
        finally:
            await ac_multi.close()

        apool = AsyncPostgresPool(min_size=0, max_size=1, **kwargs)
        try:
            ac_stmt = await apool.acquire()
            try:
                await ac_stmt.query("SET statement_timeout = '50ms'")
                with pytest.raises(QueryCancelledError) as stmt_exc:
                    await ac_stmt.query("SELECT pg_sleep(5)", timeout=5.0)
                assert stmt_exc.value.sqlstate == "57014"
                await ac_stmt.query("SET statement_timeout = 0")

                pid_before = (await ac_stmt.query("SELECT pg_backend_pid()")).rows[0][0]
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(ac_stmt.query("SELECT pg_sleep(5)", timeout=5.0), timeout=0.05)
            finally:
                await ac_stmt.close()

            ac_next = await apool.acquire()
            try:
                pid_after = (await ac_next.query("SELECT pg_backend_pid()")).rows[0][0]
                assert pid_after != pid_before
            finally:
                await ac_next.close()
        finally:
            await apool.close()

    asyncio.run(_run_async())
