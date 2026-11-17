"""Unit tests for PostgreSQL COPY stall cancellation and Arrow IPC buffer boundaries."""

from __future__ import annotations

import asyncio
import struct
import threading
import time
from typing import Any

import pyarrow as pa
import pytest

from tests.unit.db.postgres_data_peers import (
    _INT4_OID,
    _TEXT_OID,
    _as_coro,
    _CopyPeerState,
    _expected_six_binary_tuples,
    _large_arrow_ipc_batch,
    _running_copy_peer,
    _single_row_arrow_ipc,
    _three_arrow_batches,
    _wrap_ipc_buffer,
)
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.driver import DataError, InterfaceError, OperationalError


@pytest.mark.parametrize(
    "blocked_step",
    ("write_arrow_ipc", "write_rows", "finish_copy_done", "finish_commit"),
)
def test_async_copy_from_unresponsive_cancel_retains_pool_slot_until_close(
    blocked_step: str,
) -> None:
    """Verify unresponsive CancelRequest during async CopyIn retires socket cleanly while retaining max_size=1 pool permit until close()."""
    first_ipc = _single_row_arrow_ipc()
    large_ipc = _large_arrow_ipc_batch() if blocked_step == "write_arrow_ipc" else b""
    large_rows = [(idx, "r" * 32768) for idx in range(384)] if blocked_step == "write_rows" else []
    stall_frames: int | None = None
    if blocked_step == "write_arrow_ipc":
        stall_frames = 2
    elif blocked_step == "write_rows":
        stall_frames = 0

    peer_state = _CopyPeerState(
        ignore_cancel_request=True,
        stall_copy_data_after_frames=stall_frames,
        hold_finish_until_released=(blocked_step == "finish_copy_done"),
        hold_commit_until_released=(blocked_step == "finish_commit"),
    )

    async def _run(port: int, state: _CopyPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.15,
            acquire_timeout=0.06,
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await pool.acquire()
            try:
                if blocked_step == "write_rows":
                    copy_in = await ac1.copy_from('COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)')
                    started_ev = asyncio.Event()
                    blocked_task: asyncio.Task[Any] = asyncio.create_task(
                        _as_coro(copy_in.write(large_rows, [_INT4_OID, _TEXT_OID]), started_ev),
                    )
                    await asyncio.wait_for(started_ev.wait(), timeout=2.0)
                    assert await asyncio.to_thread(state.copy_data_stalled.wait, 2.0)
                    await asyncio.sleep(0)
                    assert not blocked_task.done()
                elif blocked_step == "write_arrow_ipc":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    started_ev = asyncio.Event()
                    blocked_task = asyncio.create_task(
                        _as_coro(copy_in.write_arrow_ipc(large_ipc), started_ev),
                    )
                    await asyncio.wait_for(started_ev.wait(), timeout=2.0)
                    assert await asyncio.to_thread(state.copy_data_stalled.wait, 2.0)
                    await asyncio.sleep(0)
                    assert not blocked_task.done()
                elif blocked_step == "finish_copy_done":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    blocked_task = asyncio.create_task(_as_coro(copy_in.finish()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.finish_entered.wait, 2.0)
                else:
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    blocked_task = asyncio.create_task(_as_coro(copy_in.finish()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.commit_entered.wait, 2.0)

                blocked_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await blocked_task
                assert await asyncio.to_thread(state.cancel_received.wait, 1.0)
                await asyncio.sleep(0)

                with pytest.raises(InterfaceError):
                    await copy_in.finish()
                with pytest.raises(OperationalError):
                    await pool.acquire()
            finally:
                await ac1.close()

            ac2 = await pool.acquire()
            try:
                assert (await ac2.query("SELECT 1")).rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            state.signal_release_finish()
            await pool.close()

    with _running_copy_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))

    assert len(state.cancels) == 1
    assert state.aborted_copies == []
    assert state.startup_connections == 2


@pytest.mark.parametrize(
    ("blocked_step", "recovery"),
    [
        ("finish", "rollback_then_close"),
        ("finish", "close_directly"),
        ("write_arrow_ipc", "rollback_then_close"),
    ],
)
def test_async_copy_from_ambient_tx_unresponsive_cancel_marks_ambient_failed(
    blocked_step: str,
    recovery: str,
) -> None:
    """Verify cancelled CopyIn inside ambient begin() with ignored CancelRequest enforces ambient_cancel_failed until rollback/close."""
    first_ipc = _single_row_arrow_ipc()
    large_ipc = _large_arrow_ipc_batch() if blocked_step == "write_arrow_ipc" else b""
    peer_state = _CopyPeerState(
        ignore_cancel_request=True,
        stall_copy_data_after_frames=2 if blocked_step == "write_arrow_ipc" else None,
        hold_finish_until_released=(blocked_step == "finish"),
    )

    async def _run(port: int, state: _CopyPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.15,
            acquire_timeout=0.06,
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await pool.acquire()
            try:
                await ac1.begin()
                copy_in = await ac1.copy_from_arrow("items", first_ipc)
                if blocked_step == "finish":
                    task: asyncio.Task[Any] = asyncio.create_task(_as_coro(copy_in.finish()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.finish_entered.wait, 2.0)
                else:
                    started_ev = asyncio.Event()
                    task = asyncio.create_task(_as_coro(copy_in.write_arrow_ipc(large_ipc), started_ev))
                    await asyncio.wait_for(started_ev.wait(), timeout=2.0)
                    assert await asyncio.to_thread(state.copy_data_stalled.wait, 2.0)
                    await asyncio.sleep(0)
                    assert not task.done()

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                with pytest.raises(InterfaceError, match="ambient transaction failed"):
                    await ac1.commit()
                with pytest.raises(InterfaceError, match="ambient transaction failed"):
                    await ac1.query("SELECT 1")
                with pytest.raises(OperationalError):
                    await pool.acquire()

                if recovery == "rollback_then_close":
                    await ac1.rollback()
                    with pytest.raises(OperationalError):
                        await pool.acquire()
            finally:
                await ac1.close()

            ac2 = await pool.acquire()
            try:
                assert (await ac2.query("SELECT 1")).rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            state.signal_release_finish()
            await pool.close()

    with _running_copy_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))

    assert len(state.cancels) == 1
    assert state.aborted_copies == []
    assert "Q:COMMIT" not in state.events
    assert state.startup_connections == 2


@pytest.mark.parametrize(
    ("blocked_step", "ambient_tx"),
    [
        ("finish_copy_done", False),
        ("finish_copy_done", True),
        ("finish_commit", False),
        ("write_arrow_ipc", False),
    ],
)
def test_sync_copy_from_out_of_band_cancel_unresponsive_peer_bounds_and_retires(
    blocked_step: str,
    ambient_tx: bool,
) -> None:
    """Verify SyncPostgresClient.cancel() unblocks SyncPostgresCopyIn within connect_timeout when peer ignores CancelRequest."""
    first_ipc = _single_row_arrow_ipc()
    large_ipc = _large_arrow_ipc_batch() if blocked_step == "write_arrow_ipc" else b""
    peer_state = _CopyPeerState(
        ignore_cancel_request=True,
        stall_copy_data_after_frames=2 if blocked_step == "write_arrow_ipc" else None,
        hold_finish_until_released=(blocked_step == "finish_copy_done"),
        hold_commit_until_released=(blocked_step == "finish_commit"),
    )

    with _running_copy_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.15,
            acquire_timeout=0.06,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            canceller: threading.Thread | None = None
            try:
                if ambient_tx:
                    c1.begin()
                copy_in = c1.copy_from_arrow("items", first_ipc)

                def _cancel_when_blocked() -> None:
                    if blocked_step == "finish_copy_done":
                        assert state.finish_entered.wait(timeout=2.0)
                    elif blocked_step == "finish_commit":
                        assert state.commit_entered.wait(timeout=2.0)
                    else:
                        assert state.copy_data_stalled.wait(timeout=2.0)
                    c1.cancel()

                canceller = threading.Thread(target=_cancel_when_blocked, daemon=True)
                canceller.start()
                started = time.monotonic()
                with pytest.raises(OperationalError):
                    if blocked_step == "write_arrow_ipc":
                        copy_in.write_arrow_ipc(large_ipc)
                    else:
                        copy_in.finish()
                elapsed = time.monotonic() - started
                assert elapsed < 1.5
                canceller.join(timeout=2.0)

                with pytest.raises(OperationalError):
                    pool.acquire()

                if ambient_tx:
                    with pytest.raises(InterfaceError, match="ambient transaction failed"):
                        c1.commit()
                    with pytest.raises(InterfaceError, match="ambient transaction failed"):
                        c1.query("SELECT 1")
                    c1.rollback()
            finally:
                c1.close()
                if canceller is not None:
                    canceller.join(timeout=2.0)

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            state.signal_release_finish()
            pool.close()

    assert len(state.cancels) == 1
    assert state.aborted_copies == []
    assert state.startup_connections == 2


@pytest.mark.parametrize(
    "scenario",
    (
        "copy_in_aclose",
        "copy_in_aexit",
        "copy_out_aclose",
        "unawaited_bad_ipc_bytes",
        "unawaited_bad_ipc_type",
        "unawaited_bad_write_rows",
    ),
)
def test_async_copy_cancelled_aclose_and_unawaited_bad_write_sync_with_client_close(
    scenario: str,
) -> None:
    """Verify cancelled CopyIn/CopyOut aclose/__aexit__ and dropped unawaited bad write awaitables do not abort cleanup before client.close()."""
    first_ipc = _single_row_arrow_ipc()
    copy_out_rows: list[tuple[bytes | None, ...]] = [
        (struct.pack("!i", 1), b"a"),
        (struct.pack("!i", 2), b"b"),
    ]
    peer_state = _CopyPeerState(
        copy_out_rows=copy_out_rows,
        hold_copy_fail_until_cancel=scenario in ("copy_in_aclose", "copy_in_aexit"),
        wait_for_abort_sync=scenario in ("copy_in_aclose", "copy_in_aexit"),
        stall_copy_out_after_row1=(scenario == "copy_out_aclose"),
        hold_copy_out_cancel_reply=(scenario == "copy_out_aclose"),
    )

    async def _run(port: int, state: _CopyPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.35,
            acquire_timeout=0.5,
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await pool.acquire()
            try:
                if scenario == "copy_in_aclose":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    t_close = asyncio.create_task(_as_coro(copy_in.aclose()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.copy_fail_entered.wait, 2.0)
                    t_close.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_close
                    state.abort_sync.set()
                elif scenario == "copy_in_aexit":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)

                    async def _exit_scope() -> None:
                        async with copy_in:
                            pass

                    t_exit = asyncio.create_task(_exit_scope())
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.copy_fail_entered.wait, 2.0)
                    t_exit.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_exit
                    state.abort_sync.set()
                elif scenario == "copy_out_aclose":
                    copy_out = await ac1.copy_to("COPY items TO STDOUT (FORMAT binary)")
                    row1 = await anext(copy_out)
                    assert row1 == copy_out_rows[0]
                    t_out_close = asyncio.create_task(_as_coro(copy_out.aclose()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.cancel_received.wait, 2.0)
                    t_out_close.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_out_close
                    state.allow_cancel_reply.set()
                elif scenario == "unawaited_bad_ipc_bytes":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    unawaited_ipc = copy_in.write_arrow_ipc(b"bad")
                    del unawaited_ipc
                elif scenario == "unawaited_bad_ipc_type":
                    copy_in = await ac1.copy_from_arrow("items", first_ipc)
                    bad_arg: Any = 12345
                    unawaited_type = copy_in.write_arrow_ipc(bad_arg)
                    del unawaited_type
                else:
                    copy_in = await ac1.copy_from('COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)')
                    bad_rows: Any = "not_rows"
                    unawaited_rows = copy_in.write(bad_rows, [_INT4_OID, _TEXT_OID])
                    del unawaited_rows
            finally:
                await ac1.close()

            ac2 = await pool.acquire()
            try:
                assert (await ac2.query("SELECT 1")).rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await pool.close()

    with _running_copy_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))

    if scenario == "copy_out_aclose":
        assert len(state.cancels) >= 1
        assert state.startup_connections == 2
    else:
        assert len(state.aborted_copies) == 1
        assert ("Q:ROLLBACK" in state.events) or (state.startup_connections == 2)


@pytest.mark.parametrize("is_async", (False, True))
@pytest.mark.parametrize(
    "buf_kind",
    ("bytes", "bytearray", "writable_mv", "sliced_writable_mv", "readonly_mv", "writable_pa_buffer"),
)
def test_copy_from_arrow_writable_and_readonly_buffers_roundtrip(
    is_async: bool,
    buf_kind: str,
) -> None:
    """Verify copy_from_arrow and write_arrow_ipc safely extract bytes, bytearray, read-only, and writable buffer views."""
    schema, batches = _three_arrow_batches()
    sink1 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink1, schema) as w1:
        w1.write_batch(batches[0])
    sink2 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink2, schema) as w2:
        w2.write_batch(batches[1])
        w2.write_batch(batches[2])
    raw1 = bytes(sink1.getvalue())
    raw2 = bytes(sink2.getvalue())

    anchor1, buf1 = _wrap_ipc_buffer(raw1, buf_kind)
    anchor2, buf2 = _wrap_ipc_buffer(raw2, buf_kind)

    with _running_copy_peer() as (port, state):
        if not is_async:
            client = SyncPostgresClient(
                host="127.0.0.1",
                port=port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            client.connect()
            try:
                copy_in = client.copy_from_arrow("items", buf1)
                copy_in.write_arrow_ipc(buf2)
                assert copy_in.finish() == 6
            finally:
                client.close()
        else:

            async def _run_async() -> int:
                aclient = AsyncPostgresClient(
                    host="127.0.0.1",
                    port=port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                )
                await aclient.connect()
                try:
                    acopy = await aclient.copy_from_arrow("items", buf1)
                    await acopy.write_arrow_ipc(buf2)
                    return await acopy.finish()
                finally:
                    await aclient.close()

            assert asyncio.run(_run_async()) == 6

    _ = (anchor1, anchor2)
    assert len(state.completed_copies) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(state.completed_copies[0][1]) == _expected_six_binary_tuples()


@pytest.mark.parametrize(
    ("bad_kind", "expected_exc"),
    [
        ("oversized_bytearray", DataError),
        ("oversized_writable_mv", DataError),
        ("oversized_writable_pa_buffer", DataError),
        ("strided_writable_mv", BufferError),
    ],
)
def test_copy_from_arrow_writable_buffer_size_cap_and_strided_rejection(
    bad_kind: str,
    expected_exc: type[BaseException],
) -> None:
    """Verify >16 MiB bytearray/writable buffer views raise DataError and strided writable views raise BufferError with zero initial frames."""
    first_ipc = _single_row_arrow_ipc()
    oversized_len = 16 * 1024 * 1024 + 1
    if bad_kind == "oversized_bytearray":
        anchor: Any = bytearray(oversized_len)
        bad_buf: Any = anchor
    elif bad_kind == "oversized_writable_mv":
        anchor = bytearray(oversized_len)
        bad_buf = memoryview(anchor)
    elif bad_kind == "oversized_writable_pa_buffer":
        anchor = bytearray(oversized_len)
        bad_buf = pa.py_buffer(anchor)
    else:
        anchor = bytearray(b"abcdefgh")
        bad_buf = memoryview(anchor)[::2]

    with _running_copy_peer() as (port, state):
        client = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        try:
            with pytest.raises(expected_exc):
                client.copy_from_arrow("items", bad_buf)
            assert state.post_ready_frames == 0

            copy_in = client.copy_from_arrow("items", first_ipc)
            with pytest.raises(expected_exc):
                copy_in.write_arrow_ipc(bad_buf)
            assert client.query("SELECT 1").rows == ((1,),)
        finally:
            client.close()

    _ = anchor
    assert len(state.aborted_copies) == 1
