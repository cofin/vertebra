"""Unit tests for native PostgreSQL COPY handles and Arrow COPY loading."""

from __future__ import annotations

import asyncio
import struct
import sys
import threading
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import vertebra._postgres as native_pg
from tests.unit.db.postgres_data_peers import (
    _INT4_OID,
    _PGCOPY_HEADER,
    _PGCOPY_TRAILER,
    _TEXT_OID,
    _as_coro,
    _AsyncBatchStream,
    _build_arrow_source,
    _CopyPeerState,
    _CStreamProvider,
    _expected_six_binary_tuples,
    _forbid_row_materialization,
    _running_copy_peer,
    _three_arrow_batches,
)
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db.adapters.postgres.arrow import load_arrow_async, load_arrow_sync
from vertebra.db.adapters.postgres.core import PGCOPY_BINARY_SIGNATURE, PostgreSQLCore
from vertebra.db.driver import DataError, InterfaceError, OperationalError
from vertebra.typing import MissingDependencyError, reset_dependency_cache

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


@pytest.mark.parametrize("shape", ("table", "record_batch", "reader", "c_stream", "arrow_cursor"))
def test_load_arrow_sync_all_five_shapes_zero_row_materialization(
    shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify load_arrow_sync streams all 5 input shapes with 1 PGCOPY header and zero row materialization."""
    source = _build_arrow_source(shape)
    with _forbid_row_materialization(monkeypatch), _running_copy_peer() as (port, state):
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
            copied = load_arrow_sync(client, "items", source)
            assert copied == 6
        finally:
            client.close()

    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, was_ambient = state.completed_copies[0]
    assert copy_sql == 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    assert was_ambient is True
    assert wire_bytes.startswith(_PGCOPY_HEADER)
    assert wire_bytes.endswith(_PGCOPY_TRAILER)
    assert wire_bytes.count(PGCOPY_BINARY_SIGNATURE) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == _expected_six_binary_tuples()
    assert "Q:BEGIN" in state.events
    assert "Q:COMMIT" in state.events


@pytest.mark.parametrize("shape", ("table", "record_batch", "reader", "c_stream", "arrow_cursor", "async_batches"))
def test_load_arrow_async_all_shapes_zero_row_materialization(
    shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify load_arrow_async streams all Arrow shapes with 1 PGCOPY header and zero row materialization."""
    source = _build_arrow_source(shape)

    async def _run(port: int) -> int:
        client = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        try:
            return await load_arrow_async(client, "items", source)
        finally:
            await client.close()

    with _forbid_row_materialization(monkeypatch), _running_copy_peer() as (port, state):
        copied = asyncio.run(_run(port))

    assert copied == 6
    if isinstance(source, _AsyncBatchStream):
        assert source.closed is True
    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, _ = state.completed_copies[0]
    assert copy_sql == 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    assert wire_bytes.count(PGCOPY_BINARY_SIGNATURE) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == _expected_six_binary_tuples()
    assert "Q:BEGIN" in state.events
    assert "Q:COMMIT" in state.events


@pytest.mark.parametrize("shape", ("reader", "c_stream"))
def test_load_arrow_second_batch_streaming_barrier_and_reader_close(shape: str) -> None:
    """Verify batch 1 arrives on TCP wire before batch 2 is pulled and readers close on completion."""
    schema, batches = _three_arrow_batches()

    def _make_barrier_source(wire_evt: threading.Event, closed_evt: threading.Event) -> object:
        def _gen() -> Iterator[pa.RecordBatch]:
            try:
                yield batches[0]
                assert wire_evt.wait(timeout=3.0), "batch 1 did not arrive over TCP before batch 2 pull"
                yield batches[1]
                yield batches[2]
            finally:
                closed_evt.set()

        if shape == "reader":
            return pa.RecordBatchReader.from_batches(schema, _gen())
        return _CStreamProvider(lambda: pa.RecordBatchReader.from_batches(schema, _gen()))

    sync_closed = threading.Event()
    with _running_copy_peer() as (port, sync_state):
        sync_src = _make_barrier_source(sync_state.first_tuple_on_wire, sync_closed)
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
            assert load_arrow_sync(client, "items", sync_src) == 6
        finally:
            client.close()
    assert sync_closed.is_set()
    assert PostgreSQLCore.decode_pgcopy_rows(sync_state.completed_copies[0][1]) == _expected_six_binary_tuples()

    async_closed = threading.Event()

    async def _run_async(port: int, astate: _CopyPeerState) -> int:
        async_src = _make_barrier_source(astate.first_tuple_on_wire, async_closed)
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
            return await load_arrow_async(aclient, "items", async_src)
        finally:
            await aclient.close()

    with _running_copy_peer() as (port, async_state):
        assert asyncio.run(_run_async(port, async_state)) == 6
    assert async_closed.is_set()
    assert PostgreSQLCore.decode_pgcopy_rows(async_state.completed_copies[0][1]) == _expected_six_binary_tuples()


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_empty_schema_columns_schema_name_and_ambient_transaction(is_async: bool) -> None:
    """Verify 0-row Table forwards columns + schema_name on dotted table and joins ambient transactions."""
    state = _CopyPeerState(
        catalog_rows=[
            ("analytics", "51001", "a", _INT4_OID, -1, 1),
            ("analytics", "51001", "b", _TEXT_OID, -1, 2),
            ("analytics", "51001", "ignored", _TEXT_OID, -1, 3),
        ]
    )
    empty_table = pa.table(
        {"b": pa.array([], type=pa.string()), "a": pa.array([], type=pa.int32())},
    )

    with _running_copy_peer(state) as (port, peer):
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
                client.begin()
                count = load_arrow_sync(
                    client,
                    "odd.table",
                    empty_table,
                    columns=["b", "a"],
                    schema_name="analytics",
                )
                assert count == 0
                assert peer.events.count("Q:COMMIT") == 0
                assert peer.events.count("Q:BEGIN") == 1
                client.commit()
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
                    await aclient.begin()
                    res = await load_arrow_async(
                        aclient,
                        "odd.table",
                        empty_table,
                        columns=["b", "a"],
                        schema_name="analytics",
                    )
                    assert peer.events.count("Q:COMMIT") == 0
                    assert peer.events.count("Q:BEGIN") == 1
                    await aclient.commit()
                    return res
                finally:
                    await aclient.close()

            assert asyncio.run(_run_async()) == 0

    assert state.catalog_binds == [("analytics", "odd.table")]
    assert len(state.completed_copies) == 1
    copy_sql, wire_bytes, _ = state.completed_copies[0]
    assert copy_sql == 'COPY "analytics"."odd.table" ("b", "a") FROM STDIN (FORMAT binary)'
    assert wire_bytes == _PGCOPY_HEADER + _PGCOPY_TRAILER
    assert PostgreSQLCore.decode_pgcopy_rows(wire_bytes) == []
    assert state.events.count("Q:BEGIN") == 1
    assert state.events.count("Q:COMMIT") == 1


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_missing_pyarrow_fails_before_socket_traffic(
    is_async: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify blocked pyarrow raises MissingDependencyError before any PostgreSQL wire traffic."""
    schema, batches = _three_arrow_batches()
    tbl = pa.Table.from_batches(batches[:1], schema=schema)

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
                with state.lock:
                    baseline = state.post_ready_frames
                monkeypatch.setitem(sys.modules, "pyarrow", None)
                reset_dependency_cache("pyarrow")
                try:
                    with pytest.raises(MissingDependencyError):
                        load_arrow_sync(client, "items", tbl)
                finally:
                    monkeypatch.undo()
                    reset_dependency_cache("pyarrow")
                with state.lock:
                    assert state.post_ready_frames == baseline
            finally:
                client.close()
        else:

            async def _run_async() -> None:
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
                    with state.lock:
                        baseline = state.post_ready_frames
                    monkeypatch.setitem(sys.modules, "pyarrow", None)
                    reset_dependency_cache("pyarrow")
                    try:
                        with pytest.raises(MissingDependencyError):
                            await load_arrow_async(aclient, "items", tbl)
                    finally:
                        monkeypatch.undo()
                        reset_dependency_cache("pyarrow")
                    with state.lock:
                        assert state.post_ready_frames == baseline
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert state.catalog_binds == []
    assert state.completed_copies == []


@pytest.mark.parametrize("is_async", (False, True))
def test_native_copy_arrow_ipc_rejects_malformed_oversize_and_accepts_buffers(is_async: bool) -> None:
    """Verify malformed and 17 MiB IPC raise DataError while memoryview/bytearray/pa.Buffer succeed."""
    schema, batches = _three_arrow_batches()
    sink1 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink1, schema) as w1:
        w1.write_batch(batches[0])
    ipc1 = bytes(sink1.getvalue())
    sink2 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink2, schema) as w2:
        w2.write_batch(batches[1])
    ipc2 = bytes(sink2.getvalue())

    oversize_buf = bytearray(17 * 1024 * 1024)
    oversize_view = memoryview(oversize_buf)

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
                with pytest.raises(DataError):
                    client.copy_from_arrow("items", b"not-arrow")
                with pytest.raises(DataError):
                    client.copy_from_arrow("items", oversize_view)
                with pytest.raises((BufferError, TypeError)):
                    client.copy_from_arrow("items", memoryview(bytearray(32))[::2])
                assert state.catalog_binds == []

                with (
                    client.copy_from_arrow("items", memoryview(ipc1)) as bad_malformed,
                    pytest.raises(DataError),
                ):
                    bad_malformed.write_arrow_ipc(b"not-arrow")

                with (
                    client.copy_from_arrow("items", bytearray(ipc1)) as bad_oversize,
                    pytest.raises(DataError),
                ):
                    bad_oversize.write_arrow_ipc(oversize_view)

                with client.copy_from_arrow("items", pa.py_buffer(ipc1)) as good:
                    assert isinstance(good, native_pg.SyncPostgresCopyIn)
                    good.write_arrow_ipc(memoryview(bytearray(ipc2)))
                    inserted = good.finish()
                    assert inserted == 4
            finally:
                client.close()
        else:

            async def _run_async() -> None:
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
                    with pytest.raises(DataError):
                        await aclient.copy_from_arrow("items", b"not-arrow")
                    with pytest.raises(DataError):
                        await aclient.copy_from_arrow("items", oversize_view)
                    with pytest.raises((BufferError, TypeError)):
                        await aclient.copy_from_arrow("items", memoryview(bytearray(32))[::2])
                    assert state.catalog_binds == []

                    async with await aclient.copy_from_arrow("items", memoryview(ipc1)) as bad_malformed:
                        with pytest.raises(DataError):
                            await bad_malformed.write_arrow_ipc(b"not-arrow")

                    async with await aclient.copy_from_arrow("items", bytearray(ipc1)) as bad_oversize:
                        with pytest.raises(DataError):
                            await bad_oversize.write_arrow_ipc(oversize_view)

                    async with await aclient.copy_from_arrow("items", pa.py_buffer(ipc1)) as good:
                        assert isinstance(good, native_pg.AsyncPostgresCopyIn)
                        await good.write_arrow_ipc(memoryview(bytearray(ipc2)))
                        inserted = await good.finish()
                        assert inserted == 4
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert len(state.aborted_copies) == 2
    assert len(state.completed_copies) == 1
    assert len(PostgreSQLCore.decode_pgcopy_rows(state.completed_copies[0][1])) == 4


@pytest.mark.parametrize("is_async", (False, True))
def test_load_arrow_producer_exception_aborts_and_recovers_pool(is_async: bool) -> None:
    """Verify producer exception after batch 1 sends CopyFail + ROLLBACK and recovers max_size=1 pool."""
    schema, batches = _three_arrow_batches()
    closed_evt = threading.Event()

    def _failing_gen() -> Iterator[pa.RecordBatch]:
        try:
            yield batches[0]
            msg = "boom"
            raise RuntimeError(msg)
        finally:
            closed_evt.set()

    reader = pa.RecordBatchReader.from_batches(schema, _failing_gen())

    with _running_copy_peer() as (port, state):
        if not is_async:
            pool = SyncPostgresPool(
                host="127.0.0.1",
                port=port,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
                min_size=0,
                max_size=1,
            )
            try:
                c1 = pool.acquire()
                try:
                    with pytest.raises((RuntimeError, OSError), match="boom"):
                        load_arrow_sync(c1, "items", reader)
                finally:
                    c1.close()

                c2 = pool.acquire()
                try:
                    assert c2.query("SELECT 1").rows == ((1,),)
                finally:
                    c2.close()
            finally:
                pool.close()
        else:

            async def _failing_async_gen() -> AsyncIterator[pa.RecordBatch]:
                try:
                    yield batches[0]
                    msg = "boom"
                    raise RuntimeError(msg)
                finally:
                    closed_evt.set()

            class _FailingAsyncSource:
                def __init__(self) -> None:
                    self.schema = schema

                def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
                    return _failing_async_gen()

            async def _run_async() -> None:
                apool = AsyncPostgresPool(
                    host="127.0.0.1",
                    port=port,
                    user="postgres",
                    database="appdb",
                    sslmode="disable",
                    channel_binding="disable",
                    min_size=0,
                    max_size=1,
                )
                try:
                    ac1 = await apool.acquire()
                    try:
                        with pytest.raises(RuntimeError, match="boom"):
                            await load_arrow_async(ac1, "items", _FailingAsyncSource())
                    finally:
                        await ac1.close()

                    ac2 = await apool.acquire()
                    try:
                        res = await ac2.query("SELECT 1")
                        assert res.rows == ((1,),)
                    finally:
                        await ac2.close()
                finally:
                    await apool.close()

            asyncio.run(_run_async())

    assert closed_evt.is_set()
    assert state.completed_copies == []
    assert len(state.aborted_copies) == 1
    assert "Q:ROLLBACK" in state.events
    assert "Q:COMMIT" not in state.events


def test_native_copy_to_binary_rows_early_break_and_cancel_recover_pool() -> None:
    """Verify binary copy_to yields tuple[bytes | None, ...] and early break/cancel recovers max_size=1 pool."""
    expected_rows: list[tuple[bytes | None, ...]] = [
        (struct.pack("!i", 10), b"alpha"),
        (struct.pack("!i", 20), None),
        (struct.pack("!i", 30), b""),
    ]

    full_state = _CopyPeerState(copy_out_rows=expected_rows)
    with _running_copy_peer(full_state) as (port, _):
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
            with client.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                assert isinstance(copy_out, native_pg.SyncPostgresCopyOut)
                rows = list(copy_out)
            copy_out.close()
            assert rows == expected_rows
            assert client.query("SELECT 1").rows == ((1,),)
        finally:
            client.close()

    break_state = _CopyPeerState(copy_out_rows=expected_rows, stall_copy_out_after_row1=True)
    with _running_copy_peer(break_state) as (port, peer):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            try:
                with c1.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                    first = next(copy_out)
                    assert first == expected_rows[0]
            finally:
                c1.close()
            assert len(peer.cancels) == 1

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            pool.close()

    async_cancel_state = _CopyPeerState(copy_out_rows=expected_rows, stall_copy_out_after_row1=True)

    async def _exercise_async_copy_to(port: int) -> None:
        apool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await apool.acquire()
            try:
                async with await ac1.copy_to("COPY items TO STDOUT (FORMAT binary)") as copy_out:
                    assert isinstance(copy_out, native_pg.AsyncPostgresCopyOut)
                    row1 = await anext(copy_out)
                    assert row1 == expected_rows[0]
                    row2_task = asyncio.create_task(_as_coro(anext(copy_out)))
                    await asyncio.sleep(0)
                    row2_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await row2_task
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(async_cancel_state) as (port, apeer):
        asyncio.run(_exercise_async_copy_to(port))
    assert len(apeer.cancels) >= 1


@pytest.mark.parametrize("stage", ("preflight", "start_copy_in"))
def test_native_copy_from_arrow_async_cancel_mid_preflight_and_mid_start_recovers_pool(stage: str) -> None:
    """Verify cancelling copy_from_arrow(..., schema_name='analytics') mid-preflight or mid-start recovers pool."""
    schema, batches = _three_arrow_batches()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batches[0])
    first_ipc = bytes(sink.getvalue())

    state = _CopyPeerState(
        catalog_rows=[
            ("analytics", "61001", "id", _INT4_OID, -1, 1),
            ("analytics", "61001", "label", _TEXT_OID, -1, 2),
        ],
        stall_first_preflight=(stage == "preflight"),
        stall_first_copy_in=(stage == "start_copy_in"),
    )
    trigger_evt = state.preflight_entered if stage == "preflight" else state.copy_in_entered

    async def _run_cancel(port: int) -> None:
        apool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await apool.acquire()
            try:
                task = asyncio.create_task(
                    _as_coro(ac1.copy_from_arrow("items", first_ipc, schema_name="analytics")),
                )
                await asyncio.sleep(0)
                entered = await asyncio.to_thread(trigger_evt.wait, 3.0)
                assert entered
                task.cancel()
                with pytest.raises((asyncio.CancelledError, OperationalError)):
                    await task
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(state) as (port, peer):
        asyncio.run(_run_cancel(port))

    assert peer.completed_copies == []
    assert len(peer.cancels) >= 1
    if stage == "preflight":
        assert not any(ev.startswith("Q:COPY ") for ev in peer.events)


@pytest.mark.parametrize("is_async", (False, True))
def test_native_copy_from_binary_write_rows_and_unhandled_exit_abort(is_async: bool) -> None:
    """Verify SyncPostgresCopyIn/AsyncPostgresCopyIn write(rows, oids), finish(), and abort on unhandled exit."""
    copy_sql = 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'
    rows: list[tuple[int, str | None]] = [(1, "one"), (2, None), (3, "three")]
    oids = [_INT4_OID, _TEXT_OID]

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
                with client.copy_from(copy_sql) as copy_in:
                    assert isinstance(copy_in, native_pg.SyncPostgresCopyIn)
                    copy_in.write(rows[:2], oids)
                    copy_in.write(rows[2:], oids)
                    assert copy_in.finish() == 3
                copy_in.close()

                with pytest.raises(RuntimeError, match="abort-sync"), client.copy_from(copy_sql) as aborted:
                    aborted.write([(99, "discard")], oids)
                    raise RuntimeError("abort-sync")

                assert client.query("SELECT 1").rows == ((1,),)
            finally:
                client.close()
        else:

            async def _run_async() -> None:
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
                    async with await aclient.copy_from(copy_sql) as copy_in:
                        assert isinstance(copy_in, native_pg.AsyncPostgresCopyIn)
                        await copy_in.write(rows[:2], oids)
                        await copy_in.write(rows[2:], oids)
                        assert await copy_in.finish() == 3
                    await copy_in.aclose()

                    with pytest.raises(RuntimeError, match="abort-async"):
                        async with await aclient.copy_from(copy_sql) as aborted:
                            await aborted.write([(99, "discard")], oids)
                            raise RuntimeError("abort-async")

                    res = await aclient.query("SELECT 1")
                    assert res.rows == ((1,),)
                finally:
                    await aclient.close()

            asyncio.run(_run_async())

    assert len(state.completed_copies) == 1
    assert PostgreSQLCore.decode_pgcopy_rows(state.completed_copies[0][1]) == [
        (struct.pack("!i", 1), b"one"),
        (struct.pack("!i", 2), None),
        (struct.pack("!i", 3), b"three"),
    ]
    assert len(state.aborted_copies) == 1
    assert "Q:ROLLBACK" in state.events


def test_native_copy_overlapping_and_post_close_raise_interface_error() -> None:
    """Verify overlapping async handle calls and post-close/finish calls raise InterfaceError immediately."""
    state = _CopyPeerState(
        copy_out_rows=[(struct.pack("!i", 1), b"a"), (struct.pack("!i", 2), b"b")],
        stall_copy_out_after_row1=True,
        hold_finish_until_released=True,
    )
    copy_in_sql = 'COPY "public"."items" ("id", "label") FROM STDIN (FORMAT binary)'

    async def _exercise(port: int) -> None:
        c_in = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c_in.connect()
        try:
            copy_in = await c_in.copy_from(copy_in_sql)
            await copy_in.write([(1, "ok")], [_INT4_OID, _TEXT_OID])
            finish_fut = asyncio.create_task(_as_coro(copy_in.finish()))
            await asyncio.sleep(0)
            assert await asyncio.to_thread(state.finish_entered.wait, 3.0)
            with pytest.raises(InterfaceError):
                await copy_in.write([(2, "overlap")], [_INT4_OID, _TEXT_OID])
            with pytest.raises(InterfaceError):
                await copy_in.write_arrow_ipc(b"overlap")
            with pytest.raises(InterfaceError):
                await copy_in.finish()
            state.signal_release_finish()
            assert await finish_fut == 1

            await copy_in.aclose()
            with pytest.raises(InterfaceError):
                await copy_in.write([(3, "closed")], [_INT4_OID, _TEXT_OID])
            with pytest.raises(InterfaceError):
                await copy_in.write_arrow_ipc(b"closed")
            with pytest.raises(InterfaceError):
                await copy_in.finish()
        finally:
            await c_in.close()

        c_out = AsyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c_out.connect()
        try:
            copy_out = await c_out.copy_to("COPY items TO STDOUT (FORMAT binary)")
            assert await anext(copy_out) == (struct.pack("!i", 1), b"a")
            next_task = asyncio.create_task(_as_coro(anext(copy_out)))
            await asyncio.sleep(0)
            with pytest.raises(InterfaceError):
                await anext(copy_out)
            next_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await next_task
            await copy_out.aclose()
            await copy_out.aclose()
            with pytest.raises(InterfaceError):
                await anext(copy_out)
        finally:
            await c_out.close()

    with _running_copy_peer(state) as (port, _):
        asyncio.run(_exercise(port))


@pytest.mark.parametrize("op_kind", ("finish", "write_arrow_ipc"))
def test_native_copy_async_cancel_finish_and_write_recovers_pool(op_kind: str) -> None:
    """Verify cancelling copy_in.finish() or write_arrow_ipc() recovers a max_size=1 pool without aclose."""
    schema, batches = _three_arrow_batches()
    sink1 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink1, schema) as w1:
        w1.write_batch(batches[0])
    ipc1 = bytes(sink1.getvalue())
    sink2 = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink2, schema) as w2:
        w2.write_batch(batches[1])
    ipc2 = bytes(sink2.getvalue())

    state = _CopyPeerState(hold_finish_until_released=(op_kind == "finish"))

    async def _exercise(port: int) -> None:
        apool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await apool.acquire()
            try:
                copy_in = await ac1.copy_from_arrow("items", ipc1)
                if op_kind == "finish":
                    fut = asyncio.create_task(_as_coro(copy_in.finish()))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.finish_entered.wait, 3.0)
                    fut.cancel()
                    with pytest.raises((asyncio.CancelledError, OperationalError)):
                        await fut
                else:
                    fut = asyncio.create_task(_as_coro(copy_in.write_arrow_ipc(ipc2)))
                    await asyncio.sleep(0)
                    fut.cancel()
                    with pytest.raises((asyncio.CancelledError, OperationalError)):
                        await fut
                    del copy_in
            finally:
                await ac1.close()

            ac2 = await apool.acquire()
            try:
                res = await ac2.query("SELECT 1")
                assert res.rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await apool.close()

    with _running_copy_peer(state) as (port, peer):
        asyncio.run(_exercise(port))

    assert peer.completed_copies == []
