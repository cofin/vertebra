"""Unit tests for PostgreSQL Arrow results, select_to_arrow, and Arrow streaming portals."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import ctypes
import datetime
import decimal
import inspect
import json
import random
import select
import signal
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import vertebra._postgres as native_pg
import vertebra._vertebra as native_vertebra
from tests.unit.db.postgres_data_peers import (
    _CUSTOM_ENUM_OID,
    _TEXT_OID,
    _as_coro,
    _decode_ipc_table,
    _find_loop_drainer_fd,
    _forbid_row_materialization,
    _process_arrow_shm_files,
    _running_sync_arrow_peer,
    _serialize_ipc_rows,
    _SyncArrowPeerState,
)
from vertebra._postgres import (
    AsyncPostgresClient,
    AsyncPostgresPool,
    SyncPostgresClient,
    SyncPostgresPool,
)
from vertebra.db import arrow as arrow_module
from vertebra.db.adapters.postgres.types import PostgresArrowResult
from vertebra.db.arrow import ArrowCursor, read_arrow_shm
from vertebra.db.driver import DataError, InterfaceError, OperationalError
from vertebra.typing import reset_dependency_cache
from vertebra.utils.module_loader import import_optional


@pytest.mark.parametrize(
    ("rows", "columns"),
    [
        ([], ("id", "label")),
        ([(1, "a"), (2, "bc"), (3, "def")], ("id", "id")),
    ],
)
def test_arrow_count_postgres_arrow_result_inline_and_frozen(
    rows: list[tuple[int, str]],
    columns: tuple[str, ...],
) -> None:
    """Verify frozen PostgresArrowResult preserves inline bytes, duplicate columns, and row_count."""
    ipc_bytes = _serialize_ipc_rows(rows)
    res = PostgresArrowResult(
        buffer=ipc_bytes,
        row_count=len(rows),
        columns=columns,
        used_shm=False,
    )
    assert res.buffer == ipc_bytes
    assert res.row_count == len(rows)
    assert res.columns == columns
    assert res.used_shm is False
    mutable: Any = res
    with pytest.raises(AttributeError):
        mutable.row_count = 99


def test_arrow_count_postgres_arrow_result_accepts_native_shm_cursor() -> None:
    """Verify PostgresArrowResult accepts a native SHM ArrowCursor when used_shm is True."""
    ipc_bytes = _serialize_ipc_rows([(10, "x"), (20, "yz")])
    native_cur = native_vertebra.ArrowCursor.write_ipc_to_shm(
        ArrowCursor._next_shm_segment_name(),
        ipc_bytes,
        2,
    )
    try:
        res = PostgresArrowResult(
            buffer=native_cur,
            row_count=2,
            columns=("id", "label"),
            used_shm=True,
        )
        assert res.used_shm is True
        assert res.row_count == 2
        assert res.columns == ("id", "label")
        assert res.buffer is native_cur
    finally:
        native_cur.release()


@pytest.mark.parametrize(
    ("kwargs", "exc_type"),
    [
        ({"buffer": b"", "row_count": True, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": False, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": "2", "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": -1, "columns": ("a",), "used_shm": False}, ValueError),
        ({"buffer": b"", "row_count": 0, "columns": ["a"], "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": 0, "columns": (1,), "used_shm": False}, TypeError),
        ({"buffer": b"", "row_count": 0, "columns": ("a",), "used_shm": 1}, TypeError),
        ({"buffer": bytearray(b"x"), "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": memoryview(b"x"), "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": None, "row_count": 0, "columns": ("a",), "used_shm": False}, TypeError),
        ({"buffer": b"ipc", "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
        ({"buffer": None, "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
        ({"buffer": object(), "row_count": 0, "columns": ("a",), "used_shm": True}, TypeError),
    ],
)
def test_arrow_count_postgres_arrow_result_rejects_invalid_fields(
    kwargs: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """Verify PostgresArrowResult rejects bool/negative row_count, bad columns, and buffer/used_shm mismatches."""
    with pytest.raises(exc_type):
        PostgresArrowResult(**kwargs)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
    ],
)
def test_arrow_count_inline_ipc_returns_exact_count_without_decoding_rows(
    rows: list[tuple[int, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify inline ArrowCursor(row_count=N) reports len/num_rows/row_count without decoding IPC rows."""
    ipc_bytes = _serialize_ipc_rows(rows)
    expected = len(rows)
    cursor = ArrowCursor(
        ipc_bytes,
        columns=("id", "label"),
        used_shm=False,
        row_count=expected,
    )
    assert cursor._ipc_table_cache is None
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    reset_dependency_cache("pyarrow")
    try:
        assert len(cursor) == expected
        assert cursor.num_rows == expected
        assert cursor.row_count == expected
        assert cursor._ipc_table_cache is None
    finally:
        monkeypatch.undo()
        reset_dependency_cache("pyarrow")

    tbl = cursor.to_arrow_table()
    assert tbl.num_rows == expected
    assert tbl.column_names == ["id", "label"]


@pytest.mark.parametrize(
    ("kwargs", "exc_type"),
    [
        ({"row_count": -1}, ValueError),
        ({"row_count": True}, TypeError),
        ({"row_count": False}, TypeError),
        ({"row_count": "3"}, TypeError),
        ({"rows": [(1,)], "row_count": 2}, ValueError),
        ({"rows": [(1,)], "row_count": 0}, ValueError),
    ],
)
def test_arrow_count_rejects_negative_bool_and_conflicting_row_metadata(
    kwargs: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """Verify ArrowCursor rejects negative, bool, non-int, and conflicting explicit row counts."""
    with pytest.raises(exc_type):
        ArrowCursor(b"", **kwargs)


@pytest.mark.parametrize("bad_count", [0, 4, 6])
def test_arrow_count_rejects_conflicting_native_shm_cursor_count(bad_count: int) -> None:
    """Verify ArrowCursor rejects explicit row_count values (including 0) that disagree with native SHM row_count."""
    ipc_bytes = _serialize_ipc_rows([(i, f"v{i}") for i in range(5)])
    native_cur = native_vertebra.ArrowCursor.write_ipc_to_shm(
        ArrowCursor._next_shm_segment_name(),
        ipc_bytes,
        5,
    )
    try:
        with pytest.raises(ValueError, match="row_count"):
            ArrowCursor(native_cur, used_shm=True, row_count=bad_count)
        matching = ArrowCursor(native_cur, used_shm=True, row_count=5)
        assert len(matching) == 5
        assert matching.num_rows == 5
        assert matching.row_count == 5
        assert matching._ipc_table_cache is None
    finally:
        native_cur.release()


def test_arrow_count_keeps_arrow_module_engine_agnostic() -> None:
    """Verify vertebra.db.arrow imports zero database adapter or PostgreSQL modules."""
    assert arrow_module.__file__ is not None
    source = Path(arrow_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ] + [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    assert not any("postgres" in mod or "adapters" in mod for mod in imported)


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"threshold_rows": -1}, (ValueError, DataError)),
        ("SELECT 1", None, {"threshold_rows": True}, (TypeError,)),
        ("SELECT 1", None, {"threshold_rows": False}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": -8}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": True}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": False}, (TypeError,)),
        ("SELECT 1", None, {"timeout": 0.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": -1.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("nan")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("inf")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": True}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1", [1], {"parameter_oids": [-1]}, (ValueError, DataError, OverflowError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_sync_arrow_select_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify select_to_arrow rejects invalid bounds, bools, timeouts, and parameter_oids before sending wire frames."""
    with _running_sync_arrow_peer() as (port, state):
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.select_to_arrow(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            client.close()


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"batch_size": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": -4}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": True}, (TypeError,)),
        ("SELECT 1", None, {"batch_size": False}, (TypeError,)),
        ("SELECT 1", None, {"columns": "not_seq"}, (TypeError,)),
        ("SELECT 1", None, {"columns": [123]}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_sync_arrow_stream_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify stream_arrow_batches rejects invalid batch_size, bools, columns, and parameter_oids with zero wire frames."""
    with _running_sync_arrow_peer() as (port, state):
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.stream_arrow_batches(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            client.close()


@pytest.mark.parametrize(
    ("row_count", "expect_shm"),
    [
        (0, False),
        (3, False),
        (4, True),
    ],
)
def test_sync_arrow_select_cold_custom_oids_volatile_returning_and_thresholds(
    row_count: int,
    expect_shm: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cold custom OIDs run Parse+Describe preflight + discovery before single Execute and promote at threshold_rows."""
    sql = "INSERT INTO items (tag, mood) VALUES ($1, $2) RETURNING id, vec, mood, dom_id, rng, moods, fn_proc"
    peer_state = _SyncArrowPeerState(returning_row_count=row_count)

    with _forbid_row_materialization(monkeypatch), _running_sync_arrow_peer(peer_state) as (port, state):
        client = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        res: PostgresArrowResult | None = None
        try:
            res = client.select_to_arrow(
                sql,
                ["alpha", "happy"],
                parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                threshold_rows=3,
            )
            assert isinstance(res, PostgresArrowResult)
            assert res.row_count == row_count
            assert res.columns == ("id", "vec", "mood", "dom_id", "rng", "moods", "fn_proc")
            assert res.used_shm is expect_shm

            if expect_shm:
                assert isinstance(res.buffer, native_vertebra.ArrowCursor)
                assert res.buffer.row_count == row_count
                schema, table = _decode_ipc_table(res.buffer.buffer_view())
            else:
                assert isinstance(res.buffer, bytes)
                schema, table = _decode_ipc_table(res.buffer)

            assert schema.metadata is not None
            assert schema.metadata.get(b"postgres.arrow.version") == b"1"
            assert schema.field("vec").type == pa.list_(schema.field("vec").type.value_field, 3)
            assert schema.field("vec").metadata[b"postgres.encoding"] == b"pgvector"
            assert schema.field("mood").metadata[b"postgres.encoding"] == b"pg-enum"
            assert schema.field("dom_id").metadata[b"postgres.oid"] == b"42003"
            assert schema.field("dom_id").metadata[b"postgres.base_oid"] == b"23"
            assert schema.field("dom_id").metadata[b"postgres.type_name"] == b"int4_dom"
            assert schema.field("rng").metadata[b"postgres.encoding"] == b"pg-range"
            assert schema.field("moods").metadata[b"postgres.encoding"] == b"pg-array"
            assert schema.field("fn_proc").metadata[b"postgres.encoding"] == b"regproc-text"
            assert table.num_rows == row_count

            rows_list = table.to_pylist()
            expected_ids = [100 + i for i in range(row_count)]
            assert [r["id"] for r in rows_list] == expected_ids
            if row_count > 0:
                assert rows_list[0]["vec"] == [1.0, 2.0, 0.5]
                assert rows_list[0]["mood"] == "happy"
                assert rows_list[0]["dom_id"] == 1000
                assert rows_list[0]["rng"] == "[0,10)"
                assert rows_list[0]["moods"]["values"] == ["happy", "calm"]
                assert rows_list[0]["fn_proc"] == "now"

            with pytest.raises(DataError):
                client.select_to_arrow("SELECT 42 AS answer", max_result_bytes=8)
            warm_check = client.select_to_arrow("SELECT 42 AS answer")
            assert isinstance(warm_check.buffer, bytes)
            _, warm_tbl = _decode_ipc_table(warm_check.buffer)
            assert warm_tbl.to_pylist() == [{"answer": 42}]
        finally:
            if res is not None and res.used_shm and isinstance(res.buffer, native_vertebra.ArrowCursor):
                res.buffer.release()
            client.close()

    assert state.returning_describes == 1
    assert state.returning_executes == 1
    assert state.catalog_queries >= 2
    describe_pos = state.events.index("DESCRIBE_ONLY:RETURNING")
    exec_pos = next(i for i, ev in enumerate(state.events) if ev.startswith("EXEC:RETURNING:"))
    cat_between = [i for i, ev in enumerate(state.events) if ev == "Q:CATALOG" and describe_pos < i < exec_pos]
    assert len(cat_between) == 1
    assert state.events[exec_pos] == "EXEC:RETURNING:fmts=[1, 1, 0, 1, 0, 0, 0]"


def test_sync_arrow_select_timeout_cancels_and_recovers_pool() -> None:
    """Verify select_to_arrow command timeout emits CancelRequest, cleans up socket, and recovers pool."""
    peer_state = _SyncArrowPeerState(stall_sleep_query=True)
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            try:
                with pytest.raises(OperationalError) as exc_info:
                    c1.select_to_arrow("SELECT pg_sleep(5)", timeout=0.02)
                assert exc_info.value.sqlstate is None
            finally:
                c1.close()

            assert state.cancels == [(9001, 77001)]

            c2 = pool.acquire()
            try:
                res = c2.select_to_arrow("SELECT 42 AS answer", threshold_rows=10)
                assert res.row_count == 1
                assert res.columns == ("answer",)
                assert res.used_shm is False
                assert isinstance(res.buffer, bytes)
                _, tbl = _decode_ipc_table(res.buffer)
                assert tbl.to_pylist() == [{"answer": 42}]
            finally:
                c2.close()
        finally:
            pool.close()


def test_sync_arrow_stream_portal_suspended_barrier_text_fidelity_and_pool_pinning() -> None:
    """Verify SyncPostgresArrowStream yields at PortalSuspended, preserves regproc-text and TemporalText, pins pool, and retains cursors after close."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        custom_date_style=("Postgres, MDY", "America/New_York"),
    )
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
            acquire_timeout=0.05,
        )
        cur1: Any = None
        cur2: Any = None
        mv1: memoryview | None = None
        try:
            c1 = pool.acquire()
            stream: native_pg.SyncPostgresArrowStream | None = None
            try:
                stream = c1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                assert isinstance(stream, native_pg.SyncPostgresArrowStream)
                assert state.events.count("Q:BEGIN") == 1

                cur1 = next(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 1
                assert not state.allow_batch2.is_set()
                assert len(state.execute_frames) == 1
                assert state.execute_frames[0][1] == 1

                mv1 = cur1.buffer_view()
                schema1, tbl1 = _decode_ipc_table(mv1)
                assert schema1.metadata[b"postgres.arrow.version"] == b"1"
                assert schema1.field("fn_col").metadata[b"postgres.encoding"] == b"regproc-text"
                assert schema1.field("ts_col").metadata[b"postgres.encoding"] == b"pg-timestamptz"
                assert tbl1.to_pylist() == [
                    {
                        "fn_col": "now",
                        "ts_col": {
                            "kind": 2,
                            "value": None,
                            "wire_text": "Sun Nov 18 11:59:59 1883 LMT",
                            "date_style": "Postgres, MDY",
                            "time_zone": "America/New_York",
                        },
                    }
                ]

                with pytest.raises(InterfaceError):
                    c1.query("SELECT 1")
                with pytest.raises(InterfaceError):
                    c1.select_to_arrow("SELECT 1")
                with pytest.raises(OperationalError):
                    pool.acquire()

                worker_out: list[Any] = []
                worker_err: list[BaseException] = []
                active_stream = stream

                def _fetch_batch2() -> None:
                    try:
                        worker_out.append(next(active_stream))
                    except BaseException as exc:
                        worker_err.append(exc)

                t_batch2 = threading.Thread(target=_fetch_batch2)
                t_batch2.start()
                assert state.batch2_waiting.wait(timeout=2.0)

                with pytest.raises(InterfaceError):
                    next(stream)
                with pytest.raises(InterfaceError):
                    stream.close()

                state.allow_batch2.set()
                t_batch2.join(timeout=2.0)
                assert not worker_err
                cur2 = worker_out[0]
                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1

                with pytest.raises(StopIteration):
                    next(stream)
                with pytest.raises(StopIteration):
                    next(stream)

                stream.close()
                stream.close()
            finally:
                if stream is not None:
                    with contextlib.suppress(Exception):
                        stream.close()
                c1.close()

            assert state.events.count("Q:BEGIN") == 1
            assert state.events.count("Q:COMMIT") == 1

            assert mv1 is not None
            _, tbl1_after = _decode_ipc_table(mv1)
            assert tbl1_after.to_pylist()[0]["fn_col"] == "now"
            _, tbl2_after = _decode_ipc_table(cur2.buffer_view())
            row2 = tbl2_after.to_pylist()[0]
            assert row2["fn_col"] == "pg_typeof"
            assert row2["ts_col"]["kind"] == 0
            assert isinstance(row2["ts_col"]["value"], int)

            c_check = pool.acquire()
            try:
                assert c_check.query("SELECT 1").rows == ((1,),)
            finally:
                c_check.close()
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            pool.close()


def test_sync_arrow_stream_mid_execute_8mib_byte_cap_resume_and_duplicate_columns() -> None:
    """Verify 3 x 3.5 MiB rows inside one Execute(10) flush batch 1 at 8 MiB without a second Execute and resume row 3 with duplicate column names."""
    peer_state = _SyncArrowPeerState(portal_mode="big_resume")
    with _running_sync_arrow_peer(peer_state) as (port, state):
        client = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        client.connect()
        cur1: Any = None
        cur2: Any = None
        try:
            with client.stream_arrow_batches(
                "SELECT a, b FROM big_rows",
                batch_size=10,
                columns=["x", "x"],
            ) as stream:
                cur1 = next(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 2
                assert len(state.execute_frames) == 1
                assert not state.allow_tail_complete.is_set()

                schema1, tbl1 = _decode_ipc_table(cur1.buffer_view())
                assert [f.name for f in schema1] == ["x", "x"]
                assert tbl1.column(0)[0].as_py() == "a" * (1792 * 1024)
                assert tbl1.column(1)[0].as_py() == "b" * (1792 * 1024)
                assert tbl1.column(0)[1].as_py() == "b" * (1792 * 1024)
                assert tbl1.column(1)[1].as_py() == "a" * (1792 * 1024)

                state.allow_tail_complete.set()
                cur2 = next(stream)
                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1
                assert len(state.execute_frames) == 1

                schema2, tbl2 = _decode_ipc_table(cur2.buffer_view())
                assert [f.name for f in schema2] == ["x", "x"]
                assert tbl2.column(0)[0].as_py() == "c" * (1792 * 1024)
                assert tbl2.column(1)[0].as_py() == "c" * (1792 * 1024)

                with pytest.raises(StopIteration):
                    next(stream)
        finally:
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            client.close()


def test_sync_arrow_stream_mid_execute_close_cancels_retains_cursor_and_recovers_pool() -> None:
    """Verify stream.close() while paused mid-Execute at 8 MiB sends CancelRequest, keeps retained SHM cursor readable, and recovers max_size=1 pool."""
    peer_state = _SyncArrowPeerState(portal_mode="big_stall_cancel")
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        cur1: Any = None
        mv1: memoryview | None = None
        try:
            c1 = pool.acquire()
            try:
                stream = c1.stream_arrow_batches(
                    "SELECT a, b FROM stalled_big_rows",
                    batch_size=10,
                    columns=["x", "x"],
                )
                cur1 = next(stream)
                assert cur1.row_count == 2
                mv1 = cur1.buffer_view()
                assert len(state.execute_frames) == 1

                stream.close()
                with pytest.raises(InterfaceError):
                    next(stream)
            finally:
                c1.close()

            assert state.cancels == [(9001, 77001)]
            assert mv1 is not None
            schema1, tbl1 = _decode_ipc_table(mv1)
            assert [f.name for f in schema1] == ["x", "x"]
            assert tbl1.num_rows == 2
            assert len(tbl1.column(0)[0].as_py()) == 1792 * 1024

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            pool.close()


@pytest.mark.parametrize("failure_mode", ("columns_mismatch", "oversized_row"))
def test_sync_arrow_stream_columns_mismatch_and_oversized_row_rollback_and_recover(
    failure_mode: str,
) -> None:
    """Verify columns width mismatch and single row > 8 MiB raise DataError, clean up portal scope, and keep pool healthy."""
    portal_mode = "big_resume" if failure_mode == "columns_mismatch" else "oversized_row"
    peer_state = _SyncArrowPeerState(portal_mode=portal_mode)
    with _running_sync_arrow_peer(peer_state) as (port, state):
        pool = SyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = pool.acquire()
            try:
                if failure_mode == "columns_mismatch":
                    with pytest.raises(DataError):
                        c1.stream_arrow_batches(
                            "SELECT a, b FROM big_rows",
                            batch_size=4,
                            columns=["only_one"],
                        )
                    assert "Q:ROLLBACK" in state.events
                else:
                    with (
                        pytest.raises(DataError),
                        c1.stream_arrow_batches("SELECT payload FROM huge_rows", batch_size=4) as stream,
                    ):
                        next(stream)
            finally:
                c1.close()

            c2 = pool.acquire()
            try:
                assert c2.query("SELECT 1").rows == ((1,),)
            finally:
                c2.close()
        finally:
            pool.close()


def test_sync_arrow_stream_single_scope_begin_rollback_and_ambient_savepoint() -> None:
    """Verify idle early-closed stream emits single BEGIN+ROLLBACK while ambient client.begin() uses SAVEPOINT+ROLLBACK TO SAVEPOINT+RELEASE SAVEPOINT."""
    idle_state = _SyncArrowPeerState(portal_mode="scope_tracking")
    with _running_sync_arrow_peer(idle_state) as (port, s_idle):
        c_idle = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        c_idle.connect()
        cur_idle: Any = None
        try:
            with c_idle.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_idle = next(stream)
                assert cur_idle.row_count == 1
            assert c_idle.query("SELECT 1").rows == ((1,),)
        finally:
            if cur_idle is not None:
                cur_idle.release()
            c_idle.close()

    assert s_idle.events.count("Q:BEGIN") == 1
    assert s_idle.events.count("Q:ROLLBACK") == 1
    assert any(ev.startswith("Close(P:") for ev in s_idle.events)

    amb_state = _SyncArrowPeerState(portal_mode="scope_tracking")
    with _running_sync_arrow_peer(amb_state) as (port, s_amb):
        c_amb = SyncPostgresClient(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        c_amb.connect()
        cur_amb: Any = None
        try:
            c_amb.begin()
            c_amb.query("INSERT INTO audit VALUES (1)")
            with c_amb.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_amb = next(stream)
                assert cur_amb.row_count == 1
            assert c_amb.query("SELECT 1").rows == ((1,),)
            c_amb.commit()
            assert c_amb.query("SELECT 1").rows == ((1,),)
        finally:
            if cur_amb is not None:
                cur_amb.release()
            c_amb.close()

    assert s_amb.events.count("Q:BEGIN") == 1
    assert "EXEC:INSERT_AUDIT" in s_amb.events
    savepoints = [ev for ev in s_amb.events if ev.startswith("Q:SAVEPOINT vtb_sp_")]
    rollbacks_sp = [ev for ev in s_amb.events if ev.startswith("Q:ROLLBACK TO SAVEPOINT vtb_sp_")]
    releases_sp = [ev for ev in s_amb.events if ev.startswith("Q:RELEASE SAVEPOINT vtb_sp_")]
    assert len(savepoints) == 1
    assert len(rollbacks_sp) == 1
    assert len(releases_sp) == 1
    assert "Q:ROLLBACK" not in s_amb.events
    assert s_amb.events.count("Q:COMMIT") == 1
    assert s_amb.events.index(releases_sp[0]) < s_amb.events.index("Q:COMMIT")


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"threshold_rows": -1}, (ValueError, DataError)),
        ("SELECT 1", None, {"threshold_rows": True}, (TypeError,)),
        ("SELECT 1", None, {"threshold_rows": False}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": -8}, (ValueError, DataError)),
        ("SELECT 1", None, {"max_result_bytes": True}, (TypeError,)),
        ("SELECT 1", None, {"max_result_bytes": False}, (TypeError,)),
        ("SELECT 1", None, {"timeout": 0.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": -1.0}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("nan")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": float("inf")}, (ValueError, InterfaceError)),
        ("SELECT 1", None, {"timeout": True}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1", [1], {"parameter_oids": [-1]}, (ValueError, DataError, OverflowError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_async_arrow_select_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify AsyncPostgresClient.select_to_arrow rejects invalid bounds, bools, timeouts, and parameter_oids before sending wire frames."""

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                await client.select_to_arrow(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            await client.close()

    with _running_sync_arrow_peer() as (port, state):
        asyncio.run(_run(port, state))


@pytest.mark.parametrize(
    ("sql", "params", "kwargs", "exc_types"),
    [
        ("SELECT 1", None, {"batch_size": 0}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": -4}, (ValueError, DataError)),
        ("SELECT 1", None, {"batch_size": True}, (TypeError,)),
        ("SELECT 1", None, {"batch_size": False}, (TypeError,)),
        ("SELECT 1", None, {"columns": "not_seq"}, (TypeError,)),
        ("SELECT 1", None, {"columns": [123]}, (TypeError,)),
        ("SELECT $1", [1], {"parameter_oids": [True]}, (TypeError, DataError)),
        ("SELECT $1, $2", [1, 2], {"parameter_oids": [23]}, (ValueError, DataError)),
        ("SELECT 1\x00", None, {}, (ValueError, DataError)),
    ],
)
def test_async_arrow_stream_invalid_options_send_zero_frames(
    sql: str,
    params: list[int] | None,
    kwargs: dict[str, Any],
    exc_types: tuple[type[BaseException], ...],
) -> None:
    """Verify synchronous AsyncPostgresClient.stream_arrow_batches rejects invalid batch_size, bools, columns, and parameter_oids with zero frames."""

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
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
            baseline = state.post_startup_frames
            with pytest.raises(exc_types):
                client.stream_arrow_batches(sql, params, **kwargs)
            assert state.post_startup_frames == baseline
        finally:
            await client.close()

    with _running_sync_arrow_peer() as (port, state):
        asyncio.run(_run(port, state))


@pytest.mark.parametrize(
    ("row_count", "expect_shm"),
    [
        (0, False),
        (3, False),
        (4, True),
    ],
)
def test_async_arrow_select_cold_custom_oids_volatile_returning_and_thresholds(
    row_count: int,
    expect_shm: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async select_to_arrow discovers cold custom OIDs before single volatile execution and matches sync IPC parity."""
    sql = "INSERT INTO items (tag, mood) VALUES ($1, $2) RETURNING id, vec, mood, dom_id, rng, moods, fn_proc"
    async_peer = _SyncArrowPeerState(returning_row_count=row_count)
    sync_peer = _SyncArrowPeerState(returning_row_count=row_count)

    with _forbid_row_materialization(monkeypatch):
        with _running_sync_arrow_peer(sync_peer) as (sport, _):
            sclient = SyncPostgresClient(
                host="127.0.0.1",
                port=sport,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            sclient.connect()
            sres: PostgresArrowResult | None = None
            try:
                sres = sclient.select_to_arrow(
                    sql,
                    ["alpha", "happy"],
                    parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                    threshold_rows=3,
                )
                assert isinstance(sres.buffer, (bytes, native_vertebra.ArrowCursor))
                sync_payload = (
                    bytes(sres.buffer.buffer_view())
                    if isinstance(sres.buffer, native_vertebra.ArrowCursor)
                    else sres.buffer
                )
                sync_schema, sync_tbl = _decode_ipc_table(sync_payload)
                sync_rows = sync_tbl.to_pylist()
            finally:
                if sres is not None and isinstance(sres.buffer, native_vertebra.ArrowCursor):
                    sres.buffer.release()
                sclient.close()

        async def _run_async(aport: int) -> tuple[pa.Schema, list[dict[str, Any]]]:
            aclient = AsyncPostgresClient(
                host="127.0.0.1",
                port=aport,
                user="postgres",
                database="appdb",
                sslmode="disable",
                channel_binding="disable",
            )
            await aclient.connect()
            ares: PostgresArrowResult | None = None
            try:
                ares = await aclient.select_to_arrow(
                    sql,
                    ["alpha", "happy"],
                    parameter_oids=[_TEXT_OID, _CUSTOM_ENUM_OID],
                    timeout=5.0,
                    threshold_rows=3,
                )
                assert isinstance(ares, PostgresArrowResult)
                assert ares.row_count == row_count
                assert ares.columns == ("id", "vec", "mood", "dom_id", "rng", "moods", "fn_proc")
                assert ares.used_shm is expect_shm
                if expect_shm:
                    assert isinstance(ares.buffer, native_vertebra.ArrowCursor)
                    assert ares.buffer.row_count == row_count
                    schema, tbl = _decode_ipc_table(ares.buffer.buffer_view())
                else:
                    assert isinstance(ares.buffer, bytes)
                    schema, tbl = _decode_ipc_table(ares.buffer)

                with pytest.raises(DataError):
                    await aclient.select_to_arrow("SELECT 42 AS answer", max_result_bytes=8)
                warm_check = await aclient.select_to_arrow("SELECT 42 AS answer")
                assert isinstance(warm_check.buffer, bytes)
                _, warm_tbl = _decode_ipc_table(warm_check.buffer)
                assert warm_tbl.to_pylist() == [{"answer": 42}]
                return schema, tbl.to_pylist()
            finally:
                if ares is not None and isinstance(ares.buffer, native_vertebra.ArrowCursor):
                    ares.buffer.release()
                await aclient.close()

        with _running_sync_arrow_peer(async_peer) as (aport, state):
            async_schema, async_rows = asyncio.run(_run_async(aport))

    assert async_schema == sync_schema
    assert async_rows == sync_rows
    assert state.returning_describes == 1
    assert state.returning_executes == 1
    assert state.catalog_queries >= 2
    describe_pos = state.events.index("DESCRIBE_ONLY:RETURNING")
    exec_pos = next(i for i, ev in enumerate(state.events) if ev.startswith("EXEC:RETURNING:"))
    cat_between = [i for i, ev in enumerate(state.events) if ev == "Q:CATALOG" and describe_pos < i < exec_pos]
    assert len(cat_between) == 1
    assert state.events[exec_pos] == "EXEC:RETURNING:fmts=[1, 1, 0, 1, 0, 0, 0]"


def test_async_arrow_select_timeout_and_loop_heartbeat_recovers_pool() -> None:
    """Verify async select_to_arrow timeout keeps asyncio loop responsive, sends CancelRequest, and recovers max_size=1 pool."""
    peer_state = _SyncArrowPeerState(stall_sleep_query=True)

    async def _run(port: int, state: _SyncArrowPeerState) -> int:
        ticks = 0
        stop_hb = False

        async def _heartbeat() -> None:
            nonlocal ticks
            while not stop_hb:
                await asyncio.sleep(0)
                ticks += 1

        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        hb_task = asyncio.create_task(_heartbeat())
        try:
            c1 = await pool.acquire()
            try:
                with pytest.raises(OperationalError) as exc_info:
                    await c1.select_to_arrow("SELECT pg_sleep(5)", timeout=0.05)
                assert exc_info.value.sqlstate is None
            finally:
                await c1.close()

            assert state.cancels == [(9001, 77001)]

            c2 = await pool.acquire()
            try:
                res = await c2.select_to_arrow("SELECT 42 AS answer", threshold_rows=10)
                assert res.row_count == 1
                assert res.columns == ("answer",)
                assert res.used_shm is False
                assert isinstance(res.buffer, bytes)
                _, tbl = _decode_ipc_table(res.buffer)
                assert tbl.to_pylist() == [{"answer": 42}]
            finally:
                await c2.close()
            return ticks
        finally:
            stop_hb = True
            await hb_task
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        observed_ticks = asyncio.run(_run(port, state))
    assert observed_ticks >= 3


def test_async_arrow_stream_heartbeat_overlapping_anext_and_exhausted_aclose() -> None:
    """Verify AsyncPostgresArrowStream synchronous factory, non-blocking loop heartbeat, overlapping __anext__ InterfaceError, and exhausted aclose idempotence."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        custom_date_style=("Postgres, MDY", "America/New_York"),
    )

    async def _run(port: int, state: _SyncArrowPeerState) -> int:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            min_size=0,
            max_size=1,
            acquire_timeout=0.05,
        )
        cur1: Any = None
        cur2: Any = None
        mv1: memoryview | None = None
        ticks = 0
        try:
            c1 = await pool.acquire()
            stream: Any = None
            try:
                pre_frames = state.post_startup_frames
                stream = c1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                assert not inspect.isawaitable(stream)
                assert isinstance(stream, native_pg.AsyncPostgresArrowStream)
                assert stream.__aiter__() is stream
                assert state.post_startup_frames == pre_frames

                cur1 = await anext(stream)
                assert isinstance(cur1, native_vertebra.ArrowCursor)
                assert cur1.row_count == 1
                assert state.events.count("Q:BEGIN") == 1
                assert len(state.execute_frames) == 1
                assert state.execute_frames[0][1] == 1

                mv1 = cur1.buffer_view()
                schema1, tbl1 = _decode_ipc_table(mv1)
                assert schema1.metadata[b"postgres.arrow.version"] == b"1"
                assert schema1.field("fn_col").metadata[b"postgres.encoding"] == b"regproc-text"
                assert schema1.field("ts_col").metadata[b"postgres.encoding"] == b"pg-timestamptz"
                assert tbl1.to_pylist() == [
                    {
                        "fn_col": "now",
                        "ts_col": {
                            "kind": 2,
                            "value": None,
                            "wire_text": "Sun Nov 18 11:59:59 1883 LMT",
                            "date_style": "Postgres, MDY",
                            "time_zone": "America/New_York",
                        },
                    }
                ]

                with pytest.raises(InterfaceError):
                    await c1.query("SELECT 1")
                with pytest.raises(InterfaceError):
                    await c1.select_to_arrow("SELECT 1")
                with pytest.raises(OperationalError):
                    await pool.acquire()

                stop_hb = False

                async def _heartbeat() -> None:
                    nonlocal ticks
                    while not stop_hb:
                        await asyncio.sleep(0)
                        ticks += 1

                hb_task = asyncio.create_task(_heartbeat())
                fetch2_task = asyncio.create_task(_as_coro(anext(stream)))
                try:
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.batch2_waiting.wait, 2.0)
                    while ticks < 4:
                        await asyncio.sleep(0)
                    with pytest.raises(InterfaceError):
                        stream.__anext__()
                    state.allow_batch2.set()
                    cur2 = await fetch2_task
                finally:
                    stop_hb = True
                    await hb_task

                assert isinstance(cur2, native_vertebra.ArrowCursor)
                assert cur2.row_count == 1

                with pytest.raises(StopAsyncIteration):
                    await anext(stream)
                with pytest.raises(StopAsyncIteration):
                    await anext(stream)

                await stream.aclose()
                await stream.aclose()
            finally:
                if stream is not None:
                    with contextlib.suppress(Exception):
                        await stream.aclose()
                await c1.close()

            assert state.events.count("Q:BEGIN") == 1
            assert state.events.count("Q:COMMIT") == 1

            assert mv1 is not None
            _, tbl1_after = _decode_ipc_table(mv1)
            assert tbl1_after.to_pylist()[0]["fn_col"] == "now"
            _, tbl2_after = _decode_ipc_table(cur2.buffer_view())
            row2 = tbl2_after.to_pylist()[0]
            assert row2["fn_col"] == "pg_typeof"
            assert row2["ts_col"]["kind"] == 0
            assert isinstance(row2["ts_col"]["value"], int)

            c_check = await pool.acquire()
            try:
                res_check = await c_check.query("SELECT 1")
                assert res_check.rows == ((1,),)
            finally:
                await c_check.close()
            return ticks
        finally:
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            if cur2 is not None:
                cur2.release()
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        assert asyncio.run(_run(port, state)) >= 4


def test_async_arrow_stream_aclose_lifecycle_mid_execute_8mib_and_ambient_savepoint() -> None:
    """Verify aclose() before first __anext__, during mid-Execute 8 MiB pause with retained memoryview/Table, and inside ambient savepoint scopes."""
    unopened_peer = _SyncArrowPeerState(portal_mode="scope_tracking")

    async def _exercise_unopened_and_resume(uport: int, ustate: _SyncArrowPeerState) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=uport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        try:
            baseline = ustate.post_startup_frames
            s_unopened = c.stream_arrow_batches("SELECT payload FROM items", batch_size=2)
            await s_unopened.aclose()
            await s_unopened.aclose()
            assert ustate.post_startup_frames == baseline
            with pytest.raises(InterfaceError):
                await anext(s_unopened)
            assert (await c.query("SELECT 1")).rows == ((1,),)
        finally:
            await c.close()

    with _running_sync_arrow_peer(unopened_peer) as (uport, ustate):
        asyncio.run(_exercise_unopened_and_resume(uport, ustate))

    resume_peer = _SyncArrowPeerState(portal_mode="big_resume")

    async def _exercise_resume(rport: int, rstate: _SyncArrowPeerState) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=rport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        b1: Any = None
        b2: Any = None
        try:
            async with c.stream_arrow_batches(
                "SELECT a, b FROM big_rows",
                batch_size=10,
                columns=["x", "x"],
            ) as stream:
                b1 = await anext(stream)
                assert b1.row_count == 2
                assert len(rstate.execute_frames) == 1
                rstate.allow_tail_complete.set()
                b2 = await anext(stream)
                assert b2.row_count == 1
                assert len(rstate.execute_frames) == 1
                sc2, tb2 = _decode_ipc_table(b2.buffer_view())
                assert [f.name for f in sc2] == ["x", "x"]
                assert tb2.column(0)[0].as_py() == "c" * (1792 * 1024)
                with pytest.raises(StopAsyncIteration):
                    await anext(stream)
        finally:
            if b1 is not None:
                b1.release()
            if b2 is not None:
                b2.release()
            await c.close()

    with _running_sync_arrow_peer(resume_peer) as (rport, rstate):
        asyncio.run(_exercise_resume(rport, rstate))
    assert rstate.events.count("Q:BEGIN") == 1
    assert rstate.events.count("Q:COMMIT") == 1

    mid_peer = _SyncArrowPeerState(portal_mode="big_stall_cancel")

    async def _exercise_mid_aclose(mport: int, mstate: _SyncArrowPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=mport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        cur1: Any = None
        mv1: memoryview | None = None
        retained_tbl: pa.Table | None = None
        tbl_after: pa.Table | None = None
        try:
            c1 = await pool.acquire()
            try:
                stream: Any = c1.stream_arrow_batches(
                    "SELECT a, b FROM stalled_big_rows",
                    batch_size=10,
                    columns=["x", "x"],
                )
                cur1 = await anext(stream)
                assert cur1.row_count == 2
                assert len(mstate.execute_frames) == 1
                mv1 = cur1.buffer_view()
                retained_tbl = pa.ipc.open_stream(pa.py_buffer(mv1)).read_all()

                close_aw = stream.aclose()
                with pytest.raises(InterfaceError):
                    stream.__anext__()
                await close_aw
                await stream.aclose()
                with pytest.raises(InterfaceError):
                    await anext(stream)
                del stream
            finally:
                await c1.close()

            assert mstate.events.count("Q:BEGIN") == 1
            assert mstate.cancels == [(9001, 77001)]
            assert mv1 is not None
            assert retained_tbl is not None
            assert bytes(mv1[:4]) != b""
            sc_after, tbl_after = _decode_ipc_table(mv1)
            assert [f.name for f in sc_after] == ["x", "x"]
            assert tbl_after.num_rows == 2
            assert retained_tbl.num_rows == 2
            assert len(retained_tbl.column(0)[0].as_py()) == 1792 * 1024
            assert retained_tbl.column(1)[0].as_py()[:4] == "yyyy"

            c2 = await pool.acquire()
            try:
                assert (await c2.query("SELECT 1")).rows == ((1,),)
            finally:
                await c2.close()
        finally:
            retained_tbl = None
            tbl_after = None
            if mv1 is not None:
                mv1.release()
            if cur1 is not None:
                cur1.release()
            await pool.close()

    with _running_sync_arrow_peer(mid_peer) as (mport, mstate):
        asyncio.run(_exercise_mid_aclose(mport, mstate))

    amb_peer = _SyncArrowPeerState(portal_mode="scope_tracking")

    async def _exercise_ambient(aport: int) -> None:
        c = AsyncPostgresClient(
            host="127.0.0.1",
            port=aport,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
        )
        await c.connect()
        cur_amb: Any = None
        try:
            await c.begin()
            await c.query("INSERT INTO audit VALUES (1)")
            async with c.stream_arrow_batches("SELECT payload FROM items", batch_size=1) as stream:
                cur_amb = await anext(stream)
                assert cur_amb.row_count == 1
            assert (await c.query("SELECT 1")).rows == ((1,),)
            await c.commit()
            assert (await c.query("SELECT 1")).rows == ((1,),)
        finally:
            if cur_amb is not None:
                cur_amb.release()
            await c.close()

    with _running_sync_arrow_peer(amb_peer) as (aport, astate):
        asyncio.run(_exercise_ambient(aport))

    assert astate.events.count("Q:BEGIN") == 1
    assert "EXEC:INSERT_AUDIT" in astate.events
    savepoints = [ev for ev in astate.events if ev.startswith("Q:SAVEPOINT vtb_sp_")]
    rollbacks_sp = [ev for ev in astate.events if ev.startswith("Q:ROLLBACK TO SAVEPOINT vtb_sp_")]
    releases_sp = [ev for ev in astate.events if ev.startswith("Q:RELEASE SAVEPOINT vtb_sp_")]
    assert len(savepoints) == 1
    assert len(rollbacks_sp) == 1
    assert len(releases_sp) == 1
    assert "Q:ROLLBACK" not in astate.events
    assert astate.events.count("Q:COMMIT") == 1


@pytest.mark.parametrize("failure_mode", ("columns_mismatch", "oversized_row"))
def test_async_arrow_stream_columns_mismatch_and_oversized_row_recover_pool(
    failure_mode: str,
) -> None:
    """Verify async stream_arrow_batches columns mismatch and >8 MiB row raise DataError, preserve zero leaked SHM, and recover pool."""
    portal_mode = "big_resume" if failure_mode == "columns_mismatch" else "oversized_row"
    peer_state = _SyncArrowPeerState(portal_mode=portal_mode)
    baseline_shm = _process_arrow_shm_files()

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            c1 = await pool.acquire()
            try:
                cols = ["only_one"] if failure_mode == "columns_mismatch" else None
                async with c1.stream_arrow_batches("SELECT a FROM rows", batch_size=4, columns=cols) as stream:
                    with pytest.raises(DataError):
                        await anext(stream)
                    with pytest.raises(InterfaceError):
                        await anext(stream)
                if failure_mode == "columns_mismatch":
                    assert "Q:ROLLBACK" in state.events
            finally:
                await c1.close()

            c2 = await pool.acquire()
            try:
                assert (await c2.query("SELECT 1")).rows == ((1,),)
            finally:
                await c2.close()
        finally:
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))
    assert _process_arrow_shm_files() - baseline_shm == set()


@pytest.mark.parametrize(
    "cancel_stage",
    ("open_bind", "execute_in_flight", "queued_task_cancel", "queued_future_drop"),
)
def test_async_arrow_stream_cancellation_open_execute_and_queued_drainer_zero_shm_leak(
    cancel_stage: str,
) -> None:
    """Verify task cancellation during Bind, in-flight Execute, and paused completion-drainer publication leaks 0 SHM files or pool leases."""
    peer_state = _SyncArrowPeerState(
        portal_mode="suspended_barrier",
        hold_portal_bind=(cancel_stage == "open_bind"),
        stall_execute_until_cancel=(cancel_stage == "execute_in_flight"),
    )
    baseline_shm = _process_arrow_shm_files()

    async def _run(port: int, state: _SyncArrowPeerState) -> None:
        loop = asyncio.get_running_loop()
        pool = AsyncPostgresPool(
            host="127.0.0.1",
            port=port,
            user="postgres",
            database="appdb",
            sslmode="disable",
            channel_binding="disable",
            connect_timeout=0.3,
            min_size=0,
            max_size=1,
        )
        try:
            ac1 = await pool.acquire()
            try:
                stream = ac1.stream_arrow_batches(
                    "SELECT fn_col, ts_col FROM portal_items",
                    batch_size=1,
                )
                if cancel_stage == "open_bind":
                    t_open = asyncio.create_task(_as_coro(anext(stream)))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.bind_waiting.wait, 2.0)
                    t_open.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_open
                elif cancel_stage == "execute_in_flight":
                    cur1 = await anext(stream)
                    cur1.release()
                    t_exec = asyncio.create_task(_as_coro(anext(stream)))
                    await asyncio.sleep(0)
                    assert await asyncio.to_thread(state.batch2_waiting.wait, 2.0)
                    t_exec.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t_exec
                elif cancel_stage == "queued_task_cancel":
                    drainer = getattr(loop, "_vertebra_postgres_completion_drainer", None)
                    assert drainer is not None
                    drainer_fd = _find_loop_drainer_fd(loop, drainer)
                    loop.remove_reader(drainer_fd)
                    reader_restored = False
                    try:
                        t_queued = asyncio.create_task(_as_coro(anext(stream)))
                        await asyncio.sleep(0)
                        assert await asyncio.to_thread(state.batch1_sent.wait, 2.0)
                        ready, _, _ = await asyncio.to_thread(select.select, [drainer_fd], [], [], 2.0)
                        assert ready == [drainer_fd]
                        assert len(_process_arrow_shm_files() - baseline_shm) == 1

                        t_queued.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await t_queued
                        assert _process_arrow_shm_files() - baseline_shm == set()

                        drained_ev = asyncio.Event()
                        drainer_cb: Any = drainer

                        def _drain_once_and_restore() -> None:
                            drainer_cb()
                            loop.add_reader(drainer_fd, drainer_cb)
                            drained_ev.set()

                        loop.add_reader(drainer_fd, _drain_once_and_restore)
                        reader_restored = True
                        await asyncio.wait_for(drained_ev.wait(), timeout=2.0)
                    finally:
                        if not reader_restored:
                            loop.add_reader(drainer_fd, drainer)
                else:
                    drainer = getattr(loop, "_vertebra_postgres_completion_drainer", None)
                    assert drainer is not None
                    drainer_fd = _find_loop_drainer_fd(loop, drainer)
                    loop.remove_reader(drainer_fd)
                    reader_restored = False
                    aw = stream.__anext__()
                    await_iter = aw.__await__()
                    try:
                        py_fut = next(await_iter)
                        assert await asyncio.to_thread(state.batch1_sent.wait, 2.0)
                        ready, _, _ = await asyncio.to_thread(select.select, [drainer_fd], [], [], 2.0)
                        assert ready == [drainer_fd]
                        assert len(_process_arrow_shm_files() - baseline_shm) == 1

                        py_fut.cancel()
                        drainer()
                        assert _process_arrow_shm_files() - baseline_shm == set()

                        loop.add_reader(drainer_fd, drainer)
                        reader_restored = True
                    finally:
                        await_iter.close()
                        if not reader_restored:
                            loop.add_reader(drainer_fd, drainer)

                with pytest.raises(InterfaceError):
                    await anext(stream)
            finally:
                await ac1.close()

            assert _process_arrow_shm_files() - baseline_shm == set()

            ac2 = await pool.acquire()
            try:
                assert (await ac2.query("SELECT 1")).rows == ((1,),)
            finally:
                await ac2.close()
        finally:
            await pool.close()

    with _running_sync_arrow_peer(peer_state) as (port, state):
        asyncio.run(_run(port, state))

    assert _process_arrow_shm_files() - baseline_shm == set()
    if cancel_stage in ("open_bind", "execute_in_flight"):
        assert len(state.cancels) >= 1
    else:
        assert "Q:ROLLBACK" in state.events
        assert any(ev.startswith("Close(P:") for ev in state.events)


def _capsule_name(capsule: object) -> bytes | None:
    """Return the C name of a `PyCapsule` via `PyCapsule_GetName`."""
    get_name = ctypes.pythonapi.PyCapsule_GetName
    get_name.argtypes = [ctypes.py_object]
    get_name.restype = ctypes.c_char_p
    raw: bytes | None = get_name(capsule)
    return raw


def _serialize_multi_batch_ipc(batches: list[list[tuple[int, str]]]) -> bytes:
    """Serialize zero or more `(id, label)` record batches into a single Arrow IPC stream."""
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for rows in batches:
            batch = pa.record_batch(
                {
                    "id": pa.array([r[0] for r in rows], type=pa.int64()),
                    "label": pa.array([r[1] for r in rows], type=pa.string()),
                },
                schema=schema,
            )
            writer.write_batch(batch)
    return bytes(sink.getvalue().to_pybytes())


@pytest.mark.parametrize(
    "batch_groups",
    [
        [],
        [[(1, "alpha")]],
        [[(1, "alpha"), (2, "beta")], [(3, "gamma")]],
    ],
)
@pytest.mark.parametrize(
    "cursor_kind",
    [
        "native_shm",
        "native_bytes",
        "py_shm_wrapper",
        "py_raw_bytes",
        "read_arrow_shm_raw",
        "read_arrow_shm_vtbarrow",
    ],
)
def test_arrow_c_schema_and_stream_without_pyarrow(
    batch_groups: list[list[tuple[int, str]]],
    cursor_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SHM and raw-bytes cursors export Arrow C Schema/Stream capsules and decode via Polars, DuckDB, and dicts without pyarrow."""
    ipc_bytes = _serialize_multi_batch_ipc(batch_groups)
    flat_rows = [row for grp in batch_groups for row in grp]
    expected_dicts = [{"id": r[0], "label": r[1]} for r in flat_rows]
    vtbarrow_frame = b"VTBARROW" + struct.pack("<Q", len(ipc_bytes)) + ipc_bytes

    duckdb_mod = import_optional("duckdb")
    polars_mod = import_optional("polars")

    native_shm: native_vertebra.ArrowCursor | None = None
    if cursor_kind in ("native_shm", "py_shm_wrapper"):
        native_shm = native_vertebra.ArrowCursor.write_ipc_to_shm(
            ArrowCursor._next_shm_segment_name(),
            ipc_bytes,
            len(flat_rows),
        )

    cursor: Any = None
    try:
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        reset_dependency_cache("pyarrow")
        try:
            if cursor_kind == "native_shm":
                assert native_shm is not None
                cursor = native_shm
            elif cursor_kind == "native_bytes":
                cursor = native_vertebra.ArrowCursor.from_ipc_bytes(memoryview(ipc_bytes))
            elif cursor_kind == "py_shm_wrapper":
                assert native_shm is not None
                cursor = ArrowCursor(native_shm)
            elif cursor_kind == "py_raw_bytes":
                cursor = ArrowCursor(ipc_bytes)
            elif cursor_kind == "read_arrow_shm_raw":
                cursor = read_arrow_shm(ipc_bytes)
            else:
                cursor = read_arrow_shm(vtbarrow_frame)

            assert cursor.row_count == len(flat_rows)
            if isinstance(cursor, ArrowCursor):
                assert len(cursor) == len(flat_rows)
                assert arrow_module._table_to_rows_and_cols(cursor) == (flat_rows, ("id", "label"))
            assert cursor.columns == ["id", "label"]

            schema_cap = cursor.__arrow_c_schema__()
            stream_cap = cursor.__arrow_c_stream__()
            assert _capsule_name(schema_cap) == b"arrow_schema"
            assert _capsule_name(stream_cap) == b"arrow_array_stream"
            del schema_cap
            del stream_cap

            matching_schema_cap = cursor.__arrow_c_schema__()
            matched_stream_cap = cursor.__arrow_c_stream__(matching_schema_cap)
            assert _capsule_name(matched_stream_cap) == b"arrow_array_stream"
            del matched_stream_cap
            del matching_schema_cap

            assert cursor.to_pylist() == expected_dicts

            if polars_mod is not None:
                df = cursor.to_polars()
                assert df.columns == ["id", "label"]
                assert df.to_dicts() == expected_dicts

            if duckdb_mod is not None:
                con = duckdb_mod.connect(":memory:")
                try:
                    assert con.from_arrow(cursor).fetchall() == flat_rows
                finally:
                    con.close()
        finally:
            monkeypatch.undo()
            reset_dependency_cache("pyarrow")
    finally:
        if cursor is not None and hasattr(cursor, "release"):
            cursor.release()
        if native_shm is not None:
            native_shm.release()


def test_native_from_ipc_bytes_rich_types_schema_rejection_and_corrupt_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify from_ipc_bytes and pure-Rust to_pylist handle nested/temporal/decimal/dictionary types and reject bad schemas."""
    dict_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None], type=pa.int8()),
        pa.array(["open", "closed"], type=pa.string()),
    )
    struct_arr = pa.StructArray.from_arrays(
        [pa.array([10, 20, None], type=pa.int32()), pa.array(["x", "y", "z"], type=pa.string())],
        names=["k", "v"],
    )
    table = pa.table(
        {
            "flag": pa.array([True, False, None], type=pa.bool_()),
            "num": pa.array([100, -200, None], type=pa.int64()),
            "ratio": pa.array([1.5, -2.25, None], type=pa.float64()),
            "raw": pa.array([b"ab", b"", None], type=pa.binary()),
            "amt": pa.array([decimal.Decimal("12.34"), decimal.Decimal("-0.05"), None], type=pa.decimal128(10, 2)),
            "day": pa.array([datetime.date(2026, 5, 1), datetime.date(1970, 1, 1), None], type=pa.date32()),
            "tod": pa.array([datetime.time(14, 30, 5, 123456), datetime.time(0, 0, 0), None], type=pa.time64("us")),
            "ts": pa.array(
                [
                    datetime.datetime(2026, 5, 1, 12, 0, 0, tzinfo=datetime.UTC),
                    datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
                    None,
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "tags": pa.array([[1, 2], [], None], type=pa.list_(pa.int64())),
            "meta": struct_arr,
            "state": dict_arr,
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    ipc_bytes = bytes(sink.getvalue().to_pybytes())
    expected = table.to_pylist()

    other_schema = pa.schema([("flag", pa.int32())])
    other_schema_cap = other_schema.__arrow_c_schema__()

    monkeypatch.setitem(sys.modules, "pyarrow", None)
    reset_dependency_cache("pyarrow")
    try:
        native_cur = native_vertebra.ArrowCursor.from_ipc_bytes(bytearray(ipc_bytes))
        assert native_cur.shm_name == ""
        assert native_cur.byte_len == len(ipc_bytes)
        assert native_cur.row_count == 3
        assert native_cur.columns == list(table.column_names)
        assert native_cur.read_bytes() == ipc_bytes
        assert bytes(native_cur.buffer_view()) == ipc_bytes
        assert native_cur.to_pylist() == expected

        wrapper = ArrowCursor(ipc_bytes)
        assert wrapper._native_ipc_cursor is None
        assert wrapper.to_pylist() == expected
        assert wrapper._native_ipc_cursor is not None

        with pytest.raises(NotImplementedError):
            native_cur.__arrow_c_stream__(other_schema_cap)
        with pytest.raises(NotImplementedError):
            wrapper.__arrow_c_stream__(other_schema_cap)
        with pytest.raises(ValueError, match="arrow_schema"):
            native_cur.__arrow_c_stream__("not-a-capsule")

        from_ipc_any: Any = native_vertebra.ArrowCursor.from_ipc_bytes
        with pytest.raises(TypeError):
            from_ipc_any("not-a-buffer")
        with pytest.raises(ValueError, match="Arrow IPC"):
            native_vertebra.ArrowCursor.from_ipc_bytes(b"not-arrow")
        with pytest.raises(ValueError, match="Arrow IPC"):
            native_vertebra.ArrowCursor.from_ipc_bytes(ipc_bytes[: len(ipc_bytes) // 2])
        with pytest.raises(DataError):
            read_arrow_shm(b"not-arrow")
        with pytest.raises(DataError):
            read_arrow_shm(ipc_bytes[: len(ipc_bytes) // 2])
    finally:
        monkeypatch.undo()
        reset_dependency_cache("pyarrow")


@pytest.mark.parametrize(
    "cursor_kind",
    [
        "native_shm",
        "native_bytes",
        "py_shm_wrapper",
        "py_raw_bytes",
        "read_arrow_shm",
    ],
)
def test_arrow_cursor_lazy_zero_copy_and_post_release_errors(
    cursor_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify lazy zero-copy buffer identity before release and ValueError/BufferError on operations after release()."""
    ipc_bytes = _serialize_multi_batch_ipc([[(10, "x"), (20, "y")]])
    if cursor_kind == "py_raw_bytes":
        lazy_cur = ArrowCursor(ipc_bytes, rows=[(10, "x"), (20, "y")], columns=("id", "label"))
        assert lazy_cur._native_ipc_cursor is None
        assert pa.py_buffer(lazy_cur.buffer_view()).address == pa.py_buffer(ipc_bytes).address
        assert lazy_cur.columns == ["id", "label"]
        assert len(lazy_cur) == 2
        assert lazy_cur._native_ipc_cursor is None

    native_shm: native_vertebra.ArrowCursor | None = None
    if cursor_kind in ("native_shm", "py_shm_wrapper"):
        native_shm = native_vertebra.ArrowCursor.write_ipc_to_shm(
            ArrowCursor._next_shm_segment_name(),
            ipc_bytes,
            2,
        )

    cursor: Any
    if cursor_kind == "native_shm":
        assert native_shm is not None
        cursor = native_shm
    elif cursor_kind == "native_bytes":
        cursor = native_vertebra.ArrowCursor.from_ipc_bytes(ipc_bytes)
    elif cursor_kind == "py_shm_wrapper":
        assert native_shm is not None
        cursor = ArrowCursor(native_shm)
    elif cursor_kind == "py_raw_bytes":
        cursor = ArrowCursor(ipc_bytes)
    else:
        cursor = read_arrow_shm(ipc_bytes)

    retained_mv = cursor.buffer_view()
    try:
        first_release = cursor.release()
        if cursor_kind in ("native_shm", "native_bytes", "py_shm_wrapper"):
            assert first_release is True
        assert cursor.release() is False
        assert bytes(retained_mv) == ipc_bytes

        monkeypatch.setitem(sys.modules, "pyarrow", None)
        reset_dependency_cache("pyarrow")
        try:
            with pytest.raises(BufferError, match="released"):
                cursor.buffer_view()
            with pytest.raises(BufferError, match="released"):
                memoryview(cursor)
            with pytest.raises(ValueError, match="released"):
                cursor.__arrow_c_schema__()
            with pytest.raises(ValueError, match="released"):
                cursor.__arrow_c_stream__()
            with pytest.raises(ValueError, match="released"):
                _ = cursor.columns
            with pytest.raises(ValueError, match="released"):
                cursor.to_pylist()
            if import_optional("polars") is not None:
                with pytest.raises(ValueError, match="released"):
                    cursor.to_polars()
            if hasattr(cursor, "to_pyarrow"):
                with pytest.raises((BufferError, ValueError), match="released"):
                    cursor.to_pyarrow()
            if hasattr(cursor, "read_bytes"):
                with pytest.raises(ValueError, match="released"):
                    cursor.read_bytes()
        finally:
            monkeypatch.undo()
            reset_dependency_cache("pyarrow")
    finally:
        retained_mv.release()
        if native_shm is not None:
            native_shm.release()


@pytest.mark.parametrize(
    "index_type_name",
    ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"],
)
def test_arrow_cursor_dictionary_array_to_pylist_roundtrip(index_type_name: str) -> None:
    """Native ArrowCursor.to_pylist decodes signed and unsigned dictionary-encoded columns."""
    pa = pytest.importorskip("pyarrow")
    index_type = getattr(pa, index_type_name)()
    dict_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0], type=index_type),
        pa.array(["primary", "replica"]),
    )
    table = pa.table({"role": dict_arr})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    native_cursor = native_vertebra.ArrowCursor.from_ipc_bytes(sink.getvalue().to_pybytes())
    try:
        assert native_cursor.to_pylist() == [
            {"role": "primary"},
            {"role": "replica"},
            {"role": None},
            {"role": "primary"},
        ]
    finally:
        native_cursor.release()


_ARROW_FUZZ_SEED = 20261119
_ARROW_FUZZ_MUTATION_COUNT = 300
_ARROW_FUZZ_ALLOWED_ERRORS = (ValueError, DataError, BufferError, NotImplementedError)
_ARROW_RELEASED_SCHEMA_MESSAGE = "requested_schema arrow_schema PyCapsule has already been released"
_ARROW_SCHEMA_RELEASE_OFFSET = (
    3 * ctypes.sizeof(ctypes.c_void_p) + 2 * ctypes.sizeof(ctypes.c_int64) + 2 * ctypes.sizeof(ctypes.c_void_p)
)
_ARROW_FUZZ_SUBPROCESS_SCRIPT = """
import json
import sys

import pyarrow as pa

import vertebra._vertebra as native_vertebra
from vertebra.db.arrow import ArrowCursor

consumer, source = sys.argv[1], sys.argv[2]
spec = json.load(sys.stdin)
valid = bytes.fromhex(spec["payload"])
failures = []
for offset, value in spec["mutations"]:
    payload = bytearray(valid)
    payload[offset] = value
    cursor = None
    try:
        if source == "bytes":
            cursor = native_vertebra.ArrowCursor.from_ipc_bytes(bytes(payload))
        else:
            cursor = native_vertebra.ArrowCursor.write_ipc_to_shm(
                ArrowCursor._next_shm_segment_name(), bytes(payload), 0
            )
        if consumer == "pyarrow_stream":
            pa.RecordBatchReader.from_stream(cursor).read_all()
        else:
            cursor.to_polars()
    except Exception as exc:
        if consumer == "pyarrow_stream" and not isinstance(exc, (ValueError, NotImplementedError)):
            failures.append([offset, value, type(exc).__name__, str(exc)[:200]])
    except BaseException as exc:
        failures.append([offset, value, type(exc).__name__, str(exc)[:200]])
    finally:
        if cursor is not None:
            cursor.release()
json.dump(failures, sys.stdout)
"""


def _build_arrow_fuzz_ipc() -> bytes:
    """Serialize a two-batch Arrow IPC stream covering primitive, string, list, struct, and dictionary columns."""
    table = pa.table(
        {
            "id": pa.array([1, 2, None, 4], type=pa.int64()),
            "label": pa.array(["a", None, "ccc", "dddd"], type=pa.string()),
            "flag": pa.array([True, False, None, True], type=pa.bool_()),
            "score": pa.array([1.5, 2.5, 3.5, None], type=pa.float64()),
            "tags": pa.array([[1, 2], None, [], [3]], type=pa.list_(pa.int32())),
            "point": pa.array(
                [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}, {"x": 4, "y": "d"}],
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
            "role": pa.array(["primary", "replica", None, "primary"]).dictionary_encode(),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=2)
    return bytes(sink.getvalue().to_pybytes())


def _arrow_ipc_metadata_offsets(payload: bytes) -> list[int]:
    """Return every byte offset that belongs to an encapsulated message prefix or flatbuffer metadata block."""
    offsets: list[int] = []
    pos = 0
    while pos + 8 <= len(payload):
        meta_len = struct.unpack_from("<i", payload, pos + 4)[0]
        if payload[pos : pos + 4] != b"\xff\xff\xff\xff" or meta_len <= 0:
            break
        meta_end = pos + 8 + meta_len
        offsets.extend(range(pos, meta_end))
        body_len = _message_body_length(payload[pos + 8 : meta_end])
        pos = meta_end + body_len
    return offsets


def _message_body_length(metadata: bytes) -> int:
    """Decode the `bodyLength` field (vtable slot 3) of an Arrow IPC `Message` flatbuffer."""
    table_pos = struct.unpack_from("<I", metadata, 0)[0]
    vtable_pos = table_pos - struct.unpack_from("<i", metadata, table_pos)[0]
    vtable_len = struct.unpack_from("<H", metadata, vtable_pos)[0]
    slot = 4 + 2 * 3
    if slot + 2 > vtable_len:
        return 0
    field_offset = struct.unpack_from("<H", metadata, vtable_pos + slot)[0]
    if field_offset == 0:
        return 0
    return int(struct.unpack_from("<q", metadata, table_pos + field_offset)[0])


def _arrow_fuzz_mutations(payload: bytes) -> list[tuple[int, int]]:
    """Return the fixed seeded single-byte mutations: two thirds target IPC metadata, one third any byte."""
    rng = random.Random(_ARROW_FUZZ_SEED)
    metadata_offsets = _arrow_ipc_metadata_offsets(payload)
    mutations: list[tuple[int, int]] = []
    while len(mutations) < _ARROW_FUZZ_MUTATION_COUNT:
        targeted = len(mutations) % 3 != 2
        offset = rng.choice(metadata_offsets) if targeted else rng.randrange(len(payload))
        mutations.append((offset, payload[offset] ^ rng.randrange(1, 256)))
    return mutations


_ARROW_FUZZ_VALID_IPC = _build_arrow_fuzz_ipc()
_ARROW_FUZZ_MUTATIONS = _arrow_fuzz_mutations(_ARROW_FUZZ_VALID_IPC)


def _mutated_arrow_fuzz_cursor(source: str, offset: int, value: int) -> native_vertebra.ArrowCursor:
    """Build a native cursor over the fuzz stream with one byte replaced."""
    payload = bytearray(_ARROW_FUZZ_VALID_IPC)
    payload[offset] = value
    if source == "bytes":
        return native_vertebra.ArrowCursor.from_ipc_bytes(bytes(payload))
    return native_vertebra.ArrowCursor.write_ipc_to_shm(ArrowCursor._next_shm_segment_name(), bytes(payload), 0)


def _release_schema_capsule_via_pyarrow(capsule: object) -> None:
    """Consume an `arrow_schema` capsule by importing it into pyarrow."""
    pa.Schema._import_from_c_capsule(capsule)


def _release_schema_capsule_via_callback(capsule: object) -> None:
    """Consume an `arrow_schema` capsule by invoking its C `release` callback."""
    get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    get_pointer.restype = ctypes.c_void_p
    schema_ptr = get_pointer(capsule, b"arrow_schema")
    release_ptr = ctypes.c_void_p.from_address(schema_ptr + _ARROW_SCHEMA_RELEASE_OFFSET).value
    assert release_ptr is not None
    ctypes.CFUNCTYPE(None, ctypes.c_void_p)(release_ptr)(schema_ptr)
    assert ctypes.c_void_p.from_address(schema_ptr + _ARROW_SCHEMA_RELEASE_OFFSET).value is None


def test_arrow_fuzz_seed_stream_is_valid_and_mutations_are_fixed() -> None:
    """Verify the unmutated fuzz stream decodes and the seeded mutation set is stable and metadata-heavy."""
    cursor = native_vertebra.ArrowCursor.from_ipc_bytes(_ARROW_FUZZ_VALID_IPC)
    try:
        assert cursor.row_count == 4
        assert cursor.columns == ["id", "label", "flag", "score", "tags", "point", "role"]
        assert [row["id"] for row in cursor.to_pylist()] == [1, 2, None, 4]
    finally:
        cursor.release()
    metadata_offsets = set(_arrow_ipc_metadata_offsets(_ARROW_FUZZ_VALID_IPC))
    assert len(_ARROW_FUZZ_MUTATIONS) == _ARROW_FUZZ_MUTATION_COUNT
    assert _arrow_fuzz_mutations(_ARROW_FUZZ_VALID_IPC) == _ARROW_FUZZ_MUTATIONS
    assert sum(offset in metadata_offsets for offset, _ in _ARROW_FUZZ_MUTATIONS) >= 200
    assert all(_ARROW_FUZZ_VALID_IPC[offset] != value for offset, value in _ARROW_FUZZ_MUTATIONS)


@pytest.mark.parametrize("source", ["bytes", "shm"])
@pytest.mark.parametrize(
    "entry_point",
    ["columns", "__arrow_c_schema__", "__arrow_c_stream__", "to_pylist"],
)
@pytest.mark.parametrize(("offset", "value"), _ARROW_FUZZ_MUTATIONS)
def test_arrow_cursor_mutated_ipc_never_panics_in_process(
    source: str,
    entry_point: str,
    offset: int,
    value: int,
) -> None:
    """Verify single-byte IPC mutations make each in-process decode entry point succeed or raise a mapped error."""
    cursor: native_vertebra.ArrowCursor | None = None
    try:
        cursor = _mutated_arrow_fuzz_cursor(source, offset, value)
        attr = getattr(cursor, entry_point)
        if callable(attr):
            attr()
    except _ARROW_FUZZ_ALLOWED_ERRORS:
        pass
    except BaseException as exc:
        pytest.fail(f"{entry_point} on {source} cursor raised {type(exc).__name__}: {exc}")
    finally:
        if cursor is not None:
            cursor.release()


@pytest.mark.parametrize("source", ["bytes", "shm"])
@pytest.mark.parametrize("consumer", ["pyarrow_stream", "polars"])
def test_arrow_cursor_mutated_ipc_c_stream_consumers_never_abort(source: str, consumer: str) -> None:
    """Verify C-stream consumers survive every seeded IPC mutation in a subprocess with exit code zero."""
    if consumer == "polars" and import_optional("polars") is None:
        pytest.skip("polars is not installed")
    spec = {"payload": _ARROW_FUZZ_VALID_IPC.hex(), "mutations": _ARROW_FUZZ_MUTATIONS}
    proc = subprocess.run(
        [sys.executable, "-c", _ARROW_FUZZ_SUBPROCESS_SCRIPT, consumer, source],
        input=json.dumps(spec),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert json.loads(proc.stdout) == []


@pytest.mark.parametrize(
    "consume",
    [_release_schema_capsule_via_pyarrow, _release_schema_capsule_via_callback],
    ids=["pyarrow_import", "release_callback"],
)
@pytest.mark.parametrize("source", ["bytes", "shm"])
def test_arrow_c_stream_rejects_released_requested_schema_capsule(
    source: str,
    consume: Any,
) -> None:
    """Verify a consumed `arrow_schema` capsule is rejected by its null `release` without decoding the struct."""
    ipc_bytes = _serialize_multi_batch_ipc([[(1, "alpha")]])
    if source == "bytes":
        cursor = native_vertebra.ArrowCursor.from_ipc_bytes(ipc_bytes)
    else:
        cursor = native_vertebra.ArrowCursor.write_ipc_to_shm(ArrowCursor._next_shm_segment_name(), ipc_bytes, 1)
    try:
        live_capsule = cursor.__arrow_c_schema__()
        assert _capsule_name(cursor.__arrow_c_stream__(live_capsule)) == b"arrow_array_stream"
        consumed_capsule = cursor.__arrow_c_schema__()
        consume(consumed_capsule)
        with pytest.raises(ValueError) as excinfo:
            cursor.__arrow_c_stream__(consumed_capsule)
        assert str(excinfo.value) == _ARROW_RELEASED_SCHEMA_MESSAGE
        assert cursor.to_pylist() == [{"id": 1, "label": "alpha"}]
    finally:
        cursor.release()


def _serialize_record_batches(batches: list[pa.RecordBatch]) -> bytes:
    """Serialize one or more PyArrow RecordBatches into an Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return bytes(sink.getvalue().to_pybytes())


def _patch_first_record_batch(
    payload: bytes,
    *,
    batch_length: int | None = None,
    node_updates: dict[int, tuple[int, int]] | None = None,
) -> bytes:
    """Patch the first RecordBatch FlatBuffer length and optional FieldNode entries in place."""
    buf = bytearray(payload)
    pos = 0
    while pos + 8 <= len(buf):
        meta_len = struct.unpack_from("<i", buf, pos + 4)[0]
        if buf[pos : pos + 4] != b"\xff\xff\xff\xff" or meta_len <= 0:
            break
        meta_start = pos + 8
        meta_end = meta_start + meta_len
        table_pos = meta_start + struct.unpack_from("<I", buf, meta_start)[0]
        vtable_pos = table_pos - struct.unpack_from("<i", buf, table_pos)[0]
        vtable_len = struct.unpack_from("<H", buf, vtable_pos)[0]
        header_type_slot = 6
        header_slot = 8
        header_type = 0
        if header_type_slot + 2 <= vtable_len:
            ht_off = struct.unpack_from("<H", buf, vtable_pos + header_type_slot)[0]
            if ht_off != 0:
                header_type = buf[table_pos + ht_off]
        if header_type == 3:
            assert header_slot + 2 <= vtable_len
            hdr_off = struct.unpack_from("<H", buf, vtable_pos + header_slot)[0]
            assert hdr_off != 0
            hdr_ref = table_pos + hdr_off
            rb_pos = hdr_ref + struct.unpack_from("<I", buf, hdr_ref)[0]
            rb_vtable = rb_pos - struct.unpack_from("<i", buf, rb_pos)[0]
            rb_vtable_len = struct.unpack_from("<H", buf, rb_vtable)[0]
            if batch_length is not None:
                assert rb_vtable_len >= 4 + 2
                len_off = struct.unpack_from("<H", buf, rb_vtable + 4)[0]
                assert len_off != 0
                struct.pack_into("<q", buf, rb_pos + len_off, batch_length)
            if node_updates:
                assert rb_vtable_len >= 6 + 2
                nodes_off = struct.unpack_from("<H", buf, rb_vtable + 6)[0]
                assert nodes_off != 0
                nodes_ref = rb_pos + nodes_off
                vec_pos = nodes_ref + struct.unpack_from("<I", buf, nodes_ref)[0]
                vec_len = struct.unpack_from("<I", buf, vec_pos)[0]
                for idx, (node_len, null_count) in node_updates.items():
                    assert 0 <= idx < vec_len
                    struct.pack_into("<qq", buf, vec_pos + 4 + idx * 16, node_len, null_count)
            return bytes(buf)
        body_len = _message_body_length(bytes(buf[meta_start:meta_end]))
        pos = meta_end + body_len
    msg = "No RecordBatch message found in Arrow IPC payload"
    raise AssertionError(msg)


@pytest.mark.parametrize("kind", ["null", "zero_col"])
@pytest.mark.parametrize("source", ["bytes", "shm"])
def test_arrow_cursor_to_pylist_rejects_inflated_row_count(kind: str, source: str) -> None:
    """Verify inflated Null and zero-column batches fail to_pylist immediately with ValueError."""
    inflated = 0x4000_0000_0000_0000
    if kind == "null":
        batch = pa.record_batch([pa.array([None], type=pa.null())], names=["n"])
        payload = _patch_first_record_batch(
            _serialize_record_batches([batch]),
            batch_length=inflated,
            node_updates={0: (inflated, inflated)},
        )
    else:
        batch = pa.RecordBatch.from_pydict({"x": [1]}).select([])
        payload = _patch_first_record_batch(
            _serialize_record_batches([batch]),
            batch_length=inflated,
        )
    if source == "bytes":
        cursor = native_vertebra.ArrowCursor.from_ipc_bytes(payload)
    else:
        cursor = native_vertebra.ArrowCursor.write_ipc_to_shm(ArrowCursor._next_shm_segment_name(), payload, 0)
    try:
        started = time.monotonic()
        with pytest.raises(ValueError, match="maximum Python list size"):
            cursor.to_pylist()
        elapsed = time.monotonic() - started
        assert elapsed < 0.1
    finally:
        cursor.release()


@pytest.mark.parametrize(
    ("kind", "inflated_length"),
    [
        ("fixed_size_list", 0x4000_0000_0000_0000),
        ("fixed_size_binary", 0x1000_0000_0000_0000),
    ],
)
def test_arrow_cursor_rejects_fixed_size_overflow(kind: str, inflated_length: int) -> None:
    """Verify FixedSizeList and FixedSizeBinary length overflows raise ValueError on bytes and SHM cursors."""
    if kind == "fixed_size_list":
        arr = pa.array([[1, 2, 3, 4]], type=pa.list_(pa.int32(), 4))
    else:
        arr = pa.array([b"\x00" * 16], type=pa.binary(16))
    batch = pa.record_batch([arr], names=["col"])
    payload = _patch_first_record_batch(
        _serialize_record_batches([batch]),
        batch_length=inflated_length,
        node_updates={0: (inflated_length, 0)},
    )
    with pytest.raises(ValueError):
        native_vertebra.ArrowCursor.from_ipc_bytes(payload)

    shm_cursor = native_vertebra.ArrowCursor.write_ipc_to_shm(ArrowCursor._next_shm_segment_name(), payload, 0)
    try:
        with pytest.raises(ValueError):
            pa.RecordBatchReader.from_stream(shm_cursor).read_all()
        with pytest.raises(ValueError):
            shm_cursor.to_pylist()
    finally:
        shm_cursor.release()


def test_arrow_cursor_to_pylist_honors_keyboard_interrupt() -> None:
    """Verify to_pylist checks Python signals periodically so KeyboardInterrupt stops large streams promptly."""
    if not hasattr(signal, "setitimer") or not hasattr(signal, "SIGALRM"):
        pytest.skip("POSIX interval timers are required for async signal delivery while the GIL is held")
    row_count = 5_000_000
    batch = pa.record_batch([pa.array([None], type=pa.null())], names=["n"])
    payload = _patch_first_record_batch(
        _serialize_record_batches([batch]),
        batch_length=row_count,
        node_updates={0: (row_count, row_count)},
    )
    cursor = native_vertebra.ArrowCursor.from_ipc_bytes(payload)

    def _raise_keyboard_interrupt(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_keyboard_interrupt)
    try:
        started = time.monotonic()
        signal.setitimer(signal.ITIMER_REAL, 0.01)
        with pytest.raises(KeyboardInterrupt):
            cursor.to_pylist()
        elapsed = time.monotonic() - started
        assert elapsed < 2.0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        cursor.release()


@pytest.mark.parametrize("source", ["bytes", "shm"])
def test_arrow_cursor_repeated_cached_reads(source: str) -> None:
    """Verify bytes and SHM cursors return identical rows and schema across repeated stream and to_pylist reads."""
    ipc_bytes = _serialize_multi_batch_ipc([[(1, "alpha"), (2, "beta")], [(3, "gamma")]])
    if source == "bytes":
        cursor = native_vertebra.ArrowCursor.from_ipc_bytes(ipc_bytes)
    else:
        cursor = native_vertebra.ArrowCursor.write_ipc_to_shm(ArrowCursor._next_shm_segment_name(), ipc_bytes, 3)
    expected_rows = [
        {"id": 1, "label": "alpha"},
        {"id": 2, "label": "beta"},
        {"id": 3, "label": "gamma"},
    ]
    try:
        first_table = pa.RecordBatchReader.from_stream(cursor).read_all()
        for _ in range(3):
            table = pa.RecordBatchReader.from_stream(cursor).read_all()
            assert table.schema == first_table.schema
            assert table.to_pylist() == expected_rows
            assert cursor.to_pylist() == expected_rows
    finally:
        cursor.release()
