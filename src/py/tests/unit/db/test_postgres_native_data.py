"""Unit tests for native PostgreSQL Arrow results and validated ArrowCursor counts."""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import vertebra._vertebra as native_vertebra
from vertebra.db import arrow as arrow_module
from vertebra.db.adapters.postgres.types import PostgresArrowResult
from vertebra.db.arrow import ArrowCursor
from vertebra.typing import reset_dependency_cache


def _serialize_ipc_rows(rows: list[tuple[int, str]]) -> bytes:
    """Serialize `(id, label)` rows into an Arrow IPC stream with explicit schema."""
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    ids = [r[0] for r in rows]
    labels = [r[1] for r in rows]
    table = pa.Table.from_arrays([pa.array(ids, type=pa.int64()), pa.array(labels, type=pa.string())], schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        if rows:
            writer.write_table(table)
    return bytes(sink.getvalue())


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
    source = Path(arrow_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ] + [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    assert not any("postgres" in mod or "adapters" in mod for mod in imported)
