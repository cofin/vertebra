"""Shared zero-copy Arrow IPC and POSIX SHM (`VTBARROW`) cursor and helpers across database drivers."""

from __future__ import annotations

import contextlib
import mmap
import os
import threading
from collections.abc import Buffer, Mapping, Sequence
from typing import Any, Final, Self

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.schema import to_schema
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    encode_json_bytes,
)
from vertebra.utils.type_guards import get_msgspec_rename_config

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "ArrowCursor",
    "read_arrow_shm",
    "write_arrow_shm",
)

ARROW_SHM_THRESHOLD_ROWS: Final[int] = 256
_SHM_SEQ_LOCK = threading.Lock()
_SHM_SEQ_COUNTER = 0
_VTBARROW_MAGIC: Final[bytes] = b"VTBARROW"
_VTBARROW_HEADER_SIZE: Final[int] = 16


class ArrowCursor:
    """Zero-copy shared-memory Arrow IPC cursor for bulk analytical result sets.

    `_shm_anchor` pins the source region and memoryviews backing `_buffer` so Arrow buffers
    decoded over the payload never outlive the mapping, and `_ipc_table_cache` memoizes the
    `pyarrow.Table` decoded from the IPC stream so `len()`, `to_arrow_table()`, `to_pylist()`,
    and column-wise row extraction decode the stream at most once.
    """

    __slots__ = (
        "_buffer",
        "_columns",
        "_ipc_table_cache",
        "_mmap",
        "_rows",
        "_shm_anchor",
        "_shm_cursor",
        "_used_shm",
    )

    def __init__(
        self,
        buffer: Buffer | Any,
        *,
        rows: Sequence[tuple[Any, ...]] = (),
        columns: Sequence[str] = (),
        used_shm: bool = False,
        mmap_obj: mmap.mmap | None = None,
        shm_cursor: Any = None,
        shm_anchor: Any = None,
    ) -> None:
        if (
            shm_cursor is None
            and hasattr(buffer, "shm_name")
            and hasattr(buffer, "byte_len")
            and hasattr(buffer, "buffer_view")
        ):
            shm_cursor = buffer
            used_shm = True
        self._buffer = buffer
        self._rows = list(rows)
        self._columns = tuple(columns)
        self._used_shm = used_shm
        self._mmap = mmap_obj
        self._shm_cursor = shm_cursor
        self._shm_anchor = shm_anchor
        self._ipc_table_cache: Any = None

    def __len__(self) -> int:
        """Return the number of rows in the cursor without materializing row tuples."""
        if self._rows:
            return len(self._rows)
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "row_count"):
            rc = int(self._shm_cursor.row_count)
            if rc > 0:
                return rc
        ipc_table = self._ipc_table()
        return 0 if ipc_table is None else int(ipc_table.num_rows)

    def __buffer__(self, flags: int, /) -> memoryview:
        """Return a PEP 688 zero-copy memoryview over the serialized Arrow IPC payload."""
        return self.buffer_view()

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the Arrow C Stream PyCapsule (`arrow_array_stream`) for zero-copy consumers."""
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "__arrow_c_stream__"):
            return self._shm_cursor.__arrow_c_stream__(requested_schema=requested_schema)
        tbl = self.to_arrow_table()
        if tbl is not self and hasattr(tbl, "__arrow_c_stream__"):
            return tbl.__arrow_c_stream__(requested_schema=requested_schema)
        raise TypeError("ArrowCursor payload cannot be exported as an Arrow C Stream")

    def __enter__(self) -> Self:
        """Enter the context manager and return `self`."""
        return self

    def __exit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> None:
        """Release any underlying shared-memory or mmap resources upon exiting the context."""
        self.close()

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the cursor."""
        return len(self)

    @property
    def row_count(self) -> int:
        """Return the number of rows in the cursor."""
        return len(self)

    @property
    def used_shm(self) -> bool:
        """Return True when the payload was transferred over a shared-memory region."""
        return self._used_shm

    @property
    def shm_region(self) -> Any:
        """Return the underlying shared-memory region or mmap object when SHM transfer is active."""
        if self._shm_cursor is not None:
            return self._shm_cursor
        if self._mmap is not None:
            return self._mmap
        return self._buffer if self._used_shm else None

    def buffer_view(self) -> memoryview:
        """Return a zero-copy memoryview over the serialized Arrow IPC stream."""
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "buffer_view"):
            view: memoryview = self._shm_cursor.buffer_view()
            return view
        if self._mmap is not None:
            return memoryview(self._mmap)[: len(memoryview(self._buffer))]
        return memoryview(self._buffer)

    def to_pyarrow(self) -> Any:
        """Deserialize the Arrow IPC buffer into a `pyarrow.Table` with zero copy."""
        return self.to_arrow_table()

    def to_arrow_table(self) -> Any:
        """Deserialize the Arrow IPC buffer into a `pyarrow.Table` with zero copy."""
        ipc_table = self._ipc_table()
        if ipc_table is not None:
            return ipc_table
        pa = import_optional("pyarrow")
        if pa is not None:
            cols = self._columns or tuple(f"col_{i}" for i in range(len(self._rows[0]) if self._rows else 0))
            pydict = {col: [r[idx] if idx < len(r) else None for r in self._rows] for idx, col in enumerate(cols)}
            return pa.Table.from_pydict(pydict)
        return self

    def to_polars(self) -> Any:
        """Deserialize the Arrow IPC buffer into a `polars.DataFrame`."""
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "to_polars"):
            with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError, ImportError):
                return self._shm_cursor.to_polars()
        pl = import_optional("polars")
        if pl is not None:
            view = self.buffer_view()
            if len(view) > 0 and hasattr(pl, "read_ipc_stream"):
                with contextlib.suppress(Exception):
                    return pl.read_ipc_stream(bytes(view))
            tbl = self.to_arrow_table()
            if tbl is not self:
                return pl.from_arrow(tbl)
        return self.to_pylist()

    def to_pylist(self) -> list[dict[str, Any]]:
        """Convert the buffered rows into a list of dictionaries."""
        pa = import_optional("pyarrow")
        view = self.buffer_view()
        if pa is not None and len(view) > 0:
            tbl = self.to_arrow_table()
            if hasattr(tbl, "to_pylist"):
                res: list[dict[str, Any]] = tbl.to_pylist()
                if res or not self._rows:
                    return res
        cols = self._columns or tuple(f"col_{i}" for i in range(len(self._rows[0]) if self._rows else 0))
        return [{col: (r[idx] if idx < len(r) else None) for idx, col in enumerate(cols)} for r in self._rows]

    def release(self) -> bool:
        """Release any underlying native shared-memory region or anonymous mmap.

        The cached decoded table is dropped first so its buffers no longer export the mapping.
        """
        self._ipc_table_cache = None
        released = False
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "release"):
            with contextlib.suppress(Exception):
                released = bool(self._shm_cursor.release())
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
            released = True
        return released

    def close(self) -> None:
        """Release any underlying native shared-memory region or anonymous mmap."""
        self.release()

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Any],
        columns: Sequence[str] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Build an `ArrowCursor` from tabular rows, using SHM Arrow IPC when threshold is met."""
        if rows and isinstance(rows[0], Mapping):
            dict_rows = [dict(r) for r in rows]
            cols = tuple(columns) if columns is not None else tuple(dict_rows[0].keys())
            tuple_rows = [tuple(d.get(c) for c in cols) for d in dict_rows]
        else:
            tuple_rows = [tuple(r) for r in rows]
            if columns is not None:
                cols = tuple(columns)
            elif tuple_rows:
                cols = tuple(f"col_{i}" for i in range(len(tuple_rows[0])))
            else:
                cols = ()

        pa = import_optional("pyarrow")
        if pa is None:
            raise MissingDependencyError("pyarrow", install_package="pyarrow", extra="arrow")
        pydict: dict[str, list[Any]] = {
            col: [r[idx] if idx < len(r) else None for r in tuple_rows] for idx, col in enumerate(cols)
        }
        table = pa.Table.from_pydict(pydict)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        ipc_bytes = sink.getvalue().to_pybytes()

        use_shm = len(tuple_rows) >= threshold_rows or len(ipc_bytes) >= 65_536
        shm_cursor_obj: Any = None
        if use_shm:
            native_mod = import_optional("vertebra._vertebra")
            native_arrow_cls = getattr(native_mod, "ArrowCursor", None) if native_mod is not None else None
            if native_arrow_cls is None or not hasattr(native_arrow_cls, "write_ipc_to_shm"):
                raise MissingDependencyError("vertebra._vertebra.ArrowCursor", install_package="vertebra-core")
            shm_cursor_obj = native_arrow_cls.write_ipc_to_shm(cls._next_shm_segment_name(), ipc_bytes, len(tuple_rows))

        return cls(
            ipc_bytes,
            rows=tuple_rows,
            columns=cols,
            used_shm=use_shm,
            shm_cursor=shm_cursor_obj,
        )

    @staticmethod
    def _next_shm_segment_name() -> str:
        """Return a unique POSIX shared-memory segment name for `VTBARROW` frames."""
        global _SHM_SEQ_COUNTER
        with _SHM_SEQ_LOCK:
            _SHM_SEQ_COUNTER += 1
            seq = _SHM_SEQ_COUNTER
        return f"/vtb-arrow-{os.getpid()}-{seq}"

    @staticmethod
    def _columnar_source(data: Any) -> Any:
        """Return a `pyarrow.Table` or `RecordBatch` view of `data` for column-wise row extraction.

        Returns `None` for inputs without a decodable Arrow representation, and for cursors whose
        IPC payload is empty while buffered row tuples exist, matching `to_pylist()` precedence.
        """
        if isinstance(data, ArrowCursor):
            ipc_table = data._ipc_table()
            if ipc_table is None or (ipc_table.num_rows == 0 and data._rows):
                return None
            return ipc_table
        pa = import_optional("pyarrow")
        if pa is not None and isinstance(data, (pa.Table, pa.RecordBatch)):
            return data
        return None

    def _ipc_table(self) -> Any:
        """Return the memoized `pyarrow.Table` for the native SHM cursor or IPC buffer.

        Returns `None` when the payload is not Arrow IPC; only successful decodes are cached.
        """
        if self._ipc_table_cache is None:
            self._ipc_table_cache = self._decode_ipc_table()
        return self._ipc_table_cache

    def _decode_ipc_table(self) -> Any:
        """Decode the native SHM cursor or IPC buffer into a `pyarrow.Table`, or return `None` when not Arrow IPC."""
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "to_arrow_table"):
            with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                return self._shm_cursor.to_arrow_table()
        pa = import_optional("pyarrow")
        if pa is None:
            return None
        with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError, BufferError):
            view = self.buffer_view()
            if len(view) == 0:
                return None
            return pa.ipc.open_stream(pa.py_buffer(view)).read_all()
        return None


def write_arrow_shm(table: Any) -> memoryview:
    """Serialize a PyArrow Table, RecordBatch, or row sequence into a shared-memory Arrow IPC buffer."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        native_fn = getattr(native_mod, "write_arrow_shm", None)
        if callable(native_fn):
            with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                raw_res = native_fn(table)
                if isinstance(raw_res, (bytes, bytearray, memoryview)):
                    return memoryview(raw_res)

    if isinstance(table, (bytes, bytearray, memoryview)):
        payload = bytes(table)
    else:
        pa = import_optional("pyarrow")
        if pa is not None and hasattr(table, "schema"):
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                if hasattr(table, "to_batches"):
                    writer.write_table(table)
                else:
                    writer.write_batch(table)
            payload = sink.getvalue().to_pybytes()
        elif isinstance(table, ArrowCursor):
            return table.buffer_view()
        elif isinstance(table, Sequence) and not isinstance(table, str):
            cur = ArrowCursor.from_rows(table, threshold_rows=1)
            return cur.buffer_view()
        else:
            payload = encode_json_bytes(table)

    region = mmap.mmap(-1, max(4096, len(payload)))
    region[: len(payload)] = payload
    return memoryview(region)[: len(payload)]


def read_arrow_shm(region: Any, length: int | None = None) -> ArrowCursor:
    """Wrap a shared-memory Arrow IPC buffer or memoryview in a zero-copy `ArrowCursor`.

    Accepts raw offset-0 Arrow IPC streams and `VTBARROW` frames (8-byte magic, `u64` little-endian
    payload length, payload). Arrow buffers are decoded lazily over `pa.py_buffer(payload_view)`, and
    the cursor anchors `(region, raw_view, payload_view)` so they never outlive the mapping. Non-IPC
    payloads fall back to JSON row decoding.

    The returned cursor holds a buffer export of the caller's region. Closing an `mmap` region,
    or leaving a `with mmap.mmap(...)` block, while the cursor (or a table decoded from it) is
    alive raises `BufferError`; release the cursor and drop decoded tables first.

    Raises:
        ValueError: When a `VTBARROW` header is shorter than `16` bytes or declares a payload
            longer than the region.
    """
    if isinstance(region, ArrowCursor):
        return region
    if hasattr(region, "shm_name") and hasattr(region, "byte_len") and hasattr(region, "buffer_view"):
        return ArrowCursor(region, used_shm=True, shm_cursor=region)
    raw_view = memoryview(region).cast("B")
    if length is not None:
        raw_view = raw_view[:length]
    payload_view = raw_view
    if raw_view[: len(_VTBARROW_MAGIC)] == _VTBARROW_MAGIC:
        if len(raw_view) < _VTBARROW_HEADER_SIZE:
            msg = f"VTBARROW frame of {len(raw_view)} bytes is shorter than its {_VTBARROW_HEADER_SIZE}-byte header"
            raise ValueError(msg)
        byte_len = int.from_bytes(raw_view[len(_VTBARROW_MAGIC) : _VTBARROW_HEADER_SIZE], "little")
        frame_end = _VTBARROW_HEADER_SIZE + byte_len
        if frame_end > len(raw_view):
            msg = f"VTBARROW frame declares {frame_end} bytes but the region holds {len(raw_view)}"
            raise ValueError(msg)
        payload_view = raw_view[_VTBARROW_HEADER_SIZE:frame_end]
    anchor = (region, raw_view, payload_view)
    pa = import_optional("pyarrow")
    if pa is not None and len(payload_view) > 0:
        with contextlib.suppress(TypeError, ValueError, OSError):
            schema = pa.ipc.open_stream(pa.py_buffer(payload_view)).schema
            return ArrowCursor(payload_view, columns=schema.names, used_shm=True, shm_anchor=anchor)
    rows: list[tuple[Any, ...]] = []
    cols: tuple[str, ...] = ()
    if len(payload_view) > 0:
        with contextlib.suppress(TypeError, ValueError):
            decoded = decode_json(bytes(payload_view))
            if isinstance(decoded, list) and decoded and isinstance(decoded[0], dict):
                cols = tuple(decoded[0].keys())
                rows = [tuple(d.get(c) for c in cols) for d in decoded]
    return ArrowCursor(payload_view, rows=rows, columns=cols, used_shm=True, shm_anchor=anchor)


def _hydrate_rows_with_schema(
    rows: Sequence[Any],
    columns: Sequence[str],
    schema_type: type[Any] | None = None,
) -> list[Any]:
    """Hydrate tabular rows into dictionaries or typed `schema_type` instances via C-API vectorcall or `to_schema`."""
    if not rows:
        return []
    if schema_type is None or schema_type is dict:
        if isinstance(rows[0], Mapping):
            return [dict(r) for r in rows]
        return [{col: (r[i] if i < len(r) else None) for i, col in enumerate(columns)} for r in rows]
    if get_msgspec_rename_config(schema_type) is None:
        try:
            plan = ZeroCopyCodecRegistry.compile(schema_type)
            if isinstance(rows[0], Mapping):
                col_tuple = tuple(columns) if columns else tuple(rows[0].keys())
                tuple_rows = [tuple(r.get(c) for c in col_tuple) for r in rows]
                return plan.hydrate_rows(tuple_rows, col_tuple)
            return plan.hydrate_rows(rows, columns)
        except Exception:
            pass
    dict_rows = (
        [dict(r) for r in rows]
        if isinstance(rows[0], Mapping)
        else [{col: (r[i] if i < len(r) else None) for i, col in enumerate(columns)} for r in rows]
    )
    return list(to_schema(dict_rows, schema_type=schema_type))


def _table_to_rows_and_cols(
    data: Any,
    columns: Sequence[str] | None = None,
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Extract tuple rows and column names from a PyArrow Table, ArrowCursor, Polars DataFrame, or row sequence.

    Arrow tables, record batches, and IPC-backed cursors are transposed column-wise (one
    `to_pylist()` per selected column zipped into row tuples) instead of allocating a dictionary
    per row; requested columns absent from the schema yield `None` values. Duplicate field names
    (for example from SQL joins) collapse to one column whose values come from the last field
    with that name, matching `pyarrow.Table.to_pylist()` dictionaries.
    """
    table = ArrowCursor._columnar_source(data)
    if table is not None:
        index = {name: position for position, name in enumerate(table.column_names)}
        cols = tuple(columns) if columns is not None else tuple(index)
        num_rows = table.num_rows
        if num_rows == 0 or not cols:
            return [], cols
        column_values = (table.column(index[c]).to_pylist() if c in index else [None] * num_rows for c in cols)
        return list(zip(*column_values, strict=True)), cols
    if isinstance(data, ArrowCursor) or hasattr(data, "to_pylist") and callable(data.to_pylist):
        pylist = data.to_pylist()
    elif hasattr(data, "to_dicts") and callable(data.to_dicts):
        pylist = data.to_dicts()
    elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        if not data:
            return [], tuple(columns or ())
        if isinstance(data[0], Mapping):
            pylist = [dict(r) for r in data]
        else:
            tuple_rows = [tuple(r) for r in data]
            cols = tuple(columns) if columns is not None else tuple(f"col_{i}" for i in range(len(tuple_rows[0])))
            return tuple_rows, cols
    else:
        return [], tuple(columns or ())

    if not pylist:
        return [], tuple(columns or ())
    cols = tuple(columns) if columns is not None else tuple(pylist[0].keys())
    return [tuple(d.get(c) for c in cols) for d in pylist], cols
