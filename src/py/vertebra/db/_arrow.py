"""Shared zero-copy Arrow IPC and POSIX SHM (`VTBARROW`) cursor and helpers across database drivers."""

from __future__ import annotations

import contextlib
import mmap
import os
import threading
from collections.abc import Buffer, Mapping, Sequence
from typing import Any, Final, Self

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    encode_json_bytes,
)

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "ArrowCursor",
    "_hydrate_rows_with_schema",
    "_table_to_rows_and_cols",
    "read_arrow_shm",
    "write_arrow_shm",
)

ARROW_SHM_THRESHOLD_ROWS: Final[int] = 256
_SHM_SEQ_LOCK = threading.Lock()
_SHM_SEQ_COUNTER = 0


def _next_shm_segment_name() -> str:
    """Return a unique POSIX shared-memory segment name for `VTBARROW` frames."""
    global _SHM_SEQ_COUNTER
    with _SHM_SEQ_LOCK:
        _SHM_SEQ_COUNTER += 1
        seq = _SHM_SEQ_COUNTER
    return f"/vtb-arrow-{os.getpid()}-{seq}"


class ArrowCursor:
    """Zero-copy shared-memory Arrow IPC cursor for bulk analytical result sets."""

    __slots__ = ("_buffer", "_columns", "_mmap", "_rows", "_shm_cursor", "_used_shm")

    def __init__(
        self,
        buffer: Buffer | Any,
        *,
        rows: Sequence[tuple[Any, ...]] = (),
        columns: Sequence[str] = (),
        used_shm: bool = False,
        mmap_obj: mmap.mmap | None = None,
        shm_cursor: Any = None,
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

    def __len__(self) -> int:
        """Return the number of rows in the cursor."""
        if self._rows:
            return len(self._rows)
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "row_count"):
            rc = int(self._shm_cursor.row_count)
            if rc > 0:
                return rc
        return 0

    def __buffer__(self, flags: int, /) -> memoryview:
        """Return a PEP 688 zero-copy memoryview over the serialized Arrow IPC payload."""
        return self.buffer_view()

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
        if self._shm_cursor is not None and hasattr(self._shm_cursor, "to_arrow_table"):
            with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                return self._shm_cursor.to_arrow_table()
        pa = import_optional("pyarrow")
        view = self.buffer_view()
        if pa is not None and len(view) > 0:
            with contextlib.suppress(TypeError, ValueError, OSError):
                reader = pa.ipc.open_stream(pa.py_buffer(view))
                return reader.read_all()
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
        if not self._rows and len(view) > 0:
            with contextlib.suppress(TypeError, ValueError):
                decoded = decode_json(bytes(view))
                if isinstance(decoded, list):
                    return decoded
        cols = self._columns or tuple(f"col_{i}" for i in range(len(self._rows[0]) if self._rows else 0))
        return [{col: (r[idx] if idx < len(r) else None) for idx, col in enumerate(cols)} for r in self._rows]

    def release(self) -> bool:
        """Release any underlying native shared-memory region or anonymous mmap."""
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
        ipc_bytes = b""
        if pa is not None:
            pydict: dict[str, list[Any]] = {
                col: [r[idx] if idx < len(r) else None for r in tuple_rows] for idx, col in enumerate(cols)
            }
            table = pa.Table.from_pydict(pydict)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            ipc_bytes = sink.getvalue().to_pybytes()
        else:
            dict_list = [{col: (r[idx] if idx < len(r) else None) for idx, col in enumerate(cols)} for r in tuple_rows]
            ipc_bytes = encode_json_bytes(dict_list)

        use_shm = len(tuple_rows) >= threshold_rows or len(ipc_bytes) >= 65_536
        mmap_region: mmap.mmap | None = None
        shm_cursor_obj: Any = None
        if use_shm and len(ipc_bytes) > 0:
            native_mod = import_optional("vertebra._vertebra")
            native_arrow_cls = getattr(native_mod, "ArrowCursor", None) if native_mod is not None else None
            if native_arrow_cls is not None and hasattr(native_arrow_cls, "write_ipc_to_shm"):
                with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                    shm_cursor_obj = native_arrow_cls.write_ipc_to_shm(
                        _next_shm_segment_name(),
                        ipc_bytes,
                        len(tuple_rows),
                    )
            if shm_cursor_obj is None:
                native_cursor_cls = getattr(native_mod, "NativeShmCursor", None) if native_mod is not None else None
                if native_cursor_cls is not None:
                    with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                        shm_cursor_obj = native_cursor_cls(ipc_bytes)
                mmap_region = mmap.mmap(-1, max(4096, len(ipc_bytes)))
                mmap_region[: len(ipc_bytes)] = ipc_bytes

        return cls(
            ipc_bytes,
            rows=tuple_rows,
            columns=cols,
            used_shm=use_shm,
            mmap_obj=mmap_region,
            shm_cursor=shm_cursor_obj,
        )


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
    """Wrap a shared-memory Arrow IPC buffer or memoryview in a zero-copy `ArrowCursor`."""
    if isinstance(region, ArrowCursor):
        return region
    if hasattr(region, "shm_name") and hasattr(region, "byte_len") and hasattr(region, "buffer_view"):
        return ArrowCursor(region, used_shm=True, shm_cursor=region)
    raw_view = memoryview(region)
    if length is not None:
        raw_view = raw_view[:length]
    raw_bytes = bytes(raw_view)
    rows: list[tuple[Any, ...]] = []
    cols: tuple[str, ...] = ()
    pa = import_optional("pyarrow")
    if pa is not None and len(raw_bytes) > 0:
        with contextlib.suppress(TypeError, ValueError, OSError):
            reader = pa.ipc.open_stream(pa.py_buffer(raw_view))
            tbl = reader.read_all()
            pylist = tbl.to_pylist()
            if pylist:
                cols = tuple(pylist[0].keys())
                rows = [tuple(d.get(c) for c in cols) for d in pylist]
    if not rows and len(raw_bytes) > 0:
        with contextlib.suppress(TypeError, ValueError):
            decoded = decode_json(raw_bytes)
            if isinstance(decoded, list) and decoded and isinstance(decoded[0], dict):
                cols = tuple(decoded[0].keys())
                rows = [tuple(d.get(c) for c in cols) for d in decoded]
    return ArrowCursor(raw_view, rows=rows, columns=cols, used_shm=True)


def _hydrate_rows_with_schema(
    rows: Sequence[Any],
    columns: Sequence[str],
    schema_type: type[Any] | None = None,
) -> list[Any]:
    """Hydrate tabular rows into dictionaries or typed `schema_type` instances via C-API vectorcall."""
    if not rows:
        return []
    if schema_type is None or schema_type is dict:
        if isinstance(rows[0], Mapping):
            return [dict(r) for r in rows]
        return [{col: (r[i] if i < len(r) else None) for i, col in enumerate(columns)} for r in rows]
    plan = ZeroCopyCodecRegistry.compile(schema_type)
    if isinstance(rows[0], Mapping):
        col_tuple = tuple(columns) if columns else tuple(rows[0].keys())
        tuple_rows = [tuple(r.get(c) for c in col_tuple) for r in rows]
        return plan.hydrate_rows(tuple_rows, col_tuple)
    return plan.hydrate_rows(rows, columns)


def _table_to_rows_and_cols(
    data: Any,
    columns: Sequence[str] | None = None,
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Extract tuple rows and column names from a PyArrow Table, ArrowCursor, Polars DataFrame, or row sequence."""
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
