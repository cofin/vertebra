"""Bounded Arrow IPC stream chunkers and zero-row-materialization PostgreSQL COPY loaders."""

from __future__ import annotations

import contextlib
import inspect
from typing import TYPE_CHECKING, Any, Final

from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import DataError
from vertebra.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator, Iterator, Sequence

    from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient


__all__ = (
    "DEFAULT_ARROW_COPY_MAX_BYTES",
    "DEFAULT_ARROW_COPY_MAX_ROWS",
    "aiter_arrow_ipc_batches",
    "iter_arrow_ipc_batches",
    "load_arrow_async",
    "load_arrow_sync",
)

DEFAULT_ARROW_COPY_MAX_ROWS: Final[int] = 65_536
DEFAULT_ARROW_COPY_MAX_BYTES: Final[int] = 8 * 1024 * 1024
_VTBARROW_MAGIC: Final[bytes] = b"VTBARROW"
_VTBARROW_HEADER_SIZE: Final[int] = 16


def _validate_limits(max_rows: int, max_bytes: int) -> None:
    """Validate positive integer row and byte bounds before touching Arrow streams."""
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
        msg = f"max_rows must be an integer >= 1, got {max_rows!r}"
        raise DataError(msg)
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        msg = f"max_bytes must be an integer >= 1, got {max_bytes!r}"
        raise DataError(msg)


def _serialize_empty_schema_ipc(pa: Any, schema: Any, *, max_bytes: int) -> bytes:
    """Serialize a zero-row self-contained Arrow IPC stream preserving schema metadata."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    payload: bytes = sink.getvalue().to_pybytes()
    if len(payload) > max_bytes:
        msg = f"Arrow schema IPC header ({len(payload)} bytes) exceeds max_bytes={max_bytes}"
        raise DataError(msg)
    return payload


def _serialize_batch_ipc(pa: Any, schema: Any, batch: Any) -> bytes:
    """Serialize one Arrow RecordBatch as a self-contained uncompressed IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    payload: bytes = sink.getvalue().to_pybytes()
    return payload


def _iter_bounded_batch_ipc(
    pa: Any,
    schema: Any,
    batch: Any,
    *,
    max_rows: int,
    max_bytes: int,
) -> Generator[bytes, None, None]:
    """Slice a non-empty RecordBatch by row count and iteratively halve oversized IPC chunks."""
    total_rows = int(batch.num_rows)
    offset = 0
    while offset < total_rows:
        take = min(max_rows, total_rows - offset)
        window = batch.slice(offset, take)
        offset += take
        pending: list[Any] = [window]
        while pending:
            current = pending.pop()
            ipc_bytes = _serialize_batch_ipc(pa, schema, current)
            if len(ipc_bytes) <= max_bytes:
                yield ipc_bytes
                continue
            current_rows = int(current.num_rows)
            if current_rows <= 1:
                msg = f"single Arrow row serializes to {len(ipc_bytes)} IPC bytes, exceeding max_bytes={max_bytes}"
                raise DataError(msg)
            mid = current_rows // 2
            pending.append(current.slice(mid, current_rows - mid))
            pending.append(current.slice(0, mid))


def _unwrap_vtbarrow_view(raw_bytes_view: memoryview) -> memoryview:
    """Strip an optional 16-byte VTBARROW shared-memory header from a byte-cast memoryview."""
    if raw_bytes_view[: len(_VTBARROW_MAGIC)] != _VTBARROW_MAGIC:
        return raw_bytes_view
    if len(raw_bytes_view) < _VTBARROW_HEADER_SIZE:
        msg = f"VTBARROW buffer of {len(raw_bytes_view)} bytes is truncated (expected >= {_VTBARROW_HEADER_SIZE} bytes)"
        raise DataError(msg)
    declared_len = int.from_bytes(
        raw_bytes_view[len(_VTBARROW_MAGIC) : _VTBARROW_HEADER_SIZE],
        "little",
    )
    frame_end = _VTBARROW_HEADER_SIZE + declared_len
    if frame_end > len(raw_bytes_view):
        msg = f"VTBARROW buffer declares {frame_end} bytes but holds only {len(raw_bytes_view)} bytes"
        raise DataError(msg)
    return raw_bytes_view[_VTBARROW_HEADER_SIZE:frame_end]


def _extract_ipc_buffer_view(source: Any) -> tuple[Any, memoryview] | None:
    """Return an anchored buffer and byte memoryview when source exposes an IPC byte buffer."""
    buffer_view_fn = getattr(source, "buffer_view", None)
    if isinstance(source, ArrowCursor):
        buf_obj: Any = source.buffer_view()
        base_view = buf_obj if isinstance(buf_obj, memoryview) else memoryview(buf_obj)
        byte_view = base_view if base_view.ndim == 1 and base_view.format == "B" else base_view.cast("B")
        payload_view = _unwrap_vtbarrow_view(byte_view)
        return (source, buf_obj, base_view, byte_view), payload_view
    if callable(buffer_view_fn):
        buf_obj = buffer_view_fn()
        base_view = buf_obj if isinstance(buf_obj, memoryview) else memoryview(buf_obj)
        byte_view = base_view if base_view.ndim == 1 and base_view.format == "B" else base_view.cast("B")
        payload_view = _unwrap_vtbarrow_view(byte_view)
        return (source, buf_obj, base_view, byte_view), payload_view
    if isinstance(source, (bytes, bytearray, memoryview)):
        base_view = source if isinstance(source, memoryview) else memoryview(source)
        byte_view = base_view if base_view.ndim == 1 and base_view.format == "B" else base_view.cast("B")
        payload_view = _unwrap_vtbarrow_view(byte_view)
        return (source, base_view, byte_view), payload_view
    return None


def _open_ipc_stream_reader(pa: Any, payload_view: memoryview) -> Any:
    """Open a zero-copy PyArrow IPC stream reader over a validated byte memoryview."""
    if len(payload_view) == 0:
        msg = "Arrow IPC buffer is empty and contains no schema message"
        raise DataError(msg)
    try:
        return pa.ipc.open_stream(pa.py_buffer(payload_view))
    except (OSError, TypeError, ValueError) as exc:
        msg = f"invalid Arrow IPC stream payload: {exc}"
        raise DataError(msg) from exc


def _iter_reader_ipc(
    pa: Any,
    reader: Any,
    *,
    max_rows: int,
    max_bytes: int,
    anchor: Any = None,
) -> Generator[bytes, None, None]:
    """Drain a RecordBatchReader into bounded IPC byte slices and close it on exit."""
    _ = anchor
    try:
        schema = reader.schema
        emitted_any = False
        for batch in reader:
            if int(batch.num_rows) == 0:
                continue
            emitted_any = True
            yield from _iter_bounded_batch_ipc(
                pa,
                schema,
                batch,
                max_rows=max_rows,
                max_bytes=max_bytes,
            )
        if not emitted_any:
            yield _serialize_empty_schema_ipc(pa, schema, max_bytes=max_bytes)
    finally:
        close_fn = getattr(reader, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()


def _iter_sync_chunks(
    pa: Any,
    source: Any,
    *,
    max_rows: int,
    max_bytes: int,
) -> Generator[bytes, None, None]:
    """Dispatch supported tabular Arrow sources into bounded IPC stream byte chunks."""
    if isinstance(source, pa.RecordBatch):
        if int(source.num_rows) == 0:
            yield _serialize_empty_schema_ipc(pa, source.schema, max_bytes=max_bytes)
            return
        yield from _iter_bounded_batch_ipc(
            pa,
            source.schema,
            source,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
        return

    if isinstance(source, pa.Table):
        reader = source.to_reader(max_chunksize=max_rows)
        yield from _iter_reader_ipc(pa, reader, max_rows=max_rows, max_bytes=max_bytes)
        return

    extracted = _extract_ipc_buffer_view(source)
    if extracted is not None:
        anchor, payload_view = extracted
        reader = _open_ipc_stream_reader(pa, payload_view)
        yield from _iter_reader_ipc(
            pa,
            reader,
            max_rows=max_rows,
            max_bytes=max_bytes,
            anchor=anchor,
        )
        return

    if isinstance(source, pa.RecordBatchReader):
        yield from _iter_reader_ipc(pa, source, max_rows=max_rows, max_bytes=max_bytes)
        return

    if hasattr(source, "__arrow_c_stream__"):
        try:
            reader = pa.RecordBatchReader.from_stream(source)
        except (OSError, TypeError, ValueError) as exc:
            msg = f"failed to open Arrow C stream source: {exc}"
            raise DataError(msg) from exc
        yield from _iter_reader_ipc(pa, reader, max_rows=max_rows, max_bytes=max_bytes)
        return

    msg = (
        "unsupported Arrow source type "
        f"{type(source).__qualname__!r}; expected pyarrow.Table, pyarrow.RecordBatch, "
        "pyarrow.RecordBatchReader, ArrowCursor, Arrow IPC buffer, or __arrow_c_stream__ provider"
    )
    raise DataError(msg)


def iter_arrow_ipc_batches(
    source: Any,
    *,
    max_rows: int = DEFAULT_ARROW_COPY_MAX_ROWS,
    max_bytes: int = DEFAULT_ARROW_COPY_MAX_BYTES,
) -> Iterator[bytes]:
    """Yield bounded self-contained Arrow IPC stream chunks without materializing Python rows.

    Args:
        source: Input `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`,
            `ArrowCursor`, Arrow IPC buffer, or `__arrow_c_stream__` object.
        max_rows: Maximum number of rows serialized into a single IPC chunk before splitting.
        max_bytes: Maximum serialized IPC stream byte length per yielded chunk.

    Returns:
        An iterator yielding self-contained Arrow IPC stream payloads (`<= max_bytes`), or a
        single zero-row schema-only IPC stream when `source` contains zero rows.

    Raises:
        MissingDependencyError: When `pyarrow` is not installed.
        DataError: When `source` is invalid or a single row exceeds `max_bytes`.
    """
    _validate_limits(max_rows, max_bytes)
    pa = ensure_pyarrow()
    return _iter_sync_chunks(pa, source, max_rows=max_rows, max_bytes=max_bytes)


async def aiter_arrow_ipc_batches(
    source: Any,
    *,
    max_rows: int = DEFAULT_ARROW_COPY_MAX_ROWS,
    max_bytes: int = DEFAULT_ARROW_COPY_MAX_BYTES,
) -> AsyncIterator[bytes]:
    """Asynchronously yield bounded self-contained Arrow IPC stream chunks without row materialization.

    Args:
        source: Input Arrow source supported by `iter_arrow_ipc_batches`, or an async iterable
            of Arrow sources / `RecordBatch` values.
        max_rows: Maximum number of rows serialized into a single IPC chunk before splitting.
        max_bytes: Maximum serialized IPC stream byte length per yielded chunk.

    Yields:
        Self-contained Arrow IPC stream payloads (`<= max_bytes`), or one zero-row schema-only
        IPC stream when `source` yields zero rows.

    Raises:
        MissingDependencyError: When `pyarrow` is not installed.
        DataError: When `source` is invalid, lacks a schema when empty, or exceeds `max_bytes`
            for a single row.
    """
    _validate_limits(max_rows, max_bytes)
    pa = ensure_pyarrow()
    if hasattr(source, "__aiter__"):
        ait = source.__aiter__()
        emitted_any = False
        inferred_schema: Any = getattr(source, "schema", None)
        try:
            async for item in ait:
                with contextlib.closing(
                    _iter_sync_chunks(pa, item, max_rows=max_rows, max_bytes=max_bytes)
                ) as sub_chunks:
                    item_schema = getattr(item, "schema", None)
                    if inferred_schema is None and item_schema is not None:
                        inferred_schema = item_schema
                    for chunk in sub_chunks:
                        if isinstance(item, (pa.RecordBatch, pa.Table)) and int(item.num_rows) == 0:
                            continue
                        emitted_any = True
                        yield chunk
            if not emitted_any:
                if isinstance(inferred_schema, pa.Schema):
                    yield _serialize_empty_schema_ipc(pa, inferred_schema, max_bytes=max_bytes)
                else:
                    msg = "empty asynchronous Arrow source does not expose a pyarrow.Schema"
                    raise DataError(msg)
        finally:
            aclose_fn = getattr(ait, "aclose", None)
            if callable(aclose_fn):
                with contextlib.suppress(Exception):
                    maybe_awaitable = aclose_fn()
                    if inspect.isawaitable(maybe_awaitable):
                        await maybe_awaitable
        return

    with contextlib.closing(_iter_sync_chunks(pa, source, max_rows=max_rows, max_bytes=max_bytes)) as chunks:
        for chunk in chunks:
            yield chunk


def load_arrow_sync(
    client: SyncPostgresClient,
    table_name: str,
    source: Any,
    *,
    columns: Sequence[str] | None = None,
    schema_name: str | None = None,
) -> int:
    """Stream an Arrow source into PostgreSQL binary `COPY ... FROM STDIN` synchronously.

    Args:
        client: Synchronous native PostgreSQL client exposing `copy_from_arrow`.
        table_name: Target PostgreSQL table identifier (literal when `schema_name` is supplied,
            otherwise validated `table` or `schema.table`).
        source: Arrow table, batch, reader, `ArrowCursor`, IPC buffer, or C-stream provider.
        columns: Optional explicit ordered sequence of target column names.
        schema_name: Optional literal target PostgreSQL schema name.

    Returns:
        Number of copied rows reported by PostgreSQL `CommandComplete`.
    """
    batches = iter_arrow_ipc_batches(source)
    try:
        try:
            first_batch_ipc = next(batches)
        except StopIteration as exc:
            msg = "Arrow IPC batch iterator produced no schema payload"
            raise DataError(msg) from exc

        copy_in = client.copy_from_arrow(
            table_name,
            first_batch_ipc,
            columns=columns,
            schema_name=schema_name,
        )
        try:
            for batch_ipc in batches:
                copy_in.write_arrow_ipc(batch_ipc)
            row_count = copy_in.finish()
        except BaseException:
            with contextlib.suppress(Exception):
                copy_in.close()
            raise
        return int(row_count)
    finally:
        close_fn = getattr(batches, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()


async def load_arrow_async(
    client: AsyncPostgresClient,
    table_name: str,
    source: Any,
    *,
    columns: Sequence[str] | None = None,
    schema_name: str | None = None,
) -> int:
    """Stream an Arrow source into PostgreSQL binary `COPY ... FROM STDIN` asynchronously.

    Args:
        client: Asynchronous native PostgreSQL client exposing `copy_from_arrow`.
        table_name: Target PostgreSQL table identifier (literal when `schema_name` is supplied,
            otherwise validated `table` or `schema.table`).
        source: Arrow table, batch, reader, `ArrowCursor`, IPC buffer, C-stream provider, or
            async batch iterable.
        columns: Optional explicit ordered sequence of target column names.
        schema_name: Optional literal target PostgreSQL schema name.

    Returns:
        Number of copied rows reported by PostgreSQL `CommandComplete`.
    """
    batches = aiter_arrow_ipc_batches(source)
    try:
        try:
            first_batch_ipc = await anext(batches)
        except StopAsyncIteration as exc:
            msg = "Arrow IPC batch iterator produced no schema payload"
            raise DataError(msg) from exc

        copy_in = await client.copy_from_arrow(
            table_name,
            first_batch_ipc,
            columns=columns,
            schema_name=schema_name,
        )
        try:
            async for batch_ipc in batches:
                await copy_in.write_arrow_ipc(batch_ipc)
            row_count = await copy_in.finish()
        except BaseException:
            with contextlib.suppress(Exception):
                await copy_in.aclose()
            raise
        return int(row_count)
    finally:
        aclose_fn = getattr(batches, "aclose", None)
        if callable(aclose_fn):
            with contextlib.suppress(Exception):
                maybe_awaitable = aclose_fn()
                if inspect.isawaitable(maybe_awaitable):
                    await maybe_awaitable
