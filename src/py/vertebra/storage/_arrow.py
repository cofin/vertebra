"""Demand-driven Arrow codecs over native storage streams and ranges."""

from __future__ import annotations

import asyncio
import contextlib
import io
import threading
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Buffer,
    Callable,
    Generator,
    Iterator,
    Mapping,
    Sequence,
)
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from vertebra.utils.module_loader import ensure_module
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from vertebra.storage._store import AsyncObjectStore, AsyncReadableFile, PutResult, SyncObjectStore

__all__ = (
    "iter_batches_async",
    "iter_batches_sync",
    "normalize_arrow_format",
    "read_table_async",
    "read_table_sync",
    "write_batches_async",
    "write_batches_sync",
)


def normalize_arrow_format(raw_format: Any, path: str | Path) -> str:
    """Resolve an optional format hint or path suffix to ``'parquet'`` or ``'arrow-ipc'``."""
    if not raw_format:
        suffix = Path(str(path)).suffix.lower()
        return "arrow-ipc" if suffix in {".arrow", ".ipc", ".feather"} else "parquet"
    lowered = str(raw_format).strip().lower()
    if lowered == "parquet":
        return "parquet"
    if lowered in {"arrow", "ipc", "arrow-ipc", "feather"}:
        return "arrow-ipc"
    msg = f"Unsupported Arrow format {raw_format!r}; expected 'parquet' or 'arrow-ipc'"
    raise ValueError(msg)


def iter_batches_sync(
    store: SyncObjectStore,
    key: str,
    *,
    file_format: str,
    batch_size: int = 65536,
    columns: Sequence[str] | None = None,
    options: Mapping[str, Any] | None = None,
) -> Iterator[Any]:
    """Decode batches on demand using native synchronous streams or ranges.

    Parquet sources open with ``read_ahead=0`` and ``pre_buffer=False`` so each
    rowgroup is fetched only when the consumer requests its batches.
    """
    _validate_format(file_format)
    _validate_batch_size(batch_size)
    stream = None if file_format == "parquet" else store.get_stream(key)
    source = store.open_reader(key, read_ahead=0) if stream is None else _StreamFile(lambda: next(stream, None))
    reader = _BatchReader()
    try:
        reader.open(source, file_format, batch_size, columns, options)
        while True:
            done, batch = reader.next()
            if done:
                break
            yield batch
    finally:
        try:
            reader.close()
        finally:
            source.close()
            if stream is not None:
                cast("Generator[Any, None, None]", stream).close()


async def iter_batches_async(
    store: AsyncObjectStore,
    key: str,
    *,
    file_format: str,
    batch_size: int = 65536,
    columns: Sequence[str] | None = None,
    options: Mapping[str, Any] | None = None,
) -> AsyncIterator[Any]:
    """Decode one batch per demand while native asynchronous I/O stays on-loop.

    Parquet sources open with ``read_ahead=0`` and ``pre_buffer=False`` so each
    rowgroup is fetched only when the consumer requests its batches.
    """
    _validate_format(file_format)
    _validate_batch_size(batch_size)
    bridge = _CodecBridge()
    stream = None if file_format == "parquet" else store.get_stream(key)
    backing = store.open_reader(key, size=(await store.head(key)).size, read_ahead=0) if stream is None else None
    source = _RangeFile(backing, bridge) if backing is not None else _StreamFile(lambda: bridge.io(anext, stream, None))
    reader = _BatchReader()
    try:
        await bridge.run(reader.open, source, file_format, batch_size, columns, options)
        while True:
            done, batch = await bridge.run(reader.next)
            if done:
                break
            yield batch
    finally:
        await bridge.stop()
        try:
            await bridge.run(reader.close)
        finally:
            source.close()
            if backing is not None:
                backing.close()
            if stream is not None:
                await cast("AsyncGenerator[Any, None]", stream).aclose()


def read_table_sync(
    store: SyncObjectStore,
    key: str,
    *,
    file_format: str,
    options: Mapping[str, Any] | None = None,
) -> Any:
    """Read a materialized Table through seekable native ranges, retaining its schema.

    Parquet reads default to ``pre_buffer=True`` so column chunk ranges coalesce;
    ``options`` may override it.
    """
    _validate_format(file_format)
    ensure_module("pyarrow", package="pyarrow", extra="arrow")
    with store.open_reader(key) as source:
        return _read_table(source, file_format, options)


async def read_table_async(
    store: AsyncObjectStore,
    key: str,
    *,
    file_format: str,
    options: Mapping[str, Any] | None = None,
) -> Any:
    """Read a Table off-loop while native range I/O remains on the caller loop.

    Parquet reads default to ``pre_buffer=True`` so column chunk ranges coalesce;
    ``options`` may override it.
    """
    _validate_format(file_format)
    ensure_module("pyarrow", package="pyarrow", extra="arrow")
    backing = store.open_reader(key, size=(await store.head(key)).size)
    bridge = _CodecBridge()
    source = _RangeFile(backing, bridge)
    try:
        return await bridge.run(_read_table, source, file_format, options)
    finally:
        await bridge.stop()
        source.close()
        backing.close()


def write_batches_sync(
    store: SyncObjectStore,
    key: str,
    data: Any,
    *,
    file_format: str,
    batch_size: int = 65536,
    schema: Any = None,
    options: Mapping[str, Any] | None = None,
    checksum: bool = False,
    expected_sha256: str | None = None,
    expected_crc32c: str | None = None,
    expected_md5: str | None = None,
) -> PutResult:
    """Upload incremental codec output through the native synchronous multipart path.

    ``data`` may be a Table, RecordBatch, batch iterable, or a serialized Arrow IPC
    stream exposed through the buffer protocol.
    """
    _validate_format(file_format)
    kwargs = dict(options or {})
    if file_format == "parquet":
        batch_size = kwargs.pop("row_group_size", batch_size)
    _validate_batch_size(batch_size)
    writer = _BatchWriter()

    def chunks() -> Generator[bytes, None, None]:
        try:
            yield from writer.open(data, file_format, batch_size, schema, kwargs)
            while True:
                done, output = writer.next()
                yield from output
                if done:
                    break
        finally:
            writer.close()
            writer.sink.close()

    output = chunks()
    try:
        return store.put_stream(
            key,
            output,
            content_type=_media_type(file_format),
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )
    finally:
        output.close()


async def write_batches_async(
    store: AsyncObjectStore,
    key: str,
    data: Any,
    *,
    file_format: str,
    batch_size: int = 65536,
    schema: Any = None,
    options: Mapping[str, Any] | None = None,
    checksum: bool = False,
    expected_sha256: str | None = None,
    expected_crc32c: str | None = None,
    expected_md5: str | None = None,
) -> PutResult:
    """Encode batch-sized steps off-loop and await each native multipart write.

    ``data`` may be a Table, RecordBatch, batch iterable, or a serialized Arrow IPC
    stream exposed through the buffer protocol.
    """
    _validate_format(file_format)
    kwargs = dict(options or {})
    if file_format == "parquet":
        batch_size = kwargs.pop("row_group_size", batch_size)
    _validate_batch_size(batch_size)
    bridge = _CodecBridge()
    writer = _BatchWriter()

    async def chunks() -> AsyncGenerator[bytes, None]:
        try:
            for chunk in await bridge.run(writer.open, data, file_format, batch_size, schema, kwargs):
                yield chunk
            while True:
                done, output = await bridge.run(writer.next)
                for chunk in output:
                    yield chunk
                if done:
                    break
        finally:
            await bridge.stop()
            await bridge.run(writer.close)
            writer.sink.close()

    output = chunks()
    try:
        return await store.put_stream(
            key,
            output,
            content_type=_media_type(file_format),
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )
    finally:
        await output.aclose()


class _CodecBridge:
    """Run codec steps off-loop while retaining ownership until native I/O unwinds."""

    __slots__ = ("_lock", "_loop", "_pending", "_stopped", "_tasks", "_worker")

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._lock = threading.Lock()
        self._stopped = False
        self._pending: set[Future[Any]] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._worker: asyncio.Future[Any] | None = None

    def io[T](self, fn: Callable[..., Awaitable[T]], *args: Any) -> T:
        """Wait in a codec worker for native I/O on the caller's event loop."""
        with self._lock:
            if self._stopped:
                raise io.UnsupportedOperation("Arrow source is closed")
            pending = asyncio.run_coroutine_threadsafe(self._invoke_io(fn, args), self._loop)
            self._pending.add(pending)
        try:
            return pending.result()
        finally:
            with self._lock:
                self._pending.discard(pending)

    async def run[T](self, fn: Callable[..., T], *args: Any) -> T:
        """Await one codec operation without detaching its cancellation cleanup."""
        worker = asyncio.wrap_future(SyncPool.default().submit(fn, *args))
        self._worker = worker
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            await self.stop()
            raise
        finally:
            self._worker = None

    async def stop(self) -> None:
        """Cancel pending I/O and await both codec and loop-side source cleanup."""
        with self._lock:
            self._stopped = True
            pending = tuple(self._pending)
        for future in pending:
            future.cancel()
        if self._worker is not None:
            with contextlib.suppress(BaseException):
                await self._wait_for_exit(self._worker)
        if self._tasks:
            await self._wait_for_exit(asyncio.gather(*self._tasks, return_exceptions=True))

    async def _invoke_io[T](self, fn: Callable[..., Awaitable[T]], args: tuple[Any, ...]) -> T:
        task = asyncio.current_task()
        assert task is not None
        with self._lock:
            if self._stopped:
                raise asyncio.CancelledError
            self._tasks.add(task)
        try:
            return await fn(*args)
        finally:
            self._tasks.discard(task)

    @staticmethod
    async def _wait_for_exit(future: asyncio.Future[Any]) -> None:
        """Keep cleanup ownership through further cancellation until work exits."""
        while not future.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(future)
        future.result()


class _RangeFile(io.RawIOBase):
    """Expose an asynchronous native range reader to synchronous codec callbacks."""

    __slots__ = ("_bridge", "_reader")

    def __init__(self, reader: AsyncReadableFile, bridge: _CodecBridge) -> None:
        self._reader = reader
        self._bridge = bridge

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()

    def read(self, size: int | None = -1) -> bytes:
        return self._bridge.io(self._reader.read, size)

    def readinto(self, buffer: Buffer) -> int:
        """Fill ``buffer`` through the native reader's own ``readinto`` on the caller loop."""
        return self._bridge.io(self._reader.readinto, buffer)


class _StreamFile(io.RawIOBase):
    """Read exact codec requests with at most one unfinished native stream chunk.

    Arrow's Python file adapter decodes message bodies through ``read_buffer`` and reads
    only small length prefixes through ``read``, so ``readinto`` keeps the unsupported
    ``io.RawIOBase`` default.
    """

    __slots__ = ("_next_chunk", "_pos", "_remaining")

    def __init__(self, next_chunk: Callable[[], Any]) -> None:
        self._next_chunk = next_chunk
        self._remaining = memoryview(b"")
        self._pos = 0

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def read(self, size: int | None = -1) -> bytes:
        """Return up to ``size`` bytes, copying pending chunk slices once into the result."""
        return b"".join(self._take(size))

    def read_buffer(self, nbytes: int) -> memoryview | bytes:
        """Return up to ``nbytes`` bytes for Arrow's ``ReadBuffer`` path.

        A request that fits in the pending native chunk returns a zero-copy memoryview slice
        that keeps the chunk alive for as long as Arrow holds the buffer; a request spanning
        chunks is copied once into a new ``bytes`` object.
        """
        pieces = self._take(nbytes)
        return pieces[0] if len(pieces) == 1 else b"".join(pieces)

    def _take(self, size: int | None) -> list[memoryview]:
        """Detach zero-copy slices totalling up to ``size`` bytes from pending native chunks."""
        if size is None or size < 0:
            raise io.UnsupportedOperation("Arrow streaming requires a bounded read size")
        pieces: list[memoryview] = []
        needed = size
        while needed and self._next_ready():
            count = min(needed, len(self._remaining))
            pieces.append(self._remaining[:count])
            self._remaining = self._remaining[count:]
            needed -= count
        self._pos += size - needed
        return pieces

    def _next_ready(self) -> bool:
        """Ensure an unread chunk is pending, returning ``False`` at end of stream."""
        while not self._remaining:
            chunk = self._next_chunk()
            if chunk is None:
                return False
            self._remaining = memoryview(chunk).cast("B")
        return True


class _BatchReader:
    """Retain one decoder and its schema/dictionary state across batch requests."""

    __slots__ = ("_batches", "reader")

    def __init__(self) -> None:
        self.reader: Any = None
        self._batches: Iterator[Any] = iter(())

    def open(
        self,
        source: Any,
        file_format: str,
        batch_size: int,
        columns: Sequence[str] | None,
        options: Mapping[str, Any] | None,
    ) -> None:
        kwargs = dict(options or {})
        if file_format == "parquet":
            pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
            batch_options = {key: kwargs.pop(key) for key in ("row_groups", "use_threads") if key in kwargs}
            columns = kwargs.pop("columns", columns)
            kwargs.setdefault("pre_buffer", False)
            self.reader = pq.ParquetFile(source, **kwargs)
            self._batches = self.reader.iter_batches(batch_size=batch_size, columns=columns, **batch_options)
        else:
            ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
            self.reader = ipc.open_stream(source, **_ipc_options(kwargs, ipc.IpcReadOptions))
            self._batches = (
                batch.slice(offset, batch_size)
                for batch in self.reader
                for offset in range(0, batch.num_rows, batch_size)
            )

    def next(self) -> tuple[bool, Any]:
        batch = next(self._batches, None)
        return batch is None, batch

    def close(self) -> None:
        if self.reader is not None:
            self.reader.close()


class _OutputSink(io.RawIOBase):
    """Retain only the current codec operation's output and absolute position."""

    __slots__ = ("_chunks", "_position")

    def __init__(self) -> None:
        self._chunks: list[bytes] = []
        self._position = 0

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def write(self, data: Buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed Arrow sink")
        chunk = bytes(data)
        self._chunks.append(chunk)
        self._position += len(chunk)
        return len(chunk)

    def drain(self) -> list[bytes]:
        chunks, self._chunks = self._chunks, []
        return chunks


class _BatchWriter:
    """Encode one input batch at a time with persistent dictionary and footer state."""

    __slots__ = ("_batch_size", "_batches", "_parquet", "_source", "_writer", "sink")

    def __init__(self) -> None:
        self.sink = _OutputSink()
        self._writer: Any = None
        self._source: Any = None
        self._batches: Iterator[Any] = iter(())
        self._batch_size = 65536
        self._parquet = False

    def open(
        self, data: Any, file_format: str, batch_size: int, schema: Any, options: Mapping[str, Any] | None
    ) -> list[bytes]:
        """Open the input source and encoder, returning any header output.

        Serialized IPC streams given as ``pyarrow.Buffer`` or any buffer-protocol
        object decode through ``pyarrow.py_buffer`` without copying the payload.
        """
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        self._batch_size = batch_size
        self._parquet = file_format == "parquet"
        if isinstance(data, pa.Table):
            schema = schema if schema is not None else data.schema
            self._source = data.to_reader(max_chunksize=batch_size)
        elif isinstance(data, pa.RecordBatch):
            schema = schema if schema is not None else data.schema
            self._source = iter((data,))
        elif isinstance(data, Buffer) or hasattr(data, "buffer_view"):
            payload = data if isinstance(data, Buffer) else data.buffer_view()
            self._source = pa.ipc.open_stream(payload if isinstance(payload, pa.Buffer) else pa.py_buffer(payload))
            schema = schema if schema is not None else self._source.schema
        else:
            schema = schema if schema is not None else getattr(data, "schema", None)
            self._source = iter(data)
        first = next(self._source, None)
        if schema is None:
            while first is not None and first.num_rows == 0:
                schema = first.schema
                first = next(self._source, None)
            if first is not None:
                schema = first.schema
        if schema is None:
            raise ValueError("An empty batch iterable requires an explicit schema")
        self._batches = self._split_batches(first)
        kwargs = dict(options or {})
        if self._parquet:
            pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
            self._writer = pq.ParquetWriter(self.sink, schema, **kwargs)
        else:
            self._writer = pa.ipc.new_stream(self.sink, schema, **_ipc_options(kwargs, pa.ipc.IpcWriteOptions))
        return self.sink.drain()

    def next(self) -> tuple[bool, list[bytes]]:
        batch = next(self._batches, None)
        if batch is None:
            self.close()
            return True, self.sink.drain()
        if self._parquet:
            self._writer.write_batch(batch, row_group_size=self._batch_size)
        else:
            self._writer.write_batch(batch)
        return False, self.sink.drain()

    def close(self) -> None:
        try:
            if self._writer is not None:
                writer, self._writer = self._writer, None
                writer.close()
        finally:
            try:
                close_batches = getattr(self._batches, "close", None)
                if close_batches is not None:
                    close_batches()
            finally:
                source, self._source = self._source, None
                close = getattr(source, "close", None)
                if close is not None:
                    close()

    def _split_batches(self, first: Any) -> Iterator[Any]:
        batch = first
        while batch is not None:
            for offset in range(0, batch.num_rows, self._batch_size):
                yield batch.slice(offset, self._batch_size)
            batch = next(self._source, None)


def _validate_format(file_format: str) -> None:
    if file_format not in {"parquet", "arrow-ipc"}:
        msg = f"Unsupported Arrow format {file_format!r}; expected 'parquet' or 'arrow-ipc'"
        raise ValueError(msg)


def _validate_batch_size(batch_size: int) -> None:
    ensure_module("pyarrow", package="pyarrow", extra="arrow")
    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)


def _media_type(file_format: str) -> str:
    return "application/vnd.apache.parquet" if file_format == "parquet" else "application/vnd.apache.arrow.stream"


def _ipc_options(kwargs: dict[str, Any], options_type: Any) -> dict[str, Any]:
    """Fold loose IPC option keys into one ``IpcReadOptions``/``IpcWriteOptions``.

    ``memory_pool``, ``use_legacy_format``, and a prebuilt ``options`` object pass
    through to the stream factory unchanged; every other key constructs
    ``options_type``. Combining a prebuilt ``options`` object with loose keys is
    rejected because neither can be merged into the other without guessing.
    """
    passthrough: dict[str, Any] = {
        key: kwargs.pop(key) for key in ("memory_pool", "options", "use_legacy_format") if key in kwargs
    }
    if not kwargs:
        return passthrough
    if "options" in passthrough:
        msg = f"Pass IPC settings either as a prebuilt {options_type.__name__} or as option keys, not both"
        raise ValueError(msg)
    return {**passthrough, "options": options_type(**kwargs)}


def _read_table(source: Any, file_format: str, options: Mapping[str, Any] | None) -> Any:
    kwargs = dict(options or {})
    if file_format == "parquet":
        pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
        kwargs.setdefault("pre_buffer", True)
        return pq.read_table(source, **kwargs)
    pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
    ipc_kwargs = _ipc_options(kwargs, pa.ipc.IpcReadOptions)
    try:
        reader = pa.ipc.open_stream(source, **ipc_kwargs)
    except pa.ArrowInvalid:
        source.seek(0)
        reader = pa.ipc.open_file(source, **ipc_kwargs)
    with reader:
        return reader.read_all()
