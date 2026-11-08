"""Demand-driven Arrow codecs over native storage streams and ranges."""

from __future__ import annotations

import asyncio
import contextlib
import io
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Generator, Iterator, Mapping, Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, cast

from vertebra.utils.module_loader import ensure_module
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from vertebra.storage._store import AsyncObjectStore, AsyncReadableFile, SyncObjectStore


class _CodecBridge:
    """Run codec steps off-loop while retaining ownership until native I/O unwinds."""

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
                await asyncio.shield(self._worker)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

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


class _RangeFile(io.RawIOBase):
    """Expose an asynchronous native range reader to synchronous codec callbacks."""

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

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(buffer))
        memoryview(buffer).cast("B")[: len(data)] = data
        return len(data)


class _StreamFile(io.RawIOBase):
    """Read exact codec requests with at most one unfinished native stream chunk."""

    def __init__(self, next_chunk: Callable[[], Any]) -> None:
        self._next_chunk = next_chunk
        self._remaining = memoryview(b"")
        self._pos = 0

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            raise io.UnsupportedOperation("Arrow streaming requires a bounded read size")
        result = bytearray()
        while len(result) < size:
            if not self._remaining:
                chunk = self._next_chunk()
                if chunk is None:
                    break
                self._remaining = memoryview(chunk).cast("B")
            count = min(size - len(result), len(self._remaining))
            result.extend(self._remaining[:count])
            self._remaining = self._remaining[count:]
        self._pos += len(result)
        return bytes(result)

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(buffer))
        memoryview(buffer).cast("B")[: len(data)] = data
        return len(data)


class _BatchReader:
    """Retain one decoder and its schema/dictionary state across batch requests."""

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
            self.reader = ipc.open_stream(source, **kwargs)
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


def iter_batches_sync(
    store: SyncObjectStore,
    key: str,
    *,
    file_format: str,
    batch_size: int = 65536,
    columns: Sequence[str] | None = None,
    options: Mapping[str, Any] | None = None,
) -> Iterator[Any]:
    """Decode batches on demand using native synchronous streams or ranges."""
    _validate_batch_size(batch_size)
    stream = None if file_format == "parquet" else store.get_stream(key)
    source = store.open_reader(key) if stream is None else _StreamFile(lambda: next(stream, None))
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
    """Decode one batch per demand while native asynchronous I/O stays on-loop."""
    _validate_batch_size(batch_size)
    bridge = _CodecBridge()
    stream = None if file_format == "parquet" else store.get_stream(key)
    backing = store.open_reader(key, size=(await store.head(key)).size) if stream is None else None
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
    """Read a materialized Table through seekable native ranges, retaining its schema."""
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
    """Read a Table off-loop while native range I/O remains on the caller loop."""
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


def _validate_batch_size(batch_size: int) -> None:
    ensure_module("pyarrow", package="pyarrow", extra="arrow")
    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)


def _read_table(source: Any, file_format: str, options: Mapping[str, Any] | None) -> Any:
    if file_format == "parquet":
        pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
        return pq.read_table(source, **dict(options or {}))
    pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
    try:
        reader = pa.ipc.open_stream(source, **dict(options or {}))
    except pa.ArrowInvalid:
        source.seek(0)
        reader = pa.ipc.open_file(source, **dict(options or {}))
    with reader:
        return reader.read_all()
