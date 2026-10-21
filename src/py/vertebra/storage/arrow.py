"""Seekable Arrow and Parquet range-reader file wrapper over ObjectStore."""

from __future__ import annotations

import io
from typing import Self

from vertebra.storage._store import ObjectStore

__all__ = ("ReadableFile",)


class ReadableFile(io.RawIOBase):
    """Seekable read-only file-like object backed by ObjectStore range reads (`get_range`).

    Enables PyArrow, Polars, and DataFusion to inspect Parquet footer metadata and read
    targeted row-group byte slices without downloading full multi-gigabyte blobs.
    """

    __slots__ = ("_closed", "_key", "_pos", "_size", "_store")

    def __init__(self, store: ObjectStore, key: str, *, size: int | None = None) -> None:
        super().__init__()
        self._store = store
        self._key = key
        self._size = store.head(key).size if size is None else size
        self._pos = 0
        self._closed = False

    @property
    def size(self) -> int:
        """Return total object byte size."""
        return self._size

    @property
    def closed(self) -> bool:
        """Return True if the file handle has been closed."""
        return self._closed

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the handle when exiting the context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def readable(self) -> bool:
        """Return True indicating this stream supports reading."""
        return True

    def seekable(self) -> bool:
        """Return True indicating this stream supports random access seeking."""
        return True

    def writable(self) -> bool:
        """Return False indicating this stream is read-only."""
        return False

    def tell(self) -> int:
        """Return the current cursor byte offset."""
        self._ensure_open()
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the read cursor to a new byte offset and return the updated position."""
        self._ensure_open()
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._pos + offset
        elif whence == io.SEEK_END:
            target = self._size + offset
        else:
            msg = f"Invalid whence value: {whence}"
            raise ValueError(msg)
        if target < 0:
            msg = f"Negative seek position: {target}"
            raise ValueError(msg)
        self._pos = min(target, self._size)
        return self._pos

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if self._pos >= self._size:
            return b""
        remaining = self._size - self._pos
        length = remaining if size < 0 else min(size, remaining)
        if length <= 0:
            return b""
        data = self._store.get_range(self._key, self._pos, length)
        self._pos += len(data)
        return data

    def read_at(self, offset: int, length: int) -> bytes:
        """Read length bytes at offset without mutating the stream cursor."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if offset >= self._size or length == 0:
            return b""
        bounded_len = min(length, self._size - offset)
        return self._store.get_range(self._key, offset, bounded_len)

    async def read_at_async(self, offset: int, length: int) -> bytes:
        """Asynchronously read length bytes at offset without mutating the stream cursor."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if offset >= self._size or length == 0:
            return b""
        bounded_len = min(length, self._size - offset)
        return await self._store.get_range_async(self._key, offset, bounded_len)

    def close(self) -> None:
        """Mark the file handle as closed."""
        self._closed = True
        super().close()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed ReadableFile"
            raise ValueError(msg)
