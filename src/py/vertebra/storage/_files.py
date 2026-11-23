"""Shared sync and async readable/writable object storage file handles."""

from __future__ import annotations

import contextlib
import io
import warnings
from collections.abc import AsyncIterator, Buffer, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from vertebra.storage._config import PutResult
    from vertebra.storage._store import AsyncObjectStore, SyncObjectStore

__all__ = (
    "AsyncReadableFile",
    "AsyncWritableFile",
    "SyncReadableFile",
    "SyncWritableFile",
)


def _take_part(buffer: bytearray, part_size: int) -> bytes:
    """Copy the next part out of a writer's rolling buffer as immutable bytes and drop it from the buffer."""
    size = min(part_size, len(buffer))
    with memoryview(buffer) as view:
        part = bytes(view[:size])
    del buffer[:size]
    return part


class _ReadableFileMixin:
    """Shared cursor, range-seek, and read-ahead validation helpers for readable object handles."""

    if not TYPE_CHECKING:
        __slots__ = ()

    _buf: bytes
    _buf_start: int
    _chunk_size: int
    _closed: bool
    _expected_sha256: str | None
    _key: str
    _pos: int
    _read_ahead: int
    _size: int | None
    _store: Any
    _stream_buf: bytes
    _stream_eof: bool
    _stream_reader: Any

    def _init_readable(
        self,
        store: Any,
        key: str,
        *,
        size: int | None,
        chunk_size: int,
        read_ahead: int,
        expected_sha256: str | None,
    ) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        if read_ahead < 0:
            raise ValueError("read_ahead must be >= 0")
        store._validate_expected_hex("expected_sha256", expected_sha256, 64)
        self._store = store
        self._key = key
        self._size = size
        self._chunk_size = chunk_size
        self._read_ahead = read_ahead
        self._expected_sha256 = expected_sha256
        self._buf = b""
        self._buf_start = 0
        self._pos = 0
        self._stream_buf = b""
        self._stream_eof = False
        self._stream_reader = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True if the file handle has been closed."""
        return self._closed

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
            if self._size is None:
                raise ValueError(f"Seeking relative to SEEK_END on {type(self).__name__} requires known size")
            target = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")
        if target < 0:
            raise ValueError(f"Negative seek position: {target}")
        clamped = min(target, self._size) if self._size is not None else target
        if self._expected_sha256 is not None:
            if clamped not in {0, self._pos}:
                raise io.UnsupportedOperation(
                    "Random-access seek to non-zero offset is not supported when expected_sha256 is set"
                )
            if clamped == 0:
                reader, self._stream_reader = self._stream_reader, None
                if reader is not None:
                    reader.close()
                self._stream_buf = b""
                self._stream_eof = False
        self._pos = clamped
        return self._pos

    def _validate_read_at(self, offset: int, length: int) -> None:
        self._ensure_open()
        if self._expected_sha256 is not None:
            raise io.UnsupportedOperation("Random-access read_at is not supported when expected_sha256 is set")
        if offset < 0 or length < 0:
            raise ValueError("offset and length must be non-negative")

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError(f"I/O operation on closed {type(self).__name__}")

    def _open_stream_reader(self) -> Any:
        if self._stream_reader is None:
            self._stream_reader = self._store._require_native().get_stream(
                self._store._key(self._key),
                chunk_size=self._chunk_size,
                expected_sha256=self._expected_sha256,
            )
        return self._stream_reader

    def _detach_buffers(self) -> Any:
        self._closed = True
        self._buf = b""
        self._stream_buf = b""
        reader, self._stream_reader = self._stream_reader, None
        return reader


class SyncReadableFile(_ReadableFileMixin, io.RawIOBase):
    """Seekable synchronous read-only file-like object backed by SyncObjectStore range and stream reads."""

    __slots__ = (
        "_buf",
        "_buf_start",
        "_chunk_size",
        "_closed",
        "_expected_sha256",
        "_key",
        "_pos",
        "_read_ahead",
        "_size",
        "_store",
        "_stream_buf",
        "_stream_eof",
        "_stream_reader",
    )

    _store: SyncObjectStore

    def __init__(
        self,
        store: SyncObjectStore,
        key: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
        expected_sha256: str | None = None,
    ) -> None:
        super().__init__()
        self._init_readable(
            store,
            key,
            size=size,
            chunk_size=chunk_size,
            read_ahead=read_ahead,
            expected_sha256=expected_sha256,
        )
        self._size = store.head(key).size if size is None else size

    @property
    def size(self) -> int:
        """Return total object byte size."""
        assert self._size is not None
        return self._size

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the handle when exiting the context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def __iter__(self) -> Iterator[bytes]:
        """Yield byte chunks from the current cursor position to EOF without buffering the full object."""
        self._ensure_open()
        if self._pos == 0 and self._expected_sha256 is None:
            for buf in self._store.get_stream(self._key, chunk_size=self._chunk_size):
                self._ensure_open()
                chunk = bytes(buf)
                if chunk:
                    self._pos += len(chunk)
                    yield chunk
            return
        while chunk := self.read(self._chunk_size):
            yield chunk

    def read(self, size: int | None = -1) -> bytes:
        """Read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if self._expected_sha256 is not None:
            return self._read_verified(size)
        total_size = self.size
        if size == 0 or self._pos >= total_size:
            return b""
        remaining = total_size - self._pos
        length = remaining if (size is None or size < 0) else min(size, remaining)
        if length <= 0:
            return b""
        offset = self._pos - self._buf_start
        if self._read_ahead == 0 or length >= self._read_ahead:
            data = self._store.get_range(self._key, self._pos, length)
        elif offset >= 0 and offset + length <= len(self._buf):
            data = self._buf[offset : offset + length]
        else:
            self._buf = self._store.get_range(self._key, self._pos, min(self._read_ahead, remaining))
            self._buf_start = self._pos
            data = self._buf[:length]
        self._pos += len(data)
        return data

    def readinto(self, b: Buffer) -> int:
        """Read bytes directly into a writable buffer and return the byte count."""
        self._ensure_open()
        view = memoryview(b).cast("B")
        if len(view) == 0:
            return 0
        data = self.read(len(view))
        count = len(data)
        view[:count] = data
        return count

    def read_at(self, offset: int, length: int) -> bytes:
        """Read length bytes at offset without mutating the stream cursor or read-ahead window."""
        self._validate_read_at(offset, length)
        total_size = self.size
        if offset >= total_size or length == 0:
            return b""
        return self._store.get_range(self._key, offset, min(length, total_size - offset))

    def close(self) -> None:
        """Mark the file handle as closed and release the read-ahead window."""
        reader = self._detach_buffers()
        if reader is not None:
            reader.close()
        super().close()

    def _read_verified(self, size: int | None) -> bytes:
        if size == 0 or self._stream_eof:
            return b""
        reader = self._open_stream_reader()
        if size is None or size < 0:
            chunks = [self._stream_buf] if self._stream_buf else []
            self._stream_buf = b""
            while (buf := reader.next_chunk()) is not None:
                chunks.append(bytes(buf))
            self._stream_eof = True
            data = b"".join(chunks)
            self._pos += len(data)
            return data
        collected = bytearray(self._stream_buf)
        self._stream_buf = b""
        while len(collected) < size:
            buf = reader.next_chunk()
            if buf is None:
                self._stream_eof = True
                break
            collected += memoryview(buf)
        if len(collected) > size:
            self._stream_buf = bytes(collected[size:])
            del collected[size:]
        elif not self._stream_eof and self._pos + len(collected) >= self.size:
            while (extra := reader.next_chunk()) is not None:
                self._stream_buf += bytes(extra)
            if not self._stream_buf:
                self._stream_eof = True
        data = bytes(collected)
        self._pos += len(data)
        return data


class AsyncReadableFile(_ReadableFileMixin):
    """Seekable asynchronous read-only stream backed by AsyncObjectStore range and stream reads."""

    __slots__ = (
        "_buf",
        "_buf_start",
        "_chunk_size",
        "_closed",
        "_expected_sha256",
        "_key",
        "_pos",
        "_read_ahead",
        "_size",
        "_store",
        "_stream_buf",
        "_stream_eof",
        "_stream_reader",
    )

    _store: AsyncObjectStore

    def __init__(
        self,
        store: AsyncObjectStore,
        key: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
        expected_sha256: str | None = None,
    ) -> None:
        self._init_readable(
            store, key, size=size, chunk_size=chunk_size, read_ahead=read_ahead, expected_sha256=expected_sha256
        )

    @property
    def size(self) -> int | None:
        """Return total object byte size when resolved."""
        return self._size

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return self."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the handle when exiting the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronously yield byte chunks from the current cursor position to EOF."""
        self._ensure_open()
        if self._pos == 0 and self._expected_sha256 is None:
            async for buf in self._store.get_stream(self._key, chunk_size=self._chunk_size):
                self._ensure_open()
                chunk = bytes(buf)
                if chunk:
                    self._pos += len(chunk)
                    yield chunk
            if self._size is None:
                self._size = self._pos
            return
        while chunk := await self.read(self._chunk_size):
            yield chunk

    async def read(self, size: int | None = -1) -> bytes:
        """Asynchronously read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if self._expected_sha256 is not None:
            return await self._read_verified(size)
        if size == 0:
            return b""
        total_size = await self._ensure_size()
        if self._pos >= total_size:
            return b""
        remaining = total_size - self._pos
        length = remaining if (size is None or size < 0) else min(size, remaining)
        if length <= 0:
            return b""
        offset = self._pos - self._buf_start
        if self._read_ahead == 0 or length >= self._read_ahead:
            data = await self._store.get_range(self._key, self._pos, length)
        elif offset >= 0 and offset + length <= len(self._buf):
            data = self._buf[offset : offset + length]
        else:
            self._buf = await self._store.get_range(self._key, self._pos, min(self._read_ahead, remaining))
            self._buf_start = self._pos
            data = self._buf[:length]
        self._pos += len(data)
        return data

    async def readinto(self, b: Buffer) -> int:
        """Asynchronously read bytes into a writable buffer and return the byte count."""
        self._ensure_open()
        view = memoryview(b).cast("B")
        if len(view) == 0:
            return 0
        data = await self.read(len(view))
        count = len(data)
        view[:count] = data
        return count

    async def read_at(self, offset: int, length: int) -> bytes:
        """Asynchronously read length bytes at offset without mutating the stream cursor or read-ahead window."""
        self._validate_read_at(offset, length)
        if length == 0:
            return b""
        total_size = await self._ensure_size()
        if offset >= total_size:
            return b""
        return await self._store.get_range(self._key, offset, min(length, total_size - offset))

    async def aclose(self) -> None:
        """Asynchronously close any active native stream reader and mark the file handle closed."""
        reader = self._detach_buffers()
        if reader is not None:
            await reader.aclose()

    def close(self) -> None:
        """Close any active native stream reader and mark the file handle as closed."""
        reader = self._detach_buffers()
        if reader is not None:
            reader.close()

    async def _ensure_size(self) -> int:
        if self._size is None:
            self._size = (await self._store.head(self._key)).size
        return self._size

    async def _read_verified(self, size: int | None) -> bytes:
        if size == 0 or self._stream_eof:
            return b""
        reader = self._open_stream_reader()
        if size is None or size < 0:
            chunks = [self._stream_buf] if self._stream_buf else []
            self._stream_buf = b""
            while (buf := await reader.next_chunk()) is not None:
                chunks.append(bytes(buf))
            self._stream_eof = True
            data = b"".join(chunks)
            self._pos += len(data)
            if self._size is None:
                self._size = self._pos
            return data
        collected = bytearray(self._stream_buf)
        self._stream_buf = b""
        while len(collected) < size:
            buf = await reader.next_chunk()
            if buf is None:
                self._stream_eof = True
                break
            collected += memoryview(buf)
        if len(collected) > size:
            self._stream_buf = bytes(collected[size:])
            del collected[size:]
        elif self._size is not None and not self._stream_eof and self._pos + len(collected) >= self._size:
            while (extra := await reader.next_chunk()) is not None:
                self._stream_buf += bytes(extra)
            if not self._stream_buf:
                self._stream_eof = True
        data = bytes(collected)
        self._pos += len(data)
        if self._stream_eof and self._size is None:
            self._size = self._pos
        return data


class _WritableFileMixin:
    """Shared buffer setup, digest validation, and multipart-option packing for writable object handles."""

    if not TYPE_CHECKING:
        __slots__ = ()

    _attributes: dict[str, str] | None
    _buffer: bytearray
    _checksum: bool
    _closed: bool
    _content_type: str | None
    _expected_crc32c: str | None
    _expected_md5: str | None
    _expected_sha256: str | None
    _if_match: str | None
    _if_none_match: str | None
    _if_not_exists: bool
    _key: str
    _part_size: int
    _result: PutResult | None
    _store: Any
    _upload: Any
    _written: int

    def _init_writable(
        self,
        store: Any,
        key: str,
        *,
        content_type: str | None,
        attributes: Mapping[str, str] | None,
        part_size: int,
        if_not_exists: bool,
        if_match: str | None,
        if_none_match: str | None,
        checksum: bool,
        expected_sha256: str | None,
        expected_crc32c: str | None,
        expected_md5: str | None,
    ) -> None:
        self._closed = True
        self._upload = None
        if part_size <= 0:
            raise ValueError("part_size must be > 0")
        self._store = store
        self._key = store._key(key)
        store._validate_expected_digests(
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )
        self._content_type = content_type
        self._attributes = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        self._part_size = part_size
        self._if_not_exists = if_not_exists
        self._if_match = if_match
        self._if_none_match = if_none_match
        self._checksum = checksum
        self._expected_sha256 = expected_sha256
        self._expected_crc32c = expected_crc32c
        self._expected_md5 = expected_md5
        self._buffer = bytearray()
        self._written = 0
        self._result = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True once the writer has been completed or aborted."""
        return self._closed

    @property
    def result(self) -> PutResult | None:
        """Return the completed upload metadata, or None until a successful close."""
        return self._result

    def readable(self) -> bool:
        """Return False indicating this stream is write-only."""
        return False

    def seekable(self) -> bool:
        """Return False indicating this stream only appends."""
        return False

    def writable(self) -> bool:
        """Return True indicating this stream supports writing."""
        return True

    def tell(self) -> int:
        """Return the total number of bytes accepted by ``write``."""
        self._ensure_open()
        return self._written

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError(f"I/O operation on closed {type(self).__name__}")

    def _append_buffer(self, b: Buffer) -> int:
        self._ensure_open()
        with memoryview(b) as view:
            count = view.nbytes
            self._buffer += view
        self._written += count
        return count

    def _multipart_kwargs(self) -> dict[str, Any]:
        return {
            "content_type": self._content_type,
            "attributes": self._attributes,
            "if_not_exists": self._if_not_exists,
            "if_match": self._if_match,
            "if_none_match": self._if_none_match,
            "checksum": self._checksum,
            "expected_sha256": self._expected_sha256,
            "expected_crc32c": self._expected_crc32c,
            "expected_md5": self._expected_md5,
        }

    def _finalize_result(self, native_meta: Any) -> None:
        self._upload = None
        self._closed = True
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        self._result = self._store._native_to_put_result(native_meta, fallback_etag)


class SyncWritableFile(_WritableFileMixin, io.RawIOBase):
    """Write-only synchronous stream uploading buffered ``part_size`` slices through a native multipart upload."""

    __slots__ = (
        "_attributes",
        "_buffer",
        "_checksum",
        "_closed",
        "_content_type",
        "_expected_crc32c",
        "_expected_md5",
        "_expected_sha256",
        "_if_match",
        "_if_none_match",
        "_if_not_exists",
        "_key",
        "_part_size",
        "_result",
        "_store",
        "_upload",
        "_written",
    )

    _store: SyncObjectStore

    def __init__(
        self,
        store: SyncObjectStore,
        key: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
    ) -> None:
        super().__init__()
        self._init_writable(
            store,
            key,
            content_type=content_type,
            attributes=attributes,
            part_size=part_size,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Complete the upload on success or abort when an exception escapes."""
        _ = (exc_val, exc_tb)
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def __del__(self) -> None:
        """Abort an unclosed upload on collection so a partial object is never committed."""
        if not self._closed:
            with contextlib.suppress(Exception):
                self.abort()

    def write(self, b: Buffer, /) -> int:
        """Copy a buffer into the rolling part buffer, upload every full part, and return the byte count."""
        count = self._append_buffer(b)
        try:
            while len(self._buffer) >= self._part_size:
                self._ensure_upload().write_part(_take_part(self._buffer, self._part_size))
        except BaseException:
            self.abort()
            raise
        return count

    def close(self) -> None:
        """Upload the buffered tail and complete the multipart upload, aborting if completion fails."""
        if self._closed:
            return
        try:
            upload = self._ensure_upload()
            if self._buffer:
                upload.write_part(_take_part(self._buffer, self._part_size))
            native_meta = upload.complete()
        except BaseException:
            self.abort()
            raise
        self._finalize_result(native_meta)
        super().close()

    def abort(self) -> None:
        """Discard buffered bytes and abort the multipart upload without committing an object."""
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        upload, self._upload = self._upload, None
        if upload is not None:
            with contextlib.suppress(Exception):
                upload.abort()
        super().close()

    def _ensure_upload(self) -> Any:
        if self._upload is None:
            self._upload = self._store._require_native().create_multipart(self._key, **self._multipart_kwargs())
        return self._upload


class AsyncWritableFile(_WritableFileMixin):
    """Write-only asynchronous stream uploading buffered ``part_size`` slices through a native multipart upload."""

    __slots__ = (
        "_attributes",
        "_buffer",
        "_checksum",
        "_closed",
        "_content_type",
        "_expected_crc32c",
        "_expected_md5",
        "_expected_sha256",
        "_if_match",
        "_if_none_match",
        "_if_not_exists",
        "_key",
        "_part_size",
        "_result",
        "_store",
        "_upload",
        "_written",
    )

    _store: AsyncObjectStore

    def __init__(
        self,
        store: AsyncObjectStore,
        key: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
    ) -> None:
        self._init_writable(
            store,
            key,
            content_type=content_type,
            attributes=attributes,
            part_size=part_size,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return self."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Complete the upload on success or abort when an exception escapes."""
        _ = (exc_val, exc_tb)
        if exc_type is None:
            await self.aclose()
        else:
            await self.abort()

    def __del__(self) -> None:
        """Warn when an open writer is collected, since a finalizer cannot await the native abort."""
        if not self._closed:
            warnings.warn(
                f"AsyncWritableFile for {self._key!r} was garbage collected without aclose() or abort(); "
                "the multipart upload was never completed or aborted",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )

    async def flush(self) -> None:
        """Validate that the writer is open; buffered bytes stay pending until a full part or ``aclose()``."""
        self._ensure_open()

    async def write(self, b: Buffer, /) -> int:
        """Copy a buffer into the rolling part buffer, upload every full part, and return the byte count."""
        count = self._append_buffer(b)
        try:
            while len(self._buffer) >= self._part_size:
                upload = await self._ensure_upload()
                await upload.write_part(_take_part(self._buffer, self._part_size))
        except BaseException:
            await self.abort()
            raise
        return count

    async def aclose(self) -> PutResult | None:
        """Upload the buffered tail, complete the multipart upload, and return its metadata."""
        if self._closed:
            return self._result
        try:
            upload = await self._ensure_upload()
            if self._buffer:
                await upload.write_part(_take_part(self._buffer, self._part_size))
            native_meta = await upload.complete()
        except BaseException:
            await self.abort()
            raise
        self._finalize_result(native_meta)
        return self._result

    async def abort(self) -> None:
        """Discard buffered bytes and abort the multipart upload without committing an object."""
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        upload, self._upload = self._upload, None
        if upload is not None:
            with contextlib.suppress(Exception):
                await upload.abort()

    async def _ensure_upload(self) -> Any:
        if self._upload is None:
            self._upload = await self._store._require_native().create_multipart(self._key, **self._multipart_kwargs())
        return self._upload
