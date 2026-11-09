"""Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import msgspec

from vertebra.storage._store import AsyncObjectStore, ObjectMeta, SyncObjectStore
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

    from vertebra._storage import StorageBuffer

__all__ = ("ByteRange", "RangeNotSatisfiableError", "StreamingStorageResponse", "parse_byte_range")


class ByteRange(msgspec.Struct, frozen=True, gc=False):
    """Resolved single byte range with an exclusive upper bound."""

    start: int
    """Inclusive byte offset."""
    end_exclusive: int
    """Exclusive byte offset."""
    total_size: int
    """Full representation size in bytes."""

    @property
    def content_length(self) -> int:
        """Return the number of selected bytes."""
        return self.end_exclusive - self.start

    @property
    def content_range_header(self) -> str:
        """Return the HTTP Content-Range field value."""
        return f"bytes {self.start}-{self.end_exclusive - 1}/{self.total_size}"


class RangeNotSatisfiableError(ValueError):
    """A byte range cannot select bytes from the representation."""

    def __init__(self, *, total_size: int) -> None:
        self.total_size = total_size
        super().__init__(f"Range not satisfiable for {total_size} bytes")


def parse_byte_range(range_header: str | None, total_size: int) -> ByteRange | None:
    """Resolve a single bytes range; ignore unknown range units.

    Raises:
        RangeNotSatisfiableError: The bytes range is malformed or unsatisfiable.
    """
    if range_header is None:
        return None
    unit, separator, value = range_header.partition("=")
    if unit.strip() != "bytes":
        return None
    match = re.fullmatch(r"([0-9]*)-([0-9]*)", value.strip()) if separator else None
    if match is None or total_size <= 0 or not any(match.groups()):
        raise RangeNotSatisfiableError(total_size=total_size)
    first, last = match.groups()
    try:
        if not first:
            suffix = int(last)
            if suffix == 0:
                raise RangeNotSatisfiableError(total_size=total_size)
            return ByteRange(max(0, total_size - suffix), total_size, total_size)
        start = int(first)
        end = min(int(last) + 1, total_size) if last else total_size
    except ValueError as exc:
        raise RangeNotSatisfiableError(total_size=total_size) from exc
    if start >= total_size or start >= end:
        raise RangeNotSatisfiableError(total_size=total_size)
    return ByteRange(start, end, total_size)


class StreamingStorageResponse(msgspec.Struct, frozen=True, gc=False):
    """Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

    store: SyncObjectStore | AsyncObjectStore | None = None
    """Underlying object store instance holding the target object."""
    key: str = ""
    """Object key path within `store`."""
    total_size: int = 0
    """Total byte length of the stored object."""
    chunk_size: int = 65536
    """Maximum byte length yielded per chunk iteration."""
    media_type: str = "application/octet-stream"
    """HTTP `Content-Type` media type header for the streamed response."""
    status_code: int = 200
    """HTTP status code returned with the streamed response."""
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    """Additional HTTP response headers attached to the stream."""
    content_type: str | None = None
    """Optional explicit HTTP `Content-Type` override for the streamed object."""
    etag: str | None = None
    """Optional HTTP `ETag` header value for the stored object."""
    content_range: str | None = None
    """Optional HTTP `Content-Range` header value for byte-range responses."""
    range_start: int | None = None
    """Optional zero-based inclusive start byte offset for range slicing."""
    range_end: int | None = None
    """Optional zero-based inclusive end byte offset for range slicing."""
    signed_url: str | None = None
    """Optional presigned object URL for redirect or header forwarding."""
    data: bytes | None = None
    """Optional pre-loaded object bytes when streaming without a backing store."""

    @classmethod
    async def from_async_store(
        cls,
        store: AsyncObjectStore,
        key: str,
        *,
        range_header: str | None = None,
        content_type: str | None = None,
        headers: Mapping[str, str] | None = None,
        chunk_size: int = 65536,
    ) -> StreamingStorageResponse:
        """Resolve response metadata with one asynchronous object HEAD."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        meta = await store.head(key)
        return cls._from_meta(store, key, meta, range_header, content_type, headers, chunk_size)

    @classmethod
    def from_sync_store(
        cls,
        store: SyncObjectStore,
        key: str,
        *,
        range_header: str | None = None,
        content_type: str | None = None,
        headers: Mapping[str, str] | None = None,
        chunk_size: int = 65536,
    ) -> StreamingStorageResponse:
        """Resolve response metadata with one synchronous object HEAD."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        return cls._from_meta(store, key, store.head(key), range_header, content_type, headers, chunk_size)

    def iter_chunks(self, chunk_size: int | None = None) -> Iterator[memoryview]:
        """Yield zero-copy memoryview chunks of at most `chunk_size` bytes.

        Full responses stream through `SyncObjectStore.get_stream`; byte-range responses read
        bounded `SyncObjectStore.get_range` windows, so the full object is never materialized.

        Args:
            chunk_size: Maximum bytes per chunk; defaults to the descriptor's `chunk_size`.

        Raises:
            ValueError: `chunk_size` is not positive.
            ImproperConfigurationError: The descriptor is backed by an `AsyncObjectStore`.
        """
        step = self.chunk_size if chunk_size is None else chunk_size
        if step <= 0:
            raise ValueError("chunk_size must be positive")
        if self.status_code == 416:
            return
        if self.data is not None:
            yield from self._slice_view(memoryview(self.data), step)
            return
        if self.store is None:
            return
        if isinstance(self.store, AsyncObjectStore):
            msg = "AsyncObjectStore cannot be used with synchronous iter_chunks; use aiter_chunks or SyncObjectStore"
            raise ImproperConfigurationError(msg)
        if self.range_start is not None or self.range_end is not None:
            selected = self._resolve_sync_range(self.store)
            for offset in range(selected.start, selected.end_exclusive, step):
                length = min(step, selected.end_exclusive - offset)
                yield memoryview(self.store.get_range(self.key, offset, length))
            return
        if self.total_size == 0:
            return
        for buffer in self.store.get_stream(self.key, chunk_size=step):
            yield from self._slice_view(memoryview(buffer), step)

    async def aiter_chunks(self) -> AsyncIterator[StorageBuffer | bytes | memoryview]:
        """Yield native async chunks or bounded async range reads."""
        if self.status_code == 416:
            return
        if not isinstance(self.store, AsyncObjectStore) or self.data is not None:
            for chunk in self.iter_chunks():
                yield chunk
            return
        if self.range_start is not None or self.range_end is not None:
            start = max(0, self.range_start or 0)
            end = self.range_end + 1 if self.range_end is not None else self.total_size
            if self.range_end is None and not self.total_size:
                end = (await self.store.head(self.key)).size
            for offset in range(start, end, self.chunk_size):
                yield await self.store.get_range(self.key, offset, min(self.chunk_size, end - offset))
            return
        async for native_chunk in self.store.get_stream(self.key, chunk_size=self.chunk_size):
            yield native_chunk

    async def to_async_fastpath_tuple(self) -> tuple[int, list[tuple[bytes, bytes]], memoryview | bytes]:
        """Resolve async storage without calling synchronous store methods."""
        content_range = self.content_range
        if self.status_code == 416:
            payload: memoryview | bytes = b""
        elif isinstance(self.store, AsyncObjectStore) and self.data is None:
            if self.range_start is not None or self.range_end is not None:
                start = max(0, self.range_start or 0)
                total_size = self.total_size or (await self.store.head(self.key)).size
                end = min(self.range_end + 1, total_size) if self.range_end is not None else total_size
                payload = await self.store.get_range(self.key, start, end - start)
                if content_range is None:
                    content_range = ByteRange(start, end, total_size).content_range_header
            else:
                payload = await self.store.get_view(self.key)
        else:
            return self.to_fastpath_tuple()
        descriptor = msgspec.structs.replace(self, store=None, data=b"", content_range=content_range)
        status, headers, _ = descriptor.to_fastpath_tuple()
        headers = [(key, value) for key, value in headers if key != b"content-length"]
        headers.append((b"content-length", str(len(payload)).encode("ascii")))
        return status, headers, payload

    def to_fastpath_tuple(self) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview]:
        """Serialize into a `(status_code, headers, body)` tuple for FastPath dispatch.

        Byte-range bodies are fetched with one `SyncObjectStore.get_range` call covering only the
        selected bytes; full bodies are returned as a zero-copy view over the native buffer.
        """
        if self.store is not None and isinstance(self.store, AsyncObjectStore) and self.data is None and self.key:
            msg = (
                "AsyncObjectStore cannot be used with synchronous to_fastpath_tuple; "
                "use SyncObjectStore or pre-load response data"
            )
            raise TypeError(msg)
        resolved_ct = self.content_type or self.media_type or "application/octet-stream"
        encoded_headers: list[tuple[bytes, bytes]] = [
            (b"content-type", resolved_ct.encode("latin-1")),
        ]
        if self.etag is not None:
            encoded_headers.append((b"etag", self._format_etag(self.etag).encode("latin-1")))
        resolved_range = self.content_range
        payload: bytes | memoryview
        if self.status_code == 416:
            payload = b""
        elif self.data is not None:
            payload = self.data
        elif isinstance(self.store, SyncObjectStore) and self.key:
            if self.range_start is not None or self.range_end is not None:
                selected = self._resolve_sync_range(self.store)
                payload = self.store.get_range(self.key, selected.start, selected.content_length)
                if resolved_range is None:
                    last = max(selected.start, selected.end_exclusive - 1)
                    resolved_range = f"bytes {selected.start}-{last}/{selected.total_size}"
            elif self.total_size == 0:
                payload = b""
            else:
                payload = self.store.get_view(self.key)
        else:
            payload = b""

        if resolved_range is not None:
            encoded_headers.append((b"content-range", resolved_range.encode("latin-1")))
        if self.signed_url is not None:
            url_bytes = self.signed_url.encode("latin-1")
            encoded_headers.append((b"location", url_bytes))
            encoded_headers.append((b"x-vertebra-signed-url", url_bytes))
        for hdr_key, hdr_val in self.headers.items():
            name = str(hdr_key).lower()
            value = self._format_etag(str(hdr_val)) if name == "etag" else str(hdr_val)
            encoded_headers.append((name.encode("latin-1"), value.encode("latin-1")))

        if not any(k == b"content-length" for k, _ in encoded_headers):
            encoded_headers.append((b"content-length", str(len(payload)).encode("ascii")))

        resolved_status = self.status_code
        if resolved_status == 200:
            if self.signed_url is not None and self.data is None and not self.key:
                resolved_status = 307
            elif resolved_range is not None:
                resolved_status = 206
        return resolved_status, encoded_headers, payload

    @classmethod
    def _from_meta(
        cls,
        store: SyncObjectStore | AsyncObjectStore,
        key: str,
        meta: ObjectMeta,
        range_header: str | None,
        content_type: str | None,
        headers: Mapping[str, str] | None,
        chunk_size: int,
    ) -> StreamingStorageResponse:
        """Build authoritative range headers from object metadata."""
        status = 200
        selected = None
        content_range = None
        length = meta.size
        try:
            selected = parse_byte_range(range_header, meta.size)
        except RangeNotSatisfiableError:
            status = 416
            content_range = f"bytes */{meta.size}"
            length = 0
        if selected is not None:
            status = 206
            content_range = selected.content_range_header
            length = selected.content_length
        response_headers = {
            name.lower(): value
            for name, value in (headers or {}).items()
            if name.lower() not in {"content-range", "content-length", "accept-ranges", "etag", "content-type"}
        }
        response_headers["accept-ranges"] = "bytes"
        response_headers["content-length"] = str(length)
        return cls(
            store=store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
            status_code=status,
            content_type=content_type or meta.content_type,
            etag=cls._format_etag(meta.etag) if meta.etag is not None else None,
            content_range=content_range,
            range_start=selected.start if selected is not None else None,
            range_end=selected.end_exclusive - 1 if selected is not None else None,
            headers=response_headers,
        )

    @staticmethod
    def _format_etag(value: str) -> str:
        """Quote raw backend tokens and reject invalid RFC 9110 opaque tags."""
        opaque = value[2:] if value.startswith('W/"') else value
        quoted = opaque.startswith('"') and opaque.endswith('"') and len(opaque) >= 2
        token = opaque[1:-1] if quoted else opaque
        if any(not (ord(char) == 0x21 or 0x23 <= ord(char) <= 0x7E or 0x80 <= ord(char) <= 0xFF) for char in token):
            raise ValueError("ETag contains invalid opaque tag characters")
        return value if quoted else f'"{value}"'

    @staticmethod
    def _slice_view(view: memoryview, step: int) -> Iterator[memoryview]:
        """Yield zero-copy slices of at most `step` bytes over `view`."""
        for offset in range(0, len(view), step):
            yield view[offset : offset + step]

    def _resolve_sync_range(self, store: SyncObjectStore) -> ByteRange:
        """Clamp the descriptor's inclusive range to the object size, issuing a HEAD only when the size is unknown."""
        total_size = self.total_size or store.head(self.key).size
        start = max(0, self.range_start or 0)
        end = min(self.range_end + 1, total_size) if self.range_end is not None else total_size
        return ByteRange(start, max(start, end), total_size)
