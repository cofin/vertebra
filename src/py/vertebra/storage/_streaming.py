"""Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from vertebra.storage._store import ObjectStore

__all__ = ("StreamingStorageResponse",)


class StreamingStorageResponse(msgspec.Struct, frozen=True, gc=False):
    """Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

    store: ObjectStore | None = None
    """Underlying `ObjectStore` instance holding the target object."""
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

    def iter_chunks(self) -> Iterator[memoryview]:
        """Yield fixed-size zero-copy memoryview slices over the stored object."""
        if self.data is not None:
            view = memoryview(self.data)
            for offset in range(0, len(view), self.chunk_size):
                yield view[offset : offset + self.chunk_size]
            return
        if self.store is None:
            return
        view = self.store.get_view(self.key)
        if self.range_start is not None or self.range_end is not None:
            start = max(0, int(self.range_start or 0))
            end = int(self.range_end) + 1 if self.range_end is not None else len(view)
            view = view[start:end]
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]

    async def aiter_chunks(self) -> AsyncIterator[memoryview]:
        """Asynchronously yield fixed-size zero-copy memoryview slices over the stored object."""
        if self.data is not None:
            view = memoryview(self.data)
            for offset in range(0, len(view), self.chunk_size):
                yield view[offset : offset + self.chunk_size]
            return
        if self.store is None:
            return
        view = await self.store.get_view_async(self.key)
        if self.range_start is not None or self.range_end is not None:
            start = max(0, int(self.range_start or 0))
            end = int(self.range_end) + 1 if self.range_end is not None else len(view)
            view = view[start:end]
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]

    def to_fastpath_tuple(self) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
        """Serialize into a `(status_code, headers, body)` tuple for FastPath dispatch."""
        resolved_ct = self.content_type or self.media_type or "application/octet-stream"
        encoded_headers: list[tuple[bytes, bytes]] = [
            (b"content-type", resolved_ct.encode("latin-1")),
        ]
        if self.etag is not None:
            encoded_headers.append((b"etag", self.etag.encode("latin-1")))
        resolved_range = self.content_range
        if self.data is not None:
            payload = self.data
        elif self.store is not None and self.key:
            view = self.store.get_view(self.key)
            total_len = self.total_size or len(view)
            if self.range_start is not None or self.range_end is not None:
                start = max(0, int(self.range_start or 0))
                end = int(self.range_end) + 1 if self.range_end is not None else len(view)
                payload = bytes(view[start:end])
                if resolved_range is None:
                    resolved_range = f"bytes {start}-{max(start, end - 1)}/{total_len}"
            else:
                payload = bytes(view)
        else:
            payload = b""

        if resolved_range is not None:
            encoded_headers.append((b"content-range", resolved_range.encode("latin-1")))
        if self.signed_url is not None:
            url_bytes = self.signed_url.encode("latin-1")
            encoded_headers.append((b"location", url_bytes))
            encoded_headers.append((b"x-vertebra-signed-url", url_bytes))
        for hdr_key, hdr_val in self.headers.items():
            encoded_headers.append((str(hdr_key).lower().encode("latin-1"), str(hdr_val).encode("latin-1")))

        if not any(k == b"content-length" for k, _ in encoded_headers):
            encoded_headers.append((b"content-length", str(len(payload)).encode("ascii")))

        resolved_status = self.status_code
        if resolved_status == 200:
            if self.signed_url is not None and self.data is None and not self.key:
                resolved_status = 307
            elif resolved_range is not None:
                resolved_status = 206
        return resolved_status, encoded_headers, payload
