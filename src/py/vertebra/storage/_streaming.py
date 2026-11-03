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

    store: ObjectStore
    """Underlying `ObjectStore` instance holding the target object."""
    key: str
    """Object key path within `store`."""
    total_size: int
    """Total byte length of the stored object."""
    chunk_size: int = 65536
    """Maximum byte length yielded per chunk iteration."""
    media_type: str = "application/octet-stream"
    """HTTP `Content-Type` media type header for the streamed response."""
    status_code: int = 200
    """HTTP status code returned with the streamed response."""
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    """Additional HTTP response headers attached to the stream."""

    def iter_chunks(self) -> Iterator[memoryview]:
        """Yield fixed-size zero-copy memoryview slices over the stored object."""
        view = self.store.get_view(self.key)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]

    async def aiter_chunks(self) -> AsyncIterator[memoryview]:
        """Asynchronously yield fixed-size zero-copy memoryview slices over the stored object."""
        view = await self.store.get_view_async(self.key)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]
