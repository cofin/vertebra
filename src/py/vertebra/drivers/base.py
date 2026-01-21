"""Base classes and protocols for Bun-backed drivers."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from vertebra.protocols import AsyncConnection, AsyncCursor, AsyncPool, SyncConnection, SyncCursor, SyncPool

if TYPE_CHECKING:
    from collections.abc import Iterator


class SyncDriver(ABC):
    """Abstract base class for sync driver implementations."""

    @abstractmethod
    def connect(self) -> SyncConnection:
        """Open a new connection."""

    @abstractmethod
    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncPool[SyncConnection]:
        """Create a connection pool."""

    def iter_rows(self, cursor: SyncCursor) -> "Iterator[Any]":
        """Iterate rows from a cursor until exhausted."""

        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row


class AsyncDriver(ABC):
    """Abstract base class for async driver implementations."""

    @abstractmethod
    async def connect(self) -> AsyncConnection:
        """Open a new async connection."""

    @abstractmethod
    async def pool(self, min_size: int = 1, max_size: int = 10) -> AsyncPool[AsyncConnection]:
        """Create an async connection pool."""


__all__ = (
    "AsyncConnection",
    "AsyncCursor",
    "AsyncDriver",
    "AsyncPool",
    "SyncConnection",
    "SyncCursor",
    "SyncDriver",
    "SyncPool",
)
