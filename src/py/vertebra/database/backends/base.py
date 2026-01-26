"""Base classes and protocols for Bun-backed drivers."""

from abc import ABC, abstractmethod

from vertebra.common.protocols import AsyncConnection, AsyncCursor, AsyncPool, SyncConnection, SyncCursor, SyncPool


class SyncDriver(ABC):
    """Abstract base class for sync driver implementations."""

    dialect: str | None = None
    """SQL dialect identifier (e.g., 'postgres', 'mysql', 'sqlite')."""

    @abstractmethod
    def connect(self) -> SyncConnection:
        """Open a new connection."""

    @abstractmethod
    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncPool[SyncConnection]:
        """Create a connection pool."""


class AsyncDriver(ABC):
    """Abstract base class for async driver implementations."""

    dialect: str | None = None
    """SQL dialect identifier (e.g., 'postgres', 'mysql', 'sqlite')."""

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
