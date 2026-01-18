"""Base classes and protocols for Bun-backed drivers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence


class SyncCursor(Protocol):
    """DB-API cursor protocol."""

    def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        """Execute a query and return result metadata."""

    def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        """Execute a query for many parameter sets."""

    def fetchone(self) -> Any:
        """Fetch a single row."""

    def fetchmany(self, size: int | None = None) -> list[Any]:
        """Fetch multiple rows."""

    def fetchall(self) -> list[Any]:
        """Fetch all remaining rows."""

    def close(self) -> None:
        """Close the cursor."""


class AsyncCursor(Protocol):
    """Async cursor protocol."""

    async def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        """Execute a query asynchronously and return result metadata."""

    async def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        """Execute a query for many parameter sets asynchronously."""

    async def fetchone(self) -> Any:
        """Fetch a single row asynchronously."""

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        """Fetch multiple rows asynchronously."""

    async def fetchall(self) -> list[Any]:
        """Fetch all remaining rows asynchronously."""

    async def close(self) -> None:
        """Close the cursor."""


class SyncConnection(Protocol):
    """DB-API connection protocol."""

    def cursor(self) -> SyncCursor:
        """Create a new cursor."""

    def commit(self) -> None:
        """Commit the current transaction."""

    def rollback(self) -> None:
        """Rollback the current transaction."""

    def close(self) -> None:
        """Close the connection."""


class AsyncConnection(Protocol):
    """Async driver connection protocol."""

    def cursor(self) -> AsyncCursor:
        """Create a new async cursor."""

    async def commit(self) -> None:
        """Commit the current transaction."""

    async def rollback(self) -> None:
        """Rollback the current transaction."""

    async def fetch(self, query: str, params: Sequence[Any] | None = None) -> list[Any]:
        """Fetch many rows asynchronously."""

    async def fetchrow(self, query: str, params: Sequence[Any] | None = None) -> Any:
        """Fetch a single row asynchronously."""

    async def fetchval(self, query: str, params: Sequence[Any] | None = None) -> Any:
        """Fetch a single value asynchronously."""

    async def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        """Execute a query asynchronously."""

    async def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        """Execute a query for many parameter sets asynchronously."""

    async def close(self) -> None:
        """Close the connection."""


class SyncPool(Protocol):
    """Connection pool protocol."""

    def acquire(self) -> SyncConnection:
        """Acquire a connection from the pool."""

    def release(self, connection: SyncConnection) -> None:
        """Release a connection back to the pool."""

    def close(self) -> None:
        """Close the pool."""

    def get_size(self) -> int:
        """Total pool size."""

    def get_idle_size(self) -> int:
        """Idle connection count."""


class AsyncPool(Protocol):
    """Async connection pool protocol."""

    async def acquire(self) -> AsyncConnection:
        """Acquire a connection from the pool asynchronously."""

    async def release(self, connection: AsyncConnection) -> None:
        """Release a connection back to the pool asynchronously."""

    async def close(self) -> None:
        """Close the pool asynchronously."""

    def get_size(self) -> int:
        """Total pool size."""

    def get_idle_size(self) -> int:
        """Idle connection count."""


class SyncDriver(ABC):
    """Abstract base class for sync driver implementations."""

    @abstractmethod
    def connect(self) -> SyncConnection:
        """Open a new connection."""

    @abstractmethod
    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncPool:
        """Create a connection pool."""

    def iter_rows(self, cursor: SyncCursor) -> Iterator[Any]:
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
    async def pool(self, min_size: int = 1, max_size: int = 10) -> AsyncPool:
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
