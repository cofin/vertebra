"""Base classes and protocols for Bun-backed drivers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, Protocol, Sequence


class Cursor(Protocol):
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


class Connection(Protocol):
    """DB-API connection protocol."""

    def cursor(self) -> Cursor:
        """Create a new cursor."""

    def commit(self) -> None:
        """Commit the current transaction."""

    def rollback(self) -> None:
        """Rollback the current transaction."""

    def close(self) -> None:
        """Close the connection."""


class AsyncConnection(Protocol):
    """Async driver connection protocol."""

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


class Pool(Protocol):
    """Connection pool protocol."""

    def acquire(self) -> Connection:
        """Acquire a connection from the pool."""

    def release(self, connection: Connection) -> None:
        """Release a connection back to the pool."""

    def close(self) -> None:
        """Close the pool."""

    def get_size(self) -> int:
        """Total pool size."""

    def get_idle_size(self) -> int:
        """Idle connection count."""


class BaseDriver(ABC):
    """Abstract base class for driver implementations."""

    @abstractmethod
    def connect(self) -> Connection:
        """Open a new connection."""

    @abstractmethod
    def connect_async(self) -> AsyncConnection:
        """Open a new async connection."""

    @abstractmethod
    def pool(self, min_size: int = 1, max_size: int = 10) -> Pool:
        """Create a connection pool."""

    def iter_rows(self, cursor: Cursor) -> Iterator[Any]:
        """Iterate rows from a cursor until exhausted."""

        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row


__all__ = ["AsyncConnection", "BaseDriver", "Connection", "Cursor", "Pool"]
