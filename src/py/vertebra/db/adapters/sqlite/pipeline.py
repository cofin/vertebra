"""Delayed-commit synchronous and asynchronous query pipelines for the SQLite driver."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from vertebra.db.adapters.sqlite.driver import AsyncSQLiteDriver, SyncSQLiteDriver

__all__ = (
    "AsyncSQLitePipeline",
    "SyncSQLitePipeline",
)


class SyncSQLitePipeline:
    """Synchronous delayed-commit query pipeline batching operations on `SyncSQLiteDriver`."""

    __slots__ = ("_driver", "_ops")

    def __init__(self, driver: SyncSQLiteDriver) -> None:
        """Initialize a synchronous pipeline bound to the given SQLite driver."""
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []

    def __enter__(self) -> Self:
        """Enter the synchronous pipeline context."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Discard un-flushed operations on exception or flush remaining operations on clean exit."""
        if exc_type is not None:
            self._ops.clear()
            return
        if self._ops:
            self.results()

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a DML or DDL statement for batched execution."""
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a row-returning query for batched selection."""
        self._ops.append(("select", sql, parameters))

    def results(self) -> list[Any]:
        """Flush queued pipeline operations and return their ordered results."""
        if not self._ops:
            return []
        queued = self._ops
        self._ops = []
        return self._driver.execute_pipeline(queued)


class AsyncSQLitePipeline:
    """Asynchronous delayed-commit query pipeline batching operations on `AsyncSQLiteDriver`."""

    __slots__ = ("_driver", "_ops")

    def __init__(self, driver: AsyncSQLiteDriver) -> None:
        """Initialize an asynchronous pipeline bound to the given SQLite driver."""
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pipeline context."""
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Discard un-flushed operations on exception or flush remaining operations on clean exit."""
        if exc_type is not None:
            self._ops.clear()
            return
        if self._ops:
            await self.results()

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a DML or DDL statement for batched execution."""
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a row-returning query for batched selection."""
        self._ops.append(("select", sql, parameters))

    async def results(self) -> list[Any]:
        """Flush queued pipeline operations asynchronously and return their ordered results."""
        if not self._ops:
            return []
        queued = self._ops
        self._ops = []
        return await self._driver.execute_pipeline(queued)
