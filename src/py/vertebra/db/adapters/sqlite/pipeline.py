"""Delayed-commit synchronous and asynchronous query pipelines for the SQLite driver."""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from anyio import CancelScope

if TYPE_CHECKING:
    from vertebra.db.adapters.sqlite.driver import AsyncSQLiteDriver, SyncSQLiteDriver

__all__ = (
    "AsyncSQLitePipeline",
    "SyncSQLitePipeline",
)

_PIPELINE_SAVEPOINT_COUNTER = itertools.count(1)


class SyncSQLitePipeline:
    """Synchronous delayed-commit query pipeline batching operations on `SyncSQLiteDriver`."""

    __slots__ = ("_driver", "_failed", "_ops", "_owns_tx", "_savepoint_name")

    def __init__(self, driver: SyncSQLiteDriver) -> None:
        """Initialize a synchronous pipeline bound to the given SQLite driver."""
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._failed: bool = False

    def __enter__(self) -> Self:
        """Enter the synchronous pipeline context, opening a transaction or nested savepoint."""
        self._failed = False
        if self._driver.in_transaction:
            sp_name = f"_vtb_sp_{next(_PIPELINE_SAVEPOINT_COUNTER)}"
            self._driver.execute(f'SAVEPOINT "{sp_name}"')
            self._savepoint_name = sp_name
            self._owns_tx = False
        else:
            self._driver.begin()
            self._owns_tx = True
            self._savepoint_name = None
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush remaining operations and commit on clean exit, or roll back pipeline scope on error."""
        _ = (exc, tb)
        if exc_type is not None or self._failed:
            self._ops.clear()
            self._rollback_scope()
            return
        if self._ops:
            try:
                self.results()
            except BaseException:
                self._ops.clear()
                self._rollback_scope()
                raise
        self._commit_scope()

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
        try:
            return self._driver.execute_pipeline(queued)
        except BaseException:
            self._failed = True
            raise

    def _commit_scope(self) -> None:
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            self._driver.execute(f'RELEASE SAVEPOINT "{sp_name}"')
        elif self._owns_tx:
            self._owns_tx = False
            self._driver.commit()

    def _rollback_scope(self) -> None:
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            self._driver.execute(f'ROLLBACK TO SAVEPOINT "{sp_name}"')
            self._driver.execute(f'RELEASE SAVEPOINT "{sp_name}"')
        elif self._owns_tx:
            self._owns_tx = False
            self._driver.rollback()


class AsyncSQLitePipeline:
    """Asynchronous delayed-commit query pipeline batching operations on `AsyncSQLiteDriver`."""

    __slots__ = ("_driver", "_failed", "_ops", "_owns_tx", "_savepoint_name")

    def __init__(self, driver: AsyncSQLiteDriver) -> None:
        """Initialize an asynchronous pipeline bound to the given SQLite driver."""
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._failed: bool = False

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pipeline context, opening a transaction or nested savepoint."""
        self._failed = False
        if self._driver.in_transaction:
            sp_name = f"_vtb_sp_{next(_PIPELINE_SAVEPOINT_COUNTER)}"
            await self._driver.execute(f'SAVEPOINT "{sp_name}"')
            self._savepoint_name = sp_name
            self._owns_tx = False
        else:
            await self._driver.begin()
            self._owns_tx = True
            self._savepoint_name = None
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush remaining operations and commit on clean exit, or roll back pipeline scope on error."""
        _ = (exc, tb)
        if exc_type is not None or self._failed:
            self._ops.clear()
            with CancelScope(shield=True):
                await self._rollback_scope()
            return
        if self._ops:
            try:
                await self.results()
            except BaseException:
                self._ops.clear()
                with CancelScope(shield=True):
                    await self._rollback_scope()
                raise
        await self._commit_scope()

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
        try:
            return await self._driver.execute_pipeline(queued)
        except BaseException:
            self._failed = True
            raise

    async def _commit_scope(self) -> None:
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            await self._driver.execute(f'RELEASE SAVEPOINT "{sp_name}"')
        elif self._owns_tx:
            self._owns_tx = False
            await self._driver.commit()

    async def _rollback_scope(self) -> None:
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            await self._driver.execute(f'ROLLBACK TO SAVEPOINT "{sp_name}"')
            await self._driver.execute(f'RELEASE SAVEPOINT "{sp_name}"')
        elif self._owns_tx:
            self._owns_tx = False
            await self._driver.rollback()
