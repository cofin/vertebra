"""Delayed-commit synchronous and asynchronous query pipelines for the DuckDB driver."""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from anyio import CancelScope

if TYPE_CHECKING:
    from vertebra.db.adapters.duckdb.driver import AsyncDuckDBDriver, SyncDuckDBDriver

__all__ = (
    "AsyncDuckDBPipeline",
    "SyncDuckDBPipeline",
)

_PIPELINE_SAVEPOINT_COUNTER = itertools.count(1)


class SyncDuckDBPipeline:
    """Synchronous delayed-commit query pipeline batching operations on `SyncDuckDBDriver`."""

    __slots__ = ("_driver", "_failed", "_opened_transaction", "_ops", "_savepoint", "_transaction")

    def __init__(self, driver: SyncDuckDBDriver, *, transaction: bool = True) -> None:
        self._driver = driver
        self._transaction = bool(transaction)
        self._opened_transaction = False
        self._savepoint: str | None = None
        self._failed = False
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None, type[Any] | None]] = []

    def __enter__(self) -> Self:
        """Enter the synchronous pipeline context, opening a transaction or nested savepoint."""
        self._failed = False
        if self._transaction:
            if self._driver.in_transaction:
                sp_name = f"_vtb_pipe_{next(_PIPELINE_SAVEPOINT_COUNTER)}"
                self._driver.savepoint(sp_name)
                self._savepoint = sp_name
            else:
                self._driver.begin()
                self._opened_transaction = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush remaining operations and commit on clean exit, or roll back on exception."""
        if exc_type is not None or self._failed:
            self._ops.clear()
            if self._savepoint is not None:
                sp_name = self._savepoint
                self._savepoint = None
                self._driver.rollback_to_savepoint(sp_name)
            elif self._opened_transaction:
                self._opened_transaction = False
                self._driver.rollback()
            return

        try:
            if self._ops:
                self.results()
        except BaseException:
            self._ops.clear()
            if self._savepoint is not None:
                sp_name = self._savepoint
                self._savepoint = None
                self._driver.rollback_to_savepoint(sp_name)
            elif self._opened_transaction:
                self._opened_transaction = False
                self._driver.rollback()
            raise

        if self._savepoint is not None:
            sp_name = self._savepoint
            self._savepoint = None
            self._driver.release_savepoint(sp_name)
        elif self._opened_transaction:
            self._opened_transaction = False
            self._driver.commit()

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a DML or DDL statement for ordered execution."""
        self._ops.append(("execute", sql, parameters, None))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> None:
        """Queue a `SELECT` query for ordered execution."""
        self._ops.append(("select", sql, parameters, schema_type))

    def results(self) -> list[Any]:
        """Execute all queued operations in FIFO order and return their results."""
        queued = list(self._ops)
        self._ops.clear()
        out: list[Any] = []
        try:
            for kind, sql, params, schema_type in queued:
                if kind == "select":
                    out.append(self._driver.select(sql, params, schema_type=schema_type))
                else:
                    out.append(self._driver.execute(sql, params))
        except BaseException:
            self._failed = True
            raise
        return out


class AsyncDuckDBPipeline:
    """Asynchronous delayed-commit query pipeline batching operations on `AsyncDuckDBDriver`."""

    __slots__ = ("_driver", "_failed", "_opened_transaction", "_ops", "_savepoint", "_transaction")

    def __init__(self, driver: AsyncDuckDBDriver, *, transaction: bool = True) -> None:
        self._driver = driver
        self._transaction = bool(transaction)
        self._opened_transaction = False
        self._savepoint: str | None = None
        self._failed = False
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None, type[Any] | None]] = []

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pipeline context, opening a transaction or nested savepoint."""
        self._failed = False
        if self._transaction:
            if self._driver.in_transaction:
                sp_name = f"_vtb_pipe_{next(_PIPELINE_SAVEPOINT_COUNTER)}"
                await self._driver.savepoint(sp_name)
                self._savepoint = sp_name
            else:
                await self._driver.begin()
                self._opened_transaction = True
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush remaining operations and commit on clean exit, or roll back under a shielded scope on error."""
        if exc_type is not None or self._failed:
            self._ops.clear()
            with CancelScope(shield=True):
                if self._savepoint is not None:
                    sp_name = self._savepoint
                    self._savepoint = None
                    await self._driver.rollback_to_savepoint(sp_name)
                elif self._opened_transaction:
                    self._opened_transaction = False
                    await self._driver.rollback()
            return

        try:
            if self._ops:
                await self.results()
        except BaseException:
            self._ops.clear()
            with CancelScope(shield=True):
                if self._savepoint is not None:
                    sp_name = self._savepoint
                    self._savepoint = None
                    await self._driver.rollback_to_savepoint(sp_name)
                elif self._opened_transaction:
                    self._opened_transaction = False
                    await self._driver.rollback()
            raise

        if self._savepoint is not None:
            sp_name = self._savepoint
            self._savepoint = None
            await self._driver.release_savepoint(sp_name)
        elif self._opened_transaction:
            self._opened_transaction = False
            await self._driver.commit()

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Queue a DML or DDL statement for ordered execution."""
        self._ops.append(("execute", sql, parameters, None))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> None:
        """Queue a `SELECT` query for ordered execution."""
        self._ops.append(("select", sql, parameters, schema_type))

    async def results(self) -> list[Any]:
        """Execute all queued operations asynchronously in FIFO order and return their results."""
        queued = list(self._ops)
        self._ops.clear()
        out: list[Any] = []
        try:
            for kind, sql, params, schema_type in queued:
                if kind == "select":
                    out.append(await self._driver.select(sql, params, schema_type=schema_type))
                else:
                    out.append(await self._driver.execute(sql, params))
        except BaseException:
            self._failed = True
            raise
        return out
