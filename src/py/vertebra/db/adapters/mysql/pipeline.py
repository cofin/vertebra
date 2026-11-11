"""Sequential transaction-scoped pipeline context managers for MySQL and MariaDB drivers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver, SyncMySQLDriver

__all__ = (
    "AsyncMySQLPipeline",
    "SyncMySQLPipeline",
)


class SyncMySQLPipeline:
    """Synchronous batch pipeline for `SyncMySQLDriver`.

    Queues `execute()` and `select()` operations and runs them inside an outer
    transaction or nested `SAVEPOINT` when `results()` is invoked, committing or
    releasing the savepoint on clean context exit and rolling back on error.
    """

    def __init__(self, driver: SyncMySQLDriver) -> None:
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._executed_results: list[Any] | None = None
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._settled: bool = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_type is not None:
            self._rollback_scope()
            return
        if self._executed_results is None and self._ops:
            try:
                self._run_ops()
            except BaseException:
                self._rollback_scope()
                raise
        self._commit_scope()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML or DDL statement to return an affected row count."""
        self._ops.append(("execute", sql, parameters))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a `SELECT` statement to return `list[dict[str, Any]]`."""
        self._ops.append(("select", sql, parameters))

    def results(self) -> list[Any]:
        """Execute all queued operations in order and return their results."""
        if self._executed_results is None:
            try:
                self._run_ops()
            except BaseException:
                self._rollback_scope()
                raise
        return list(self._executed_results or ())

    def _begin_scope(self) -> None:
        if self._owns_tx or self._savepoint_name is not None:
            return
        if self._driver.in_transaction:
            self._savepoint_name = self._driver.savepoint("_vtb_mysql_pipe")
        else:
            self._driver.begin()
            self._owns_tx = True

    def _run_ops(self) -> None:
        self._begin_scope()
        out: list[Any] = []
        for kind, sql, parameters in self._ops:
            if kind == "execute":
                out.append(self._driver.execute(sql, parameters))
            else:
                out.append(self._driver.select(sql, parameters))
        self._executed_results = out

    def _commit_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            self._driver.release_savepoint(self._savepoint_name)
            self._savepoint_name = None
        elif self._owns_tx:
            self._driver.commit()
            self._owns_tx = False

    def _rollback_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            self._driver.rollback_to_savepoint(self._savepoint_name)
            self._driver.release_savepoint(self._savepoint_name)
            self._savepoint_name = None
        elif self._owns_tx:
            self._driver.rollback()
            self._owns_tx = False


class AsyncMySQLPipeline:
    """Asynchronous batch pipeline for `AsyncMySQLDriver`.

    Queues `execute()` and `select()` operations and runs them inside an outer
    transaction or nested `SAVEPOINT` when `await results()` is invoked,
    committing or releasing the savepoint on clean context exit and rolling back on error.
    """

    def __init__(self, driver: AsyncMySQLDriver) -> None:
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._executed_results: list[Any] | None = None
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._settled: bool = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_type is not None:
            await self._rollback_scope()
            return
        if self._executed_results is None and self._ops:
            try:
                await self._run_ops()
            except BaseException:
                await self._rollback_scope()
                raise
        await self._commit_scope()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML or DDL statement to return an affected row count."""
        self._ops.append(("execute", sql, parameters))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a `SELECT` statement to return `list[dict[str, Any]]`."""
        self._ops.append(("select", sql, parameters))

    async def results(self) -> list[Any]:
        """Execute all queued operations in order and return their results."""
        if self._executed_results is None:
            try:
                await self._run_ops()
            except BaseException:
                await self._rollback_scope()
                raise
        return list(self._executed_results or ())

    async def _begin_scope(self) -> None:
        if self._owns_tx or self._savepoint_name is not None:
            return
        if self._driver.in_transaction:
            self._savepoint_name = await self._driver.savepoint("_vtb_mysql_pipe")
        else:
            await self._driver.begin()
            self._owns_tx = True

    async def _run_ops(self) -> None:
        await self._begin_scope()
        out: list[Any] = []
        for kind, sql, parameters in self._ops:
            if kind == "execute":
                out.append(await self._driver.execute(sql, parameters))
            else:
                out.append(await self._driver.select(sql, parameters))
        self._executed_results = out

    async def _commit_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            await self._driver.release_savepoint(self._savepoint_name)
            self._savepoint_name = None
        elif self._owns_tx:
            await self._driver.commit()
            self._owns_tx = False

    async def _rollback_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            await self._driver.rollback_to_savepoint(self._savepoint_name)
            await self._driver.release_savepoint(self._savepoint_name)
            self._savepoint_name = None
        elif self._owns_tx:
            await self._driver.rollback()
            self._owns_tx = False
