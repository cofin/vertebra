"""Sequential transaction-scoped pipeline context managers for MySQL and MariaDB drivers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from anyio import CancelScope

if TYPE_CHECKING:
    from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver, SyncMySQLDriver

__all__ = (
    "AsyncMySQLPipeline",
    "SyncMySQLPipeline",
)


class SyncMySQLPipeline:
    """Synchronous batch pipeline for `SyncMySQLDriver`.

    Queues `execute()` and `select()` operations and runs them inside an outer
    transaction or nested `SAVEPOINT` when `results()` is invoked or on clean
    context exit, committing or releasing the savepoint on clean exit and
    discarding unexecuted ops and rolling back on error.
    """

    __slots__ = (
        "_driver",
        "_failed",
        "_ops",
        "_owns_tx",
        "_savepoint_name",
        "_settled",
    )

    def __init__(self, driver: SyncMySQLDriver) -> None:
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._failed: bool = False
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._settled: bool = False

    def __enter__(self) -> Self:
        self._failed = False
        self._settled = False
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        _ = (exc_val, exc_tb)
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
        if not self._ops:
            return []
        queued = list(self._ops)
        self._ops.clear()
        try:
            return self._run_ops(queued)
        except BaseException:
            self._failed = True
            self._rollback_scope()
            raise

    def _begin_scope(self) -> None:
        if self._owns_tx or self._savepoint_name is not None:
            return
        if self._driver.in_transaction:
            self._savepoint_name = self._driver.savepoint("_vtb_mysql_pipe")
        else:
            self._driver.begin()
            self._owns_tx = True

    def _run_ops(
        self,
        queued: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]],
    ) -> list[Any]:
        self._begin_scope()
        out: list[Any] = []
        for kind, sql, parameters in queued:
            if kind == "execute":
                out.append(self._driver.execute(sql, parameters))
            else:
                out.append(self._driver.select(sql, parameters))
        return out

    def _commit_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            self._driver.release_savepoint(sp_name)
        elif self._owns_tx:
            self._owns_tx = False
            self._driver.commit()

    def _rollback_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            self._driver.rollback_to_savepoint(sp_name)
            self._driver.release_savepoint(sp_name)
        elif self._owns_tx:
            self._owns_tx = False
            self._driver.rollback()


class AsyncMySQLPipeline:
    """Asynchronous batch pipeline for `AsyncMySQLDriver`.

    Queues `execute()` and `select()` operations and runs them inside an outer
    transaction or nested `SAVEPOINT` when `await results()` is invoked or on
    clean context exit, committing or releasing the savepoint on clean exit and
    discarding unexecuted ops and rolling back on error.
    """

    __slots__ = (
        "_driver",
        "_failed",
        "_ops",
        "_owns_tx",
        "_savepoint_name",
        "_settled",
    )

    def __init__(self, driver: AsyncMySQLDriver) -> None:
        self._driver = driver
        self._ops: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]] = []
        self._failed: bool = False
        self._owns_tx: bool = False
        self._savepoint_name: str | None = None
        self._settled: bool = False

    async def __aenter__(self) -> Self:
        self._failed = False
        self._settled = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        _ = (exc_val, exc_tb)
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
        if not self._ops:
            return []
        queued = list(self._ops)
        self._ops.clear()
        try:
            return await self._run_ops(queued)
        except BaseException:
            self._failed = True
            with CancelScope(shield=True):
                await self._rollback_scope()
            raise

    async def _begin_scope(self) -> None:
        if self._owns_tx or self._savepoint_name is not None:
            return
        if self._driver.in_transaction:
            self._savepoint_name = await self._driver.savepoint("_vtb_mysql_pipe")
        else:
            await self._driver.begin()
            self._owns_tx = True

    async def _run_ops(
        self,
        queued: list[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]],
    ) -> list[Any]:
        await self._begin_scope()
        out: list[Any] = []
        for kind, sql, parameters in queued:
            if kind == "execute":
                out.append(await self._driver.execute(sql, parameters))
            else:
                out.append(await self._driver.select(sql, parameters))
        return out

    async def _commit_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            await self._driver.release_savepoint(sp_name)
        elif self._owns_tx:
            self._owns_tx = False
            await self._driver.commit()

    async def _rollback_scope(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._savepoint_name is not None:
            sp_name = self._savepoint_name
            self._savepoint_name = None
            await self._driver.rollback_to_savepoint(sp_name)
            await self._driver.release_savepoint(sp_name)
        elif self._owns_tx:
            self._owns_tx = False
            await self._driver.rollback()
