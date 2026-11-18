"""Transactional query pipelining (`SyncMSSQLPipeline` and `AsyncMSSQLPipeline`) for SQL Server."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from anyio import CancelScope

from vertebra.typing import FrozenBaseStruct

if TYPE_CHECKING:
    from vertebra.db.adapters.mssql.driver import AsyncMSSQLDriver, SyncMSSQLDriver

__all__ = (
    "AsyncMSSQLPipeline",
    "SyncMSSQLPipeline",
)


class _QueuedMSSQLOperation(FrozenBaseStruct, frozen=True, gc=True):
    """Single queued statement inside a SQL Server query pipeline."""

    kind: str
    """Operation type (`execute` or `select`)."""

    sql: str
    """SQL statement text."""

    parameters: Sequence[Any] | Mapping[str, Any] | None = None
    """Optional bound parameters."""


class SyncMSSQLPipeline:
    """Synchronous SQL Server query pipeline with delayed commit and `SAVE TRANSACTION` nesting."""

    __slots__ = (
        "_driver",
        "_failed",
        "_owns_outer_tx",
        "_queue",
        "_savepoint_name",
        "_savepoint_seq",
        "_tx_active",
    )

    def __init__(self, driver: SyncMSSQLDriver) -> None:
        self._driver = driver
        self._queue: list[_QueuedMSSQLOperation] = []
        self._tx_active: bool = False
        self._owns_outer_tx: bool = False
        self._savepoint_name: str | None = None
        self._savepoint_seq: int = 0
        self._failed: bool = False

    def __enter__(self) -> Self:
        """Enter the synchronous pipeline context."""
        self._failed = False
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush queued operations and commit on clean exit, or discard and roll back on error."""
        _ = (exc, tb)
        if exc_type is not None or self._failed:
            self._queue.clear()
            if self._tx_active:
                self._rollback_active()
            return
        if self._queue:
            try:
                self.results()
            except BaseException:
                self._queue.clear()
                if self._tx_active:
                    self._rollback_active()
                raise
        if self._tx_active:
            self._commit_active()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML or DDL statement for batched pipeline execution."""
        self._queue.append(_QueuedMSSQLOperation(kind="execute", sql=sql, parameters=parameters))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a row-returning `SELECT` statement for batched pipeline execution."""
        self._queue.append(_QueuedMSSQLOperation(kind="select", sql=sql, parameters=parameters))

    def results(self) -> list[Any]:
        """Execute all queued pipeline operations inside a delayed-commit transaction or savepoint."""
        if not self._queue:
            return []
        self._ensure_tx_started()
        ops = list(self._queue)
        self._queue.clear()
        out: list[Any] = []
        try:
            for op in ops:
                if op.kind == "execute":
                    out.append(self._driver.execute(op.sql, op.parameters))
                else:
                    out.append(self._driver.select(op.sql, op.parameters))
        except BaseException:
            self._failed = True
            self._rollback_active()
            raise
        return out

    def _ensure_tx_started(self) -> None:
        if self._tx_active:
            return
        if self._driver.in_transaction:
            self._savepoint_seq += 1
            sp_name = f"_vtb_pipe_{self._savepoint_seq}"
            self._driver.savepoint(sp_name)
            self._savepoint_name = sp_name
            self._owns_outer_tx = False
        else:
            self._driver.begin()
            self._savepoint_name = None
            self._owns_outer_tx = True
        self._tx_active = True

    def _commit_active(self) -> None:
        try:
            if self._owns_outer_tx:
                self._driver.commit()
            elif self._savepoint_name is not None:
                self._driver.release_savepoint(self._savepoint_name)
        finally:
            self._tx_active = False
            self._owns_outer_tx = False
            self._savepoint_name = None

    def _rollback_active(self) -> None:
        try:
            if self._owns_outer_tx:
                self._driver.rollback()
            elif self._savepoint_name is not None:
                self._driver.rollback_to_savepoint(self._savepoint_name)
        finally:
            self._tx_active = False
            self._owns_outer_tx = False
            self._savepoint_name = None


class AsyncMSSQLPipeline:
    """Asynchronous SQL Server query pipeline with delayed commit and `SAVE TRANSACTION` nesting."""

    __slots__ = (
        "_driver",
        "_failed",
        "_owns_outer_tx",
        "_queue",
        "_savepoint_name",
        "_savepoint_seq",
        "_tx_active",
    )

    def __init__(self, driver: AsyncMSSQLDriver) -> None:
        self._driver = driver
        self._queue: list[_QueuedMSSQLOperation] = []
        self._tx_active: bool = False
        self._owns_outer_tx: bool = False
        self._savepoint_name: str | None = None
        self._savepoint_seq: int = 0
        self._failed: bool = False

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pipeline context."""
        self._failed = False
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Flush queued operations and commit on clean exit, or discard and roll back on error."""
        _ = (exc, tb)
        if exc_type is not None or self._failed:
            self._queue.clear()
            if self._tx_active:
                with CancelScope(shield=True):
                    await self._rollback_active()
            return
        if self._queue:
            try:
                await self.results()
            except BaseException:
                self._queue.clear()
                if self._tx_active:
                    with CancelScope(shield=True):
                        await self._rollback_active()
                raise
        if self._tx_active:
            await self._commit_active()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML or DDL statement for batched pipeline execution."""
        self._queue.append(_QueuedMSSQLOperation(kind="execute", sql=sql, parameters=parameters))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a row-returning `SELECT` statement for batched pipeline execution."""
        self._queue.append(_QueuedMSSQLOperation(kind="select", sql=sql, parameters=parameters))

    async def results(self) -> list[Any]:
        """Execute all queued pipeline operations asynchronously inside a transaction or savepoint."""
        if not self._queue:
            return []
        await self._ensure_tx_started()
        ops = list(self._queue)
        self._queue.clear()
        out: list[Any] = []
        try:
            for op in ops:
                if op.kind == "execute":
                    out.append(await self._driver.execute(op.sql, op.parameters))
                else:
                    out.append(await self._driver.select(op.sql, op.parameters))
        except BaseException:
            self._failed = True
            with CancelScope(shield=True):
                await self._rollback_active()
            raise
        return out

    async def _ensure_tx_started(self) -> None:
        if self._tx_active:
            return
        if self._driver.in_transaction:
            self._savepoint_seq += 1
            sp_name = f"_vtb_pipe_{self._savepoint_seq}"
            await self._driver.savepoint(sp_name)
            self._savepoint_name = sp_name
            self._owns_outer_tx = False
        else:
            await self._driver.begin()
            self._savepoint_name = None
            self._owns_outer_tx = True
        self._tx_active = True

    async def _commit_active(self) -> None:
        try:
            if self._owns_outer_tx:
                await self._driver.commit()
            elif self._savepoint_name is not None:
                await self._driver.release_savepoint(self._savepoint_name)
        finally:
            self._tx_active = False
            self._owns_outer_tx = False
            self._savepoint_name = None

    async def _rollback_active(self) -> None:
        try:
            if self._owns_outer_tx:
                await self._driver.rollback()
            elif self._savepoint_name is not None:
                await self._driver.rollback_to_savepoint(self._savepoint_name)
        finally:
            self._tx_active = False
            self._owns_outer_tx = False
            self._savepoint_name = None
