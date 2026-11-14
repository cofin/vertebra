"""Synchronous and asynchronous query pipelining for Oracle 26ai."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.arrow import _hydrate_rows_with_schema
from vertebra.db.driver import InterfaceError

if TYPE_CHECKING:
    from vertebra.db.adapters.oracle._async import AsyncOracleDriver
    from vertebra.db.adapters.oracle._sync import SyncOracleDriver

__all__ = (
    "AsyncOraclePipeline",
    "SyncOraclePipeline",
)


class SyncOraclePipeline:
    """Delayed-commit synchronous query pipeline bound to a `SyncOracleDriver`."""

    __slots__ = (
        "_driver",
        "_entered",
        "_failed",
        "_flushed",
        "_outcomes",
        "_owns_transaction",
        "_pending_ops",
        "_savepoint_name",
    )

    def __init__(self, driver: SyncOracleDriver) -> None:
        self._driver = driver
        self._pending_ops: list[tuple[str, Sequence[Any] | Mapping[str, Any] | None, bool]] = []
        self._outcomes: tuple[Any, ...] = ()
        self._owns_transaction: bool = False
        self._savepoint_name: str | None = None
        self._entered: bool = False
        self._flushed: bool = False
        self._failed: bool = False

    @property
    def outcomes(self) -> tuple[Any, ...]:
        """Return the `OraclePipelineOutcome` items produced by the most recent `results()` flush."""
        return self._outcomes

    def __enter__(self) -> Self:
        """Pin a session and open a transaction or nested `SAVEPOINT _vtb_pipe_<n>`."""
        if self._driver._pipeline_active:
            raise InterfaceError("An Oracle pipeline is already active on this driver")
        self._driver._pipeline_active = True
        self._entered = True
        try:
            if self._driver.in_transaction:
                self._owns_transaction = False
                self._driver._pipe_savepoint_seq += 1
                self._savepoint_name = f"_vtb_pipe_{self._driver._pipe_savepoint_seq}"
                self._driver.savepoint(self._savepoint_name)
            else:
                self._owns_transaction = True
                self._savepoint_name = None
                self._driver.begin()
        except BaseException:
            self._driver._pipeline_active = False
            self._entered = False
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Commit on clean exit after `results()`, or roll back the transaction/savepoint on error."""
        _ = (exc_val, exc_tb)
        self._driver._pipeline_active = False
        self._entered = False
        unexecuted = bool(self._pending_ops)
        self._pending_ops.clear()
        should_rollback = exc_type is not None or self._failed or unexecuted
        if should_rollback:
            if self._savepoint_name is not None:
                self._driver.rollback_to_savepoint(self._savepoint_name)
            elif self._owns_transaction:
                self._driver.rollback()
            if exc_type is None and unexecuted:
                raise InterfaceError("Oracle pipeline exited with unexecuted operations")
            return
        if self._owns_transaction:
            self._driver.commit()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML, DDL, or PL/SQL statement for batched execution."""
        self._pending_ops.append((sql, parameters, False))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a row-returning query for batched selection."""
        self._pending_ops.append((sql, parameters, True))

    def results(self) -> list[Any]:
        """Flush queued pipeline operations on the pinned worker thread without committing."""
        if not self._entered:
            raise InterfaceError("Oracle pipeline must be entered via 'with driver.pipeline() as pipe:'")
        ops = list(self._pending_ops)
        self._pending_ops.clear()
        self._flushed = True
        if not ops:
            self._outcomes = ()
            self._driver._last_pipeline_outcomes = ()
            return []

        batch_payload: list[tuple[str, Any, bool]] = []
        for sql, params, returns_rows in ops:
            coerced = self._driver._coerce_parameters(params)
            self._driver._track_session_context_sql(sql, params)
            batch_payload.append((sql, coerced, returns_rows))

        with self._driver._checkout_client() as active_client:
            raw_outcomes = tuple(active_client.execute_pipeline_batch(batch_payload, continue_on_error=False))
        self._outcomes = raw_outcomes
        self._driver._last_pipeline_outcomes = raw_outcomes

        for outcome in raw_outcomes:
            if outcome.status == "error" and outcome.error is not None:
                self._failed = True
                raise outcome.error

        hydrated: list[Any] = []
        for (_, _, returns_rows), outcome in zip(ops, raw_outcomes, strict=True):
            if returns_rows:
                rows, cols = self._driver._coerce_result_rows_and_cols(outcome.result)
                hydrated.append(_hydrate_rows_with_schema(rows, cols, None) if rows else [])
            else:
                rc = int(outcome.result.rowcount) if outcome.result is not None else 0
                hydrated.append(max(rc, 0))
        return hydrated


class AsyncOraclePipeline:
    """Delayed-commit asynchronous query pipeline bound to an `AsyncOracleDriver`."""

    __slots__ = (
        "_driver",
        "_entered",
        "_failed",
        "_flushed",
        "_outcomes",
        "_owns_transaction",
        "_pending_ops",
        "_savepoint_name",
    )

    def __init__(self, driver: AsyncOracleDriver) -> None:
        self._driver = driver
        self._pending_ops: list[tuple[str, Sequence[Any] | Mapping[str, Any] | None, bool]] = []
        self._outcomes: tuple[Any, ...] = ()
        self._owns_transaction: bool = False
        self._savepoint_name: str | None = None
        self._entered: bool = False
        self._flushed: bool = False
        self._failed: bool = False

    @property
    def outcomes(self) -> tuple[Any, ...]:
        """Return the `OraclePipelineOutcome` items produced by the most recent `results()` flush."""
        return self._outcomes

    async def __aenter__(self) -> Self:
        """Pin a session and open an async transaction or nested `SAVEPOINT _vtb_pipe_<n>`."""
        if self._driver._pipeline_active:
            raise InterfaceError("An Oracle pipeline is already active on this driver")
        self._driver._pipeline_active = True
        self._entered = True
        try:
            if self._driver.in_transaction:
                self._owns_transaction = False
                self._driver._pipe_savepoint_seq += 1
                self._savepoint_name = f"_vtb_pipe_{self._driver._pipe_savepoint_seq}"
                await self._driver.savepoint(self._savepoint_name)
            else:
                self._owns_transaction = True
                self._savepoint_name = None
                await self._driver.begin()
        except BaseException:
            self._driver._pipeline_active = False
            self._entered = False
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Commit on clean exit after `results()`, or roll back the transaction/savepoint on error."""
        _ = (exc_val, exc_tb)
        self._driver._pipeline_active = False
        self._entered = False
        unexecuted = bool(self._pending_ops)
        self._pending_ops.clear()
        should_rollback = exc_type is not None or self._failed or unexecuted
        if should_rollback:
            if self._savepoint_name is not None:
                await self._driver.rollback_to_savepoint(self._savepoint_name)
            elif self._owns_transaction:
                await self._driver.rollback()
            if exc_type is None and unexecuted:
                raise InterfaceError("Oracle pipeline exited with unexecuted operations")
            return
        if self._owns_transaction:
            await self._driver.commit()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a DML, DDL, or PL/SQL statement for batched execution."""
        self._pending_ops.append((sql, parameters, False))

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a row-returning query for batched selection."""
        self._pending_ops.append((sql, parameters, True))

    async def results(self) -> list[Any]:
        """Flush queued pipeline operations asynchronously on the pinned worker thread without committing."""
        if not self._entered:
            raise InterfaceError("Oracle pipeline must be entered via 'async with driver.pipeline() as pipe:'")
        ops = list(self._pending_ops)
        self._pending_ops.clear()
        self._flushed = True
        if not ops:
            self._outcomes = ()
            self._driver._last_pipeline_outcomes = ()
            return []

        batch_payload: list[tuple[str, Any, bool]] = []
        for sql, params, returns_rows in ops:
            coerced = self._driver._coerce_parameters(params)
            self._driver._track_session_context_sql(sql, params)
            batch_payload.append((sql, coerced, returns_rows))

        async with self._driver._checkout_client() as active_client:
            raw_outcomes = tuple(await active_client.execute_pipeline_batch(batch_payload, continue_on_error=False))
        self._outcomes = raw_outcomes
        self._driver._last_pipeline_outcomes = raw_outcomes

        for outcome in raw_outcomes:
            if outcome.status == "error" and outcome.error is not None:
                self._failed = True
                raise outcome.error

        hydrated: list[Any] = []
        for (_, _, returns_rows), outcome in zip(ops, raw_outcomes, strict=True):
            if returns_rows:
                rows, cols = self._driver._coerce_result_rows_and_cols(outcome.result)
                hydrated.append(_hydrate_rows_with_schema(rows, cols, None) if rows else [])
            else:
                rc = int(outcome.result.rowcount) if outcome.result is not None else 0
                hydrated.append(max(rc, 0))
        return hydrated
