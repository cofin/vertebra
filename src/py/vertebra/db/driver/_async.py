"""Asynchronous base driver contracts for Vertebra database drivers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any, Self, TypeVar

from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver._common import BaseDatabaseConfig, CommonDriverMixin
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.utils.schema import to_value_type

__all__ = (
    "AsyncDatabaseDriver",
    "ThreadOffloadAsyncDatabaseDriver",
)

SyncDriverT = TypeVar("SyncDriverT", bound=SyncDatabaseDriver[Any])


class AsyncDatabaseDriver[ConfigT: BaseDatabaseConfig](CommonDriverMixin[ConfigT]):
    """Base execution contract for asynchronous SQL and analytical database drivers."""

    __slots__ = ()

    async def __aenter__(self) -> Self:
        """Connect and enter the asynchronous driver context."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the driver upon exiting the asynchronous context."""
        await self.close()

    async def connect(self) -> Any:
        """Establish or validate the underlying connection."""
        self._closed = False
        return self

    async def close(self) -> None:
        """Close the driver and release pooled resources."""
        self._closed = True

    async def commit(self) -> None:
        """Commit the active transaction when supported by the engine."""

    async def rollback(self) -> None:
        """Roll back the active transaction when supported by the engine."""

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return len(rows)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across a sequence of parameter sets and return total affected rows."""
        total = 0
        for params in parameter_sets:
            total += await self.execute(sql, params, **kwargs)
        return total

    async def executemany(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        **kwargs: Any,
    ) -> int:
        """Alias for `execute_many` matching DB-API 2.0 conventions."""
        return await self.execute_many(sql, parameter_sets, **kwargs)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        raise NotImplementedError

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return rows

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows into dictionaries or `schema_type` instances."""
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        return self.hydrate_rows(rows, cols, schema_type)

    async def fetch(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Alias for `select`."""
        return await self.select(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `KeyError` if no rows match."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise KeyError(msg)
        return rows[0]

    async def fetchrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row or `None` when empty."""
        return await self.select_one_or_none(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when empty."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first column of the first row, or `None` when empty."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def fetchval(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Alias for `select_value`."""
        return await self.select_value(sql, parameters, value_type=value_type, **kwargs)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and return a zero-copy `ArrowCursor`."""
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    async def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Alias for `select_to_arrow`."""
        return await self.select_to_arrow(sql_or_table, parameters, threshold_rows=threshold_rows, **kwargs)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an `ArrowCursor`, PyArrow Table, Polars DataFrame, or row sequence into `table_name`."""
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        col_list = ", ".join(cols)
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows, **kwargs)

    async def insert_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Alias for `load_from_arrow`."""
        return await self.load_from_arrow(table_name, data, columns=columns, **kwargs)


class ThreadOffloadAsyncDatabaseDriver[
    ConfigT: BaseDatabaseConfig,
    SyncDriverT: SyncDatabaseDriver[Any],
](AsyncDatabaseDriver[ConfigT]):
    """Asynchronous driver backed by `asyncio.to_thread` delegation to a paired synchronous driver."""

    __slots__ = ("_sync",)

    def __init__(self, config: ConfigT, sync_driver: SyncDriverT, *, broker: Any = None) -> None:
        super().__init__(config, broker=broker)
        self._sync: SyncDriverT = sync_driver

    @property
    def sync_driver(self) -> SyncDriverT:
        """Return the underlying synchronous driver instance."""
        return self._sync

    async def connect(self) -> Any:
        """Connect the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.connect)
        self._closed = False
        return self

    async def close(self) -> None:
        """Close the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.close)
        self._closed = True

    async def commit(self) -> None:
        """Commit the active transaction on the underlying synchronous driver."""
        await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        """Roll back the active transaction on the underlying synchronous driver."""
        await asyncio.to_thread(self._sync.rollback)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a statement via the paired synchronous driver."""
        return await asyncio.to_thread(self._sync.execute, sql, parameters, **kwargs)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a batch statement via the paired synchronous driver."""
        return await asyncio.to_thread(self._sync.execute_many, sql, parameter_sets, **kwargs)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query with column descriptions via the paired synchronous driver."""
        return await asyncio.to_thread(self._sync.execute_with_description, sql, parameters, **kwargs)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query returning tuple rows via the paired synchronous driver."""
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters, **kwargs)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query via the paired synchronous driver."""
        return await asyncio.to_thread(
            self._sync.select,
            sql,
            parameters,
            schema_type=schema_type,
            **kwargs,
        )

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query returning an `ArrowCursor` via the paired synchronous driver."""
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            parameters,
            threshold_rows=threshold_rows,
            **kwargs,
        )

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an Arrow payload into `table_name` via the paired synchronous driver."""
        return await asyncio.to_thread(
            self._sync.load_from_arrow,
            table_name,
            data,
            columns=columns,
            **kwargs,
        )
