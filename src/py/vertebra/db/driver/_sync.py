"""Synchronous base driver contract for Vertebra database drivers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Self

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver._common import BaseDatabaseConfig, CommonDriverMixin
from vertebra.utils.schema import to_value_type

__all__ = ("SyncDatabaseDriver",)


class SyncDatabaseDriver[ConfigT: BaseDatabaseConfig](CommonDriverMixin[ConfigT]):
    """Base execution contract for synchronous SQL and analytical database drivers."""

    __slots__ = ()

    def __enter__(self) -> Self:
        """Connect and enter the synchronous driver context."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the driver upon exiting the synchronous context."""
        self.close()

    def connect(self) -> Any:
        """Establish or validate the underlying connection."""
        self._closed = False
        return self

    def close(self) -> None:
        """Close the driver and release pooled resources."""
        self._closed = True

    def commit(self) -> None:
        """Commit the active transaction when supported by the engine."""

    def rollback(self) -> None:
        """Roll back the active transaction when supported by the engine."""

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return len(rows)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across a sequence of parameter sets and return total affected rows."""
        total = 0
        for params in parameter_sets:
            total += self.execute(sql, params, **kwargs)
        return total

    def executemany(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        **kwargs: Any,
    ) -> int:
        """Alias for `execute_many` matching DB-API 2.0 conventions."""
        return self.execute_many(sql, parameter_sets, **kwargs)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        raise NotImplementedError

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows into dictionaries or `schema_type` instances."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        return self.hydrate_rows(rows, cols, schema_type)

    def fetch(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Alias for `select`."""
        return self.select(sql, parameters, schema_type=schema_type, **kwargs)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `KeyError` if no rows match."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise KeyError(msg)
        return rows[0]

    def fetchrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row or `None` when empty."""
        return self.select_one_or_none(sql, parameters, schema_type=schema_type, **kwargs)

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first column of the first row, or `None` when empty."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def fetchval(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Alias for `select_value`."""
        return self.select_value(sql, parameters, value_type=value_type, **kwargs)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and return a zero-copy `ArrowCursor`."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Alias for `select_to_arrow`."""
        return self.select_to_arrow(sql_or_table, parameters, threshold_rows=threshold_rows, **kwargs)

    def load_from_arrow(
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
        return self.execute_many(sql, rows, **kwargs)

    def insert_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Alias for `load_from_arrow`."""
        return self.load_from_arrow(table_name, data, columns=columns, **kwargs)
