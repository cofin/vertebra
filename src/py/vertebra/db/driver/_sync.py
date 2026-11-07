"""Synchronous base driver contract for Vertebra database drivers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Self

from vertebra._vertebra import NotFoundError
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver._common import BaseDatabaseConfig, CommonDriverMixin
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

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

    def connect(self) -> Self:
        """Establish or validate the underlying connection."""
        self._closed = False
        return self

    def close(self) -> None:
        """Close the driver and release pooled resources."""
        self._closed = True

    def begin(self) -> None:
        """Begin an explicit transaction when supported by the engine."""

    def commit(self) -> None:
        """Commit the active transaction when supported by the engine."""

    def rollback(self) -> None:
        """Roll back the active transaction when supported by the engine."""

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """Execute a block inside a `begin()` / `commit()` / `rollback()` transaction scope."""
        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def cancel(self) -> None:
        """Interrupt an in-flight query when supported, or act as an idempotent no-op."""

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
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across a sequence of parameter sets and return total affected rows."""
        total = 0
        for params in parameter_sets:
            total += self.execute(sql, params, **kwargs)
        return total

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        raise NotImplementedError

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

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

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
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        if not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

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

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Execute a query and yield Arrow record batches of at most `batch_size` rows."""
        chunk = max(1, int(batch_size))
        cursor = self.select_to_arrow(sql, parameters, **kwargs)
        table = cursor.to_arrow_table()
        if hasattr(table, "to_batches"):
            batches = table.to_batches(max_chunksize=chunk)
            if batches:
                yield from batches
                return
            if cursor.row_count == 0:
                return
        rows = cursor.to_pylist()
        for idx in range(0, len(rows), chunk):
            yield rows[idx : idx + chunk]

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an `ArrowCursor`, PyArrow Table, Polars DataFrame, or row sequence into `table_name`."""
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dialect = getattr(self, "dialect", "ansi")
        quoted_table = quote_dialect_identifier(table_name, dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), dialect) for col in cols)
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        return self.execute_many(sql, rows, **kwargs)
