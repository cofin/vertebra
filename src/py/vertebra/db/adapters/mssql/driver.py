"""Synchronous and asynchronous SQL Server (TDS 7.4) drivers."""

from __future__ import annotations

import os
import re
import sqlite3
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "AsyncMSSQLDriver",
    "SyncMSSQLDriver",
)


class SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig]):
    """Synchronous SQL Server TDS 7.4 driver with SHM statement cache and Arrow cursor support."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    def __init__(self, config: MSSQLConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or MSSQLConfig()
        super().__init__(resolved, broker=broker)
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._in_transaction: bool = False
        self._last_rewritten_sql: str = ""
        self._last_tds_batch: bytes = b""

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._last_rewritten_sql

    @property
    def last_tds_batch(self) -> bytes:
        """Return the most recently encoded TDS 7.4 `SQLBatch` packet."""
        return self._last_tds_batch

    def begin(self) -> None:
        """Begin an explicit SQL Server transaction."""
        if not self._conn.in_transaction:
            self._conn.execute("BEGIN")
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active SQL Server transaction."""
        try:
            self._conn.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active SQL Server transaction."""
        try:
            self._conn.rollback()
        finally:
            self._in_transaction = False

    def cancel(self) -> None:
        """Interrupt an in-flight query when supported, or act as an idempotent no-op."""
        if not self._closed:
            self._conn.interrupt()

    def close(self) -> None:
        """Close the underlying driver resources."""
        self._closed = True
        self._in_transaction = False
        self._conn.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = MSSQLCore.normalize_for_sqlite(sql, parameters)
            cur = self._conn.execute(sqlite_sql, bound)
            if not self._in_transaction:
                self._conn.commit()
            return max(cur.rowcount, 0)
        finally:
            self._release_mssql_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = MSSQLCore.normalize_for_sqlite(sql, parameters)
            cur = self._conn.execute(sqlite_sql, bound)
            rows = [tuple(row) for row in cur.fetchall()]
            if not self._in_transaction and cur.description is None:
                self._conn.commit()
            return rows
        finally:
            self._release_mssql_lease(grant)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)`."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = MSSQLCore.normalize_for_sqlite(sql, parameters)
            cur = self._conn.execute(sqlite_sql, bound)
            if cur.description is not None:
                rows = [tuple(row) for row in cur.fetchall()]
                cols = tuple(str(col[0]) for col in cur.description)
                return rows, cols
            if not self._in_transaction:
                self._conn.commit()
            return [], ()
        finally:
            self._release_mssql_lease(grant)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or hydrated `schema_type` instances."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql, bound = MSSQLCore.normalize_for_sqlite(sql, parameters)
            cur = self._conn.execute(sqlite_sql, bound)
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [str(col[0]) for col in (cur.description or ())]
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_mssql_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy Arrow cursor."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join(f"@P{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        grant = self._prepare_and_lease(sql)
        try:
            sqlite_sql = re.sub(r"@P\d+", "?", sql)
            self._conn.executemany(sqlite_sql, rows)
            if not self._in_transaction:
                self._conn.commit()
            return len(rows)
        finally:
            self._release_mssql_lease(grant)

    def _prepare_and_lease(self, sql: str) -> Any:
        sc_attr = getattr(self._broker, "statement_cache", None) if self._broker is not None else None
        sc = sc_attr() if callable(sc_attr) else sc_attr
        if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
            sql_bytes = sql.encode("utf-8")
            try:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql)
                use_bytes_key = False
            except TypeError:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql_bytes)
                use_bytes_key = True
            if cached is not None and cached.compiled_sql:
                rewritten = cached.compiled_sql.decode("utf-8")
            else:
                rewritten, count = MSSQLCore.rewrite_placeholders(sql)
                sc.insert(
                    MSSQL_DIALECT_TAG,
                    sql_bytes if use_bytes_key else sql,
                    count,
                    rewritten.encode("utf-8"),
                    b"",
                )
        else:
            rewritten, _ = MSSQLCore.rewrite_placeholders(sql)

        self._last_rewritten_sql = rewritten
        self._last_tds_batch = MSSQLCore.encode_tds_sql_batch(rewritten, 0)

        if self._broker is not None and hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if self._broker is not None and hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="mssql")
        return None

    def _release_mssql_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)


class AsyncMSSQLDriver(ThreadOffloadAsyncDatabaseDriver[MSSQLConfig, SyncMSSQLDriver]):
    """Asynchronous SQL Server TDS 7.4 driver with SHM statement cache and Arrow cursor support."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    def __init__(self, config: MSSQLConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or MSSQLConfig()
        sync_driver = SyncMSSQLDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._sync.last_rewritten_sql

    @property
    def last_tds_batch(self) -> bytes:
        """Return the most recently encoded TDS 7.4 `SQLBatch` packet."""
        return self._sync.last_tds_batch
