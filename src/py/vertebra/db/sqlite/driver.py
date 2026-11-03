"""Synchronous and asynchronous SQLite WAL drivers."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from vertebra.db._arrow import (
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    SyncDatabaseDriver,
    ThreadOffloadAsyncDatabaseDriver,
    _normalize_driver_params,
)
from vertebra.db.sqlite.config import SQLiteConfig
from vertebra.db.sqlite.core import VALID_SQLITE_SYNCHRONOUS

__all__ = (
    "AsyncSQLiteDriver",
    "SyncSQLiteDriver",
)


class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]):
    """Synchronous SQLite driver returning plain Python tuples with SHM broker and Arrow IPC support."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = 3

    __slots__ = ("_conn",)

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or SQLiteConfig()
        if resolved.synchronous not in VALID_SQLITE_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {resolved.synchronous!r}"
            raise ValueError(msg)
        super().__init__(resolved, broker=broker)
        self._conn = sqlite3.connect(str(self.config.path), check_same_thread=False)
        for pragma in self.config.initialization_pragmas():
            self._conn.execute(pragma)

    @property
    def _config(self) -> SQLiteConfig:
        """Return the active SQLiteConfig."""
        return self.config

    @property
    def raw_connection(self) -> sqlite3.Connection:
        """Return the underlying sqlite3.Connection."""
        return self._conn

    def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement and return plain tuples and column descriptions."""
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            cur = self._conn.execute(sql, parameters)
            rows = [tuple(row) for row in cur.fetchall()]
            desc = (
                tuple((d[0], d[1], None, None, None, None, None) for d in cur.description) if cur.description else None
            )
            return rows, desc
        finally:
            if lease is not None:
                if hasattr(lease, "release"):
                    lease.release()
                elif self._broker is not None and hasattr(self._broker, "release") and hasattr(lease, "slot_index"):
                    self._broker.release(lease.slot_index, lease.generation)
                elif (
                    self._broker is not None
                    and hasattr(self._broker, "lease_table")
                    and self._broker.lease_table is not None
                ):
                    self._broker.lease_table.release(lease.slot_index, lease.generation)

    def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples (`list[tuple[Any, ...]]`)."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def execute(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement with variadic positional or explicit parameters."""
        resolved = _normalize_driver_params(params, parameters)
        return self.execute_tuples(sql, resolved)

    def select(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a SELECT statement and return rows as dictionaries or hydrated `schema_type` instances."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    def select_one(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a SELECT statement and return the first row or raise `ValueError`."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a SELECT statement and return the first row or `None`."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            self._conn.executemany(sql, rows)
            self._conn.commit()
            return len(rows)
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        shm_threshold_rows: int = 0,
    ) -> ArrowCursor:
        """Execute a query and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=shm_threshold_rows)

    def commit(self) -> None:
        """Commit the current SQLite transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._closed = True
        self._conn.close()

    def _acquire_sqlite_broker_lease(self, sql: str) -> Any:
        """Acquire a lease and statement cache plan from the attached broker if present."""
        if self._broker is None:
            return None
        if hasattr(self._broker, "get_or_compile_statement"):
            self._broker.get_or_compile_statement("sqlite", sql, lambda s: (s, ()))
        elif hasattr(self._broker, "statement_cache") and self._broker.statement_cache is not None:
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    if sc.lookup(3, sql) is None:
                        sc.insert(3, sql, 0, sql_bytes, b"")
                except TypeError:
                    if sc.lookup(3, sql_bytes) is None:
                        sc.insert(3, sql_bytes, 0, sql_bytes, b"")
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(bind_key="sqlite")
            except TypeError:
                return self._broker.acquire(os.getpid())
        return None


class AsyncSQLiteDriver(ThreadOffloadAsyncDatabaseDriver[SQLiteConfig, SyncSQLiteDriver]):
    """Asynchronous SQLite driver facade compatible with Python 3.12+ and free-threaded 3.14t."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = 3

    __slots__ = ()

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or SQLiteConfig()
        sync_driver = SyncSQLiteDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def closed(self) -> bool:
        """Return True when the SQLite connection has been closed."""
        return self._sync.closed

    async def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement returning rows and column descriptions."""
        return await asyncio.to_thread(self._sync.execute_with_description, sql, parameters)

    async def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters)

    async def execute(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(self._sync.execute_tuples, sql, resolved)

    async def select(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Asynchronously execute a SELECT statement and return rows as dictionaries or `schema_type` instances."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(self._sync.select, sql, parameters=resolved, schema_type=schema_type)

    async def select_one(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Asynchronously execute a SELECT statement and return the first row or raise `ValueError`."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(self._sync.select_one, sql, parameters=resolved, schema_type=schema_type)

    async def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Asynchronously execute a SELECT statement and return the first row or `None`."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(
            self._sync.select_one_or_none,
            sql,
            parameters=resolved,
            schema_type=schema_type,
        )

    async def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        return await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, columns=columns)

    async def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        shm_threshold_rows: int = 0,
    ) -> ArrowCursor:
        """Asynchronously execute a query and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            parameters=resolved,
            shm_threshold_rows=shm_threshold_rows,
        )
