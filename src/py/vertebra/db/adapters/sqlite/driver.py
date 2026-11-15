"""Synchronous and asynchronous SQLite WAL drivers."""

from __future__ import annotations

import asyncio
import datetime
import os
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import closing
from decimal import Decimal
from typing import Any, ClassVar, Self
from uuid import UUID

from vertebra._vertebra import LeaseBroker, classify_sql_statement
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.adapters.sqlite.core import VALID_SQLITE_SYNCHRONOUS
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    SQLITE_DIALECT_TAG,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    SyncDatabaseDriver,
    ThreadOffloadAsyncDatabaseDriver,
)
from vertebra.utils.serializers import encode_json
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

__all__ = (
    "AsyncSQLiteDriver",
    "SyncSQLiteDriver",
)


class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]):
    """Synchronous SQLite driver returning plain Python tuples with SHM broker and Arrow IPC support."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = SQLITE_DIALECT_TAG

    __slots__ = ("_conn", "_in_transaction", "_lastrowid", "_lock")

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or SQLiteConfig()
        if resolved.synchronous not in VALID_SQLITE_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {resolved.synchronous!r}"
            raise ValueError(msg)
        super().__init__(resolved, broker=broker)
        self._lock = threading.RLock()
        self._in_transaction: bool = False
        self._lastrowid: int | None = None
        try:
            self._conn = self._open_connection(self.config)
        except sqlite3.Error as exc:
            raise _translate_sqlite_error(exc) from exc

    @property
    def _config(self) -> SQLiteConfig:
        """Return the active SQLiteConfig."""
        return self.config

    @property
    def raw_connection(self) -> sqlite3.Connection:
        """Return the underlying sqlite3.Connection."""
        return self._conn

    @property
    def lastrowid(self) -> int | None:
        """Return the last captured SQLite INSERT rowid on this driver."""
        return self._lastrowid

    def connect(self) -> Self:
        """Establish or reopen the underlying SQLite connection."""
        with self._lock:
            if self._closed:
                try:
                    self._conn = self._open_connection(self.config)
                except sqlite3.Error as exc:
                    raise _translate_sqlite_error(exc) from exc
                self._in_transaction = False
                self._lastrowid = None
                self._closed = False
            return self

    def begin(self) -> None:
        """Begin an explicit SQLite transaction."""
        with self._lock:
            try:
                if not self._conn.in_transaction:
                    self._conn.execute("BEGIN")
                self._in_transaction = True
            except sqlite3.Error as exc:
                raise _translate_sqlite_error(exc) from exc

    def commit(self) -> None:
        """Commit the current SQLite transaction."""
        with self._lock:
            try:
                self._conn.commit()
            except sqlite3.Error as exc:
                raise _translate_sqlite_error(exc) from exc
            finally:
                self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        with self._lock:
            try:
                self._conn.rollback()
            except sqlite3.Error as exc:
                raise _translate_sqlite_error(exc) from exc
            finally:
                self._in_transaction = False

    def cancel(self) -> None:
        """Interrupt an active SQLite query, or act as an idempotent no-op when idle."""
        if not self._closed:
            self._conn.interrupt()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._closed = True
            self._in_transaction = False
            self._lastrowid = None
            self._conn.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        rowcount, _ = self.execute_dml(sql, parameters, **kwargs)
        return rowcount

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement and atomically return ``(rowcount, lastrowid)``."""
        _ = kwargs
        bound = _normalize_sqlite_parameters(parameters)
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            with self._lock:
                with closing(self._conn.execute(sql, bound)) as cur:
                    if cur.description is not None:
                        for _ in cur:
                            pass
                    rowcount = max(cur.rowcount, 0)
                    raw_rowid = cur.lastrowid
                    _, is_insert, _ = classify_sql_statement(sql)
                    rowid = int(raw_rowid) if raw_rowid is not None and is_insert else None
                    self._lastrowid = rowid
                if not self._in_transaction:
                    self._conn.commit()
                return rowcount, rowid
        except sqlite3.Error as exc:
            raise _translate_sqlite_error(exc) from exc
        finally:
            self._release_sqlite_broker_lease(lease)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a SQL statement across multiple parameter sets and return total affected rows."""
        _ = kwargs
        if not parameter_sets:
            return 0
        bound_sets = [_normalize_sqlite_parameters(item) for item in parameter_sets]
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            with self._lock:
                cur = self._conn.executemany(sql, bound_sets)
                if not self._in_transaction:
                    self._conn.commit()
                return cur.rowcount if cur.rowcount >= 0 else len(bound_sets)
        except sqlite3.Error as exc:
            raise _translate_sqlite_error(exc) from exc
        finally:
            self._release_sqlite_broker_lease(lease)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a SQL statement and return plain tuple rows and column names."""
        _ = kwargs
        bound = _normalize_sqlite_parameters(parameters)
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            with self._lock, closing(self._conn.execute(sql, bound)) as cur:
                rows = [tuple(row) for row in cur.fetchall()]
                cols = tuple(str(d[0]) for d in cur.description) if cur.description else ()
                raw_rowid = cur.lastrowid
                _, is_insert, is_read_only = classify_sql_statement(sql)
                self._lastrowid = int(raw_rowid) if raw_rowid is not None and is_insert else None
                if not self._in_transaction and not is_read_only:
                    self._conn.commit()
                return rows, cols
        except sqlite3.Error as exc:
            raise _translate_sqlite_error(exc) from exc
        finally:
            self._release_sqlite_broker_lease(lease)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and return an `ArrowCursor` via zero-copy ShmRegion Arrow IPC."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        col_list = list(cols) if cols else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        return ArrowCursor.from_rows(rows, col_list, threshold_rows=threshold_rows)

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
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        bound_rows = [_normalize_sqlite_parameters(row) for row in rows]
        lease = self._acquire_sqlite_broker_lease(sql)
        try:
            with self._lock:
                self._conn.executemany(sql, bound_rows)
                if not self._in_transaction:
                    self._conn.commit()
                return len(rows)
        except sqlite3.Error as exc:
            raise _translate_sqlite_error(exc) from exc
        finally:
            self._release_sqlite_broker_lease(lease)

    def last_insert_rowid(self) -> int | None:
        """Return the last inserted SQLite rowid under the driver lock."""
        with self._lock, closing(self._conn.cursor()) as cur:
            cur.execute("SELECT last_insert_rowid()")
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    @staticmethod
    def _open_connection(config: SQLiteConfig) -> sqlite3.Connection:
        """Open a sqlite3 connection with URI detection and configured pragmas."""
        target = str(config.path)
        conn = sqlite3.connect(target, check_same_thread=False, uri=target.startswith("file:"))
        for pragma in config.initialization_pragmas():
            conn.execute(pragma)
        return conn

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
                    if sc.lookup(SQLITE_DIALECT_TAG, sql) is None:
                        sc.insert(SQLITE_DIALECT_TAG, sql, 0, sql_bytes, b"")
                except TypeError:
                    if sc.lookup(SQLITE_DIALECT_TAG, sql_bytes) is None:
                        sc.insert(SQLITE_DIALECT_TAG, sql_bytes, 0, sql_bytes, b"")
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        acquire_sync = getattr(self._broker, "acquire_sync", None)
        if callable(acquire_sync):
            return acquire_sync(bind_key="sqlite")
        if isinstance(self._broker, LeaseBroker):
            return self._broker.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            return self._broker.acquire(bind_key="sqlite")
        return None

    def _release_sqlite_broker_lease(self, lease: Any) -> None:
        """Release a previously acquired SQLite broker lease."""
        if lease is None:
            return
        release_sync = getattr(lease, "release_sync", None)
        if callable(release_sync):
            release_sync()
            return
        if hasattr(lease, "release") and callable(lease.release):
            lease.release()
        elif self._broker is not None and hasattr(self._broker, "release") and hasattr(lease, "slot_index"):
            self._broker.release(lease.slot_index, lease.generation)
        elif self._broker is not None and hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            self._broker.lease_table.release(lease.slot_index, lease.generation)


class AsyncSQLiteDriver(ThreadOffloadAsyncDatabaseDriver[SQLiteConfig, SyncSQLiteDriver]):
    """Asynchronous SQLite driver facade compatible with Python 3.12+ and free-threaded 3.14t."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = SQLITE_DIALECT_TAG

    __slots__ = ()

    def __init__(self, config: SQLiteConfig | None = None, *, broker: Any = None) -> None:
        resolved = config or SQLiteConfig()
        sync_driver = SyncSQLiteDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def closed(self) -> bool:
        """Return True when the SQLite connection has been closed."""
        return self._sync.closed

    @property
    def lastrowid(self) -> int | None:
        """Return the last captured SQLite INSERT rowid on this driver."""
        return self._sync.lastrowid

    async def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement off the loop and return ``(rowcount, lastrowid)``."""
        return await asyncio.to_thread(self._sync.execute_dml, sql, parameters, **kwargs)

    async def last_insert_rowid(self) -> int | None:
        """Return the last inserted SQLite rowid asynchronously off the event loop."""
        return await asyncio.to_thread(self._sync.last_insert_rowid)


def _translate_sqlite_error(exc: sqlite3.Error) -> DatabaseError:
    """Translate a `sqlite3.Error` into the matching `vertebra.db.driver.DatabaseError` subclass."""
    msg = str(exc)
    if isinstance(exc, sqlite3.IntegrityError):
        return IntegrityError(msg)
    if isinstance(exc, sqlite3.ProgrammingError):
        return ProgrammingError(msg)
    if isinstance(exc, sqlite3.DataError):
        return DataError(msg)
    if isinstance(exc, sqlite3.OperationalError):
        return OperationalError(msg)
    if isinstance(exc, sqlite3.InterfaceError):
        return InterfaceError(msg)
    if isinstance(exc, sqlite3.NotSupportedError):
        return NotSupportedError(msg)
    if isinstance(exc, sqlite3.InternalError):
        return InternalError(msg)
    return DatabaseError(msg)


def _normalize_sqlite_param(value: Any) -> Any:
    """Normalize rich Python bind values into SQLite-compatible scalar types."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return encode_json(value)
    return value


def _normalize_sqlite_parameters(
    parameters: Sequence[Any] | Mapping[str, Any] | None,
) -> tuple[Any, ...] | dict[str, Any]:
    """Normalize positional or named bind parameters for `sqlite3.Connection`."""
    if parameters is None:
        return ()
    if isinstance(parameters, Mapping):
        return {str(key): _normalize_sqlite_param(val) for key, val in parameters.items()}
    return tuple(_normalize_sqlite_param(val) for val in parameters)
