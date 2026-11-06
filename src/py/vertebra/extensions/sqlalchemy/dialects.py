"""SQLAlchemy 2.1 +vertebra / +vtb sync and async dialects, PEP 249 DBAPI bridges, and 3.14t pool adapters."""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect as BaseMySQLDialect
from sqlalchemy.dialects.oracle.base import OracleDialect as BaseOracleDialect
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect as BaseSQLiteDialect
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, Pool, QueuePool, StaticPool

from vertebra.db.adapters.bigquery.config import BigQueryConfig
from vertebra.db.adapters.spanner.config import SpannerConfig
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.registry import DatabaseRegistry
from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.types import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "ALL_DIALECT_SCHEMES",
    "DIALECT_DESCRIPTORS",
    "DIALECT_SCHEMES",
    "AsyncDBAPIConnection",
    "AsyncDBAPICursor",
    "AsyncDBAPIModule",
    "AsyncBigQueryDialect",
    "SyncBigQueryDialect",
    "SyncDBAPIConnection",
    "SyncDBAPICursor",
    "SyncDBAPIModule",
    "DataError",
    "DatabaseError",
    "AsyncDuckDBDialect",
    "SyncDuckDBDialect",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "AsyncMSSQLDialect",
    "SyncMSSQLDialect",
    "AsyncMySQLDialect",
    "SyncMySQLDialect",
    "NotSupportedError",
    "NullPool",
    "OperationalError",
    "AsyncOracleDialect",
    "SyncOracleDialect",
    "AsyncPostgreSQLDialect",
    "SyncPostgreSQLDialect",
    "ProgrammingError",
    "AsyncSQLiteDialect",
    "SyncSQLiteDialect",
    "AsyncSpangresDialect",
    "SyncSpangresDialect",
    "AsyncSpannerDialect",
    "SyncSpannerDialect",
    "SyncEngineAdapter",
    "Warning",
    "attach_serverless_disconnect_guard",
    "is_free_threaded",
)


def is_free_threaded() -> bool:
    """Return True when the CPython GIL is disabled."""
    checker = getattr(sys, "_is_gil_enabled", None)
    return bool(checker and not checker())


DIALECT_SCHEMES: tuple[str, ...] = (
    "postgresql+vertebra",
    "postgresql+vtb",
    "oracle+vertebra",
    "oracle+vtb",
    "mysql+vertebra",
    "mysql+vtb",
    "sqlite+vertebra",
    "sqlite+vtb",
)

ALL_DIALECT_SCHEMES: tuple[str, ...] = (
    *DIALECT_SCHEMES,
    "postgresql+vertebra_async",
    "postgresql+vtb_async",
    "oracle+vertebra_async",
    "oracle+vtb_async",
    "mysql+vertebra_async",
    "mysql+vtb_async",
    "sqlite+vertebra_async",
    "sqlite+vtb_async",
    "mssql+vertebra",
    "mssql+vtb",
    "mssql+vertebra_async",
    "mssql+vtb_async",
    "duckdb+vertebra",
    "duckdb+vtb",
    "duckdb+vertebra_async",
    "duckdb+vtb_async",
    "bigquery+vertebra",
    "bigquery+vtb",
    "bigquery+vertebra_async",
    "bigquery+vtb_async",
    "spanner+vertebra",
    "spanner+vtb",
    "spanner+vertebra_async",
    "spanner+vtb_async",
    "spangres+vertebra",
    "spangres+vtb",
    "spangres+vertebra_async",
    "spangres+vtb_async",
)


class Error(Exception):
    """Base PEP 249 DBAPI exception."""

    def __getattr__(self, key: str) -> Any:
        msg = f"{type(self).__name__!r} object has no attribute {key!r}"
        raise AttributeError(msg)


class Warning(Exception):
    """PEP 249 DBAPI warning exception."""


class InterfaceError(Error):
    """PEP 249 database interface error."""


class DatabaseError(Error):
    """PEP 249 database operation error."""


class DataError(DatabaseError):
    """PEP 249 data processing error."""


class OperationalError(DatabaseError):
    """PEP 249 database operational error."""


class IntegrityError(DatabaseError):
    """PEP 249 relational integrity constraint error."""


class InternalError(DatabaseError):
    """PEP 249 database internal error."""


class ProgrammingError(DatabaseError):
    """PEP 249 SQL programming error."""


class NotSupportedError(DatabaseError):
    """PEP 249 feature not supported error."""


Binary = bytes


class SyncDBAPICursor:
    """PEP 249 DBAPI 2.0 cursor returning plain Python tuples for SQLAlchemy 2.1 ORM loaders."""

    __slots__ = ("_conn", "_pos", "_raw_cursor", "_rows", "arraysize", "description", "lastrowid", "rowcount")

    def __init__(self, connection: SyncDBAPIConnection, raw_cursor: Any = None) -> None:
        """Initialize PEP 249 cursor with connection and optional underlying DBAPI cursor."""
        self._conn = connection
        self._raw_cursor = raw_cursor
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> SyncDBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        if self._raw_cursor is not None:
            self._raw_cursor.execute(operation, parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
            return self

        if not _is_row_returning_sql(operation):
            affected = self._conn.execute_dml(operation, parameters)
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = affected
            return self

        self._rows, self.description = self._conn.execute_with_description(operation, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]]
    ) -> SyncDBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        if self._raw_cursor is not None:
            self._raw_cursor.executemany(operation, seq_of_parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
            return self

        if not _is_row_returning_sql(operation):
            affected = self._conn.execute_many_dml(operation, seq_of_parameters)
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = affected
            return self

        total = 0
        last_rows: list[tuple[Any, ...]] = []
        last_desc: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        for params in seq_of_parameters:
            rows, desc = self._conn.execute_with_description(operation, params)
            total += len(rows)
            last_rows = rows
            last_desc = desc
        self._rows = last_rows
        self.description = last_desc
        self._pos = 0
        self.rowcount = total
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next plain-tuple row or None."""
        if self._raw_cursor is not None:
            row = self._raw_cursor.fetchone()
            return tuple(row) if row is not None and not isinstance(row, tuple) else row
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to size plain-tuple rows."""
        if self._raw_cursor is not None:
            rows = self._raw_cursor.fetchmany(size if size is not None else self.arraysize)
            return [tuple(r) if not isinstance(r, tuple) else r for r in rows]
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining plain-tuple rows."""
        if self._raw_cursor is not None:
            rows = self._raw_cursor.fetchall()
            return [tuple(r) if not isinstance(r, tuple) else r for r in rows]
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        """Close the cursor."""
        if self._raw_cursor is not None and hasattr(self._raw_cursor, "close"):
            self._raw_cursor.close()
        self._rows = []
        self._pos = 0


class SyncDBAPIConnection:
    """PEP 249 connection backed by Vertebra native drivers or sqlite3.

    BigQuery, Spanner, and Spangres use statement autocommit and reject
    ``autocommit=False``. Use their native driver APIs for explicit transaction
    or batching scopes.
    """

    __slots__ = (
        "_autocommit",
        "_broker",
        "_config",
        "_driver",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_sqlite_conn",
        "_url",
        "closed",
    )

    def __init__(
        self,
        url: str = "sqlite+vtb:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with URL or database parameters."""
        resolved_url = url
        if database is not None and (url == "sqlite+vtb:///:memory:" or not url):
            resolved_url = f"sqlite+vtb:///{database}"
        self._url = resolved_url
        self._share_pool = bool(share_pool)
        self._broker = (
            broker if broker is not None else (DatabaseRegistry.default_broker() if self._share_pool else None)
        )
        self._in_transaction = False
        self.closed: bool = False

        self._config: Any = None
        try:
            self._config = (
                DatabaseRegistry.parse_url(resolved_url, broker=self._broker)
                if self._broker is not None
                else DatabaseRegistry.parse_url(resolved_url)
            )
        except Exception:
            self._config = SQLiteConfig(database=database or ":memory:", broker=self._broker)

        self._autocommit = isinstance(self._config, (BigQueryConfig, SpannerConfig))
        self._driver = driver
        self._owns_driver = driver is not None
        self._sqlite_conn: sqlite3.Connection | None = None

        if self._driver is None:
            if isinstance(self._config, SQLiteConfig) and self._broker is None:
                db_target = str(self._config.database or ":memory:")
                if db_target.startswith("sqlite+"):
                    db_target = db_target.split(":///", 1)[-1] if ":///" in db_target else ":memory:"
                self._sqlite_conn = sqlite3.connect(db_target, check_same_thread=False)
            if self._share_pool and broker is None:
                self._driver = DatabaseRegistry.default().get_shared_sync_driver(self._config)
            else:
                target_registry = DatabaseRegistry(broker=self._broker)
                self._driver = self._config.build_sync_driver(target_registry)
                self._owns_driver = True

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> Any:
        """Return the parsed database configuration."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached shared-memory database broker, if any."""
        return self._broker

    @property
    def driver(self) -> Any:
        """Return the active underlying driver instance."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for cloud backends."""
        if not value and isinstance(self._config, (BigQueryConfig, SpannerConfig)):
            msg = "BigQuery, Spanner, and Spangres DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        if value and self._in_transaction:
            self.commit()
        self._autocommit = value
        if self._sqlite_conn is not None:
            self._sqlite_conn.autocommit = value

    def cursor(self) -> SyncDBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        if self._sqlite_conn is not None:
            return SyncDBAPICursor(self, raw_cursor=self._sqlite_conn.cursor())
        return SyncDBAPICursor(self)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a query returning rows and column descriptions."""
        if self._sqlite_conn is not None:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, parameters)
            rows = [tuple(r) for r in cur.fetchall()]
            desc = cur.description
            return rows, desc

        self.begin()
        if self._driver is not None:
            if hasattr(self._driver, "execute_with_description"):
                raw_res = self._driver.execute_with_description(sql, parameters)
                if isinstance(raw_res, tuple) and len(raw_res) >= 2:
                    col_names = raw_res[1]
                    desc = (
                        tuple(
                            (col[0] if isinstance(col, tuple) else str(col), None, None, None, None, None, None)
                            for col in col_names
                        )
                        if col_names
                        else None
                    )
                    if self._autocommit:
                        self.commit()
                    return raw_res[0], desc
                return [], None
            if hasattr(self._driver, "execute_tuples"):
                rows = self._driver.execute_tuples(sql, parameters)
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0])))
                    if rows
                    else None
                )
                if self._autocommit:
                    self.commit()
                return rows, desc
            if hasattr(self._driver, "select"):
                dict_params = parameters if isinstance(parameters, dict) else None
                dict_rows = self._driver.select(sql, dict_params)
                if not dict_rows:
                    return [], None
                keys = tuple(dict_rows[0].keys())
                desc = tuple((str(k), None, None, None, None, None, None) for k in keys)
                rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
                if self._autocommit:
                    self.commit()
                return rows, desc
        return [], None

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> int:
        """Execute a non-row-returning SQL statement and return the affected row count."""
        if self._sqlite_conn is not None:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, parameters)
            return cur.rowcount
        self.begin()
        if self._driver is not None:
            if hasattr(self._driver, "execute"):
                affected = int(self._driver.execute(sql, parameters))
                if self._autocommit:
                    self.commit()
                return affected
            rows, _ = self.execute_with_description(sql, parameters)
            return len(rows)
        return -1

    def execute_many_dml(
        self,
        sql: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> int:
        """Execute a non-row-returning SQL statement across multiple parameter sets and return affected rows."""
        if not seq_of_parameters:
            return 0
        if self._sqlite_conn is not None:
            cur = self._sqlite_conn.cursor()
            cur.executemany(sql, seq_of_parameters)
            return cur.rowcount
        self.begin()
        if self._driver is not None:
            if hasattr(self._driver, "execute_many"):
                affected = int(self._driver.execute_many(sql, seq_of_parameters))
                if self._autocommit:
                    self.commit()
                return affected
            if hasattr(self._driver, "execute"):
                return sum(self.execute_dml(sql, params) for params in seq_of_parameters)
            return sum(len(self.execute_with_description(sql, params)[0]) for params in seq_of_parameters)
        return -1

    def execute_raw_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        if self._autocommit or self._in_transaction or self._sqlite_conn is not None:
            return
        if self._driver is not None and hasattr(self._driver, "begin"):
            self._driver.begin()
            self._in_transaction = True

    def commit(self) -> None:
        """Commit the active transaction."""
        self._in_transaction = False
        if self._sqlite_conn is not None:
            self._sqlite_conn.commit()
        elif self._driver is not None and hasattr(self._driver, "commit"):
            self._driver.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        self._in_transaction = False
        if self._sqlite_conn is not None:
            self._sqlite_conn.rollback()
        elif self._driver is not None and hasattr(self._driver, "rollback"):
            self._driver.rollback()

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            if self._in_transaction:
                if self._autocommit:
                    self.commit()
                else:
                    self.rollback()
            if self._sqlite_conn is not None:
                self._sqlite_conn.close()
            if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                self._driver.close()
            self.closed = True


class SyncDBAPIModule:
    """PEP 249 DBAPI 2.0 module descriptor exposed by Vertebra SQLAlchemy dialects."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str = "qmark"

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    Binary = Binary

    sqlite_version: str = sqlite3.sqlite_version
    sqlite_version_info: tuple[int, ...] = sqlite3.sqlite_version_info

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        """Initialize PEP 249 DBAPI module with target parameter style."""
        self.paramstyle = paramstyle

    def __getattr__(self, key: str) -> Any:
        msg = f"{type(self).__name__!r} object has no attribute {key!r}"
        raise AttributeError(msg)

    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection:
        """Open a PEP 249 connection."""
        url_arg = "sqlite+vtb:///:memory:"
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        return SyncDBAPIConnection(url_arg, **kwargs)


class AsyncDBAPICursor:
    """Asynchronous cursor wrapping async drivers and reporting execution metrics to AsyncAdapt."""

    __slots__ = (
        "_conn",
        "_driver",
        "_pos",
        "_rows",
        "arraysize",
        "description",
        "lastrowid",
        "rowcount",
    )

    def __init__(self, connection: AsyncDBAPIConnection) -> None:
        """Initialize asynchronous cursor."""
        self._conn = connection
        self._driver = connection.driver
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1

    async def __aenter__(self) -> AsyncDBAPICursor:
        """Enter asynchronous cursor context."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit asynchronous cursor context."""
        await self.close()

    async def execute(
        self, operation: str, parameters: Sequence[Any] | dict[str, Any] | None = None
    ) -> AsyncDBAPICursor:
        """Execute a SQL statement asynchronously."""
        params = () if parameters is None else parameters
        await self._conn.begin()
        if not _is_row_returning_sql(operation) and self._driver is not None and hasattr(self._driver, "execute"):
            affected = await self._driver.execute(operation, params)
            self._rows = []
            self.description = None
            self.rowcount = int(affected)
        elif self._driver is not None and hasattr(self._driver, "execute_with_description"):
            raw_res = await self._driver.execute_with_description(operation, params)
            if isinstance(raw_res, tuple) and len(raw_res) >= 2:
                self._rows = raw_res[0]
                col_names = raw_res[1]
                self.description = (
                    tuple(
                        (col[0] if isinstance(col, tuple) else str(col), None, None, None, None, None, None)
                        for col in col_names
                    )
                    if col_names
                    else None
                )
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = -1
        elif self._driver is not None and hasattr(self._driver, "execute_tuples"):
            self._rows = await self._driver.execute_tuples(operation, params)
            self.description = (
                tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._rows[0])))
                if self._rows
                else None
            )
            self.rowcount = len(self._rows)
        elif self._driver is not None and hasattr(self._driver, "select"):
            dict_params = params if isinstance(params, dict) else None
            dict_rows = await self._driver.select(operation, dict_params)
            if dict_rows:
                keys = tuple(dict_rows[0].keys())
                self.description = tuple((str(k), None, None, None, None, None, None) for k in keys)
                self._rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = 0
        else:
            self._rows = []
            self.description = None
            self.rowcount = -1

        self._pos = 0
        if operation.strip().upper().startswith("INSERT") and hasattr(self._driver, "_sync"):
            sync_drv = getattr(self._driver, "_sync", None)
            raw_conn = getattr(sync_drv, "raw_connection", None)
            if raw_conn is not None:
                row = await asyncio.to_thread(self._fetch_last_insert_rowid, raw_conn)
                if row:
                    self.lastrowid = row[0]
        if self._conn.autocommit:
            await self._conn.commit()
        return self

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> AsyncDBAPICursor:
        """Execute parameterized SQL across multiple parameter sets asynchronously."""
        if not seq_of_parameters:
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = 0
            return self
        await self._conn.begin()
        if not _is_row_returning_sql(operation) and self._driver is not None and hasattr(self._driver, "execute_many"):
            affected = await self._driver.execute_many(operation, seq_of_parameters)
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = int(affected)
            if self._conn.autocommit:
                await self._conn.commit()
            return self
        total = 0
        last_rows: list[tuple[Any, ...]] = []
        last_desc: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        for params in seq_of_parameters:
            await self.execute(operation, params)
            total += self.rowcount
            last_rows = self._rows
            last_desc = self.description
        self._rows = last_rows
        self.description = last_desc
        self._pos = 0
        self.rowcount = total
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row asynchronously."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to size rows asynchronously."""
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows asynchronously."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    async def close(self) -> None:
        """Close the asynchronous cursor."""
        self._rows = []
        self._pos = 0

    @staticmethod
    def _fetch_last_insert_rowid(raw_conn: Any) -> Any:
        """Query ``last_insert_rowid()`` from a SQLite raw connection on a worker thread."""
        cur = raw_conn.cursor()
        cur.execute("SELECT last_insert_rowid()")
        return cur.fetchone()


class AsyncDBAPIConnection:
    """Asynchronous DBAPI connection adapting Vertebra async drivers.

    BigQuery, Spanner, and Spangres use statement autocommit and reject
    ``autocommit=False``. Use their native driver APIs for explicit transaction
    or batching scopes.
    """

    __slots__ = (
        "_autocommit",
        "_broker",
        "_config",
        "_driver",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_url",
        "closed",
    )

    def __init__(
        self,
        url: str = "sqlite+vtb_async:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous connection."""
        resolved_url = url
        if database is not None and (url == "sqlite+vtb_async:///:memory:" or not url):
            resolved_url = f"sqlite+vtb_async:///{database}"
        self._url = resolved_url
        self._share_pool = bool(share_pool)
        self._broker = (
            broker if broker is not None else (DatabaseRegistry.default_broker() if self._share_pool else None)
        )
        self._in_transaction = False
        self.closed: bool = False

        self._config: Any = None
        try:
            self._config = (
                DatabaseRegistry.parse_url(resolved_url, broker=self._broker)
                if self._broker is not None
                else DatabaseRegistry.parse_url(resolved_url)
            )
        except Exception:
            self._config = SQLiteConfig(database=database or ":memory:", broker=self._broker)

        self._autocommit = isinstance(self._config, (BigQueryConfig, SpannerConfig))
        self._driver = driver
        self._owns_driver = driver is not None
        if self._driver is None:
            if self._share_pool and broker is None:
                self._driver = DatabaseRegistry.default().get_shared_driver(self._config)
            else:
                target_registry = DatabaseRegistry(broker=self._broker)
                self._driver = self._config.build_async_driver(target_registry)
                self._owns_driver = True

    @property
    def driver(self) -> Any:
        """Return the active asynchronous driver."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for cloud backends."""
        if not value and isinstance(self._config, (BigQueryConfig, SpannerConfig)):
            msg = "BigQuery, Spanner, and Spangres DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        self._autocommit = value

    def __getattr__(self, name: str) -> Any:
        if self._driver is not None and hasattr(self._driver, name):
            return getattr(self._driver, name)
        msg = f"{type(self).__name__!r} object has no attribute {name!r}"
        raise AttributeError(msg)

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        """Create a new asynchronous DBAPI cursor."""
        _ = (args, kwargs)
        return AsyncDBAPICursor(self)

    async def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        if self._autocommit:
            if self._in_transaction:
                await self.commit()
            return
        if not self._in_transaction and self._driver is not None and hasattr(self._driver, "begin"):
            await self._driver.begin()
            self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active asynchronous transaction."""
        self._in_transaction = False
        if self._driver is not None and hasattr(self._driver, "commit"):
            await self._driver.commit()

    async def rollback(self) -> None:
        """Roll back the active asynchronous transaction."""
        self._in_transaction = False
        if self._driver is not None and hasattr(self._driver, "rollback"):
            await self._driver.rollback()

    async def close(self) -> None:
        """Close the asynchronous driver connection."""
        if not self.closed:
            if self._in_transaction:
                if self._autocommit:
                    await self.commit()
                else:
                    await self.rollback()
            if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                await self._driver.close()
            self.closed = True


class AsyncDBAPIModule:
    """Asynchronous DBAPI module adapter delegating execution to AsyncDBAPIConnection."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str = "qmark"

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    Binary = Binary

    sqlite_version: str = sqlite3.sqlite_version
    sqlite_version_info: tuple[int, ...] = sqlite3.sqlite_version_info

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        """Initialize asynchronous DBAPI module."""
        self.paramstyle = paramstyle

    def __getattr__(self, key: str) -> Any:
        msg = f"{type(self).__name__!r} object has no attribute {key!r}"
        raise AttributeError(msg)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_dbapi_connection:
        """Establish an asynchronous connection wrapped in SQLAlchemy's AsyncAdapt_dbapi_connection."""
        url_arg = "sqlite+vtb_async:///:memory:"
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        async_conn = AsyncDBAPIConnection(url_arg, **kwargs)
        return AsyncAdapt_dbapi_connection(self, async_conn)


class _NativeTransactionDialect:
    """Forward SQLAlchemy transaction begin to the owning DBAPI connection."""

    __slots__ = ()

    def do_begin(self, dbapi_connection: Any) -> None:
        """Begin through the sync connection or SQLAlchemy's async adaptation."""
        if hasattr(dbapi_connection, "run_async"):
            dbapi_connection.run_async(lambda conn: conn.begin())
        else:
            dbapi_connection.begin()


class SyncPostgreSQLDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for PostgreSQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    paramstyle = "pyformat"
    use_insertmanyvalues = True
    insert_executemany_returning = True
    supports_native_jsonb = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="pyformat"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise standard QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement."""
        return clause.to_sql()

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the PostgreSQL dialect."""
        return col_type


class AsyncPostgreSQLDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for PostgreSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="pyformat"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncSQLiteDialect(_NativeTransactionDialect, BaseSQLiteDialect):
    """SQLAlchemy 2.1 synchronous dialect for SQLite (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return QueuePool


class AsyncSQLiteDialect(_NativeTransactionDialect, BaseSQLiteDialect):
    """SQLAlchemy 2.1 asynchronous dialect for SQLite (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return AsyncAdaptedQueuePool


class SyncMySQLDialect(_NativeTransactionDialect, BaseMySQLDialect):
    """SQLAlchemy 2.1 synchronous dialect for MySQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    paramstyle = "format"
    use_insertmanyvalues = True
    insert_executemany_returning = False
    supports_native_jsonb = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="format"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MySQL dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


class AsyncMySQLDialect(_NativeTransactionDialect, BaseMySQLDialect):
    """SQLAlchemy 2.1 asynchronous dialect for MySQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="format"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncOracleDialect(_NativeTransactionDialect, BaseOracleDialect):
    """SQLAlchemy 2.1 synchronous dialect for Oracle (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING ... INTO query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return (
            f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) "
            f"RETURNING {pk_column}, {sentinel_col} INTO :ret_{pk_column}, :ret_{sentinel_col}"
        )

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the Oracle dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


class AsyncOracleDialect(_NativeTransactionDialect, BaseOracleDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Oracle (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncMSSQLDialect(_NativeTransactionDialect, MSDialect):
    """SQLAlchemy 2.1 synchronous dialect for Microsoft SQL Server (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool


class AsyncMSSQLDialect(_NativeTransactionDialect, MSDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Microsoft SQL Server (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncDuckDBDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for DuckDB (+vtb / +vertebra)."""

    name = "duckdb"
    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool


class AsyncDuckDBDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 asynchronous dialect for DuckDB (+vtb_async / +vertebra_async)."""

    name = "duckdb"
    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncBigQueryDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for Google BigQuery (+vtb / +vertebra)."""

    name = "bigquery"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so BigQuery delegates connection pooling to DatabaseRegistry."""
        _ = url
        return NullPool

    @classmethod
    def compile_merge_upsert(
        cls,
        target_table: str,
        source_table: str,
        on_keys: Sequence[str],
        update_columns: Sequence[str],
        insert_columns: Sequence[str] | None = None,
    ) -> str:
        """Compile a GoogleSQL `MERGE INTO` upsert statement for BigQuery."""
        if not on_keys:
            msg = "BigQuery MERGE upsert requires at least one key column in on_keys"
            raise ValueError(msg)
        resolved_insert = list(insert_columns) if insert_columns is not None else [*on_keys, *update_columns]
        on_clause = " AND ".join(f"T.{k} = S.{k}" for k in on_keys)
        set_clause = ", ".join(f"T.{c} = S.{c}" for c in update_columns)
        ins_cols = ", ".join(resolved_insert)
        ins_vals = ", ".join(f"S.{c}" for c in resolved_insert)
        return (
            f"MERGE INTO {target_table} AS T "
            f"USING {source_table} AS S "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
        )


class AsyncBigQueryDialect(SyncBigQueryDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Google BigQuery (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="named"))


class SyncSpannerDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner GoogleSQL (+vtb / +vertebra)."""

    name = "spanner"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool

    @classmethod
    def compile_insert_or_update(
        cls,
        table_name: str,
        columns: Sequence[str],
    ) -> str:
        """Compile a GoogleSQL `INSERT OR UPDATE INTO` statement with `@param` bindings."""
        if not columns:
            msg = "Spanner INSERT OR UPDATE requires at least one column"
            raise ValueError(msg)
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f"@{c}" for c in columns)
        return f"INSERT OR UPDATE INTO {table_name} ({cols_str}) VALUES ({vals_str})"

    @classmethod
    def extract_interleave_options(cls, table_kwargs: Mapping[str, Any]) -> tuple[str | None, str]:
        """Extract `(parent_table, on_delete)` from SQLAlchemy table dialect options."""
        parent = table_kwargs.get("spanner_interleave_in_parent")
        on_delete = str(table_kwargs.get("spanner_interleave_on_delete") or "CASCADE").upper()
        if on_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Invalid spanner_interleave_on_delete option: {on_delete!r}"
            raise ValueError(msg)
        return (str(parent) if parent else None, on_delete)

    @classmethod
    def compile_interleave_clause(
        cls,
        parent_table: str,
        *,
        on_delete: str = "CASCADE",
    ) -> str:
        """Render a Cloud Spanner `INTERLEAVE IN PARENT` table suffix clause."""
        norm_delete = on_delete.upper()
        if norm_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Unsupported INTERLEAVE ON DELETE action: {on_delete!r}"
            raise ValueError(msg)
        return f", INTERLEAVE IN PARENT {parent_table} ON DELETE {norm_delete}"


class AsyncSpannerDialect(SyncSpannerDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner GoogleSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="named"))


class SyncSpangresDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner Spangres (+vtb / +vertebra)."""

    name = "spangres"
    driver = "vtb"
    paramstyle = "pyformat"
    supports_statement_cache = True
    supports_native_jsonb = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="pyformat"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner Spangres multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool


class AsyncSpangresDialect(SyncSpangresDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner Spangres (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: AsyncDBAPIModule(paramstyle="pyformat"))


class SyncEngineAdapter:
    """Optional-greenlet SQLAlchemy 2.1 execution adapter for free-threaded Python 3.14t."""

    __slots__ = ("_pool",)

    def __init__(self, *, max_workers: int = 8) -> None:
        """Initialize SyncEngineAdapter with thread pool capacity."""
        self._pool = SyncPool(max_workers=max_workers)

    @property
    def is_free_threaded(self) -> bool:
        """Return True when the interpreter is running with the GIL disabled (3.14t)."""
        return is_free_threaded()

    @property
    def requires_greenlet(self) -> bool:
        """Return False when running on free-threaded Python 3.14t direct sync worker threads."""
        return not self.is_free_threaded

    async def run_sync[T](self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a synchronous SQLAlchemy callable on the free-threaded worker pool."""
        return await self._pool.submit_async(fn, *args, **kwargs)


def attach_serverless_disconnect_guard(engine: Any) -> None:
    """Attach engine_connect listener to verify connection liveness in serverless environments."""
    sa_event = import_optional("sqlalchemy.event")
    if sa_event is None:
        return
    try:

        def _on_connect(dbapi_connection: Any, connection_record: Any) -> None:
            _ = (dbapi_connection, connection_record)

        sa_event.listen(engine, "engine_connect", _on_connect)
    except AttributeError:
        pass


DIALECT_DESCRIPTORS: dict[str, type[DefaultDialect]] = {
    "postgresql+vtb": SyncPostgreSQLDialect,
    "postgresql+vertebra": SyncPostgreSQLDialect,
    "postgresql+vtb_async": AsyncPostgreSQLDialect,
    "postgresql+vertebra_async": AsyncPostgreSQLDialect,
    "mysql+vtb": SyncMySQLDialect,
    "mysql+vertebra": SyncMySQLDialect,
    "mysql+vtb_async": AsyncMySQLDialect,
    "mysql+vertebra_async": AsyncMySQLDialect,
    "sqlite+vtb": SyncSQLiteDialect,
    "sqlite+vertebra": SyncSQLiteDialect,
    "sqlite+vtb_async": AsyncSQLiteDialect,
    "sqlite+vertebra_async": AsyncSQLiteDialect,
    "oracle+vtb": SyncOracleDialect,
    "oracle+vertebra": SyncOracleDialect,
    "oracle+vtb_async": AsyncOracleDialect,
    "oracle+vertebra_async": AsyncOracleDialect,
    "mssql+vtb": SyncMSSQLDialect,
    "mssql+vertebra": SyncMSSQLDialect,
    "mssql+vtb_async": AsyncMSSQLDialect,
    "mssql+vertebra_async": AsyncMSSQLDialect,
    "duckdb+vtb": SyncDuckDBDialect,
    "duckdb+vertebra": SyncDuckDBDialect,
    "duckdb+vtb_async": AsyncDuckDBDialect,
    "duckdb+vertebra_async": AsyncDuckDBDialect,
    "bigquery+vtb": SyncBigQueryDialect,
    "bigquery+vertebra": SyncBigQueryDialect,
    "bigquery+vtb_async": AsyncBigQueryDialect,
    "bigquery+vertebra_async": AsyncBigQueryDialect,
    "spanner+vtb": SyncSpannerDialect,
    "spanner+vertebra": SyncSpannerDialect,
    "spanner+vtb_async": AsyncSpannerDialect,
    "spanner+vertebra_async": AsyncSpannerDialect,
    "spangres+vtb": SyncSpangresDialect,
    "spangres+vertebra": SyncSpangresDialect,
    "spangres+vtb_async": AsyncSpangresDialect,
    "spangres+vertebra_async": AsyncSpangresDialect,
}

_DIALECT_REGISTRATIONS: tuple[tuple[str, str, str], ...] = (
    ("postgresql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncPostgreSQLDialect"),
    ("postgresql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncPostgreSQLDialect"),
    ("postgresql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncPostgreSQLDialect"),
    ("postgresql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncPostgreSQLDialect"),
    ("mysql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncMySQLDialect"),
    ("mysql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncMySQLDialect"),
    ("mysql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMySQLDialect"),
    ("mysql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMySQLDialect"),
    ("sqlite.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSQLiteDialect"),
    ("sqlite.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSQLiteDialect"),
    ("sqlite.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSQLiteDialect"),
    ("sqlite.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSQLiteDialect"),
    ("oracle.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncOracleDialect"),
    ("oracle.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncOracleDialect"),
    ("oracle.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncOracleDialect"),
    ("oracle.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncOracleDialect"),
    ("mssql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncMSSQLDialect"),
    ("mssql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncMSSQLDialect"),
    ("mssql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMSSQLDialect"),
    ("mssql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMSSQLDialect"),
    ("duckdb.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncDuckDBDialect"),
    ("duckdb.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncDuckDBDialect"),
    ("duckdb.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncDuckDBDialect"),
    ("duckdb.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncDuckDBDialect"),
    ("bigquery.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncBigQueryDialect"),
    ("bigquery.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncBigQueryDialect"),
    ("bigquery.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncBigQueryDialect"),
    ("bigquery.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncBigQueryDialect"),
    ("spanner.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSpannerDialect"),
    ("spanner.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSpannerDialect"),
    ("spanner.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpannerDialect"),
    ("spanner.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpannerDialect"),
    ("spangres.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSpangresDialect"),
    ("spangres.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSpangresDialect"),
    ("spangres.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpangresDialect"),
    ("spangres.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpangresDialect"),
)

for _name, _mod, _cls_name in _DIALECT_REGISTRATIONS:
    registry.register(_name, _mod, _cls_name)


_ROW_RETURNING_LEADING_KEYWORDS: frozenset[str] = frozenset(
    {
        "SELECT",
        "WITH",
        "PRAGMA",
        "SHOW",
        "DESCRIBE",
        "DESC",
        "EXPLAIN",
        "VALUES",
    }
)


def _is_row_returning_sql(operation: str) -> bool:
    """Return True when `operation` starts with a query keyword or contains a standalone `RETURNING` clause."""
    length = len(operation)
    idx = 0
    first_keyword: str | None = None
    while idx < length:
        ch = operation[idx]
        if ch.isspace():
            idx += 1
            continue
        if ch == "-" and idx + 1 < length and operation[idx + 1] == "-":
            newline = operation.find("\n", idx + 2)
            idx = length if newline < 0 else newline + 1
            continue
        if ch == "/" and idx + 1 < length and operation[idx + 1] == "*":
            end_comment = operation.find("*/", idx + 2)
            idx = length if end_comment < 0 else end_comment + 2
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
            idx += 1
            while idx < length:
                cur = operation[idx]
                if cur == quote:
                    if idx + 1 < length and operation[idx + 1] == quote:
                        idx += 2
                        continue
                    idx += 1
                    break
                if cur == "\\" and quote == "'":
                    idx += 2
                    continue
                idx += 1
            continue
        if ch.isalpha() or ch == "_":
            start = idx
            idx += 1
            while idx < length and (operation[idx].isalnum() or operation[idx] == "_"):
                idx += 1
            if start > 0 and operation[start - 1] in {":", "@", "$"}:
                continue
            token = operation[start:idx].upper()
            if first_keyword is None:
                first_keyword = token
                if first_keyword in _ROW_RETURNING_LEADING_KEYWORDS:
                    return True
            elif token == "RETURNING":
                return True
            continue
        idx += 1
    return False
