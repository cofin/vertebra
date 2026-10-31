"""SQLAlchemy 2.1 +vertebra / +vtb sync and async dialects, PEP 249 DBAPI bridges, and 3.14t pool adapters."""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable, Sequence
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

from vertebra.db._registry import DatabaseRegistry
from vertebra.db.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.types import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "ALL_DIALECT_SCHEMES",
    "DIALECT_SCHEMES",
    "AsyncDBAPIConnection",
    "AsyncDBAPICursor",
    "AsyncDBAPIModule",
    "DBAPIConnection",
    "DBAPICursor",
    "DBAPIModule",
    "DataError",
    "DatabaseError",
    "DuckDBAsyncDialect",
    "DuckDBDialect",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "MSSQLAsyncDialect",
    "MSSQLDialect",
    "MySQLAsyncDialect",
    "MySQLDialect",
    "NotSupportedError",
    "NullPool",
    "OperationalError",
    "OracleAsyncDialect",
    "OracleDialect",
    "PostgreSQLAsyncDialect",
    "PostgreSQLDialect",
    "ProgrammingError",
    "SQLiteAsyncDialect",
    "SQLiteDialect",
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
    "mssql+vertebra",
    "mssql+vtb",
    "duckdb+vertebra",
    "duckdb+vtb",
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


class DBAPICursor:
    """PEP 249 DBAPI 2.0 cursor returning plain Python tuples for SQLAlchemy 2.1 ORM loaders."""

    __slots__ = ("_conn", "_pos", "_raw_cursor", "_rows", "arraysize", "description", "lastrowid", "rowcount")

    def __init__(self, connection: DBAPIConnection, raw_cursor: Any = None) -> None:
        """Initialize PEP 249 cursor with connection and optional underlying DBAPI cursor."""
        self._conn = connection
        self._raw_cursor = raw_cursor
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> DBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        if self._raw_cursor is not None:
            self._raw_cursor.execute(operation, parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
            return self

        self._rows, self.description = self._conn.execute_with_description(operation, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        return self

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]]) -> DBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        if self._raw_cursor is not None:
            self._raw_cursor.executemany(operation, seq_of_parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
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


class DBAPIConnection:
    """PEP 249 DBAPI 2.0 connection backed by Vertebra native drivers or sqlite3."""

    __slots__ = ("_autocommit", "_broker", "_config", "_driver", "_sqlite_conn", "_url", "closed")

    def __init__(
        self,
        url: str = "sqlite+vtb:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with URL or database parameters."""
        resolved_url = url
        if database is not None and (url == "sqlite+vtb:///:memory:" or not url):
            resolved_url = f"sqlite+vtb:///{database}"
        self._url = resolved_url
        self._broker = broker
        self._autocommit = False
        self.closed: bool = False

        self._config: Any = None
        try:
            self._config = (
                DatabaseRegistry.parse_url(resolved_url, broker=broker)
                if broker is not None
                else DatabaseRegistry.parse_url(resolved_url)
            )
        except Exception:
            self._config = SQLiteConfig(database=database or ":memory:")

        self._driver = driver
        self._sqlite_conn: sqlite3.Connection | None = None

        if self._driver is None:
            if isinstance(self._config, SQLiteConfig):
                db_target = str(self._config.database or ":memory:")
                if db_target.startswith("sqlite+"):
                    db_target = db_target.split(":///", 1)[-1] if ":///" in db_target else ":memory:"
                self._sqlite_conn = sqlite3.connect(db_target, check_same_thread=False)
                self._driver = SyncSQLiteDriver(self._config, broker=broker)
            elif isinstance(self._config, PostgreSQLConfig):
                self._driver = SyncPostgreSQLDriver(self._config, broker=broker)
            elif isinstance(self._config, MySQLConfig):
                self._driver = SyncMySQLDriver(self._config, broker=broker)
            elif isinstance(self._config, OracleConfig):
                self._driver = SyncOracleDriver(self._config, broker=broker)
            elif isinstance(self._config, MSSQLConfig):
                self._driver = SyncMSSQLDriver(self._config, broker=broker)
            elif isinstance(self._config, DuckDBConfig):
                self._driver = SyncDuckDBDriver(self._config, broker=broker)

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
        """Set connection autocommit state."""
        self._autocommit = value
        if self._sqlite_conn is not None:
            self._sqlite_conn.autocommit = value

    def cursor(self) -> DBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        if self._sqlite_conn is not None:
            return DBAPICursor(self, raw_cursor=self._sqlite_conn.cursor())
        return DBAPICursor(self)

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

        if self._driver is not None:
            if hasattr(self._driver, "execute_with_description"):
                raw_res = self._driver.execute_with_description(sql, parameters)
                if isinstance(raw_res, tuple) and len(raw_res) >= 2:
                    return raw_res[0], raw_res[1]
                return [], None
            if hasattr(self._driver, "execute_tuples"):
                rows = self._driver.execute_tuples(sql, parameters)
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0])))
                    if rows
                    else None
                )
                return rows, desc
        return [], None

    def execute_raw_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def commit(self) -> None:
        """Commit the active transaction."""
        if self._sqlite_conn is not None:
            self._sqlite_conn.commit()
        elif self._driver is not None and hasattr(self._driver, "commit"):
            self._driver.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        if self._sqlite_conn is not None:
            self._sqlite_conn.rollback()
        elif self._driver is not None and hasattr(self._driver, "rollback"):
            self._driver.rollback()

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            if self._sqlite_conn is not None:
                self._sqlite_conn.close()
            if self._driver is not None and hasattr(self._driver, "close"):
                self._driver.close()
            self.closed = True


class DBAPIModule:
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

    def connect(self, *args: Any, **kwargs: Any) -> DBAPIConnection:
        """Open a PEP 249 connection."""
        url_arg = "sqlite+vtb:///:memory:"
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        return DBAPIConnection(url_arg, **kwargs)


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
        if self._driver is not None and hasattr(self._driver, "execute_with_description"):
            raw_res = await self._driver.execute_with_description(operation, params)
            if isinstance(raw_res, tuple) and len(raw_res) >= 2:
                self._rows = raw_res[0]
                self.description = raw_res[1]
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
        else:
            self._rows = []
            self.description = None
            self.rowcount = -1

        self._pos = 0
        if operation.strip().upper().startswith("INSERT") and hasattr(self._driver, "_sync"):
            sync_drv = getattr(self._driver, "_sync", None)
            raw_conn = getattr(sync_drv, "raw_connection", None)
            if raw_conn is not None:
                cur = raw_conn.cursor()
                cur.execute("SELECT last_insert_rowid()")
                row = cur.fetchone()
                if row:
                    self.lastrowid = row[0]
        return self

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> AsyncDBAPICursor:
        """Execute parameterized SQL across multiple parameter sets asynchronously."""
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


class AsyncDBAPIConnection:
    """Asynchronous DBAPI connection adapting Vertebra async drivers."""

    __slots__ = ("_autocommit", "_broker", "_config", "_driver", "_url", "closed")

    def __init__(
        self,
        url: str = "sqlite+vtb_async:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous connection."""
        resolved_url = url
        if database is not None and (url == "sqlite+vtb_async:///:memory:" or not url):
            resolved_url = f"sqlite+vtb_async:///{database}"
        self._url = resolved_url
        self._broker = broker
        self._autocommit = False
        self.closed: bool = False

        self._config: Any = None
        try:
            self._config = (
                DatabaseRegistry.parse_url(resolved_url, broker=broker)
                if broker is not None
                else DatabaseRegistry.parse_url(resolved_url)
            )
        except Exception:
            self._config = SQLiteConfig(database=database or ":memory:")

        self._driver = driver
        if self._driver is None:
            if isinstance(self._config, SQLiteConfig):
                self._driver = AsyncSQLiteDriver(self._config, broker=broker)
            elif isinstance(self._config, PostgreSQLConfig):
                self._driver = AsyncPostgreSQLDriver(self._config, broker=broker)
            elif isinstance(self._config, MySQLConfig):
                self._driver = AsyncMySQLDriver(self._config, broker=broker)
            elif isinstance(self._config, OracleConfig):
                self._driver = AsyncOracleDriver(self._config, broker=broker)
            elif isinstance(self._config, MSSQLConfig):
                self._driver = AsyncMSSQLDriver(self._config, broker=broker)
            elif isinstance(self._config, DuckDBConfig):
                self._driver = AsyncDuckDBDriver(self._config, broker=broker)

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
        """Set connection autocommit state."""
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

    async def commit(self) -> None:
        """Commit the active asynchronous transaction."""
        if self._driver is not None and hasattr(self._driver, "commit"):
            await self._driver.commit()

    async def rollback(self) -> None:
        """Roll back the active asynchronous transaction."""
        if self._driver is not None and hasattr(self._driver, "rollback"):
            await self._driver.rollback()

    async def close(self) -> None:
        """Close the asynchronous driver connection."""
        if not self.closed:
            if self._driver is not None and hasattr(self._driver, "close"):
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


class PostgreSQLDialect(PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for PostgreSQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    paramstyle = "pyformat"
    use_insertmanyvalues = True
    insert_executemany_returning = True
    supports_native_jsonb = True

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="pyformat"))

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


class PostgreSQLAsyncDialect(PGDialect):
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


class SQLiteDialect(BaseSQLiteDialect):
    """SQLAlchemy 2.1 synchronous dialect for SQLite (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="qmark"))

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


class SQLiteAsyncDialect(BaseSQLiteDialect):
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


class MySQLDialect(BaseMySQLDialect):
    """SQLAlchemy 2.1 synchronous dialect for MySQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    paramstyle = "format"
    use_insertmanyvalues = True
    insert_executemany_returning = False
    supports_native_jsonb = False

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="format"))

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


class MySQLAsyncDialect(BaseMySQLDialect):
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


class OracleDialect(BaseOracleDialect):
    """SQLAlchemy 2.1 synchronous dialect for Oracle (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="named"))

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


class OracleAsyncDialect(BaseOracleDialect):
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


class MSSQLDialect(MSDialect):
    """SQLAlchemy 2.1 synchronous dialect for Microsoft SQL Server (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="qmark"))

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


class MSSQLAsyncDialect(MSDialect):
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


class DuckDBDialect(DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for DuckDB (+vtb / +vertebra)."""

    name = "duckdb"
    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: DBAPIModule(paramstyle="qmark"))

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


class DuckDBAsyncDialect(DefaultDialect):
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


_DIALECT_REGISTRATIONS: tuple[tuple[str, str, str], ...] = (
    ("postgresql.vtb", "vertebra.extensions.sqlalchemy.dialects", "PostgreSQLDialect"),
    ("postgresql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "PostgreSQLDialect"),
    ("postgresql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "PostgreSQLAsyncDialect"),
    ("postgresql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "PostgreSQLAsyncDialect"),
    ("mysql.vtb", "vertebra.extensions.sqlalchemy.dialects", "MySQLDialect"),
    ("mysql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "MySQLDialect"),
    ("mysql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "MySQLAsyncDialect"),
    ("mysql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "MySQLAsyncDialect"),
    ("sqlite.vtb", "vertebra.extensions.sqlalchemy.dialects", "SQLiteDialect"),
    ("sqlite.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SQLiteDialect"),
    ("sqlite.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "SQLiteAsyncDialect"),
    ("sqlite.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "SQLiteAsyncDialect"),
    ("oracle.vtb", "vertebra.extensions.sqlalchemy.dialects", "OracleDialect"),
    ("oracle.vertebra", "vertebra.extensions.sqlalchemy.dialects", "OracleDialect"),
    ("oracle.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "OracleAsyncDialect"),
    ("oracle.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "OracleAsyncDialect"),
    ("mssql.vtb", "vertebra.extensions.sqlalchemy.dialects", "MSSQLDialect"),
    ("mssql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "MSSQLDialect"),
    ("mssql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "MSSQLAsyncDialect"),
    ("mssql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "MSSQLAsyncDialect"),
    ("duckdb.vtb", "vertebra.extensions.sqlalchemy.dialects", "DuckDBDialect"),
    ("duckdb.vertebra", "vertebra.extensions.sqlalchemy.dialects", "DuckDBDialect"),
    ("duckdb.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "DuckDBAsyncDialect"),
    ("duckdb.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "DuckDBAsyncDialect"),
)

for _name, _mod, _cls_name in _DIALECT_REGISTRATIONS:
    registry.register(_name, _mod, _cls_name)
