"""SQLAlchemy asyncio DBAPI adapter wrapping standalone ``vertebra.db.dbapi`` connections."""

from __future__ import annotations

import collections
import inspect
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_module,
    AsyncAdapt_dbapi_ss_cursor,
    AsyncAdapt_terminate,
)
from sqlalchemy.util.concurrency import await_

import vertebra.db.dbapi._sync as dbapi_sync
from vertebra.db.dbapi._async import AsyncDBAPICursor, AsyncDBAPIModule
from vertebra.db.dbapi._sync import (
    BFILE,
    BINARY,
    BLOB,
    CLOB,
    DATETIME,
    FIXED_CHAR,
    NCLOB,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    DatabaseError,
    DataError,
    Date,
    DateFromTicks,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Warning,
    _apply_dialect_metadata,
)

__all__ = (
    "AsyncAdapt_vertebra_connection",
    "AsyncAdapt_vertebra_cursor",
    "AsyncAdapt_vertebra_ss_cursor",
    "SQLAlchemyAsyncDBAPI",
)


class AsyncAdapt_vertebra_cursor(AsyncAdapt_dbapi_cursor):
    """SQLAlchemy asyncio cursor adapting ``AsyncDBAPICursor`` with synchronous ``setinputsizes``."""

    __slots__ = ()

    _awaitable_cursor_close: bool = True

    def setinputsizes(self, *inputsizes: Any, **kwargs: Any) -> None:
        """Forward PEP 249 input size hints synchronously without awaiting."""
        raw_cursor: Any = self._cursor
        try:
            if kwargs:
                raw_cursor.setinputsizes(*inputsizes, **kwargs)
            elif len(inputsizes) == 1:
                raw_cursor.setinputsizes(inputsizes[0])
            elif isinstance(raw_cursor, AsyncDBAPICursor):
                raw_cursor.setinputsizes(inputsizes)
            else:
                raw_cursor.setinputsizes(*inputsizes)
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def var(self, *args: Any, **kwargs: Any) -> Any:
        """Create an output bind variable when supported by the underlying cursor."""
        var_fn = getattr(self._cursor, "var", None)
        if not callable(var_fn):
            msg = f"{type(self._cursor).__name__} does not support cursor.var()"
            raise self._adapt_connection.dbapi.NotSupportedError(msg)
        try:
            return var_fn(*args, **kwargs)
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def nextset(self) -> Any:
        """Advance to the next result set and buffer returned rows."""
        try:
            has_next = await_(self._cursor.nextset())
            if has_next and self._cursor.description and not self.server_side:
                self._rows = collections.deque(await_(self._cursor.fetchall()))
            elif not has_next and not self.server_side:
                self._rows.clear()
            return has_next
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    async def _execute_async(
        self,
        operation: Any,
        parameters: Any,
    ) -> Any:
        async with self._adapt_connection._execute_mutex:
            if parameters is None:
                result = await self._cursor.execute(operation)
            else:
                result = await self._cursor.execute(operation, parameters)
            if not self.server_side:
                if self._cursor.description:
                    self._rows = collections.deque(await self._cursor.fetchall())
                else:
                    self._rows.clear()
            return result

    async def _executemany_async(
        self,
        operation: Any,
        seq_of_parameters: Any,
    ) -> Any:
        async with self._adapt_connection._execute_mutex:
            result = await self._cursor.executemany(operation, seq_of_parameters)
            if not self.server_side:
                if self._cursor.description:
                    self._rows = collections.deque(await self._cursor.fetchall())
                else:
                    self._rows.clear()
            return result


class AsyncAdapt_vertebra_ss_cursor(AsyncAdapt_dbapi_ss_cursor, AsyncAdapt_vertebra_cursor):
    """Server-side streaming cursor adapter backed by ``AsyncDBAPICursor``."""

    __slots__ = ()

    def __iter__(self) -> Iterator[Any]:
        """Yield rows sequentially via async iteration or ``fetchone`` fallback."""
        if hasattr(self._cursor, "__aiter__"):
            yield from super().__iter__()
            return
        while True:
            row = await_(self._cursor.fetchone())
            if row is None:
                break
            yield row


class AsyncAdapt_vertebra_connection(AsyncAdapt_terminate, AsyncAdapt_dbapi_connection):
    """SQLAlchemy asyncio connection adapter supporting termination and autocommit forwarding."""

    __slots__ = ()

    _cursor_cls = AsyncAdapt_vertebra_cursor
    _ss_cursor_cls = AsyncAdapt_vertebra_ss_cursor

    @property
    def autocommit(self) -> bool:
        """Return underlying async DBAPI autocommit state."""
        return bool(self._connection.autocommit)

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Update underlying async DBAPI autocommit state."""
        try:
            self._connection.autocommit = value
        except Exception as error:
            self._handle_exception(error)

    async def _terminate_graceful_close(self) -> None:
        """Gracefully close the underlying async connection inside a greenlet."""
        res = self._connection.close()
        if inspect.isawaitable(res):
            await res

    def _terminate_force_close(self) -> None:
        """Synchronously force-close the underlying driver during GC outside greenlet context."""
        terminate_fn = getattr(self._connection, "terminate", None)
        if callable(terminate_fn):
            terminate_fn()
        else:
            driver = getattr(self._connection, "driver", None)
            driver_terminate = getattr(driver, "terminate", None)
            if callable(driver_terminate):
                driver_terminate()
            else:
                driver_close_sync = getattr(driver, "close_sync", None)
                if callable(driver_close_sync):
                    driver_close_sync()
        if hasattr(self._connection, "closed"):
            self._connection.closed = True


class SQLAlchemyAsyncDBAPI(AsyncAdapt_dbapi_module):
    """SQLAlchemy asyncio DBAPI module adapting ``AsyncDBAPIModule`` via ``AsyncAdapt_vertebra_connection``."""

    apilevel: str = AsyncDBAPIModule.apilevel
    threadsafety: int = AsyncDBAPIModule.threadsafety

    if not TYPE_CHECKING:
        Error = Error
        InterfaceError = InterfaceError
        OperationalError = OperationalError
        IntegrityError = IntegrityError
    Warning = Warning
    DatabaseError = DatabaseError
    DataError = DataError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    Date = staticmethod(Date)
    Time = staticmethod(Time)
    Timestamp = staticmethod(Timestamp)
    DateFromTicks = staticmethod(DateFromTicks)
    TimeFromTicks = staticmethod(TimeFromTicks)
    TimestampFromTicks = staticmethod(TimestampFromTicks)
    Binary = Binary
    STRING = STRING
    BINARY = BINARY
    BLOB = BLOB
    BFILE = BFILE
    CLOB = CLOB
    NCLOB = NCLOB
    FIXED_CHAR = FIXED_CHAR
    NUMBER = NUMBER
    DATETIME = DATETIME
    ROWID = ROWID

    sqlite_version: str
    sqlite_version_info: tuple[int, ...]

    def __init__(self, *, paramstyle: str = "qmark", dialect: str | None = None) -> None:
        """Initialize the asyncio DBAPI module with engine-specific paramstyle and metadata."""
        self._module = AsyncDBAPIModule(paramstyle=paramstyle, dialect=dialect)
        super().__init__(cast("Any", self._module), dbapi_module=dbapi_sync)
        self.paramstyle = paramstyle
        self.dialect = dialect
        _apply_dialect_metadata(self, dialect)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_vertebra_connection:
        """Establish an asynchronous connection wrapped in ``AsyncAdapt_vertebra_connection``."""
        creator_fn = kwargs.pop("async_creator_fn", None)
        if creator_fn is not None:
            try:
                created = creator_fn(*args, **kwargs)
                async_conn = await_(created) if inspect.isawaitable(created) else created
            except Exception as error:
                AsyncAdapt_vertebra_connection._handle_exception_no_connection(self, error)
            return AsyncAdapt_vertebra_connection(self, cast("Any", async_conn))
        async_conn = self._module.connect(*args, **kwargs)
        return AsyncAdapt_vertebra_connection(self, cast("Any", async_conn))
