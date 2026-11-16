"""Standalone asynchronous PEP 249 DB-API 2.0 connection, cursor, and module."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from vertebra.db.dbapi._sync import (
    Binary,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    _translate_errors,
    coerce_dbapi_rows_and_desc,
    is_insert_sql,
    is_read_only_sql,
    is_row_returning_sql,
)
from vertebra.db.registry import DatabaseRegistry

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "AsyncDBAPIConnection",
    "AsyncDBAPICursor",
    "AsyncDBAPIModule",
    "async_connect",
)


class AsyncDBAPICursor:
    """Asynchronous cursor wrapping Vertebra async drivers."""

    __slots__ = (
        "_closed",
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
        self._closed: bool = False
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
        self,
        operation: str,
        parameters: Sequence[Any] | dict[str, Any] | None = None,
    ) -> AsyncDBAPICursor:
        """Execute a SQL statement asynchronously."""
        self._require_open()
        self.lastrowid = None
        params: Sequence[Any] | dict[str, Any] = () if parameters is None else parameters
        await self._conn.begin()
        is_row_ret = is_row_returning_sql(operation)
        with _translate_errors():
            if not is_row_ret and self._driver is not None and hasattr(self._driver, "execute_dml"):
                affected, captured_rowid = await self._driver.execute_dml(operation, params)
                self._rows = []
                self.description = None
                self.rowcount = int(affected)
                self.lastrowid = (
                    captured_rowid if captured_rowid is not None else await self._resolve_driver_lastrowid(operation)
                )
            elif not is_row_ret and self._driver is not None and hasattr(self._driver, "execute"):
                affected = await self._driver.execute(operation, params)
                self._rows = []
                self.description = None
                self.rowcount = int(affected)
                self.lastrowid = await self._resolve_driver_lastrowid(operation)
            elif self._driver is not None and hasattr(self._driver, "execute_with_description"):
                raw_res = await self._driver.execute_with_description(operation, params)
                self._rows, self.description = coerce_dbapi_rows_and_desc(raw_res)
                self.rowcount = len(self._rows)
                self.lastrowid = await self._resolve_driver_lastrowid(operation)
            elif self._driver is not None and hasattr(self._driver, "execute_tuples"):
                self._rows = await self._driver.execute_tuples(operation, params)
                self.description = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._rows[0])))
                    if self._rows
                    else None
                )
                self.rowcount = len(self._rows)
                self.lastrowid = await self._resolve_driver_lastrowid(operation)
            elif self._driver is not None and hasattr(self._driver, "select"):
                select_params = params if params else None
                dict_rows = await self._driver.select(operation, select_params)
                if dict_rows:
                    keys = tuple(dict_rows[0].keys())
                    self.description = tuple((str(k), None, None, None, None, None, None) for k in keys)
                    self._rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
                else:
                    self._rows = []
                    self.description = None
                self.rowcount = len(self._rows)
                self.lastrowid = await self._resolve_driver_lastrowid(operation)
            else:
                self._rows = []
                self.description = None
                self.rowcount = -1
        self._pos = 0
        if self._conn.autocommit and (self._conn._in_transaction or not is_read_only_sql(operation)):
            await self._conn.commit()
        return self

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> AsyncDBAPICursor:
        """Execute a parameterized SQL statement across parameter sets asynchronously."""
        self._require_open()
        self.lastrowid = None
        if not seq_of_parameters:
            self.rowcount = 0
            return self
        await self._conn.begin()
        is_row_ret = is_row_returning_sql(operation)
        with _translate_errors():
            if not is_row_ret and self._driver is not None and hasattr(self._driver, "execute_many"):
                affected = await self._driver.execute_many(operation, seq_of_parameters)
                self._rows = []
                self.description = None
                self._pos = 0
                self.rowcount = int(affected)
            elif not is_row_ret:
                total_affected = 0
                for params in seq_of_parameters:
                    if self._driver is not None and hasattr(self._driver, "execute"):
                        total_affected += int(await self._driver.execute(operation, params))
                    elif self._driver is not None and hasattr(self._driver, "execute_dml"):
                        aff, _ = await self._driver.execute_dml(operation, params)
                        total_affected += int(aff)
                self._rows = []
                self.description = None
                self._pos = 0
                self.rowcount = total_affected
            else:
                total = 0
                last_rows: list[tuple[Any, ...]] = []
                last_desc: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
                for params in seq_of_parameters:
                    if hasattr(self._driver, "execute_with_description"):
                        res = await self._driver.execute_with_description(operation, params)
                        rows, desc = coerce_dbapi_rows_and_desc(res)
                    elif hasattr(self._driver, "select"):
                        dict_rows = await self._driver.select(operation, params)
                        if dict_rows:
                            keys = tuple(dict_rows[0].keys())
                            desc = tuple((str(k), None, None, None, None, None, None) for k in keys)
                            rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
                        else:
                            rows, desc = [], None
                    else:
                        rows, desc = [], None
                    total += len(rows)
                    last_rows = rows
                    last_desc = desc
                self._rows = last_rows
                self.description = last_desc
                self._pos = 0
                self.rowcount = total
        if self._conn.autocommit and (self._conn._in_transaction or not is_read_only_sql(operation)):
            await self._conn.commit()
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next plain-tuple row asynchronously."""
        self._require_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to size plain-tuple rows asynchronously."""
        self._require_open()
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows asynchronously."""
        self._require_open()
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def setinputsizes(self, sizes: Any) -> None:
        """Accept PEP 249 input buffer size hints as a no-op."""
        self._require_open()
        _ = sizes

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Accept PEP 249 output column buffer size hints as a no-op."""
        self._require_open()
        _ = (size, column)

    async def close(self) -> None:
        """Close the asynchronous cursor."""
        self._rows = []
        self._pos = 0
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor.")
        if self._conn.closed:
            raise InterfaceError("Cannot operate on a closed connection.")

    async def _resolve_driver_lastrowid(self, operation: str) -> int | None:
        if not is_insert_sql(operation):
            return None
        driver = self._conn.driver
        if driver is None:
            return None
        direct = getattr(driver, "lastrowid", None)
        if isinstance(direct, int) and not isinstance(direct, bool):
            return direct
        last_rowid_fn = getattr(driver, "last_insert_rowid", None)
        if callable(last_rowid_fn):
            val = last_rowid_fn()
            if inspect.isawaitable(val):
                val = await val
            return int(val) if val is not None else None
        return None


class AsyncDBAPIConnection:
    """Standalone asynchronous PEP 249 connection backed by Vertebra async drivers."""

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
        url: str | None = None,
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous PEP 249 connection with a non-empty database URL."""
        _ = kwargs

        if url is None and driver is None:
            exc = ValueError("Unsupported database URL scheme: ''")
            raise InterfaceError(f"Cannot parse Vertebra database URL: {exc}") from exc

        resolved_url = str(url) if url is not None else getattr(driver, "url", getattr(driver, "dsn", ""))
        self._url = resolved_url
        self._share_pool = bool(share_pool)
        self._broker = (
            broker
            if broker is not None
            else (DatabaseRegistry.default_broker() if self._share_pool and not lease_managed else None)
        )
        self._in_transaction = False
        self.closed: bool = False

        self._config: Any = None
        driver_broker = None if lease_managed else self._broker
        if resolved_url or driver is None:
            try:
                self._config = (
                    DatabaseRegistry.parse_url(resolved_url, broker=driver_broker)
                    if driver_broker is not None
                    else DatabaseRegistry.parse_url(resolved_url)
                )
            except ValueError as exc:
                msg = f"Cannot parse Vertebra database URL: {exc}"
                raise InterfaceError(msg) from exc

        self._autocommit = bool(getattr(self._config, "requires_statement_autocommit", False))
        self._driver = driver
        self._owns_driver = driver is not None
        if self._driver is None:
            has_shared_key = self._config is not None and self._config.pool_key() is not None
            has_custom_broker = broker is not None and broker is not DatabaseRegistry._DEFAULT_BROKER
            if self._share_pool and not lease_managed and has_shared_key and not has_custom_broker:
                assert self._config is not None
                self._driver = DatabaseRegistry.default().get_shared_driver(self._config)
            else:
                target_registry = DatabaseRegistry(broker=None if lease_managed else self._broker)
                self._driver = self._config.build_async_driver(target_registry)
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
        """Return the active asynchronous driver."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for autocommit-only backends."""
        if not value and getattr(self._config, "requires_statement_autocommit", False):
            config_name = type(self._config).__name__ if self._config is not None else "Backend"
            msg = f"{config_name} DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        self._autocommit = value

    def __getattr__(self, name: str) -> Any:
        if self._driver is not None and hasattr(self._driver, name):
            return getattr(self._driver, name)
        msg = f"{type(self).__name__!r} object has no attribute {name!r}"
        raise AttributeError(msg)

    def cursor(self, *args: Any, **kwargs: Any) -> AsyncDBAPICursor:
        """Create a new asynchronous DBAPI cursor."""
        self._require_open()
        _ = (args, kwargs)
        return AsyncDBAPICursor(self)

    async def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        self._require_open()
        if self._autocommit:
            if self._in_transaction:
                await self.commit()
            return
        if not self._in_transaction and self._driver is not None and hasattr(self._driver, "begin"):
            with _translate_errors():
                await self._driver.begin()
                self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active asynchronous transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "commit"):
                with _translate_errors():
                    await self._driver.commit()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active asynchronous transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "rollback"):
                with _translate_errors():
                    await self._driver.rollback()
        finally:
            self._in_transaction = False

    async def close(self) -> None:
        """Close the asynchronous driver connection."""
        if not self.closed:
            try:
                if self._in_transaction:
                    if self._autocommit:
                        await self.commit()
                    else:
                        await self.rollback()
            finally:
                self._in_transaction = False
                if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                    with _translate_errors():
                        await self._driver.close()
                self.closed = True

    def _require_open(self) -> None:
        if self.closed:
            raise InterfaceError("Cannot operate on a closed connection.")


class AsyncDBAPIModule:
    """Standalone asynchronous DBAPI module adapter returning AsyncDBAPIConnection instances."""

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

    sqlite_version: str = "3.45.0"
    sqlite_version_info: tuple[int, ...] = (3, 45, 0)

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        """Initialize asynchronous DBAPI module."""
        self.paramstyle = paramstyle

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        """Open a standalone AsyncDBAPIConnection instance with a required URL."""
        url_arg: str | None = None
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        if (not url_arg or not url_arg.strip()) and "driver" not in kwargs:
            raise InterfaceError("A non-empty database connection URL is required.")
        return AsyncDBAPIConnection(url_arg, **kwargs)


async def async_connect(url: str | None = None, **kwargs: Any) -> AsyncDBAPIConnection:
    """Open a standalone asynchronous PEP 249 DB-API connection."""
    if (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return AsyncDBAPIConnection(url, **kwargs)
