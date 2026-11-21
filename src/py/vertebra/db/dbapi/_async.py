"""Standalone asynchronous PEP 249 DB-API 2.0 connection and module."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any, Protocol, cast

from vertebra.db.dbapi._sync import (
    _POOL_ROUTED_DIALECTS,
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
    _config_dialect,
    _connect_error,
    _resolve_dbapi_config,
)
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.db.registry import AnyDatabaseConfig, DatabaseRegistry

__all__ = (
    "AsyncDBAPIConnection",
    "AsyncDBAPIModule",
    "AsyncSQLDriverProtocol",
    "async_connect",
)


class AsyncSQLDriverProtocol(Protocol):
    """Protocol describing asynchronous Vertebra SQL drivers consumed by AsyncDBAPIConnection."""

    dialect: str
    closed: bool

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a statement asynchronously and return affected row count."""
        ...

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a statement asynchronously across parameter sets and return affected row count."""
        ...

    async def begin(self) -> None:
        """Begin a native asynchronous transaction."""
        ...

    async def commit(self) -> None:
        """Commit the active asynchronous transaction."""
        ...

    async def rollback(self) -> None:
        """Roll back the active asynchronous transaction."""
        ...

    async def close(self) -> None:
        """Close the underlying asynchronous driver."""
        ...


def _build_async_driver(
    config: AnyDatabaseConfig | BaseDatabaseConfig,
    *,
    broker: Any,
    share_pool: bool,
    lease_managed: bool,
) -> tuple[AsyncSQLDriverProtocol, bool]:
    """Construct or acquire an asynchronous driver for config inside PEP 249 error translation."""
    try:
        has_shared_key = config.pool_key() is not None
        has_custom_broker = broker is not None and broker is not DatabaseRegistry._DEFAULT_BROKER
        if share_pool and not lease_managed and has_shared_key and not has_custom_broker:
            driver = DatabaseRegistry.default().get_shared_driver(cast("Any", config))
            return cast("AsyncSQLDriverProtocol", driver), False
        target_registry = DatabaseRegistry(broker=None if lease_managed else broker)
        build_fn = getattr(config, "build_async_driver", None)
        if not callable(build_fn):
            msg = f"{type(config).__name__} does not support asynchronous DBAPI connections"
            raise InterfaceError(msg)
        driver = build_fn(target_registry)
        return cast("AsyncSQLDriverProtocol", driver), True
    except Exception as exc:
        mapped = _connect_error(exc)
        if mapped is None:
            raise
        raise mapped from exc


class AsyncDBAPIConnection:
    """Standalone asynchronous PEP 249 connection backed by Vertebra async drivers."""

    __slots__ = (
        "_autocommit",
        "_broker",
        "_config",
        "_driver",
        "_driver_cursor",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_url",
        "closed",
        "cursor",
    )

    def __init__(
        self,
        url: str | BaseDatabaseConfig | None = None,
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        autocommit: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous PEP 249 connection with a non-empty database URL."""
        self._share_pool = bool(share_pool)
        self._broker = (
            broker
            if broker is not None
            else (DatabaseRegistry.default_broker() if self._share_pool and not lease_managed else None)
        )
        self._in_transaction = False
        self.closed: bool = False

        driver_broker = None if lease_managed else self._broker
        resolved_url, resolved_config = _resolve_dbapi_config(
            url,
            driver=driver,
            driver_broker=driver_broker,
            connect_kwargs=kwargs,
        )
        self._url: str = resolved_url
        self._config: AnyDatabaseConfig | BaseDatabaseConfig | None = resolved_config
        self._autocommit: bool = bool(getattr(self._config, "requires_statement_autocommit", False))
        self._driver: AsyncSQLDriverProtocol | None = driver
        self._owns_driver: bool = driver is not None

        if self._driver is None and self._config is not None:
            self._driver, self._owns_driver = _build_async_driver(
                self._config,
                broker=broker,
                share_pool=self._share_pool,
                lease_managed=lease_managed,
            )

        self._driver_cursor: Callable[..., Any] | None = None
        if self._driver is not None:
            factory = getattr(self._driver, "cursor", None)
            if not callable(factory):
                msg = f"{type(self._driver).__name__} does not provide cursor()"
                raise InterfaceError(msg)
            self._driver_cursor = factory
        self.cursor: Callable[..., Any] = self._bound_cursor()

        if autocommit is not None:
            self.autocommit = bool(autocommit)

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> AnyDatabaseConfig | BaseDatabaseConfig | None:
        """Return the parsed database configuration."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached shared-memory database broker, if any."""
        return self._broker

    @property
    def driver(self) -> AsyncSQLDriverProtocol | None:
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
        if not self.closed:
            self.cursor = self._bound_cursor()

    def __getattr__(self, name: str) -> Any:
        if self._driver is not None and hasattr(self._driver, name):
            return getattr(self._driver, name)
        msg = f"{type(self).__name__!r} object has no attribute {name!r}"
        raise AttributeError(msg)

    async def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        self._require_open()
        if self._autocommit:
            if self._in_transaction:
                await self.commit()
            return
        if not self._in_transaction and self._driver is not None and hasattr(self._driver, "begin"):
            await self._driver.begin()
            self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active asynchronous transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "commit"):
                await self._driver.commit()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active asynchronous transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "rollback"):
                await self._driver.rollback()
        finally:
            self._in_transaction = False

    async def close(self) -> None:
        """Close the asynchronous driver connection."""
        if not self.closed:
            try:
                if self._transaction_open():
                    if self._autocommit:
                        await self.commit()
                    else:
                        await self.rollback()
            finally:
                self._in_transaction = False
                if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                    await self._driver.close()
                self.closed = True
                self.cursor = self._closed_cursor

    def _bound_cursor(self) -> Callable[..., Any]:
        """Return the callable stored in `cursor` for the current autocommit state.

        The driver's `cursor` method is stored unchanged when autocommit is on. PostgreSQL, whose driver
        accepts lazy begin only while a session is pinned, gets `_session_routed_cursor`, which decides on
        each call. Every other engine gets the driver method with `lazy_begin=True` bound.
        """
        factory = self._driver_cursor
        if factory is None:
            return self._no_driver_cursor
        if self._autocommit:
            return factory
        if self._engine_key() in _POOL_ROUTED_DIALECTS:
            return self._session_routed_cursor
        return partial(factory, lazy_begin=True)

    def _session_routed_cursor(self) -> Any:
        """Return a lazy-begin cursor when the driver has a pinned session, else a plain cursor.

        Raises:
            InterfaceError: When the driver cannot provide a cursor at all.
        """
        factory = cast("Callable[..., Any]", self._driver_cursor)
        try:
            return factory(lazy_begin=True)
        except InterfaceError:
            return factory(lazy_begin=False)

    def _engine_key(self) -> str:
        """Return the lowercase engine name from the config, or from the driver when no config resolved."""
        if self._config is not None:
            return _config_dialect(self._config)
        return str(getattr(self._driver, "dialect", "")).lower()

    def _no_driver_cursor(self) -> Any:
        """Raise `InterfaceError` for a connection built without a driver."""
        msg = "AsyncDBAPIConnection has no underlying driver cursor factory"
        raise InterfaceError(msg)

    def _closed_cursor(self) -> Any:
        """Raise `InterfaceError` for a cursor request on a closed connection."""
        raise InterfaceError("Cannot operate on a closed connection.")

    def _transaction_open(self) -> bool:
        """Return whether a transaction is open on the underlying driver."""
        if self._driver is not None:
            in_tx = getattr(self._driver, "in_transaction", None)
            if in_tx is not None:
                return bool(in_tx)
        return self._in_transaction

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
        """Initialize asynchronous DBAPI module with optional dialect version metadata."""
        self.paramstyle = paramstyle
        self.dialect = dialect
        _apply_dialect_metadata(self, dialect)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncDBAPIConnection:
        """Open a standalone AsyncDBAPIConnection instance with a required URL."""
        url_arg: str | BaseDatabaseConfig | None = None
        if args and isinstance(args[0], (str, BaseDatabaseConfig)):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        if (
            not isinstance(url_arg, BaseDatabaseConfig)
            and (not url_arg or not str(url_arg).strip())
            and "driver" not in kwargs
        ):
            raise InterfaceError("A non-empty database connection URL is required.")
        return AsyncDBAPIConnection(url_arg, **kwargs)


async def async_connect(url: str | BaseDatabaseConfig | None = None, **kwargs: Any) -> AsyncDBAPIConnection:
    """Open a standalone asynchronous PEP 249 DB-API connection."""
    if not isinstance(url, BaseDatabaseConfig) and (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return AsyncDBAPIConnection(url, **kwargs)
