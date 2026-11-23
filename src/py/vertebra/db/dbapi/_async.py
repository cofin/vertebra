"""Standalone asynchronous PEP 249 DB-API 2.0 connection and module."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, cast

from vertebra.db.dbapi._sync import (
    InterfaceError,
    _BaseDBAPIConnection,
    _BaseDBAPIModule,
    _connect_error,
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


class AsyncDBAPIConnection(_BaseDBAPIConnection):
    """Standalone asynchronous PEP 249 connection backed by Vertebra async drivers."""

    __slots__ = ("cursor",)

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
        self._init_connection(
            url,
            driver=driver,
            broker=broker,
            share_pool=share_pool,
            lease_managed=lease_managed,
            autocommit=autocommit,
            connect_kwargs=kwargs,
            driver_builder=_build_async_driver,
        )

    @property
    def driver(self) -> AsyncSQLDriverProtocol | None:
        """Return the active asynchronous driver."""
        return cast("AsyncSQLDriverProtocol | None", self._driver)

    def __getattr__(self, name: str) -> Any:
        if self._driver is not None and hasattr(self._driver, name):
            return getattr(self._driver, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

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


class AsyncDBAPIModule(_BaseDBAPIModule):
    """Standalone asynchronous DBAPI module adapter returning AsyncDBAPIConnection instances."""

    def connect(self, *args: Any, **kwargs: Any) -> AsyncDBAPIConnection:
        """Open a standalone AsyncDBAPIConnection instance with a required URL."""
        return AsyncDBAPIConnection(self._extract_connect_url(args, kwargs), **kwargs)


async def async_connect(url: str | BaseDatabaseConfig | None = None, **kwargs: Any) -> AsyncDBAPIConnection:
    """Open a standalone asynchronous PEP 249 DB-API connection."""
    if not isinstance(url, BaseDatabaseConfig) and (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return AsyncDBAPIConnection(url, **kwargs)
