"""Shared SQLSpec broker helpers."""

import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.drivers.broker import (
    AsyncBrokerConnection,
    AsyncBrokerPool,
    SyncBrokerConnection,
    SyncBrokerPool,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig


class BrokerSyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "SyncBrokerPool") -> None:
        self._pool = pool
        self._connection: "SyncBrokerConnection | None" = None

    def __enter__(self) -> "SyncBrokerConnection":
        self._connection = self._pool.acquire()
        return self._connection

    def __exit__(self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any) -> "bool | None":
        if self._connection is not None:
            self._pool.release(self._connection)
            self._connection = None
        return None


class BrokerAsyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "AsyncBrokerPool") -> None:
        self._pool = pool
        self._connection: "AsyncBrokerConnection | None" = None

    async def __aenter__(self) -> "AsyncBrokerConnection":
        self._connection = await self._pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._pool.release(self._connection)
            self._connection = None
        return None


class BrokerSyncSessionContext:
    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
        "_driver_type",
    )

    def __init__(
        self,
        *,
        acquire_connection: "Callable[[], SyncBrokerConnection]",
        release_connection: "Callable[[SyncBrokerConnection], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[Any], Any]",
        driver_type: "type[Any]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "SyncBrokerConnection | None" = None
        self._driver: Any = None

    def __enter__(self) -> Any:
        self._connection = self._acquire_connection()
        self._driver = self._driver_type(
            connection=self._connection,
            statement_config=self._statement_config,
            driver_features=self._driver_features,
        )
        return self._prepare_driver(self._driver)

    def __exit__(self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class BrokerAsyncSessionContext:
    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
        "_driver_type",
    )

    def __init__(
        self,
        *,
        acquire_connection: "Callable[[], Awaitable[AsyncBrokerConnection]]",
        release_connection: "Callable[[AsyncBrokerConnection], Awaitable[Any]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[Any], Any]",
        driver_type: "type[Any]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "AsyncBrokerConnection | None" = None
        self._driver: Any = None

    async def __aenter__(self) -> Any:
        self._connection = await self._acquire_connection()
        self._driver = self._driver_type(
            connection=self._connection,
            statement_config=self._statement_config,
            driver_features=self._driver_features,
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "AsyncBrokerConnection",
    "AsyncBrokerPool",
    "BrokerAsyncConnectionContext",
    "BrokerAsyncSessionContext",
    "BrokerSyncConnectionContext",
    "BrokerSyncSessionContext",
    "SyncBrokerConnection",
    "SyncBrokerPool",
)
