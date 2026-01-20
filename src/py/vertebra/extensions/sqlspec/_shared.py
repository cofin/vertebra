"""Shared SQLSpec broker helpers."""

from types import TracebackType
from typing import TYPE_CHECKING, Protocol, TypeVar

from vertebra.drivers.broker import AsyncBrokerConnection, AsyncBrokerPool, SyncBrokerConnection, SyncBrokerPool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig

_T = TypeVar("_T")


class SyncSessionContextProtocol(Protocol[_T]):
    def __enter__(self) -> _T: ...

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


class AsyncSessionContextProtocol(Protocol[_T]):
    async def __aenter__(self) -> _T: ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


class BrokerSyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "SyncBrokerPool") -> None:
        self._pool = pool
        self._connection: "SyncBrokerConnection | None" = None

    def __enter__(self) -> "SyncBrokerConnection":
        self._connection = self._pool.acquire()
        return self._connection

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            self._pool.release(self._connection)
            self._connection = None


class BrokerAsyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "AsyncBrokerPool") -> None:
        self._pool = pool
        self._connection: "AsyncBrokerConnection | None" = None

    async def __aenter__(self) -> "AsyncBrokerConnection":
        self._connection = await self._pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            await self._pool.release(self._connection)
            self._connection = None


class BrokerSyncSessionContext:
    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_driver_type",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        *,
        acquire_connection: "Callable[[], SyncBrokerConnection]",
        release_connection: "Callable[[SyncBrokerConnection], None]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[object], object]",
        driver_type: "type[object]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "SyncBrokerConnection | None" = None
        self._driver: object | None = None

    def __enter__(self) -> object:
        self._connection = self._acquire_connection()
        self._driver = self._driver_type(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None


class BrokerAsyncSessionContext:
    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_driver_type",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        *,
        acquire_connection: "Callable[[], Awaitable[AsyncBrokerConnection]]",
        release_connection: "Callable[[AsyncBrokerConnection], Awaitable[None]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[object], object]",
        driver_type: "type[object]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "AsyncBrokerConnection | None" = None
        self._driver: object | None = None

    async def __aenter__(self) -> object:
        self._connection = await self._acquire_connection()
        self._driver = self._driver_type(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None


__all__ = (
    "AsyncBrokerConnection",
    "AsyncBrokerPool",
    "AsyncSessionContextProtocol",
    "BrokerAsyncConnectionContext",
    "BrokerAsyncSessionContext",
    "BrokerSyncConnectionContext",
    "BrokerSyncSessionContext",
    "SyncBrokerConnection",
    "SyncBrokerPool",
    "SyncSessionContextProtocol",
)
