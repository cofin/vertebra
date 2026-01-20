"""Shared SQLSpec broker helpers."""

from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from vertebra.drivers.broker import (
    VertebraAsyncConnection,
    VertebraAsyncPool,
    VertebraSyncConnection,
    VertebraSyncPool,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig

_T_co = TypeVar("_T_co", covariant=True)


class SyncSessionContextProtocol(Protocol[_T_co]):
    def __enter__(self) -> _T_co: ...

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


class AsyncSessionContextProtocol(Protocol[_T_co]):
    async def __aenter__(self) -> _T_co: ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


class VertebraSyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "VertebraSyncPool") -> None:
        self._pool = pool
        self._connection: "VertebraSyncConnection | None" = None

    def __enter__(self) -> "VertebraSyncConnection":
        self._connection = self._pool.acquire()
        return self._connection

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            self._pool.release(self._connection)
            self._connection = None


class VertebraAsyncConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "VertebraAsyncPool") -> None:
        self._pool = pool
        self._connection: "VertebraAsyncConnection | None" = None

    async def __aenter__(self) -> "VertebraAsyncConnection":
        self._connection = await self._pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._connection is not None:
            await self._pool.release(self._connection)
            self._connection = None


class VertebraSyncSessionContext:
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
        acquire_connection: "Callable[[], VertebraSyncConnection]",
        release_connection: "Callable[[VertebraSyncConnection], None]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[Any], Any]",
        driver_type: "type[Any]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "VertebraSyncConnection | None" = None
        self._driver: Any | None = None

    def __enter__(self) -> Any:
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


class VertebraAsyncSessionContext:
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
        acquire_connection: "Callable[[], Awaitable[VertebraAsyncConnection]]",
        release_connection: "Callable[[VertebraAsyncConnection], Awaitable[None]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[Any], Any]",
        driver_type: "type[Any]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_type = driver_type
        self._connection: "VertebraAsyncConnection | None" = None
        self._driver: Any | None = None

    async def __aenter__(self) -> Any:
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
    "AsyncSessionContextProtocol",
    "SyncSessionContextProtocol",
    "VertebraAsyncConnection",
    "VertebraAsyncConnectionContext",
    "VertebraAsyncPool",
    "VertebraAsyncSessionContext",
    "VertebraSyncConnection",
    "VertebraSyncConnectionContext",
    "VertebraSyncPool",
    "VertebraSyncSessionContext",
)
