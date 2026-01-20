"""SQLite broker adapter type definitions."""

from types import TracebackType
from typing import TYPE_CHECKING

from vertebra.drivers.broker import VertebraAsyncConnection, VertebraSyncConnection

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig

    from vertebra.extensions.sqlspec.sqlite.driver import VertebraSqliteAsyncDriver, VertebraSqliteSyncDriver

    type VertebraSqliteSyncConnection = VertebraSyncConnection
    type VertebraSqliteAsyncConnection = VertebraAsyncConnection
else:
    VertebraSqliteSyncConnection = VertebraSyncConnection
    VertebraSqliteAsyncConnection = VertebraAsyncConnection


class VertebraSqliteSyncSessionContext:
    """Sync session context manager for broker-backed SQLite."""

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], VertebraSyncConnection]",
        release_connection: "Callable[[VertebraSyncConnection], None]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[VertebraSqliteSyncDriver], VertebraSqliteSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: "VertebraSyncConnection | None" = None
        self._driver: "VertebraSqliteSyncDriver | None" = None

    def __enter__(self) -> "VertebraSqliteSyncDriver":
        from vertebra.extensions.sqlspec.sqlite.driver import VertebraSqliteSyncDriver

        self._connection = self._acquire_connection()
        self._driver = VertebraSqliteSyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None:
        _ = (exc_type, exc_val, exc_tb)
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


class VertebraSqliteAsyncSessionContext:
    """Async session context manager for broker-backed SQLite."""

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Awaitable[VertebraAsyncConnection]]",
        release_connection: "Callable[[VertebraAsyncConnection], Awaitable[None]]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, object]",
        prepare_driver: "Callable[[VertebraSqliteAsyncDriver], VertebraSqliteAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: "VertebraAsyncConnection | None" = None
        self._driver: "VertebraSqliteAsyncDriver | None" = None

    async def __aenter__(self) -> "VertebraSqliteAsyncDriver":
        from vertebra.extensions.sqlspec.sqlite.driver import VertebraSqliteAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = VertebraSqliteAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None:
        _ = (exc_type, exc_val, exc_tb)
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "VertebraSqliteAsyncConnection",
    "VertebraSqliteAsyncSessionContext",
    "VertebraSqliteSyncConnection",
    "VertebraSqliteSyncSessionContext",
)