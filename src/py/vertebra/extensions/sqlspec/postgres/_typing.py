"""PostgreSQL broker adapter type definitions."""

from types import TracebackType
from typing import TYPE_CHECKING

from vertebra.database.backends.broker import VertebraAsyncConnection, VertebraSyncConnection

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig

    from vertebra.extensions.sqlspec.postgres.driver import VertebraPostgresAsyncDriver, VertebraPostgresSyncDriver

    type VertebraPostgresSyncConnection = VertebraSyncConnection
    type VertebraPostgresAsyncConnection = VertebraAsyncConnection
else:
    VertebraPostgresSyncConnection = VertebraSyncConnection
    VertebraPostgresAsyncConnection = VertebraAsyncConnection


class VertebraPostgresSyncSessionContext:
    """Sync session context manager for broker-backed PostgreSQL."""

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
        prepare_driver: "Callable[[VertebraPostgresSyncDriver], VertebraPostgresSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: "VertebraSyncConnection | None" = None
        self._driver: "VertebraPostgresSyncDriver | None" = None

    def __enter__(self) -> "VertebraPostgresSyncDriver":
        from vertebra.extensions.sqlspec.postgres.driver import VertebraPostgresSyncDriver

        self._connection = self._acquire_connection()
        self._driver = VertebraPostgresSyncDriver(
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


class VertebraPostgresAsyncSessionContext:
    """Async session context manager for broker-backed PostgreSQL."""

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
        prepare_driver: "Callable[[VertebraPostgresAsyncDriver], VertebraPostgresAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: "VertebraAsyncConnection | None" = None
        self._driver: "VertebraPostgresAsyncDriver | None" = None

    async def __aenter__(self) -> "VertebraPostgresAsyncDriver":
        from vertebra.extensions.sqlspec.postgres.driver import VertebraPostgresAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = VertebraPostgresAsyncDriver(
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
    "VertebraPostgresAsyncConnection",
    "VertebraPostgresAsyncSessionContext",
    "VertebraPostgresSyncConnection",
    "VertebraPostgresSyncSessionContext",
)
