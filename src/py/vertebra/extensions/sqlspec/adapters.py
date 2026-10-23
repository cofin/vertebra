"""First-class SQLSpec adapter configurations backed by Vertebra Rust-native SHM drivers."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, cast

from vertebra.db.duckdb import AsyncDuckdbDriver, DuckdbConfig, SyncDuckdbDriver
from vertebra.db.mssql import AsyncMssqlDriver, MssqlConfig, SyncMssqlDriver
from vertebra.db.mysql import AsyncMysqlDriver, MysqlConfig, SyncMysqlDriver
from vertebra.db.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    AsyncPostgresDriver,
    PostgresConfig,
    SyncPostgresDriver,
    VertebraArrowCursor,
)
from vertebra.db.sqlite import AsyncSqliteDriver, SqliteConfig, SyncSqliteDriver

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping, Sequence

__all__ = [
    "VertebraDuckdbAsyncConfig",
    "VertebraDuckdbSyncConfig",
    "VertebraMssqlAsyncConfig",
    "VertebraMssqlSyncConfig",
    "VertebraMysqlAsyncConfig",
    "VertebraMysqlSyncConfig",
    "VertebraOracleAsyncConfig",
    "VertebraOracleSyncConfig",
    "VertebraPostgresAsyncConfig",
    "VertebraPostgresSyncConfig",
    "VertebraSqlSpecAsyncConfig",
    "VertebraSqlSpecSyncConfig",
    "VertebraSqliteAsyncConfig",
    "VertebraSqliteSyncConfig",
]


class _SyncAdapterSession:
    """SQLSpec-compatible synchronous session facade over a Vertebra native sync driver."""

    __slots__ = ("_arrow_threshold_rows", "_driver")

    def __init__(self, driver: Any, *, arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS) -> None:
        self._driver = driver
        self._arrow_threshold_rows = arrow_threshold_rows

    @property
    def driver(self) -> Any:
        """Return the underlying Vertebra sync driver."""
        return self._driver

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement and return affected rows."""
        res = self._driver.execute(sql) if params is None else self._driver.execute(sql, params)
        return len(res) if isinstance(res, list) else int(res)

    def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` statement and return rows as dictionaries."""
        res = self._driver.select(sql) if params is None else self._driver.select(sql, params)
        return list(res)

    def select_one(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        """Execute a `SELECT` statement and return the first row or `None`."""
        rows = self.select(sql, params)
        return rows[0] if rows else None

    def select_value(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> Any:
        """Execute a `SELECT` statement and return the first column of the first row."""
        row = self.select_one(sql, params)
        if not row:
            return None
        return next(iter(row.values()))

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int | None = None,
    ) -> VertebraArrowCursor:
        """Execute a query and return a zero-copy `VertebraArrowCursor`."""
        threshold = self._arrow_threshold_rows if shm_threshold_rows is None else shm_threshold_rows
        if params is None:
            return cast("VertebraArrowCursor", self._driver.select_to_arrow(sql, shm_threshold_rows=threshold))
        return cast("VertebraArrowCursor", self._driver.select_to_arrow(sql, params, shm_threshold_rows=threshold))


class _AsyncAdapterSession:
    """SQLSpec-compatible asynchronous session facade over a Vertebra native async driver."""

    __slots__ = ("_arrow_threshold_rows", "_driver")

    def __init__(self, driver: Any, *, arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS) -> None:
        self._driver = driver
        self._arrow_threshold_rows = arrow_threshold_rows

    @property
    def driver(self) -> Any:
        """Return the underlying Vertebra async driver."""
        return self._driver

    async def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DML or DDL statement asynchronously."""
        res = await self._driver.execute(sql) if params is None else await self._driver.execute(sql, params)
        return len(res) if isinstance(res, list) else int(res)

    async def select(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a `SELECT` statement asynchronously and return rows as dictionaries."""
        res = await self._driver.select(sql) if params is None else await self._driver.select(sql, params)
        return list(res)

    async def select_one(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Execute a `SELECT` statement asynchronously and return the first row or `None`."""
        rows = await self.select(sql, params)
        return rows[0] if rows else None

    async def select_value(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> Any:
        """Execute a `SELECT` statement asynchronously and return the first scalar value."""
        row = await self.select_one(sql, params)
        if not row:
            return None
        return next(iter(row.values()))

    async def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int | None = None,
    ) -> VertebraArrowCursor:
        """Execute a query asynchronously and return a zero-copy `VertebraArrowCursor`."""
        threshold = self._arrow_threshold_rows if shm_threshold_rows is None else shm_threshold_rows
        if params is None:
            return cast("VertebraArrowCursor", await self._driver.select_to_arrow(sql, shm_threshold_rows=threshold))
        return cast(
            "VertebraArrowCursor", await self._driver.select_to_arrow(sql, params, shm_threshold_rows=threshold)
        )


class VertebraSqlSpecSyncConfig[ConfigT, DriverT]:
    """Base synchronous SQLSpec adapter configuration backed by a Vertebra native driver."""

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = False
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]

    def __init__(
        self,
        config: ConfigT | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> None:
        self.bind_key = bind_key
        self.broker = broker
        self.arrow_threshold_rows = arrow_threshold_rows
        if config is not None:
            self.driver_config: ConfigT = config
        else:
            if broker is not None and "broker" not in kwargs:
                kwargs["broker"] = broker
            self.driver_config = self._config_cls(**kwargs)
        self._driver_instance: DriverT | None = None

    def create_driver(self) -> DriverT:
        """Instantiate or return the cached synchronous Vertebra database driver."""
        if self._driver_instance is None:
            self._driver_instance = self._driver_cls(self.driver_config, broker=self.broker)
        return self._driver_instance

    @contextmanager
    def provide_session(self) -> Iterator[_SyncAdapterSession]:
        """Yield a synchronous SQLSpec adapter session."""
        driver = self.create_driver()
        yield _SyncAdapterSession(driver, arrow_threshold_rows=self.arrow_threshold_rows)

    def close(self) -> None:
        """Close the cached synchronous driver if initialized."""
        if self._driver_instance is not None and hasattr(self._driver_instance, "close"):
            self._driver_instance.close()
            self._driver_instance = None


class VertebraSqlSpecAsyncConfig[ConfigT, DriverT]:
    """Base asynchronous SQLSpec adapter configuration backed by a Vertebra native driver."""

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = True
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]

    def __init__(
        self,
        config: ConfigT | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> None:
        self.bind_key = bind_key
        self.broker = broker
        self.arrow_threshold_rows = arrow_threshold_rows
        if config is not None:
            self.driver_config: ConfigT = config
        else:
            if broker is not None and "broker" not in kwargs:
                kwargs["broker"] = broker
            self.driver_config = self._config_cls(**kwargs)
        self._driver_instance: DriverT | None = None

    def create_driver(self) -> DriverT:
        """Instantiate or return the cached asynchronous Vertebra database driver."""
        if self._driver_instance is None:
            self._driver_instance = self._driver_cls(self.driver_config, broker=self.broker)
        return self._driver_instance

    @asynccontextmanager
    async def provide_session(self) -> AsyncIterator[_AsyncAdapterSession]:
        """Yield an asynchronous SQLSpec adapter session."""
        driver = self.create_driver()
        yield _AsyncAdapterSession(driver, arrow_threshold_rows=self.arrow_threshold_rows)

    async def close(self) -> None:
        """Close the cached asynchronous driver if initialized."""
        if self._driver_instance is not None and hasattr(self._driver_instance, "close"):
            await self._driver_instance.close()
            self._driver_instance = None


class VertebraPostgresSyncConfig(VertebraSqlSpecSyncConfig[PostgresConfig, SyncPostgresDriver]):
    """Synchronous SQLSpec adapter configuration for PostgreSQL (`postgres+vtb`)."""

    dialect: ClassVar[str] = "postgres"
    _config_cls: ClassVar[type[PostgresConfig]] = PostgresConfig
    _driver_cls: ClassVar[type[SyncPostgresDriver]] = SyncPostgresDriver


class VertebraPostgresAsyncConfig(VertebraSqlSpecAsyncConfig[PostgresConfig, AsyncPostgresDriver]):
    """Asynchronous SQLSpec adapter configuration for PostgreSQL (`postgres+vtb`)."""

    dialect: ClassVar[str] = "postgres"
    _config_cls: ClassVar[type[PostgresConfig]] = PostgresConfig
    _driver_cls: ClassVar[type[AsyncPostgresDriver]] = AsyncPostgresDriver


class VertebraMysqlSyncConfig(VertebraSqlSpecSyncConfig[MysqlConfig, SyncMysqlDriver]):
    """Synchronous SQLSpec adapter configuration for MySQL/MariaDB (`mysql+vtb`)."""

    dialect: ClassVar[str] = "mysql"
    _config_cls: ClassVar[type[MysqlConfig]] = MysqlConfig
    _driver_cls: ClassVar[type[SyncMysqlDriver]] = SyncMysqlDriver


class VertebraMysqlAsyncConfig(VertebraSqlSpecAsyncConfig[MysqlConfig, AsyncMysqlDriver]):
    """Asynchronous SQLSpec adapter configuration for MySQL/MariaDB (`mysql+vtb`)."""

    dialect: ClassVar[str] = "mysql"
    _config_cls: ClassVar[type[MysqlConfig]] = MysqlConfig
    _driver_cls: ClassVar[type[AsyncMysqlDriver]] = AsyncMysqlDriver


class VertebraSqliteSyncConfig(VertebraSqlSpecSyncConfig[SqliteConfig, SyncSqliteDriver]):
    """Synchronous SQLSpec adapter configuration for SQLite (`sqlite+vtb`)."""

    dialect: ClassVar[str] = "sqlite"
    _config_cls: ClassVar[type[SqliteConfig]] = SqliteConfig
    _driver_cls: ClassVar[type[SyncSqliteDriver]] = SyncSqliteDriver


class VertebraSqliteAsyncConfig(VertebraSqlSpecAsyncConfig[SqliteConfig, AsyncSqliteDriver]):
    """Asynchronous SQLSpec adapter configuration for SQLite (`sqlite+vtb`)."""

    dialect: ClassVar[str] = "sqlite"
    _config_cls: ClassVar[type[SqliteConfig]] = SqliteConfig
    _driver_cls: ClassVar[type[AsyncSqliteDriver]] = AsyncSqliteDriver


class VertebraOracleSyncConfig(VertebraSqlSpecSyncConfig[OracleConfig, SyncOracleDriver]):
    """Synchronous SQLSpec adapter configuration for Oracle Thin (`oracle+vtb`)."""

    dialect: ClassVar[str] = "oracle"
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver


class VertebraOracleAsyncConfig(VertebraSqlSpecAsyncConfig[OracleConfig, AsyncOracleDriver]):
    """Asynchronous SQLSpec adapter configuration for Oracle Thin (`oracle+vtb`)."""

    dialect: ClassVar[str] = "oracle"
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver


class VertebraMssqlSyncConfig(VertebraSqlSpecSyncConfig[MssqlConfig, SyncMssqlDriver]):
    """Synchronous SQLSpec adapter configuration for SQL Server (`mssql+vtb`)."""

    dialect: ClassVar[str] = "mssql"
    _config_cls: ClassVar[type[MssqlConfig]] = MssqlConfig
    _driver_cls: ClassVar[type[SyncMssqlDriver]] = SyncMssqlDriver


class VertebraMssqlAsyncConfig(VertebraSqlSpecAsyncConfig[MssqlConfig, AsyncMssqlDriver]):
    """Asynchronous SQLSpec adapter configuration for SQL Server (`mssql+vtb`)."""

    dialect: ClassVar[str] = "mssql"
    _config_cls: ClassVar[type[MssqlConfig]] = MssqlConfig
    _driver_cls: ClassVar[type[AsyncMssqlDriver]] = AsyncMssqlDriver


class VertebraDuckdbSyncConfig(VertebraSqlSpecSyncConfig[DuckdbConfig, SyncDuckdbDriver]):
    """Synchronous SQLSpec adapter configuration for DuckDB (`duckdb+vtb`)."""

    dialect: ClassVar[str] = "duckdb"
    _config_cls: ClassVar[type[DuckdbConfig]] = DuckdbConfig
    _driver_cls: ClassVar[type[SyncDuckdbDriver]] = SyncDuckdbDriver


class VertebraDuckdbAsyncConfig(VertebraSqlSpecAsyncConfig[DuckdbConfig, AsyncDuckdbDriver]):
    """Asynchronous SQLSpec adapter configuration for DuckDB (`duckdb+vtb`)."""

    dialect: ClassVar[str] = "duckdb"
    _config_cls: ClassVar[type[DuckdbConfig]] = DuckdbConfig
    _driver_cls: ClassVar[type[AsyncDuckdbDriver]] = AsyncDuckdbDriver
