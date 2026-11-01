"""Multi-database driver registry and URL scheme resolver."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from vertebra.db.dialer.alloydb import AlloyDBConfig, AlloyDBDialer
from vertebra.db.dialer.cloudsql import CloudSQLConfig, CloudSQLDialer
from vertebra.db.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig

__all__ = ("AnyDatabaseConfig", "AnyDatabaseDriver", "AnySyncDatabaseDriver", "DatabaseRegistry")

type AnyDatabaseConfig = (
    PostgreSQLConfig
    | MySQLConfig
    | SQLiteConfig
    | OracleConfig
    | MSSQLConfig
    | DuckDBConfig
    | RedisConfig
    | ValkeyConfig
    | CloudSQLConfig
    | AlloyDBConfig
)

type AnyDatabaseDriver = (
    AsyncPostgreSQLDriver
    | AsyncMySQLDriver
    | AsyncSQLiteDriver
    | AsyncOracleDriver
    | AsyncMSSQLDriver
    | AsyncDuckDBDriver
    | AsyncRedisDriver
    | AsyncValkeyDriver
    | CloudSQLDialer
    | AlloyDBDialer
)

type AnySyncDatabaseDriver = (
    SyncPostgreSQLDriver
    | SyncMySQLDriver
    | SyncSQLiteDriver
    | SyncOracleDriver
    | SyncMSSQLDriver
    | SyncDuckDBDriver
    | SyncRedisDriver
    | SyncValkeyDriver
    | CloudSQLDialer
    | AlloyDBDialer
)


class DatabaseRegistry:
    """Named registry of configured database, cache, and cloud dialer instances."""

    __slots__ = ("_broker", "_configs", "_drivers", "_sync_drivers")

    def __init__(self, *, broker: Any = None) -> None:
        self._configs: dict[str, AnyDatabaseConfig] = {}
        self._drivers: dict[str, AnyDatabaseDriver] = {}
        self._sync_drivers: dict[str, AnySyncDatabaseDriver] = {}
        self._broker = broker

    @property
    def names(self) -> tuple[str, ...]:
        """Return registered database aliases."""
        return tuple(self._configs.keys())

    @classmethod
    def parse_url(cls, url: str, **overrides: Any) -> AnyDatabaseConfig:
        """Parse a database or cache URL into its strongly typed configuration struct."""
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        base_scheme = scheme.split("+", 1)[0]

        if base_scheme in ("postgres", "postgresql"):
            return PostgreSQLConfig(dsn=url, **overrides)
        if base_scheme in ("mysql", "mariadb"):
            return MySQLConfig(dsn=url, **overrides)
        if base_scheme == "sqlite":
            if parsed.path in ("", "/", "/:memory:", ":memory:"):
                db_path = ":memory:"
            elif parsed.path.startswith("//"):
                db_path = parsed.path[1:]
            else:
                db_path = parsed.path.lstrip("/")
            return SQLiteConfig(path=db_path, database=db_path, **overrides)
        if base_scheme == "oracle":
            return OracleConfig(dsn=url, **overrides)
        if base_scheme in ("mssql", "sqlserver"):
            return MSSQLConfig(dsn=url, **overrides)
        if base_scheme == "duckdb":
            if parsed.path in ("", "/", "/:memory:", ":memory:"):
                duck_path = ":memory:"
            elif parsed.path.startswith("//"):
                duck_path = parsed.path[1:]
            else:
                duck_path = parsed.path.lstrip("/")
            return DuckDBConfig(path=duck_path, database=duck_path, **overrides)
        if base_scheme in ("redis", "rediss", "memory"):
            return RedisConfig(url=url, **overrides)
        if base_scheme in ("valkey", "valkeys"):
            return ValkeyConfig(url=url, **overrides)
        if base_scheme == "cloudsql":
            instance_uri = f"{parsed.netloc}{parsed.path}"
            return CloudSQLConfig(instance_connection_name=instance_uri, **overrides)
        if base_scheme == "alloydb":
            instance_uri = parsed.path if parsed.path.startswith("/projects/") else f"/{parsed.netloc}{parsed.path}"
            return AlloyDBConfig(instance_uri=instance_uri, **overrides)

        msg = f"Unsupported database URL scheme: {scheme!r}"
        raise ValueError(msg)

    def register(self, name: str, config_or_url: AnyDatabaseConfig | str) -> AnyDatabaseConfig:
        """Register a database alias from a typed config or URL."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        self._configs[name] = config
        self._drivers.pop(name, None)
        self._sync_drivers.pop(name, None)
        return config

    def get_config(self, name: str = "default") -> AnyDatabaseConfig:
        """Retrieve a registered configuration by alias."""
        if name not in self._configs:
            msg = f"Database alias {name!r} is not registered"
            raise KeyError(msg)
        return self._configs[name]

    def get_driver(self, name: str = "default") -> AnyDatabaseDriver:
        """Lazily instantiate and return the asynchronous driver for the given alias."""
        if name in self._drivers:
            return self._drivers[name]
        config = self.get_config(name)
        driver: AnyDatabaseDriver
        if isinstance(config, PostgreSQLConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = (
                    CloudSQLDialer(dialer_cfg) if isinstance(dialer_cfg, CloudSQLConfig) else AlloyDBDialer(dialer_cfg)
                )
            driver = AsyncPostgreSQLDriver(config, dialer=dialer, broker=self._broker)
        elif isinstance(config, MySQLConfig):
            mysql_dialer: CloudSQLDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_dialer = CloudSQLDialer(csql_cfg)
            driver = AsyncMySQLDriver(config, dialer=mysql_dialer, broker=self._broker)
        elif isinstance(config, SQLiteConfig):
            driver = AsyncSQLiteDriver(config, broker=self._broker)
        elif isinstance(config, OracleConfig):
            driver = AsyncOracleDriver(config, broker=self._broker)
        elif isinstance(config, MSSQLConfig):
            driver = AsyncMSSQLDriver(config, broker=self._broker)
        elif isinstance(config, DuckDBConfig):
            driver = AsyncDuckDBDriver(config, broker=self._broker)
        elif isinstance(config, RedisConfig):
            driver = AsyncRedisDriver(config)
        elif isinstance(config, ValkeyConfig):
            driver = AsyncValkeyDriver(config)
        elif isinstance(config, CloudSQLConfig):
            driver = CloudSQLDialer(config)
        elif isinstance(config, AlloyDBConfig):
            driver = AlloyDBDialer(config)
        else:
            msg = f"Unsupported config type: {type(config)!r}"
            raise TypeError(msg)
        self._drivers[name] = driver
        return driver

    def get_sync_driver(self, name: str = "default") -> AnySyncDatabaseDriver:
        """Lazily instantiate and return the synchronous driver for the given alias."""
        if name in self._sync_drivers:
            return self._sync_drivers[name]
        config = self.get_config(name)
        driver: AnySyncDatabaseDriver
        if isinstance(config, PostgreSQLConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = (
                    CloudSQLDialer(dialer_cfg) if isinstance(dialer_cfg, CloudSQLConfig) else AlloyDBDialer(dialer_cfg)
                )
            driver = SyncPostgreSQLDriver(config, dialer=dialer, broker=self._broker)
        elif isinstance(config, MySQLConfig):
            mysql_sync_dialer: CloudSQLDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_sync_dialer = CloudSQLDialer(csql_cfg)
            driver = SyncMySQLDriver(config, dialer=mysql_sync_dialer, broker=self._broker)
        elif isinstance(config, SQLiteConfig):
            driver = SyncSQLiteDriver(config, broker=self._broker)
        elif isinstance(config, OracleConfig):
            driver = SyncOracleDriver(config, broker=self._broker)
        elif isinstance(config, MSSQLConfig):
            driver = SyncMSSQLDriver(config, broker=self._broker)
        elif isinstance(config, DuckDBConfig):
            driver = SyncDuckDBDriver(config, broker=self._broker)
        elif isinstance(config, RedisConfig):
            driver = SyncRedisDriver(config)
        elif isinstance(config, ValkeyConfig):
            driver = SyncValkeyDriver(config)
        elif isinstance(config, CloudSQLConfig):
            driver = CloudSQLDialer(config)
        elif isinstance(config, AlloyDBConfig):
            driver = AlloyDBDialer(config)
        else:
            msg = f"Unsupported config type: {type(config)!r}"
            raise TypeError(msg)
        self._sync_drivers[name] = driver
        return driver
