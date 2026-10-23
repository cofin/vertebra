"""Multi-database driver registry and URL scheme resolver."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from vertebra.db.dialer.alloydb import AlloyDbConfig, AlloyDbDialer
from vertebra.db.dialer.cloudsql import CloudSqlConfig, CloudSqlDialer
from vertebra.db.mysql import AsyncMysqlDriver, MysqlConfig, SyncMysqlDriver
from vertebra.db.postgres import AsyncPostgresDriver, PostgresConfig, SyncPostgresDriver
from vertebra.db.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.sqlite import AsyncSqliteDriver, SqliteConfig, SyncSqliteDriver
from vertebra.db.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig

__all__ = ("AnyDatabaseConfig", "AnyDatabaseDriver", "AnySyncDatabaseDriver", "DatabaseRegistry")

type AnyDatabaseConfig = (
    PostgresConfig | MysqlConfig | SqliteConfig | RedisConfig | ValkeyConfig | CloudSqlConfig | AlloyDbConfig
)

type AnyDatabaseDriver = (
    AsyncPostgresDriver
    | AsyncMysqlDriver
    | AsyncSqliteDriver
    | AsyncRedisDriver
    | AsyncValkeyDriver
    | CloudSqlDialer
    | AlloyDbDialer
)

type AnySyncDatabaseDriver = (
    SyncPostgresDriver
    | SyncMysqlDriver
    | SyncSqliteDriver
    | SyncRedisDriver
    | SyncValkeyDriver
    | CloudSqlDialer
    | AlloyDbDialer
)


class DatabaseRegistry:
    """Named registry of configured database, cache, and cloud dialer instances."""

    __slots__ = ("_configs", "_drivers", "_sync_drivers")

    def __init__(self) -> None:
        self._configs: dict[str, AnyDatabaseConfig] = {}
        self._drivers: dict[str, AnyDatabaseDriver] = {}
        self._sync_drivers: dict[str, AnySyncDatabaseDriver] = {}

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
            return PostgresConfig(dsn=url, **overrides)
        if base_scheme in ("mysql", "mariadb"):
            return MysqlConfig(dsn=url, **overrides)
        if base_scheme == "sqlite":
            if parsed.path in ("", "/", "/:memory:", ":memory:"):
                db_path = ":memory:"
            elif parsed.path.startswith("//"):
                db_path = parsed.path[1:]
            else:
                db_path = parsed.path.lstrip("/")
            return SqliteConfig(database=db_path, **overrides)
        if base_scheme in ("redis", "rediss", "memory"):
            return RedisConfig(url=url, **overrides)
        if base_scheme in ("valkey", "valkeys"):
            return ValkeyConfig(url=url, **overrides)
        if base_scheme == "cloudsql":
            instance_uri = f"{parsed.netloc}{parsed.path}"
            return CloudSqlConfig(instance_connection_name=instance_uri, **overrides)
        if base_scheme == "alloydb":
            instance_uri = parsed.path if parsed.path.startswith("/projects/") else f"/{parsed.netloc}{parsed.path}"
            return AlloyDbConfig(instance_uri=instance_uri, **overrides)

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
        if isinstance(config, PostgresConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = (
                    CloudSqlDialer(dialer_cfg) if isinstance(dialer_cfg, CloudSqlConfig) else AlloyDbDialer(dialer_cfg)
                )
            driver = AsyncPostgresDriver(config, dialer=dialer)
        elif isinstance(config, MysqlConfig):
            mysql_dialer: CloudSqlDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_dialer = CloudSqlDialer(csql_cfg)
            driver = AsyncMysqlDriver(config, dialer=mysql_dialer)
        elif isinstance(config, SqliteConfig):
            driver = AsyncSqliteDriver(config)
        elif isinstance(config, RedisConfig):
            driver = AsyncRedisDriver(config)
        elif isinstance(config, ValkeyConfig):
            driver = AsyncValkeyDriver(config)
        elif isinstance(config, CloudSqlConfig):
            driver = CloudSqlDialer(config)
        elif isinstance(config, AlloyDbConfig):
            driver = AlloyDbDialer(config)
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
        if isinstance(config, PostgresConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = (
                    CloudSqlDialer(dialer_cfg) if isinstance(dialer_cfg, CloudSqlConfig) else AlloyDbDialer(dialer_cfg)
                )
            driver = SyncPostgresDriver(config, dialer=dialer)
        elif isinstance(config, MysqlConfig):
            mysql_sync_dialer: CloudSqlDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_sync_dialer = CloudSqlDialer(csql_cfg)
            driver = SyncMysqlDriver(config, dialer=mysql_sync_dialer)
        elif isinstance(config, SqliteConfig):
            driver = SyncSqliteDriver(config)
        elif isinstance(config, RedisConfig):
            driver = SyncRedisDriver(config)
        elif isinstance(config, ValkeyConfig):
            driver = SyncValkeyDriver(config)
        elif isinstance(config, CloudSqlConfig):
            driver = CloudSqlDialer(config)
        elif isinstance(config, AlloyDbConfig):
            driver = AlloyDbDialer(config)
        else:
            msg = f"Unsupported config type: {type(config)!r}"
            raise TypeError(msg)
        self._sync_drivers[name] = driver
        return driver
