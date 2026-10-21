"""Multi-database driver registry and URL scheme resolver."""

from __future__ import annotations

from urllib.parse import urlparse

from vertebra.db.dialer.alloydb import AlloyDbConfig, AlloyDbDialer
from vertebra.db.dialer.cloudsql import CloudSqlConfig, CloudSqlDialer
from vertebra.db.mysql import AsyncMysqlDriver, MysqlConfig
from vertebra.db.oracle.config import OracleConfig
from vertebra.db.oracle.driver import AsyncOracleDriver
from vertebra.db.postgres import AsyncPostgresDriver, PostgresConfig
from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.sqlite import AsyncSqliteDriver, SqliteConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig

__all__ = (
    "AnyDatabaseConfig",
    "DatabaseRegistry",
    "parse_database_url",
)

type AnyDatabaseConfig = (
    PostgresConfig
    | MysqlConfig
    | SqliteConfig
    | RedisConfig
    | ValkeyConfig
    | OracleConfig
    | CloudSqlConfig
    | AlloyDbConfig
)


def parse_database_url(url: str) -> AnyDatabaseConfig:
    """Parse a database or cache URL into its strongly typed configuration struct."""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    base_scheme = scheme.split("+", 1)[0]

    if base_scheme in ("postgres", "postgresql"):
        return PostgresConfig(dsn=url)
    if base_scheme in ("mysql", "mariadb"):
        return MysqlConfig(dsn=url)
    if base_scheme == "sqlite":
        if parsed.path in ("", "/", "/:memory:", ":memory:"):
            db_path = ":memory:"
        elif parsed.path.startswith("//"):
            db_path = parsed.path[1:]
        else:
            db_path = parsed.path.lstrip("/")
        return SqliteConfig(database=db_path)
    if base_scheme in ("redis", "rediss"):
        return RedisConfig(url=url)
    if base_scheme in ("valkey", "valkeys"):
        return ValkeyConfig(url=url)
    if base_scheme == "oracle":
        return OracleConfig(
            user=parsed.username or "system",
            password=parsed.password or "",
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 1521,
            service_name=parsed.path.lstrip("/") or "FREEPDB1",
        )
    if base_scheme == "cloudsql":
        instance_uri = f"{parsed.netloc}{parsed.path}"
        return CloudSqlConfig(instance_connection_name=instance_uri)
    if base_scheme == "alloydb":
        instance_uri = parsed.path if parsed.path.startswith("/projects/") else f"/{parsed.netloc}{parsed.path}"
        return AlloyDbConfig(instance_uri=instance_uri)

    msg = f"Unsupported database URL scheme: {scheme!r}"
    raise ValueError(msg)


class DatabaseRegistry:
    """Named registry of configured database, cache, and cloud dialer instances."""

    def __init__(self) -> None:
        self._configs: dict[str, AnyDatabaseConfig] = {}
        self._drivers: dict[str, object] = {}

    @property
    def names(self) -> tuple[str, ...]:
        """Return registered database aliases."""
        return tuple(self._configs.keys())

    def register(self, name: str, config_or_url: AnyDatabaseConfig | str) -> AnyDatabaseConfig:
        """Register a database alias from a typed config or URL."""
        config = parse_database_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        self._configs[name] = config
        self._drivers.pop(name, None)
        return config

    def get_config(self, name: str = "default") -> AnyDatabaseConfig:
        """Retrieve a registered configuration by alias."""
        if name not in self._configs:
            msg = f"Database alias {name!r} is not registered"
            raise KeyError(msg)
        return self._configs[name]

    def get_driver(self, name: str = "default") -> object:
        """Lazily instantiate and return the driver for the given alias."""
        if name in self._drivers:
            return self._drivers[name]
        config = self.get_config(name)
        driver: object
        if isinstance(config, PostgresConfig):
            driver = AsyncPostgresDriver(config)
        elif isinstance(config, MysqlConfig):
            driver = AsyncMysqlDriver(config)
        elif isinstance(config, SqliteConfig):
            driver = AsyncSqliteDriver(config)
        elif isinstance(config, RedisConfig):
            driver = AsyncRedisDriver(config)
        elif isinstance(config, ValkeyConfig):
            driver = AsyncValkeyDriver(config)
        elif isinstance(config, OracleConfig):
            driver = AsyncOracleDriver(config)
        elif isinstance(config, CloudSqlConfig):
            driver = CloudSqlDialer(config)
        elif isinstance(config, AlloyDbConfig):
            driver = AlloyDbDialer(config)
        else:
            msg = f"Unsupported config type: {type(config)!r}"
            raise TypeError(msg)
        self._drivers[name] = driver
        return driver
