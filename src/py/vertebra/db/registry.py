"""Multi-database driver registry and URL scheme resolver."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast, overload
from urllib.parse import urlparse

from vertebra._vertebra import LeaseBroker
from vertebra.db.adapters.bigquery.config import BigQueryConfig
from vertebra.db.adapters.datafusion.config import DataFusionConfig
from vertebra.db.adapters.duckdb.config import DuckDBConfig
from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mysql.config import MySQLConfig
from vertebra.db.adapters.oracle.config import OracleConfig
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.redis.config import RedisConfig
from vertebra.db.adapters.spanner.config import SpannerConfig
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.adapters.valkey.config import ValkeyConfig
from vertebra.db.dialer.alloydb import (
    AlloyDBConfig,
    AsyncAlloyDBDialer,
    SyncAlloyDBDialer,
)
from vertebra.db.dialer.cloudsql import (
    AsyncCloudSQLDialer,
    CloudSQLConfig,
    SyncCloudSQLDialer,
)
from vertebra.db.driver._common import BaseDatabaseConfig

if TYPE_CHECKING:
    from vertebra.db.adapters.bigquery.driver import AsyncBigQueryDriver, SyncBigQueryDriver
    from vertebra.db.adapters.datafusion.driver import DataFusionEngine
    from vertebra.db.adapters.duckdb.driver import AsyncDuckDBDriver, SyncDuckDBDriver
    from vertebra.db.adapters.mssql.driver import AsyncMSSQLDriver, SyncMSSQLDriver
    from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver, SyncMySQLDriver
    from vertebra.db.adapters.oracle.driver import AsyncOracleDriver, SyncOracleDriver
    from vertebra.db.adapters.postgres.driver import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
    from vertebra.db.adapters.redis.driver import AsyncRedisDriver, SyncRedisDriver
    from vertebra.db.adapters.spanner.driver import AsyncSpannerDriver, SyncSpannerDriver
    from vertebra.db.adapters.sqlite.driver import AsyncSQLiteDriver, SyncSQLiteDriver
    from vertebra.db.adapters.valkey.driver import AsyncValkeyDriver, SyncValkeyDriver

__all__ = (
    "REGISTERED_CONFIG_TYPES",
    "SCHEME_REGISTRY",
    "AnyDatabaseConfig",
    "AnyDatabaseDriver",
    "AnySyncDatabaseDriver",
    "DatabaseRegistry",
    "provide_driver",
)

type AnyDatabaseConfig = (
    PostgreSQLConfig
    | MySQLConfig
    | SQLiteConfig
    | OracleConfig
    | MSSQLConfig
    | DuckDBConfig
    | BigQueryConfig
    | SpannerConfig
    | RedisConfig
    | ValkeyConfig
    | DataFusionConfig
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
    | AsyncBigQueryDriver
    | AsyncSpannerDriver
    | AsyncRedisDriver
    | AsyncValkeyDriver
    | DataFusionEngine
    | AsyncCloudSQLDialer
    | AsyncAlloyDBDialer
)

type AnySyncDatabaseDriver = (
    SyncPostgreSQLDriver
    | SyncMySQLDriver
    | SyncSQLiteDriver
    | SyncOracleDriver
    | SyncMSSQLDriver
    | SyncDuckDBDriver
    | SyncBigQueryDriver
    | SyncSpannerDriver
    | SyncRedisDriver
    | SyncValkeyDriver
    | DataFusionEngine
    | SyncCloudSQLDialer
    | SyncAlloyDBDialer
)

REGISTERED_CONFIG_TYPES: tuple[type[AnyDatabaseConfig], ...] = (
    PostgreSQLConfig,
    MySQLConfig,
    SQLiteConfig,
    OracleConfig,
    MSSQLConfig,
    DuckDBConfig,
    BigQueryConfig,
    SpannerConfig,
    RedisConfig,
    ValkeyConfig,
    DataFusionConfig,
    CloudSQLConfig,
    AlloyDBConfig,
)

SCHEME_REGISTRY: dict[str, type[AnyDatabaseConfig]] = {
    scheme: cfg_cls for cfg_cls in REGISTERED_CONFIG_TYPES for scheme in cfg_cls.url_schemes
}


class DatabaseRegistry:
    """Named registry of configured database, cache, analytics, and cloud dialer instances."""

    _DEFAULT_REGISTRY: ClassVar[DatabaseRegistry | None] = None
    _DEFAULT_BROKER: ClassVar[Any] = None

    __slots__ = (
        "_broker",
        "_configs",
        "_drivers",
        "_shared_async_drivers",
        "_shared_dialers",
        "_shared_sync_drivers",
        "_sync_drivers",
    )

    def __init__(self, *, broker: Any = None) -> None:
        self._configs: dict[str, AnyDatabaseConfig | BaseDatabaseConfig] = {}
        self._drivers: dict[str, AnyDatabaseDriver] = {}
        self._sync_drivers: dict[str, AnySyncDatabaseDriver] = {}
        self._shared_sync_drivers: dict[tuple[Any, ...], AnySyncDatabaseDriver] = {}
        self._shared_async_drivers: dict[tuple[Any, ...], AnyDatabaseDriver] = {}
        self._shared_dialers: dict[
            tuple[Any, ...],
            SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer,
        ] = {}
        self._broker = broker

    @property
    def names(self) -> tuple[str, ...]:
        """Return registered database aliases."""
        return tuple(self._configs.keys())

    @classmethod
    def default_broker(cls, *, pool_size: int = 32, max_overflow: int = 16) -> Any:
        """Return the process-wide shared Rust SHM LeaseBroker."""
        if cls._DEFAULT_BROKER is None:
            env_broker = LeaseBroker.from_env()
            cls._DEFAULT_BROKER = (
                env_broker if env_broker is not None else LeaseBroker(pool_size=pool_size, max_overflow=max_overflow)
            )
        return cls._DEFAULT_BROKER

    @classmethod
    def default(cls) -> DatabaseRegistry:
        """Return the process-wide default DatabaseRegistry sharing pools across frameworks."""
        if cls._DEFAULT_REGISTRY is None:
            cls._DEFAULT_REGISTRY = cls(broker=cls.default_broker())
        return cls._DEFAULT_REGISTRY

    @classmethod
    def pool_fingerprint(cls, config_or_url: AnyDatabaseConfig | BaseDatabaseConfig | str) -> tuple[Any, ...] | None:
        """Compute a canonical endpoint fingerprint for cross-framework native pool sharing."""
        config = cls.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        return config.pool_key()

    @classmethod
    def parse_url(cls, url: str, **overrides: Any) -> AnyDatabaseConfig:
        """Parse a database, cache, analytics, or dialer URL into its typed configuration struct."""
        raw_scheme, sep, rest = url.partition("://")
        scheme = (raw_scheme if sep else urlparse(url).scheme).lower()
        base_scheme = scheme
        if "+" in scheme:
            prefix, _, driver_tag = scheme.rpartition("+")
            if driver_tag in {"vtb", "vertebra"}:
                base_scheme = prefix
            elif driver_tag in {"vtb_async", "vertebra_async"}:
                base_scheme = prefix
                if sep:
                    sync_tag = driver_tag.removesuffix("_async")
                    url = f"{prefix}+{sync_tag}://{rest}"
        config_cls = (
            SCHEME_REGISTRY.get(scheme)
            or SCHEME_REGISTRY.get(base_scheme)
            or SCHEME_REGISTRY.get(base_scheme.split("+", 1)[0])
        )
        if config_cls is None:
            msg = f"Unsupported database URL scheme: {scheme!r}"
            raise ValueError(msg)
        return config_cls.from_url(url, **overrides)

    def register(
        self,
        name: str,
        config_or_url: AnyDatabaseConfig | BaseDatabaseConfig | str,
    ) -> AnyDatabaseConfig | BaseDatabaseConfig:
        """Register a database alias from a typed config or URL."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        self._configs[name] = config
        self._drivers.pop(name, None)
        self._sync_drivers.pop(name, None)
        return config

    def get_config(self, name: str = "default") -> AnyDatabaseConfig | BaseDatabaseConfig:
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
        driver = self._build_async_driver(config)
        self._drivers[name] = driver
        return driver

    def get_sync_driver(self, name: str = "default") -> AnySyncDatabaseDriver:
        """Lazily instantiate and return the synchronous driver for the given alias."""
        if name in self._sync_drivers:
            return self._sync_drivers[name]
        config = self.get_config(name)
        driver = self._build_sync_driver(config)
        self._sync_drivers[name] = driver
        return driver

    @overload
    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: Literal[False] = False,
    ) -> AsyncCloudSQLDialer | AsyncAlloyDBDialer: ...

    @overload
    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: Literal[True],
    ) -> SyncCloudSQLDialer | SyncAlloyDBDialer: ...

    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: bool = False,
    ) -> SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer:
        """Return a cached Cloud SQL or AlloyDB dialer for `config` and `sync` mode."""
        key = (*config.pool_key(), bool(sync))
        existing = self._shared_dialers.get(key)
        if existing is not None:
            return existing
        if isinstance(config, CloudSQLConfig):
            created: SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer = (
                SyncCloudSQLDialer(config) if sync else AsyncCloudSQLDialer(config)
            )
        else:
            created = SyncAlloyDBDialer(config) if sync else AsyncAlloyDBDialer(config)
        self._shared_dialers[key] = created
        return created

    def get_shared_driver(self, config_or_url: AnyDatabaseConfig | BaseDatabaseConfig | str) -> AnyDatabaseDriver:
        """Return a loop-sharded shared asynchronous driver keyed by canonical endpoint fingerprint."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        if isinstance(config, (CloudSQLConfig, AlloyDBConfig)):
            return self.get_dialer(config, sync=False)
        base_key = config.pool_key()
        if base_key is None:
            return self._build_async_driver(config)
        loop_id = 0
        with contextlib.suppress(RuntimeError):
            loop_id = id(asyncio.get_running_loop())
        key = (*base_key, loop_id)
        cached = self._shared_async_drivers.get(key)
        if cached is not None and not getattr(cached, "_closed", False) and not getattr(cached, "closed", False):
            return cached
        driver = self._build_async_driver(config)
        self._shared_async_drivers[key] = driver
        return driver

    def get_shared_sync_driver(
        self,
        config_or_url: AnyDatabaseConfig | BaseDatabaseConfig | str,
    ) -> AnySyncDatabaseDriver:
        """Return a shared synchronous driver keyed by canonical endpoint fingerprint."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        if isinstance(config, (CloudSQLConfig, AlloyDBConfig)):
            return self.get_dialer(config, sync=True)
        key = config.pool_key()
        if key is None:
            return self._build_sync_driver(config)
        cached = self._shared_sync_drivers.get(key)
        if cached is not None and not getattr(cached, "_closed", False) and not getattr(cached, "closed", False):
            return cached
        driver = self._build_sync_driver(config)
        self._shared_sync_drivers[key] = driver
        return driver

    def _build_async_driver(self, config: AnyDatabaseConfig | BaseDatabaseConfig) -> AnyDatabaseDriver:
        if isinstance(config, (CloudSQLConfig, AlloyDBConfig)):
            return self.get_dialer(config, sync=False)
        if isinstance(config, BaseDatabaseConfig):
            return cast("AnyDatabaseDriver", config.build_async_driver(self))
        msg = f"Unsupported config type: {type(config)!r}"
        raise TypeError(msg)

    def _build_sync_driver(self, config: AnyDatabaseConfig | BaseDatabaseConfig) -> AnySyncDatabaseDriver:
        if isinstance(config, (CloudSQLConfig, AlloyDBConfig)):
            return self.get_dialer(config, sync=True)
        if isinstance(config, BaseDatabaseConfig):
            return cast("AnySyncDatabaseDriver", config.build_sync_driver(self))
        msg = f"Unsupported config type: {type(config)!r}"
        raise TypeError(msg)


@overload
def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: Literal[False] = False,
    registry: DatabaseRegistry | None = None,
) -> Any: ...


@overload
def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: Literal[True],
    registry: DatabaseRegistry | None = None,
) -> Any: ...


def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: bool = False,
    registry: DatabaseRegistry | None = None,
) -> Any:
    """Resolve a database, cache, or analytics driver from `DatabaseRegistry` for framework DI."""
    if isinstance(name_or_config, (CloudSQLConfig, AlloyDBConfig)):
        msg = (
            "CloudSQLConfig and AlloyDBConfig are transport dialers, not database drivers; "
            "use DatabaseRegistry.get_dialer() or attach cloud_sql_instance/alloydb_instance_uri to a database config"
        )
        raise TypeError(msg)
    reg = registry if registry is not None else DatabaseRegistry.default()
    if isinstance(name_or_config, str):
        if "://" not in name_or_config:
            cfg = reg.get_config(name_or_config)
            if isinstance(cfg, (CloudSQLConfig, AlloyDBConfig)):
                msg = (
                    f"Registered alias {name_or_config!r} points to a transport dialer; "
                    "use DatabaseRegistry.get_dialer() or register a database config"
                )
                raise TypeError(msg)
            return reg.get_sync_driver(name_or_config) if sync else reg.get_driver(name_or_config)
        parsed = reg.parse_url(name_or_config)
        if isinstance(parsed, (CloudSQLConfig, AlloyDBConfig)):
            msg = (
                f"URL {name_or_config!r} resolves to a transport dialer; "
                "use DatabaseRegistry.get_dialer() or attach cloud_sql_instance/alloydb_instance_uri to a database config"
            )
            raise TypeError(msg)
        return reg.get_shared_sync_driver(parsed) if sync else reg.get_shared_driver(parsed)
    if isinstance(name_or_config, BaseDatabaseConfig):
        return reg.get_shared_sync_driver(name_or_config) if sync else reg.get_shared_driver(name_or_config)
    msg = f"Unsupported driver target: {type(name_or_config)!r}"
    raise TypeError(msg)
