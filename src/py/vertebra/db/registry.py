"""Multi-database driver registry and URL scheme resolver."""

from __future__ import annotations

import asyncio
import contextlib
import os
from typing import Any, ClassVar, cast
from urllib.parse import parse_qsl, urlparse

from vertebra._vertebra import LeaseBroker
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, SyncSpannerDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
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

__all__ = (
    "AnyDatabaseConfig",
    "AnyDatabaseDriver",
    "AnySyncDatabaseDriver",
    "DatabaseRegistry",
    "provide_bigquery_driver",
    "provide_spanner_driver",
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
    | SyncCloudSQLDialer
    | SyncAlloyDBDialer
)


class DatabaseRegistry:
    """Named registry of configured database, cache, and cloud dialer instances."""

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
        self._configs: dict[str, AnyDatabaseConfig] = {}
        self._drivers: dict[str, AnyDatabaseDriver] = {}
        self._sync_drivers: dict[str, AnySyncDatabaseDriver] = {}
        self._shared_sync_drivers: dict[tuple[Any, ...], AnySyncDatabaseDriver] = {}
        self._shared_async_drivers: dict[tuple[Any, ...], AnyDatabaseDriver] = {}
        self._shared_dialers: dict[
            tuple[str, str],
            SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer,
        ] = {}
        self._broker = broker

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
    def pool_fingerprint(cls, config_or_url: AnyDatabaseConfig | str) -> tuple[Any, ...]:
        """Compute a canonical endpoint fingerprint for cross-framework native pool sharing."""
        config = cls.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        if isinstance(config, PostgreSQLConfig):
            parsed = urlparse(config.dsn)
            host = (parsed.hostname or "localhost").lower()
            port = int(parsed.port or 5432)
            user = parsed.username or ""
            database = parsed.path.lstrip("/") if parsed.path else "postgres"
            pooler_mode = (
                "transaction"
                if (config.pgbouncer or config.pgbouncer_mode or config.pooler_mode == "transaction")
                else str(config.pooler_mode)
            )
            return (
                "postgresql",
                host,
                port,
                database,
                user,
                pooler_mode,
                config.cloud_sql_instance or "",
                config.alloydb_instance_uri or "",
            )
        if isinstance(config, MySQLConfig):
            parsed = urlparse(config.dsn)
            host = (parsed.hostname or "localhost").lower()
            port = int(parsed.port or 3306)
            user = parsed.username or ""
            database = parsed.path.lstrip("/") if parsed.path else ""
            return ("mysql", host, port, database, user, config.cloud_sql_instance or "")
        if isinstance(config, SQLiteConfig):
            raw_path = str(config.path or config.database or ":memory:")
            norm_path = raw_path if (raw_path == ":memory:" or "mode=memory" in raw_path) else os.path.abspath(raw_path)
            return ("sqlite", norm_path, bool(config.wal_mode))
        if isinstance(config, OracleConfig):
            desc = config.connect_descriptor()
            return ("oracle", desc.host.lower(), int(desc.port), desc.service_name or desc.sid or "", config.user)
        if isinstance(config, MSSQLConfig):
            return (
                "mssql",
                config.host.lower(),
                int(config.port),
                config.database,
                config.user,
                config.cloudsql_instance or "",
            )
        if isinstance(config, DuckDBConfig):
            raw_duck = str(config.path if config.path != ":memory:" else (config.database or ":memory:"))
            norm_duck = raw_duck if raw_duck == ":memory:" else os.path.abspath(raw_duck)
            return ("duckdb", norm_duck, bool(config.read_only))
        if isinstance(config, BigQueryConfig):
            return (
                "bigquery",
                config.effective_project_id(),
                config.effective_dataset_id() or "",
                config.location,
                config.rest_endpoint,
                config.storage_endpoint,
            )
        if isinstance(config, SpannerConfig):
            return (
                "spanner",
                config.database_uri,
                config.database_role or "",
                config.dialect,
                config.endpoint,
            )
        if isinstance(config, ValkeyConfig):
            return ("valkey", config.url)
        if isinstance(config, RedisConfig):
            return ("redis", config.url)
        if isinstance(config, CloudSQLConfig):
            return ("cloudsql", config.instance_connection_name)
        if isinstance(config, AlloyDBConfig):
            return ("alloydb", config.instance_uri)
        return ("unknown", repr(config))

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
        query_params = dict(parse_qsl(parsed.query)) if parsed.query else {}

        if base_scheme in ("postgres", "postgresql"):
            if "pgbouncer" in query_params and "pgbouncer" not in overrides:
                overrides["pgbouncer"] = query_params["pgbouncer"].lower() in {"1", "true", "yes"}
            if "pooler_mode" in query_params and "pooler_mode" not in overrides:
                overrides["pooler_mode"] = query_params["pooler_mode"]
            if "cloud_sql_instance" in query_params and "cloud_sql_instance" not in overrides:
                overrides["cloud_sql_instance"] = query_params["cloud_sql_instance"]
            if "alloydb_instance_uri" in query_params and "alloydb_instance_uri" not in overrides:
                overrides["alloydb_instance_uri"] = query_params["alloydb_instance_uri"]
            if "direct_dsn" in query_params and "direct_dsn" not in overrides:
                overrides["direct_dsn"] = query_params["direct_dsn"]
            return PostgreSQLConfig(dsn=url, **overrides)
        if base_scheme in ("mysql", "mariadb"):
            if "cloud_sql_instance" in query_params and "cloud_sql_instance" not in overrides:
                overrides["cloud_sql_instance"] = query_params["cloud_sql_instance"]
            return MySQLConfig(dsn=url, **overrides)
        if base_scheme == "sqlite":
            if parsed.path in ("", "/", "/:memory:", ":memory:"):
                db_path = ":memory:"
            elif parsed.path.startswith("//"):
                db_path = parsed.path[1:]
            elif os.path.isabs(parsed.path) and os.path.exists(os.path.dirname(parsed.path)):
                db_path = parsed.path
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
        if base_scheme in ("bigquery", "bq"):
            return BigQueryConfig.from_dsn(url, **overrides)
        if base_scheme in ("spanner", "spangres"):
            return SpannerConfig.from_dsn(url, **overrides)

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

    def get_shared_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnyDatabaseDriver:
        """Return a loop-sharded shared asynchronous driver keyed by canonical endpoint fingerprint."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        if isinstance(config, SQLiteConfig) and str(config.path or config.database or ":memory:") == ":memory:":
            return self._build_async_driver(config)
        if isinstance(config, DuckDBConfig) and config.resolved_path() == ":memory:":
            return self._build_async_driver(config)
        loop_id = 0
        with contextlib.suppress(RuntimeError):
            loop_id = id(asyncio.get_running_loop())
        key = (*self.pool_fingerprint(config), loop_id)
        cached = self._shared_async_drivers.get(key)
        if cached is not None and not getattr(cached, "_closed", False) and not getattr(cached, "closed", False):
            return cached
        driver = self._build_async_driver(config)
        self._shared_async_drivers[key] = driver
        return driver

    def get_shared_sync_driver(self, config_or_url: AnyDatabaseConfig | str) -> AnySyncDatabaseDriver:
        """Return a shared synchronous driver keyed by canonical endpoint fingerprint."""
        config = self.parse_url(config_or_url) if isinstance(config_or_url, str) else config_or_url
        if isinstance(config, SQLiteConfig) and str(config.path or config.database or ":memory:") == ":memory:":
            return self._build_sync_driver(config)
        if isinstance(config, DuckDBConfig) and config.resolved_path() == ":memory:":
            return self._build_sync_driver(config)
        key = self.pool_fingerprint(config)
        cached = self._shared_sync_drivers.get(key)
        if cached is not None and not getattr(cached, "_closed", False) and not getattr(cached, "closed", False):
            return cached
        driver = self._build_sync_driver(config)
        self._shared_sync_drivers[key] = driver
        return driver

    def _get_or_create_dialer(
        self, dialer_cfg: CloudSQLConfig | AlloyDBConfig, *, is_async: bool = False
    ) -> SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer:
        if isinstance(dialer_cfg, CloudSQLConfig):
            key = ("cloudsql_async" if is_async else "cloudsql_sync", dialer_cfg.instance_connection_name)
            existing = self._shared_dialers.get(key)
            if existing is None:
                existing = AsyncCloudSQLDialer(dialer_cfg) if is_async else SyncCloudSQLDialer(dialer_cfg)
                self._shared_dialers[key] = existing
            return existing
        key = ("alloydb_async" if is_async else "alloydb_sync", dialer_cfg.instance_uri)
        existing = self._shared_dialers.get(key)
        if existing is None:
            existing = AsyncAlloyDBDialer(dialer_cfg) if is_async else SyncAlloyDBDialer(dialer_cfg)
            self._shared_dialers[key] = existing
        return existing

    def _build_async_driver(self, config: AnyDatabaseConfig) -> AnyDatabaseDriver:
        broker = getattr(config, "broker", None)
        if broker is None:
            broker = self._broker
        if isinstance(config, PostgreSQLConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = self._get_or_create_dialer(dialer_cfg, is_async=True)
            return AsyncPostgreSQLDriver(config, dialer=dialer, broker=broker)
        if isinstance(config, MySQLConfig):
            mysql_dialer: AsyncCloudSQLDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_dialer = cast("AsyncCloudSQLDialer", self._get_or_create_dialer(csql_cfg, is_async=True))
            return AsyncMySQLDriver(config, dialer=mysql_dialer, broker=broker)
        if isinstance(config, SQLiteConfig):
            return AsyncSQLiteDriver(config, broker=broker)
        if isinstance(config, OracleConfig):
            return AsyncOracleDriver(config, broker=broker)
        if isinstance(config, MSSQLConfig):
            return AsyncMSSQLDriver(config, broker=broker)
        if isinstance(config, DuckDBConfig):
            return AsyncDuckDBDriver(config, broker=broker)
        if isinstance(config, BigQueryConfig):
            return AsyncBigQueryDriver(config, broker=broker)
        if isinstance(config, SpannerConfig):
            return AsyncSpannerDriver(config, broker=broker)
        if isinstance(config, ValkeyConfig):
            return AsyncValkeyDriver(config)
        if isinstance(config, RedisConfig):
            return AsyncRedisDriver(config)
        if isinstance(config, CloudSQLConfig):
            return cast("AsyncCloudSQLDialer", self._get_or_create_dialer(config, is_async=True))
        if isinstance(config, AlloyDBConfig):
            return cast("AsyncAlloyDBDialer", self._get_or_create_dialer(config, is_async=True))
        msg = f"Unsupported config type: {type(config)!r}"
        raise TypeError(msg)

    def _build_sync_driver(self, config: AnyDatabaseConfig) -> AnySyncDatabaseDriver:
        broker = getattr(config, "broker", None)
        if broker is None:
            broker = self._broker
        if isinstance(config, PostgreSQLConfig):
            dialer: object = None
            if (dialer_cfg := config.to_cloud_dialer_config()) is not None:
                dialer = self._get_or_create_dialer(dialer_cfg, is_async=False)
            return SyncPostgreSQLDriver(config, dialer=dialer, broker=broker)
        if isinstance(config, MySQLConfig):
            mysql_sync_dialer: SyncCloudSQLDialer | None = None
            if (csql_cfg := config.to_cloud_dialer_config()) is not None:
                mysql_sync_dialer = cast("SyncCloudSQLDialer", self._get_or_create_dialer(csql_cfg, is_async=False))
            return SyncMySQLDriver(config, dialer=mysql_sync_dialer, broker=broker)
        if isinstance(config, SQLiteConfig):
            return SyncSQLiteDriver(config, broker=broker)
        if isinstance(config, OracleConfig):
            return SyncOracleDriver(config, broker=broker)
        if isinstance(config, MSSQLConfig):
            return SyncMSSQLDriver(config, broker=broker)
        if isinstance(config, DuckDBConfig):
            return SyncDuckDBDriver(config, broker=broker)
        if isinstance(config, BigQueryConfig):
            return SyncBigQueryDriver(config, broker=broker)
        if isinstance(config, SpannerConfig):
            return SyncSpannerDriver(config, broker=broker)
        if isinstance(config, ValkeyConfig):
            return SyncValkeyDriver(config)
        if isinstance(config, RedisConfig):
            return SyncRedisDriver(config)
        if isinstance(config, CloudSQLConfig):
            return cast("SyncCloudSQLDialer", self._get_or_create_dialer(config, is_async=False))
        if isinstance(config, AlloyDBConfig):
            return cast("SyncAlloyDBDialer", self._get_or_create_dialer(config, is_async=False))
        msg = f"Unsupported config type: {type(config)!r}"
        raise TypeError(msg)


def provide_bigquery_driver(
    config_or_url: BigQueryConfig | str = "default",
    *,
    sync: bool = False,
    registry: DatabaseRegistry | None = None,
) -> AsyncBigQueryDriver | SyncBigQueryDriver:
    """Resolve a shared BigQuery driver from `DatabaseRegistry.default()` for framework DI."""
    reg = registry if registry is not None else DatabaseRegistry.default()
    resolved_cfg: AnyDatabaseConfig | str = (
        reg.get_config(config_or_url)
        if (isinstance(config_or_url, str) and "://" not in config_or_url and config_or_url in reg._configs)
        else config_or_url
    )
    if isinstance(resolved_cfg, str) and "://" not in resolved_cfg:
        driver = reg.get_sync_driver(resolved_cfg) if sync else reg.get_driver(resolved_cfg)
    else:
        driver = reg.get_shared_sync_driver(resolved_cfg) if sync else reg.get_shared_driver(resolved_cfg)
    return cast("AsyncBigQueryDriver | SyncBigQueryDriver", driver)


def provide_spanner_driver(
    config_or_url: SpannerConfig | str = "default",
    *,
    sync: bool = False,
    registry: DatabaseRegistry | None = None,
) -> AsyncSpannerDriver | SyncSpannerDriver:
    """Resolve a shared Cloud Spanner driver from `DatabaseRegistry.default()` for framework DI."""
    reg = registry if registry is not None else DatabaseRegistry.default()
    resolved_cfg: AnyDatabaseConfig | str = (
        reg.get_config(config_or_url)
        if (isinstance(config_or_url, str) and "://" not in config_or_url and config_or_url in reg._configs)
        else config_or_url
    )
    if isinstance(resolved_cfg, str) and "://" not in resolved_cfg:
        driver = reg.get_sync_driver(resolved_cfg) if sync else reg.get_driver(resolved_cfg)
    else:
        driver = reg.get_shared_sync_driver(resolved_cfg) if sync else reg.get_shared_driver(resolved_cfg)
    return cast("AsyncSpannerDriver | SyncSpannerDriver", driver)
