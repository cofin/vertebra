"""SQL Server (TDS 7.4) configuration."""

from __future__ import annotations

from typing import Any, ClassVar, Self
from urllib.parse import parse_qsl, urlparse

from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.db.driver._common import BaseDatabaseConfig

__all__ = ("MSSQLConfig",)


class MSSQLConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for the native SQL Server (TDS 7.4) driver."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("mssql", "sqlserver")
    driver_module: ClassVar[str] = "vertebra.db.adapters.mssql.driver"
    sync_driver_name: ClassVar[str] = "SyncMSSQLDriver"
    async_driver_name: ClassVar[str] = "AsyncMSSQLDriver"

    dsn: str = "mssql://127.0.0.1:1433/master"
    """SQL Server connection URI (`mssql://host:port/database`)."""
    host: str = "127.0.0.1"
    """SQL Server TDS endpoint hostname or IP address."""
    port: int = 1433
    """SQL Server TDS TCP port (default `1433`)."""
    database: str = "master"
    """Initial database catalog selected during TDS7 Login."""
    user: str = "sa"
    """SQL Server login username."""
    password: str = ""
    """SQL Server login password."""
    trust_server_certificate: bool = False
    """When True, skip X.509 hostname and CA chain verification during TDS Pre-Login TLS negotiation."""
    cloud_sql_instance: str | None = None
    """Optional Google Cloud SQL for SQL Server instance connection name (`project:region:instance`)."""
    pool_min_size: int = 1
    """Minimum number of pooled TDS sessions per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled TDS sessions per worker."""
    statement_cache_size: int = 256
    """Maximum number of rewritten `@P1` statements stored in the shared-memory statement cache."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating cross-worker connection and statement leases."""

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1` placeholders into T-SQL `@P1, @P2, ...` parameters."""
        return MSSQLCore.rewrite_placeholders(sql)

    @staticmethod
    def encode_sql_batch(sql: str, transaction_descriptor: int = 0) -> bytes:
        """Encode a T-SQL statement into a TDS 7.4 `SQLBatch` packet."""
        return MSSQLCore.encode_tds_sql_batch(sql, transaction_descriptor)

    def to_cloud_dialer_config(self) -> CloudSQLConfig | None:
        """Return `CloudSQLConfig` when `cloud_sql_instance` is set."""
        if self.cloud_sql_instance:
            return CloudSQLConfig(instance_connection_name=self.cloud_sql_instance)
        return None

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct an `MSSQLConfig` from an `mssql://` or `sqlserver://` URI."""
        parsed = urlparse(url)
        query_params = dict(parse_qsl(parsed.query)) if parsed.query else {}
        kwargs: dict[str, Any] = {"dsn": url}
        if parsed.hostname:
            kwargs["host"] = parsed.hostname
        if parsed.port is not None:
            kwargs["port"] = int(parsed.port)
        db_name = parsed.path.lstrip("/") if parsed.path else ""
        if db_name:
            kwargs["database"] = db_name
        if parsed.username:
            kwargs["user"] = parsed.username
        if parsed.password is not None:
            kwargs["password"] = parsed.password
        if "cloud_sql_instance" in query_params:
            kwargs["cloud_sql_instance"] = query_params["cloud_sql_instance"]
        elif "cloudsql_instance" in query_params:
            kwargs["cloud_sql_instance"] = query_params["cloudsql_instance"]
        kwargs.update(overrides)
        return cls(**kwargs)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        return (
            "mssql",
            self.host.lower(),
            int(self.port),
            self.database,
            self.user,
            self.cloud_sql_instance or "",
        )

    def build_sync_driver(self, registry: Any) -> Any:
        """Instantiate `SyncMSSQLDriver` with shared `LeaseBroker` and optional Cloud SQL dialer."""
        sync_cls, _ = self.resolve_driver_classes()
        broker = self.broker if self.broker is not None else getattr(registry, "_broker", None)
        dialer = (
            registry.get_dialer(dialer_cfg, sync=True)
            if registry is not None and (dialer_cfg := self.to_cloud_dialer_config()) is not None
            else None
        )
        return sync_cls(self, dialer=dialer, broker=broker)

    def build_async_driver(self, registry: Any) -> Any:
        """Instantiate `AsyncMSSQLDriver` with shared `LeaseBroker` and optional Cloud SQL dialer."""
        _, async_cls = self.resolve_driver_classes()
        broker = self.broker if self.broker is not None else getattr(registry, "_broker", None)
        dialer = (
            registry.get_dialer(dialer_cfg, sync=True)
            if registry is not None and (dialer_cfg := self.to_cloud_dialer_config()) is not None
            else None
        )
        return async_cls(self, dialer=dialer, broker=broker)
