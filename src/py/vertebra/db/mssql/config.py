"""SQL Server (TDS 7.4) configuration."""

from __future__ import annotations

from typing import Any, ClassVar

from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.db.driver import BaseDatabaseConfig
from vertebra.db.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)

__all__ = ("MSSQLConfig",)


class MSSQLConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for the native SQL Server (TDS 7.4) driver."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

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
    cloudsql_instance: str | None = None
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
        """Return `CloudSQLConfig` when `cloudsql_instance` is set."""
        if self.cloudsql_instance:
            return CloudSQLConfig(instance_connection_name=self.cloudsql_instance)
        return None
