"""MySQL and MariaDB configuration supporting Cloud SQL (:3307) and direct IAM authentication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

import msgspec

from vertebra.db.adapters.mysql.core import MySQLCore
from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.db.driver import BaseDatabaseConfig
from vertebra.utils.module_loader import import_optional

__all__ = ("MySQLConfig",)


class MySQLConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for MySQL 8.0+/9.0+ and MariaDB connections."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = 2

    dsn: str = "mysql://localhost:3306/mysql"
    """MySQL or MariaDB connection URI."""
    min_size: int = 1
    """Minimum number of idle connections retained in the worker pool."""
    max_size: int = 10
    """Maximum number of concurrent connections allowed in the worker pool."""
    enable_cloud_sql: bool = False
    """When True, enable opt-in zero-sidecar Google Cloud SQL (:3307) mTLS connection dialing."""
    cloud_sql_instance: str | None = None
    """Optional Google Cloud SQL instance connection name (`project:region:instance`) for zero-sidecar port 3307 mTLS."""
    enable_iam_auth: bool = False
    """When True, authenticate using Google Cloud SQL IAM database authentication tokens (on direct :3306 TCP/TLS or :3307 dialer)."""
    iam_auth_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    """Optional `GcpTokenCache` options (`service_account_key`, `application_credentials_path`, `scopes`) for direct :3306 or :3307 IAM token resolution."""
    charset: str = "utf8mb4"
    """Connection character set negotiated during handshake."""
    autocommit: bool = False
    """When True, execute statements in autocommit mode outside explicit transactions."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` for cross-worker connection and statement leasing."""

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)
        msgspec.structs.force_setattr(self, "iam_auth_options", dict(self.iam_auth_options))

    def resolve_iam_password_sync(self, *, force_refresh: bool = False) -> str | None:
        """Resolve a short-lived Google Cloud IAM OAuth2 access token via `SyncGcpTokenCache` when `enable_iam_auth=True`."""
        if not self.enable_iam_auth:
            return None
        opts = dict(self.iam_auth_options)
        if "access_token" in opts or "bearer_token" in opts:
            opts.setdefault("allow_ambient_adc", "false")
        opts.setdefault(
            "scopes",
            "https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/cloud-platform",
        )
        native_mod = import_optional("vertebra._vertebra")
        token_cache_cls = getattr(native_mod, "SyncGcpTokenCache", None) if native_mod is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    async def resolve_iam_password_async(self, *, force_refresh: bool = False) -> str | None:
        """Asynchronously resolve a short-lived Google Cloud IAM OAuth2 access token via `AsyncGcpTokenCache` when `enable_iam_auth=True`."""
        if not self.enable_iam_auth:
            return None
        opts = dict(self.iam_auth_options)
        if "access_token" in opts or "bearer_token" in opts:
            opts.setdefault("allow_ambient_adc", "false")
        opts.setdefault(
            "scopes",
            "https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/cloud-platform",
        )
        native_mod = import_optional("vertebra._vertebra")
        token_cache_cls = getattr(native_mod, "AsyncGcpTokenCache", None) if native_mod is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = await cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    def to_cloud_dialer_config(self) -> CloudSQLConfig | None:
        """Return CloudSQLConfig when `cloud_sql_instance` is set."""
        if self.cloud_sql_instance:
            return CloudSQLConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None

    def parse_dsn_params(self) -> dict[str, Any]:
        """Parse DSN connection string into connection parameters for driver pools."""
        iam_password: str | None = None
        if self.enable_iam_auth and self.cloud_sql_instance is None:
            iam_password = self.resolve_iam_password_sync()
        return MySQLCore.parse_dsn_params(
            self.dsn,
            default_charset=self.charset,
            default_autocommit=self.autocommit,
            iam_password=iam_password,
        )
