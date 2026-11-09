"""SQL Server connection configuration."""

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
    """Configuration for the SQL Server driver backed by the native `vertebra._mssql` module."""

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
    application_name: str = "vertebra-mssql"
    """Application name reported in TDS7 `LOGIN7` client properties."""
    encryption: str = "required"
    """TDS encryption mode (`required`, `optional`, `not_supported`, or `strict`)."""
    trust_server_certificate: bool = False
    """When True, skip X.509 hostname and CA chain verification during TDS Pre-Login TLS negotiation."""
    ca_pem: bytes | None = None
    """Optional custom root CA certificate bundle in PEM format."""
    client_cert_pem: bytes | None = None
    """Optional mTLS client leaf certificate in PEM format."""
    client_key_pem: bytes | None = None
    """Optional mTLS client private key in PEM format."""
    server_name: str | None = None
    """Optional TLS SNI server name override."""
    connect_timeout_ms: int = 10_000
    """TCP and TDS handshake timeout in milliseconds."""
    query_timeout_ms: int = 30_000
    """Per-statement execution timeout in milliseconds before sending an out-of-band `ATTENTION` packet."""
    read_only_intent: bool = False
    """When True, set `ApplicationIntent=ReadOnly` in `LOGIN7` for Always On read-intent routing."""
    cloud_sql_instance: str | None = None
    """Optional Google Cloud SQL for SQL Server instance connection name (`project:region:instance`)."""
    cloudsql_ip_type: str = "PUBLIC"
    """Cloud SQL instance IP address type (`PUBLIC`, `PRIVATE`, or `PSC`)."""
    cloudsql_auth_type: str = "PASSWORD"
    """Cloud SQL authentication mode (`PASSWORD` or `IAM`)."""
    cloudsql_proxy_addr: str | None = None
    """Optional local address override for Cloud SQL Admin API mTLS `:3307` proxy testing."""
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
        if "application_name" in query_params or "appname" in query_params:
            kwargs["application_name"] = query_params.get("application_name") or query_params["appname"]
        if "encrypt" in query_params or "encryption" in query_params:
            kwargs["encryption"] = query_params.get("encryption") or query_params["encrypt"]
        if "trust_server_certificate" in query_params or "TrustServerCertificate" in query_params:
            raw_trust = (
                query_params.get("trust_server_certificate") or query_params["TrustServerCertificate"]
            ).lower()
            kwargs["trust_server_certificate"] = raw_trust in {"1", "true", "yes"}
        if "application_intent" in query_params or "ApplicationIntent" in query_params:
            raw_intent = (query_params.get("application_intent") or query_params["ApplicationIntent"]).lower()
            kwargs["read_only_intent"] = raw_intent in {"readonly", "read_only"}
        if "connect_timeout_ms" in query_params:
            kwargs["connect_timeout_ms"] = int(query_params["connect_timeout_ms"])
        if "query_timeout_ms" in query_params:
            kwargs["query_timeout_ms"] = int(query_params["query_timeout_ms"])
        if "cloud_sql_instance" in query_params:
            kwargs["cloud_sql_instance"] = query_params["cloud_sql_instance"]
        elif "cloudsql_instance" in query_params:
            kwargs["cloud_sql_instance"] = query_params["cloudsql_instance"]
        if "cloudsql_ip_type" in query_params:
            kwargs["cloudsql_ip_type"] = query_params["cloudsql_ip_type"]
        if "cloudsql_auth_type" in query_params:
            kwargs["cloudsql_auth_type"] = query_params["cloudsql_auth_type"]
        if "cloudsql_proxy_addr" in query_params:
            kwargs["cloudsql_proxy_addr"] = query_params["cloudsql_proxy_addr"]
        kwargs.update(overrides)
        return cls(**kwargs)

    @classmethod
    def from_dsn(cls, dsn: str, **overrides: Any) -> Self:
        """Construct an `MSSQLConfig` from an `mssql://` or `sqlserver://` DSN."""
        return cls.from_url(dsn, **overrides)

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

    def create_sync_driver(self, **kwargs: Any) -> Any:
        """Instantiate `SyncMSSQLDriver` bound to this configuration."""
        sync_cls, _ = self.resolve_driver_classes()
        return sync_cls(self, **kwargs)

    def create_async_driver(self, **kwargs: Any) -> Any:
        """Instantiate `AsyncMSSQLDriver` bound to this configuration."""
        _, async_cls = self.resolve_driver_classes()
        return async_cls(self, **kwargs)

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
