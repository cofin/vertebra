"""SQL Server (TDS 7.4) configuration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, ClassVar, Final, Self
from urllib.parse import parse_qsl, urlparse, urlunparse

import msgspec

from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.db.driver._cloud_auth import AZURE_MSSQL_SCOPE, AzureEntraTokenProvider, is_loopback_host
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.typing import ImproperConfigurationError

__all__ = ("MSSQLConfig",)

_VALID_ENCRYPTION_MODES: Final[frozenset[str]] = frozenset(
    {
        "required",
        "require",
        "true",
        "on",
        "yes",
        "mandatory",
        "optional",
        "login_only",
        "login-only",
        "prefer",
        "strict",
        "verify-full",
        "verify_full",
        "verify-ca",
        "verify_ca",
        "not_supported",
        "disabled",
        "disable",
        "false",
        "off",
        "no",
    }
)
_VERIFIED_SESSION_ENCRYPTION_MODES: Final[frozenset[str]] = frozenset(
    {
        "required",
        "require",
        "true",
        "on",
        "yes",
        "mandatory",
        "strict",
        "verify-full",
        "verify_full",
        "verify-ca",
        "verify_ca",
    }
)


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
    token_provider: Any = None
    """Optional cloud database token provider (`AzureEntraTokenProvider`) for TDS `FEDAUTH` authentication."""
    pool_min_size: int = 1
    """Minimum number of pooled TDS sessions per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled TDS sessions per worker."""
    statement_cache_size: int = 256
    """Maximum number of rewritten `@P1` statements stored in the shared-memory statement cache."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating cross-worker connection and statement leases."""

    def __post_init__(self) -> None:
        for field_name, value in (
            ("dsn", self.dsn),
            ("host", self.host),
            ("database", self.database),
            ("user", self.user),
            ("application_name", self.application_name),
            ("server_name", self.server_name),
        ):
            if value is not None and "\x00" in value:
                msg = f"MSSQLConfig.{field_name} must not contain NUL bytes"
                raise ImproperConfigurationError(msg)
        if not self.host or not self.host.strip():
            msg = "MSSQLConfig.host must not be empty"
            raise ImproperConfigurationError(msg)
        if int(self.port) < 1 or int(self.port) > 65535:
            msg = f"MSSQLConfig.port must be in 1..=65535 (got {self.port})"
            raise ImproperConfigurationError(msg)
        if self.encryption.strip().lower() not in _VALID_ENCRYPTION_MODES:
            msg = f"Unsupported MSSQLConfig.encryption mode: {self.encryption!r}"
            raise ImproperConfigurationError(msg)
        self._validate_pem_bytes(self.ca_pem, "ca_pem")
        self._validate_pem_bytes(self.client_cert_pem, "client_cert_pem")
        self._validate_pem_bytes(self.client_key_pem, "client_key_pem")
        if isinstance(self.token_provider, AzureEntraTokenProvider):
            self._validate_entra_transport_security()
            msgspec.structs.force_setattr(
                self,
                "token_provider",
                self.token_provider.with_default_scope(AZURE_MSSQL_SCOPE),
            )

    def __repr__(self) -> str:
        redacted_dsn = self._redact_dsn(self.dsn)
        provider_name = type(self.token_provider).__name__ if self.token_provider is not None else None
        return (
            f"MSSQLConfig(dsn={redacted_dsn!r}, host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r}, password='[REDACTED]', "
            f"encryption={self.encryption!r}, trust_server_certificate={self.trust_server_certificate!r}, "
            f"read_only_intent={self.read_only_intent!r}, cloud_sql_instance={self.cloud_sql_instance!r}, "
            f"token_provider={provider_name!r})"
        )

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1` placeholders into T-SQL `@P1, @P2, ...` parameters."""
        return MSSQLCore.rewrite_placeholders(sql)

    def to_cloud_dialer_config(self) -> CloudSQLConfig | None:
        """Return `CloudSQLConfig` when `cloud_sql_instance` is set."""
        if self.cloud_sql_instance:
            ip_raw = self.cloudsql_ip_type.strip().upper()
            ip_type: Any = ip_raw if ip_raw in {"PUBLIC", "PRIVATE", "PSC"} else "PUBLIC"
            return CloudSQLConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=(self.cloudsql_auth_type.strip().upper() == "IAM"),
                ip_type=ip_type,
            )
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
            raw_trust = (query_params.get("trust_server_certificate") or query_params["TrustServerCertificate"]).lower()
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
        base_key = (
            "mssql",
            self.host.lower(),
            int(self.port),
            self.database,
            self.user,
            self.cloud_sql_instance or "",
        )
        if self.token_provider is None:
            return base_key
        try:
            provider_key: Any = hash(self.token_provider)
        except TypeError:
            provider_key = id(self.token_provider)
        return (*base_key, type(self.token_provider).__qualname__, provider_key)

    def resolve_aad_token_sync(self, *, force_refresh: bool = False) -> str | None:
        """Synchronously resolve an Azure Entra ID bearer token from `token_provider`."""
        if self.token_provider is None:
            return None
        resolve_fn: Callable[..., Any] | None = getattr(self.token_provider, "resolve_sync", None)
        if not callable(resolve_fn):
            msg = "MSSQLConfig.token_provider must implement resolve_sync()"
            raise ImproperConfigurationError(msg)
        raw = resolve_fn(force_refresh=force_refresh)
        return self._validate_resolved_token(raw)

    async def resolve_aad_token_async(self, *, force_refresh: bool = False) -> str | None:
        """Asynchronously resolve an Azure Entra ID bearer token from `token_provider`."""
        if self.token_provider is None:
            return None
        resolve_fn: Callable[..., Awaitable[Any]] | None = getattr(self.token_provider, "resolve_async", None)
        if not callable(resolve_fn):
            msg = "MSSQLConfig.token_provider must implement resolve_async()"
            raise ImproperConfigurationError(msg)
        raw = await resolve_fn(force_refresh=force_refresh)
        return self._validate_resolved_token(raw)

    @staticmethod
    def _validate_resolved_token(raw: Any) -> str:
        """Validate a resolved bearer token string and reject blank or NUL-containing values."""
        if not isinstance(raw, str) or "\x00" in raw or not raw.strip():
            msg = "MSSQLConfig.token_provider must return a non-empty bearer token string"
            raise ImproperConfigurationError(msg)
        return raw.strip()

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
            registry.get_dialer(dialer_cfg, sync=False)
            if registry is not None and (dialer_cfg := self.to_cloud_dialer_config()) is not None
            else None
        )
        return async_cls(self, dialer=dialer, broker=broker)

    @staticmethod
    def _redact_dsn(dsn: str) -> str:
        """Return `dsn` with any embedded URI password replaced by `[REDACTED]`."""
        parsed = urlparse(dsn)
        if parsed.password is None:
            return dsn
        host_part = f"[{parsed.hostname}]" if parsed.hostname and ":" in parsed.hostname else (parsed.hostname or "")
        if parsed.port is not None:
            host_part = f"{host_part}:{parsed.port}"
        user_part = f"{parsed.username}:[REDACTED]@" if parsed.username is not None else ":[REDACTED]@"
        return urlunparse(parsed._replace(netloc=f"{user_part}{host_part}"))

    def _validate_entra_transport_security(self) -> None:
        """Reject settings that could expose an Entra bearer token to a remote or unverified server.

        Login-only and disabled encryption modes let the `LOGIN7` packet travel without TLS when the
        server declines encryption, and `trust_server_certificate` accepts any server certificate.
        Loopback hosts are exempt.
        """
        if is_loopback_host(self.host):
            return
        if self.encryption.strip().lower() not in _VERIFIED_SESSION_ENCRYPTION_MODES:
            msg = (
                f"AzureEntraTokenProvider requires an encrypted login on remote hosts and cannot be used "
                f"with MSSQLConfig.encryption={self.encryption!r}; use 'required' or 'strict'"
            )
            raise ImproperConfigurationError(msg)
        if self.trust_server_certificate:
            msg = (
                "AzureEntraTokenProvider requires server certificate verification on remote hosts "
                "and cannot be used with MSSQLConfig.trust_server_certificate=True"
            )
            raise ImproperConfigurationError(msg)

    @staticmethod
    def _validate_pem_bytes(pem: bytes | None, field_name: str) -> None:
        """Validate that optional PEM bytes contain PEM header and footer markers."""
        if pem is None:
            return
        if not isinstance(pem, (bytes, bytearray, memoryview)) or b"-----BEGIN " not in pem or b"-----END " not in pem:
            msg = f"Invalid PEM bundle in MSSQLConfig.{field_name}: missing PEM header/footer"
            raise ImproperConfigurationError(msg)

    @classmethod
    def _resolve_driver_config(
        cls,
        config: MSSQLConfig | str | None,
        overrides: dict[str, Any],
        dialer: Any = None,
    ) -> MSSQLConfig:
        """Resolve `config`, keyword `overrides`, and optional `dialer` into an `MSSQLConfig` instance."""
        if isinstance(config, str):
            resolved = cls.from_url(config, **overrides)
        elif isinstance(config, MSSQLConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("mssql://", "sqlserver://")):
                resolved = cls.from_url(dsn, **overrides)
            else:
                if dsn is not None:
                    overrides["dsn"] = dsn
                resolved = cls(**overrides)

        if dialer is not None and not resolved.cloud_sql_instance:
            dialer_cfg = getattr(dialer, "config", None)
            instance_name = getattr(dialer_cfg, "instance_connection_name", None) if dialer_cfg is not None else None
            if instance_name:
                ip_type = str(getattr(dialer_cfg, "ip_type", resolved.cloudsql_ip_type))
                auth_type = "IAM" if getattr(dialer_cfg, "enable_iam_auth", False) else resolved.cloudsql_auth_type
                resolved = msgspec.structs.replace(
                    resolved,
                    cloud_sql_instance=str(instance_name),
                    cloudsql_ip_type=ip_type,
                    cloudsql_auth_type=auth_type,
                )
        return resolved
