"""MySQL and MariaDB configuration and statement compilation settings."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Self
from urllib.parse import parse_qs, unquote, urlparse, urlsplit

import msgspec

from vertebra.db.dialer import CloudSQLConfig
from vertebra.db.driver._common import MYSQL_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import import_optional

_vertebra = import_optional("vertebra._vertebra")

__all__ = (
    "MySQLConfig",
    "MySQLPoolConfig",
    "MySQLStatementConfig",
)

_ALLOWED_SSL_MODES = frozenset(
    {
        "disable",
        "disabled",
        "false",
        "0",
        "off",
        "none",
        "prefer",
        "preferred",
        "require",
        "required",
        "true",
        "1",
        "on",
        "verify-ca",
        "verify_ca",
        "verify-full",
        "verify_full",
        "verify-identity",
        "verify_identity",
    }
)

_CANONICAL_SSL_MODE_MAP: dict[str, str] = {
    "disable": "disable",
    "disabled": "disable",
    "false": "disable",
    "0": "disable",
    "off": "disable",
    "none": "disable",
    "prefer": "prefer",
    "preferred": "prefer",
    "require": "require",
    "required": "require",
    "true": "require",
    "1": "require",
    "on": "require",
    "verify-ca": "verify-ca",
    "verify_ca": "verify-ca",
    "verify-full": "verify-full",
    "verify_full": "verify-full",
    "verify-identity": "verify-full",
    "verify_identity": "verify-full",
}


class MySQLPoolConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Pool configuration for MySQL and MariaDB connections."""

    min_size: int = 1
    max_size: int = 10
    max_lifetime: float = 1800.0
    idle_timeout: float = 300.0
    acquire_timeout: float = 30.0


class MySQLStatementConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Statement compilation configuration for MySQL and MariaDB (`?` / `%s` binds)."""

    dialect: str = "mysql"
    param_style: str = "qmark"
    statement_cache_size: int = 128


class MySQLConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for `SyncMySQLDriver` and `AsyncMySQLDriver`."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = MYSQL_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = (
        "mysql",
        "mariadb",
        "mysql+pymysql",
        "mysql+asyncmy",
        "mysql+vtb",
        "mysql+vertebra",
        "mysql+vtb_async",
        "mysql+vertebra_async",
    )
    driver_module: ClassVar[str] = "vertebra.db.adapters.mysql.driver"
    sync_driver_name: ClassVar[str] = "SyncMySQLDriver"
    async_driver_name: ClassVar[str] = "AsyncMySQLDriver"

    dsn: str = "mysql://localhost:3306/vertebra"
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "vertebra"
    unix_socket: str | None = None
    charset: str = "utf8mb4"
    autocommit: bool = True
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    ssl_mode: str = "prefer"
    ssl_ca: str | None = None
    ssl_cert: str | None = None
    ssl_key: str | None = None
    init_command: str | None = None
    min_size: int = 1
    max_size: int = 10
    statement_cache_size: int = 128
    cloud_sql_instance: str | None = None
    cloud_sql_ip_type: str = "PUBLIC"
    enable_iam_auth: bool = False
    iam_auth_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    credentials_path: str | None = None
    credentials_json: str | None = None
    target_service_account: str | None = None
    pool: MySQLPoolConfig = msgspec.field(default_factory=MySQLPoolConfig)
    statement: MySQLStatementConfig = msgspec.field(default_factory=MySQLStatementConfig)
    broker: Any = None

    def __post_init__(self) -> None:
        """Validate MySQL connection parameters, port bounds, NUL bytes, and TLS settings."""
        for field_name, val in (
            ("dsn", self.dsn),
            ("host", self.host),
            ("user", self.user),
            ("database", self.database),
            ("unix_socket", self.unix_socket),
            ("ssl_ca", self.ssl_ca),
            ("ssl_cert", self.ssl_cert),
            ("ssl_key", self.ssl_key),
            ("init_command", self.init_command),
            ("cloud_sql_instance", self.cloud_sql_instance),
        ):
            if val is not None and "\x00" in val:
                msg = f"MySQL {field_name} must not contain NUL bytes"
                raise ImproperConfigurationError(msg)

        trimmed_dsn = self.dsn.strip()
        lower_dsn = trimmed_dsn.lower()
        if not trimmed_dsn:
            msg = "MySQL dsn must not be empty"
            raise ImproperConfigurationError(msg)

        if (
            lower_dsn in {"memory", ":memory:"}
            or lower_dsn.startswith("memory://")
            or "://memory" in lower_dsn
            or "/:memory:" in lower_dsn
            or self.database.strip().lower() in {"memory", ":memory:"}
        ):
            msg = (
                "MySQL does not support in-memory ('memory://') execution; configure a MySQL or MariaDB server endpoint"
            )
            raise ImproperConfigurationError(msg)

        if not (1 <= self.port <= 65535):
            msg = f"MySQL port must be in 1..65535, got {self.port}"
            raise ImproperConfigurationError(msg)

        mode_key = self.ssl_mode.strip().lower()
        if mode_key not in _ALLOWED_SSL_MODES:
            msg = (
                f"Unsupported MySQL ssl_mode '{self.ssl_mode}'; "
                "expected disable, prefer, require, verify-ca, or verify-full"
            )
            raise ImproperConfigurationError(msg)

        if self.connect_timeout <= 0 or self.read_timeout <= 0 or self.write_timeout <= 0:
            msg = "MySQL timeouts must be positive"
            raise ImproperConfigurationError(msg)

        if self.min_size < 1 or self.max_size < self.min_size:
            msg = f"Invalid MySQL pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ImproperConfigurationError(msg)

        if self.statement_cache_size < 0:
            msg = "MySQL statement_cache_size must be non-negative"
            raise ImproperConfigurationError(msg)

        ip_upper = self.cloud_sql_ip_type.strip().upper()
        if ip_upper not in {"PUBLIC", "PRIVATE", "PSC"}:
            msg = f"Unsupported MySQL cloud_sql_ip_type '{self.cloud_sql_ip_type}'"
            raise ImproperConfigurationError(msg)

        if self.cloud_sql_instance is not None:
            parts = self.cloud_sql_instance.strip().split(":")
            if len(parts) != 3 or not all(parts):
                msg = f"Invalid Cloud SQL instance '{self.cloud_sql_instance}'; expected 'project:region:instance'"
                raise ImproperConfigurationError(msg)

        if "://" in trimmed_dsn and not lower_dsn.startswith("cloudsql://"):
            parsed = urlsplit(trimmed_dsn)
            try:
                dsn_port = parsed.port
            except ValueError as exc:
                msg = f"Invalid port in MySQL DSN '{trimmed_dsn}': {exc}"
                raise ImproperConfigurationError(msg) from exc
            if dsn_port is not None and not (1 <= dsn_port <= 65535):
                msg = f"MySQL DSN port must be in 1..65535, got {dsn_port}"
                raise ImproperConfigurationError(msg)
            query_params = parse_qs(parsed.query)
            for ssl_param in ("ssl_mode", "ssl-mode", "sslmode", "ssl"):
                if ssl_param in query_params:
                    q_mode = query_params[ssl_param][0].strip().lower()
                    if q_mode not in _ALLOWED_SSL_MODES:
                        msg = f"Unsupported MySQL {ssl_param} '{query_params[ssl_param][0]}'"
                        raise ImproperConfigurationError(msg)

    @property
    def canonical_ssl_mode(self) -> str:
        """Return the normalized `ssl_mode` string (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`)."""
        return _CANONICAL_SSL_MODE_MAP[self.ssl_mode.strip().lower()]

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Parse a `mysql://` or `mariadb://` URL into a `MySQLConfig` instance."""
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        kwargs: dict[str, Any] = dict(overrides)
        for ssl_key in ("ssl_mode", "ssl-mode", "sslmode", "ssl"):
            if ssl_key in query and "ssl_mode" not in kwargs:
                kwargs["ssl_mode"] = query[ssl_key][0]
                break
        if "unix_socket" in query and "unix_socket" not in kwargs:
            kwargs["unix_socket"] = query["unix_socket"][0]
        elif "socket" in query and "unix_socket" not in kwargs:
            kwargs["unix_socket"] = query["socket"][0]
        for csql_key in ("cloud_sql_instance", "cloudsql_instance"):
            if csql_key in query and "cloud_sql_instance" not in kwargs:
                kwargs["cloud_sql_instance"] = query[csql_key][0]
                break
        if "enable_iam_auth" in query and "enable_iam_auth" not in kwargs:
            kwargs["enable_iam_auth"] = query["enable_iam_auth"][0].lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        if "charset" in query and "charset" not in kwargs:
            kwargs["charset"] = query["charset"][0]
        for cache_key in ("statement_cache_size", "statement_cache_capacity"):
            if cache_key in query and "statement_cache_size" not in kwargs:
                kwargs["statement_cache_size"] = int(query[cache_key][0])
                break
        if "autocommit" in query and "autocommit" not in kwargs:
            kwargs["autocommit"] = query["autocommit"][0].lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        return cls(dsn=url, **kwargs)

    def pool_key(self) -> tuple[Any, ...]:
        """Return a deterministic pool identity tuple for `DatabaseRegistry`."""
        parsed = urlparse(self.dsn)
        has_dsn = bool(self.dsn and self.dsn != "mysql://localhost:3306/vertebra")
        host = (parsed.hostname or self.host) if has_dsn else self.host
        port = (parsed.port or self.port) if has_dsn else self.port
        db = (parsed.path.lstrip("/") or self.database) if has_dsn else self.database
        user = (unquote(parsed.username) if parsed.username else self.user) if has_dsn else self.user
        return (
            self.dialect,
            host,
            port,
            db,
            user,
            self.canonical_ssl_mode,
            self.cloud_sql_instance or "",
        )

    def parse_dsn_params(self) -> dict[str, Any]:
        """Extract normalized connection parameters from `dsn` and fields."""
        parsed = urlparse(self.dsn)
        has_dsn = bool(self.dsn and self.dsn != "mysql://localhost:3306/vertebra")
        query = parse_qs(parsed.query)
        host = (parsed.hostname or self.host) if has_dsn else self.host
        port = (parsed.port or self.port) if has_dsn else self.port
        user = (unquote(parsed.username) if parsed.username else self.user) if has_dsn else self.user
        password = (
            (unquote(parsed.password) if parsed.password is not None else self.password) if has_dsn else self.password
        )
        database = (parsed.path.lstrip("/") or self.database) if has_dsn else self.database
        if self.enable_iam_auth:
            iam_token = self.resolve_iam_password_sync()
            if iam_token:
                password = iam_token
        ssl_mode = self.canonical_ssl_mode
        for ssl_key in ("ssl_mode", "ssl-mode", "sslmode", "ssl"):
            if ssl_key in query:
                ssl_mode = _CANONICAL_SSL_MODE_MAP.get(
                    query[ssl_key][0].strip().lower(),
                    self.canonical_ssl_mode,
                )
                break
        unix_socket = self.unix_socket
        if unix_socket is None and "unix_socket" in query:
            unix_socket = query["unix_socket"][0]
        elif unix_socket is None and "socket" in query:
            unix_socket = query["socket"][0]
        return {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "database": database,
            "charset": query.get("charset", [self.charset])[0],
            "autocommit": self.autocommit,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
            "write_timeout": self.write_timeout,
            "ssl_mode": ssl_mode,
            "unix_socket": unix_socket,
            "cloud_sql_instance": self.cloud_sql_instance,
            "statement_cache_size": self.statement_cache_size,
        }

    async def parse_dsn_params_async(self) -> dict[str, Any]:
        """Extract normalized connection parameters asynchronously, refreshing IAM tokens if enabled."""
        params = self.parse_dsn_params()
        if self.enable_iam_auth:
            iam_token = await self.resolve_iam_password_async()
            if iam_token:
                params["password"] = iam_token
        return params

    def to_cloud_dialer_config(self) -> CloudSQLConfig | None:
        """Return a `CloudSQLConfig` when `cloud_sql_instance` is set, else `None`."""
        if not self.cloud_sql_instance:
            return None
        return CloudSQLConfig(
            instance_connection_name=self.cloud_sql_instance,
            enable_iam_auth=self.enable_iam_auth,
        )

    def _build_iam_options(self) -> dict[str, str]:
        opts = dict(self.iam_auth_options)
        if self.credentials_path and "credentials_path" not in opts:
            opts["credentials_path"] = self.credentials_path
        if self.credentials_json and "credentials_json" not in opts:
            opts["credentials_json"] = self.credentials_json
        if self.target_service_account and "target_service_account" not in opts:
            opts["target_service_account"] = self.target_service_account
        if "access_token" in opts or "bearer_token" in opts:
            opts.setdefault("allow_ambient_adc", "false")
        opts.setdefault(
            "scopes",
            "https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/cloud-platform",
        )
        return opts

    def resolve_iam_password_sync(self, *, force_refresh: bool = False) -> str | None:
        """Resolve a Cloud SQL IAM OAuth2 token synchronously via `SyncGcpTokenCache`."""
        if not self.enable_iam_auth:
            return None
        opts = self._build_iam_options()
        token_cache_cls = getattr(_vertebra, "SyncGcpTokenCache", None) if _vertebra is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    async def resolve_iam_password_async(self, *, force_refresh: bool = False) -> str | None:
        """Resolve a Cloud SQL IAM OAuth2 token asynchronously via `AsyncGcpTokenCache`."""
        if not self.enable_iam_auth:
            return None
        opts = self._build_iam_options()
        token_cache_cls = getattr(_vertebra, "AsyncGcpTokenCache", None) if _vertebra is not None else None
        if token_cache_cls is None:
            return opts.get("access_token") or opts.get("bearer_token")
        cache = token_cache_cls(opts)
        snap = await cache.get_token(force_refresh=force_refresh)
        return str(snap.access_token)

    def build_sync_driver(self, registry: Any) -> Any:
        """Build a `SyncMySQLDriver` wired with the registry's shared Cloud SQL dialer and broker."""
        from vertebra.db.adapters.mysql.driver import SyncMySQLDriver

        dialer_cfg = self.to_cloud_dialer_config()
        dialer = registry.get_dialer(dialer_cfg, sync=True) if dialer_cfg is not None else None
        return SyncMySQLDriver(self, dialer=dialer, broker=self.broker)

    def build_async_driver(self, registry: Any) -> Any:
        """Build an `AsyncMySQLDriver` wired with the registry's shared Cloud SQL dialer and broker."""
        from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver

        dialer_cfg = self.to_cloud_dialer_config()
        dialer = registry.get_dialer(dialer_cfg, sync=False) if dialer_cfg is not None else None
        return AsyncMySQLDriver(self, dialer=dialer, broker=self.broker)

    def __repr__(self) -> str:
        """Return a redacted representation that omits credentials and tokens."""
        return (
            f"MySQLConfig(host={self.host!r}, port={self.port}, "
            f"user={self.user!r}, database={self.database!r}, "
            f"ssl_mode={self.canonical_ssl_mode!r}, password='***')"
        )
