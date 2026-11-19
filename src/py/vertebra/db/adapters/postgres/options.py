"""Libpq-compatible PostgreSQL DSN/PG* option resolution into native client kwargs."""

from __future__ import annotations

import getpass
import ipaddress
import os
import posixpath
import re
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import msgspec

from vertebra.db.dialer import aws_rds_root_ca_pem
from vertebra.db.driver._cloud_auth import (
    AZURE_POSTGRES_SCOPE,
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.config import PostgreSQLConfig

__all__ = (
    "ParsedPostgresDsn",
    "PostgresClientOptions",
    "PostgresTarget",
    "build_native_client_options",
    "is_postgres_auth_rejection",
    "is_postgres_loopback_host",
    "merge_postgres_options",
    "normalize_pem_bytes",
    "parse_libpq_conninfo",
    "parse_postgres_dsn",
    "parse_query_pairs_strict",
    "validate_cockroach_cluster",
)

_CLOUD_TLS_TOKEN_PROVIDERS = (
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
)

_VALID_SSLMODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
_VALID_CHANNEL_BINDINGS = frozenset({"prefer", "require", "disable"})
_VALID_POOLER_MODES = frozenset({"direct", "transaction", "session", "statement", "auto"})
_VALID_CLOUD_IP_TYPES = frozenset({"PUBLIC", "PRIVATE", "PSC"})
_BOOL_TRUE = frozenset({"1", "true", "yes", "on"})
_BOOL_FALSE = frozenset({"0", "false", "no", "off"})


class ParsedPostgresDsn(msgspec.Struct, frozen=True):
    """Structured decomposition of either a PostgreSQL URI or libpq conninfo string."""

    scheme: str | None = None
    hosts: tuple[str, ...] = ()
    ports: tuple[int | None, ...] = ()
    user: str | None = None
    password: str | None = None
    database: str | None = None
    explicit_empty_user: bool = False
    explicit_empty_database: bool = False
    params: dict[str, str] = msgspec.field(default_factory=dict)


class PostgresTarget(msgspec.Struct, frozen=True, gc=False):
    """Normalized PostgreSQL endpoint target (TCP or Unix domain socket)."""

    host: str
    port: int
    unix_socket: str | None = None
    is_unix: bool = False
    sslmode: str = "prefer"


class PostgresClientOptions(msgspec.Struct, frozen=True):
    """Validated kwargs container matching ``SyncPostgresClient`` / ``AsyncPostgresClient`` / Pools."""

    user: str
    database: str
    host: str
    port: int
    password: str | None
    unix_socket: str | None
    sslmode: str
    ca_pem: bytes | None
    client_cert_pem: bytes | None
    client_key_pem: bytes | None
    server_name: str | None
    channel_binding: str
    connect_timeout: float
    application_name: str
    session_gucs: dict[str, str]
    keepalives: bool
    keepalives_idle: int | None
    keepalives_interval: int | None
    keepalives_count: int | None
    cloud_sql_instance: str | None
    cloud_sql_ip_type: str
    alloydb_instance_uri: str | None
    alloydb_ip_type: str
    enable_iam_auth: bool
    cloud_auth_options: dict[str, str]
    iam_auth_options: dict[str, str]
    pooler_mode: str
    protocol_level_pooler_cache: bool
    statement_cache_size: int
    min_size: int
    max_size: int
    acquire_timeout: float
    reset_timeout: float
    close_timeout: float
    targets: tuple[PostgresTarget, ...] = ()

    def for_target(self, index: int = 0) -> PostgresClientOptions:
        """Return a copy bound to multi-host endpoint candidate ``index``."""
        if not self.targets:
            return self
        target = self.targets[index]
        return PostgresClientOptions(
            user=self.user,
            database=self.database,
            host=target.host,
            port=target.port,
            password=self.password,
            unix_socket=target.unix_socket,
            sslmode=target.sslmode,
            ca_pem=self.ca_pem,
            client_cert_pem=self.client_cert_pem,
            client_key_pem=self.client_key_pem,
            server_name=self.server_name,
            channel_binding=self.channel_binding,
            connect_timeout=self.connect_timeout,
            application_name=self.application_name,
            session_gucs=dict(self.session_gucs),
            keepalives=self.keepalives,
            keepalives_idle=self.keepalives_idle,
            keepalives_interval=self.keepalives_interval,
            keepalives_count=self.keepalives_count,
            cloud_sql_instance=self.cloud_sql_instance,
            cloud_sql_ip_type=self.cloud_sql_ip_type,
            alloydb_instance_uri=self.alloydb_instance_uri,
            alloydb_ip_type=self.alloydb_ip_type,
            enable_iam_auth=self.enable_iam_auth,
            cloud_auth_options=dict(self.cloud_auth_options),
            iam_auth_options=dict(self.iam_auth_options),
            pooler_mode=self.pooler_mode,
            protocol_level_pooler_cache=self.protocol_level_pooler_cache,
            statement_cache_size=self.statement_cache_size,
            min_size=self.min_size,
            max_size=self.max_size,
            acquire_timeout=self.acquire_timeout,
            reset_timeout=self.reset_timeout,
            close_timeout=self.close_timeout,
            targets=self.targets,
        )

    def to_client_kwargs(self) -> dict[str, Any]:
        """Return kwargs accepted directly by ``SyncPostgresClient`` and ``AsyncPostgresClient``."""
        return {
            "user": self.user,
            "database": self.database,
            "host": self.host,
            "port": self.port,
            "password": self.password,
            "unix_socket": self.unix_socket,
            "sslmode": self.sslmode,
            "ca_pem": self.ca_pem,
            "client_cert_pem": self.client_cert_pem,
            "client_key_pem": self.client_key_pem,
            "server_name": self.server_name,
            "channel_binding": self.channel_binding,
            "connect_timeout": self.connect_timeout,
            "application_name": self.application_name,
            "session_gucs": dict(self.session_gucs),
            "keepalives": self.keepalives,
            "keepalives_idle": self.keepalives_idle,
            "keepalives_interval": self.keepalives_interval,
            "keepalives_count": self.keepalives_count,
            "cloud_sql_instance": self.cloud_sql_instance,
            "cloud_sql_ip_type": self.cloud_sql_ip_type,
            "alloydb_instance_uri": self.alloydb_instance_uri,
            "alloydb_ip_type": self.alloydb_ip_type,
            "enable_iam_auth": self.enable_iam_auth,
            "cloud_auth_options": dict(self.cloud_auth_options),
            "iam_auth_options": dict(self.iam_auth_options),
        }

    def to_pool_kwargs(self) -> dict[str, Any]:
        """Return kwargs accepted directly by ``SyncPostgresPool`` and ``AsyncPostgresPool``."""
        kwargs = self.to_client_kwargs()
        kwargs.update(
            {
                "pooler_mode": self.pooler_mode,
                "protocol_level_pooler_cache": self.protocol_level_pooler_cache,
                "statement_cache_size": self.statement_cache_size,
                "min_size": self.min_size,
                "max_size": self.max_size,
                "acquire_timeout": self.acquire_timeout,
                "reset_timeout": self.reset_timeout,
                "close_timeout": self.close_timeout,
            }
        )
        return kwargs


def validate_cockroach_cluster(cluster: str) -> str:
    """Validate a CockroachDB cluster routing identifier (`--cluster=<id>`)."""
    if not isinstance(cluster, str) or not cluster or any(ch.isspace() or ch == "\0" for ch in cluster):
        raise ImproperConfigurationError("cockroach_cluster must be a non-empty string without whitespace or NUL bytes")
    return cluster


def merge_postgres_options(*raw_options: str | None, cockroach_cluster: str | None = None) -> str | None:
    """Merge PostgreSQL startup `options` flags with an optional CockroachDB `--cluster=<id>` token."""
    validated_cluster = validate_cockroach_cluster(cockroach_cluster) if cockroach_cluster is not None else None
    chunks: list[str] = []
    for raw in raw_options:
        if raw is None:
            continue
        if "\0" in raw:
            raise ImproperConfigurationError("PostgreSQL options must not contain NUL bytes")
        cleaned = raw.strip()
        if not cleaned:
            continue
        if validated_cluster is not None:
            cleaned = re.sub(r"(?:^|\s)--cluster=\S+", " ", cleaned).strip()
        if cleaned:
            chunks.append(cleaned)
    if validated_cluster is not None:
        chunks.append(f"--cluster={validated_cluster}")
    merged = " ".join(chunks).strip()
    return merged or None


def is_postgres_auth_rejection(exc: BaseException) -> bool:
    """Return True when an exception represents a PostgreSQL authentication or token rejection."""
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate in ("28P01", "28000"):
        return True
    detail = str(exc).lower()
    return (
        "28p01" in detail
        or "28000" in detail
        or "password authentication failed" in detail
        or "authentication failed" in detail
    )


def parse_query_pairs_strict(query: str) -> dict[str, str]:
    """Parse URI query parameters preserving literal ``+`` characters."""
    params: dict[str, str] = {}
    if not query:
        return params
    for part in query.split("&"):
        if not part:
            continue
        if "=" in part:
            raw_key, raw_val = part.split("=", 1)
        else:
            raw_key, raw_val = part, ""
        key = _unquote_strict(raw_key, field_name="query key")
        val = _unquote_strict(raw_val, field_name="query value")
        params[key] = val
    return params


def parse_libpq_conninfo(conninfo: str) -> dict[str, str]:
    """Parse a libpq keyword=value connection string respecting single quotes and escapes."""
    params: dict[str, str] = {}
    n = len(conninfo)
    i = 0
    while i < n:
        while i < n and conninfo[i].isspace():
            i += 1
        if i >= n:
            break
        key_start = i
        while i < n and conninfo[i] not in ("=", " ", "\t", "\n", "\r"):
            i += 1
        key = conninfo[key_start:i]
        while i < n and conninfo[i].isspace():
            i += 1
        if i >= n or conninfo[i] != "=" or not key:
            raise ImproperConfigurationError(f"Malformed libpq keyword-value parameter near {conninfo[key_start:]!r}")
        i += 1
        while i < n and conninfo[i].isspace():
            i += 1
        if i < n and conninfo[i] == "'":
            i += 1
            chars: list[str] = []
            closed = False
            while i < n:
                ch = conninfo[i]
                if ch == "\\":
                    i += 1
                    if i >= n:
                        raise ImproperConfigurationError("Unterminated escape in quoted libpq conninfo value")
                    chars.append(conninfo[i])
                    i += 1
                elif ch == "'":
                    closed = True
                    i += 1
                    break
                else:
                    chars.append(ch)
                    i += 1
            if not closed:
                raise ImproperConfigurationError(f"Unterminated single-quoted value for libpq parameter {key!r}")
            params[key] = "".join(chars)
        else:
            val_start = i
            while i < n and not conninfo[i].isspace():
                if conninfo[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                i += 1
            raw_val = conninfo[val_start:i]
            params[key] = raw_val.replace("\\\\", "\\").replace("\\'", "'")
    return params


def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn:
    """Parse a ``postgresql://``/``postgres://`` URI or libpq keyword-value string."""
    stripped = dsn.strip()
    if not stripped:
        raise ImproperConfigurationError("PostgreSQL DSN cannot be empty")
    if "://" in stripped:
        scheme, remainder = stripped.split("://", 1)
        scheme_lower = scheme.lower()
        if scheme_lower not in {"postgresql", "postgres"}:
            raise ImproperConfigurationError(f"Unsupported PostgreSQL DSN scheme: {scheme!r}")
        if "?" in remainder:
            path_and_auth, query = remainder.split("?", 1)
        else:
            path_and_auth, query = remainder, ""
        params = parse_query_pairs_strict(query)
        if "/" in path_and_auth:
            authority, raw_path = path_and_auth.split("/", 1)
        else:
            authority, raw_path = path_and_auth, ""

        user: str | None = None
        password: str | None = None
        explicit_empty_user = False
        if "@" in authority:
            userinfo, host_section = authority.rsplit("@", 1)
            if ":" in userinfo:
                raw_user, raw_password = userinfo.split(":", 1)
                password = _unquote_strict(raw_password, field_name="password")
            else:
                raw_user = userinfo
            decoded_user = _unquote_strict(raw_user, field_name="user")
            if raw_user != "" or decoded_user != "":
                user = decoded_user
        else:
            host_section = authority

        host_pairs = _split_authority_hosts(host_section)
        hosts = tuple(pair[0] for pair in host_pairs)
        ports = tuple(pair[1] for pair in host_pairs)

        database: str | None = None
        if raw_path:
            decoded_db = _unquote_strict(raw_path, field_name="database")
            if decoded_db:
                database = decoded_db

        explicit_empty_database = False
        if "user" in params:
            if params["user"] == "":
                explicit_empty_user = True
            else:
                user = params["user"]
        if "password" in params:
            password = params["password"]
        db_param_key = "dbname" if "dbname" in params else ("database" if "database" in params else None)
        if db_param_key is not None:
            if params[db_param_key] == "":
                explicit_empty_database = True
            else:
                database = params[db_param_key]

        return ParsedPostgresDsn(
            scheme=scheme_lower,
            hosts=hosts,
            ports=ports,
            user=user,
            password=password,
            database=database,
            explicit_empty_user=explicit_empty_user,
            explicit_empty_database=explicit_empty_database,
            params=params,
        )

    params = parse_libpq_conninfo(stripped)
    user = params.get("user")
    explicit_empty_user = "user" in params and params["user"] == ""
    db_val = params["dbname"] if "dbname" in params else params.get("database")
    explicit_empty_database = ("dbname" in params and params["dbname"] == "") or (
        "database" in params and params["database"] == ""
    )
    kv_hosts: tuple[str, ...] = ()
    kv_ports: tuple[int | None, ...] = ()
    if "host" in params and params["host"] != "":
        kv_hosts = tuple(item for item in params["host"].split(",") if item)
    elif "hostaddr" in params and params["hostaddr"] != "":
        kv_hosts = tuple(item for item in params["hostaddr"].split(",") if item)
    if "port" in params and params["port"] != "":
        kv_ports = tuple(_parse_int(p, field_name="port", minimum=1, maximum=65535) for p in params["port"].split(","))
    return ParsedPostgresDsn(
        scheme=None,
        hosts=kv_hosts,
        ports=kv_ports,
        user=None if explicit_empty_user else user,
        password=params.get("password"),
        database=None if explicit_empty_database else db_val,
        explicit_empty_user=explicit_empty_user,
        explicit_empty_database=explicit_empty_database,
        params=params,
    )


def build_native_client_options(
    config: PostgreSQLConfig,
    *,
    dsn: str | None = None,
    env: Mapping[str, str] | None = None,
    **overrides: Any,
) -> PostgresClientOptions:
    """Resolve `PostgreSQLConfig`, DSN/conninfo, and `PG*` env into validated native kwargs."""
    active_env = os.environ if env is None else env
    raw_dsn = dsn if dsn is not None else config.dsn
    parsed = parse_postgres_dsn(raw_dsn)

    explicit: dict[str, Any] = dict(overrides)
    if "user" in explicit and explicit["user"] == "":
        raise ImproperConfigurationError("user cannot be an empty string")
    if parsed.explicit_empty_user and "user" not in explicit:
        raise ImproperConfigurationError("DSN user cannot be an empty string")
    if ("database" in explicit and explicit["database"] == "") or ("dbname" in explicit and explicit["dbname"] == ""):
        raise ImproperConfigurationError("database cannot be an empty string")
    if parsed.explicit_empty_database and "database" not in explicit and "dbname" not in explicit:
        raise ImproperConfigurationError("DSN dbname/database cannot be an empty string")

    user_val = _resolve_scalar(
        explicit,
        parsed.params,
        {},
        explicit_keys=("user",),
        dsn_keys=("user",),
    )
    if user_val is None:
        user_val = parsed.user
    if user_val is None and active_env.get("PGUSER"):
        user_val = active_env["PGUSER"]
    if user_val is None:
        user_val = _resolve_os_username()
    user_str = str(user_val)
    if not user_str:
        raise ImproperConfigurationError("PostgreSQL user cannot be empty")

    db_val = _resolve_scalar(
        explicit,
        parsed.params,
        {},
        explicit_keys=("database", "dbname"),
        dsn_keys=("dbname", "database"),
    )
    if db_val is None:
        db_val = parsed.database
    if db_val is None and active_env.get("PGDATABASE"):
        db_val = active_env["PGDATABASE"]
    if db_val is None:
        db_val = user_str
    db_str = str(db_val)
    if not db_str:
        raise ImproperConfigurationError("PostgreSQL database cannot be empty")

    password_val = _resolve_scalar(
        explicit,
        parsed.params,
        {},
        explicit_keys=("password",),
        dsn_keys=("password",),
    )
    if password_val is None:
        password_val = parsed.password
    if password_val is None and active_env.get("PGPASSWORD"):
        password_val = active_env["PGPASSWORD"]
    password_str = None if password_val is None else str(password_val)

    explicit_sslmode = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("sslmode",),
        dsn_keys=("sslmode",),
        env_key="PGSSLMODE",
    )
    if explicit_sslmode is not None:
        explicit_sslmode = str(explicit_sslmode).strip().lower()
        if explicit_sslmode not in _VALID_SSLMODES:
            raise ImproperConfigurationError(f"Unsupported PostgreSQL sslmode: {explicit_sslmode!r}")

    targets = _resolve_targets(
        parsed,
        explicit,
        active_env,
        explicit_sslmode=explicit_sslmode,
    )

    token_provider = explicit.get("token_provider", config.token_provider)
    if isinstance(token_provider, AzureEntraTokenProvider):
        token_provider = token_provider.with_default_scope(AZURE_POSTGRES_SCOPE)

    has_remote_target = False
    resolved_targets: list[PostgresTarget] = []
    for target in targets:
        is_remote_target = not target.is_unix and not is_postgres_loopback_host(target.host)
        if is_remote_target:
            has_remote_target = True
            if isinstance(token_provider, _CLOUD_TLS_TOKEN_PROVIDERS) and target.sslmode == "disable":
                raise ImproperConfigurationError(
                    f"{type(token_provider).__name__} requires TLS on remote hosts "
                    "and cannot be used with sslmode='disable'"
                )
            if isinstance(token_provider, AwsRdsIamTokenProvider) and explicit_sslmode is None:
                target = msgspec.structs.replace(target, sslmode="verify-full")
        resolved_targets.append(target)
    targets = tuple(resolved_targets)
    primary = targets[0]

    if password_str is None and token_provider is not None:
        password_str = str(token_provider.resolve_sync())

    channel_binding_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("channel_binding",),
        dsn_keys=("channel_binding",),
        env_key="PGCHANNELBINDING",
    )
    channel_binding = str(channel_binding_raw).strip().lower() if channel_binding_raw is not None else "prefer"
    if channel_binding not in _VALID_CHANNEL_BINDINGS:
        raise ImproperConfigurationError(f"Unsupported PostgreSQL channel_binding policy: {channel_binding!r}")
    if primary.sslmode == "disable" and channel_binding == "require":
        raise ImproperConfigurationError("channel_binding='require' is incompatible with sslmode='disable'")

    ca_pem: bytes | None = None
    if explicit.get("ca_pem") is not None:
        ca_pem = normalize_pem_bytes(explicit["ca_pem"], field_name="ca_pem")
    else:
        ca_path = _resolve_scalar(
            explicit,
            parsed.params,
            {},
            explicit_keys=("sslrootcert",),
            dsn_keys=("sslrootcert", "ssl_ca", "sslca"),
        )
        if ca_path is not None:
            ca_pem = _read_pem_file(str(ca_path), field_name="sslrootcert")
        elif config.ca_pem is not None:
            ca_pem = normalize_pem_bytes(config.ca_pem, field_name="ca_pem")
        elif active_env.get("PGSSLROOTCERT"):
            ca_pem = _read_pem_file(str(active_env["PGSSLROOTCERT"]), field_name="sslrootcert")

    if ca_pem is None and has_remote_target and isinstance(token_provider, AwsRdsIamTokenProvider):
        ca_pem = aws_rds_root_ca_pem().encode("utf-8")

    client_cert_pem: bytes | None = explicit.get("client_cert_pem")
    if client_cert_pem is None:
        cert_path = _resolve_scalar(
            explicit,
            parsed.params,
            active_env,
            explicit_keys=("sslcert",),
            dsn_keys=("sslcert", "ssl_cert"),
            env_key="PGSSLCERT",
        )
        if cert_path is not None:
            client_cert_pem = _read_pem_file(str(cert_path), field_name="sslcert")

    client_key_pem: bytes | None = explicit.get("client_key_pem")
    if client_key_pem is None:
        key_path = _resolve_scalar(
            explicit,
            parsed.params,
            active_env,
            explicit_keys=("sslkey",),
            dsn_keys=("sslkey", "ssl_key"),
            env_key="PGSSLKEY",
        )
        if key_path is not None:
            client_key_pem = _read_pem_file(str(key_path), field_name="sslkey")

    server_name_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("server_name",),
        dsn_keys=("server_name", "sslservername"),
    )
    server_name = str(server_name_raw) if server_name_raw is not None else None

    connect_timeout_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("connect_timeout",),
        dsn_keys=("connect_timeout",),
        env_key="PGCONNECT_TIMEOUT",
    )
    connect_timeout = (
        _parse_float(connect_timeout_raw, field_name="connect_timeout", positive=True)
        if connect_timeout_raw is not None
        else 10.0
    )

    app_name_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("application_name",),
        dsn_keys=("application_name",),
        env_key="PGAPPNAME",
    )
    application_name = str(app_name_raw) if app_name_raw is not None else config.application_name

    keepalives_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("keepalives",),
        dsn_keys=("keepalives",),
        env_key="PGKEEPALIVES",
    )
    keepalives = _parse_bool(keepalives_raw, field_name="keepalives") if keepalives_raw is not None else True

    keepalives_idle_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("keepalives_idle",),
        dsn_keys=("keepalives_idle",),
        env_key="PGKEEPALIVES_IDLE",
    )
    keepalives_idle = (
        _parse_int(keepalives_idle_raw, field_name="keepalives_idle", minimum=0)
        if keepalives_idle_raw is not None
        else None
    )

    keepalives_interval_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("keepalives_interval",),
        dsn_keys=("keepalives_interval",),
        env_key="PGKEEPALIVES_INTERVAL",
    )
    keepalives_interval = (
        _parse_int(keepalives_interval_raw, field_name="keepalives_interval", minimum=0)
        if keepalives_interval_raw is not None
        else None
    )

    keepalives_count_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("keepalives_count",),
        dsn_keys=("keepalives_count",),
        env_key="PGKEEPALIVES_COUNT",
    )
    keepalives_count = (
        _parse_int(keepalives_count_raw, field_name="keepalives_count", minimum=0)
        if keepalives_count_raw is not None
        else None
    )

    pgbouncer_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("pgbouncer",),
        dsn_keys=("pgbouncer",),
    )
    pgbouncer_enabled = (
        _parse_bool(pgbouncer_raw, field_name="pgbouncer")
        if pgbouncer_raw is not None
        else (config.pgbouncer or config.pgbouncer_mode)
    )

    pooler_mode_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("pooler_mode",),
        dsn_keys=("pooler_mode",),
    )
    pooler_mode = str(pooler_mode_raw or config.pooler_mode).strip().lower()
    if pooler_mode not in _VALID_POOLER_MODES:
        raise ImproperConfigurationError(f"Unsupported PostgreSQL pooler_mode: {pooler_mode!r}")
    if pgbouncer_enabled and pooler_mode in {"direct", "session", "auto"}:
        pooler_mode = "transaction"

    proto_cache_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("protocol_level_pooler_cache",),
        dsn_keys=("protocol_level_pooler_cache",),
    )
    protocol_level_pooler_cache = (
        _parse_bool(proto_cache_raw, field_name="protocol_level_pooler_cache")
        if proto_cache_raw is not None
        else config.protocol_level_pooler_cache
    )

    if "statement_cache_size" in explicit and explicit["statement_cache_size"] is not None:
        statement_cache_size = _parse_int(
            explicit["statement_cache_size"], field_name="statement_cache_size", minimum=0
        )
    elif "statement_cache_size" in parsed.params:
        statement_cache_size = _parse_int(
            parsed.params["statement_cache_size"], field_name="statement_cache_size", minimum=0
        )
    elif pooler_mode in {"transaction", "statement"} and not protocol_level_pooler_cache:
        statement_cache_size = 0
    else:
        statement_cache_size = config.effective_statement_cache_size

    cloud_sql_instance = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("cloud_sql_instance",),
        dsn_keys=("cloud_sql_instance",),
    )
    if cloud_sql_instance is None:
        cloud_sql_instance = config.cloud_sql_instance

    alloydb_instance_uri = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("alloydb_instance_uri",),
        dsn_keys=("alloydb_instance_uri",),
    )
    if alloydb_instance_uri is None:
        alloydb_instance_uri = config.alloydb_instance_uri

    if cloud_sql_instance and alloydb_instance_uri:
        raise ImproperConfigurationError(
            "Cannot configure both cloud_sql_instance and alloydb_instance_uri simultaneously"
        )
    if primary.is_unix and (cloud_sql_instance or alloydb_instance_uri):
        raise ImproperConfigurationError(
            "Unix domain socket transport cannot be used with Cloud SQL or AlloyDB dialers"
        )

    cloud_sql_ip_type = (
        str(
            _resolve_scalar(
                explicit,
                parsed.params,
                active_env,
                explicit_keys=("cloud_sql_ip_type",),
                dsn_keys=("cloud_sql_ip_type",),
            )
            or config.cloud_sql_ip_type
        )
        .strip()
        .upper()
    )
    if cloud_sql_ip_type not in _VALID_CLOUD_IP_TYPES:
        raise ImproperConfigurationError(f"Unsupported cloud_sql_ip_type: {cloud_sql_ip_type!r}")
    alloydb_ip_type = (
        str(
            _resolve_scalar(
                explicit,
                parsed.params,
                active_env,
                explicit_keys=("alloydb_ip_type",),
                dsn_keys=("alloydb_ip_type",),
            )
            or config.alloydb_ip_type
        )
        .strip()
        .upper()
    )
    if alloydb_ip_type not in _VALID_CLOUD_IP_TYPES:
        raise ImproperConfigurationError(f"Unsupported alloydb_ip_type: {alloydb_ip_type!r}")

    enable_iam_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("enable_iam_auth",),
        dsn_keys=("enable_iam_auth",),
    )
    enable_iam_auth = (
        _parse_bool(enable_iam_raw, field_name="enable_iam_auth")
        if enable_iam_raw is not None
        else config.enable_iam_auth
    )

    enable_columnar_raw = _resolve_scalar(
        explicit,
        parsed.params,
        active_env,
        explicit_keys=("enable_alloydb_columnar_engine",),
        dsn_keys=("enable_alloydb_columnar_engine",),
    )
    enable_columnar = (
        _parse_bool(enable_columnar_raw, field_name="enable_alloydb_columnar_engine")
        if enable_columnar_raw is not None
        else config.enable_alloydb_columnar_engine
    )

    session_gucs = config.build_session_gucs()
    if enable_columnar or alloydb_instance_uri is not None:
        session_gucs.setdefault("google_columnar_engine.enabled", "on")
        session_gucs.setdefault("alloydb.enable_columnar_engine", "on")
    if application_name:
        session_gucs["application_name"] = application_name
    config_options_raw = session_gucs.pop("options", None)
    extra_options_raw: str | None = None
    extra_gucs = explicit.get("session_gucs")
    if isinstance(extra_gucs, Mapping):
        for guc_key, guc_val in extra_gucs.items():
            key_str = str(guc_key)
            if key_str.lower() == "options":
                extra_options_raw = str(guc_val)
            else:
                session_gucs[key_str] = str(guc_val)

    dsn_or_env_options = _resolve_scalar(
        {},
        parsed.params,
        active_env,
        explicit_keys=(),
        dsn_keys=("options",),
        env_key="PGOPTIONS",
    )
    explicit_options = explicit.get("options")
    cockroach_cluster_raw = _resolve_scalar(
        explicit,
        parsed.params,
        {},
        explicit_keys=("cockroach_cluster",),
        dsn_keys=("cockroach_cluster",),
    )
    cockroach_cluster = (
        validate_cockroach_cluster(cockroach_cluster_raw)
        if cockroach_cluster_raw is not None
        else config.cockroach_cluster
    )
    merged_options = merge_postgres_options(
        str(dsn_or_env_options) if dsn_or_env_options is not None else None,
        config_options_raw,
        extra_options_raw,
        str(explicit_options) if explicit_options is not None else None,
        cockroach_cluster=cockroach_cluster,
    )
    if merged_options is not None:
        session_gucs["options"] = merged_options

    min_size = _parse_int(explicit.get("min_size", config.min_size), field_name="min_size", minimum=0)
    max_size = _parse_int(explicit.get("max_size", config.max_size), field_name="max_size", minimum=1)
    if min_size > max_size:
        raise ImproperConfigurationError(f"min_size ({min_size}) cannot exceed max_size ({max_size})")

    acquire_timeout = _parse_float(explicit.get("acquire_timeout", 30.0), field_name="acquire_timeout", positive=True)
    reset_timeout = _parse_float(explicit.get("reset_timeout", 5.0), field_name="reset_timeout", positive=True)
    close_timeout = _parse_float(explicit.get("close_timeout", 10.0), field_name="close_timeout", positive=True)

    return PostgresClientOptions(
        user=user_str,
        database=db_str,
        host=primary.host,
        port=primary.port,
        password=password_str,
        unix_socket=primary.unix_socket,
        sslmode=primary.sslmode,
        ca_pem=ca_pem,
        client_cert_pem=client_cert_pem,
        client_key_pem=client_key_pem,
        server_name=server_name,
        channel_binding=channel_binding,
        connect_timeout=connect_timeout,
        application_name=application_name,
        session_gucs=session_gucs,
        keepalives=keepalives,
        keepalives_idle=keepalives_idle,
        keepalives_interval=keepalives_interval,
        keepalives_count=keepalives_count,
        cloud_sql_instance=str(cloud_sql_instance) if cloud_sql_instance else None,
        cloud_sql_ip_type=cloud_sql_ip_type,
        alloydb_instance_uri=str(alloydb_instance_uri) if alloydb_instance_uri else None,
        alloydb_ip_type=alloydb_ip_type,
        enable_iam_auth=enable_iam_auth,
        cloud_auth_options=dict(explicit.get("cloud_auth_options") or {}),
        iam_auth_options={
            str(k): str(v) for k, v in (explicit.get("iam_auth_options") or config.iam_auth_options).items()
        },
        pooler_mode=pooler_mode,
        protocol_level_pooler_cache=protocol_level_pooler_cache,
        statement_cache_size=statement_cache_size,
        min_size=min_size,
        max_size=max_size,
        acquire_timeout=acquire_timeout,
        reset_timeout=reset_timeout,
        close_timeout=close_timeout,
        targets=targets,
    )


def _unquote_strict(raw: str, *, field_name: str) -> str:
    """Percent-decode ``raw`` as strict UTF-8 without turning ``+`` into space."""
    try:
        return unquote(raw, encoding="utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ImproperConfigurationError(f"Invalid UTF-8 percent-encoding in PostgreSQL {field_name}: {raw!r}") from exc


def _parse_int(value: Any, *, field_name: str, minimum: int | None = None, maximum: int | None = None) -> int:
    """Parse strict integer option rejecting booleans and out-of-range values."""
    if isinstance(value, bool):
        raise ImproperConfigurationError(f"{field_name} cannot be a boolean")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ImproperConfigurationError(f"{field_name} cannot be empty")
        try:
            parsed = int(stripped, 10)
        except ValueError as exc:
            raise ImproperConfigurationError(f"Invalid integer for {field_name}: {value!r}") from exc
    else:
        raise ImproperConfigurationError(f"Invalid type for {field_name}: {type(value).__name__}")
    if minimum is not None and parsed < minimum:
        raise ImproperConfigurationError(f"{field_name} must be >= {minimum}, got {parsed}")
    if maximum is not None and parsed > maximum:
        raise ImproperConfigurationError(f"{field_name} must be <= {maximum}, got {parsed}")
    return parsed


def _parse_float(value: Any, *, field_name: str, positive: bool = False) -> float:
    """Parse float duration option rejecting booleans and non-positive values when requested."""
    if isinstance(value, bool):
        raise ImproperConfigurationError(f"{field_name} cannot be a boolean")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ImproperConfigurationError(f"Invalid float for {field_name}: {value!r}") from exc
    if positive and parsed <= 0.0:
        raise ImproperConfigurationError(f"{field_name} must be > 0, got {parsed}")
    return parsed


def _parse_bool(value: Any, *, field_name: str) -> bool:
    """Parse libpq boolean flag string or Python bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in _BOOL_TRUE:
            return True
        if norm in _BOOL_FALSE:
            return False
    raise ImproperConfigurationError(f"Invalid boolean value for {field_name}: {value!r}")


def _split_authority_hosts(host_section: str) -> list[tuple[str, int | None]]:
    """Split comma-separated URI authority hosts while preserving IPv6 brackets."""
    entries: list[str] = []
    start = 0
    in_brackets = False
    for idx, ch in enumerate(host_section):
        if ch == "[":
            in_brackets = True
        elif ch == "]":
            in_brackets = False
        elif ch == "," and not in_brackets:
            entries.append(host_section[start:idx])
            start = idx + 1
    if in_brackets:
        raise ImproperConfigurationError("Unclosed IPv6 bracket '[' in PostgreSQL URI")
    entries.append(host_section[start:])

    parsed: list[tuple[str, int | None]] = []
    for raw_entry in entries:
        if not raw_entry:
            if len(entries) == 1:
                continue
            raise ImproperConfigurationError("Empty host entry in multi-host PostgreSQL URI")
        if raw_entry.startswith("["):
            close_idx = raw_entry.find("]")
            if close_idx == -1:
                raise ImproperConfigurationError("Malformed IPv6 host in PostgreSQL URI")
            host_part = raw_entry[1:close_idx]
            rest = raw_entry[close_idx + 1 :]
            if not rest:
                parsed.append((_unquote_strict(host_part, field_name="host"), None))
            elif rest.startswith(":"):
                port_text = rest[1:]
                port_val = _parse_int(port_text, field_name="port", minimum=1, maximum=65535)
                parsed.append((_unquote_strict(host_part, field_name="host"), port_val))
            else:
                raise ImproperConfigurationError(f"Invalid authority suffix after IPv6 host: {raw_entry!r}")
        else:
            if ":" in raw_entry:
                host_part, port_text = raw_entry.rsplit(":", 1)
                if ":" in host_part:
                    raise ImproperConfigurationError(f"IPv6 host must be enclosed in brackets: {raw_entry!r}")
                port_val = _parse_int(port_text, field_name="port", minimum=1, maximum=65535)
                parsed.append((_unquote_strict(host_part, field_name="host"), port_val))
            else:
                parsed.append((_unquote_strict(raw_entry, field_name="host"), None))
    return parsed


def _normalize_unix_socket_path(directory_or_socket: str, port: int) -> str:
    """Convert a Unix socket directory or full ``.s.PGSQL.<port>`` path into canonical socket path."""
    cleaned = directory_or_socket.rstrip("/") or "/"
    basename = posixpath.basename(cleaned)
    if basename.startswith(".s.PGSQL."):
        return cleaned
    return posixpath.join(cleaned, f".s.PGSQL.{port}")


def is_postgres_loopback_host(host: str) -> bool:
    """Return True when ``host`` targets localhost or an IPv4/IPv6 loopback address."""
    cleaned = host.strip().lower()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    cleaned = cleaned.partition("%")[0]
    if cleaned in {"localhost", "localhost."}:
        return True
    try:
        addr = ipaddress.ip_address(cleaned)
    except ValueError:
        return False
    if addr.is_loopback:
        return True
    return isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None and addr.ipv4_mapped.is_loopback


def normalize_pem_bytes(value: Any, *, field_name: str = "ca_pem") -> bytes:
    """Normalize an inline PEM string or byte buffer into validated UTF-8 bytes."""
    if isinstance(value, bytes):
        if b"\x00" in value or not value.strip():
            raise ImproperConfigurationError(f"PostgreSQL {field_name} must be non-empty and free of NUL bytes")
        return value
    if isinstance(value, str):
        if "\x00" in value or not value.strip():
            raise ImproperConfigurationError(f"PostgreSQL {field_name} must be non-empty and free of NUL bytes")
        return value.encode("utf-8")
    raise ImproperConfigurationError(f"PostgreSQL {field_name} must be str or bytes, got {type(value).__name__}")


def _read_pem_file(path: str, *, field_name: str) -> bytes:
    """Read a PEM certificate/key file preserving OS errors when the path is missing."""
    if not path:
        raise ImproperConfigurationError(f"{field_name} path cannot be empty")
    return Path(path).read_bytes()


def _resolve_os_username() -> str:
    """Resolve current OS username via ``getpass.getuser()`` as libpq final fallback."""
    try:
        username = getpass.getuser()
    except Exception as exc:
        raise ImproperConfigurationError(
            "Unable to determine PostgreSQL user from config, DSN, PGUSER, or OS user"
        ) from exc
    if not username:
        raise ImproperConfigurationError("OS username fallback returned empty string for PostgreSQL user")
    return username


def _resolve_scalar(
    explicit: Mapping[str, Any],
    dsn_params: Mapping[str, str],
    env: Mapping[str, str],
    *,
    explicit_keys: tuple[str, ...],
    dsn_keys: tuple[str, ...] = (),
    env_key: str | None = None,
) -> Any:
    """Resolve the highest-precedence non-None value across explicit, DSN, and env."""
    for key in explicit_keys:
        if key in explicit and explicit[key] is not None:
            return explicit[key]
    for key in dsn_keys:
        if key in dsn_params:
            return dsn_params[key]
    if env_key is not None and env_key in env and env[env_key] != "":
        return env[env_key]
    return None


def _resolve_targets(
    parsed_dsn: ParsedPostgresDsn,
    explicit: Mapping[str, Any],
    env: Mapping[str, str],
    *,
    explicit_sslmode: str | None,
) -> tuple[PostgresTarget, ...]:
    """Build normalized TCP or Unix socket target tuples enforcing port/TLS parity."""
    raw_hosts: list[str] = []
    explicit_ports: list[int | None] = []

    if explicit.get("unix_socket") is not None:
        sock = str(explicit["unix_socket"])
        if not sock:
            raise ImproperConfigurationError("unix_socket cannot be empty")
        port_raw = _resolve_scalar(
            explicit,
            parsed_dsn.params,
            env,
            explicit_keys=("port",),
            dsn_keys=("port",),
            env_key="PGPORT",
        )
        port_val = (
            _parse_int(port_raw, field_name="port", minimum=1, maximum=65535)
            if port_raw is not None
            else (parsed_dsn.ports[0] if parsed_dsn.ports and parsed_dsn.ports[0] is not None else 5432)
        )
        sslmode = explicit_sslmode or "disable"
        if sslmode != "disable":
            raise ImproperConfigurationError(f"sslmode={sslmode!r} is incompatible with Unix socket connections")
        return (
            PostgresTarget(
                host="localhost",
                port=port_val,
                unix_socket=_normalize_unix_socket_path(sock, port_val),
                is_unix=True,
                sslmode="disable",
            ),
        )

    if explicit.get("host") is not None:
        host_val = str(explicit["host"])
        if not host_val:
            raise ImproperConfigurationError("host cannot be empty")
        raw_hosts = [part for part in host_val.split(",") if part]
    elif "host" in parsed_dsn.params and parsed_dsn.params["host"] != "":
        raw_hosts = [part for part in parsed_dsn.params["host"].split(",") if part]
    elif parsed_dsn.hosts:
        raw_hosts = list(parsed_dsn.hosts)
        explicit_ports = list(parsed_dsn.ports)
    elif env.get("PGHOST"):
        raw_hosts = [part for part in env["PGHOST"].split(",") if part]
    elif env.get("PGHOSTADDR"):
        raw_hosts = [part for part in env["PGHOSTADDR"].split(",") if part]
    else:
        raw_hosts = ["localhost"]

    if not explicit_ports:
        port_raw = _resolve_scalar(
            explicit,
            parsed_dsn.params,
            env,
            explicit_keys=("port",),
            dsn_keys=("port",),
            env_key="PGPORT",
        )
        if port_raw is not None:
            if isinstance(port_raw, str) and "," in port_raw:
                explicit_ports = [
                    _parse_int(item, field_name="port", minimum=1, maximum=65535) for item in port_raw.split(",")
                ]
            else:
                explicit_ports = [_parse_int(port_raw, field_name="port", minimum=1, maximum=65535)]
    elif explicit.get("port") is not None:
        override_port = _parse_int(explicit["port"], field_name="port", minimum=1, maximum=65535)
        explicit_ports = [override_port] * len(raw_hosts)

    if len(explicit_ports) == 1 and len(raw_hosts) > 1:
        explicit_ports = explicit_ports * len(raw_hosts)
    elif explicit_ports and len(explicit_ports) != len(raw_hosts):
        raise ImproperConfigurationError(
            f"Multi-host target count ({len(raw_hosts)}) does not match port count ({len(explicit_ports)})"
        )

    default_port_raw = _resolve_scalar(
        explicit,
        parsed_dsn.params,
        env,
        explicit_keys=(),
        dsn_keys=("port",),
        env_key="PGPORT",
    )
    fallback_port = (
        _parse_int(default_port_raw, field_name="port", minimum=1, maximum=65535)
        if default_port_raw is not None
        else 5432
    )

    targets: list[PostgresTarget] = []
    seen_unix: bool | None = None
    for idx, raw_host in enumerate(raw_hosts):
        explicit_port = explicit_ports[idx] if idx < len(explicit_ports) else None
        port_val = explicit_port if explicit_port is not None else fallback_port
        if not raw_host:
            raise ImproperConfigurationError("PostgreSQL host cannot be empty")
        is_unix = raw_host.startswith("/")
        if seen_unix is None:
            seen_unix = is_unix
        elif seen_unix != is_unix:
            raise ImproperConfigurationError("Cannot mix TCP hosts and Unix domain sockets in a single DSN")
        if is_unix:
            target_sslmode = explicit_sslmode or "disable"
            if target_sslmode != "disable":
                raise ImproperConfigurationError(
                    f"sslmode={target_sslmode!r} is incompatible with Unix socket connections"
                )
            targets.append(
                PostgresTarget(
                    host="localhost",
                    port=port_val,
                    unix_socket=_normalize_unix_socket_path(raw_host, port_val),
                    is_unix=True,
                    sslmode="disable",
                )
            )
        else:
            if "://" in raw_host or "/" in raw_host:
                raise ImproperConfigurationError(f"TCP host must not contain '/' or URI scheme: {raw_host!r}")
            target_sslmode = explicit_sslmode or "prefer"
            targets.append(
                PostgresTarget(
                    host=raw_host,
                    port=port_val,
                    unix_socket=None,
                    is_unix=False,
                    sslmode=target_sslmode,
                )
            )
    return tuple(targets)
