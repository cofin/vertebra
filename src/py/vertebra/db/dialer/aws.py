"""Zero-sidecar AWS RDS, Aurora, and Aurora DSQL TLS and SigV4 IAM dialers."""

from __future__ import annotations

import asyncio
import os
import socket
import ssl
from collections.abc import Callable
from typing import Any, Final
from urllib.parse import quote

from vertebra._vertebra import AwsAuroraDsqlDialer, AwsRdsDialer, aws_rds_root_ca_pem
from vertebra.db.dialer._base import (
    build_verify_full_tls_context,
    format_dialer_url_host,
    validate_dialer_non_empty,
    validate_dialer_port,
)
from vertebra.db.driver._cloud_auth import (
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    DatabaseTokenProvider,
)
from vertebra.typing import ImproperConfigurationError

_POSTGRES_ENGINE_ALIASES: Final[frozenset[str]] = frozenset({"postgres", "postgresql", "pg"})
_MYSQL_ENGINE_ALIASES: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})

__all__ = (
    "AsyncAwsAuroraDsqlDialer",
    "AsyncAwsRdsDialer",
    "SyncAwsAuroraDsqlDialer",
    "SyncAwsRdsDialer",
    "aws_rds_root_ca_pem",
)


def _ambient_aws_region() -> str | None:
    """Return `AWS_REGION` or `AWS_DEFAULT_REGION` when set in the process environment."""
    raw = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "").strip()
    return raw or None


class _BaseAwsRdsDialer:
    """Shared metadata, RDS Root CA `verify-full` TLS context, and SigV4 IAM token provider wrapper."""

    __slots__ = (
        "_ca_pem",
        "_database",
        "_engine",
        "_native",
        "_ssl_context",
        "_token_provider",
    )

    @staticmethod
    def _normalize_rds_engine(engine: str | None, *, default_from_port: int | None = None) -> str:
        """Normalize an AWS RDS engine identifier to `'postgres'` or `'mysql'`."""
        if engine is None:
            return "mysql" if default_from_port == 3306 else "postgres"
        lowered = validate_dialer_non_empty(engine, field_name="engine").lower()
        if lowered in _POSTGRES_ENGINE_ALIASES:
            return "postgres"
        if lowered in _MYSQL_ENGINE_ALIASES:
            return "mysql"
        msg = f"Unsupported AWS RDS engine {engine!r}; expected 'postgres' or 'mysql'"
        raise ImproperConfigurationError(msg)

    @staticmethod
    def _has_embedded_port(endpoint_host: str) -> bool:
        """Return True when an endpoint string explicitly embeds a `:port` suffix."""
        if endpoint_host.startswith("["):
            return "]:" in endpoint_host
        return endpoint_host.count(":") == 1

    def __init__(
        self,
        endpoint_host: str | AwsRdsDialer,
        *,
        port: int | None = None,
        region: str | None = None,
        username: str | None = None,
        database: str | None = None,
        engine: str | None = None,
        token_provider: AwsRdsIamTokenProvider | DatabaseTokenProvider | None = None,
        ca_pem: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        sts_endpoint: str | None = None,
        ecs_endpoint: str | None = None,
        imds_endpoint: str | None = None,
        refresh_margin_seconds: float = 60.0,
        token_ttl_seconds: int = 900,
        clock: Callable[[], float] | None = None,
        signer: Callable[..., Any] | None = None,
    ) -> None:
        if port is not None:
            validate_dialer_port(port)
        if token_provider is not None and not isinstance(token_provider, DatabaseTokenProvider):
            msg = "token_provider must implement DatabaseTokenProvider"
            raise ImproperConfigurationError(msg)

        provisional_engine = self._normalize_rds_engine(engine, default_from_port=port) if engine is not None else None
        provider_user = getattr(token_provider, "username", None) if token_provider is not None else None
        provider_region = getattr(token_provider, "region", None) if token_provider is not None else None

        if isinstance(endpoint_host, AwsRdsDialer):
            if port is None and region is None and username is None:
                native = endpoint_host
            else:
                resolved_user = (
                    username
                    if username is not None
                    else (str(provider_user) if provider_user else endpoint_host.username)
                )
                try:
                    native = AwsRdsDialer(
                        endpoint_host.endpoint_host,
                        port=port if port is not None else endpoint_host.port,
                        region=region if region is not None else endpoint_host.region,
                        username=resolved_user,
                    )
                except ValueError as exc:
                    raise ImproperConfigurationError(str(exc)) from exc
        else:
            effective_port = port
            if effective_port is None and provisional_engine == "mysql" and not self._has_embedded_port(endpoint_host):
                effective_port = 3306
            effective_user = (
                username
                if username is not None
                else (
                    str(provider_user) if provider_user else ("admin" if provisional_engine == "mysql" else "postgres")
                )
            )
            effective_region = region if region is not None else (str(provider_region) if provider_region else None)
            lower_host = endpoint_host.lower()
            if (
                effective_region is None
                and not lower_host.endswith((".rds.amazonaws.com", ".rds.amazonaws.com.cn"))
                and lower_host not in {"rds.amazonaws.com", "rds.amazonaws.com.cn"}
            ):
                effective_region = _ambient_aws_region()
            try:
                native = AwsRdsDialer(
                    endpoint_host,
                    port=effective_port,
                    region=effective_region,
                    username=effective_user,
                )
            except ValueError as exc:
                raise ImproperConfigurationError(str(exc)) from exc

        resolved_engine = (
            provisional_engine
            if provisional_engine is not None
            else self._normalize_rds_engine(None, default_from_port=native.port)
        )
        resolved_db = (
            validate_dialer_non_empty(database, field_name="database")
            if database is not None
            else ("mysql" if resolved_engine == "mysql" else "postgres")
        )
        effective_ca_pem = aws_rds_root_ca_pem() if ca_pem is None else ca_pem
        ssl_ctx = build_verify_full_tls_context(effective_ca_pem)

        resolved_provider: DatabaseTokenProvider = (
            token_provider
            if token_provider is not None
            else AwsRdsIamTokenProvider(
                endpoint_host=native.endpoint_host,
                port=native.port,
                region=native.region,
                username=native.username,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                sts_endpoint=sts_endpoint,
                ecs_endpoint=ecs_endpoint,
                imds_endpoint=imds_endpoint,
                refresh_margin_seconds=refresh_margin_seconds,
                token_ttl_seconds=token_ttl_seconds,
                clock=clock,
                signer=signer,
            )
        )

        self._native = native
        self._engine = resolved_engine
        self._database = resolved_db
        self._ca_pem = effective_ca_pem
        self._ssl_context = ssl_ctx
        self._token_provider = resolved_provider

    def __repr__(self) -> str:
        """Return a redacted representation omitting IAM credentials and cached tokens."""
        return (
            f"{type(self).__name__}(endpoint_host={self.endpoint_host!r}, port={self.port!r}, "
            f"region={self.region!r}, username={self.username!r}, engine={self._engine!r})"
        )

    @property
    def native_dialer(self) -> AwsRdsDialer:
        """Return the underlying Rust `AwsRdsDialer` metadata instance."""
        return self._native

    @property
    def endpoint_host(self) -> str:
        """Return the normalized RDS instance, cluster, or RDS Proxy hostname."""
        return self._native.endpoint_host

    @property
    def port(self) -> int:
        """Return the RDS TCP server port."""
        return self._native.port

    @property
    def region(self) -> str:
        """Return the resolved AWS signing region."""
        return self._native.region

    @property
    def username(self) -> str:
        """Return the IAM database username."""
        return self._native.username

    @property
    def engine(self) -> str:
        """Return the normalized database engine (`'postgres'` or `'mysql'`)."""
        return self._engine

    @property
    def database(self) -> str:
        """Return the default database name used by `connection_kwargs()`."""
        return self._database

    @property
    def is_cluster_endpoint(self) -> bool:
        """Return True when the endpoint is an Aurora cluster writer or reader endpoint."""
        return self._native.is_cluster_endpoint

    @property
    def is_proxy_endpoint(self) -> bool:
        """Return True when the endpoint is an Amazon RDS Proxy endpoint."""
        return self._native.is_proxy_endpoint

    @property
    def ca_pem(self) -> str:
        """Return the active Root CA PEM bundle (`aws_rds_root_ca_pem()` by default)."""
        return self._ca_pem

    @property
    def ssl_context(self) -> ssl.SSLContext:
        """Return the cached `verify-full` client `ssl.SSLContext`."""
        return self._ssl_context

    @property
    def token_provider(self) -> DatabaseTokenProvider:
        """Return the `DatabaseTokenProvider` minting short-lived RDS IAM tokens."""
        return self._token_provider

    def build_tls_context(self, *, ca_pem: str | None = None) -> ssl.SSLContext:
        """Construct a fresh `verify-full` `ssl.SSLContext` seeded with `ca_pem` or the dialer's CA bundle."""
        return build_verify_full_tls_context(self._ca_pem if ca_pem is None else ca_pem)

    def invalidate_token(self) -> None:
        """Clear any cached RDS IAM token snapshot."""
        self._token_provider.invalidate()

    def connection_kwargs(self, *, database: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Return driver-ready `PostgreSQLConfig` or `MySQLConfig` keyword arguments."""
        db_name = validate_dialer_non_empty(database, field_name="database") if database is not None else self._database
        url_host = format_dialer_url_host(self.endpoint_host)
        escaped_user = quote(self.username, safe="")
        escaped_db = quote(db_name, safe="")
        if self._engine == "mysql":
            kwargs: dict[str, Any] = {
                "dsn": f"mysql://{escaped_user}@{url_host}:{self.port}/{escaped_db}?ssl_mode=verify-full",
                "host": self.endpoint_host,
                "port": self.port,
                "user": self.username,
                "database": db_name,
                "ssl_mode": "verify-full",
                "ssl_ca": self._ca_pem,
                "token_provider": self._token_provider,
            }
        else:
            kwargs = {
                "dsn": f"postgresql://{escaped_user}@{url_host}:{self.port}/{escaped_db}?sslmode=verify-full",
                "token_provider": self._token_provider,
                "ca_pem": self._ca_pem,
            }
        kwargs.update(overrides)
        return kwargs


class SyncAwsRdsDialer(_BaseAwsRdsDialer):
    """Synchronous AWS RDS, Aurora, and RDS Proxy `verify-full` TLS and SigV4 IAM dialer."""

    __slots__ = ()

    def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed RDS IAM authentication token."""
        return self._token_provider.resolve_sync(force_refresh=force_refresh)

    def connect(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous `verify-full` TLS socket to the RDS endpoint."""
        target_host = self.endpoint_host if host is None else validate_dialer_non_empty(host, field_name="host")
        target_port = self.port if port is None else validate_dialer_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else validate_dialer_non_empty(server_hostname, field_name="server_hostname")
        )
        ctx = self._ssl_context if ssl_context is None else ssl_context
        raw_sock = socket.create_connection((target_host, target_port), timeout=timeout)
        try:
            return ctx.wrap_socket(raw_sock, server_hostname=tls_hostname)
        except BaseException:
            raw_sock.close()
            raise

    def close(self) -> None:
        """Clear cached IAM token state."""
        self.invalidate_token()

    def __call__(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a verified synchronous TLS socket."""
        return self.connect(
            host,
            port=port,
            server_hostname=server_hostname,
            ssl_context=ssl_context,
            timeout=timeout,
        )


class AsyncAwsRdsDialer(_BaseAwsRdsDialer):
    """Asynchronous AWS RDS, Aurora, and RDS Proxy `verify-full` TLS and SigV4 IAM dialer."""

    __slots__ = ()

    async def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed RDS IAM token without blocking the event loop."""
        return await self._token_provider.resolve_async(force_refresh=force_refresh)

    async def connect(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous `verify-full` TLS stream pair to the RDS endpoint."""
        target_host = self.endpoint_host if host is None else validate_dialer_non_empty(host, field_name="host")
        target_port = self.port if port is None else validate_dialer_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else validate_dialer_non_empty(server_hostname, field_name="server_hostname")
        )
        ctx = self._ssl_context if ssl_context is None else ssl_context
        async with asyncio.timeout(timeout):
            return await asyncio.open_connection(
                target_host,
                target_port,
                ssl=ctx,
                server_hostname=tls_hostname,
            )

    async def close(self) -> None:
        """Clear cached IAM token state."""
        self.invalidate_token()


class _BaseAwsAuroraDsqlDialer:
    """Shared metadata, WebPKI `verify-full` TLS context, and SigV4 `dsql` token provider wrapper."""

    __slots__ = (
        "_ca_pem",
        "_database",
        "_native",
        "_ssl_context",
        "_token_provider",
        "_username",
    )

    def __init__(
        self,
        endpoint_host: str | AwsAuroraDsqlDialer,
        *,
        port: int | None = None,
        region: str | None = None,
        admin: bool = False,
        username: str | None = None,
        database: str = "postgres",
        token_provider: AwsAuroraDsqlIamTokenProvider | DatabaseTokenProvider | None = None,
        ca_pem: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        sts_endpoint: str | None = None,
        ecs_endpoint: str | None = None,
        imds_endpoint: str | None = None,
        refresh_margin_seconds: float = 60.0,
        token_ttl_seconds: int = 900,
        clock: Callable[[], float] | None = None,
        signer: Callable[..., Any] | None = None,
    ) -> None:
        if port is not None:
            validate_dialer_port(port)
        if token_provider is not None and not isinstance(token_provider, DatabaseTokenProvider):
            msg = "token_provider must implement DatabaseTokenProvider"
            raise ImproperConfigurationError(msg)

        provider_region = getattr(token_provider, "region", None) if token_provider is not None else None
        effective_admin = admin or (isinstance(token_provider, AwsAuroraDsqlIamTokenProvider) and token_provider.admin)

        if isinstance(endpoint_host, AwsAuroraDsqlDialer):
            if port is None and region is None and admin == endpoint_host.admin:
                native = endpoint_host
            else:
                try:
                    native = AwsAuroraDsqlDialer(
                        endpoint_host.endpoint_host,
                        port=port if port is not None else endpoint_host.port,
                        region=region if region is not None else endpoint_host.region,
                        admin=effective_admin,
                    )
                except ValueError as exc:
                    raise ImproperConfigurationError(str(exc)) from exc
        else:
            effective_region = region if region is not None else (str(provider_region) if provider_region else None)
            lower_host = endpoint_host.lower()
            if effective_region is None and not lower_host.endswith(".on.aws") and lower_host != "on.aws":
                effective_region = _ambient_aws_region()
            try:
                native = AwsAuroraDsqlDialer(
                    endpoint_host,
                    port=port,
                    region=effective_region,
                    admin=effective_admin,
                )
            except ValueError as exc:
                raise ImproperConfigurationError(str(exc)) from exc

        resolved_user = (
            validate_dialer_non_empty(username, field_name="username")
            if username is not None
            else ("admin" if native.admin else "postgres")
        )
        resolved_db = validate_dialer_non_empty(database, field_name="database")
        ssl_ctx = build_verify_full_tls_context(ca_pem)

        resolved_provider: DatabaseTokenProvider = (
            token_provider
            if token_provider is not None
            else AwsAuroraDsqlIamTokenProvider(
                hostname=native.hostname,
                region=native.region,
                admin=native.admin,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                sts_endpoint=sts_endpoint,
                ecs_endpoint=ecs_endpoint,
                imds_endpoint=imds_endpoint,
                refresh_margin_seconds=refresh_margin_seconds,
                token_ttl_seconds=token_ttl_seconds,
                clock=clock,
                signer=signer,
            )
        )

        self._native = native
        self._username = resolved_user
        self._database = resolved_db
        self._ca_pem = ca_pem
        self._ssl_context = ssl_ctx
        self._token_provider = resolved_provider

    def __repr__(self) -> str:
        """Return a redacted representation omitting IAM credentials and cached tokens."""
        return (
            f"{type(self).__name__}(endpoint_host={self.endpoint_host!r}, port={self.port!r}, "
            f"region={self.region!r}, admin={self.admin!r})"
        )

    @property
    def native_dialer(self) -> AwsAuroraDsqlDialer:
        """Return the underlying Rust `AwsAuroraDsqlDialer` metadata instance."""
        return self._native

    @property
    def endpoint_host(self) -> str:
        """Return the normalized Aurora DSQL cluster endpoint hostname."""
        return self._native.endpoint_host

    @property
    def hostname(self) -> str:
        """Alias for `endpoint_host` matching `AwsAuroraDsqlIamTokenProvider`."""
        return self._native.hostname

    @property
    def port(self) -> int:
        """Return the Aurora DSQL TCP server port (default `5432`)."""
        return self._native.port

    @property
    def region(self) -> str:
        """Return the resolved AWS signing region."""
        return self._native.region

    @property
    def cluster_id(self) -> str | None:
        """Return the parsed Aurora DSQL cluster identifier when inferred from `.on.aws`."""
        return self._native.cluster_id

    @property
    def admin(self) -> bool:
        """Return True when minting `DbConnectAdmin` tokens."""
        return self._native.admin

    @property
    def action(self) -> str:
        """Return the SigV4 `dsql` action (`'DbConnect'` or `'DbConnectAdmin'`)."""
        return self._native.action

    @property
    def username(self) -> str:
        """Return the PostgreSQL role name used in `connection_kwargs()`."""
        return self._username

    @property
    def database(self) -> str:
        """Return the default database name used in `connection_kwargs()`."""
        return self._database

    @property
    def ca_pem(self) -> str | None:
        """Return the custom Root CA PEM override, or `None` when using system WebPKI roots."""
        return self._ca_pem

    @property
    def ssl_context(self) -> ssl.SSLContext:
        """Return the cached `verify-full` client `ssl.SSLContext`."""
        return self._ssl_context

    @property
    def token_provider(self) -> DatabaseTokenProvider:
        """Return the `DatabaseTokenProvider` minting Aurora DSQL IAM tokens."""
        return self._token_provider

    def build_tls_context(self, *, ca_pem: str | None = None) -> ssl.SSLContext:
        """Construct a fresh `verify-full` `ssl.SSLContext` using WebPKI roots or `ca_pem`."""
        return build_verify_full_tls_context(self._ca_pem if ca_pem is None else ca_pem)

    def invalidate_token(self) -> None:
        """Clear any cached Aurora DSQL IAM token snapshot."""
        self._token_provider.invalidate()

    def connection_kwargs(self, *, database: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Return driver-ready `PostgreSQLConfig` keyword arguments for Aurora DSQL."""
        db_name = validate_dialer_non_empty(database, field_name="database") if database is not None else self._database
        url_host = format_dialer_url_host(self.endpoint_host)
        escaped_user = quote(self._username, safe="")
        escaped_db = quote(db_name, safe="")
        kwargs: dict[str, Any] = {
            "dsn": f"postgresql://{escaped_user}@{url_host}:{self.port}/{escaped_db}?sslmode=verify-full",
            "token_provider": self._token_provider,
        }
        if self._ca_pem is not None:
            kwargs["ca_pem"] = self._ca_pem
        kwargs.update(overrides)
        return kwargs


class SyncAwsAuroraDsqlDialer(_BaseAwsAuroraDsqlDialer):
    """Synchronous AWS Aurora DSQL `verify-full` TLS and SigV4 IAM dialer."""

    __slots__ = ()

    def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed Aurora DSQL IAM token."""
        return self._token_provider.resolve_sync(force_refresh=force_refresh)

    def connect(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous `verify-full` TLS socket to the Aurora DSQL endpoint."""
        target_host = self.endpoint_host if host is None else validate_dialer_non_empty(host, field_name="host")
        target_port = self.port if port is None else validate_dialer_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else validate_dialer_non_empty(server_hostname, field_name="server_hostname")
        )
        ctx = self._ssl_context if ssl_context is None else ssl_context
        raw_sock = socket.create_connection((target_host, target_port), timeout=timeout)
        try:
            return ctx.wrap_socket(raw_sock, server_hostname=tls_hostname)
        except BaseException:
            raw_sock.close()
            raise

    def close(self) -> None:
        """Clear cached IAM token state."""
        self.invalidate_token()

    def __call__(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a verified synchronous TLS socket."""
        return self.connect(
            host,
            port=port,
            server_hostname=server_hostname,
            ssl_context=ssl_context,
            timeout=timeout,
        )


class AsyncAwsAuroraDsqlDialer(_BaseAwsAuroraDsqlDialer):
    """Asynchronous AWS Aurora DSQL `verify-full` TLS and SigV4 IAM dialer."""

    __slots__ = ()

    async def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed Aurora DSQL IAM token without blocking the loop."""
        return await self._token_provider.resolve_async(force_refresh=force_refresh)

    async def connect(
        self,
        host: str | None = None,
        *,
        port: int | None = None,
        server_hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous `verify-full` TLS stream pair to the Aurora DSQL endpoint."""
        target_host = self.endpoint_host if host is None else validate_dialer_non_empty(host, field_name="host")
        target_port = self.port if port is None else validate_dialer_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else validate_dialer_non_empty(server_hostname, field_name="server_hostname")
        )
        ctx = self._ssl_context if ssl_context is None else ssl_context
        async with asyncio.timeout(timeout):
            return await asyncio.open_connection(
                target_host,
                target_port,
                ssl=ctx,
                server_hostname=tls_hostname,
            )

    async def close(self) -> None:
        """Clear cached IAM token state."""
        self.invalidate_token()
