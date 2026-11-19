"""Zero-sidecar Microsoft Azure Entra ID TLS and OAuth2 database dialers."""

from __future__ import annotations

import asyncio
import socket
import ssl
from collections.abc import Callable
from typing import Any, Final
from urllib.parse import quote

from vertebra._vertebra import AzureEntraDialer
from vertebra.db.driver._cloud_auth import (
    AZURE_MSSQL_SCOPE,
    AZURE_MYSQL_SCOPE,
    AZURE_POSTGRES_SCOPE,
    AzureEntraTokenProvider,
    DatabaseTokenProvider,
)
from vertebra.typing import ImproperConfigurationError

_ENGINE_DEFAULT_SCOPES: Final[dict[str, str]] = {
    "postgres": AZURE_POSTGRES_SCOPE,
    "mysql": AZURE_MYSQL_SCOPE,
    "mssql": AZURE_MSSQL_SCOPE,
}
_ENGINE_DEFAULT_DATABASES: Final[dict[str, str]] = {
    "postgres": "postgres",
    "mysql": "mysql",
    "mssql": "master",
}
_ENGINE_DEFAULT_USERS: Final[dict[str, str]] = {
    "postgres": "postgres",
    "mysql": "root",
    "mssql": "",
}

__all__ = (
    "AsyncAzureEntraDialer",
    "SyncAzureEntraDialer",
)


def _validate_port(port: int) -> int:
    """Validate that a TCP port is a non-boolean integer within 1..65535."""
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        msg = f"port must be an integer between 1 and 65535, got {port!r}"
        raise ImproperConfigurationError(msg)
    return port


def _validate_non_empty(value: str, *, field_name: str) -> str:
    """Validate that a string configuration field is non-empty and free of NUL bytes."""
    if "\x00" in value:
        msg = f"{field_name} must not contain NUL bytes"
        raise ImproperConfigurationError(msg)
    stripped = value.strip()
    if not stripped:
        msg = f"{field_name} must be a non-empty string"
        raise ImproperConfigurationError(msg)
    return stripped


def _format_url_host(host: str) -> str:
    """Format a hostname or IPv6 literal for safe inclusion in a database URI authority."""
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        return f"[{host}]"
    return host


def _build_verify_full_tls_context(ca_pem: str | None = None) -> ssl.SSLContext:
    """Build a `verify-full` client `SSLContext` enforcing TLS 1.2+, `CERT_REQUIRED`, and hostname verification."""
    if ca_pem is not None:
        validated_pem = _validate_non_empty(ca_pem, field_name="ca_pem")
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cadata=validated_pem)
    else:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx


class _BaseAzureEntraDialer:
    """Shared Azure database endpoint metadata, `verify-full` TLS context, and Entra token provider wrapper."""

    __slots__ = (
        "_ca_pem",
        "_database",
        "_native",
        "_scope",
        "_ssl_context",
        "_token_provider",
        "_username",
    )

    def __init__(
        self,
        endpoint_host: str | AzureEntraDialer,
        *,
        port: int | None = None,
        engine: str | None = None,
        username: str | None = None,
        database: str | None = None,
        token_provider: AzureEntraTokenProvider | DatabaseTokenProvider | None = None,
        ca_pem: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        federated_token_file: str | None = None,
        authority_host: str | None = None,
        scope: str | None = None,
        token_endpoint: str | None = None,
        imds_endpoint: str | None = None,
        api_version: str = "2019-08-01",
        refresh_margin_seconds: float = 120.0,
        clock: Callable[[], float] | None = None,
        fetcher: Callable[..., Any] | None = None,
    ) -> None:
        if port is not None:
            _validate_port(port)
        if token_provider is not None and not isinstance(token_provider, DatabaseTokenProvider):
            msg = "token_provider must implement DatabaseTokenProvider"
            raise ImproperConfigurationError(msg)

        if isinstance(endpoint_host, AzureEntraDialer):
            if port is None and engine is None:
                native = endpoint_host
            else:
                try:
                    native = AzureEntraDialer(
                        endpoint_host.endpoint_host,
                        port=port if port is not None else endpoint_host.port,
                        engine=engine if engine is not None else endpoint_host.engine,
                    )
                except ValueError as exc:
                    raise ImproperConfigurationError(str(exc)) from exc
        else:
            try:
                native = AzureEntraDialer(endpoint_host, port=port, engine=engine)
            except ValueError as exc:
                raise ImproperConfigurationError(str(exc)) from exc

        default_scope = _ENGINE_DEFAULT_SCOPES[native.engine]
        explicit_scope = _validate_non_empty(scope, field_name="scope") if scope is not None else None

        if token_provider is not None:
            if isinstance(token_provider, AzureEntraTokenProvider):
                scoped_provider = token_provider.with_default_scope(explicit_scope or default_scope)
                resolved_scope = scoped_provider.scope or default_scope
                resolved_provider: DatabaseTokenProvider = scoped_provider
            else:
                resolved_scope = explicit_scope or default_scope
                resolved_provider = token_provider
        else:
            resolved_scope = explicit_scope or default_scope
            provider_kwargs: dict[str, Any] = {
                "tenant_id": tenant_id,
                "client_id": client_id,
                "client_secret": client_secret,
                "federated_token_file": federated_token_file,
                "authority_host": authority_host,
                "scope": resolved_scope,
                "token_endpoint": token_endpoint,
                "api_version": api_version,
                "refresh_margin_seconds": refresh_margin_seconds,
                "clock": clock,
                "fetcher": fetcher,
            }
            if imds_endpoint is not None:
                provider_kwargs["imds_endpoint"] = imds_endpoint
            resolved_provider = AzureEntraTokenProvider(**provider_kwargs)

        if username is not None:
            resolved_user = _validate_non_empty(username, field_name="username")
        else:
            resolved_user = _ENGINE_DEFAULT_USERS[native.engine]

        resolved_db = (
            _validate_non_empty(database, field_name="database")
            if database is not None
            else _ENGINE_DEFAULT_DATABASES[native.engine]
        )
        ssl_ctx = _build_verify_full_tls_context(ca_pem)

        self._native = native
        self._scope = resolved_scope
        self._username = resolved_user
        self._database = resolved_db
        self._ca_pem = ca_pem
        self._ssl_context = ssl_ctx
        self._token_provider = resolved_provider

    def __repr__(self) -> str:
        """Return a redacted representation omitting client secrets and cached tokens."""
        return (
            f"{type(self).__name__}(endpoint_host={self.endpoint_host!r}, port={self.port!r}, "
            f"engine={self.engine!r}, scope={self._scope!r})"
        )

    @property
    def native_dialer(self) -> AzureEntraDialer:
        """Return the underlying Rust `AzureEntraDialer` metadata instance."""
        return self._native

    @property
    def endpoint_host(self) -> str:
        """Return the normalized Azure database hostname."""
        return self._native.endpoint_host

    @property
    def port(self) -> int:
        """Return the resolved Azure database server port (`5432`, `3306`, or `1433`)."""
        return self._native.port

    @property
    def engine(self) -> str:
        """Return the canonical Azure database engine (`'postgres'`, `'mysql'`, or `'mssql'`)."""
        return self._native.engine

    @property
    def scope(self) -> str:
        """Return the active Microsoft Entra ID OAuth2 token scope."""
        return self._scope

    @property
    def server_name(self) -> str | None:
        """Return the Azure server resource prefix when parsed from a canonical Azure hostname."""
        return self._native.server_name

    @property
    def username(self) -> str:
        """Return the database login username used by `connection_kwargs()`."""
        return self._username

    @property
    def database(self) -> str:
        """Return the default database name used by `connection_kwargs()`."""
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
        """Return the `DatabaseTokenProvider` acquiring Microsoft Entra ID database tokens."""
        return self._token_provider

    def build_tls_context(self, *, ca_pem: str | None = None) -> ssl.SSLContext:
        """Construct a fresh `verify-full` `ssl.SSLContext` using WebPKI roots or `ca_pem`."""
        return _build_verify_full_tls_context(self._ca_pem if ca_pem is None else ca_pem)

    def invalidate_token(self) -> None:
        """Clear any cached Azure Entra ID token snapshot."""
        self._token_provider.invalidate()

    def connection_kwargs(self, *, database: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Return driver-ready `PostgreSQLConfig`, `MySQLConfig`, or `MSSQLConfig` keyword arguments."""
        db_name = _validate_non_empty(database, field_name="database") if database is not None else self._database
        url_host = _format_url_host(self.endpoint_host)
        escaped_db = quote(db_name, safe="")

        if self.engine == "mysql":
            user_val = self._username or "root"
            escaped_user = quote(user_val, safe="")
            kwargs: dict[str, Any] = {
                "dsn": f"mysql://{escaped_user}@{url_host}:{self.port}/{escaped_db}?ssl_mode=verify-full",
                "host": self.endpoint_host,
                "port": self.port,
                "user": user_val,
                "database": db_name,
                "ssl_mode": "verify-full",
                "token_provider": self._token_provider,
            }
            if self._ca_pem is not None:
                kwargs["ssl_ca"] = self._ca_pem
        elif self.engine == "mssql":
            user_prefix = f"{quote(self._username, safe='')}@" if self._username else ""
            kwargs = {
                "dsn": f"mssql://{user_prefix}{url_host}:{self.port}/{escaped_db}",
                "host": self.endpoint_host,
                "port": self.port,
                "database": db_name,
                "encryption": "required",
                "trust_server_certificate": False,
                "token_provider": self._token_provider,
            }
            if self._username:
                kwargs["user"] = self._username
            if self._ca_pem is not None:
                kwargs["ca_pem"] = self._ca_pem.encode("utf-8")
        else:
            user_val = self._username or "postgres"
            escaped_user = quote(user_val, safe="")
            kwargs = {
                "dsn": f"postgresql://{escaped_user}@{url_host}:{self.port}/{escaped_db}?sslmode=verify-full",
                "token_provider": self._token_provider,
            }
            if self._ca_pem is not None:
                kwargs["ca_pem"] = self._ca_pem

        kwargs.update(overrides)
        return kwargs


class SyncAzureEntraDialer(_BaseAzureEntraDialer):
    """Synchronous Microsoft Azure Entra ID `verify-full` TLS and OAuth2 database dialer."""

    __slots__ = ()

    def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly acquired Azure Entra ID database token."""
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
        """Establish a synchronous `verify-full` TLS socket to the Azure database endpoint."""
        target_host = self.endpoint_host if host is None else _validate_non_empty(host, field_name="host")
        target_port = self.port if port is None else _validate_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else _validate_non_empty(server_hostname, field_name="server_hostname")
        )
        ctx = self._ssl_context if ssl_context is None else ssl_context
        raw_sock = socket.create_connection((target_host, target_port), timeout=timeout)
        try:
            return ctx.wrap_socket(raw_sock, server_hostname=tls_hostname)
        except BaseException:
            raw_sock.close()
            raise

    def close(self) -> None:
        """Clear cached Entra ID token state."""
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


class AsyncAzureEntraDialer(_BaseAzureEntraDialer):
    """Asynchronous Microsoft Azure Entra ID `verify-full` TLS and OAuth2 database dialer."""

    __slots__ = ()

    async def resolve_token(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly acquired Azure Entra ID token without blocking the loop."""
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
        """Establish an asynchronous `verify-full` TLS stream pair to the Azure database endpoint."""
        target_host = self.endpoint_host if host is None else _validate_non_empty(host, field_name="host")
        target_port = self.port if port is None else _validate_port(port)
        tls_hostname = (
            self.endpoint_host
            if server_hostname is None
            else _validate_non_empty(server_hostname, field_name="server_hostname")
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
        """Clear cached Entra ID token state."""
        self.invalidate_token()
