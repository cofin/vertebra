"""Zero-sidecar Microsoft Azure Entra ID TLS and OAuth2 database dialers."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Final
from urllib.parse import quote

from vertebra._vertebra import AzureEntraDialer
from vertebra.db.dialer._base import (
    _AsyncTokenDialerMixin,
    _BaseTokenDialerMixin,
    _SyncTokenDialerMixin,
    build_verify_full_tls_context,
    format_dialer_url_host,
    validate_dialer_non_empty,
    validate_dialer_port,
)
from vertebra.db.driver._cloud_auth import (
    AzureEntraTokenProvider,
    DatabaseTokenProvider,
)
from vertebra.typing import ImproperConfigurationError

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


class _BaseAzureEntraDialer(_BaseTokenDialerMixin):
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

    _native: AzureEntraDialer
    _token_provider: DatabaseTokenProvider

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
            validate_dialer_port(port)
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

        default_scope = native.scope
        explicit_scope = validate_dialer_non_empty(scope, field_name="scope") if scope is not None else None
        resolved_authority_host = authority_host
        if resolved_authority_host is None and native.cloud != "public":
            resolved_authority_host = os.environ.get("AZURE_AUTHORITY_HOST", "").strip() or native.authority_host

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
                "authority_host": resolved_authority_host,
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
            resolved_user = validate_dialer_non_empty(username, field_name="username")
        else:
            resolved_user = _ENGINE_DEFAULT_USERS[native.engine]

        resolved_db = (
            validate_dialer_non_empty(database, field_name="database")
            if database is not None
            else _ENGINE_DEFAULT_DATABASES[native.engine]
        )
        ssl_ctx = build_verify_full_tls_context(ca_pem)

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
    def ca_pem(self) -> str | None:
        """Return the custom Root CA PEM override, or `None` when using system WebPKI roots."""
        return self._ca_pem

    @property
    def token_provider(self) -> DatabaseTokenProvider:
        """Return the `DatabaseTokenProvider` acquiring Microsoft Entra ID database tokens."""
        return self._token_provider

    def connection_kwargs(self, *, database: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Return driver-ready `PostgreSQLConfig`, `MySQLConfig`, or `MSSQLConfig` keyword arguments."""
        db_name = validate_dialer_non_empty(database, field_name="database") if database is not None else self._database
        url_host = format_dialer_url_host(self.endpoint_host)
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


class SyncAzureEntraDialer(_SyncTokenDialerMixin, _BaseAzureEntraDialer):
    """Synchronous Microsoft Azure Entra ID `verify-full` TLS and OAuth2 database dialer."""

    __slots__ = ()


class AsyncAzureEntraDialer(_AsyncTokenDialerMixin, _BaseAzureEntraDialer):
    """Asynchronous Microsoft Azure Entra ID `verify-full` TLS and OAuth2 database dialer."""

    __slots__ = ()
