"""Shared cloud IAM database token providers with pre-expiry caching, SigV4 signing, and Entra ID."""

from __future__ import annotations

import configparser
import hashlib
import hmac
import inspect
import ipaddress
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
import weakref
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Any, Final, Protocol, runtime_checkable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import anyio
import msgspec
from anyio.lowlevel import RunVar

from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json

AZURE_POSTGRES_SCOPE: Final[str] = "https://ossrdbms-aad.database.windows.net/.default"
AZURE_MYSQL_SCOPE: Final[str] = "https://ossrdbms-aad.database.windows.net/.default"
AZURE_MSSQL_SCOPE: Final[str] = "https://database.windows.net/.default"
AZURE_REDIS_SCOPE: Final[str] = "https://redis.azure.com/.default"

_MAX_RDS_IAM_TTL_SECONDS: Final[int] = 900
_MAX_DSQL_IAM_TTL_SECONDS: Final[int] = 604_800
_DEFAULT_AWS_HTTP_TIMEOUT_SECONDS: Final[float] = 5.0
_DEFAULT_AWS_IMDS_TIMEOUT_SECONDS: Final[float] = 1.0
_MAX_CREDENTIAL_RESPONSE_BYTES: Final[int] = 1024 * 1024
_DEFAULT_AWS_ECS_ENDPOINT: Final[str] = "http://169.254.170.2"
_DEFAULT_AWS_IMDS_IPV4_ENDPOINT: Final[str] = "http://169.254.169.254"
_DEFAULT_AWS_IMDS_IPV6_ENDPOINT: Final[str] = "http://[fd00:ec2::254]"
_AWS_ECS_LINK_LOCAL_HOSTS: Final[frozenset[str]] = frozenset({"169.254.170.2", "169.254.170.23"})
_AWS_IMDS_LINK_LOCAL_HOSTS: Final[frozenset[str]] = frozenset({"169.254.169.254"})
_AWS_IPV6_METADATA_NETWORK: Final[ipaddress.IPv6Network] = ipaddress.IPv6Network("fd00:ec2::/32")

_DEFAULT_ENTRA_AUTHORITY_HOST: Final[str] = "https://login.microsoftonline.com"
_DEFAULT_ENTRA_IMDS_ENDPOINT: Final[str] = "http://169.254.169.254/metadata/identity/oauth2/token"
_DEFAULT_ENTRA_API_VERSION: Final[str] = "2019-08-01"
_DEFAULT_ENTRA_HTTP_TIMEOUT_SECONDS: Final[float] = 10.0
_DEFAULT_ENTRA_FALLBACK_TTL_SECONDS: Final[float] = 3600.0
_JWT_BEARER_ASSERTION_TYPE: Final[str] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
_VALID_TENANT_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._-]+$")

__all__ = (
    "AZURE_MSSQL_SCOPE",
    "AZURE_MYSQL_SCOPE",
    "AZURE_POSTGRES_SCOPE",
    "AZURE_REDIS_SCOPE",
    "AwsAuroraDsqlIamTokenProvider",
    "AwsRdsIamTokenProvider",
    "AzureEntraTokenProvider",
    "DatabaseTokenProvider",
    "TokenSnapshot",
    "is_loopback_host",
)


class TokenSnapshot(msgspec.Struct, frozen=True, gc=False):
    """Cached cloud database authentication token and its expiration epoch timestamp."""

    token: str
    expires_at: float

    def __repr__(self) -> str:
        """Return a redacted representation that omits the cached token secret."""
        return f"TokenSnapshot(token='<redacted>', expires_at={self.expires_at!r})"


@runtime_checkable
class DatabaseTokenProvider(Protocol):
    """Protocol implemented by short-lived cloud database authentication token providers."""

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a valid token, refreshing if missing, forced, or within margin."""
        ...

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a valid token, refreshing if missing, forced, or within margin."""
        ...

    def invalidate(self) -> None:
        """Clear any cached token snapshot so the next resolution mints a fresh token."""
        ...


class AwsRdsIamTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """AWS RDS IAM database authentication token provider using SigV4 `rds-db` presigned URLs."""

    endpoint_host: str
    username: str
    port: int = 5432
    region: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    sts_endpoint: str | None = None
    ecs_endpoint: str | None = None
    imds_endpoint: str | None = None
    refresh_margin_seconds: float = 60.0
    token_ttl_seconds: int = _MAX_RDS_IAM_TTL_SECONDS
    clock: Callable[[], float] | None = None
    signer: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        """Normalize host, validate RDS token bounds and endpoints, and initialize cache state."""
        normalized_host = _normalize_host(self.endpoint_host, field_name="endpoint_host")
        if normalized_host != self.endpoint_host:
            msgspec.structs.force_setattr(self, "endpoint_host", normalized_host)
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            msg = f"port must be an integer between 1 and 65535, got {self.port!r}"
            raise ImproperConfigurationError(msg)
        normalized_region = _validate_timing_bounds(
            self.region,
            self.token_ttl_seconds,
            self.refresh_margin_seconds,
            max_ttl_seconds=_MAX_RDS_IAM_TTL_SECONDS,
        )
        if normalized_region != self.region:
            msgspec.structs.force_setattr(self, "region", normalized_region)
        normalized_user = self.username.strip()
        if not normalized_user:
            msg = "username must be a non-empty string"
            raise ImproperConfigurationError(msg)
        if normalized_user != self.username:
            msgspec.structs.force_setattr(self, "username", normalized_user)
        if self.sts_endpoint is not None:
            normalized_sts = _validate_aws_sts_url(self.sts_endpoint, field_name="sts_endpoint")
            if normalized_sts != self.sts_endpoint:
                msgspec.structs.force_setattr(self, "sts_endpoint", normalized_sts)
        if self.ecs_endpoint is not None:
            normalized_ecs, _ = _validate_ecs_credentials_url(self.ecs_endpoint)
            normalized_ecs = normalized_ecs.rstrip("/")
            if normalized_ecs != self.ecs_endpoint:
                msgspec.structs.force_setattr(self, "ecs_endpoint", normalized_ecs)
        if self.imds_endpoint is not None:
            normalized_imds = _validate_aws_imds_base_url(self.imds_endpoint, field_name="imds_endpoint")
            if normalized_imds != self.imds_endpoint:
                msgspec.structs.force_setattr(self, "imds_endpoint", normalized_imds)
        msgspec.structs.force_setattr(self, "_cache_slot", _TokenCacheSlot())

    def __repr__(self) -> str:
        """Return a redacted representation that omits AWS keys and session tokens."""
        return (
            f"AwsRdsIamTokenProvider(endpoint_host={self.endpoint_host!r}, port={self.port!r}, "
            f"region={self.region!r}, username={self.username!r}, "
            f"access_key_id={'<redacted>' if self.access_key_id else None!r}, "
            f"secret_access_key={'<redacted>' if self.secret_access_key else None!r}, "
            f"session_token={'<redacted>' if self.session_token else None!r}, "
            f"refresh_margin_seconds={self.refresh_margin_seconds!r}, "
            f"token_ttl_seconds={self.token_ttl_seconds!r})"
        )

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed RDS IAM database token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return _resolve_snapshot_sync(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_sync,
            force_refresh=force_refresh,
        )

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed RDS IAM database token without blocking the loop."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return await _resolve_snapshot_async(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_async,
            force_refresh=force_refresh,
        )

    def invalidate(self) -> None:
        """Invalidate the cached RDS IAM token snapshot."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        slot.clear()

    def _signer_context(self, now_ts: float) -> dict[str, Any]:
        return {
            "endpoint_host": self.endpoint_host,
            "host": self.endpoint_host,
            "port": self.port,
            "region": self.region,
            "username": self.username,
            "service": "rds-db",
            "action": "connect",
            "token_ttl_seconds": self.token_ttl_seconds,
            "ttl": self.token_ttl_seconds,
            "now_ts": now_ts,
        }

    def _mint_sigv4_snapshot(self, now_ts: float) -> TokenSnapshot:
        region = self.region
        assert region is not None
        creds = _resolve_aws_credentials(
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
            region=region,
            sts_endpoint=self.sts_endpoint,
            ecs_endpoint=self.ecs_endpoint,
            imds_endpoint=self.imds_endpoint,
        )
        token = _sign_sigv4_presigned_token(
            host_header=f"{self.endpoint_host}:{self.port}",
            region=region,
            service="rds-db",
            action="connect",
            ttl_seconds=self.token_ttl_seconds,
            now_ts=now_ts,
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
            db_user=self.username,
        )
        expires_at = now_ts + self.token_ttl_seconds
        if creds.expires_at is not None:
            expires_at = min(expires_at, creds.expires_at)
        return TokenSnapshot(token=token, expires_at=expires_at)

    def _acquire_sync(self, now_ts: float) -> TokenSnapshot:
        if self.signer is not None:
            raw = _invoke_custom_signer(self.signer, self._signer_context(now_ts))
            if inspect.isawaitable(raw):
                close_coro = getattr(raw, "close", None)
                if callable(close_coro):
                    close_coro()
                msg = "Async token signer cannot be used with resolve_sync(); use resolve_async()"
                raise ImproperConfigurationError(msg)
            return TokenSnapshot(token=_normalize_signed_token(raw), expires_at=now_ts + self.token_ttl_seconds)
        return self._mint_sigv4_snapshot(now_ts)

    async def _acquire_async(self, now_ts: float) -> TokenSnapshot:
        if self.signer is not None:
            raw = _invoke_custom_signer(self.signer, self._signer_context(now_ts))
            if inspect.isawaitable(raw):
                raw = await raw
            return TokenSnapshot(token=_normalize_signed_token(raw), expires_at=now_ts + self.token_ttl_seconds)
        return await anyio.to_thread.run_sync(self._mint_sigv4_snapshot, now_ts)


class AwsAuroraDsqlIamTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """AWS Aurora DSQL IAM authentication token provider using SigV4 `dsql` presigned URLs."""

    hostname: str
    region: str | None = None
    admin: bool = False
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    sts_endpoint: str | None = None
    ecs_endpoint: str | None = None
    imds_endpoint: str | None = None
    refresh_margin_seconds: float = 60.0
    token_ttl_seconds: int = _MAX_RDS_IAM_TTL_SECONDS
    clock: Callable[[], float] | None = None
    signer: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        """Normalize hostname, validate Aurora DSQL bounds and endpoints, and initialize cache state."""
        normalized_host = _normalize_host(self.hostname, field_name="hostname")
        if normalized_host != self.hostname:
            msgspec.structs.force_setattr(self, "hostname", normalized_host)
        normalized_region = _validate_timing_bounds(
            self.region,
            self.token_ttl_seconds,
            self.refresh_margin_seconds,
            max_ttl_seconds=_MAX_DSQL_IAM_TTL_SECONDS,
        )
        if normalized_region != self.region:
            msgspec.structs.force_setattr(self, "region", normalized_region)
        if self.sts_endpoint is not None:
            normalized_sts = _validate_aws_sts_url(self.sts_endpoint, field_name="sts_endpoint")
            if normalized_sts != self.sts_endpoint:
                msgspec.structs.force_setattr(self, "sts_endpoint", normalized_sts)
        if self.ecs_endpoint is not None:
            normalized_ecs, _ = _validate_ecs_credentials_url(self.ecs_endpoint)
            normalized_ecs = normalized_ecs.rstrip("/")
            if normalized_ecs != self.ecs_endpoint:
                msgspec.structs.force_setattr(self, "ecs_endpoint", normalized_ecs)
        if self.imds_endpoint is not None:
            normalized_imds = _validate_aws_imds_base_url(self.imds_endpoint, field_name="imds_endpoint")
            if normalized_imds != self.imds_endpoint:
                msgspec.structs.force_setattr(self, "imds_endpoint", normalized_imds)
        msgspec.structs.force_setattr(self, "_cache_slot", _TokenCacheSlot())

    def __repr__(self) -> str:
        """Return a redacted representation that omits AWS keys and session tokens."""
        return (
            f"AwsAuroraDsqlIamTokenProvider(hostname={self.hostname!r}, region={self.region!r}, "
            f"admin={self.admin!r}, "
            f"access_key_id={'<redacted>' if self.access_key_id else None!r}, "
            f"secret_access_key={'<redacted>' if self.secret_access_key else None!r}, "
            f"session_token={'<redacted>' if self.session_token else None!r}, "
            f"refresh_margin_seconds={self.refresh_margin_seconds!r}, "
            f"token_ttl_seconds={self.token_ttl_seconds!r})"
        )

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed Aurora DSQL IAM token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return _resolve_snapshot_sync(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_sync,
            force_refresh=force_refresh,
        )

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed Aurora DSQL IAM token without blocking the loop."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return await _resolve_snapshot_async(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_async,
            force_refresh=force_refresh,
        )

    def invalidate(self) -> None:
        """Invalidate the cached Aurora DSQL IAM token snapshot."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        slot.clear()

    def _signer_context(self, now_ts: float) -> dict[str, Any]:
        action = "DbConnectAdmin" if self.admin else "DbConnect"
        return {
            "hostname": self.hostname,
            "host": self.hostname,
            "region": self.region,
            "admin": self.admin,
            "service": "dsql",
            "action": action,
            "token_ttl_seconds": self.token_ttl_seconds,
            "ttl": self.token_ttl_seconds,
            "now_ts": now_ts,
        }

    def _mint_sigv4_snapshot(self, now_ts: float) -> TokenSnapshot:
        region = self.region
        assert region is not None
        action = "DbConnectAdmin" if self.admin else "DbConnect"
        creds = _resolve_aws_credentials(
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
            region=region,
            sts_endpoint=self.sts_endpoint,
            ecs_endpoint=self.ecs_endpoint,
            imds_endpoint=self.imds_endpoint,
        )
        token = _sign_sigv4_presigned_token(
            host_header=self.hostname,
            region=region,
            service="dsql",
            action=action,
            ttl_seconds=self.token_ttl_seconds,
            now_ts=now_ts,
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
        )
        expires_at = now_ts + self.token_ttl_seconds
        if creds.expires_at is not None:
            expires_at = min(expires_at, creds.expires_at)
        return TokenSnapshot(token=token, expires_at=expires_at)

    def _acquire_sync(self, now_ts: float) -> TokenSnapshot:
        if self.signer is not None:
            raw = _invoke_custom_signer(self.signer, self._signer_context(now_ts))
            if inspect.isawaitable(raw):
                close_coro = getattr(raw, "close", None)
                if callable(close_coro):
                    close_coro()
                msg = "Async token signer cannot be used with resolve_sync(); use resolve_async()"
                raise ImproperConfigurationError(msg)
            return TokenSnapshot(token=_normalize_signed_token(raw), expires_at=now_ts + self.token_ttl_seconds)
        return self._mint_sigv4_snapshot(now_ts)

    async def _acquire_async(self, now_ts: float) -> TokenSnapshot:
        if self.signer is not None:
            raw = _invoke_custom_signer(self.signer, self._signer_context(now_ts))
            if inspect.isawaitable(raw):
                raw = await raw
            return TokenSnapshot(token=_normalize_signed_token(raw), expires_at=now_ts + self.token_ttl_seconds)
        return await anyio.to_thread.run_sync(self._mint_sigv4_snapshot, now_ts)


class AzureEntraTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """Azure Entra ID database token provider supporting client secret, Workload Identity, MSI, and IMDS."""

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    federated_token_file: str | None = None
    authority_host: str | None = None
    scope: str | None = None
    token_endpoint: str | None = None
    imds_endpoint: str = _DEFAULT_ENTRA_IMDS_ENDPOINT
    api_version: str = _DEFAULT_ENTRA_API_VERSION
    refresh_margin_seconds: float = 120.0
    clock: Callable[[], float] | None = None
    fetcher: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        """Validate Entra token provider configuration and initialize lock-backed cache state."""
        if (
            isinstance(self.refresh_margin_seconds, bool)
            or not isinstance(self.refresh_margin_seconds, int | float)
            or not math.isfinite(self.refresh_margin_seconds)
            or self.refresh_margin_seconds < 0
        ):
            msg = f"refresh_margin_seconds must be a finite non-negative number, got {self.refresh_margin_seconds!r}"
            raise ImproperConfigurationError(msg)

        if self.tenant_id is not None:
            normalized_tenant = _validate_tenant_id(self.tenant_id)
            if normalized_tenant != self.tenant_id:
                msgspec.structs.force_setattr(self, "tenant_id", normalized_tenant)

        if self.client_id is not None:
            normalized_client_id = self.client_id.strip()
            if not normalized_client_id:
                msg = "client_id must be a non-empty string when provided"
                raise ImproperConfigurationError(msg)
            if normalized_client_id != self.client_id:
                msgspec.structs.force_setattr(self, "client_id", normalized_client_id)

        if self.client_secret is not None and not self.client_secret.strip():
            msg = "client_secret must be a non-empty string when provided"
            raise ImproperConfigurationError(msg)

        if self.federated_token_file is not None:
            normalized_ftf = self.federated_token_file.strip()
            if not normalized_ftf:
                msg = "federated_token_file must be a non-empty string when provided"
                raise ImproperConfigurationError(msg)
            if normalized_ftf != self.federated_token_file:
                msgspec.structs.force_setattr(self, "federated_token_file", normalized_ftf)

        if self.authority_host is not None:
            normalized_ah = _validate_azure_authority_host(self.authority_host, field_name="authority_host")
            if normalized_ah != self.authority_host:
                msgspec.structs.force_setattr(self, "authority_host", normalized_ah)

        if self.scope is not None:
            normalized_scope = self.scope.strip()
            if not normalized_scope:
                msg = "scope must be a non-empty string when provided"
                raise ImproperConfigurationError(msg)
            if normalized_scope != self.scope:
                msgspec.structs.force_setattr(self, "scope", normalized_scope)

        if self.token_endpoint is not None:
            normalized_token_ep = _require_https_unless_loopback(
                _validate_http_url(self.token_endpoint, field_name="token_endpoint"),
                field_name="token_endpoint",
            )
            if normalized_token_ep != self.token_endpoint:
                msgspec.structs.force_setattr(self, "token_endpoint", normalized_token_ep)

        normalized_imds = _validate_http_url(self.imds_endpoint, field_name="imds_endpoint")
        if normalized_imds != self.imds_endpoint:
            msgspec.structs.force_setattr(self, "imds_endpoint", normalized_imds)

        normalized_api_version = self.api_version.strip()
        if not normalized_api_version:
            msg = "api_version must be a non-empty string"
            raise ImproperConfigurationError(msg)
        if normalized_api_version != self.api_version:
            msgspec.structs.force_setattr(self, "api_version", normalized_api_version)

        if self.fetcher is None and (self.client_secret is not None or self.federated_token_file is not None):
            effective_client_id = self.client_id or (os.environ.get("AZURE_CLIENT_ID") or "").strip()
            effective_tenant_id = self.tenant_id or (os.environ.get("AZURE_TENANT_ID") or "").strip()
            if not effective_client_id:
                msg = "Azure Entra client authentication requires client_id or AZURE_CLIENT_ID"
                raise ImproperConfigurationError(msg)
            if self.token_endpoint is None:
                if not effective_tenant_id:
                    msg = "Azure Entra client authentication requires tenant_id, AZURE_TENANT_ID, or token_endpoint"
                    raise ImproperConfigurationError(msg)
                _validate_tenant_id(effective_tenant_id)

        msgspec.structs.force_setattr(self, "_cache_slot", _TokenCacheSlot())

    def __repr__(self) -> str:
        """Return a redacted representation that omits client secrets and cached tokens."""
        return (
            f"AzureEntraTokenProvider(tenant_id={self.tenant_id!r}, client_id={self.client_id!r}, "
            f"client_secret={'<redacted>' if self.client_secret else None!r}, "
            f"federated_token_file={self.federated_token_file!r}, "
            f"authority_host={self.authority_host!r}, "
            f"scope={self.scope!r}, token_endpoint={self.token_endpoint!r}, "
            f"imds_endpoint={self.imds_endpoint!r}, api_version={self.api_version!r}, "
            f"refresh_margin_seconds={self.refresh_margin_seconds!r})"
        )

    def with_default_scope(self, scope: str) -> AzureEntraTokenProvider:
        """Return a provider copy with `scope` populated when `self.scope` is unset."""
        normalized_scope = scope.strip()
        if not normalized_scope:
            msg = "Default Azure Entra scope must be a non-empty string"
            raise ImproperConfigurationError(msg)
        if self.scope is not None:
            return self
        return msgspec.structs.replace(self, scope=normalized_scope)

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly acquired Azure Entra ID token."""
        self._require_scope()
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return _resolve_snapshot_sync(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_sync,
            force_refresh=force_refresh,
        )

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly acquired Azure Entra ID token without blocking the loop."""
        self._require_scope()
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return await _resolve_snapshot_async(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self._acquire_async,
            force_refresh=force_refresh,
        )

    def invalidate(self) -> None:
        """Invalidate the cached Azure Entra ID token snapshot."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        slot.clear()

    def _require_scope(self) -> str:
        if not self.scope:
            msg = "AzureEntraTokenProvider requires 'scope' before resolving tokens"
            raise ImproperConfigurationError(msg)
        return self.scope

    def _invoke_fetcher(self, scope: str) -> Any:
        fetcher = self.fetcher
        assert fetcher is not None
        sig = inspect.signature(fetcher)
        params = sig.parameters
        if not params:
            return fetcher()
        scope_param = params.get("scope")
        if scope_param is not None and scope_param.kind == inspect.Parameter.KEYWORD_ONLY:
            return fetcher(scope=scope)
        return fetcher(scope)

    def _resolve_oauth_token_url(self, effective_tenant_id: str | None) -> str:
        if self.token_endpoint is not None:
            return self.token_endpoint
        if not effective_tenant_id:
            msg = "Azure Entra OAuth2 token acquisition requires tenant_id, AZURE_TENANT_ID, or token_endpoint"
            raise ImproperConfigurationError(msg)
        validated_tenant = _validate_tenant_id(effective_tenant_id)
        raw_authority = (
            self.authority_host
            or (os.environ.get("AZURE_AUTHORITY_HOST") or "").strip()
            or _DEFAULT_ENTRA_AUTHORITY_HOST
        )
        authority = _validate_azure_authority_host(raw_authority, field_name="AZURE_AUTHORITY_HOST")
        return f"{authority}/{quote(validated_tenant, safe='')}/oauth2/v2.0/token"

    def _fetch_client_credentials_snapshot(
        self,
        now_ts: float,
        scope: str,
        *,
        client_id: str | None,
        client_secret: str,
        tenant_id: str | None,
    ) -> TokenSnapshot:
        if not client_id:
            msg = "Azure Entra client-credentials authentication requires client_id or AZURE_CLIENT_ID"
            raise ImproperConfigurationError(msg)
        if not client_secret.strip():
            msg = "Azure Entra client-credentials authentication requires a non-empty client_secret"
            raise ImproperConfigurationError(msg)
        url = self._resolve_oauth_token_url(tenant_id)
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        host = (urlsplit(url).hostname or "").lower()
        return _execute_entra_token_request(
            req,
            now_ts=now_ts,
            bypass_proxy=_should_bypass_proxy(host),
        )

    def _fetch_workload_identity_snapshot(
        self,
        now_ts: float,
        scope: str,
        *,
        client_id: str | None,
        token_file: str,
        tenant_id: str | None,
    ) -> TokenSnapshot:
        if not client_id:
            msg = "Azure Entra Workload Identity authentication requires client_id or AZURE_CLIENT_ID"
            raise ImproperConfigurationError(msg)
        assertion = _read_secret_token_file(token_file, label="Azure federated token file")
        url = self._resolve_oauth_token_url(tenant_id)
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_assertion_type": _JWT_BEARER_ASSERTION_TYPE,
                "client_assertion": assertion,
                "scope": scope,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        host = (urlsplit(url).hostname or "").lower()
        return _execute_entra_token_request(
            req,
            now_ts=now_ts,
            bypass_proxy=_should_bypass_proxy(host),
        )

    def _fetch_app_service_msi_snapshot(
        self,
        now_ts: float,
        scope: str,
        *,
        identity_endpoint: str,
        identity_header: str,
        client_id: str | None,
    ) -> TokenSnapshot:
        validated_ep = _validate_http_url(identity_endpoint, field_name="IDENTITY_ENDPOINT")
        if "\r" in identity_header or "\n" in identity_header:
            msg = "IDENTITY_HEADER must not contain CR or LF characters"
            raise ImproperConfigurationError(msg)
        resource = scope.removesuffix("/.default")
        if not resource:
            msg = "Azure Entra Managed Identity resource derived from scope cannot be empty"
            raise ImproperConfigurationError(msg)
        parsed = urlsplit(validated_ep)
        existing_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_pairs = [(k, v) for k, v in existing_pairs if k not in {"api-version", "resource", "client_id"}]
        query_pairs.append(("api-version", self.api_version))
        query_pairs.append(("resource", resource))
        if client_id:
            query_pairs.append(("client_id", client_id))
        url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_pairs), ""))
        req = urllib.request.Request(
            url,
            headers={
                "X-IDENTITY-HEADER": identity_header,
                "Metadata": "true",
                "Accept": "application/json",
            },
            method="GET",
        )
        return _execute_entra_token_request(req, now_ts=now_ts, bypass_proxy=True)

    def _fetch_imds_snapshot(self, now_ts: float, scope: str, *, client_id: str | None) -> TokenSnapshot:
        resource = scope.removesuffix("/.default")
        if not resource:
            msg = "Azure Entra IMDS resource derived from scope cannot be empty"
            raise ImproperConfigurationError(msg)
        parsed = urlsplit(self.imds_endpoint)
        existing_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_pairs = [(k, v) for k, v in existing_pairs if k not in {"api-version", "resource", "client_id"}]
        query_pairs.append(("api-version", self.api_version))
        query_pairs.append(("resource", resource))
        if client_id:
            query_pairs.append(("client_id", client_id))
        url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_pairs), ""))
        req = urllib.request.Request(
            url,
            headers={
                "Metadata": "true",
                "Accept": "application/json",
            },
            method="GET",
        )
        return _execute_entra_token_request(req, now_ts=now_ts, bypass_proxy=True)

    def _acquire_ambient_sync(self, now_ts: float, scope: str) -> TokenSnapshot:
        effective_client_id = self.client_id or (os.environ.get("AZURE_CLIENT_ID") or "").strip() or None
        effective_tenant_id = self.tenant_id or (os.environ.get("AZURE_TENANT_ID") or "").strip() or None

        if self.client_secret is not None:
            return self._fetch_client_credentials_snapshot(
                now_ts,
                scope,
                client_id=effective_client_id,
                client_secret=self.client_secret,
                tenant_id=effective_tenant_id,
            )

        if self.federated_token_file is not None:
            return self._fetch_workload_identity_snapshot(
                now_ts,
                scope,
                client_id=effective_client_id,
                token_file=self.federated_token_file,
                tenant_id=effective_tenant_id,
            )

        if self.imds_endpoint != _DEFAULT_ENTRA_IMDS_ENDPOINT:
            return self._fetch_imds_snapshot(now_ts, scope, client_id=effective_client_id)

        env_secret = (os.environ.get("AZURE_CLIENT_SECRET") or "").strip()
        if env_secret:
            return self._fetch_client_credentials_snapshot(
                now_ts,
                scope,
                client_id=effective_client_id,
                client_secret=env_secret,
                tenant_id=effective_tenant_id,
            )

        env_fed_file = (os.environ.get("AZURE_FEDERATED_TOKEN_FILE") or "").strip()
        if env_fed_file:
            return self._fetch_workload_identity_snapshot(
                now_ts,
                scope,
                client_id=effective_client_id,
                token_file=env_fed_file,
                tenant_id=effective_tenant_id,
            )

        identity_endpoint = (os.environ.get("IDENTITY_ENDPOINT") or "").strip()
        identity_header = (os.environ.get("IDENTITY_HEADER") or "").strip()
        if identity_endpoint or identity_header:
            if not identity_endpoint or not identity_header:
                msg = "Azure Managed Identity requires both IDENTITY_ENDPOINT and IDENTITY_HEADER"
                raise ImproperConfigurationError(msg)
            return self._fetch_app_service_msi_snapshot(
                now_ts,
                scope,
                identity_endpoint=identity_endpoint,
                identity_header=identity_header,
                client_id=effective_client_id,
            )

        return self._fetch_imds_snapshot(now_ts, scope, client_id=effective_client_id)

    def _acquire_sync(self, now_ts: float) -> TokenSnapshot:
        scope = self._require_scope()
        if self.fetcher is not None:
            raw = self._invoke_fetcher(scope)
            if inspect.isawaitable(raw):
                close_coro = getattr(raw, "close", None)
                if callable(close_coro):
                    close_coro()
                msg = "Async token fetcher cannot be used with resolve_sync(); use resolve_async()"
                raise ImproperConfigurationError(msg)
            return _normalize_entra_snapshot(raw, now_ts=now_ts)
        return self._acquire_ambient_sync(now_ts, scope)

    async def _acquire_async(self, now_ts: float) -> TokenSnapshot:
        scope = self._require_scope()
        if self.fetcher is not None:
            raw = self._invoke_fetcher(scope)
            if inspect.isawaitable(raw):
                raw = await raw
            return _normalize_entra_snapshot(raw, now_ts=now_ts)
        return await anyio.to_thread.run_sync(self._acquire_ambient_sync, now_ts, scope)


class _TokenCacheSlot:
    """Cached token snapshot plus refresh serialization shared by one frozen token provider.

    `state_lock` guards only the cached-snapshot read and write and is never held across I/O or an
    await, so taking it on an event-loop thread cannot stall the loop. `sync_refresh_lock` keeps one
    `resolve_sync` refresh in flight and is held across network I/O, so only blocking callers take it.
    Async refreshes are serialized per running event loop through `async_refresh_lock()`, because an
    async lock cannot be shared between event loops.
    """

    __slots__ = ("__weakref__", "snapshot", "state_lock", "sync_refresh_lock")

    def __init__(self) -> None:
        self.state_lock = threading.Lock()
        self.sync_refresh_lock = threading.Lock()
        self.snapshot: TokenSnapshot | None = None

    def fresh_token(self, now: float, refresh_margin_seconds: float) -> str | None:
        """Return the cached token when it stays valid beyond the refresh margin."""
        with self.state_lock:
            snap = self.snapshot
        if snap is not None and now + refresh_margin_seconds < snap.expires_at:
            return snap.token
        return None

    def store(self, snapshot: TokenSnapshot) -> None:
        """Replace the cached token snapshot."""
        with self.state_lock:
            self.snapshot = snapshot

    def clear(self) -> None:
        """Discard any cached token snapshot."""
        with self.state_lock:
            self.snapshot = None

    def async_refresh_lock(self) -> anyio.Lock:
        """Return the refresh lock owned by the running event loop, creating it on first use."""
        try:
            loop_locks = _LOOP_REFRESH_LOCKS.get()
        except LookupError:
            loop_locks = weakref.WeakKeyDictionary()
            _LOOP_REFRESH_LOCKS.set(loop_locks)
        lock = loop_locks.get(self)
        if lock is None:
            lock = anyio.Lock()
            loop_locks[self] = lock
        return lock


_LOOP_REFRESH_LOCKS: Final[RunVar[weakref.WeakKeyDictionary[_TokenCacheSlot, anyio.Lock]]] = RunVar(
    "vertebra_db_token_refresh_locks"
)


class _CredentialResponseRejectedError(ImproperConfigurationError):
    """Raised when a credential endpoint answers with a redirect or an oversized body."""


class _RefuseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Redirect handler that fails closed so credential headers and bodies never reach a second host."""

    def __init__(self, endpoint_label: str) -> None:
        self._endpoint_label = endpoint_label

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        """Refuse every redirect without contacting or naming the redirect target."""
        fp.close()
        error_msg = (
            f"{self._endpoint_label} answered with an HTTP {code} redirect; credential requests never follow redirects"
        )
        raise _CredentialResponseRejectedError(error_msg)


class _ResolvedAwsCredentials(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Resolved AWS IAM signing credentials and optional temporary credential expiry epoch."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    expires_at: float | None = None


def _strip_url_scheme(raw: str) -> str:
    """Strip an `https://` or `http://` scheme prefix without modifying the remainder."""
    stripped = raw.strip()
    lowered = stripped.lower()
    if lowered.startswith("https://"):
        return stripped[8:]
    if lowered.startswith("http://"):
        return stripped[7:]
    return stripped


def _normalize_host(raw: str, *, field_name: str) -> str:
    """Normalize a hostname by stripping URL scheme prefixes and trailing slashes."""
    host = _strip_url_scheme(raw).rstrip("/")
    if not host or "/" in host:
        msg = f"{field_name} must be a non-empty hostname without path segments"
        raise ImproperConfigurationError(msg)
    return host


def is_loopback_host(host: str) -> bool:
    """Return True when `host` is syntactically `localhost` or an IPv4/IPv6 loopback literal.

    A `%zone` suffix is accepted only on an IPv6 literal, so a hostname containing `%` is never loopback.
    """
    cleaned = host.strip().lower()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    if cleaned in {"localhost", "localhost."}:
        return True
    try:
        addr = ipaddress.ip_address(cleaned)
    except ValueError:
        return False
    if addr.is_loopback:
        return True
    return isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None and addr.ipv4_mapped.is_loopback


def _should_bypass_proxy(host: str) -> bool:
    """Return True when `host` is a loopback, link-local, or EC2 IPv6 metadata address."""
    if is_loopback_host(host):
        return True
    cleaned = host.strip().lower()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    try:
        ip = ipaddress.ip_address(cleaned)
    except ValueError:
        return False
    if ip.is_link_local:
        return True
    return isinstance(ip, ipaddress.IPv6Address) and ip in _AWS_IPV6_METADATA_NETWORK


def _validate_http_url(raw: str, *, field_name: str) -> str:
    """Validate and normalize an HTTP or HTTPS endpoint URL."""
    normalized = raw.strip()
    parsed = urlsplit(normalized)
    if not normalized or parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc or not parsed.hostname:
        msg = f"{field_name} must be a valid HTTP or HTTPS URL"
        raise ImproperConfigurationError(msg)
    return normalized


def _require_https_unless_loopback(normalized: str, *, field_name: str) -> str:
    """Reject a plain-HTTP URL whose host is not a local loopback endpoint."""
    parsed = urlsplit(normalized)
    if parsed.scheme.lower() == "http" and not is_loopback_host((parsed.hostname or "").lower()):
        msg = f"{field_name} must use HTTPS unless targeting a local loopback endpoint"
        raise ImproperConfigurationError(msg)
    return normalized


def _validate_aws_sts_url(raw: str, *, field_name: str) -> str:
    """Validate an AWS STS endpoint URL, requiring HTTPS for non-loopback hosts."""
    return _require_https_unless_loopback(_validate_http_url(raw, field_name=field_name), field_name=field_name)


def _validate_ecs_credentials_url(raw_url: str) -> tuple[str, bool]:
    """Validate an AWS ECS / Pod Identity credentials URL against the host and HTTPS allowlist."""
    normalized = _validate_http_url(raw_url, field_name="AWS_CONTAINER_CREDENTIALS_URI")
    parsed = urlsplit(normalized)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    bypass_proxy = _should_bypass_proxy(host)
    if scheme == "https":
        return normalized, bypass_proxy
    if is_loopback_host(host) or host in _AWS_ECS_LINK_LOCAL_HOSTS:
        return normalized, True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if isinstance(ip, ipaddress.IPv6Address) and ip in _AWS_IPV6_METADATA_NETWORK:
        return normalized, True
    msg = (
        f"ECS container credentials HTTP host {host!r} is not allowlisted; "
        "require HTTPS, loopback, 169.254.170.2, or fd00:ec2::/32"
    )
    raise ImproperConfigurationError(msg)


def _validate_aws_imds_base_url(raw_url: str, *, field_name: str) -> str:
    """Validate an EC2 IMDSv2 base URL against loopback, link-local IMDS, IPv6 metadata, or HTTPS."""
    normalized = _validate_http_url(raw_url, field_name=field_name).rstrip("/")
    parsed = urlsplit(normalized)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    if scheme == "https" or is_loopback_host(host) or host in _AWS_IMDS_LINK_LOCAL_HOSTS:
        return normalized
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if isinstance(ip, ipaddress.IPv6Address) and ip in _AWS_IPV6_METADATA_NETWORK:
        return normalized
    msg = f"{field_name} HTTP host {host!r} is not allowlisted for EC2 IMDSv2"
    raise ImproperConfigurationError(msg)


def _validate_azure_authority_host(raw: str, *, field_name: str) -> str:
    """Validate an Azure authority base URL, requiring HTTPS for non-loopback hosts."""
    normalized = _validate_http_url(raw, field_name=field_name).rstrip("/")
    return _require_https_unless_loopback(normalized, field_name=field_name)


def _validate_tenant_id(tenant_id: str) -> str:
    """Validate an Azure Entra tenant identifier (GUID or domain) against path injection."""
    normalized = tenant_id.strip()
    if (
        not normalized
        or normalized in {".", ".."}
        or ".." in normalized
        or not _VALID_TENANT_ID_RE.fullmatch(normalized)
    ):
        msg = "tenant_id must be a valid Azure Entra tenant GUID or domain name"
        raise ImproperConfigurationError(msg)
    return normalized


def _read_ini_section(path: Path, section_candidates: tuple[str, ...]) -> dict[str, str]:
    """Read key-value settings from the first matching INI section in an AWS config or credentials file."""
    if not path.is_file():
        return {}
    parser = configparser.RawConfigParser()
    try:
        content = path.read_text(encoding="utf-8")
        parser.read_string(content, source=str(path))
    except (OSError, configparser.Error) as exc:
        msg = f"Failed to read AWS configuration file '{path}'"
        raise ImproperConfigurationError(msg) from exc
    for section in section_candidates:
        if parser.has_section(section):
            return {k.strip().lower(): v.strip() for k, v in parser.items(section) if v.strip()}
    return {}


def _load_aws_profile_settings() -> tuple[str, dict[str, str], bool]:
    """Load merged `~/.aws/config` and `~/.aws/credentials` settings for the active profile."""
    explicit_profile = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
    profile = (explicit_profile or "default").strip() or "default"
    config_path = Path(os.environ.get("AWS_CONFIG_FILE") or "~/.aws/config").expanduser()
    creds_path = Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE") or "~/.aws/credentials").expanduser()
    config_candidates = ("default", "profile default") if profile == "default" else (f"profile {profile}", profile)
    config_vals = _read_ini_section(config_path, config_candidates)
    creds_vals = _read_ini_section(creds_path, (profile,))
    merged = dict(config_vals)
    merged.update(creds_vals)
    found_any_section = bool(config_vals or creds_vals)
    return profile, merged, found_any_section


def _resolve_ambient_aws_region() -> str:
    """Resolve an omitted AWS region from `AWS_REGION`, `AWS_DEFAULT_REGION`, or active `~/.aws` profile."""
    env_region = (os.environ.get("AWS_REGION") or "").strip() or (os.environ.get("AWS_DEFAULT_REGION") or "").strip()
    if env_region:
        return env_region
    _, profile_vals, _ = _load_aws_profile_settings()
    profile_region = profile_vals.get("region", "").strip()
    if profile_region:
        return profile_region
    msg = "AWS region must be provided via 'region', AWS_REGION, AWS_DEFAULT_REGION, or ~/.aws/config"
    raise ImproperConfigurationError(msg)


def _validate_timing_bounds(
    region: str | None,
    token_ttl_seconds: int,
    refresh_margin_seconds: float,
    *,
    max_ttl_seconds: int,
) -> str:
    """Validate AWS region (or resolve ambient fallback), bounded TTL ceiling, and pre-expiry margin."""
    if region is None:
        normalized_region = _resolve_ambient_aws_region()
    else:
        normalized_region = region.strip()
        if not normalized_region:
            msg = "AWS region must be a non-empty string"
            raise ImproperConfigurationError(msg)
    if (
        isinstance(token_ttl_seconds, bool)
        or not isinstance(token_ttl_seconds, int)
        or token_ttl_seconds <= 0
        or token_ttl_seconds > max_ttl_seconds
    ):
        msg = f"token_ttl_seconds must be an integer between 1 and {max_ttl_seconds}, got {token_ttl_seconds!r}"
        raise ImproperConfigurationError(msg)
    if (
        isinstance(refresh_margin_seconds, bool)
        or not isinstance(refresh_margin_seconds, int | float)
        or not math.isfinite(refresh_margin_seconds)
        or refresh_margin_seconds < 0
        or refresh_margin_seconds >= token_ttl_seconds
    ):
        msg = (
            f"refresh_margin_seconds must be finite, non-negative, and less than token_ttl_seconds "
            f"({token_ttl_seconds}), got {refresh_margin_seconds!r}"
        )
        raise ImproperConfigurationError(msg)
    return normalized_region


def _read_secret_token_file(file_path: str, *, label: str) -> str:
    """Read a non-empty bearer or federated assertion token from disk without caching or leaking contents."""
    try:
        raw = Path(file_path).expanduser().read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"Failed to read {label}"
        raise ImproperConfigurationError(msg) from exc
    cleaned = raw.strip()
    if not cleaned:
        msg = f"{label} cannot be empty"
        raise ImproperConfigurationError(msg)
    return cleaned


def _parse_iso8601_timestamp(raw: Any, *, field_name: str) -> float:
    """Parse an ISO-8601 UTC timestamp string or positive epoch number into a finite UTC epoch float."""
    if isinstance(raw, bool):
        msg = f"Credential response field '{field_name}' must be a valid timestamp"
        raise ImproperConfigurationError(msg)
    if isinstance(raw, int | float):
        ts = float(raw)
        if not math.isfinite(ts) or ts <= 0:
            msg = f"Credential response field '{field_name}' must be finite and positive"
            raise ImproperConfigurationError(msg)
        return ts
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            msg = f"Credential response field '{field_name}' cannot be empty"
            raise ImproperConfigurationError(msg)
        try:
            dt = datetime.fromisoformat(stripped)
        except ValueError as exc:
            msg = f"Credential response field '{field_name}' must be a valid ISO-8601 timestamp"
            raise ImproperConfigurationError(msg) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        ts = dt.timestamp()
        if not math.isfinite(ts) or ts <= 0:
            msg = f"Credential response field '{field_name}' must be finite and positive"
            raise ImproperConfigurationError(msg)
        return ts
    msg = f"Credential response field '{field_name}' must be a valid timestamp"
    raise ImproperConfigurationError(msg)


def _read_credential_response(
    req: urllib.request.Request,
    *,
    timeout: float,
    bypass_proxy: bool,
    endpoint_label: str,
) -> bytes:
    """Send one credential HTTP request that refuses redirects and reads at most the response size cap."""
    handlers: list[urllib.request.BaseHandler] = [_RefuseRedirectHandler(endpoint_label)]
    if bypass_proxy:
        handlers.append(urllib.request.ProxyHandler({}))
    opener = urllib.request.build_opener(*handlers)
    with opener.open(req, timeout=timeout) as resp:
        raw_bytes = bytes(resp.read(_MAX_CREDENTIAL_RESPONSE_BYTES + 1))
    if len(raw_bytes) > _MAX_CREDENTIAL_RESPONSE_BYTES:
        msg = f"{endpoint_label} response exceeds the {_MAX_CREDENTIAL_RESPONSE_BYTES}-byte limit"
        raise _CredentialResponseRejectedError(msg)
    return raw_bytes


def _open_aws_http_bytes(
    req: urllib.request.Request,
    *,
    timeout: float,
    bypass_proxy: bool,
    endpoint_label: str,
) -> bytes:
    """Execute an AWS credential HTTP request with optional proxy bypass and secret-redacted errors."""
    try:
        return _read_credential_response(
            req,
            timeout=timeout,
            bypass_proxy=bypass_proxy,
            endpoint_label=endpoint_label,
        )
    except urllib.error.HTTPError as exc:
        msg = f"{endpoint_label} returned HTTP {exc.code}"
        raise ImproperConfigurationError(msg) from exc
    except urllib.error.URLError as exc:
        msg = f"{endpoint_label} request failed: {type(exc.reason).__name__}"
        raise ImproperConfigurationError(msg) from exc
    except OSError as exc:
        msg = f"{endpoint_label} request failed: {type(exc).__name__}"
        raise ImproperConfigurationError(msg) from exc


def _parse_aws_credentials_json_bytes(raw_bytes: bytes, *, source_label: str) -> _ResolvedAwsCredentials:
    """Decode an ECS or EC2 IMDSv2 JSON credential payload via centralized `decode_json`."""
    try:
        decoded = decode_json(raw_bytes)
    except Exception as exc:
        msg = f"Malformed {source_label} JSON payload"
        raise ImproperConfigurationError(msg) from exc
    if not isinstance(decoded, Mapping):
        msg = f"{source_label} JSON payload must be an object"
        raise ImproperConfigurationError(msg)
    code_val = decoded.get("Code")
    if code_val is not None and str(code_val).strip().lower() != "success":
        msg = f"{source_label} returned non-success status code"
        raise ImproperConfigurationError(msg)
    raw_ak = decoded.get("AccessKeyId")
    raw_sk = decoded.get("SecretAccessKey")
    if not isinstance(raw_ak, str) or not raw_ak.strip() or not isinstance(raw_sk, str) or not raw_sk.strip():
        msg = f"{source_label} response missing non-empty AccessKeyId or SecretAccessKey"
        raise ImproperConfigurationError(msg)
    raw_token = decoded.get("Token")
    if raw_token is None:
        raw_token = decoded.get("SessionToken")
    session_token: str | None = None
    if raw_token is not None:
        if not isinstance(raw_token, str):
            msg = f"{source_label} response field 'Token' must be a string"
            raise ImproperConfigurationError(msg)
        session_token = raw_token.strip() or None
    raw_exp = decoded.get("Expiration")
    expires_at = _parse_iso8601_timestamp(raw_exp, field_name="Expiration") if raw_exp is not None else None
    return _ResolvedAwsCredentials(
        access_key_id=raw_ak.strip(),
        secret_access_key=raw_sk.strip(),
        session_token=session_token,
        expires_at=expires_at,
    )


def _parse_sts_web_identity_xml(raw_xml: bytes) -> _ResolvedAwsCredentials:
    """Parse an AWS STS `AssumeRoleWithWebIdentityResponse` XML document while rejecting DTD declarations."""
    upper_head = raw_xml.upper()
    if b"<!DOCTYPE" in upper_head or b"<!ENTITY" in upper_head:
        msg = "AWS STS XML response must not contain DTD or ENTITY declarations"
        raise ImproperConfigurationError(msg)
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        msg = "Malformed AWS STS AssumeRoleWithWebIdentity XML response"
        raise ImproperConfigurationError(msg) from exc

    creds_elem: ET.Element | None = None
    for elem in root.iter():
        local_name = elem.tag.rsplit("}", 1)[-1]
        if local_name == "Credentials":
            creds_elem = elem
            break
    if creds_elem is None:
        msg = "AWS STS XML response missing <Credentials> element"
        raise ImproperConfigurationError(msg)

    fields: dict[str, str] = {}
    for child in creds_elem:
        local_tag = child.tag.rsplit("}", 1)[-1]
        if child.text and child.text.strip():
            fields[local_tag] = child.text.strip()

    ak = fields.get("AccessKeyId", "")
    sk = fields.get("SecretAccessKey", "")
    if not ak or not sk:
        msg = "AWS STS XML <Credentials> missing AccessKeyId or SecretAccessKey"
        raise ImproperConfigurationError(msg)
    st = fields.get("SessionToken") or None
    raw_exp = fields.get("Expiration")
    expires_at = _parse_iso8601_timestamp(raw_exp, field_name="Expiration") if raw_exp is not None else None
    return _ResolvedAwsCredentials(
        access_key_id=ak,
        secret_access_key=sk,
        session_token=st,
        expires_at=expires_at,
    )


def _fetch_sts_web_identity_credentials(
    *,
    token_file: str,
    role_arn: str,
    session_name: str,
    region: str,
    sts_endpoint: str | None,
) -> _ResolvedAwsCredentials:
    """Re-read the Web Identity JWT token file and exchange it via AWS STS `AssumeRoleWithWebIdentity`."""
    web_identity_token = _read_secret_token_file(token_file, label="AWS Web Identity token file")
    raw_url = (
        sts_endpoint
        or (os.environ.get("AWS_ENDPOINT_URL_STS") or "").strip()
        or (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
        or f"https://sts.{region}.amazonaws.com/"
    )
    validated_url = _validate_aws_sts_url(raw_url, field_name="AWS STS endpoint")
    body = urlencode(
        {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "RoleArn": role_arn,
            "RoleSessionName": session_name,
            "WebIdentityToken": web_identity_token,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        validated_url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "text/xml, application/xml",
        },
        method="POST",
    )
    host = (urlsplit(validated_url).hostname or "").lower()
    raw_xml = _open_aws_http_bytes(
        req,
        timeout=_DEFAULT_AWS_HTTP_TIMEOUT_SECONDS,
        bypass_proxy=_should_bypass_proxy(host),
        endpoint_label="AWS STS endpoint",
    )
    return _parse_sts_web_identity_xml(raw_xml)


def _fetch_ecs_container_credentials(
    *, rel_uri: str, full_uri: str, ecs_endpoint: str | None
) -> _ResolvedAwsCredentials:
    """Fetch temporary IAM credentials from an allowlisted ECS or EKS Pod Identity container endpoint."""
    if rel_uri:
        if not rel_uri.startswith("/") or rel_uri.startswith("//"):
            msg = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must start with a single '/'"
            raise ImproperConfigurationError(msg)
        base = (ecs_endpoint or _DEFAULT_AWS_ECS_ENDPOINT).rstrip("/")
        candidate_url = f"{base}{rel_uri}"
    else:
        candidate_url = full_uri
    validated_url, bypass_proxy = _validate_ecs_credentials_url(candidate_url)

    headers: dict[str, str] = {"Accept": "application/json"}
    auth_token_file = (os.environ.get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") or "").strip()
    auth_token_env = (os.environ.get("AWS_CONTAINER_AUTHORIZATION_TOKEN") or "").strip()
    if auth_token_file:
        auth_token = _read_secret_token_file(
            auth_token_file,
            label="AWS container authorization token file",
        )
        if "\r" in auth_token or "\n" in auth_token:
            msg = "AWS container authorization token must not contain CR or LF characters"
            raise ImproperConfigurationError(msg)
        headers["Authorization"] = auth_token
    elif auth_token_env:
        if "\r" in auth_token_env or "\n" in auth_token_env:
            msg = "AWS_CONTAINER_AUTHORIZATION_TOKEN must not contain CR or LF characters"
            raise ImproperConfigurationError(msg)
        headers["Authorization"] = auth_token_env

    req = urllib.request.Request(validated_url, headers=headers, method="GET")
    raw_bytes = _open_aws_http_bytes(
        req,
        timeout=_DEFAULT_AWS_HTTP_TIMEOUT_SECONDS,
        bypass_proxy=bypass_proxy,
        endpoint_label="AWS ECS container credentials endpoint",
    )
    return _parse_aws_credentials_json_bytes(raw_bytes, source_label="AWS ECS container credentials")


def _fetch_ec2_imdsv2_credentials(*, imds_endpoint: str | None) -> _ResolvedAwsCredentials:
    """Acquire temporary IAM role credentials from EC2 IMDSv2 using a 2-step PUT token exchange."""
    if (os.environ.get("AWS_EC2_METADATA_DISABLED") or "").strip().lower() == "true":
        msg = "AWS IAM database token signing requires AWS credentials or a custom signer"
        raise ImproperConfigurationError(msg)

    env_imds = (os.environ.get("AWS_EC2_METADATA_SERVICE_ENDPOINT") or "").strip()
    endpoint_mode = (os.environ.get("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE") or "").strip().lower()
    default_imds = _DEFAULT_AWS_IMDS_IPV6_ENDPOINT if endpoint_mode == "ipv6" else _DEFAULT_AWS_IMDS_IPV4_ENDPOINT
    base_url = _validate_aws_imds_base_url(
        imds_endpoint or env_imds or default_imds,
        field_name="AWS_EC2_METADATA_SERVICE_ENDPOINT",
    )

    token_req = urllib.request.Request(
        f"{base_url}/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        method="PUT",
    )
    try:
        imds_token_bytes = _open_aws_http_bytes(
            token_req,
            timeout=_DEFAULT_AWS_IMDS_TIMEOUT_SECONDS,
            bypass_proxy=True,
            endpoint_label="AWS EC2 IMDSv2 token endpoint",
        )
    except _CredentialResponseRejectedError:
        raise
    except ImproperConfigurationError as exc:
        msg = "AWS IAM database token signing requires AWS credentials or a custom signer"
        raise ImproperConfigurationError(msg) from exc

    imds_token = imds_token_bytes.decode("utf-8", errors="replace").strip()
    if not imds_token or "\r" in imds_token or "\n" in imds_token:
        msg = "AWS EC2 IMDSv2 returned an invalid metadata session token"
        raise ImproperConfigurationError(msg)

    role_list_req = urllib.request.Request(
        f"{base_url}/latest/meta-data/iam/security-credentials/",
        headers={"X-aws-ec2-metadata-token": imds_token},
        method="GET",
    )
    role_list_bytes = _open_aws_http_bytes(
        role_list_req,
        timeout=_DEFAULT_AWS_IMDS_TIMEOUT_SECONDS,
        bypass_proxy=True,
        endpoint_label="AWS EC2 IMDSv2 role discovery endpoint",
    )
    role_lines = [
        line.strip() for line in role_list_bytes.decode("utf-8", errors="replace").splitlines() if line.strip()
    ]
    if not role_lines:
        msg = "AWS EC2 IMDSv2 reported no IAM role attached to instance"
        raise ImproperConfigurationError(msg)
    role_name = role_lines[0]
    if "/" in role_name or "\\" in role_name or ".." in role_name:
        msg = "AWS EC2 IMDSv2 returned an invalid IAM role name"
        raise ImproperConfigurationError(msg)

    creds_req = urllib.request.Request(
        f"{base_url}/latest/meta-data/iam/security-credentials/{quote(role_name, safe='')}",
        headers={
            "X-aws-ec2-metadata-token": imds_token,
            "Accept": "application/json",
        },
        method="GET",
    )
    creds_bytes = _open_aws_http_bytes(
        creds_req,
        timeout=_DEFAULT_AWS_IMDS_TIMEOUT_SECONDS,
        bypass_proxy=True,
        endpoint_label="AWS EC2 IMDSv2 credentials endpoint",
    )
    return _parse_aws_credentials_json_bytes(creds_bytes, source_label="AWS EC2 IMDSv2")


def _resolve_aws_credentials(
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
    *,
    region: str,
    sts_endpoint: str | None = None,
    ecs_endpoint: str | None = None,
    imds_endpoint: str | None = None,
) -> _ResolvedAwsCredentials:
    """Resolve AWS IAM signing credentials through explicit, env, `~/.aws`, STS, ECS, and IMDSv2 chains.

    A profile that pairs `role_arn` with `source_profile` or `credential_source` is an assume-role chain,
    which this resolver does not implement, so it is skipped in favour of the next credential source.
    """
    if access_key_id is not None or secret_access_key is not None:
        resolved_ak = (access_key_id or "").strip()
        resolved_sk = (secret_access_key or "").strip()
        if not resolved_ak or not resolved_sk:
            msg = "AWS IAM database token signing requires both access_key_id and secret_access_key"
            raise ImproperConfigurationError(msg)
        resolved_st = session_token.strip() if session_token and session_token.strip() else None
        return _ResolvedAwsCredentials(
            access_key_id=resolved_ak,
            secret_access_key=resolved_sk,
            session_token=resolved_st,
        )

    env_ak = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    env_sk = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    if env_ak or env_sk:
        if not env_ak or not env_sk:
            msg = (
                "Incomplete AWS environment credentials: both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required"
            )
            raise ImproperConfigurationError(msg)
        raw_st = session_token if session_token is not None else os.environ.get("AWS_SESSION_TOKEN")
        resolved_st = raw_st.strip() if raw_st and raw_st.strip() else None
        return _ResolvedAwsCredentials(
            access_key_id=env_ak,
            secret_access_key=env_sk,
            session_token=resolved_st,
        )

    profile_name, profile_vals, found_profile_section = _load_aws_profile_settings()
    if (os.environ.get("AWS_PROFILE") or "").strip() and not found_profile_section:
        msg = f"Configured AWS_PROFILE {profile_name!r} was not found in AWS credentials or config files"
        raise ImproperConfigurationError(msg)

    prof_ak = profile_vals.get("aws_access_key_id", "")
    prof_sk = profile_vals.get("aws_secret_access_key", "")
    if prof_ak or prof_sk:
        if not prof_ak or not prof_sk:
            msg = f"Incomplete AWS profile {profile_name!r}: both aws_access_key_id and aws_secret_access_key are required"
            raise ImproperConfigurationError(msg)
        prof_st = profile_vals.get("aws_session_token") or None
        return _ResolvedAwsCredentials(
            access_key_id=prof_ak,
            secret_access_key=prof_sk,
            session_token=prof_st,
        )

    prof_web_tok = profile_vals.get("web_identity_token_file", "")
    prof_role_arn = profile_vals.get("role_arn", "")
    is_assume_role_profile = (
        bool(prof_role_arn)
        and not prof_web_tok
        and ("source_profile" in profile_vals or "credential_source" in profile_vals)
    )
    if (prof_web_tok or prof_role_arn) and not is_assume_role_profile:
        if not prof_web_tok or not prof_role_arn:
            msg = f"AWS profile {profile_name!r} Web Identity requires both web_identity_token_file and role_arn"
            raise ImproperConfigurationError(msg)
        prof_session = profile_vals.get("role_session_name") or f"vertebra-db-{os.getpid()}"
        return _fetch_sts_web_identity_credentials(
            token_file=prof_web_tok,
            role_arn=prof_role_arn,
            session_name=prof_session,
            region=region,
            sts_endpoint=sts_endpoint,
        )

    env_web_tok = (os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE") or "").strip()
    env_role_arn = (os.environ.get("AWS_ROLE_ARN") or "").strip()
    if env_web_tok or env_role_arn:
        if not env_web_tok or not env_role_arn:
            msg = "AWS Web Identity authentication requires both AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN"
            raise ImproperConfigurationError(msg)
        env_session = (os.environ.get("AWS_ROLE_SESSION_NAME") or "").strip() or f"vertebra-db-{os.getpid()}"
        return _fetch_sts_web_identity_credentials(
            token_file=env_web_tok,
            role_arn=env_role_arn,
            session_name=env_session,
            region=region,
            sts_endpoint=sts_endpoint,
        )

    rel_uri = (os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") or "").strip()
    full_uri = (os.environ.get("AWS_CONTAINER_CREDENTIALS_FULL_URI") or "").strip()
    if rel_uri or full_uri:
        return _fetch_ecs_container_credentials(rel_uri=rel_uri, full_uri=full_uri, ecs_endpoint=ecs_endpoint)

    return _fetch_ec2_imdsv2_credentials(imds_endpoint=imds_endpoint)


def _sign_sigv4_presigned_token(
    *,
    host_header: str,
    region: str,
    service: str,
    action: str,
    ttl_seconds: int,
    now_ts: float,
    access_key_id: str,
    secret_access_key: str,
    session_token: str | None,
    db_user: str | None = None,
) -> str:
    """Sign an AWS SigV4 presigned GET token URL with scheme prefix omitted."""
    dt = datetime.fromtimestamp(now_ts, tz=UTC)
    amz_date = dt.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = dt.strftime("%Y%m%d")
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"

    params: list[tuple[str, str]] = [
        ("Action", action),
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{access_key_id}/{credential_scope}"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", str(ttl_seconds)),
        ("X-Amz-SignedHeaders", "host"),
    ]
    if db_user is not None:
        params.append(("DBUser", db_user))
    if session_token:
        params.append(("X-Amz-Security-Token", session_token))

    encoded_pairs = sorted((quote(k, safe="-._~"), quote(v, safe="-._~")) for k, v in params)
    canonical_query = "&".join(f"{k}={v}" for k, v in encoded_pairs)
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_request = f"GET\n/\n{canonical_query}\nhost:{host_header}\n\nhost\n{payload_hash}"
    hashed_canonical = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical}"

    k_date = hmac.new(f"AWS4{secret_access_key}".encode(), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{host_header}/?{canonical_query}&X-Amz-Signature={signature}"


def _invoke_custom_signer(signer: Callable[..., Any], context: dict[str, Any]) -> Any:
    """Invoke a custom signer callable adapting cleanly to zero-arg, keyword, or dict signatures."""
    sig = inspect.signature(signer)
    params = sig.parameters
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return signer(**context)
    if not params:
        return signer()
    matched_kwargs = {name: context[name] for name in params if name in context}
    required_params = [
        p
        for p in params.values()
        if p.default is inspect.Parameter.empty
        and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    if all(p.name in matched_kwargs for p in required_params):
        return signer(**matched_kwargs)
    if len(required_params) == 1 and required_params[0].kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        return signer(context)
    return signer(**matched_kwargs)


def _normalize_signed_token(raw_token: Any) -> str:
    """Validate a signed token string and strip any leading HTTP/HTTPS scheme prefix."""
    if not isinstance(raw_token, str):
        msg = f"Signed IAM token must be a string, got {type(raw_token).__name__}"
        raise ImproperConfigurationError(msg)
    cleaned = _strip_url_scheme(raw_token)
    if not cleaned:
        msg = "Signed IAM token cannot be empty"
        raise ImproperConfigurationError(msg)
    return cleaned


def _resolve_snapshot_sync(
    slot: _TokenCacheSlot,
    clock: Callable[[], float] | None,
    refresh_margin_seconds: float,
    fetch_fn: Callable[[float], Any],
    *,
    force_refresh: bool,
) -> str:
    """Resolve or synchronously mint and cache a `TokenSnapshot` with one sync refresh in flight."""
    now_fn = clock or time.time
    if not force_refresh:
        cached = slot.fresh_token(float(now_fn()), refresh_margin_seconds)
        if cached is not None:
            return cached
    with slot.sync_refresh_lock:
        now = float(now_fn())
        if force_refresh:
            slot.clear()
        else:
            cached = slot.fresh_token(now, refresh_margin_seconds)
            if cached is not None:
                return cached
        raw = fetch_fn(now)
        if inspect.isawaitable(raw):
            close_coro = getattr(raw, "close", None)
            if callable(close_coro):
                close_coro()
            msg = "Async token fetcher cannot be used with resolve_sync(); use resolve_async()"
            raise ImproperConfigurationError(msg)
        if not isinstance(raw, TokenSnapshot):
            msg = f"Expected TokenSnapshot from token fetcher, got {type(raw).__name__}"
            raise ImproperConfigurationError(msg)
        slot.store(raw)
        return raw.token


async def _resolve_snapshot_async(
    slot: _TokenCacheSlot,
    clock: Callable[[], float] | None,
    refresh_margin_seconds: float,
    fetch_fn: Callable[[float], Any],
    *,
    force_refresh: bool,
) -> str:
    """Resolve or asynchronously mint and cache a `TokenSnapshot` with one refresh in flight per event loop."""
    now_fn = clock or time.time
    if not force_refresh:
        cached = slot.fresh_token(float(now_fn()), refresh_margin_seconds)
        if cached is not None:
            return cached
    async with slot.async_refresh_lock():
        now = float(now_fn())
        if force_refresh:
            slot.clear()
        else:
            cached = slot.fresh_token(now, refresh_margin_seconds)
            if cached is not None:
                return cached
        raw = fetch_fn(now)
        if inspect.isawaitable(raw):
            raw = await raw
        if not isinstance(raw, TokenSnapshot):
            msg = f"Expected TokenSnapshot from token fetcher, got {type(raw).__name__}"
            raise ImproperConfigurationError(msg)
        slot.store(raw)
        return raw.token


def _parse_numeric_field(raw: Any, *, field_name: str) -> float:
    """Parse a positive finite numeric value from an integer, float, or numeric string."""
    if isinstance(raw, bool):
        msg = f"Azure Entra token response field '{field_name}' must be numeric"
        raise ImproperConfigurationError(msg)
    if isinstance(raw, int | float):
        value = float(raw)
    elif isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            msg = f"Azure Entra token response field '{field_name}' cannot be empty"
            raise ImproperConfigurationError(msg)
        try:
            value = float(stripped)
        except ValueError as exc:
            msg = f"Azure Entra token response field '{field_name}' must be numeric"
            raise ImproperConfigurationError(msg) from exc
    else:
        msg = f"Azure Entra token response field '{field_name}' must be numeric"
        raise ImproperConfigurationError(msg)
    if not math.isfinite(value) or value <= 0:
        msg = f"Azure Entra token response field '{field_name}' must be finite and positive"
        raise ImproperConfigurationError(msg)
    return value


def _parse_entra_expires_on(raw: Any) -> float:
    """Parse an Entra or Managed Identity `expires_on` value as epoch seconds or ISO-8601 UTC."""
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            msg = "Azure Entra token response field 'expires_on' cannot be empty"
            raise ImproperConfigurationError(msg)
        try:
            numeric_val = float(stripped)
        except ValueError:
            return _parse_iso8601_timestamp(stripped, field_name="expires_on")
        if not math.isfinite(numeric_val) or numeric_val <= 0:
            msg = "Azure Entra token response field 'expires_on' must be finite and positive"
            raise ImproperConfigurationError(msg)
        return numeric_val
    return _parse_numeric_field(raw, field_name="expires_on")


def _parse_entra_token_mapping(payload: Mapping[str, Any], *, now_ts: float) -> TokenSnapshot:
    """Parse an Entra OAuth2, Managed Identity, or IMDS token JSON mapping into a `TokenSnapshot`."""
    raw_token = payload.get("access_token")
    if not isinstance(raw_token, str) or not raw_token.strip():
        msg = "Azure Entra token response missing a non-empty 'access_token'"
        raise ImproperConfigurationError(msg)
    token = raw_token.strip()
    expires_on_raw = payload.get("expires_on")
    expires_in_raw = payload.get("expires_in")
    if expires_on_raw is not None:
        expires_at = _parse_entra_expires_on(expires_on_raw)
    elif expires_in_raw is not None:
        ttl = _parse_numeric_field(expires_in_raw, field_name="expires_in")
        expires_at = now_ts + ttl
    else:
        msg = "Azure Entra token response missing 'expires_in' or 'expires_on'"
        raise ImproperConfigurationError(msg)
    return TokenSnapshot(token=token, expires_at=expires_at)


def _parse_entra_token_bytes(raw_bytes: bytes, *, now_ts: float) -> TokenSnapshot:
    """Decode an Azure Entra JSON token payload without leaking secrets on failure."""
    try:
        decoded = decode_json(raw_bytes)
    except Exception as exc:
        msg = "Malformed Azure Entra token JSON payload"
        raise ImproperConfigurationError(msg) from exc
    if not isinstance(decoded, Mapping):
        msg = "Azure Entra token JSON payload must be an object"
        raise ImproperConfigurationError(msg)
    return _parse_entra_token_mapping(decoded, now_ts=now_ts)


def _normalize_entra_snapshot(raw: Any, *, now_ts: float) -> TokenSnapshot:
    """Normalize a custom fetcher return value into a validated `TokenSnapshot`."""
    if isinstance(raw, TokenSnapshot):
        if not raw.token.strip():
            msg = "Azure Entra token fetcher returned an empty token"
            raise ImproperConfigurationError(msg)
        if not math.isfinite(raw.expires_at):
            msg = "Azure Entra token fetcher returned a non-finite expiration timestamp"
            raise ImproperConfigurationError(msg)
        return TokenSnapshot(token=raw.token.strip(), expires_at=float(raw.expires_at))
    if isinstance(raw, Mapping):
        return _parse_entra_token_mapping(raw, now_ts=now_ts)
    if isinstance(raw, tuple) and len(raw) == 2:
        raw_token, raw_expires_at = raw
        if not isinstance(raw_token, str) or not raw_token.strip():
            msg = "Azure Entra token fetcher tuple must begin with a non-empty string token"
            raise ImproperConfigurationError(msg)
        if (
            isinstance(raw_expires_at, bool)
            or not isinstance(raw_expires_at, int | float)
            or not math.isfinite(raw_expires_at)
        ):
            msg = "Azure Entra token fetcher tuple must end with a finite numeric expires_at timestamp"
            raise ImproperConfigurationError(msg)
        return TokenSnapshot(token=raw_token.strip(), expires_at=float(raw_expires_at))
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            msg = "Azure Entra token fetcher returned an empty string"
            raise ImproperConfigurationError(msg)
        return TokenSnapshot(token=stripped, expires_at=now_ts + _DEFAULT_ENTRA_FALLBACK_TTL_SECONDS)
    msg = f"Unsupported Azure Entra token fetcher return type: {type(raw).__name__}"
    raise ImproperConfigurationError(msg)


def _execute_entra_token_request(
    req: urllib.request.Request,
    *,
    now_ts: float,
    bypass_proxy: bool = False,
) -> TokenSnapshot:
    """Execute an Entra OAuth2, Managed Identity, or IMDS HTTP request and decode the token snapshot."""
    try:
        raw_bytes = _read_credential_response(
            req,
            timeout=_DEFAULT_ENTRA_HTTP_TIMEOUT_SECONDS,
            bypass_proxy=bypass_proxy,
            endpoint_label="Azure Entra token endpoint",
        )
    except urllib.error.HTTPError as exc:
        msg = f"Azure Entra token endpoint returned HTTP {exc.code}"
        raise ImproperConfigurationError(msg) from exc
    except urllib.error.URLError as exc:
        msg = f"Azure Entra token request failed: {type(exc.reason).__name__}"
        raise ImproperConfigurationError(msg) from exc
    except OSError as exc:
        msg = f"Azure Entra token request failed: {type(exc).__name__}"
        raise ImproperConfigurationError(msg) from exc
    return _parse_entra_token_bytes(raw_bytes, now_ts=now_ts)
