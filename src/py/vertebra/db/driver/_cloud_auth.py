"""Shared cloud IAM database token providers with pre-expiry caching, SigV4 signing, and Entra ID."""

from __future__ import annotations

import hashlib
import hmac
import inspect
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Final, Protocol, runtime_checkable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import anyio
import msgspec

from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json

AZURE_POSTGRES_SCOPE: Final[str] = "https://ossrdbms-aad.database.windows.net/.default"
AZURE_MYSQL_SCOPE: Final[str] = "https://ossrdbms-aad.database.windows.net/.default"
AZURE_MSSQL_SCOPE: Final[str] = "https://database.windows.net/.default"
AZURE_REDIS_SCOPE: Final[str] = "https://redis.azure.com/.default"

_DEFAULT_ENTRA_IMDS_ENDPOINT: Final[str] = "http://169.254.169.254/metadata/identity/oauth2/token"
_DEFAULT_ENTRA_API_VERSION: Final[str] = "2019-08-01"
_DEFAULT_ENTRA_HTTP_TIMEOUT_SECONDS: Final[float] = 10.0
_DEFAULT_ENTRA_FALLBACK_TTL_SECONDS: Final[float] = 3600.0
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


class _TokenCacheSlot:
    """Thread-safe and task-safe mutable cache container held by frozen token providers."""

    __slots__ = ("async_lock", "snapshot", "sync_lock")

    def __init__(self) -> None:
        self.sync_lock = threading.Lock()
        self.async_lock: anyio.Lock | None = None
        self.snapshot: TokenSnapshot | None = None

    def get_async_lock(self) -> anyio.Lock:
        """Lazily initialize and return the async lock under the thread lock."""
        with self.sync_lock:
            if self.async_lock is None:
                self.async_lock = anyio.Lock()
            return self.async_lock

    def clear(self) -> None:
        """Discard any cached token snapshot under the thread lock."""
        with self.sync_lock:
            self.snapshot = None


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


def _validate_timing_bounds(region: str, token_ttl_seconds: int, refresh_margin_seconds: float) -> str:
    """Validate AWS region, positive TTL, and finite pre-expiry refresh margin."""
    normalized_region = region.strip()
    if not normalized_region:
        msg = "AWS region must be a non-empty string"
        raise ImproperConfigurationError(msg)
    if isinstance(token_ttl_seconds, bool) or not isinstance(token_ttl_seconds, int) or token_ttl_seconds <= 0:
        msg = f"token_ttl_seconds must be a positive integer, got {token_ttl_seconds!r}"
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


def _resolve_aws_credentials(
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
) -> tuple[str, str, str | None]:
    """Resolve static or environment AWS IAM credentials without permitting unsigned fallback."""
    if access_key_id is not None or secret_access_key is not None:
        resolved_ak = access_key_id or ""
        resolved_sk = secret_access_key or ""
        resolved_st = session_token
    else:
        resolved_ak = os.environ.get("AWS_ACCESS_KEY_ID", "")
        resolved_sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        resolved_st = session_token if session_token is not None else os.environ.get("AWS_SESSION_TOKEN")
    if not resolved_ak.strip() or not resolved_sk.strip():
        msg = "AWS IAM database token signing requires AWS credentials or a custom signer"
        raise ImproperConfigurationError(msg)
    return resolved_ak, resolved_sk, (resolved_st if resolved_st else None)


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
    """Resolve or synchronously mint and cache a `TokenSnapshot` under thread synchronization."""
    now_fn = clock or time.time
    with slot.sync_lock:
        now = float(now_fn())
        snap = slot.snapshot
        if not force_refresh and snap is not None and now + refresh_margin_seconds < snap.expires_at:
            return snap.token
        if force_refresh:
            slot.snapshot = None
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
        slot.snapshot = raw
        return raw.token


async def _resolve_snapshot_async(
    slot: _TokenCacheSlot,
    clock: Callable[[], float] | None,
    refresh_margin_seconds: float,
    fetch_fn: Callable[[float], Any],
    *,
    force_refresh: bool,
) -> str:
    """Resolve or asynchronously mint and cache a `TokenSnapshot` under async task and thread locks."""
    now_fn = clock or time.time
    with slot.sync_lock:
        now = float(now_fn())
        snap = slot.snapshot
        if not force_refresh and snap is not None and now + refresh_margin_seconds < snap.expires_at:
            return snap.token

    async_lock = slot.get_async_lock()
    async with async_lock:
        with slot.sync_lock:
            now = float(now_fn())
            snap = slot.snapshot
            if not force_refresh and snap is not None and now + refresh_margin_seconds < snap.expires_at:
                return snap.token
            if force_refresh:
                slot.snapshot = None

        raw = fetch_fn(now)
        if inspect.isawaitable(raw):
            raw = await raw
        if not isinstance(raw, TokenSnapshot):
            msg = f"Expected TokenSnapshot from token fetcher, got {type(raw).__name__}"
            raise ImproperConfigurationError(msg)
        with slot.sync_lock:
            slot.snapshot = raw
        return raw.token


def _resolve_cached_sync(
    slot: _TokenCacheSlot,
    clock: Callable[[], float] | None,
    refresh_margin_seconds: float,
    token_ttl_seconds: int,
    mint_fn: Callable[[float], Any],
    *,
    force_refresh: bool,
) -> str:
    """Resolve or synchronously mint and cache a token under thread synchronization."""

    def _fetch(now_ts: float) -> TokenSnapshot:
        raw = mint_fn(now_ts)
        if inspect.isawaitable(raw):
            close_coro = getattr(raw, "close", None)
            if callable(close_coro):
                close_coro()
            msg = "Async token signer cannot be used with resolve_sync(); use resolve_async()"
            raise ImproperConfigurationError(msg)
        token = _normalize_signed_token(raw)
        return TokenSnapshot(token=token, expires_at=now_ts + token_ttl_seconds)

    return _resolve_snapshot_sync(
        slot,
        clock,
        refresh_margin_seconds,
        _fetch,
        force_refresh=force_refresh,
    )


async def _resolve_cached_async(
    slot: _TokenCacheSlot,
    clock: Callable[[], float] | None,
    refresh_margin_seconds: float,
    token_ttl_seconds: int,
    mint_fn: Callable[[float], Any],
    *,
    force_refresh: bool,
) -> str:
    """Resolve or asynchronously mint and cache a token under async task and thread locks."""

    async def _fetch(now_ts: float) -> TokenSnapshot:
        raw = mint_fn(now_ts)
        if inspect.isawaitable(raw):
            raw = await raw
        token = _normalize_signed_token(raw)
        return TokenSnapshot(token=token, expires_at=now_ts + token_ttl_seconds)

    return await _resolve_snapshot_async(
        slot,
        clock,
        refresh_margin_seconds,
        _fetch,
        force_refresh=force_refresh,
    )


class AwsRdsIamTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """AWS RDS IAM database authentication token provider using SigV4 `rds-db` presigned URLs."""

    endpoint_host: str
    port: int = 5432
    region: str
    username: str
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    refresh_margin_seconds: float = 60.0
    token_ttl_seconds: int = 900
    clock: Callable[[], float] | None = None
    signer: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        """Normalize host, validate RDS token parameters, and initialize cache state."""
        normalized_host = _normalize_host(self.endpoint_host, field_name="endpoint_host")
        if normalized_host != self.endpoint_host:
            msgspec.structs.force_setattr(self, "endpoint_host", normalized_host)
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            msg = f"port must be an integer between 1 and 65535, got {self.port!r}"
            raise ImproperConfigurationError(msg)
        normalized_region = _validate_timing_bounds(self.region, self.token_ttl_seconds, self.refresh_margin_seconds)
        if normalized_region != self.region:
            msgspec.structs.force_setattr(self, "region", normalized_region)
        normalized_user = self.username.strip()
        if not normalized_user:
            msg = "username must be a non-empty string"
            raise ImproperConfigurationError(msg)
        if normalized_user != self.username:
            msgspec.structs.force_setattr(self, "username", normalized_user)
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

    def _mint_token(self, now_ts: float) -> Any:
        """Mint a fresh RDS IAM authentication token via custom signer or HMAC-SHA256 SigV4."""
        if self.signer is not None:
            context = {
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
            return _invoke_custom_signer(self.signer, context)
        ak, sk, st = _resolve_aws_credentials(self.access_key_id, self.secret_access_key, self.session_token)
        return _sign_sigv4_presigned_token(
            host_header=f"{self.endpoint_host}:{self.port}",
            region=self.region,
            service="rds-db",
            action="connect",
            ttl_seconds=self.token_ttl_seconds,
            now_ts=now_ts,
            access_key_id=ak,
            secret_access_key=sk,
            session_token=st,
            db_user=self.username,
        )

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed RDS IAM database token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return _resolve_cached_sync(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self.token_ttl_seconds,
            self._mint_token,
            force_refresh=force_refresh,
        )

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed RDS IAM database token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return await _resolve_cached_async(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self.token_ttl_seconds,
            self._mint_token,
            force_refresh=force_refresh,
        )

    def invalidate(self) -> None:
        """Invalidate the cached RDS IAM token snapshot."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        slot.clear()


class AwsAuroraDsqlIamTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """AWS Aurora DSQL IAM authentication token provider using SigV4 `dsql` presigned URLs."""

    hostname: str
    region: str
    admin: bool = False
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    refresh_margin_seconds: float = 60.0
    token_ttl_seconds: int = 900
    clock: Callable[[], float] | None = None
    signer: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        """Normalize hostname, validate Aurora DSQL parameters, and initialize cache state."""
        normalized_host = _normalize_host(self.hostname, field_name="hostname")
        if normalized_host != self.hostname:
            msgspec.structs.force_setattr(self, "hostname", normalized_host)
        normalized_region = _validate_timing_bounds(self.region, self.token_ttl_seconds, self.refresh_margin_seconds)
        if normalized_region != self.region:
            msgspec.structs.force_setattr(self, "region", normalized_region)
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

    def _mint_token(self, now_ts: float) -> Any:
        """Mint a fresh Aurora DSQL IAM token via custom signer or HMAC-SHA256 SigV4."""
        action = "DbConnectAdmin" if self.admin else "DbConnect"
        if self.signer is not None:
            context = {
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
            return _invoke_custom_signer(self.signer, context)
        ak, sk, st = _resolve_aws_credentials(self.access_key_id, self.secret_access_key, self.session_token)
        return _sign_sigv4_presigned_token(
            host_header=self.hostname,
            region=self.region,
            service="dsql",
            action=action,
            ttl_seconds=self.token_ttl_seconds,
            now_ts=now_ts,
            access_key_id=ak,
            secret_access_key=sk,
            session_token=st,
        )

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        """Synchronously return a cached or freshly signed Aurora DSQL IAM token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return _resolve_cached_sync(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self.token_ttl_seconds,
            self._mint_token,
            force_refresh=force_refresh,
        )

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        """Asynchronously return a cached or freshly signed Aurora DSQL IAM token."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        return await _resolve_cached_async(
            slot,
            self.clock,
            self.refresh_margin_seconds,
            self.token_ttl_seconds,
            self._mint_token,
            force_refresh=force_refresh,
        )

    def invalidate(self) -> None:
        """Invalidate the cached Aurora DSQL IAM token snapshot."""
        slot: _TokenCacheSlot = self.__dict__["_cache_slot"]
        slot.clear()


def _validate_http_url(raw: str, *, field_name: str) -> str:
    """Validate and normalize an HTTP or HTTPS endpoint URL."""
    normalized = raw.strip()
    parsed = urlsplit(normalized)
    if not normalized or parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        msg = f"{field_name} must be a valid HTTP or HTTPS URL"
        raise ImproperConfigurationError(msg)
    return normalized


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


def _parse_entra_token_mapping(payload: Mapping[str, Any], *, now_ts: float) -> TokenSnapshot:
    """Parse an Entra OAuth2 or IMDS token JSON mapping into a `TokenSnapshot`."""
    raw_token = payload.get("access_token")
    if not isinstance(raw_token, str) or not raw_token.strip():
        msg = "Azure Entra token response missing a non-empty 'access_token'"
        raise ImproperConfigurationError(msg)
    token = raw_token.strip()
    expires_on_raw = payload.get("expires_on")
    expires_in_raw = payload.get("expires_in")
    if expires_on_raw is not None:
        expires_at = _parse_numeric_field(expires_on_raw, field_name="expires_on")
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
    """Execute an Entra OAuth2 or IMDS HTTP request and decode the returned token snapshot."""
    opener = (
        urllib.request.build_opener(urllib.request.ProxyHandler({})) if bypass_proxy else urllib.request.build_opener()
    )
    try:
        with opener.open(req, timeout=_DEFAULT_ENTRA_HTTP_TIMEOUT_SECONDS) as resp:
            raw_bytes = bytes(resp.read())
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


class AzureEntraTokenProvider(msgspec.Struct, frozen=True, kw_only=True, dict=True):
    """Azure Entra ID database token provider supporting client credentials, IMDS, and custom fetchers."""

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
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

        if self.scope is not None:
            normalized_scope = self.scope.strip()
            if not normalized_scope:
                msg = "scope must be a non-empty string when provided"
                raise ImproperConfigurationError(msg)
            if normalized_scope != self.scope:
                msgspec.structs.force_setattr(self, "scope", normalized_scope)

        if self.token_endpoint is not None:
            normalized_token_ep = _validate_http_url(self.token_endpoint, field_name="token_endpoint")
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

        if self.fetcher is None and self.client_secret is not None:
            if not self.client_id:
                msg = "Azure Entra client-credentials authentication requires client_id"
                raise ImproperConfigurationError(msg)
            if self.token_endpoint is None and not self.tenant_id:
                msg = "Azure Entra client-credentials authentication requires tenant_id or token_endpoint"
                raise ImproperConfigurationError(msg)

        msgspec.structs.force_setattr(self, "_cache_slot", _TokenCacheSlot())

    def __repr__(self) -> str:
        """Return a redacted representation that omits client secrets and cached tokens."""
        return (
            f"AzureEntraTokenProvider(tenant_id={self.tenant_id!r}, client_id={self.client_id!r}, "
            f"client_secret={'<redacted>' if self.client_secret else None!r}, "
            f"scope={self.scope!r}, token_endpoint={self.token_endpoint!r}, "
            f"imds_endpoint={self.imds_endpoint!r}, api_version={self.api_version!r}, "
            f"refresh_margin_seconds={self.refresh_margin_seconds!r})"
        )

    def _require_scope(self) -> str:
        if not self.scope:
            msg = "AzureEntraTokenProvider requires 'scope' before resolving tokens"
            raise ImproperConfigurationError(msg)
        return self.scope

    def with_default_scope(self, scope: str) -> AzureEntraTokenProvider:
        """Return a provider copy with `scope` populated when `self.scope` is unset."""
        normalized_scope = scope.strip()
        if not normalized_scope:
            msg = "Default Azure Entra scope must be a non-empty string"
            raise ImproperConfigurationError(msg)
        if self.scope is not None:
            return self
        return msgspec.structs.replace(self, scope=normalized_scope)

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

    def _fetch_client_credentials_snapshot(self, now_ts: float, scope: str) -> TokenSnapshot:
        if not self.client_id:
            msg = "Azure Entra client-credentials authentication requires client_id"
            raise ImproperConfigurationError(msg)
        if not self.client_secret:
            msg = "Azure Entra client-credentials authentication requires client_secret"
            raise ImproperConfigurationError(msg)
        if self.token_endpoint is not None:
            url = self.token_endpoint
        else:
            if not self.tenant_id:
                msg = "Azure Entra client-credentials authentication requires tenant_id or token_endpoint"
                raise ImproperConfigurationError(msg)
            url = f"https://login.microsoftonline.com/{quote(self.tenant_id, safe='')}/oauth2/v2.0/token"
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
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
            bypass_proxy=host in {"127.0.0.1", "localhost", "::1"},
        )

    def _fetch_imds_snapshot(self, now_ts: float, scope: str) -> TokenSnapshot:
        resource = scope.removesuffix("/.default")
        if not resource:
            msg = "Azure Entra IMDS resource derived from scope cannot be empty"
            raise ImproperConfigurationError(msg)
        parsed = urlsplit(self.imds_endpoint)
        existing_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_pairs = [(k, v) for k, v in existing_pairs if k not in {"api-version", "resource", "client_id"}]
        query_pairs.append(("api-version", self.api_version))
        query_pairs.append(("resource", resource))
        if self.client_id:
            query_pairs.append(("client_id", self.client_id))
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
        if self.client_secret is not None:
            return self._fetch_client_credentials_snapshot(now_ts, scope)
        return self._fetch_imds_snapshot(now_ts, scope)

    async def _acquire_async(self, now_ts: float) -> TokenSnapshot:
        scope = self._require_scope()
        if self.fetcher is not None:
            raw = self._invoke_fetcher(scope)
            if inspect.isawaitable(raw):
                raw = await raw
            return _normalize_entra_snapshot(raw, now_ts=now_ts)
        if self.client_secret is not None:
            return await anyio.to_thread.run_sync(self._fetch_client_credentials_snapshot, now_ts, scope)
        return await anyio.to_thread.run_sync(self._fetch_imds_snapshot, now_ts, scope)

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
        """Asynchronously return a cached or freshly acquired Azure Entra ID token."""
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
