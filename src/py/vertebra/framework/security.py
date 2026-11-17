"""Authentication, RBAC/ABAC guards, CSRF, rate-limiting, and JWKS verification primitives."""

from __future__ import annotations

import hmac
import secrets
import threading
import time
from typing import TYPE_CHECKING, Any

import anyio
import msgspec

from vertebra._vertebra import JWTGuard as NativeJWTGuard
from vertebra._vertebra import RateLimiter as NativeRateLimiter
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    import httpx

__all__ = (
    "AuthenticatedPrincipal",
    "CsrfConfig",
    "CsrfMiddleware",
    "Guard",
    "JWTAuthGuard",
    "JWKSConfig",
    "JWKSVerifier",
    "PermissionDeniedError",
    "Principal",
    "RateLimitConfig",
    "RateLimitGuard",
    "RateLimiter",
    "SecurityConfig",
    "SecurityContext",
    "requires_capability",
    "requires_role",
    "requires_scope",
    "requires_tenant",
)


class PermissionDeniedError(PermissionError):
    """Raised when a security guard rejects a principal or request."""


class Principal(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Authenticated subject identity with roles, scopes, capabilities, and tenant context."""

    subject: str
    email: str | None = None
    tenant_id: str | None = None
    roles: frozenset[str] = msgspec.field(default_factory=frozenset)
    scopes: frozenset[str] = msgspec.field(default_factory=frozenset)
    capabilities: frozenset[str] = msgspec.field(default_factory=frozenset)
    claims: dict[str, Any] = msgspec.field(default_factory=dict)

    def has_role(self, role: str) -> bool:
        """Return True if the principal holds role."""
        return role in self.roles

    def has_scope(self, scope: str) -> bool:
        """Return True if the principal holds scope."""
        return scope in self.scopes

    def has_capability(self, capability: str) -> bool:
        """Return True if the principal holds capability."""
        return capability in self.capabilities


class AuthenticatedPrincipal(msgspec.Struct, frozen=True, gc=False):
    """Verified JWT principal decoded from HMAC or asymmetric JWKS tokens."""

    sub: str
    scopes: tuple[str, ...] = ()
    claims: dict[str, Any] = msgspec.field(default_factory=dict)

    @property
    def is_authenticated(self) -> bool:
        """Return True for authenticated principals."""
        return True

    @property
    def display_name(self) -> str:
        """Return the principal subject identifier."""
        return self.sub


class Guard:
    """Security guard evaluating a SecurityContext."""

    __slots__ = ("__name__", "_any_role", "_capabilities", "_roles", "_scopes", "_tenants")

    def __init__(
        self,
        *,
        roles: Iterable[str] = (),
        scopes: Iterable[str] = (),
        capabilities: Iterable[str] = (),
        tenants: Iterable[str] = (),
        any_role: bool = False,
        name: str | None = None,
    ) -> None:
        """Initialize guard with required identity attributes."""
        self._roles = frozenset(roles)
        self._scopes = frozenset(scopes)
        self._capabilities = frozenset(capabilities)
        self._tenants = frozenset(tenants)
        self._any_role = any_role
        self.__name__ = name or "Guard.requires"

    def __call__(self, ctx: SecurityContext) -> bool:
        """Evaluate the security context against this guard's rules."""
        if ctx.principal is None:
            return False
        if self._roles:
            if self._any_role:
                if not (ctx.principal.roles & self._roles):
                    return False
            elif not self._roles.issubset(ctx.principal.roles):
                return False
        if self._scopes and not self._scopes.issubset(ctx.principal.scopes):
            return False
        if self._capabilities and not self._capabilities.issubset(ctx.principal.capabilities):
            return False
        return not (self._tenants and (ctx.principal.tenant_id is None or ctx.principal.tenant_id not in self._tenants))

    @classmethod
    def requires(
        cls,
        *,
        roles: Iterable[str] = (),
        scopes: Iterable[str] = (),
        capabilities: Iterable[str] = (),
        tenants: Iterable[str] = (),
        any_role: bool = False,
    ) -> Guard:
        """Construct a compiled guard with required roles, scopes, capabilities, or tenants."""
        return cls(
            roles=roles,
            scopes=scopes,
            capabilities=capabilities,
            tenants=tenants,
            any_role=any_role,
        )


class SecurityContext(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Request-scoped security context evaluated by route guards."""

    principal: Principal | None = None
    client_ip: str = "127.0.0.1"
    headers: dict[str, str] = msgspec.field(default_factory=dict)

    @property
    def is_authenticated(self) -> bool:
        """Return True when a verified Principal is present."""
        return self.principal is not None

    def enforce(self, *guards: Callable[[SecurityContext], bool] | Guard) -> Principal:
        """Evaluate all guards and return the authenticated Principal or raise PermissionDeniedError."""
        if self.principal is None:
            msg = "Authentication required"
            raise PermissionDeniedError(msg)
        for guard in guards:
            if not guard(self):
                guard_name = getattr(guard, "__name__", repr(guard))
                msg = f"Security guard {guard_name} rejected principal {self.principal.subject!r}"
                raise PermissionDeniedError(msg)
        return self.principal


class CsrfConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Cross-Site Request Forgery protection configuration."""

    secret: str
    cookie_name: str = "vtb_csrf"
    header_name: str = "x-csrf-token"
    safe_methods: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})


class RateLimitConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Token-bucket rate limit configuration."""

    requests_per_window: int = 100
    window_seconds: int = 60
    key_header: str = "x-forwarded-for"


class SecurityConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Aggregate security configuration for Vertebra and Litestar applications."""

    jwt_issuer: str | None = None
    jwt_audience: str | None = None
    enable_iap: bool = False
    csrf: CsrfConfig | None = None
    rate_limit: RateLimitConfig | None = None
    exclude_paths: tuple[str, ...] = ("/health", "/healthz", "/readyz")


class JWKSConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for asymmetric JWT verification against a remote JWKS endpoint."""

    jwks_url: str
    issuer: str | None = None
    audience: str | tuple[str, ...] | None = None
    algorithms: tuple[str, ...] = ("RS256", "ES256", "EdDSA")
    cache_ttl_seconds: float = 300.0
    min_refresh_interval_seconds: float = 5.0
    request_timeout_seconds: float = 5.0
    leeway_seconds: float = 0.0


class CsrfMiddleware:
    """Validates CSRF tokens on unsafe HTTP methods."""

    __slots__ = ("_config",)

    def __init__(self, config: CsrfConfig) -> None:
        """Initialize middleware with CSRF configuration."""
        self._config = config

    @property
    def config(self) -> CsrfConfig:
        """Return the CSRF configuration."""
        return self._config

    def generate_token(self) -> str:
        """Generate a cryptographically secure URL-safe CSRF token."""
        return secrets.token_urlsafe(32)

    def verify_request(
        self,
        method: str,
        headers: Mapping[str, str],
        cookies: Mapping[str, str],
    ) -> bool:
        """Verify request CSRF token, allowing safe methods and validating header matching cookie on unsafe methods."""
        if method.upper() in self._config.safe_methods:
            return True
        header_val = headers.get(self._config.header_name) or headers.get(self._config.header_name.lower())
        cookie_val = cookies.get(self._config.cookie_name)
        if not header_val or not cookie_val:
            return False
        return hmac.compare_digest(header_val, cookie_val)


class RateLimiter:
    """Token-bucket rate limiter tracking request quotas per client key."""

    __slots__ = ("_buckets", "_config")

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialize rate limiter with configuration."""
        self._config = config
        self._buckets: dict[str, tuple[float, float]] = {}

    @property
    def config(self) -> RateLimitConfig:
        """Return the rate limit configuration."""
        return self._config

    def allow(self, key: str, *, now: float | None = None) -> bool:
        """Check whether a request for client key is allowed under current rate limits."""
        current_time = time.monotonic() if now is None else now
        capacity = float(self._config.requests_per_window)
        refill_rate = capacity / float(self._config.window_seconds)

        tokens, last_time = self._buckets.get(key, (capacity, current_time))
        elapsed = max(0.0, current_time - last_time)
        tokens = min(capacity, tokens + elapsed * refill_rate)

        if tokens >= 1.0:
            self._buckets[key] = (tokens - 1.0, current_time)
            return True
        self._buckets[key] = (tokens, current_time)
        return False

    def reset(self) -> None:
        """Clear all active client rate limit buckets."""
        self._buckets.clear()


class RateLimitGuard:
    """Framework-neutral token-bucket rate limit guard backed by Rust ``RateLimiter``."""

    __slots__ = ("_capacity", "_limiter", "_lock", "_refill_rate", "_state")

    def __init__(
        self,
        capacity: int = 100,
        refill_rate: float = 10.0,
        *,
        refill_rate_per_sec: float | None = None,
    ) -> None:
        """Initialize the Rust-backed rate limit guard with capacity and refill rate."""
        self._capacity = max(1, int(capacity))
        resolved_rate = refill_rate if refill_rate_per_sec is None else refill_rate_per_sec
        self._refill_rate = max(0.0, float(resolved_rate))
        self._limiter = NativeRateLimiter(self._capacity, self._refill_rate)
        self._lock = threading.Lock()
        self._state: dict[str, tuple[float, float]] = {}

    @property
    def capacity(self) -> int:
        """Return the burst token bucket capacity."""
        return self._capacity

    @property
    def refill_rate(self) -> float:
        """Return the refill rate in tokens per second."""
        return self._refill_rate

    @property
    def refill_rate_per_sec(self) -> float:
        """Return the refill rate in tokens per second."""
        return self._refill_rate

    def allow(self, key: str = "default", tokens: int = 1) -> bool:
        """Return True if ``key`` has at least ``tokens`` available and consume them."""
        requested = max(1, int(tokens))
        allowed = bool(self._limiter.acquire(key, requested))
        now = time.monotonic()
        with self._lock:
            current = self._current_tokens_locked(key, now)
            if allowed:
                updated = max(0.0, current - float(requested))
            else:
                updated = min(current, float(requested) - 1e-6) if current >= float(requested) else current
            self._state[key] = (max(0.0, updated), now)
        return allowed

    def remaining(self, key: str = "default") -> int:
        """Return the estimated integer number of tokens currently available for ``key``."""
        now = time.monotonic()
        with self._lock:
            return int(self._current_tokens_locked(key, now))

    def reset_after(self, key: str = "default", tokens: int = 1) -> float:
        """Return seconds until ``tokens`` are available for ``key``, or ``inf`` when refill is disabled."""
        requested = float(max(1, int(tokens)))
        now = time.monotonic()
        with self._lock:
            current = self._current_tokens_locked(key, now)
        deficit = requested - current
        if deficit <= 0.0:
            return 0.0
        if self._refill_rate <= 0.0:
            return float("inf")
        return deficit / self._refill_rate

    def _current_tokens_locked(self, key: str, now: float) -> float:
        entry = self._state.get(key)
        if entry is None:
            return float(self._capacity)
        stored_tokens, last_ts = entry
        elapsed = max(0.0, now - last_ts)
        return min(float(self._capacity), stored_tokens + elapsed * self._refill_rate)


class JWTAuthGuard:
    """Framework-neutral HMAC JWT guard backed by Rust ``JWTGuard``."""

    __slots__ = ("_guard", "_scheme")

    def __init__(
        self,
        secret: str | bytes,
        *,
        leeway_secs: int = 60,
        scheme: str = "Bearer",
    ) -> None:
        """Initialize the HMAC JWT guard with secret bytes and clock leeway."""
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self._guard = NativeJWTGuard(secret_bytes, int(leeway_secs))
        self._scheme = scheme.lower()

    def verify_token(self, token: str) -> AuthenticatedPrincipal:
        """Validate an HMAC JWT string and return an ``AuthenticatedPrincipal``."""
        if not isinstance(token, str) or not token.strip():
            msg = "JWT token must be a non-empty string"
            raise ValueError(msg)
        payload_json = self._guard.validate(token.strip())
        try:
            claims = decode_json(payload_json.encode("utf-8") if isinstance(payload_json, str) else payload_json)
        except msgspec.DecodeError as exc:
            msg = f"Invalid JWT claims payload: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(claims, dict):
            msg = "Decoded JWT claims must be a JSON object"
            raise ValueError(msg)
        return _principal_from_claims(claims)

    def verify_header(self, header_value: str | None) -> AuthenticatedPrincipal | None:
        """Validate an Authorization header, returning None when absent/non-Bearer or raising ValueError."""
        if not header_value or not isinstance(header_value, str):
            return None
        parts = header_value.strip().split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != self._scheme:
            return None
        token = parts[1].strip()
        if not token:
            msg = "Empty Bearer token in Authorization header"
            raise ValueError(msg)
        return self.verify_token(token)


class JWKSVerifier:
    """Asymmetric JWT verifier using PyJWT/cryptography, TTL caching, and single-flight kid rotation."""

    __slots__ = (
        "_algorithms",
        "_async_client",
        "_async_lock",
        "_clock",
        "_config",
        "_fetched_at",
        "_httpx_mod",
        "_jwk_set",
        "_jwt_mod",
        "_last_refresh_attempt",
        "_owns_async_client",
        "_owns_sync_client",
        "_sync_client",
        "_sync_lock",
    )

    def __init__(
        self,
        config: JWKSConfig,
        *,
        async_client: httpx.AsyncClient | None = None,
        sync_client: httpx.Client | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize the JWKS verifier with configuration, optional HTTP clients, and clock."""
        self._httpx_mod = ensure_module("httpx")
        self._jwt_mod = ensure_module("jwt", install_package="pyjwt[crypto]")
        allowed_algs = tuple(alg for alg in config.algorithms if alg.strip().lower() != "none")
        if not allowed_algs:
            msg = "JWKSConfig.algorithms must include at least one non-'none' algorithm"
            raise ValueError(msg)
        self._config = config
        self._algorithms = allowed_algs
        self._async_client = async_client
        self._sync_client = sync_client
        self._owns_async_client = async_client is None
        self._owns_sync_client = sync_client is None
        self._clock: Callable[[], float] = clock or time.monotonic
        self._jwk_set: Any = None
        self._fetched_at: float | None = None
        self._last_refresh_attempt: float | None = None
        self._async_lock = anyio.Lock()
        self._sync_lock = threading.Lock()

    @property
    def config(self) -> JWKSConfig:
        """Return the verifier configuration."""
        return self._config

    async def verify_token(self, token: str) -> AuthenticatedPrincipal:
        """Asynchronously verify a JWT against cached or refreshed JWKS keys."""
        alg, kid = self._parse_unverified_header(token)
        now = self._clock()
        signing_key = self._lookup_cached_key(kid, now) if self._is_cache_valid(now) else None

        if signing_key is None:
            async with self._async_lock:
                now = self._clock()
                if self._jwk_set is None or not self._is_cache_valid(now):
                    await self._fetch_jwks_async(now)
                signing_key = self._lookup_key(kid)
                if signing_key is None and self._can_refresh(now):
                    await self._fetch_jwks_async(now)
                    signing_key = self._lookup_key(kid)

        if signing_key is None:
            msg = f"Unrecognized JWKS key id: {kid!r}"
            raise ValueError(msg)
        return self._decode_with_key(token, signing_key, alg)

    def verify_token_sync(self, token: str) -> AuthenticatedPrincipal:
        """Synchronously verify a JWT against cached or refreshed JWKS keys."""
        alg, kid = self._parse_unverified_header(token)
        now = self._clock()
        signing_key = self._lookup_cached_key(kid, now) if self._is_cache_valid(now) else None

        if signing_key is None:
            with self._sync_lock:
                now = self._clock()
                if self._jwk_set is None or not self._is_cache_valid(now):
                    self._fetch_jwks_sync(now)
                signing_key = self._lookup_key(kid)
                if signing_key is None and self._can_refresh(now):
                    self._fetch_jwks_sync(now)
                    signing_key = self._lookup_key(kid)

        if signing_key is None:
            msg = f"Unrecognized JWKS key id: {kid!r}"
            raise ValueError(msg)
        return self._decode_with_key(token, signing_key, alg)

    async def verify_header(self, header_value: str | None) -> AuthenticatedPrincipal | None:
        """Verify an Authorization Bearer header, returning None when absent or non-Bearer."""
        if not header_value or not isinstance(header_value, str):
            return None
        parts = header_value.strip().split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        if not token:
            msg = "Empty Bearer token in Authorization header"
            raise ValueError(msg)
        return await self.verify_token(token)

    async def refresh(self, force: bool = False) -> None:
        """Refresh the cached JWKS document when expired or when ``force=True``."""
        async with self._async_lock:
            now = self._clock()
            if not force and self._is_cache_valid(now):
                return
            await self._fetch_jwks_async(now)

    async def aclose(self) -> None:
        """Close the internally managed async HTTP client if present."""
        if self._owns_async_client and self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None

    def close(self) -> None:
        """Close the internally managed sync HTTP client if present."""
        if self._owns_sync_client and self._sync_client is not None:
            self._sync_client.close()
            self._sync_client = None

    def _is_cache_valid(self, now: float) -> bool:
        if self._jwk_set is None or self._fetched_at is None:
            return False
        return (now - self._fetched_at) < self._config.cache_ttl_seconds

    def _can_refresh(self, now: float) -> bool:
        if self._last_refresh_attempt is None:
            return True
        return (now - self._last_refresh_attempt) >= self._config.min_refresh_interval_seconds

    def _parse_unverified_header(self, token: str) -> tuple[str, str | None]:
        if not isinstance(token, str) or not token.strip():
            msg = "JWT token must be a non-empty string"
            raise ValueError(msg)
        try:
            header = self._jwt_mod.get_unverified_header(token)
        except Exception as exc:
            msg = f"Invalid JWT header: {exc}"
            raise ValueError(msg) from exc
        alg = header.get("alg")
        if not isinstance(alg, str) or alg.strip().lower() == "none" or alg not in self._algorithms:
            msg = f"Disallowed JWT algorithm: {alg!r}"
            raise ValueError(msg)
        raw_kid = header.get("kid")
        kid = str(raw_kid) if raw_kid is not None else None
        return alg, kid

    def _lookup_cached_key(self, kid: str | None, now: float) -> Any:
        if not self._is_cache_valid(now):
            return None
        return self._lookup_key(kid)

    def _lookup_key(self, kid: str | None) -> Any:
        jwk_set = self._jwk_set
        if jwk_set is None:
            return None
        keys = list(getattr(jwk_set, "keys", ()))
        if kid is not None:
            for key in keys:
                if getattr(key, "key_id", None) == kid:
                    return key
            return None
        if len(keys) == 1:
            return keys[0]
        return None

    async def _fetch_jwks_async(self, now: float) -> None:
        self._last_refresh_attempt = now
        if self._async_client is None:
            self._async_client = self._httpx_mod.AsyncClient(timeout=self._config.request_timeout_seconds)
        try:
            response = await self._async_client.get(self._config.jwks_url)
            response.raise_for_status()
        except self._httpx_mod.HTTPError as exc:
            msg = f"Failed to fetch JWKS from {self._config.jwks_url}: {exc}"
            raise RuntimeError(msg) from exc
        self._update_jwk_set(response.content, now)

    def _fetch_jwks_sync(self, now: float) -> None:
        self._last_refresh_attempt = now
        if self._sync_client is None:
            self._sync_client = self._httpx_mod.Client(timeout=self._config.request_timeout_seconds)
        try:
            response = self._sync_client.get(self._config.jwks_url)
            response.raise_for_status()
        except self._httpx_mod.HTTPError as exc:
            msg = f"Failed to fetch JWKS from {self._config.jwks_url}: {exc}"
            raise RuntimeError(msg) from exc
        self._update_jwk_set(response.content, now)

    def _update_jwk_set(self, payload_bytes: bytes, now: float) -> None:
        try:
            doc = decode_json(payload_bytes)
        except msgspec.DecodeError as exc:
            msg = f"Invalid JWKS JSON document from {self._config.jwks_url}: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(doc, dict) or not isinstance(doc.get("keys"), list):
            msg = f"Invalid JWKS payload from {self._config.jwks_url}: expected object with 'keys' list"
            raise ValueError(msg)
        try:
            jwk_set = self._jwt_mod.PyJWKSet.from_dict(doc)
        except Exception as exc:
            msg = f"Failed to parse JWKS keys from {self._config.jwks_url}: {exc}"
            raise ValueError(msg) from exc
        self._jwk_set = jwk_set
        self._fetched_at = now

    def _decode_with_key(self, token: str, signing_key: Any, alg: str) -> AuthenticatedPrincipal:
        options: dict[str, Any] = {
            "verify_signature": True,
            "verify_exp": True,
            "verify_nbf": True,
            "verify_iss": self._config.issuer is not None,
            "verify_aud": self._config.audience is not None,
        }
        try:
            claims = self._jwt_mod.decode(
                token,
                signing_key.key,
                algorithms=[alg],
                issuer=self._config.issuer,
                audience=self._config.audience,
                leeway=self._config.leeway_seconds,
                options=options,
            )
        except Exception as exc:
            msg = f"JWT verification failed: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(claims, dict):
            msg = "Decoded JWT claims must be a mapping"
            raise ValueError(msg)
        return _principal_from_claims(claims)


def _principal_from_claims(claims: dict[str, Any]) -> AuthenticatedPrincipal:
    sub = str(claims.get("sub", ""))
    raw_scopes = claims.get("scope") or claims.get("scopes") or ()
    if isinstance(raw_scopes, str):
        scopes = tuple(s for s in raw_scopes.split() if s)
    elif isinstance(raw_scopes, list | tuple):
        scopes = tuple(str(s) for s in raw_scopes)
    else:
        scopes = ()
    return AuthenticatedPrincipal(sub=sub, scopes=scopes, claims=claims)


def requires_role(*roles: str, any_role: bool = False) -> Guard:
    """Return a guard requiring all (or any) of the specified roles."""
    return Guard.requires(roles=roles, any_role=any_role)


def requires_scope(*scopes: str) -> Guard:
    """Return a guard requiring all specified OAuth2/OIDC scopes."""
    return Guard.requires(scopes=scopes)


def requires_tenant(allowed_tenants: str | Iterable[str]) -> Guard:
    """Return a guard restricting access to principals belonging to allowed_tenants."""
    tenants = (allowed_tenants,) if isinstance(allowed_tenants, str) else tuple(allowed_tenants)
    return Guard.requires(tenants=tenants)


def requires_capability(*capabilities: str) -> Guard:
    """Return a guard requiring all specified fine-grained capabilities."""
    return Guard.requires(capabilities=capabilities)
