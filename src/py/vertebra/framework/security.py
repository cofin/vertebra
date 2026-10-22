"""Authentication, RBAC/ABAC guards, CSRF, and rate-limiting primitives."""

from __future__ import annotations

import hmac
import secrets
import time
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = (
    "CsrfConfig",
    "CsrfMiddleware",
    "Guard",
    "PermissionDeniedError",
    "Principal",
    "RateLimitConfig",
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
