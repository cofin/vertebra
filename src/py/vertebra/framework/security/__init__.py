"""Authentication, RBAC/ABAC guards, CSRF, and rate-limiting primitives."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = (
    "CsrfConfig",
    "Guard",
    "PermissionDeniedError",
    "Principal",
    "RateLimitConfig",
    "SecurityConfig",
    "SecurityContext",
    "requires_capability",
    "requires_role",
    "requires_scope",
    "requires_tenant",
)

type Guard = Callable[[SecurityContext], bool]


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
        """Return True if the principal holds ``role``."""
        return role in self.roles

    def has_scope(self, scope: str) -> bool:
        """Return True if the principal holds ``scope``."""
        return scope in self.scopes

    def has_capability(self, capability: str) -> bool:
        """Return True if the principal holds ``capability``."""
        return capability in self.capabilities


class SecurityContext(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Request-scoped security context evaluated by route guards."""

    principal: Principal | None = None
    client_ip: str = "127.0.0.1"
    headers: dict[str, str] = msgspec.field(default_factory=dict)

    @property
    def is_authenticated(self) -> bool:
        """Return True when a verified ``Principal`` is present."""
        return self.principal is not None

    def enforce(self, *guards: Guard) -> Principal:
        """Evaluate all ``guards`` and return the authenticated ``Principal`` or raise ``PermissionDeniedError``."""
        if self.principal is None:
            msg = "Authentication required"
            raise PermissionDeniedError(msg)
        for guard in guards:
            if not guard(self):
                msg = f"Security guard {getattr(guard, '__name__', repr(guard))} rejected principal {self.principal.subject!r}"
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


def requires_role(*roles: str, any_role: bool = False) -> Guard:
    """Return a guard requiring all (or any) of the specified roles."""
    required = frozenset(roles)

    def _guard(ctx: SecurityContext) -> bool:
        if ctx.principal is None:
            return False
        if any_role:
            return bool(ctx.principal.roles & required)
        return required.issubset(ctx.principal.roles)

    _guard.__name__ = f"requires_role({','.join(roles)})"
    return _guard


def requires_scope(*scopes: str) -> Guard:
    """Return a guard requiring all specified OAuth2/OIDC scopes."""
    required = frozenset(scopes)

    def _guard(ctx: SecurityContext) -> bool:
        if ctx.principal is None:
            return False
        return required.issubset(ctx.principal.scopes)

    _guard.__name__ = f"requires_scope({','.join(scopes)})"
    return _guard


def requires_tenant(allowed_tenants: str | Iterable[str]) -> Guard:
    """Return a guard restricting access to principals belonging to ``allowed_tenants``."""
    allowed = {allowed_tenants} if isinstance(allowed_tenants, str) else set(allowed_tenants)

    def _guard(ctx: SecurityContext) -> bool:
        if ctx.principal is None or ctx.principal.tenant_id is None:
            return False
        return ctx.principal.tenant_id in allowed

    _guard.__name__ = "requires_tenant"
    return _guard


def requires_capability(*capabilities: str) -> Guard:
    """Return a guard requiring all specified fine-grained capabilities."""
    required = frozenset(capabilities)

    def _guard(ctx: SecurityContext) -> bool:
        if ctx.principal is None:
            return False
        return required.issubset(ctx.principal.capabilities)

    _guard.__name__ = f"requires_capability({','.join(capabilities)})"
    return _guard
