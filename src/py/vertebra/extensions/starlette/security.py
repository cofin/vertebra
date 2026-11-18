"""Security guards, JWT/JWKS authentication, CSRF, and rate limiting for Starlette and FastAPI apps."""

from __future__ import annotations

from typing import Any

from vertebra.framework.security import (
    AuthenticatedPrincipal,
    CsrfConfig,
    CsrfMiddleware,
    Guard,
    JWKSConfig,
    JWKSVerifier,
    JWTAuthGuard,
    PermissionDeniedError,
    Principal,
    RateLimitConfig,
    RateLimiter,
    RateLimitGuard,
    SecurityConfig,
    SecurityContext,
    requires_capability,
    requires_role,
    requires_scope,
    requires_tenant,
)

__all__ = (
    "AuthenticatedPrincipal",
    "CsrfConfig",
    "CsrfMiddleware",
    "Guard",
    "JWTAuthBackend",
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


class JWTAuthBackend:
    """Starlette authentication backend delegating to neutral JWTAuthGuard or JWKSVerifier."""

    __slots__ = ("_guard", "_header_name", "_jwks_verifier")

    def __init__(
        self,
        secret: str | bytes = b"",
        *,
        header_name: str = "authorization",
        scheme: str = "Bearer",
        leeway_secs: int = 60,
        jwks_verifier: JWKSVerifier | None = None,
    ) -> None:
        self._jwks_verifier = jwks_verifier
        self._guard = JWTAuthGuard(secret, leeway_secs=leeway_secs, scheme=scheme) if jwks_verifier is None else None
        self._header_name = header_name.lower()

    async def authenticate(self, conn: Any) -> tuple[tuple[str, ...], AuthenticatedPrincipal] | None:
        """Validate Bearer JWT token on ``conn`` using neutral JWTAuthGuard or JWKSVerifier."""
        headers = getattr(conn, "headers", None) or {}
        auth_val = headers.get(self._header_name)
        if not auth_val or not isinstance(auth_val, str):
            return None
        try:
            if self._jwks_verifier is not None:
                principal = await self._jwks_verifier.verify_header(auth_val)
            else:
                assert self._guard is not None
                principal = self._guard.verify_header(auth_val)
        except ValueError:
            return None
        if principal is None:
            return None
        return principal.scopes, principal
