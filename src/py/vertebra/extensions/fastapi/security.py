"""Rust JWTGuard, JWKS verifier, and RateLimiter security integration for FastAPI applications."""

from __future__ import annotations

from typing import Any

from vertebra.framework.security import (
    AuthenticatedPrincipal,
    JWKSConfig,
    JWKSVerifier,
    JWTAuthGuard,
    RateLimitGuard,
)

__all__ = (
    "AuthenticatedPrincipal",
    "JWTAuthBackend",
    "JWTAuthGuard",
    "JWKSConfig",
    "JWKSVerifier",
    "RateLimitGuard",
)


class JWTAuthBackend:
    """FastAPI authentication backend backed by framework ``JWTAuthGuard``."""

    __slots__ = ("_guard", "_header_name")

    def __init__(
        self,
        secret: str | bytes,
        *,
        header_name: str = "authorization",
        scheme: str = "Bearer",
        leeway_secs: int = 60,
    ) -> None:
        self._guard = JWTAuthGuard(secret, leeway_secs=leeway_secs, scheme=scheme)
        self._header_name = header_name.lower()

    async def authenticate(self, conn: Any) -> tuple[tuple[str, ...], AuthenticatedPrincipal] | None:
        """Validate Bearer JWT token on ``conn`` using ``JWTAuthGuard``."""
        headers = getattr(conn, "headers", None) or {}
        auth_val = headers.get(self._header_name)
        if not auth_val or not isinstance(auth_val, str):
            return None
        try:
            principal = self._guard.verify_header(auth_val)
        except ValueError:
            return None
        if principal is None:
            return None
        return principal.scopes, principal
