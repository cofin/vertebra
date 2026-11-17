"""Rust-backed RateLimitGuard, JWTAuthGuard, JWKSVerifier, and SecurityBridge for Django."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.framework.security import (
    AuthenticatedPrincipal,
    JWKSConfig,
    JWKSVerifier,
    JWTAuthGuard,
    RateLimitGuard,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "AuthenticatedPrincipal",
    "JWKSConfig",
    "JWKSVerifier",
    "JWTAuthGuard",
    "RateLimitGuard",
    "SecurityBridge",
)


class SecurityBridge:
    """Combines Rust JWT/JWKS verification and token-bucket rate limiting for Django views."""

    __slots__ = ("_jwks", "_jwt", "_limiter")

    def __init__(
        self,
        *,
        jwt_secret: str | bytes | None = None,
        jwks_verifier: JWKSVerifier | None = None,
        rate_limit_capacity: int = 100,
        rate_limit_refill: float = 10.0,
    ) -> None:
        self._limiter = RateLimitGuard(capacity=rate_limit_capacity, refill_rate=rate_limit_refill)
        self._jwt = JWTAuthGuard(jwt_secret) if jwt_secret is not None else None
        self._jwks = jwks_verifier

    @property
    def rate_limiter(self) -> RateLimitGuard:
        """Return the ``RateLimitGuard``."""
        return self._limiter

    @property
    def jwt_guard(self) -> JWTAuthGuard | None:
        """Return the ``JWTAuthGuard`` if configured."""
        return self._jwt

    @property
    def jwks_verifier(self) -> JWKSVerifier | None:
        """Return the ``JWKSVerifier`` if configured."""
        return self._jwks

    def create_middleware(self, get_response: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """Build a Django middleware enforcing rate limits and Bearer JWT verification."""
        django_http = ensure_module("django.http", package="django", extra="django")

        def _middleware(request: Any) -> Any:
            meta = getattr(request, "META", {}) or {}
            client_key = str(meta.get("REMOTE_ADDR") or "default")
            if not self._limiter.allow(client_key):
                return django_http.JsonResponse({"detail": "Rate limit exceeded"}, status=429)
            if self._jwt is not None:
                auth_header = meta.get("HTTP_AUTHORIZATION")
                try:
                    principal = self._jwt.verify_header(auth_header)
                except ValueError:
                    return django_http.JsonResponse({"detail": "Invalid token"}, status=401)
                if principal is None:
                    return django_http.JsonResponse({"detail": "Authentication required"}, status=401)
                request.vertebra_principal = principal
            return get_response(request)

        return _middleware
