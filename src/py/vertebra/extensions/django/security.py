"""Rust-backed RateLimitGuard, JWTAuthGuard, and SecurityBridge for Django."""

from __future__ import annotations

from vertebra.extensions.flask.security import JWTAuthGuard, RateLimitGuard

__all__ = (
    "JWTAuthGuard",
    "RateLimitGuard",
    "SecurityBridge",
)


class SecurityBridge:
    """Combines Rust JWT verification and token-bucket rate limiting for Django views."""

    __slots__ = ("_jwt", "_limiter")

    def __init__(
        self,
        *,
        jwt_secret: str | bytes | None = None,
        rate_limit_capacity: int = 100,
        rate_limit_refill: float = 10.0,
    ) -> None:
        self._limiter = RateLimitGuard(capacity=rate_limit_capacity, refill_rate=rate_limit_refill)
        self._jwt = JWTAuthGuard(jwt_secret) if jwt_secret is not None else None

    @property
    def rate_limiter(self) -> RateLimitGuard:
        """Return the ``RateLimitGuard``."""
        return self._limiter

    @property
    def jwt_guard(self) -> JWTAuthGuard | None:
        """Return the ``JWTAuthGuard`` if configured."""
        return self._jwt
