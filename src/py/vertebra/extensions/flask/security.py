"""Rust-backed rate limiting and JWT verification guards for Flask applications."""

from __future__ import annotations

from typing import Any

from vertebra._vertebra import JWTGuard, RateLimiter

__all__ = (
    "JWTAuthGuard",
    "RateLimitGuard",
)


class RateLimitGuard:
    """Rust token-bucket rate limiter guard for Flask routes."""

    __slots__ = ("_limiter",)

    def __init__(self, *, capacity: int = 100, refill_rate: float = 10.0) -> None:
        self._limiter = RateLimiter(int(capacity), float(refill_rate))

    def allow(self, key: str, tokens: int = 1) -> bool:
        """Return True when ``key`` has at least ``tokens`` available."""
        return bool(self._limiter.acquire(key, int(tokens)))


class JWTAuthGuard:
    """Rust HMAC-SHA256 JWT verification guard for Flask routes."""

    __slots__ = ("_guard",)

    def __init__(self, secret: str | bytes) -> None:
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self._guard = JWTGuard(secret_bytes)

    def verify_header(self, authorization_header: str | None) -> dict[str, Any] | None:
        """Verify a ``Bearer <token>`` Authorization header and return decoded claims."""
        if not authorization_header:
            return None
        parts = authorization_header.split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        try:
            claims = self._guard.validate(token)
            return dict(claims) if isinstance(claims, dict) else {"sub": str(claims)}
        except Exception:
            return None
