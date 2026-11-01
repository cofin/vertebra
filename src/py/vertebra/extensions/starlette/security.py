"""Rust JWTGuard and RateLimiter security integration for Starlette and FastAPI applications."""

from __future__ import annotations

from typing import Any

import msgspec

from vertebra._vertebra import JWTGuard, RateLimiter
from vertebra.utils.serializers import decode_json

__all__ = (
    "AuthenticatedPrincipal",
    "JWTAuthBackend",
    "RateLimitGuard",
)


class AuthenticatedPrincipal(msgspec.Struct, frozen=True, gc=False):
    """Decoded JWT principal returned by JWTAuthBackend."""

    sub: str
    scopes: tuple[str, ...] = ()
    claims: dict[str, Any] = {}

    @property
    def is_authenticated(self) -> bool:
        """Return True for authenticated principals."""
        return True

    @property
    def display_name(self) -> str:
        """Return the principal subject identifier."""
        return self.sub


class JWTAuthBackend:
    """Starlette AuthenticationBackend backed by Rust zero-GIL JWTGuard."""

    __slots__ = ("_guard", "_header_name", "_scheme")

    def __init__(
        self,
        secret: str | bytes,
        *,
        header_name: str = "authorization",
        scheme: str = "Bearer",
    ) -> None:
        raw_secret = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self._guard = JWTGuard(raw_secret)
        self._header_name = header_name.lower()
        self._scheme = scheme.lower()

    async def authenticate(self, conn: Any) -> tuple[tuple[str, ...], AuthenticatedPrincipal] | None:
        """Validate Bearer JWT token on ``conn`` using Rust JWTGuard."""
        headers = getattr(conn, "headers", None) or {}
        auth_val = headers.get(self._header_name)
        if not auth_val or not isinstance(auth_val, str):
            return None
        parts = auth_val.strip().split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != self._scheme:
            return None
        token = parts[1].strip()
        try:
            payload_json = self._guard.validate(token)
        except ValueError:
            return None
        claims = decode_json(payload_json.encode("utf-8") if isinstance(payload_json, str) else payload_json)
        if not isinstance(claims, dict):
            return None
        sub = str(claims.get("sub", ""))
        raw_scopes = claims.get("scope") or claims.get("scopes") or ()
        if isinstance(raw_scopes, str):
            scopes = tuple(s for s in raw_scopes.split() if s)
        elif isinstance(raw_scopes, (list, tuple)):
            scopes = tuple(str(s) for s in raw_scopes)
        else:
            scopes = ()
        return scopes, AuthenticatedPrincipal(sub=sub, scopes=scopes, claims=claims)


class RateLimitGuard:
    """Token-bucket rate limit guard backed by Rust RateLimiter."""

    __slots__ = ("_limiter",)

    def __init__(self, *, capacity: int = 100, refill_rate: float = 100.0) -> None:
        self._limiter = RateLimiter(int(capacity), float(refill_rate))

    def allow(self, key: str = "default", tokens: int = 1) -> bool:
        """Return True if the request for ``key`` is within rate limits."""
        return bool(self._limiter.acquire(key, int(tokens)))
