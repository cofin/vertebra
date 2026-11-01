"""Rate limiting guard backed by Rust token bucket limiter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from litestar.exceptions import PermissionDeniedException

from vertebra._vertebra import RateLimiter
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar.connection import ASGIConnection


class RateLimitGuard:
    """Litestar route guard powered by Rust token-bucket RateLimiter."""

    __slots__ = ("_capacity", "_key_func", "_limiter", "_refill_rate_per_sec")

    def __init__(
        self,
        capacity: int = 100,
        refill_rate_per_sec: float = 10.0,
        *,
        key_func: Callable[[ASGIConnection[Any, Any, Any, Any]], str] | None = None,
    ) -> None:
        """Initialize the token bucket rate limiter guard with capacity and refill rate."""
        ensure_module("litestar_security", extra="security")
        self._capacity = max(1, int(capacity))
        self._refill_rate_per_sec = max(0.001, float(refill_rate_per_sec))
        self._limiter = RateLimiter(self._capacity, self._refill_rate_per_sec)
        self._key_func = key_func

    @property
    def capacity(self) -> int:
        """Return the burst token bucket capacity."""
        return self._capacity

    @property
    def refill_rate_per_sec(self) -> float:
        """Return the refill rate in tokens per second."""
        return self._refill_rate_per_sec

    def __call__(self, connection: ASGIConnection[Any, Any, Any, Any], _: Any) -> None:
        """Evaluate rate limit admission for the incoming connection."""
        if self._key_func is not None:
            key = self._key_func(connection)
        else:
            client = getattr(connection, "client", None)
            key = getattr(client, "host", None) or "global"

        if not self._limiter.acquire(str(key)):
            raise PermissionDeniedException(detail="Rate limit exceeded")
