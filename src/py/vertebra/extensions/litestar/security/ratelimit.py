"""Rate limiting guard delegating token-bucket enforcement to neutral RateLimitGuard."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.framework.security import RateLimitGuard as _NeutralRateLimitGuard
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar.connection import ASGIConnection

__all__ = ("RateLimitGuard",)


class RateLimitGuard:
    """Litestar route guard powered by neutral token-bucket RateLimitGuard."""

    __slots__ = ("_guard", "_key_func", "_permission_denied_cls")

    def __init__(
        self,
        capacity: int = 100,
        refill_rate_per_sec: float = 10.0,
        *,
        key_func: Callable[[ASGIConnection[Any, Any, Any, Any]], str] | None = None,
    ) -> None:
        """Initialize the token bucket rate limiter guard with capacity and refill rate."""
        exc_mod = ensure_module("litestar.exceptions", extra="litestar")
        self._permission_denied_cls = exc_mod.PermissionDeniedException
        self._guard = _NeutralRateLimitGuard(capacity, refill_rate_per_sec)
        self._key_func = key_func

    @property
    def capacity(self) -> int:
        """Return the burst token bucket capacity."""
        return self._guard.capacity

    @property
    def refill_rate_per_sec(self) -> float:
        """Return the refill rate in tokens per second."""
        return self._guard.refill_rate_per_sec

    def __call__(self, connection: ASGIConnection[Any, Any, Any, Any], _: Any) -> None:
        """Evaluate rate limit admission for the incoming connection."""
        if self._key_func is not None:
            key = self._key_func(connection)
        else:
            client = getattr(connection, "client", None)
            key = getattr(client, "host", None) or "global"

        if not self._guard.allow(str(key)):
            raise self._permission_denied_cls(detail="Rate limit exceeded")
