"""Hierarchical guard chains, authentication checks, and rate/budget enforcement.

This module provides deterministic parent-to-child guard resolution across application,
controller, and tool boundaries, supporting both native AgentContext guards and Litestar guards.
"""

import inspect
import time
from collections.abc import Callable, Coroutine, Sequence
from typing import Any, Protocol

import msgspec

from vertebra.agent.core.context import AgentContext


class PermissionDeniedError(Exception):
    """Raised when an execution fails an authorization or identity guard check."""


class RateLimitExceededError(Exception):
    """Raised when a request exceeds configured token bucket or rate limits."""


class BudgetExceededError(Exception):
    """Raised when a turn exceeds its allocated token, step, or time budget."""


class TurnBudget(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Execution boundaries for a single agent turn."""

    max_steps: int = 16
    max_tool_calls: int = 32
    max_input_tokens: int = 128_000
    max_output_tokens: int = 32_000
    timeout_seconds: float = 120.0


class GuardProtocol(Protocol):
    """Protocol for async guards validating an AgentContext."""

    async def __call__(self, ctx: AgentContext) -> None: ...


class RequireAuthenticated:
    """Guard ensuring the caller possesses an authenticated identity."""

    async def __call__(self, ctx: AgentContext) -> None:
        if not ctx.identity.is_authenticated:
            msg = "Authentication required for this operation"
            raise PermissionDeniedError(msg)


class RequireScopes:
    """Guard requiring the caller to possess all specified permission scopes."""

    def __init__(self, *required_scopes: str) -> None:
        self.required_scopes = set(required_scopes)

    async def __call__(self, ctx: AgentContext) -> None:
        caller_scopes = set(ctx.identity.scopes)
        missing = self.required_scopes - caller_scopes
        if missing:
            msg = f"Missing required scopes: {sorted(missing)}"
            raise PermissionDeniedError(msg)


class RequireRoles:
    """Guard requiring the caller to possess at least one of the specified roles."""

    def __init__(self, *required_roles: str) -> None:
        self.required_roles = set(required_roles)

    async def __call__(self, ctx: AgentContext) -> None:
        caller_roles = set(ctx.identity.roles)
        if not (self.required_roles & caller_roles):
            msg = f"Caller lacks any required role from: {sorted(self.required_roles)}"
            raise PermissionDeniedError(msg)


class TenantIsolationGuard:
    """Guard enforcing tenant boundary matching."""

    def __init__(self, *allowed_tenants: str) -> None:
        self.allowed_tenants = set(allowed_tenants)

    async def __call__(self, ctx: AgentContext) -> None:
        if self.allowed_tenants and ctx.identity.tenant_id not in self.allowed_tenants:
            msg = f"Access denied for tenant {ctx.identity.tenant_id}"
            raise PermissionDeniedError(msg)


class TokenBucketGuard:
    """In-memory token bucket rate limiter per owner identity."""

    def __init__(self, rate_per_second: float = 10.0, capacity: float = 20.0) -> None:
        self.rate = rate_per_second
        self.capacity = capacity
        self._buckets: dict[str, tuple[float, float]] = {}

    async def __call__(self, ctx: AgentContext) -> None:
        key = ctx.identity.owner_key() or "anonymous"
        now = time.monotonic()
        tokens, last_update = self._buckets.get(key, (self.capacity, now))

        tokens = min(self.capacity, tokens + (now - last_update) * self.rate)
        if tokens < 1.0:
            msg = f"Rate limit exceeded for {key}"
            raise RateLimitExceededError(msg)

        self._buckets[key] = (tokens - 1.0, now)


type GuardCallable = Callable[..., Coroutine[Any, Any, None] | None]


class GuardChain:
    """Hierarchical chain of guards executed sequentially in parent-to-child order."""

    def __init__(self, guards: Sequence[GuardCallable] = ()) -> None:
        self._guards: list[GuardCallable] = list(guards)

    @property
    def guards(self) -> tuple[GuardCallable, ...]:
        """Return the immutable sequence of guards in this chain."""
        return tuple(self._guards)

    @classmethod
    def merge(cls, *layers: Sequence[GuardCallable]) -> "GuardChain":
        """Merge multiple layers of guards preserving app -> controller -> handler order."""
        combined: list[GuardCallable] = []
        for layer in layers:
            combined.extend(layer)
        return cls(combined)

    async def execute(
        self,
        ctx: AgentContext,
        connection: Any = None,
        route_handler: Any = None,
    ) -> None:
        """Execute all guards in the chain, short-circuiting on the first denial.

        Args:
            ctx: The active AgentContext.
            connection: Optional ASGI/Litestar connection for Litestar guard compatibility.
            route_handler: Optional route handler object for Litestar guard compatibility.

        Raises:
            PermissionDeniedError: If any guard denies access.
        """
        for guard in self._guards:
            sig = inspect.signature(guard)
            param_count = len(sig.parameters)

            res = guard(connection, route_handler) if param_count == 2 and connection is not None else guard(ctx)

            if inspect.isawaitable(res):
                await res


__all__ = [
    "BudgetExceededError",
    "GuardCallable",
    "GuardChain",
    "GuardProtocol",
    "PermissionDeniedError",
    "RateLimitExceededError",
    "RequireAuthenticated",
    "RequireRoles",
    "RequireScopes",
    "TenantIsolationGuard",
    "TokenBucketGuard",
    "TurnBudget",
]
