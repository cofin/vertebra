"""Hierarchical guard chains, authentication checks, and rate/budget enforcement.

This module provides deterministic parent-to-child guard resolution across application,
controller, and tool boundaries, supporting both native AgentContext guards and Litestar guards.
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any, Protocol, cast

import msgspec

from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.identity import caller_identity_from_principal, extract_bearer_token
from vertebra.framework.security import JWKSConfig, JWKSVerifier

__all__ = [
    "BearerAuthGuard",
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
    "UnauthorizedError",
    "authorize",
]


class PermissionDeniedError(Exception):
    """Raised when an execution fails an authorization or identity guard check."""

    status_code: int = 403


class UnauthorizedError(PermissionDeniedError):
    """Raised when a request lacks valid authentication credentials."""

    status_code: int = 401


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


class BearerAuthGuard:
    """HTTP and AgentContext Bearer JWT authentication guard backed by ``JWKSVerifier``."""

    def __init__(
        self,
        jwks_verifier: JWKSVerifier | None = None,
        *,
        jwks_config: JWKSConfig | None = None,
        required_scopes: Sequence[str] = (),
        header_name: str = "authorization",
        tenant_claim: str = "tenant_id",
        roles_claim: str = "roles",
    ) -> None:
        if jwks_verifier is None and jwks_config is None:
            msg = "BearerAuthGuard requires either jwks_verifier or jwks_config"
            raise ValueError(msg)
        if jwks_verifier is not None and jwks_config is not None:
            msg = "BearerAuthGuard accepts either jwks_verifier or jwks_config, not both"
            raise ValueError(msg)
        if jwks_verifier is not None:
            self.verifier = jwks_verifier
        else:
            assert jwks_config is not None
            self.verifier = JWKSVerifier(jwks_config)
        self.required_scopes = set(required_scopes)
        self.header_name = header_name.lower()
        self.tenant_claim = tenant_claim
        self.roles_claim = roles_claim

    async def verify_headers(
        self,
        headers: Mapping[str, Any] | Sequence[tuple[bytes, bytes]],
    ) -> CallerIdentity:
        """Verify Authorization Bearer JWT headers and return a populated ``CallerIdentity``."""
        token = extract_bearer_token(headers, header_name=self.header_name)
        if not token:
            msg = "Missing or invalid Bearer token"
            raise UnauthorizedError(msg)
        try:
            principal = await self.verifier.verify_token(token)
        except (ValueError, RuntimeError) as exc:
            msg = f"Bearer JWT verification failed: {exc}"
            raise UnauthorizedError(msg) from exc

        identity = caller_identity_from_principal(
            principal,
            tenant_claim=self.tenant_claim,
            roles_claim=self.roles_claim,
        )
        if self.required_scopes:
            missing = self.required_scopes - set(identity.scopes)
            if missing:
                msg = f"Missing required scopes: {sorted(missing)}"
                raise PermissionDeniedError(msg)
        return identity

    async def check(self, connection: Any, route_handler: Any) -> None:
        """Validate Bearer JWT on an HTTP/ASGI connection and propagate verified claims."""
        _ = route_handler
        headers = self._extract_connection_headers(connection)
        identity = await self.verify_headers(headers)
        scope = getattr(connection, "scope", None)
        if isinstance(scope, dict):
            scope["user"] = identity
        elif isinstance(connection, dict):
            connection["user"] = identity

    async def __call__(self, connection_or_ctx: Any, route_handler: Any = None) -> None:
        """Validate either an HTTP connection or an ``AgentContext`` carrying request headers."""
        if isinstance(connection_or_ctx, AgentContext):
            ctx = connection_or_ctx
            raw_headers = ctx.metadata.get("headers") if isinstance(ctx.metadata, Mapping) else None
            if raw_headers is not None:
                identity = await self.verify_headers(raw_headers)
                ctx.identity = identity
                return
            if not ctx.identity.is_authenticated:
                msg = "Authentication required for this operation"
                raise UnauthorizedError(msg)
            if self.required_scopes:
                missing = self.required_scopes - set(ctx.identity.scopes)
                if missing:
                    msg = f"Missing required scopes: {sorted(missing)}"
                    raise PermissionDeniedError(msg)
            return
        await self.check(connection_or_ctx, route_handler)

    @staticmethod
    def _extract_connection_headers(
        connection: Any,
    ) -> Mapping[str, Any] | Sequence[tuple[bytes, bytes]]:
        headers = getattr(connection, "headers", None)
        if isinstance(headers, Mapping):
            return headers
        scope = getattr(connection, "scope", None)
        if isinstance(scope, Mapping) and "headers" in scope:
            return cast("Mapping[str, Any] | Sequence[tuple[bytes, bytes]]", scope["headers"])
        if isinstance(connection, Mapping):
            if "headers" in connection:
                return cast("Mapping[str, Any] | Sequence[tuple[bytes, bytes]]", connection["headers"])
            return connection
        return {}


class RequireAuthenticated:
    """Guard ensuring the caller possesses an authenticated identity or valid JWKS Bearer token."""

    def __init__(
        self,
        *,
        jwks_verifier: JWKSVerifier | None = None,
        jwks_config: JWKSConfig | None = None,
        required_scopes: Sequence[str] = (),
        header_name: str = "authorization",
        tenant_claim: str = "tenant_id",
        roles_claim: str = "roles",
    ) -> None:
        self._bearer_guard: BearerAuthGuard | None = (
            BearerAuthGuard(
                jwks_verifier,
                jwks_config=jwks_config,
                required_scopes=required_scopes,
                header_name=header_name,
                tenant_claim=tenant_claim,
                roles_claim=roles_claim,
            )
            if jwks_verifier is not None or jwks_config is not None
            else None
        )
        self.required_scopes = set(required_scopes)

    async def __call__(self, ctx: AgentContext) -> None:
        if self._bearer_guard is not None:
            await self._bearer_guard(ctx)
            return
        if not ctx.identity.is_authenticated:
            msg = "Authentication required for this operation"
            raise UnauthorizedError(msg)
        if self.required_scopes:
            missing = self.required_scopes - set(ctx.identity.scopes)
            if missing:
                msg = f"Missing required scopes: {sorted(missing)}"
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
    def merge(cls, *layers: Sequence[GuardCallable]) -> GuardChain:
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
            PermissionDeniedError: If any guard denies access or requires a missing HTTP connection.
        """
        for guard in self._guards:
            check_fn = getattr(guard, "check", None)
            target = check_fn if callable(check_fn) else guard
            sig = inspect.signature(target)
            positional_params = [
                param
                for param in sig.parameters.values()
                if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]

            if len(positional_params) == 2:
                if connection is None:
                    msg = "Guard requires an HTTP connection"
                    raise PermissionDeniedError(msg)
                res = target(connection, route_handler)
            else:
                res = target(ctx)

            if inspect.isawaitable(res):
                await res


async def authorize(
    ctx: AgentContext | None,
    spec: Any,
    *,
    connection: Any = None,
    route_handler: Any = None,
) -> None:
    """Execute all guards declared on a ToolSpec or Tool against the caller context."""
    raw_guards = getattr(spec, "guards", ()) or ()
    if not raw_guards:
        return
    active_ctx = ctx if ctx is not None else AgentContext()
    chain = GuardChain(raw_guards)
    await chain.execute(active_ctx, connection=connection, route_handler=route_handler)
