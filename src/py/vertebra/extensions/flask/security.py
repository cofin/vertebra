"""Rust-backed rate limiting, JWT/JWKS verification, and RBAC guards for Flask applications."""

from __future__ import annotations

from vertebra.framework.security import (
    AuthenticatedPrincipal,
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
    "Guard",
    "JWKSConfig",
    "JWKSVerifier",
    "JWTAuthGuard",
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
