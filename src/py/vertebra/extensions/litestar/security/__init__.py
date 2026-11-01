"""Vertebra security extension for litestar-security."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.security.exclusions import (
        configure_vertebra_exclusions,
        exempt_vertebra_routes,
    )
    from vertebra.extensions.litestar.security.ingress import (
        IngressIdentityAuthenticator,
        IngressIdentityMechanism,
        IngressIdentityResolver,
        IngressIdentitySlot,
        create_ingress_identity_mechanism,
    )
    from vertebra.extensions.litestar.security.jwt import (
        BearerTokenSlot,
        JWTAuthenticationMechanism,
        JWTAuthenticator,
        JWTIdentityResolver,
        create_jwt_authentication_mechanism,
    )
    from vertebra.extensions.litestar.security.ratelimit import RateLimitGuard

__all__ = (
    "BearerTokenSlot",
    "IngressIdentityAuthenticator",
    "IngressIdentityMechanism",
    "IngressIdentityResolver",
    "IngressIdentitySlot",
    "JWTAuthenticationMechanism",
    "JWTAuthenticator",
    "JWTIdentityResolver",
    "RateLimitGuard",
    "configure_vertebra_exclusions",
    "create_ingress_identity_mechanism",
    "create_jwt_authentication_mechanism",
    "exempt_vertebra_routes",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "BearerTokenSlot": ("vertebra.extensions.litestar.security.jwt", "BearerTokenSlot"),
    "IngressIdentityAuthenticator": ("vertebra.extensions.litestar.security.ingress", "IngressIdentityAuthenticator"),
    "IngressIdentityMechanism": ("vertebra.extensions.litestar.security.ingress", "IngressIdentityMechanism"),
    "IngressIdentityResolver": ("vertebra.extensions.litestar.security.ingress", "IngressIdentityResolver"),
    "IngressIdentitySlot": ("vertebra.extensions.litestar.security.ingress", "IngressIdentitySlot"),
    "JWTAuthenticationMechanism": ("vertebra.extensions.litestar.security.jwt", "JWTAuthenticationMechanism"),
    "JWTAuthenticator": ("vertebra.extensions.litestar.security.jwt", "JWTAuthenticator"),
    "JWTIdentityResolver": ("vertebra.extensions.litestar.security.jwt", "JWTIdentityResolver"),
    "RateLimitGuard": ("vertebra.extensions.litestar.security.ratelimit", "RateLimitGuard"),
    "configure_vertebra_exclusions": (
        "vertebra.extensions.litestar.security.exclusions",
        "configure_vertebra_exclusions",
    ),
    "create_ingress_identity_mechanism": (
        "vertebra.extensions.litestar.security.ingress",
        "create_ingress_identity_mechanism",
    ),
    "create_jwt_authentication_mechanism": (
        "vertebra.extensions.litestar.security.jwt",
        "create_jwt_authentication_mechanism",
    ),
    "exempt_vertebra_routes": ("vertebra.extensions.litestar.security.exclusions", "exempt_vertebra_routes"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
