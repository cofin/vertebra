"""Trusted mTLS and XFCC ingress identity authentication mechanism."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from litestar.openapi.spec import SecurityScheme

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_security import (
        Authenticated,
        AuthenticationEvidence,
        AuthenticationMechanism,
        InvalidCredentials,
        NoCredentials,
        PresentedCredential,
        Principal,
    )
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-security",
        install_package="litestar-security",
        extra="security",
    ) from exc

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from litestar.connection import ASGIConnection


class IngressIdentitySlot:
    """Synchronous credential slot extracting mTLS / XFCC identity headers from trusted ingress."""

    __slots__ = ("_header_names", "name")

    def __init__(
        self,
        name: str = "ingress_identity",
        *,
        header_names: Sequence[str] = (
            "x-forwarded-client-cert",
            "ssl-client-subject-dn",
            "x-client-ssl-san-uri",
        ),
    ) -> None:
        """Initialize the ingress identity slot with target header names."""
        self.name = name
        self._header_names = tuple(h.lower() for h in header_names)

    def extract(self, connection: ASGIConnection[Any, Any, Any, Any]) -> Any:
        """Extract ingress identity header from connection headers."""
        for header_name in self._header_names:
            val = connection.headers.get(header_name)
            if val:
                return PresentedCredential({"header": header_name, "raw": val})
        return NoCredentials()


class IngressIdentityAuthenticator:
    """Async credential verifier validating extracted ingress identity claims."""

    __slots__ = ("_trusted_san_prefixes", "name", "participates_by_default", "slot")

    def __init__(
        self,
        *,
        trusted_san_prefixes: Sequence[str] = (),
        name: str = "ingress",
        slot: str = "ingress_identity",
        participates_by_default: bool = True,
    ) -> None:
        """Initialize the authenticator with optional trusted SAN prefixes."""
        ensure_module("litestar_security", extra="security")
        self._trusted_san_prefixes = tuple(trusted_san_prefixes)
        self.name = name
        self.slot = slot
        self.participates_by_default = participates_by_default

    async def authenticate(
        self,
        credential: Any,
        connection: ASGIConnection[Any, Any, Any, Any],
    ) -> Any:
        """Verify the presented ingress certificate or XFCC headers."""
        _ = connection
        payload = getattr(credential, "value", credential)
        if not isinstance(payload, dict):
            return InvalidCredentials("Malformed ingress credential")

        raw = payload.get("raw", "")
        header_name = payload.get("header", "x-forwarded-client-cert")

        claims: dict[str, Any] = {"header": header_name}
        if header_name == "x-forwarded-client-cert":
            parsed = self._parse_xfcc(raw)
            claims.update(parsed)
        else:
            claims["subject"] = raw
            claims["identity"] = raw

        identity = claims.get("uri") or claims.get("subject") or claims.get("identity")
        if not identity:
            return InvalidCredentials("No identity found in ingress credentials")

        if self._trusted_san_prefixes and not any(identity.startswith(prefix) for prefix in self._trusted_san_prefixes):
            return InvalidCredentials(f"Ingress identity '{identity}' does not match any trusted prefix")

        evidence = AuthenticationEvidence(
            mechanism=self.name,
            slot=self.slot,
            authenticated_at=datetime.now(UTC),
        )
        return Authenticated(claims=claims, evidence=evidence)

    def _parse_xfcc(self, header: str) -> dict[str, str]:
        """Parse Envoy / Istio XFCC header fields."""
        fields: dict[str, str] = {}
        for part in header.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                fields[k.strip().lower()] = v.strip().strip('"')
        return fields


class IngressIdentityResolver:
    """Async identity resolver mapping verified ingress claims to an authenticated Principal."""

    __slots__ = ("_principal_factory",)

    def __init__(
        self,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        """Initialize the identity resolver with an optional principal factory."""
        self._principal_factory = principal_factory

    async def resolve(self, claims: dict[str, Any]) -> Any:
        """Resolve verified ingress claims to a Principal."""
        identity = claims.get("uri") or claims.get("subject") or claims.get("identity")
        if not identity:
            return InvalidCredentials("Missing identity in ingress claims")

        user = self._principal_factory(claims) if self._principal_factory is not None else claims

        return Principal(
            id=str(identity),
            display_name=claims.get("subject", identity),
            user=user,
        )


class IngressIdentityMechanism(AuthenticationMechanism[Any, dict[str, Any], Any]):
    """Pair an ingress identity authenticator with an identity resolver and OpenAPI scheme."""

    def __init__(
        self,
        *,
        trusted_san_prefixes: Sequence[str] = (),
        scheme_name: str = "IngressIdentity",
        security_scheme: SecurityScheme | None = None,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
        slot: str = "ingress_identity",
        session_capable: bool = False,
    ) -> None:
        """Initialize the ingress identity mechanism."""
        ensure_module("litestar_security", extra="security")
        authenticator = IngressIdentityAuthenticator(
            trusted_san_prefixes=trusted_san_prefixes,
            slot=slot,
        )
        resolver = IngressIdentityResolver(principal_factory)
        resolved_scheme = (
            security_scheme
            if security_scheme is not None
            else SecurityScheme(
                type="apiKey",
                name="x-forwarded-client-cert",
                security_scheme_in="header",
                description="Trusted mTLS ingress client certificate identity",
            )
        )
        super().__init__(
            authenticator=authenticator,
            resolver=resolver,
            scheme_name=scheme_name,
            security_scheme=resolved_scheme,
            session_capable=session_capable,
        )


def create_ingress_identity_mechanism(
    *,
    trusted_san_prefixes: Sequence[str] = (),
    scheme_name: str = "IngressIdentity",
    security_scheme: SecurityScheme | None = None,
    principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    slot: str = "ingress_identity",
    session_capable: bool = False,
) -> AuthenticationMechanism[Any, Any, Any]:
    """Create an IngressIdentityMechanism for trusted mTLS/XFCC proxy ingress."""
    return IngressIdentityMechanism(
        trusted_san_prefixes=trusted_san_prefixes,
        scheme_name=scheme_name,
        security_scheme=security_scheme,
        principal_factory=principal_factory,
        slot=slot,
        session_capable=session_capable,
    )
