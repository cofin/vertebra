"""Accelerated JWT authentication mechanism backed by Rust JWTGuard."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from litestar.openapi.spec import SecurityScheme

from vertebra._vertebra import JWTGuard
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json

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
    from collections.abc import Callable

    from litestar.connection import ASGIConnection


class BearerTokenSlot:
    """Synchronous credential slot extracting Bearer tokens from the Authorization header."""

    __slots__ = ("name",)

    def __init__(self, name: str = "bearer") -> None:
        """Initialize the bearer token slot."""
        self.name = name

    def extract(self, connection: ASGIConnection[Any, Any, Any, Any]) -> Any:
        """Extract Bearer credential from connection headers."""
        auth_header = connection.headers.get("authorization")
        if not auth_header:
            return NoCredentials()
        parts = auth_header.split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return InvalidCredentials("Malformed Authorization header; expected 'Bearer <token>'")
        token = parts[1].strip()
        if not token:
            return InvalidCredentials("Empty Bearer token in Authorization header")
        return PresentedCredential(token)


class JWTAuthenticator:
    """Async credential verifier using Rust JWTGuard for zero-copy HS256 validation."""

    __slots__ = ("_guard", "name", "participates_by_default", "slot")

    def __init__(
        self,
        secret: str | bytes,
        *,
        leeway_secs: int = 60,
        name: str = "jwt",
        slot: str = "bearer",
        participates_by_default: bool = True,
    ) -> None:
        """Initialize the JWT authenticator with secret and validation leeway."""
        ensure_module("litestar_security", extra="security")
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        self._guard = JWTGuard(secret_bytes, leeway_secs)
        self.name = name
        self.slot = slot
        self.participates_by_default = participates_by_default

    async def authenticate(
        self,
        credential: Any,
        connection: ASGIConnection[Any, Any, Any, Any],
    ) -> Any:
        """Verify the presented JWT token and produce claims."""
        _ = connection
        token = getattr(credential, "value", credential)
        if not isinstance(token, str):
            return InvalidCredentials("Invalid token format")
        try:
            claims_json = self._guard.validate(token)
            claims = decode_json(claims_json)
        except (ValueError, Exception) as exc:
            return InvalidCredentials(f"Invalid or expired JWT: {exc}")

        expires_at: datetime | None = None
        exp = claims.get("exp")
        if isinstance(exp, (int, float)):
            expires_at = datetime.fromtimestamp(exp, tz=UTC)

        evidence = AuthenticationEvidence(
            mechanism=self.name,
            slot=self.slot,
            authenticated_at=datetime.now(UTC),
            expires_at=expires_at,
        )
        return Authenticated(claims=claims, evidence=evidence)


class JWTIdentityResolver:
    """Async identity resolver mapping verified JWT claims to a Principal."""

    __slots__ = ("_principal_factory",)

    def __init__(
        self,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        """Initialize the identity resolver with an optional principal factory."""
        self._principal_factory = principal_factory

    async def resolve(self, claims: dict[str, Any]) -> Any:
        """Resolve verified claims to an application principal."""
        sub = claims.get("sub")
        if sub is None:
            return InvalidCredentials("Missing subject claim in JWT")

        user = self._principal_factory(claims) if self._principal_factory is not None else claims

        return Principal(
            id=str(sub),
            display_name=claims.get("name"),
            user=user,
        )


class JWTAuthenticationMechanism(AuthenticationMechanism[Any, dict[str, Any], Any]):
    """Pair a Rust-accelerated JWT authenticator with an identity resolver and OpenAPI scheme."""

    def __init__(
        self,
        secret: str | bytes,
        *,
        leeway_secs: int = 60,
        scheme_name: str = "BearerAuth",
        security_scheme: SecurityScheme | None = None,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
        slot: str = "bearer",
        session_capable: bool = False,
    ) -> None:
        """Initialize the JWT authentication mechanism with Rust verification."""
        ensure_module("litestar_security", extra="security")
        authenticator = JWTAuthenticator(secret, leeway_secs=leeway_secs, slot=slot)
        resolver = JWTIdentityResolver(principal_factory)
        resolved_scheme = (
            security_scheme
            if security_scheme is not None
            else SecurityScheme(type="http", scheme="bearer", bearer_format="JWT")
        )
        super().__init__(
            authenticator=authenticator,
            resolver=resolver,
            scheme_name=scheme_name,
            security_scheme=resolved_scheme,
            session_capable=session_capable,
        )


def create_jwt_authentication_mechanism(
    secret: str | bytes,
    *,
    leeway_secs: int = 60,
    scheme_name: str = "BearerAuth",
    security_scheme: SecurityScheme | None = None,
    principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    slot: str = "bearer",
    session_capable: bool = False,
) -> AuthenticationMechanism[Any, Any, Any]:
    """Create an AuthenticationMechanism backed by Rust JWTGuard."""
    return JWTAuthenticationMechanism(
        secret,
        leeway_secs=leeway_secs,
        scheme_name=scheme_name,
        security_scheme=security_scheme,
        principal_factory=principal_factory,
        slot=slot,
        session_capable=session_capable,
    )
