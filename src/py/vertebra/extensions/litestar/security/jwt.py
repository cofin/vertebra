"""Accelerated JWT authentication mechanism delegating to neutral security guards."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from vertebra.framework.security import JWKSVerifier, JWTAuthGuard
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar.connection import ASGIConnection
    from litestar.openapi.spec import SecurityScheme
    from litestar_security import AuthenticationMechanism

__all__ = (
    "BearerTokenSlot",
    "JWTAuthenticationMechanism",
    "JWTAuthenticator",
    "JWTIdentityResolver",
    "create_jwt_authentication_mechanism",
)

_CACHED_MECHANISM_CLS: type[Any] | None = None


class BearerTokenSlot:
    """Synchronous credential slot extracting Bearer tokens from the Authorization header."""

    __slots__ = ("name",)

    def __init__(self, name: str = "bearer") -> None:
        """Initialize the bearer token slot."""
        ensure_module("litestar_security", extra="security")
        self.name = name

    def extract(self, connection: ASGIConnection[Any, Any, Any, Any]) -> Any:
        """Extract Bearer credential from connection headers."""
        sec_mod = ensure_module("litestar_security", extra="security")
        auth_header = connection.headers.get("authorization")
        if not auth_header:
            return sec_mod.NoCredentials()
        parts = auth_header.split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return sec_mod.InvalidCredentials("Malformed Authorization header; expected 'Bearer <token>'")
        token = parts[1].strip()
        if not token:
            return sec_mod.InvalidCredentials("Empty Bearer token in Authorization header")
        return sec_mod.PresentedCredential(token)


class JWTAuthenticator:
    """Async credential verifier delegating HS256 or asymmetric JWKS validation to neutral guards."""

    __slots__ = ("_guard", "_jwks_verifier", "name", "participates_by_default", "slot")

    def __init__(
        self,
        secret: str | bytes | None = None,
        *,
        leeway_secs: int = 60,
        name: str = "jwt",
        slot: str = "bearer",
        participates_by_default: bool = True,
        jwks_verifier: JWKSVerifier | None = None,
    ) -> None:
        """Initialize the JWT authenticator with secret or JWKS verifier and leeway."""
        ensure_module("litestar_security", extra="security")
        if jwks_verifier is None and secret is None:
            msg = "Either secret or jwks_verifier must be provided"
            raise ValueError(msg)
        self._jwks_verifier = jwks_verifier
        self._guard = JWTAuthGuard(secret, leeway_secs=leeway_secs) if secret is not None else None
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
        sec_mod = ensure_module("litestar_security", extra="security")
        token = getattr(credential, "value", credential)
        if not isinstance(token, str):
            return sec_mod.InvalidCredentials("Invalid token format")
        try:
            if self._jwks_verifier is not None:
                principal = await self._jwks_verifier.verify_token(token)
            elif self._guard is not None:
                principal = self._guard.verify_token(token)
            else:
                return sec_mod.InvalidCredentials("JWT verifier is not configured")
            claims = dict(principal.claims)
        except Exception as exc:
            return sec_mod.InvalidCredentials(f"Invalid or expired JWT: {exc}")

        expires_at: datetime | None = None
        exp = claims.get("exp")
        if isinstance(exp, (int, float)):
            expires_at = datetime.fromtimestamp(exp, tz=UTC)

        evidence = sec_mod.AuthenticationEvidence(
            mechanism=self.name,
            slot=self.slot,
            authenticated_at=datetime.now(UTC),
            expires_at=expires_at,
        )
        return sec_mod.Authenticated(claims=claims, evidence=evidence)


class JWTIdentityResolver:
    """Async identity resolver mapping verified JWT claims to a Principal."""

    __slots__ = ("_principal_factory",)

    def __init__(
        self,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        """Initialize the identity resolver with an optional principal factory."""
        ensure_module("litestar_security", extra="security")
        self._principal_factory = principal_factory

    async def resolve(self, claims: dict[str, Any]) -> Any:
        """Resolve verified claims to an application principal."""
        sec_mod = ensure_module("litestar_security", extra="security")
        sub = claims.get("sub")
        if sub is None:
            return sec_mod.InvalidCredentials("Missing subject claim in JWT")

        user = self._principal_factory(claims) if self._principal_factory is not None else claims

        return sec_mod.Principal(
            id=str(sub),
            display_name=claims.get("name"),
            user=user,
        )


class _JWTAuthenticationMechanismImpl:
    """Pair a JWT authenticator with an identity resolver and OpenAPI scheme."""

    def __init__(
        self,
        secret: str | bytes | None = None,
        *,
        leeway_secs: int = 60,
        scheme_name: str = "BearerAuth",
        security_scheme: SecurityScheme | None = None,
        principal_factory: Callable[[dict[str, Any]], Any] | None = None,
        slot: str = "bearer",
        session_capable: bool = False,
        jwks_verifier: JWKSVerifier | None = None,
    ) -> None:
        """Initialize the JWT authentication mechanism."""
        sec_mod = ensure_module("litestar_security", extra="security")
        spec_mod = ensure_module("litestar.openapi.spec", extra="litestar")
        authenticator = JWTAuthenticator(
            secret,
            leeway_secs=leeway_secs,
            slot=slot,
            jwks_verifier=jwks_verifier,
        )
        resolver = JWTIdentityResolver(principal_factory)
        resolved_scheme = (
            security_scheme
            if security_scheme is not None
            else spec_mod.SecurityScheme(type="http", scheme="bearer", bearer_format="JWT")
        )
        sec_mod.AuthenticationMechanism.__init__(
            self,
            authenticator=authenticator,
            resolver=resolver,
            scheme_name=scheme_name,
            security_scheme=resolved_scheme,
            session_capable=session_capable,
        )


if TYPE_CHECKING:

    class JWTAuthenticationMechanism(
        _JWTAuthenticationMechanismImpl, AuthenticationMechanism[Any, dict[str, Any], Any]
    ):
        """Pair a JWT authenticator with an identity resolver and OpenAPI scheme."""


def _get_mechanism_cls() -> type[JWTAuthenticationMechanism]:
    global _CACHED_MECHANISM_CLS
    if _CACHED_MECHANISM_CLS is None:
        sec_mod = ensure_module("litestar_security", extra="security")
        base_cls = sec_mod.AuthenticationMechanism
        _CACHED_MECHANISM_CLS = type(
            "JWTAuthenticationMechanism",
            (_JWTAuthenticationMechanismImpl, base_cls),
            {
                "__module__": __name__,
                "__doc__": _JWTAuthenticationMechanismImpl.__doc__,
            },
        )
    return cast("type[JWTAuthenticationMechanism]", _CACHED_MECHANISM_CLS)


def __getattr__(name: str) -> Any:
    """Lazily bind ``JWTAuthenticationMechanism`` to ``litestar_security.AuthenticationMechanism``."""
    if name == "JWTAuthenticationMechanism":
        return _get_mechanism_cls()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound ``JWTAuthenticationMechanism``."""
    return sorted(set(globals()) | set(__all__))


def create_jwt_authentication_mechanism(
    secret: str | bytes | None = None,
    *,
    leeway_secs: int = 60,
    scheme_name: str = "BearerAuth",
    security_scheme: SecurityScheme | None = None,
    principal_factory: Callable[[dict[str, Any]], Any] | None = None,
    slot: str = "bearer",
    session_capable: bool = False,
    jwks_verifier: JWKSVerifier | None = None,
) -> AuthenticationMechanism[Any, Any, Any]:
    """Create an AuthenticationMechanism backed by neutral JWTAuthGuard or JWKSVerifier."""
    mechanism_cls = _get_mechanism_cls()
    return mechanism_cls(
        secret,
        leeway_secs=leeway_secs,
        scheme_name=scheme_name,
        security_scheme=security_scheme,
        principal_factory=principal_factory,
        slot=slot,
        session_capable=session_capable,
        jwks_verifier=jwks_verifier,
    )
