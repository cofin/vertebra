"""Explicit trusted proxy policies for agent caller identity."""

import ipaddress
import logging
import threading
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

import vertebra.agent.core.context as context_module
from vertebra.framework.security import AuthenticatedPrincipal, JWKSConfig, JWKSVerifier
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from vertebra.agent.core.context import CallerIdentity

logger = logging.getLogger(__name__)

IAP_PUBLIC_KEY_URL = "https://www.gstatic.com/iap/verify/public_key-jwk"
IAP_ISSUER = "https://cloud.google.com/iap"
IAP_ASSERTION_HEADER = "x-goog-iap-jwt-assertion"

__all__ = [
    "IAP_ASSERTION_HEADER",
    "IAP_ISSUER",
    "IAP_PUBLIC_KEY_URL",
    "CallableIdentitySource",
    "IapIdentitySource",
    "IdentityRequest",
    "IdentitySource",
    "JwksIdentitySource",
    "MtlsIdentitySource",
    "caller_identity_from_principal",
    "extract_bearer_token",
]


class IdentityRequest(msgspec.Struct, frozen=True, kw_only=True):
    """Normalized request headers and immediate peer for an identity policy."""

    headers: Mapping[str, str]
    peer: str | None = None


class IdentitySource(Protocol):
    """Application policy resolving a caller from trusted request metadata."""

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve identity, or return None when the request establishes none."""
        ...


class MtlsIdentitySource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Trust a certificate subject header only from explicitly configured proxies."""

    trusted_proxies: tuple[str, ...]
    header: str = "x-vertebra-mtls-subject"

    def __post_init__(self) -> None:
        if not self.trusted_proxies:
            raise ValueError("MtlsIdentitySource requires at least one trusted proxy CIDR")
        for value in self.trusted_proxies:
            try:
                ipaddress.ip_network(value, strict=False)
            except ValueError as exc:
                raise ValueError(f"Invalid trusted proxy CIDR: {value!r}") from exc
        msgspec.structs.force_setattr(self, "header", self.header.lower())

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve the certificate subject only for peers inside trusted networks."""
        if not request.peer or not (subject := request.headers.get(self.header)):
            return None
        try:
            peer = ipaddress.ip_address(request.peer)
        except ValueError:
            return None
        if any(peer in ipaddress.ip_network(value, strict=False) for value in self.trusted_proxies):
            return context_module.CallerIdentity(client_cert_subject=subject, auth_scheme="mtls")
        return None


class CallableIdentitySource(msgspec.Struct, frozen=True, kw_only=True):
    """Delegate caller resolution to an application-owned synchronous policy."""

    resolver: Callable[[IdentityRequest], "CallerIdentity | None"]

    def __post_init__(self) -> None:
        if not callable(self.resolver):
            raise TypeError("CallableIdentitySource.resolver must be callable")

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve identity with the configured application policy."""
        return self.resolver(request)


class IapIdentitySource:
    """Verify Google Cloud Identity-Aware Proxy ES256 JWT assertion headers."""

    def __init__(
        self,
        audience: str,
        *,
        key_url: str = IAP_PUBLIC_KEY_URL,
        key_cache_seconds: float = 3600.0,
        jwks: Mapping[str, Any] | None = None,
    ) -> None:
        """Configure IAP audience, JWK endpoint, cache TTL, or static JWK set."""
        normalized_aud = audience.strip() if isinstance(audience, str) else ""
        parts = [segment for segment in normalized_aud.split("/") if segment]
        if (
            not normalized_aud
            or not normalized_aud.startswith("/projects/")
            or len(parts) < 4
            or not parts[1].isdigit()
        ):
            msg = f"Invalid IAP audience {audience!r}; expected '/projects/<number>/...'"
            raise ValueError(msg)
        if key_cache_seconds < 0:
            msg = "key_cache_seconds must be non-negative"
            raise ValueError(msg)
        self.audience = normalized_aud
        self.key_url = key_url
        self.key_cache_seconds = float(key_cache_seconds)
        self.jwks: dict[str, Any] | None = dict(jwks) if jwks is not None else None
        self._lock = threading.Lock()
        self._cached_jwks: dict[str, dict[str, Any]] = {}
        self._cached_at: float = 0.0
        if self.jwks is not None:
            self._cached_jwks = self._index_jwks(self.jwks)

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Verify x-goog-iap-jwt-assertion using ES256 and return a verified CallerIdentity."""
        jwt_mod = ensure_module(
            "jwt",
            package="pyjwt",
            install_package="pyjwt[crypto]",
            extra="iap",
        )
        assertion: str | None = None
        for key, value in request.headers.items():
            if key.lower() == IAP_ASSERTION_HEADER:
                assertion = value.strip()
                break
        if not assertion:
            return None

        try:
            unverified_header = jwt_mod.get_unverified_header(assertion)
            if not isinstance(unverified_header, Mapping) or unverified_header.get("alg") != "ES256":
                return None
            kid = unverified_header.get("kid")
            if not isinstance(kid, str) or not kid:
                return None
            jwk_entry = self._resolve_jwk(kid)
            if jwk_entry is None:
                return None
            signing_key = jwt_mod.PyJWK.from_dict(jwk_entry).key
            claims = jwt_mod.decode(
                assertion,
                signing_key,
                algorithms=["ES256"],
                audience=self.audience,
                issuer=IAP_ISSUER,
                options={"require": ["exp", "aud", "iss", "sub"]},
            )
        except Exception as exc:
            logger.warning("IAP assertion verification failed (%s)", type(exc).__name__)
            return None

        if not isinstance(claims, Mapping):
            return None
        subject = claims.get("sub")
        email = claims.get("email")
        user_id = str(subject) if isinstance(subject, str) and subject else None
        email_str = str(email) if isinstance(email, str) and email else None
        if user_id is None and email_str is None:
            return None
        return context_module.CallerIdentity(
            user_id=user_id,
            email=email_str,
            auth_scheme="iap",
        )

    def _resolve_jwk(self, kid: str) -> dict[str, Any] | None:
        if self.jwks is not None:
            return self._cached_jwks.get(kid)

        with self._lock:
            now = time.monotonic()
            is_fresh = bool(self._cached_jwks) and (now - self._cached_at) < self.key_cache_seconds
            if is_fresh and kid in self._cached_jwks:
                return self._cached_jwks[kid]
            try:
                fetched = self._fetch_jwks()
            except Exception as exc:
                logger.warning("Failed to fetch IAP JWK set from %s (%s)", self.key_url, type(exc).__name__)
                return self._cached_jwks.get(kid) if is_fresh else None
            self._cached_jwks = self._index_jwks(fetched)
            self._cached_at = now
            return self._cached_jwks.get(kid)

    def _fetch_jwks(self) -> dict[str, Any]:
        req = urllib.request.Request(self.key_url, headers={"Accept": "application/json"}, method="GET")
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            if getattr(resp, "status", 200) != 200:
                msg = f"Unexpected status {getattr(resp, 'status', 0)}"
                raise RuntimeError(msg)
            payload = decode_json(resp.read())
        if not isinstance(payload, Mapping):
            msg = "IAP JWK response must be a JSON object"
            raise ValueError(msg)
        return dict(payload)

    @staticmethod
    def _index_jwks(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
        keys = payload.get("keys")
        if not isinstance(keys, list):
            return {}
        indexed: dict[str, dict[str, Any]] = {}
        for entry in keys:
            if isinstance(entry, Mapping):
                kid = entry.get("kid")
                if isinstance(kid, str) and kid:
                    indexed[kid] = dict(entry)
        return indexed


def extract_bearer_token(
    headers: Mapping[str, Any] | Sequence[tuple[bytes, bytes]],
    *,
    header_name: str = "authorization",
) -> str | None:
    """Extract a Bearer token string from HTTP/ASGI headers, returning None if absent or non-Bearer."""
    target = header_name.lower()
    raw_value: str | None = None
    if isinstance(headers, Mapping):
        for key, value in headers.items():
            if str(key).lower() == target and isinstance(value, str):
                raw_value = value
                break
    else:
        for raw_key, raw_val in headers:
            if raw_key.decode("latin-1").lower() == target:
                raw_value = raw_val.decode("latin-1")
                break
    if not raw_value:
        return None
    parts = raw_value.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip()


def caller_identity_from_principal(
    principal: AuthenticatedPrincipal,
    *,
    tenant_claim: str = "tenant_id",
    roles_claim: str = "roles",
) -> "CallerIdentity":
    """Convert a verified ``AuthenticatedPrincipal`` into a ``CallerIdentity``."""
    claims = principal.claims
    user_id = principal.sub if principal.sub else None
    raw_email = claims.get("email")
    email = raw_email if isinstance(raw_email, str) and raw_email else None
    raw_tenant = claims.get(tenant_claim)
    tenant_id = raw_tenant.strip() if isinstance(raw_tenant, str) and raw_tenant.strip() else "default"
    raw_roles = claims.get(roles_claim)
    if isinstance(raw_roles, str):
        roles = tuple(role for role in raw_roles.split() if role)
    elif isinstance(raw_roles, (list, tuple)):
        roles = tuple(str(role) for role in raw_roles if str(role))
    else:
        roles = ()
    return context_module.CallerIdentity(
        user_id=user_id,
        email=email,
        tenant_id=tenant_id,
        roles=roles,
        scopes=principal.scopes,
        auth_scheme="jwt",
    )


class JwksIdentitySource:
    """Resolve ``CallerIdentity`` from inbound Bearer JWTs verified against a JWKS endpoint."""

    def __init__(
        self,
        verifier: JWKSVerifier | None = None,
        *,
        config: JWKSConfig | None = None,
        async_client: Any = None,
        sync_client: Any = None,
        clock: Callable[[], float] | None = None,
        tenant_claim: str = "tenant_id",
        roles_claim: str = "roles",
    ) -> None:
        if verifier is None and config is None:
            msg = "JwksIdentitySource requires either verifier or config"
            raise ValueError(msg)
        if verifier is not None and config is not None:
            msg = "JwksIdentitySource accepts either verifier or config, not both"
            raise ValueError(msg)
        if verifier is not None:
            self.verifier = verifier
        else:
            assert config is not None
            self.verifier = JWKSVerifier(
                config,
                async_client=async_client,
                sync_client=sync_client,
                clock=clock,
            )
        self.tenant_claim = tenant_claim
        self.roles_claim = roles_claim

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Synchronously verify an Authorization Bearer JWT and return a CallerIdentity or None."""
        token = extract_bearer_token(request.headers)
        if not token:
            return None
        try:
            principal = self.verifier.verify_token_sync(token)
        except Exception as exc:
            logger.warning("JWKS bearer verification failed (%s)", type(exc).__name__)
            return None
        return caller_identity_from_principal(
            principal,
            tenant_claim=self.tenant_claim,
            roles_claim=self.roles_claim,
        )

    async def resolve_async(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Asynchronously verify an Authorization Bearer JWT and return a CallerIdentity or None."""
        token = extract_bearer_token(request.headers)
        if not token:
            return None
        try:
            principal = await self.verifier.verify_token(token)
        except Exception as exc:
            logger.warning("JWKS bearer verification failed (%s)", type(exc).__name__)
            return None
        return caller_identity_from_principal(
            principal,
            tenant_claim=self.tenant_claim,
            roles_claim=self.roles_claim,
        )
