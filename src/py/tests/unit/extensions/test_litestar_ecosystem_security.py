"""Unit tests for Vertebra litestar-security integration."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import subprocess
import sys
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from litestar import Litestar, get
from litestar.exceptions import PermissionDeniedException
from litestar.testing import TestClient

from vertebra.extensions.litestar.security import (
    BearerTokenSlot,
    IngressIdentityAuthenticator,
    IngressIdentityMechanism,
    IngressIdentityResolver,
    IngressIdentitySlot,
    JWTAuthenticationMechanism,
    JWTAuthenticator,
    JWTIdentityResolver,
    RateLimitGuard,
    configure_vertebra_exclusions,
    create_ingress_identity_mechanism,
    create_jwt_authentication_mechanism,
    exempt_vertebra_routes,
)
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

TEST_SECRET = b"test-secret-key-at-least-32-bytes-long-for-jwt"


def _create_jwt_token(payload: dict[str, Any], secret: bytes = TEST_SECRET) -> str:
    """Helper creating signed HS256 JWT tokens for testing."""
    header = {"alg": "HS256", "typ": "JWT"}

    def b64url(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    h_b64 = b64url(json.dumps(header).encode("utf-8"))
    p_b64 = b64url(json.dumps(payload).encode("utf-8"))
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    sig = hmac.new(secret, signing_input, hashlib.sha256).digest()
    return f"{h_b64}.{p_b64}.{b64url(sig)}"


def test_bearer_token_slot_extraction() -> None:
    """Verify BearerTokenSlot extracts valid tokens and rejects malformed headers."""
    slot = BearerTokenSlot()

    conn_valid = MagicMock()
    conn_valid.headers = {"authorization": "Bearer valid-token-123"}
    res_valid = slot.extract(conn_valid)
    assert res_valid.value == "valid-token-123"

    conn_missing = MagicMock()
    conn_missing.headers = {}
    res_missing = slot.extract(conn_missing)
    assert type(res_missing).__name__ == "NoCredentials"

    conn_malformed = MagicMock()
    conn_malformed.headers = {"authorization": "Basic xyz"}
    res_malformed = slot.extract(conn_malformed)
    assert type(res_malformed).__name__ == "InvalidCredentials"


@pytest.mark.anyio
async def test_jwt_authenticator_valid_token() -> None:
    """Verify JWTAuthenticator verifies valid JWT tokens and extracts claims."""
    authenticator = JWTAuthenticator(TEST_SECRET)
    token = _create_jwt_token(
        {
            "sub": "user-42",
            "name": "Jane Doe",
            "exp": int(time.time()) + 3600,
        }
    )

    conn = MagicMock()
    outcome = await authenticator.authenticate(token, conn)
    assert type(outcome).__name__ == "Authenticated"
    assert outcome.claims["sub"] == "user-42"
    assert outcome.claims["name"] == "Jane Doe"


@pytest.mark.anyio
async def test_jwt_authenticator_expired_token() -> None:
    """Verify JWTAuthenticator rejects expired tokens with InvalidCredentials."""
    authenticator = JWTAuthenticator(TEST_SECRET, leeway_secs=0)
    token = _create_jwt_token(
        {
            "sub": "user-42",
            "exp": int(time.time()) - 3600,
        }
    )

    conn = MagicMock()
    outcome = await authenticator.authenticate(token, conn)
    assert type(outcome).__name__ == "InvalidCredentials"


@pytest.mark.anyio
async def test_jwt_identity_resolver_principal() -> None:
    """Verify JWTIdentityResolver creates an authenticated Principal."""
    resolver = JWTIdentityResolver()
    claims = {"sub": "user-99", "name": "Agent"}
    principal = await resolver.resolve(claims)
    assert principal.id == "user-99"
    assert principal.display_name == "Agent"


def test_create_jwt_authentication_mechanism() -> None:
    """Verify factory helper initializes JWTAuthenticationMechanism."""
    mech = create_jwt_authentication_mechanism(TEST_SECRET)
    assert isinstance(mech, JWTAuthenticationMechanism)


def test_ingress_identity_slot_extraction() -> None:
    """Verify IngressIdentitySlot extracts mTLS and XFCC header values."""
    slot = IngressIdentitySlot()

    conn_xfcc = MagicMock()
    conn_xfcc.headers = {"x-forwarded-client-cert": "By=spiffe://cluster/ns/svc;URI=spiffe://cluster/ns/client"}
    extracted = slot.extract(conn_xfcc)
    assert extracted.value["header"] == "x-forwarded-client-cert"
    assert "spiffe" in extracted.value["raw"]

    conn_empty = MagicMock()
    conn_empty.headers = {}
    assert type(slot.extract(conn_empty)).__name__ == "NoCredentials"


@pytest.mark.anyio
async def test_ingress_identity_authenticator_trusted_prefixes() -> None:
    """Verify IngressIdentityAuthenticator enforces trusted SAN prefixes."""
    auth_trusted = IngressIdentityAuthenticator(
        trusted_san_prefixes=("spiffe://trusted.corp/",),
    )
    conn = MagicMock()

    trusted_cred = {
        "header": "x-forwarded-client-cert",
        "raw": "URI=spiffe://trusted.corp/ns/app",
    }
    outcome_trusted = await auth_trusted.authenticate(trusted_cred, conn)
    assert type(outcome_trusted).__name__ == "Authenticated"

    untrusted_cred = {
        "header": "x-forwarded-client-cert",
        "raw": "URI=spiffe://attacker.corp/ns/app",
    }
    outcome_untrusted = await auth_trusted.authenticate(untrusted_cred, conn)
    assert type(outcome_untrusted).__name__ == "InvalidCredentials"


@pytest.mark.anyio
async def test_ingress_identity_resolver() -> None:
    """Verify IngressIdentityResolver maps claims to Principal."""
    resolver = IngressIdentityResolver()
    claims = {"uri": "spiffe://trusted.corp/app", "subject": "app-cert"}
    principal = await resolver.resolve(claims)
    assert principal.id == "spiffe://trusted.corp/app"


def test_create_ingress_identity_mechanism() -> None:
    """Verify factory creates IngressIdentityMechanism."""
    mech = create_ingress_identity_mechanism(trusted_san_prefixes=("spiffe://",))
    assert isinstance(mech, IngressIdentityMechanism)


def test_rate_limit_guard_admission_and_rejection() -> None:
    """Verify RateLimitGuard admits calls up to capacity and rejects excess calls."""
    guard = RateLimitGuard(capacity=2, refill_rate_per_sec=0.1)
    conn = MagicMock()
    conn.client.host = "127.0.0.1"
    handler = MagicMock()

    guard(conn, handler)
    guard(conn, handler)

    with pytest.raises(PermissionDeniedException) as exc_info:
        guard(conn, handler)
    assert exc_info.value.status_code == 403


def test_exempt_vertebra_routes_opt_in() -> None:
    """Verify exempt_vertebra_routes marks handlers as excluded from authentication."""
    app_config = MagicMock()
    handler = MagicMock()
    handler.paths = ["/__vertebra/health"]
    handler.opt = {}
    app_config.route_handlers = [handler]
    exempt_vertebra_routes(app_config)
    assert handler.opt.get("exclude_from_auth") is True


def test_configure_vertebra_exclusions() -> None:
    """Verify configure_vertebra_exclusions does not warn on empty security configs."""
    cfg = MagicMock()
    cfg.exclude = []
    configure_vertebra_exclusions(cfg)
    assert isinstance(cfg.exclude, list)


def test_missing_dependency_error_on_missing_security(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='security' when litestar_security is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_security", extra="security")
    assert "security" in exc_info.value.install_package


def test_rate_limit_guard_on_litestar_route() -> None:
    """RateLimitGuard delegates to neutral RateLimitGuard and enforces HTTP 403 on real Litestar routes."""
    guard = RateLimitGuard(capacity=2, refill_rate_per_sec=0.01, key_func=lambda _conn: "client-1")

    @get("/limited", guards=[guard], sync_to_thread=False)
    def limited_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    app = Litestar(route_handlers=[limited_endpoint])
    with TestClient(app=app) as client:
        assert client.get("/limited").status_code == 200
        assert client.get("/limited").status_code == 200
        assert client.get("/limited").status_code == 403


@pytest.mark.parametrize(
    "module_name",
    [
        "vertebra.extensions.litestar.security.ingress",
        "vertebra.extensions.litestar.security.jwt",
        "vertebra.extensions.litestar.security.ratelimit",
    ],
)
def test_security_submodules_import_with_host_packages_blocked(module_name: str) -> None:
    """Security submodules import cleanly when litestar and litestar_security are blocked in sys.modules."""
    script = (
        "import importlib, sys\n"
        "for mod in ('litestar', 'litestar.exceptions', 'litestar.openapi', 'litestar.openapi.spec', "
        "'litestar_security'):\n"
        "    sys.modules[mod] = None\n"
        f"imported = importlib.import_module({module_name!r})\n"
        "from vertebra.typing import MissingDependencyError\n"
        f"if {module_name!r}.endswith('ratelimit'):\n"
        "    try:\n"
        "        imported.RateLimitGuard()\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for RateLimitGuard')\n"
        f"elif {module_name!r}.endswith('jwt'):\n"
        "    try:\n"
        "        imported.JWTAuthenticator('test-token-placeholder')\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for JWTAuthenticator')\n"
        f"elif {module_name!r}.endswith('ingress'):\n"
        "    try:\n"
        "        imported.create_ingress_identity_mechanism()\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for create_ingress_identity_mechanism')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
