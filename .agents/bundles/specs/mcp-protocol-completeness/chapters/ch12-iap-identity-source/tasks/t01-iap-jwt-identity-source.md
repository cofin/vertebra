---
type: Task
id: "mcp-protocol-completeness:ch12:t01"
chapter_id: ch12-iap-identity-source
title: Verify IAP signed-header JWTs as an identity source
description: Add IapIdentitySource, which verifies the ES256 x-goog-iap-jwt-assertion against Google's IAP public keys with the configured audience, issuer, and expiry, never trusts x-goog-authenticated-user-email, and fails loud on missing audience or missing pyjwt.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch01:t01"
read_set:
  - src/py/vertebra/agent/core/identity.py
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/_typing.py
  - pyproject.toml
  - src/py/tests/unit/agent/test_core_and_observability.py
files:
  - src/py/vertebra/agent/core/identity.py
  - pyproject.toml
  - uv.lock
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
estimated_loc: 190
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: identity-module
    resource: src/py/vertebra/agent/core/identity.py::IdentitySource
    title: Identity-source protocol added by ch01:t01
  - id: missing-dependency
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Optional dependency error
  - id: iap-signed-headers
    resource: https://cloud.google.com/iap/docs/signed-headers-howto
    title: Securing your app with signed headers (Identity-Aware Proxy)
tags:
  - mcp
  - identity
  - iap
---

# Task t01: Verify IAP Signed-Header JWTs as an Identity Source

## Objective & Scope Boundary

Add `IapIdentitySource` to `vertebra.agent.core.identity` so applications behind Identity-Aware Proxy opt in to IAP identity by naming the expected audience [^identity-module] [^iap-signed-headers]. The source verifies `x-goog-iap-jwt-assertion` and is the only IAP identity path; the plain `x-goog-authenticated-user-email` header is never read. Declare `pyjwt[crypto]` as the `iap` optional extra. Out of scope: `MCPConfig` wiring (`ch01:t03`, which accepts any `IdentitySource`) and docs (`ch11`).

## Bounded Read Manifest

- `src/py/vertebra/agent/core/identity.py` — `IdentityRequest`, `IdentitySource`, preset struct style from `ch01:t01`.
- `src/py/vertebra/agent/core/context.py::CallerIdentity` — Identity fields and `auth_scheme` literal.
- `src/py/vertebra/_typing.py::MissingDependencyError` — `extra=` produces `vertebra-core[<extra>]` install hints [^missing-dependency].
- `pyproject.toml` — `[project.optional-dependencies]` layout.
- `src/py/tests/unit/agent/test_core_and_observability.py` — Identity-source tests from `ch01:t01`.

## Symbol & Signature Contracts

- `src/py/vertebra/agent/core/identity.py`:
  - `IAP_PUBLIC_KEY_URL: Final = "https://www.gstatic.com/iap/verify/public_key-jwk"`, `IAP_ISSUER: Final = "https://cloud.google.com/iap"`, `IAP_ASSERTION_HEADER: Final = "x-goog-iap-jwt-assertion"`.
  - `class IapIdentitySource` (plain class with `__slots__`, because it owns a key cache): `__init__(self, audience: str, *, key_url: str = IAP_PUBLIC_KEY_URL, key_cache_seconds: float = 3600.0, jwks: Mapping[str, Any] | None = None) -> None`.
    - Raises `ValueError("IapIdentitySource requires an audience")` when `audience` is empty.
    - Raises `ValueError(f"Invalid IAP audience: {audience!r}")` unless `audience` matches `^/projects/\d+/global/backendServices/\d+$` or `^/projects/\d+/apps/[a-z][-a-z0-9]{4,28}[a-z0-9]$`.
    - Imports `jwt` through `vertebra.utils.module_loader.import_optional`; when unavailable raises `MissingDependencyError("pyjwt", extra="iap")`.
    - With `jwks`, keys come from `jwt.PyJWKSet.from_dict(jwks)` and no network call occurs; otherwise `jwt.PyJWKClient(key_url, cache_jwk_set=True, lifespan=key_cache_seconds, timeout=5)` fetches and caches the key set.
  - `IapIdentitySource.resolve(self, request: IdentityRequest) -> CallerIdentity | None`:
    - Reads only `request.headers.get(IAP_ASSERTION_HEADER)`; absent -> `None`.
    - Selects the key by the token's `kid`, then `jwt.decode(token, key, algorithms=["ES256"], audience=self.audience, issuer=IAP_ISSUER, options={"require": ["exp", "iat", "aud", "iss", "sub", "email"]})`.
    - Success -> `CallerIdentity(user_id=claims["sub"], email=claims["email"], auth_scheme="iap")`.
    - Any `jwt.PyJWTError` (bad signature, wrong audience, wrong issuer, expired, missing claim, unknown `kid`, `alg` other than `ES256`) or key-fetch `jwt.PyJWKClientError` -> `None` and one `logger.warning("IAP assertion rejected: %s", type(exc).__name__)`; the token is never logged.
- `pyproject.toml`: `[project.optional-dependencies]` gains `iap = ["pyjwt[crypto]>=2.10"]`.
- `uv.lock`: regenerated by `uv lock` for the new extra; `pyjwt` and `cryptography` are already locked through `litestar-security`.

## Edge Cases & Error Semantics

- Verification failures never raise into the request path; the caller is anonymous and guards decide access.
- `x-goog-authenticated-user-email` alone, or alongside an invalid assertion, yields no identity.
- `alg: none` and `HS256` tokens are rejected because `algorithms` is pinned to `ES256`.
- Key-fetch timeouts reject the request identity rather than falling back to unverified headers.
- The source verifies regardless of peer address; IAP strips client-supplied `x-goog-iap-jwt-assertion` headers, and signature verification prevents forgery from other paths.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_core_and_observability.py` (in-process; parametrized `test_iap_identity_source`, `test_iap_identity_source_config_errors`). A module-level fixture generates an EC P-256 key with `cryptography`, builds `jwks = {"keys": [{**jwt.algorithms.ECAlgorithm.to_jwk(public_key, as_dict=True), "kid": "k1", "alg": "ES256"}]}`, and signs claims with `jwt.encode(claims, private_key, algorithm="ES256", headers={"kid": "k1"})`. `AUD = "/projects/123/global/backendServices/456"`; base claims `{"iss": IAP_ISSUER, "aud": AUD, "sub": "accounts.google.com:1", "email": "ana@example.com", "iat": now, "exp": now + 600}`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Valid JWT | `IapIdentitySource(AUD, jwks=jwks).resolve(IdentityRequest(headers={"x-goog-iap-jwt-assertion": token}))` | `user_id == "accounts.google.com:1"`, `email == "ana@example.com"`, `auth_scheme == "iap"` |
| Valid JWT through context | `AgentContext.from_headers({"X-Goog-IAP-JWT-Assertion": token}, identity_source=src)` | `identity.email == "ana@example.com"`, `is_authenticated is True` |
| App Engine audience | audience and `aud` `"/projects/123/apps/my-project"` | Identity returned |
| Bad signature | token signed with a second generated key, same `kid` | `None`; warning logged with `InvalidSignatureError` |
| Wrong audience | claims `aud="/projects/999/global/backendServices/1"` | `None` |
| Wrong issuer | claims `iss="https://accounts.google.com"` | `None` |
| Expired | claims `exp=now - 60`, `iat=now - 660` | `None` |
| Unknown kid | header `kid="k2"` | `None` |
| HS256 token | `jwt.encode(claims, "secret", algorithm="HS256", headers={"kid": "k1"})` | `None` |
| Plain email header alone | `from_headers({"x-goog-authenticated-user-email": "accounts.google.com:ana@example.com"}, identity_source=src)` | `email is None`, `auth_scheme == "anonymous"` |
| Email header with invalid assertion | email header plus expired token | `email is None` |
| Key fetch failure | `IapIdentitySource(AUD, key_url="http://127.0.0.1:9/keys")` with a valid token | `None`; warning logged with `PyJWKClientError` |
| Config error: no audience | `IapIdentitySource("")` | `ValueError` matching `"requires an audience"` |
| Config error: malformed audience | `IapIdentitySource("my-backend")` | `ValueError` matching `"Invalid IAP audience"` |
| Config error: pyjwt missing | `monkeypatch` `import_optional` in `vertebra.agent.core.identity` to return `None`; `IapIdentitySource(AUD)` | `MissingDependencyError`; `install_package == "vertebra-core[iap]"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q -k iap` fails with `ImportError` for `IapIdentitySource`.
- **Green**: `uv lock && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q && uv run --no-sync ruff check src/py/vertebra/agent/core && uv run --no-sync mypy src/py/vertebra/agent/core/identity.py`.

## Acceptance Criteria

- [ ] R1.4: IAP identity comes only from a verified ES256 `x-goog-iap-jwt-assertion` with the configured audience, IAP issuer, and unexpired `exp`; the plain email header never sets identity.
- [ ] R1.3: a missing or malformed audience and a missing `pyjwt` fail at construction.
- [ ] `iap` extra declared; `uv.lock` updated.
- [ ] No inline `#` comments; module-level imports only.

## Notes & Discoveries

- [2026-11-12 00:30] `pyjwt` 2.15.1 with the `crypto` extra and `cryptography` are locked at `c8722c76` as dependencies of `litestar-security`; `jwt.PyJWKClient.__init__` accepts `cache_jwk_set`, `lifespan`, and `timeout`.

[^identity-module]: `src/py/vertebra/agent/core/identity.py::IdentitySource` — Identity-source protocol added by ch01:t01.
[^missing-dependency]: `src/py/vertebra/_typing.py::MissingDependencyError` — Optional dependency error.
[^iap-signed-headers]: https://cloud.google.com/iap/docs/signed-headers-howto — Securing your app with signed headers (Identity-Aware Proxy).
