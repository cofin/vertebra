---
type: Task
id: "mcp-protocol-completeness:ch06:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch06-stdio-serve-bridge-and-gcp-auth
title: Implement IapIdentitySource ES256 Signed-Header JWT Verification
description: Add IapIdentitySource verifying x-goog-iap-jwt-assertion against Google IAP JWKs in agent/core/identity.py and declare the iap optional dependency extra.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/core/identity.py
  - pyproject.toml
  - uv.lock
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
read_set:
  - src/py/vertebra/agent/core/identity.py
  - src/py/vertebra/agent/core/context.py
  - pyproject.toml
  - src/py/tests/unit/agent/test_core_and_observability.py
estimated_loc: 180
tags:
  - mcp
  - iap
  - identity
  - security
sources:
  - id: identity-py
    resource: "src/py/vertebra/agent/core/identity.py::IdentitySource"
    title: IdentitySource protocol and mTLS/callable presets
  - id: iap-docs
    resource: "https://cloud.google.com/iap/docs/signed-headers-howto"
    title: Google Cloud IAP signed JWT header specification
---

# Task `t03`: Implement IapIdentitySource ES256 Signed-Header JWT Verification

## Objective & Scope Boundary

Implement `IapIdentitySource` in `src/py/vertebra/agent/core/identity.py` and declare the `iap` extra in `pyproject.toml` per [`../spec.md` §2](../spec.md) [^identity-py] [^iap-docs].

## Bounded Read Manifest

- `src/py/vertebra/agent/core/{identity,context}.py` & `pyproject.toml`.

## Contracts & Edge Cases

- Define `IAP_PUBLIC_KEY_URL = 'https://www.gstatic.com/iap/verify/public_key-jwk'`, `IAP_ISSUER = 'https://cloud.google.com/iap'`, `IAP_ASSERTION_HEADER = 'x-goog-iap-jwt-assertion'`, and `IapIdentitySource(audience, *, key_url=..., key_cache_seconds=3600.0, jwks=None)`.
- Validate non-empty `/projects/...` audience at construction (`ValueError` otherwise), load `jwt` via `module_loader` (`MissingDependencyError` naming `vertebra-core[iap]`), verify ES256 signature + `aud` + `iss` + `exp`, return `CallerIdentity(user_id=sub, email=email, auth_scheme='iap')`, and fail closed (`None` + warning without logging raw token) on bad signature, expired token, key-fetch error, or plain `x-goog-authenticated-user-email`.
- Add `iap = ['pyjwt[crypto]>=2.9.0']` to `pyproject.toml` and sync `uv.lock` via `uv lock --offline`.

## Test Matrix

- Parameterized static-JWK ES256 tests in `test_core_and_observability.py` covering valid assertion, wrong signing key, wrong audience, expired `exp`, plain `x-goog-authenticated-user-email` bypass attempt, unreachable JWK URL fail-closed `None`, and malformed audience `ValueError`.

## Verification

```bash
uv lock --check --offline && uv run --no-sync ruff check src/py/vertebra/agent/core/identity.py && uv run --no-sync mypy src/py/vertebra/agent/core && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q
```

## Acceptance Criteria

- [ ] `IapIdentitySource` authenticates only cryptographically verified ES256 `x-goog-iap-jwt-assertion` tokens.
- [ ] Plain `x-goog-authenticated-user-email` headers never grant caller identity.

[^identity-py]: `src/py/vertebra/agent/core/identity.py::IdentitySource` — IdentitySource protocol and mTLS/callable presets.
[^iap-docs]: `https://cloud.google.com/iap/docs/signed-headers-howto` — Google Cloud IAP signed JWT header specification.
