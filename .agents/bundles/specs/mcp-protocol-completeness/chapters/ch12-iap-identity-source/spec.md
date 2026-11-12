---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch12-iap-identity-source
title: "Chapter 12: IAP Identity Source"
description: Add the IAP identity-source preset that verifies the signed x-goog-iap-jwt-assertion JWT and never trusts the plain IAP email header.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-agent-context-propagation
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: identity-module
    resource: src/py/vertebra/agent/core/identity.py::IdentitySource
    title: Identity-source protocol added by ch01:t01
  - id: iap-signed-headers
    resource: https://cloud.google.com/iap/docs/signed-headers-howto
    title: Securing your app with signed headers (Identity-Aware Proxy)
tags:
  - mcp
  - identity
  - iap
---

# Chapter 12: IAP Identity Source

<!-- truth: start -->
- **Verified IAP identity**: `IapIdentitySource(audience)` verifies `x-goog-iap-jwt-assertion` as an ES256 JWT against the key set at `https://www.gstatic.com/iap/verify/public_key-jwk`, requiring the configured audience, issuer `https://cloud.google.com/iap`, and an unexpired `exp` [^identity-module] [^iap-signed-headers].
- **No plain header trust**: `x-goog-authenticated-user-email` never sets identity.
- **Fail loud at construction**: an empty or malformed audience raises `ValueError`; missing `pyjwt` raises `MissingDependencyError` with the `vertebra-core[iap]` hint.
- **Fail closed per request**: any verification or key-fetch failure yields no identity and logs the failure class without the token.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`AgentContext.from_request` reads `x-goog-authenticated-user-email` from any caller. Any client that reaches the server without passing through IAP can assert any email. Google documents the signed `x-goog-iap-jwt-assertion` header as the verifiable IAP identity [^iap-signed-headers]. This chapter adds the opt-in IAP preset for the identity-source seam introduced in `ch01:t01`.

### 1.2 Upstream Execution Baseline

- Depends on `ch01:t01` (`IdentityRequest`, `IdentitySource`, `vertebra.agent.core.identity`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam & Dependency Category |
| --- | --- | --- |
| Identity | `src/py/vertebra/agent/core/identity.py` (from `ch01:t01`) [^identity-module] | In-process; `IapIdentitySource.resolve` is the seam. |
| Keys | Google IAP JWK endpoint | Remote HTTP; tests inject a static `jwks` mapping. |
| Packaging | `pyproject.toml` optional dependencies | `pyjwt[crypto]` already locked through `litestar-security`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- `.agents/skills/python-framework/SKILL.md` for module layout and fail-loud conventions.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1.3**: Misconfigured identity sources fail at construction.
- **R1.4**: IAP identity comes only from a verified ES256 `x-goog-iap-jwt-assertion` with the configured audience, IAP issuer, and unexpired `exp`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
IAP_PUBLIC_KEY_URL: Final = "https://www.gstatic.com/iap/verify/public_key-jwk"
IAP_ISSUER: Final = "https://cloud.google.com/iap"
IAP_ASSERTION_HEADER: Final = "x-goog-iap-jwt-assertion"

class IapIdentitySource:
    def __init__(self, audience: str, *, key_url: str = IAP_PUBLIC_KEY_URL, key_cache_seconds: float = 3600.0, jwks: Mapping[str, Any] | None = None) -> None: ...
    def resolve(self, request: IdentityRequest) -> CallerIdentity | None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `MCPConfig(trust_proxy_identity=True, identity_source=IapIdentitySource(...))` -> adapters (`ch01:t03`, `ch01:t04`) -> `AgentContext.from_scope` / `from_headers` -> `IapIdentitySource.resolve` -> `jwt.PyJWKClient` / `jwt.decode`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Valid assertion | ES256 token, matching `aud`, `iss`, `exp` | `CallerIdentity(user_id=sub, email=email, auth_scheme="iap")` | Verified identity only |
| Bad signature | Token signed by another key | No identity; warning | Forgery rejected |
| Wrong audience | `aud` for another backend | No identity | Tokens are backend-scoped |
| Expired | `exp` in the past | No identity | Replay window bounded |
| Plain email header alone | `x-goog-authenticated-user-email` only | No identity | Unsigned headers never trusted |
| Key fetch failure | Key URL unreachable | No identity; warning | Fail closed |
| No audience | `IapIdentitySource("")` | `ValueError` | Fail loud at construction |
| Malformed audience | `IapIdentitySource("my-backend")` | `ValueError` | Fail loud at construction |

### 2.5 Acceptance Scenarios

- Given an app behind IAP configured with its backend-service audience, when a request carries a valid assertion, then tools see `AgentContext.current().identity.email` from the verified token.
- Given the same app, when a caller bypasses IAP and sends only `x-goog-authenticated-user-email`, then the caller is anonymous.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| IAP identity input | Signed `x-goog-iap-jwt-assertion` | The plain email header is spoofable by any client that reaches the server directly. |
| Verification failure | No identity, guards decide | Raising inside context construction turns a bad token into a transport error with no guard context. |
| JWT library | `pyjwt[crypto]` via `iap` extra | Hand-rolled ES256 verification duplicates audited code. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch12]` — identity-source configuration is documented in `ch11:t01` (`docs/usage/agent-protocols.md`) and `ch11:t03` (harness chapter).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1.3 (IAP), R1.4 | [t01](tasks/t01-iap-jwt-identity-source.md) | `behavior_tdd` | `src/py/tests/unit/agent/test_core_and_observability.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch12:t01 — Verify IAP signed-header JWTs as an identity source (tasks/t01-iap-jwt-identity-source.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q && uv run --no-sync mypy src/py/vertebra/agent/core` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/1` tasks completed.
- **Remaining Tasks**: `t01`.
- **Downstream Dependents**: `ch11` documents the IAP preset.

[^identity-module]: `src/py/vertebra/agent/core/identity.py::IdentitySource` — Identity-source protocol added by ch01:t01.
[^iap-signed-headers]: https://cloud.google.com/iap/docs/signed-headers-howto — Securing your app with signed headers (Identity-Aware Proxy).
