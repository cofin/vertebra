---
type: Task
id: "mcp-protocol-completeness:ch01:t01"
chapter_id: ch01-agent-context-propagation
title: Build trusted contexts from headers, scopes, and environs
description: Add AgentContext.from_headers, from_scope, from_wsgi_environ, fork, and reset_agent_context, add the identity-source seam with mTLS-subject and custom-callable presets, and ignore proxy identity headers unless a source is configured.
status: draft
state: completed
priority: P0
verification_strategy: regression_tdd
depends_on: []
read_set:
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/agent/core/__init__.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/tests/unit/agent/test_core_and_observability.py
files:
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/agent/core/identity.py
  - src/py/vertebra/agent/core/__init__.py
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
estimated_loc: 230
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T02:40:00Z"
last_verified_commit: ff668194
verification_evidence: "Red: missing reset/context API; Green: 31 canonical tests, Ruff check/format and Mypy core (8 files), all exit 0."
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: ff668194
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: agent-context
    resource: src/py/vertebra/agent/core/context.py::AgentContext.from_request
    title: Existing header and scope identity extraction
  - id: caller-identity
    resource: src/py/vertebra/agent/core/context.py::CallerIdentity
    title: Normalized caller identity struct
tags:
  - mcp
  - context
  - identity
---

# Task t01: Build Trusted Contexts from Headers, Scopes, and Environs

## Objective & Scope Boundary

Replace the header-trusting body of `AgentContext.from_request` with three constructors that share one identity builder, add `AgentContext.fork()` and `reset_agent_context`, and add the identity-source seam in a new public module `vertebra.agent.core.identity` with the mTLS-subject and custom-callable presets [^agent-context] [^caller-identity]. Proxy identity headers (`x-vertebra-mtls-subject`, `x-forwarded-client-cert`, `x-goog-authenticated-user-email`, `x-goog-iap-jwt-assertion`, `x-tenant-id`) never set identity unless an identity source is passed. Out of scope: the IAP preset (`ch12:t01`), `MCPConfig` fields and adapter call sites (`t03`, `t04`), and `MCPCore` binding (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/agent/core/context.py` — `CallerIdentity`, `AgentContext.from_request`, `bind_agent_context`, `_CURRENT_AGENT_CONTEXT`.
- `src/py/vertebra/agent/core/__init__.py` — Export list.
- `src/py/vertebra/utils/module_loader.py::import_optional` — Optional import helper style.
- `src/py/tests/unit/agent/test_core_and_observability.py::test_agent_context_identity_and_guard_chain_inheritance` — Existing context assertions to update.

## Symbol & Signature Contracts

- `src/py/vertebra/agent/core/identity.py` (new, `__all__` lists every public name below):
  - `class IdentityRequest(msgspec.Struct, frozen=True, kw_only=True)`: `headers: Mapping[str, str]` (lowercased names), `peer: str | None = None` (immediate peer IP address).
  - `class IdentitySource(Protocol)`: `def resolve(self, request: IdentityRequest) -> CallerIdentity | None`. `None` means no identity is established.
  - `class MtlsIdentitySource(msgspec.Struct, frozen=True, kw_only=True)`: `trusted_proxies: tuple[str, ...]`, `header: str = "x-vertebra-mtls-subject"`.
    - `__post_init__` raises `ValueError("MtlsIdentitySource requires at least one trusted proxy CIDR")` when `trusted_proxies` is empty and `ValueError(f"Invalid trusted proxy CIDR: {value!r}")` when `ipaddress.ip_network(value, strict=False)` fails.
    - `resolve` returns `CallerIdentity(client_cert_subject=<header value>, auth_scheme="mtls")` only when `request.peer` parses as an IP inside one of the networks and the header is non-empty; otherwise `None`. The header name is lowercased once at construction.
  - `class CallableIdentitySource(msgspec.Struct, frozen=True, kw_only=True)`: `resolver: Callable[[IdentityRequest], CallerIdentity | None]`; `__post_init__` raises `TypeError("CallableIdentitySource.resolver must be callable")` when `resolver` is not callable; `resolve` returns `self.resolver(request)`. Exceptions raised by the resolver propagate.
- `src/py/vertebra/agent/core/context.py`:
  - `AgentContext.from_headers(cls, headers: Mapping[str, str], *, identity_source: IdentitySource | None = None, peer: str | None = None, user: Any = None, auth: Any = None, session_id: str | None = None) -> AgentContext`:
    - Lowercases header names.
    - With `identity_source=None`, no header sets `user_id`, `email`, `client_cert_subject`, or `tenant_id`; `tenant_id` stays `"default"`.
    - With a source, `identity_source.resolve(IdentityRequest(headers=lowered, peer=peer))` supplies the base identity (tenant included); `None` keeps the anonymous base.
    - `authorization: Bearer <token>` sets `auth_scheme="jwt"` only when the scheme is still `"anonymous"`; it never sets an identity.
    - `user`: `.id` attribute, `Mapping["id"]`, or `str` becomes `user_id`; `.email` fills `email` when unset; `.roles` extends roles; scheme becomes `"connection"` when still `"anonymous"`.
    - `auth`: `.scopes` / `.roles` attributes or `Mapping` keys `"scopes"` / `"roles"` extend scopes and roles.
    - Roles and scopes are sorted, de-duplicated tuples.
  - `AgentContext.from_scope(cls, scope: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> AgentContext` decodes `scope["headers"]` (list of `(bytes, bytes)` latin-1 pairs or a mapping), takes `peer` from `scope["client"][0]` when present, and passes `scope.get("user")` / `scope.get("auth")`; never calls `getattr` on request objects.
  - `AgentContext.from_wsgi_environ(cls, environ: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> AgentContext` maps `HTTP_*` keys to lowercase dashed names, adds `CONTENT_TYPE`, takes `peer` from `REMOTE_ADDR`, and passes `environ.get("REMOTE_USER")` as `user`.
  - `AgentContext.from_request(...)` keeps its progress parameters, adds `identity_source`, and delegates to `from_scope` when `request_or_connection.scope` is a `Mapping`, otherwise to `from_headers` over the object's header mapping or header pair list.
  - `AgentContext.fork(self) -> AgentContext` returns a new context with the same `identity`, `session_id`, `turn_id`, `progress_reporter`, and `trace_context`, a fresh `CancellationToken`, and `dict(self.metadata)`.
  - `reset_agent_context(token: contextvars.Token[AgentContext | None]) -> None` calls `_CURRENT_AGENT_CONTEXT.reset(token)`.
- `src/py/vertebra/agent/core/__init__.py`: export `reset_agent_context`. Identity sources are imported from `vertebra.agent.core.identity`.

## Edge Cases & Error Semantics

- Proxy identity headers without a source are ignored; no exception and no log.
- mTLS header from a peer outside `trusted_proxies`, from an unparsable peer, or with `peer=None` is ignored (`resolve` returns `None`).
- `x-tenant-id` never sets `tenant_id`; tenants come only from a source's `CallerIdentity`.
- `x-goog-authenticated-user-email` never sets identity under any source in this module.
- `scope["headers"]` absent yields an empty header map; `scope["client"]` absent yields `peer=None`.
- A Starlette `Request` without `AuthenticationMiddleware` never has `.user` evaluated.
- `fork()` never shares the `CancellationToken` or the `metadata` dict.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_core_and_observability.py` (in-process; extend `test_agent_context_identity_and_guard_chain_inheritance` and add parametrized `test_agent_context_identity_sources` and `test_identity_source_config_errors`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Headers ignored by default | parametrize headers `{"x-goog-authenticated-user-email": "accounts.google.com:ana@example.com"}`, `{"x-vertebra-mtls-subject": "CN=svc"}`, `{"x-forwarded-client-cert": "By=spiffe://a"}`, `{"x-tenant-id": "acme"}`; `from_headers(h)` | `email is None`, `client_cert_subject is None`, `tenant_id == "default"`, `auth_scheme == "anonymous"`, `is_authenticated is False` |
| Plain IAP email with mTLS source | `from_headers({"x-goog-authenticated-user-email": "accounts.google.com:ana@example.com"}, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)), peer="10.0.0.5")` | `email is None`, `auth_scheme == "anonymous"` |
| mTLS trusted CIDR | `from_headers({"x-vertebra-mtls-subject": "CN=svc"}, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)), peer="10.0.0.5")` | `client_cert_subject == "CN=svc"`, `auth_scheme == "mtls"` |
| mTLS untrusted CIDR | same, `peer="203.0.113.9"` | `client_cert_subject is None`, `auth_scheme == "anonymous"` |
| mTLS no peer | same, `peer=None` | `client_cert_subject is None` |
| mTLS custom header | `MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",), header="X-Forwarded-Client-Cert")`, header `{"x-forwarded-client-cert": "By=spiffe://a"}`, `peer="10.1.2.3"` | `client_cert_subject == "By=spiffe://a"` |
| mTLS from scope client | `from_scope({"type": "http", "headers": [(b"x-vertebra-mtls-subject", b"CN=svc")], "client": ("10.0.0.5", 5000)}, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)))` | `client_cert_subject == "CN=svc"` |
| mTLS from WSGI REMOTE_ADDR | `from_wsgi_environ({"HTTP_X_VERTEBRA_MTLS_SUBJECT": "CN=svc", "REMOTE_ADDR": "198.51.100.7"}, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)))` | `client_cert_subject is None` |
| Custom callable | `CallableIdentitySource(resolver=lambda r: CallerIdentity(user_id=r.headers["x-user"], tenant_id="acme"))`; `from_headers({"X-User": "u1"}, identity_source=src)` | `owner_key() == "acme:u1"` |
| Custom callable returns None | `resolver=lambda r: None` | `auth_scheme == "anonymous"` |
| Config error: empty CIDRs | `MtlsIdentitySource(trusted_proxies=())` | `ValueError` matching `"at least one trusted proxy CIDR"` |
| Config error: bad CIDR | `MtlsIdentitySource(trusted_proxies=("not-a-cidr",))` | `ValueError` matching `"Invalid trusted proxy CIDR"` |
| Config error: resolver not callable | `CallableIdentitySource(resolver="x")` | `TypeError` |
| Bearer marker | `from_headers({"Authorization": "Bearer abc"})` | `auth_scheme == "jwt"`, `user_id is None` |
| Scope user and auth | `from_scope({"type": "http", "headers": [(b"x-tenant-id", b"acme")], "user": SimpleNamespace(id=7, email="b@x.io", roles=["admin"]), "auth": SimpleNamespace(scopes=["read"])})` | `user_id == "7"`, `roles == ("admin",)`, `scopes == ("read",)`, `auth_scheme == "connection"`, `tenant_id == "default"` |
| Starlette without auth | `from_request(starlette.requests.Request({"type": "http", "method": "POST", "path": "/mcp", "headers": []}))` | No exception; `auth_scheme == "anonymous"` |
| WSGI REMOTE_USER | `from_wsgi_environ({"REMOTE_USER": "carol", "HTTP_X_TENANT_ID": "t9"})` | `owner_key() == "default:carol"` |
| Fork | `ctx = AgentContext(metadata={"a": 1}); child = ctx.fork()` | `child.identity is ctx.identity`, `child.cancellation_token is not ctx.cancellation_token`, `child.metadata == {"a": 1}`, `child.metadata is not ctx.metadata` |
| Reset | `token = bind_agent_context(AgentContext()); reset_agent_context(token)` | `current_agent_context() is None` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q -k "agent_context or identity_source"` fails on the default-ignore, mTLS, callable, Starlette, and fork cases.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q && uv run --no-sync ruff check src/py/vertebra/agent/core && uv run --no-sync mypy src/py/vertebra/agent/core`.

## Acceptance Criteria

- [x] R1.1: proxy identity headers never set identity without an identity source; the mTLS preset honors its header only from trusted proxy CIDRs; the callable preset supplies identity verbatim.
- [x] R1.2: Starlette requests without authentication middleware build contexts.
- [x] R1.3: invalid preset construction (empty or invalid CIDRs, non-callable resolver) raises at construction.
- [x] `fork` and `reset_agent_context` behave per the matrix.
- [x] No inline `#` comments; module-level imports only.

## Notes & Discoveries

- Proxy identity requires an explicit source; framework authentication properties are never evaluated. Forks copy metadata and own independent cancellation tokens.

[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext.from_request` — Existing header and scope identity extraction.
[^caller-identity]: `src/py/vertebra/agent/core/context.py::CallerIdentity` — Normalized caller identity struct.
