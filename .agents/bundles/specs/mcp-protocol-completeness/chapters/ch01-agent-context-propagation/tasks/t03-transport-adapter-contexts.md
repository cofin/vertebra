---
type: Task
id: "mcp-protocol-completeness:ch01:t03"
chapter_id: ch01-agent-context-propagation
title: Pass contexts from ASGI and WSGI adapters
description: Build AgentContext values in AsgiMcpApp and WsgiMcpApp from ASGI scopes, Starlette requests, native Requests, Flask requests, and WSGI environs, honoring the opt-in MCPConfig.trust_proxy_identity / identity_source pair (validated at construction), and remove the fabricated Flask fallback body.
status: draft
state: completed
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "mcp-protocol-completeness:ch01:t02"
read_set:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/mcp/models.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 190
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T03:46:37Z"
last_verified_commit: 5fccef87
verification_evidence: "Red: 13 adapter context/config regressions failed; Green: 42 canonical conformance tests, Ruff routes/models/test and Mypy routes/models, all exit 0."
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: 5fccef87
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: mcp-routes
    resource: src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp
    title: Shared ASGI and WSGI adapters
  - id: agent-context
    resource: src/py/vertebra/agent/core/context.py::AgentContext.from_scope
    title: Trusted context constructors from t01
tags:
  - mcp
  - context
  - adapters
---

# Task t03: Pass Contexts from ASGI and WSGI Adapters

## Objective & Scope Boundary

Every request entering `AsgiMcpApp` or `WsgiMcpApp` carries an `AgentContext` built with `self.config.identity_source` (set only when `MCPConfig.trust_proxy_identity` is `True`); this task also adds the `MCPConfig` identity fields and their construction-time validation [^mcp-routes] [^agent-context]. Out of scope: streaming responses (`ch04:t02`), native and Litestar mounts (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/routes.py` — `AsgiMcpApp.__call__`, `handle_starlette_request`, `_extract_scope_headers`, `WsgiMcpApp.handle_http`, `handle_vertebra_request`, `handle_flask_view`, `__call__`, `_extract_wsgi_headers`.
- `src/py/vertebra/agent/core/context.py` — `from_headers`, `from_scope`, `from_wsgi_environ`.
- `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — Starlette endpoint wiring.
- `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — Flask view wiring.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_http_handler_conformance_and_negative_cases` — Adapter test patterns.

## Symbol & Signature Contracts

- `AsgiMcpApp.__call__`: `context = AgentContext.from_headers(headers, identity_source=self.config.identity_source, peer=self._scope_client_host(scope), user=self._scope_value(scope, "user"), auth=self._scope_value(scope, "auth"))`; passes `context=context` in `McpHttpRequest`.
- `AsgiMcpApp._scope_value(scope: Any, key: str) -> Any` (private static, below public methods): returns `scope.get(key)` when `scope` has `get`, else `None`.
- `AsgiMcpApp._scope_client_host(scope: Any) -> str | None` (private static): returns `scope["client"][0]` when present, else `None`.
- `src/py/vertebra/agent/mcp/models.py::MCPConfig` gains `trust_proxy_identity: bool = False` (docstring: proxy identity headers are ignored unless this is `True` and `identity_source` names a trusted source) and `identity_source: IdentitySource | None = None` (docstring: `IapIdentitySource`, `MtlsIdentitySource`, or `CallableIdentitySource` from `vertebra.agent.core.identity`). `MCPConfig.__post_init__` raises `ValueError("MCPConfig.trust_proxy_identity=True requires identity_source (IapIdentitySource, MtlsIdentitySource, or CallableIdentitySource)")` when the flag is set without a source and `ValueError("MCPConfig.identity_source requires trust_proxy_identity=True")` when a source is set without the flag. `IdentitySource` is imported under `TYPE_CHECKING`.
- `AsgiMcpApp.handle_starlette_request`: `AgentContext.from_scope(request.scope, identity_source=self.config.identity_source)` when `request.scope` is a `Mapping`, else `AgentContext.from_headers(headers, identity_source=self.config.identity_source)`.
- `WsgiMcpApp.handle_vertebra_request`: native `Request` -> `AgentContext.from_headers(request.headers.as_dict(), identity_source=self.config.identity_source)`; raw `bytes` -> `AgentContext()`; passes `context` to `handle_http`.
- `WsgiMcpApp.handle_flask_view`: `flask.request` -> `AgentContext.from_wsgi_environ(fl_req.environ, identity_source=self.config.identity_source)`; duck-typed request objects -> `from_headers`; when `request is None` and Flask is not importable, raises `MissingDependencyError("flask", install_package="flask")` instead of dispatching a fabricated `b"{}"` body.
- `WsgiMcpApp.__call__`: mapping environs -> `AgentContext.from_wsgi_environ(environ, identity_source=self.config.identity_source)`.

## Edge Cases & Error Semantics

- ASGI scopes without `user` produce anonymous contexts.
- Header values decode as latin-1, matching `_extract_scope_headers`.
- Flask missing with an explicit native `Request` argument still works (no Flask import needed).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (external framework objects constructed directly; new parametrized `test_mcp_adapters_propagate_agent_context`).
- Shared fixture: `MCPService` with tool `whoami(context: AgentContext) -> str` returning `f"{context.identity.user_id}|{context.identity.email}"` and a guarded tool `admin_only` with `guards=(RequireRoles("admin"),)` recording calls in a list.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| ASGI scope user | `await AsgiMcpApp(core)(scope={"type": "http", "method": "POST", "path": "/mcp", "headers": [(b"content-type", b"application/json")], "user": SimpleNamespace(id="ana", roles=["admin"])}, receive=<one body event: tools/call whoami>, send=<collector>)` | Collected body result text `"ana|None"` |
| ASGI guard allows | Same scope, `tools/call admin_only` | Result without `error`; calls list `== ["ran"]` |
| ASGI anonymous guard | Scope without `user`, `tools/call admin_only` | Payload contains `error`; calls list stays `[]` |
| Starlette request | `await app.handle_starlette_request(starlette.requests.Request(scope_with_user_bob, receive))` | Result text `"bob|None"` |
| WSGI environ | `WsgiMcpApp(core)({"REQUEST_METHOD": "POST", "PATH_INFO": "/mcp", "REMOTE_USER": "carol", "CONTENT_TYPE": "application/json", "wsgi.input": io.BytesIO(<tools/call whoami>)}, start_response)` | Body result text `"carol|None"` |
| Headers ignored by default | Default `MCPConfig()`; ASGI headers `(b"x-goog-authenticated-user-email", b"accounts.google.com:dee@example.com")`, `(b"x-vertebra-mtls-subject", b"CN=svc")`, scope `"client": ("10.0.0.5", 1)` | Result text `"None|None"` |
| mTLS trusted CIDR via config | `MCPConfig(trust_proxy_identity=True, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)))`; tool returning `context.identity.client_cert_subject`; scope `"client": ("10.0.0.5", 1)`, header `(b"x-vertebra-mtls-subject", b"CN=svc")` | Result text `"CN=svc"` |
| mTLS untrusted CIDR via config | Same config, scope `"client": ("203.0.113.9", 1)` | Result text `"None"` |
| mTLS via WSGI REMOTE_ADDR | Same config; environ `"REMOTE_ADDR": "10.0.0.5"`, `"HTTP_X_VERTEBRA_MTLS_SUBJECT": "CN=svc"` | Result text `"CN=svc"` |
| Custom callable via config | `MCPConfig(trust_proxy_identity=True, identity_source=CallableIdentitySource(resolver=lambda r: CallerIdentity(user_id=r.headers.get("x-user"))))`; header `(b"x-user", b"erin")` | Result text `"erin|None"` |
| Config error: opt-in without source | `MCPConfig(trust_proxy_identity=True)` | `ValueError` matching `"requires identity_source"` |
| Config error: source without opt-in | `MCPConfig(identity_source=CallableIdentitySource(resolver=lambda r: None))` | `ValueError` matching `"requires trust_proxy_identity=True"` |
| Config error: IAP without audience | `MCPConfig(trust_proxy_identity=True, identity_source=IapIdentitySource(""))` | `ValueError` from `IapIdentitySource` (runs after `ch12:t01`; marked `skipif` on `ImportError` until then) |
| Flask missing | `monkeypatch.setattr(routes_mod, "import_optional", lambda name: None)`; `WsgiMcpApp(core).handle_flask_view()` | Raises `MissingDependencyError` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "propagate_agent_context"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/routes.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/routes.py`.

## Acceptance Criteria

- [x] R3.1: ASGI, Starlette/FastAPI, WSGI, Flask, and Django (via `handle_vertebra_request`) requests carry contexts.
- [x] R1.3: `MCPConfig` rejects `trust_proxy_identity=True` without `identity_source` and a source without the flag; the configured source and peer address flow into every adapter.
- [x] The fabricated Flask fallback body is gone.

## Notes & Discoveries

- [2026-11-12 00:30] Guard denial surfaces as `-32603` until `ch02:t01`; this task asserts only that the guarded tool does not run.

[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp` — Shared ASGI and WSGI adapters.
[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext.from_scope` — Trusted context constructors from t01.
