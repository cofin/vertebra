---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch01-agent-context-propagation
title: "Chapter 1: Agent Context Propagation"
description: Build trusted AgentContext values from headers, ASGI scopes, and WSGI environs, bind them for every MCPCore dispatch and promoted task, and pass them from every transport adapter.
status: draft
state: active
planning_status: complete
depends_on_chapters: []
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
  - id: agent-context
    resource: src/py/vertebra/agent/core/context.py::AgentContext
    title: AgentContext, CallerIdentity, and context variable helpers
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: MCP core dispatch and task promotion
  - id: task-store
    resource: src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore
    title: In-memory MCP task records
  - id: mcp-routes
    resource: src/py/vertebra/agent/mcp/routes.py::WsgiMcpApp
    title: ASGI and WSGI adapters over MCPCore
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native
    title: Native and Litestar MCP endpoint mounting
  - id: stdio
    resource: src/py/vertebra/agent/mcp/stdio.py::run_stdio_server
    title: Stdio transport runner
tags:
  - mcp
  - context
  - identity
  - tasks
---

# Chapter 1: Agent Context Propagation

<!-- truth: start -->
- **Trusted construction**: `AgentContext.from_headers`, `from_scope`, and `from_wsgi_environ` read `scope["user"]`, `scope["auth"]`, and `REMOTE_USER` always; proxy identity headers set identity only through an explicit `IdentitySource` (`MtlsIdentitySource` accepted only from trusted proxy CIDRs, `CallableIdentitySource`, or the IAP preset from `ch12`); `MCPConfig.trust_proxy_identity` defaults to `False` and fails at construction without `identity_source` [^agent-context].
- **Core binding**: `MCPCore.handle` and `MCPCore.handle_payload` bind the request context (or a fresh anonymous `AgentContext`) with `bind_agent_context` and reset it with `reset_agent_context` in `finally` [^mcp-core].
- **Task isolation**: promoted `tools/call` tasks run under `AgentContext.fork()` with a fresh `CancellationToken`; `tasks/cancel` cancels the token and the asyncio task; `MCPTaskStore.complete` and `fail` leave terminal records unchanged; anonymous callers cannot access owner-bound tasks [^task-store].
- **Adapters**: `AsgiMcpApp`, `WsgiMcpApp`, native `AgentPlugin` endpoints, the Litestar fallback endpoint, and `run_stdio_server` pass a context into `MCPCore`; the Litestar fallback returns `litestar.Response` with the core's status and headers [^mcp-routes] [^agent-plugin] [^stdio].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

MCP dispatch requires authenticated application identity to reach guards, tools, confirmation ownership, and promoted tasks. `AgentContext` derives that identity from application scope state or an explicit proxy policy, and `MCPCore` binds it throughout dispatch. Promoted tasks own independent cancellation tokens; cancelling a task stops its coroutine and preserves its terminal record. Anonymous task requests cannot access owner-bound records.

Every transport adapter passes context. Native and Litestar mounts retain core HTTP semantics, and stdio frames share one session context. IAP JWT verification remains assigned to `ch12`.

### 1.2 Upstream Execution Baseline

- Branch base `main @ c8722c76`; no upstream chapter.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam & Dependency Category |
| --- | --- | --- |
| Context | `src/py/vertebra/agent/core/context.py::AgentContext.from_request`, `bind_agent_context` [^agent-context] | In-process; trusted constructors are the seam. |
| Core | `src/py/vertebra/agent/mcp/service.py::MCPCore.handle`, `handle_payload`, `_handle_tools_call`, `_handle_tasks_cancel` [^mcp-core] | In-process. |
| Tasks | `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore.complete`, `fail`, `cancel` [^task-store] | In-process. |
| Adapters | `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp.__call__`, `handle_starlette_request`, `WsgiMcpApp.handle_vertebra_request`, `handle_flask_view`, `__call__` [^mcp-routes] | External framework objects at the boundary. |
| Mounts | `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native`, `_mount_litestar_routes` [^agent-plugin] | Native DI and Litestar handlers. |
| Stdio | `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` [^stdio] | Pipe I/O. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [`python-framework`](../../../../../skills/python-framework/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1.1**: Header, scope, and environ constructors ignore proxy identity headers unless an explicit `IdentitySource` is configured; the mTLS preset honors its header only from trusted proxy CIDRs; the callable preset supplies application-defined identity.
- **R1.2**: Starlette requests without authentication middleware build contexts without raising.
- **R1.3**: Identity misconfiguration fails at construction: `trust_proxy_identity=True` without `identity_source`, `identity_source` without `trust_proxy_identity=True`, the mTLS preset without valid CIDRs, a non-callable resolver, and the IAP preset without a valid audience (`ch12`).
- **R1.4**: IAP identity comes only from a verified `x-goog-iap-jwt-assertion` (`ch12`).
- **R2.1**: Every `MCPCore.handle` / `handle_payload` call binds a context and restores the previous value.
- **R2.2**: Promoted tasks run under a forked context; `tasks/cancel` stops execution; terminal records stay terminal.
- **R3.1**: ASGI, Starlette/FastAPI, WSGI, Flask, and Django adapters pass contexts derived from their request objects.
- **R3.2**: Native and Litestar fallback endpoints pass contexts; the Litestar fallback preserves status and headers; stdio passes one session context.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# vertebra.agent.core.identity
class IdentityRequest(msgspec.Struct, frozen=True, kw_only=True):
    headers: Mapping[str, str]
    peer: str | None = None

class IdentitySource(Protocol):
    def resolve(self, request: IdentityRequest) -> CallerIdentity | None: ...

class MtlsIdentitySource(msgspec.Struct, frozen=True, kw_only=True):
    trusted_proxies: tuple[str, ...]
    header: str = "x-vertebra-mtls-subject"

class CallableIdentitySource(msgspec.Struct, frozen=True, kw_only=True):
    resolver: Callable[[IdentityRequest], CallerIdentity | None]

# vertebra.agent.core.context

class AgentContext:
    @classmethod
    def from_headers(cls, headers: Mapping[str, str], *, identity_source: IdentitySource | None = None, peer: str | None = None, user: Any = None, auth: Any = None, session_id: str | None = None) -> "AgentContext": ...
    @classmethod
    def from_scope(cls, scope: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> "AgentContext": ...
    @classmethod
    def from_wsgi_environ(cls, environ: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> "AgentContext": ...
    @classmethod
    def from_request(cls, request_or_connection: Any, *, identity_source: IdentitySource | None = None, session_id: str | None = None, turn_id: str | None = None, progress_token: str | int | None = None, progress_callback: ProgressCallback | None = None) -> "AgentContext": ...
    def fork(self) -> "AgentContext": ...

def reset_agent_context(token: contextvars.Token[AgentContext | None]) -> None: ...

class MCPConfig:
    trust_proxy_identity: bool = False
    identity_source: IdentitySource | None = None

async def run_stdio_server(service: MCPService, *, stdin: AsyncIterable[bytes] | None = None, stdout: Callable[[bytes], Awaitable[None]] | None = None, max_message_bytes: int = 4_194_304, context: AgentContext | None = None) -> int: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `AsgiMcpApp.__call__` -> `AgentContext.from_headers(headers, user=scope.get("user"), auth=scope.get("auth"))` -> `MCPCore.handle` -> `bind_agent_context`.
- `AsgiMcpApp.handle_starlette_request` -> `AgentContext.from_scope(request.scope)` -> `MCPCore.handle`.
- `WsgiMcpApp.__call__` / `handle_flask_view` -> `AgentContext.from_wsgi_environ(environ)`; `handle_vertebra_request` -> `AgentContext.from_headers(request.headers.as_dict())` -> `WsgiMcpApp.handle_http`.
- `AgentPlugin.mount_native` endpoints -> `AgentContext.from_headers` -> `MCPCore.handle`; Litestar fallback -> `AgentContext.from_scope(request.scope)` -> `litestar.Response`.
- `MCPCore._handle_tools_call` (task path) -> `AgentContext.fork()` -> background coroutine; `_handle_tasks_cancel` -> `CancellationToken.cancel` + `asyncio.Task.cancel`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Proxy headers by default | IAP email, mTLS subject, XFCC, `x-tenant-id`; default config | Ignored | Identity from explicit sources only |
| mTLS from trusted proxy | `MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",))`, peer `10.0.0.5` | `client_cert_subject` set | Header trusted only from known proxies |
| mTLS from other peer | Same source, peer `203.0.113.9` | Ignored | Direct clients cannot assert a subject |
| Custom callable | `CallableIdentitySource(resolver=...)` | Resolver identity used | Application owns the policy |
| Opt-in without source | `MCPConfig(trust_proxy_identity=True)` | `ValueError` | Fail loud at construction |
| Source without opt-in | `MCPConfig(identity_source=src)` | `ValueError` | Opt-in is explicit |
| mTLS without CIDRs | `MtlsIdentitySource(trusted_proxies=())` | `ValueError` | Fail loud at construction |
| Tenant header | `x-tenant-id: acme` | `tenant_id == "default"` unless a source sets it | Owner keys cannot be steered |
| Starlette without auth | `scope` lacks `user` | Anonymous context, no exception | Adapters never crash |
| Nested binding | `handle` called inside a bound context | Previous context restored after return | Context variable hygiene |
| Cancel running task | `tasks/cancel` | Token cancelled, coroutine cancelled, status `cancelled` | Terminal immutability |
| Flask absent | `handle_flask_view()` without Flask | `MissingDependencyError` | No fabricated request body |

### 2.5 Acceptance Scenarios

- Given an ASGI scope whose `user` has `id="ana"`, when `tools/call` runs a tool that reads `AgentContext.current()`, then the tool returns `"ana"`.
- Given a promoted task, when the owner calls `tasks/cancel`, then the coroutine observes cancellation and the record stays `cancelled`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Header trust | Off by default; `trust_proxy_identity=True` plus a named `IdentitySource` | Trusting headers by default lets any client assert identity; a header allow-list cannot verify where a header came from. |
| IAP identity | Verified `x-goog-iap-jwt-assertion` (`ch12`) | The plain `x-goog-authenticated-user-email` header is spoofable by clients that bypass IAP. |
| Binding location | Inside `MCPCore` | Binding in each adapter duplicates `try/finally` across seven call sites. |
| Task context | `fork()` with fresh token | Sharing the request token lets request teardown cancel tasks. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[3/3 files documented in ch01]`.
- `docs/usage/agent-protocols.md` documents trusted context configuration and task ownership/cancellation.
- `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` records constructor, dispatch, task, and adapter contracts.
- `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` separates shipped context behavior from remaining protocol gaps; `ch11` completes coverage for later chapters.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1.1, R1.2, R1.3 (presets) | [t01](tasks/t01-trusted-context-construction.md) | `regression_tdd` | `src/py/tests/unit/agent/test_core_and_observability.py` |
| R1.3 (`MCPConfig`) | [t03](tasks/t03-transport-adapter-contexts.md) | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R1.4 | [ch12:t01](../ch12-iap-identity-source/tasks/t01-iap-jwt-identity-source.md) | `behavior_tdd` | `test_core_and_observability.py` |
| R2.1, R2.2 | [t02](tasks/t02-core-context-binding-and-task-cancellation.md) | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R3.1 | [t03](tasks/t03-transport-adapter-contexts.md) | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R3.2 | [t04](tasks/t04-plugin-litestar-and-stdio-contexts.md) | `regression_tdd` | `test_mcp_conformance.py`, `src/py/tests/unit/cli/test_mcp_cli.py` |

## 5. Implementation Plan

- [x] [ff668194] mcp-protocol-completeness:ch01:t01 — Build trusted contexts from headers, scopes, and environs (tasks/t01-trusted-context-construction.md)
- [x] [7e4129c4] mcp-protocol-completeness:ch01:t02 — Bind contexts in MCPCore and isolate promoted tasks (tasks/t02-core-context-binding-and-task-cancellation.md)
- [x] [5fccef87] mcp-protocol-completeness:ch01:t03 — Pass contexts from ASGI and WSGI adapters (tasks/t03-transport-adapter-contexts.md)
- [x] [c007f853] mcp-protocol-completeness:ch01:t04 — Pass contexts from native, Litestar fallback, and stdio entry points (tasks/t04-plugin-litestar-and-stdio-contexts.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/cli/test_mcp_cli.py -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Implementation**: `c007f853`; `4/4` tasks completed with committed verification evidence.
- **Current Contracts**: trusted context constructors; explicit mTLS/callable policies; binding restoration; independent task cancellation; anonymous access refusal for owner-bound tasks; context forwarding through every adapter.
- **Recovery Checkpoint**: the canonical core, MCP conformance, and CLI suite passes `86` tests; focused Ruff and Mypy gates pass.
- **Remaining Chapter Work**: present-tense documentation synchronization and fresh chapter quality review; `last_reviewed_commit` remains null until review closes.
- **Downstream Dependents**: `ch02` addresses duplicate guard execution and confirmation semantics; `ch12` adds verified IAP identity on the source seam.

[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext` — AgentContext, CallerIdentity, and context variable helpers.
[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — MCP core dispatch and task promotion.
[^task-store]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — In-memory MCP task records.
[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::WsgiMcpApp` — ASGI and WSGI adapters over MCPCore.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Native and Litestar MCP endpoint mounting.
[^stdio]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Stdio transport runner.
