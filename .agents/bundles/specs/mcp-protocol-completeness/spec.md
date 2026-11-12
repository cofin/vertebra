---
type: Spec
flow_id: mcp-protocol-completeness
title: MCP Protocol Completeness
description: Trusted AgentContext propagation, single-pass guards and single-use confirmation tickets, dual-mode MCP version negotiation with legacy sessions and GET SSE streams, resource subscriptions, the skills extension, MCP Apps UI metadata, Litestar delegation parity, and the `vtb ai mcp serve` / `bridge` commands with OIDC identity.
status: draft
state: active
has_chapters: true
chapters:
  - ch01-agent-context-propagation
  - ch02-guard-and-confirmation-correctness
  - ch03-version-negotiation-and-sessions
  - ch04-get-sse-streams
  - ch05-resource-subscriptions
  - ch06-skills-extension
  - ch07-mcp-apps-ui-metadata
  - ch08-litestar-delegation-parity
  - ch09-stdio-and-serve
  - ch10-bridge-dual-mode-and-oidc
  - ch12-iap-identity-source
  - ch11-docs-and-okf
current_chapter: ch01-agent-context-propagation
current_task: "mcp-protocol-completeness:ch01:t04"
parent_prd: integration-remediation
depends_on_flows:
  - server-runtime-correctness
  - gcp-embeddings-and-identity
research: []
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_chapter: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore
    title: Transport-agnostic MCP JSON-RPC and HTTP core
  - id: mcp-routes
    resource: src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp
    title: Shared ASGI and WSGI MCP adapters
  - id: mcp-models
    resource: src/py/vertebra/agent/mcp/models.py::MCPConfig
    title: MCP configuration, envelopes, and resource models
  - id: agent-context
    resource: src/py/vertebra/agent/core/context.py::AgentContext
    title: Caller identity, cancellation, and context variable binding
  - id: guards
    resource: src/py/vertebra/agent/core/guards.py::authorize
    title: Guard chain execution and authorization entry point
  - id: confirmation-gate
    resource: src/py/vertebra/agent/runtime.py::ConfirmationGate
    title: Human confirmation ticket store
  - id: tool-invoke
    resource: src/py/vertebra/agent/core/schema.py::Tool
    title: Tool descriptor invocation, guard execution, and argument binding
  - id: sse-broker
    resource: src/py/vertebra/agent/core/sse.py::SseStreamBroker
    title: SSE replay buffers and per-session subscriber channels
  - id: skills
    resource: src/py/vertebra/agent/mcp/skills.py::SkillCatalog
    title: Skill catalog and filesystem skill source
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin
    title: Native App and Litestar agent harness mounting
  - id: cli-ai
    resource: src/py/vertebra/cli/ai.py::serve
    title: vtb ai mcp serve and bridge commands
  - id: mcp-bridge
    resource: src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge
    title: Stdio-to-HTTP MCP bridge
  - id: litestar-mcp-plugin
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: LitestarMCP factory and well-known document merge
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agent harness OKF chapter
tags:
  - mcp
  - agent
  - protocol
  - sessions
  - sse
  - skills
  - cli
---

# Flow: MCP Protocol Completeness

<!-- truth: start -->
- **Canonical seam**: `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` stays the single transport-agnostic engine for native `App`, Starlette, FastAPI, Flask, Django, stdio, and the Litestar fallback; Litestar apps with `LitestarMCP` delegate to `litestar-mcp>=0.20.0` [^mcp-core] [^litestar-mcp-plugin].
- **Context first**: every adapter builds an `AgentContext` (`from_scope`, `from_headers`, `from_wsgi_environ`) and `MCPCore` binds it with `bind_agent_context` for the whole dispatch; proxy identity headers are ignored by default and set identity only when `MCPConfig.trust_proxy_identity=True` names an `identity_source` (verified IAP JWT, mTLS subject from trusted proxy CIDRs, or a custom callable) [^agent-context] [^mcp-routes].
- **Authorization**: guards run once per `tools/call` through `Tool.authorize` before confirmation and task promotion; denial maps to JSON-RPC `-32004`, rate or budget exhaustion to `-32005` [^guards] [^tool-invoke].
- **Confirmation**: tickets are decided only while `pending`, owner-bound tickets require the exact owner, approvals are consumed once through `ConfirmationGate.consume_call_approval`, and tickets expire after `ttl_seconds` (default `600`) [^confirmation-gate].
- **Dual-mode transport**: `SUPPORTED_PROTOCOL_VERSIONS = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26")`; `2026-07-28` requests stay stateless, legacy requests use `initialize`, `Mcp-Session-Id` sessions in `MCPSessionStore`, `GET` SSE streams, `DELETE` termination, `resources/subscribe`, and `logging/setLevel` [^mcp-core] [^sse-broker].
- **Extensions**: skills advertise `capabilities.extensions["io.modelcontextprotocol/skills"]`; MCP Apps (SEP-1865) advertise `capabilities.extensions["io.modelcontextprotocol/ui"]` with `ui://` resources and tool `_meta.ui` links validated at mount [^skills] [^mcp-models].
- **CLI**: `vtb ai mcp serve` serves the discovered application's MCP surface over incremental stdio; `vtb ai mcp bridge` forwards both protocol modes, relays legacy `GET` SSE notifications, and mints OIDC id_tokens for `--audience` through the `gcp-embeddings-and-identity` minting API [^cli-ai] [^mcp-bridge].
- **Dependencies**: `ch04` waits for incremental ASGI streaming from `server-runtime-correctness` ([#81](https://github.com/cofin/vertebra/issues/81)); `ch10:t02` waits for id_token minting from `gcp-embeddings-and-identity` ([#113](https://github.com/cofin/vertebra/issues/113)); `ch08` waits for the `litestar-mcp` `0.20.0` release.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Vertebra's MCP server implements only the stateless `2026-07-28` Streamable HTTP profile. Clients that speak `2025-11-25`, `2025-06-18`, or `2025-03-26` cannot connect: `initialize`, `ping`, `resources/subscribe`, and `logging/setLevel` return `-32601`, `GET /mcp` returns `405`, and any other `MCP-Protocol-Version` value returns `-32022` without the `data.supported` list clients need to retry ([#109](https://github.com/cofin/vertebra/issues/109)). `MCPCore.notify_resource_updated` reads a `_resource_subscriptions` map that nothing ever populates ([#112](https://github.com/cofin/vertebra/issues/112)). The skill catalog is never wired by `AgentPlugin` or the framework mounts and advertises a non-standard `capabilities.skills` key ([#111](https://github.com/cofin/vertebra/issues/111)). Resources and tools carry no `title`, `icons`, or `_meta`, so MCP Apps UI resources cannot be described ([#110](https://github.com/cofin/vertebra/issues/110)). `vtb ai mcp serve` starts an empty `MCPService`, reads all of stdin before answering, and ignores `--name`; `vtb ai mcp bridge` only forwards `2026-07-28` POSTs and accepts only a static bearer token ([#114](https://github.com/cofin/vertebra/issues/114)).

Correctness defects sit underneath these gaps. No adapter passes an `AgentContext` to `MCPCore`, and nothing calls `bind_agent_context`, so guards see an anonymous caller and `AgentContext.current()` is `None` inside tools. `MCPCore._handle_tools_call` runs `authorize` and then `Tool.invoke` runs the same guards again with a raw `None` context, so token-bucket guards consume twice and guard denials surface as `-32603 Internal error`. `ConfirmationGate.approve` lets a caller with `owner_id=None` decide an owner-bound ticket, re-approves rejected tickets, never expires tickets, and `is_call_approved` lets one approval authorize unlimited replays. `tasks/cancel` marks a task cancelled while the background coroutine keeps running and can later overwrite the status with `completed`.

This flow fixes the correctness layer first, then completes the protocol surface in dependency order: version negotiation and sessions, `GET` SSE, resource subscriptions, skills, MCP Apps metadata, Litestar delegation parity, the CLI commands, and documentation.

### 1.2 Non-Goals

- TypeScript `McpClient` legacy-session support in `src/ts/vertebra-client/src/agent/index.ts`; the TypeScript client keeps the stateless `2026-07-28` profile (recorded as an open decision for the coordinator).
- Changes to the unwired Rust `src/rs/vertebra-http/src/mcp_bridge.rs` library (no production caller; recorded as an open decision).
- Persistent or cross-process session stores; `MCPSessionStore` is per-process in-memory and the limitation is recorded in the limitations chapter.
- Re-implementing `litestar-mcp` behaviour inside Vertebra for Litestar apps that mount `LitestarMCP`.
- Implementing OIDC id_token minting itself (owned by `gcp-embeddings-and-identity`).

### 1.3 Upstream Execution Baseline

- Branch base: `main @ c8722c76` (`feat/cleanup-2`).
- Parent PRD: [`integration-remediation`](../integration-remediation/spec.md).
- Upstream flows: `server-runtime-correctness` (incremental ASGI streaming, [#81](https://github.com/cofin/vertebra/issues/81)) gates `ch04`; `gcp-embeddings-and-identity` (id_token minting, [#113](https://github.com/cofin/vertebra/issues/113)) gates `ch10:t02`.
- External release gate: `litestar-mcp` `0.20.0` on PyPI gates `ch08` and the Litestar branch of `ch09:t02` (`uv.lock` pins `0.19.0` today).

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Verified consequence |
| --- | --- | --- |
| Core engine | `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` [^mcp-core] | Rejects non-`POST` with `405`, accepts only `MCP_PROTOCOL_VERSION`, echoes any `Mcp-Session-Id`, never binds context. |
| Subscriptions | `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated` [^mcp-core] | Reads `_resource_subscriptions`, which no handler writes. |
| Tool calls | `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call` [^mcp-core] | Calls `authorize` and then `Tool.invoke`, which re-runs `self.guards` with `target(context)`. |
| Tool invoke | `src/py/vertebra/agent/core/schema.py::Tool.invoke` [^tool-invoke] | Guard loop passes `context` unchanged (may be `None`). |
| Guards | `src/py/vertebra/agent/core/guards.py::GuardChain.execute` [^guards] | Two-parameter guards are called with one argument when `connection` is `None`. |
| Confirmation | `src/py/vertebra/agent/runtime.py::ConfirmationGate` [^confirmation-gate] | `approve` skips the owner check for `owner_id=None`, ignores ticket status, never expires or consumes. |
| Context | `src/py/vertebra/agent/core/context.py::AgentContext.from_request` [^agent-context] | Trusts `x-tenant-id`, mTLS, XFCC, and IAP headers from any caller; `getattr(request, "user")` raises `AssertionError` on Starlette without auth middleware. No production caller. |
| Adapters | `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp`, `WsgiMcpApp` [^mcp-routes] | Build `McpHttpRequest` without `context`; responses are fully buffered `bytes`. |
| Native & Litestar mounts | `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native`, `_mount_litestar_routes` [^agent-plugin] | Native endpoints pass headers without context; Litestar fallback drops headers and returns a `dict`, losing status codes. No skill catalog wiring. |
| SSE | `src/py/vertebra/agent/core/sse.py::SseStreamBroker` [^sse-broker] | Replay buffers and channels exist but no `GET` handler or stream writer uses them. |
| Skills | `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` [^skills] | Loads once, no filters, no listeners, no resource projection; `MCPCore` exposes `capabilities.skills`. |
| CLI | `src/py/vertebra/cli/ai.py::serve`, `bridge` [^cli-ai] | `serve` builds an empty `MCPService` and deletes `--name`; `bridge` takes `--token` only. |
| Stdio | `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` | Default path reads every stdin line before dispatching; no context, no notifications. |
| Bridge | `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` [^mcp-bridge] | Synchronous POST forwarding only; injects `MCP-Protocol-Version: 2026-07-28` on every frame. |
| Litestar | `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` [^litestar-mcp-plugin] | Merges registry tool names into the well-known document (done); pins `litestar-mcp>=0.16.0`. |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Agentic Harness, MCP 2026-07-28 & A2A 1.0](../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md) [^harness-okf]
- [`vtb` CLI](../../knowledge/domains/framework/cli.md), [Litestar extensions](../../knowledge/domains/extensions/litestar.md), [Polyglot test matrix](../../knowledge/testing/polyglot-test-matrix.md)
- Skills: [`python-framework`](../../../skills/python-framework/SKILL.md), [`review-python-framework`](../../../skills/review-python-framework/SKILL.md), [`review-tests`](../../../skills/review-tests/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: `AgentContext` construction from headers, ASGI scopes, and WSGI environs honors `scope["user"]` / `scope["auth"]` always and proxy identity headers only through an explicit `IdentitySource`; the mTLS preset accepts its header only from trusted proxy CIDRs; the IAP preset verifies the ES256 `x-goog-iap-jwt-assertion` against Google's IAP keys with the configured audience, issuer `https://cloud.google.com/iap`, and `exp`, and never trusts `x-goog-authenticated-user-email`; misconfiguration (opt-in without a source, IAP without audience, mTLS without CIDRs) fails at construction; Starlette requests without authentication middleware never raise.
- **R2**: `MCPCore` binds the request `AgentContext` (anonymous when absent) for the full dispatch, runs promoted tasks under a forked context with its own `CancellationToken`, cancels the coroutine on `tasks/cancel`, and keeps terminal task states immutable.
- **R3**: Every transport adapter (raw ASGI, Starlette/FastAPI, WSGI, Flask, Django, native `App`, Litestar fallback, stdio) passes an `AgentContext`; the Litestar fallback preserves HTTP status and headers.
- **R4**: Guards run exactly once per `tools/call` before confirmation and task promotion; `PermissionDeniedError` maps to `-32004`, `RateLimitExceededError` / `BudgetExceededError` to `-32005`; two-parameter guards fail closed without a connection.
- **R5**: Confirmation tickets are decided only while `pending` and unexpired, owner-bound tickets require the exact owner, approvals are single-use, tickets expire after `ttl_seconds`, and the store is bounded by `max_tickets`.
- **R6**: Version negotiation recognizes `SUPPORTED_PROTOCOL_VERSIONS`, returns `-32022` with `data.supported`, `data.supportedVersions`, and `data.requested`, rejects legacy-only methods in `2026-07-28` mode with HTTP `200` and a JSON-RPC `-32601` `Method not found` body, returns the same HTTP `200` / `-32601` body for unknown methods (HTTP `404` is reserved for unknown or expired legacy `Mcp-Session-Id` values, which legacy clients treat as session expiry), and echoes the negotiated version header ([#109](https://github.com/cofin/vertebra/issues/109)).
- **R7**: Legacy `initialize`, `ping`, and `notifications/initialized` work; `initialize` creates an owner-scoped `MCPSessionStore` session and returns `Mcp-Session-Id`; legacy POSTs validate sessions; `DELETE` terminates sessions; `OPTIONS` advertises `GET, POST, DELETE, OPTIONS` ([#109](https://github.com/cofin/vertebra/issues/109)).
- **R8**: `server/discover` and `initialize` share one capability builder honoring config flags, standard `tasks` and `logging` shapes, and registered extensions; client capabilities are captured into `AgentContext.metadata`; legacy `logging/setLevel` updates the session log level ([#109](https://github.com/cofin/vertebra/issues/109)).
- **R9**: Legacy `GET /mcp` with `Accept: text/event-stream` opens a session or anonymous SSE stream with `Last-Event-ID` replay, heartbeats, and per-owner stream limits, written incrementally by every adapter ([#109](https://github.com/cofin/vertebra/issues/109)).
- **R10**: Registry mutations publish `notifications/{tools,resources,prompts}/list_changed`; `MCPCore.log` publishes level-gated `notifications/message`; legacy progress tokens publish `notifications/progress` to the caller's session stream.
- **R11**: `resources/subscribe` / `resources/unsubscribe` bind URIs to legacy sessions behind a fail-closed authorizer; `notify_resource_updated` fans out to subscribed sessions and `2026-07-28` `subscriptions/listen` streams; termination and expiry drop subscriptions ([#112](https://github.com/cofin/vertebra/issues/112)).
- **R12**: The skills extension filters and paginates `skills/list`, resolves `skill://` URIs, projects skills into `resources/list`, `resources/templates/list`, and optionally `prompts/*`, advertises `capabilities.extensions["io.modelcontextprotocol/skills"]`, publishes skills list changes, and is wired by `AgentPlugin` and every framework mount ([#111](https://github.com/cofin/vertebra/issues/111)).
- **R13**: Resources, templates, and tools carry `title`, `icons`, and `_meta`; `ui://` resources and `_meta.ui.resourceUri` links are validated at mount; `capabilities.extensions["io.modelcontextprotocol/ui"]` advertises MIME types; app-only tools are hidden from clients without UI capability and from WebMCP manifests ([#110](https://github.com/cofin/vertebra/issues/110)).
- **R14**: The `mcp` extra requires `litestar-mcp>=0.20.0`, and `create_mcp_plugin` forwards dual-mode transport options.
- **R15**: `vtb ai mcp serve` reads stdio incrementally, supports legacy `initialize` sessions and notifications over stdout, and serves the discovered application's MCP surface, failing loud when none exists ([#114](https://github.com/cofin/vertebra/issues/114)).
- **R16**: `vtb ai mcp bridge` forwards both protocol modes without injecting `2026-07-28` headers into legacy handshakes, relays legacy `GET` SSE notifications, terminates sessions on EOF, and authenticates with static tokens or OIDC id_tokens minted for `--audience` with refresh and one `401` retry ([#114](https://github.com/cofin/vertebra/issues/114)).
- **R17**: User documentation, CLI reference, changelog, OKF chapters, and a limitations chapter describe the shipped behaviour.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# src/py/vertebra/agent/core/context.py
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

class IapIdentitySource:
    def __init__(self, audience: str, *, key_url: str = IAP_PUBLIC_KEY_URL, key_cache_seconds: float = 3600.0, jwks: Mapping[str, Any] | None = None) -> None: ...
    def resolve(self, request: IdentityRequest) -> CallerIdentity | None: ...

class AgentContext:
    @classmethod
    def from_headers(
        cls,
        headers: Mapping[str, str],
        *,
        identity_source: IdentitySource | None = None,
        peer: str | None = None,
        user: Any = None,
        auth: Any = None,
        session_id: str | None = None,
    ) -> "AgentContext": ...
    @classmethod
    def from_scope(cls, scope: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> "AgentContext": ...
    @classmethod
    def from_wsgi_environ(cls, environ: Mapping[str, Any], *, identity_source: IdentitySource | None = None) -> "AgentContext": ...
    @classmethod
    def from_request(cls, request_or_connection: Any, *, identity_source: IdentitySource | None = None, session_id: str | None = None, turn_id: str | None = None, progress_token: str | int | None = None, progress_callback: ProgressCallback | None = None) -> "AgentContext": ...
    def fork(self) -> "AgentContext": ...

def reset_agent_context(token: contextvars.Token[AgentContext | None]) -> None: ...

# src/py/vertebra/agent/mcp/models.py
MCP_PROTOCOL_VERSION: str = "2026-07-28"
LEGACY_PROTOCOL_VERSIONS: tuple[str, ...] = ("2025-11-25", "2025-06-18", "2025-03-26")
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = (MCP_PROTOCOL_VERSION, *LEGACY_PROTOCOL_VERSIONS)
MCP_FALLBACK_SESSION_VERSION: str = "2025-11-25"
LEGACY_ONLY_METHODS: frozenset[str] = frozenset({"initialize", "ping", "resources/subscribe", "resources/unsubscribe", "logging/setLevel"})
SESSION_CAPACITY_EXHAUSTED: int = -32000
SKILLS_EXTENSION: str = "io.modelcontextprotocol/skills"
UI_EXTENSION: str = "io.modelcontextprotocol/ui"
UI_MIME_TYPE: str = "text/html;profile=mcp-app"
type McpLogLevel = Literal["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]

class McpIcon(FrozenBaseStruct, frozen=True, kw_only=True):
    src: str
    mimeType: str | None = None
    sizes: tuple[str, ...] = ()

class NegotiatedProtocol(FrozenBaseStruct, frozen=True, kw_only=True):
    version: str
    modern: bool

class MCPConfig(FrozenBaseStruct, frozen=True, kw_only=True):
    # existing fields unchanged, plus:
    trust_proxy_identity: bool = False
    identity_source: IdentitySource | None = None
    session_ttl_seconds: int = 3600
    max_sessions: int = 10_000
    enable_get_sse: bool = True
    sse_heartbeat_seconds: float = 15.0
    max_streams_per_owner: int = 8
    logging: bool = False
    default_log_level: McpLogLevel = "info"
    resource_subscriptions: bool = True
    subscription_authorizer: Callable[[AgentContext, str], bool | Awaitable[bool]] | None = None
    ui_mime_types: tuple[str, ...] | None = None
    hide_app_only_tools: bool = True

class McpHttpResponse(FrozenBaseStruct, frozen=True, kw_only=True):
    status: int = 200
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    body: bytes = b""
    stream: AsyncIterator[bytes] | None = None

class ResourceEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    uri: str
    name: str
    title: str = ""
    description: str = ""
    mimeType: str = "text/plain"
    icons: tuple[McpIcon, ...] = ()
    meta: dict[str, Any] | None = None
    reader: Callable[..., Any] | None = None
# ResourceTemplateEntry gains the same title / icons / meta fields.

# src/py/vertebra/agent/mcp/sessions.py (new)
class MCPSession(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    session_id: str
    owner_id: str | None
    protocol_version: str
    client_capabilities: dict[str, Any]
    client_info: dict[str, Any]
    log_level: McpLogLevel
    subscriptions: frozenset[str] = frozenset()
    created_at_unix_ms: int
    expires_at_unix_ms: int

class SessionLimitExceededError(VertebraError, RuntimeError): ...

class MCPSessionStore:
    def __init__(self, *, ttl_seconds: int = 3600, max_sessions: int = 10_000, clock: Callable[[], float] = time.monotonic) -> None: ...
    async def create(self, *, owner_id: str | None, protocol_version: str, client_capabilities: Mapping[str, Any], client_info: Mapping[str, Any], log_level: McpLogLevel) -> MCPSession: ...
    async def get(self, session_id: str, owner_id: str | None) -> MCPSession | None: ...
    async def update(self, session_id: str, owner_id: str | None, *, log_level: McpLogLevel | None = None, subscriptions: frozenset[str] | None = None) -> MCPSession | None: ...
    async def delete(self, session_id: str, owner_id: str | None) -> MCPSession | None: ...
    async def subscribed_session_ids(self, uri: str) -> tuple[str, ...]: ...
    async def active_session_ids(self) -> tuple[str, ...]: ...

# src/py/vertebra/agent/core/sse.py
class StreamLimitExceededError(VertebraError, RuntimeError): ...
class SseStreamBroker:
    def open_stream(self, session_id: str, *, owner_id: str | None, last_event_id: str | None = None, heartbeat_seconds: float = 15.0, max_streams_per_owner: int = 8) -> AsyncIterator[bytes]: ...
    def close_session(self, session_id: str) -> None: ...

# src/py/vertebra/agent/core/schema.py
class ToolSpec(FrozenBaseStruct, frozen=True, kw_only=True):
    # existing fields unchanged, plus:
    meta: dict[str, Any] | None = None
class Tool:
    async def authorize(self, context: AgentContext | None = None) -> None: ...
    async def call(self, arguments: dict[str, Any], context: Any = None) -> Any: ...
    async def invoke(self, arguments: dict[str, Any], context: Any = None) -> Any: ...  # authorize + call

# src/py/vertebra/agent/runtime.py
class ConfirmationTicket(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    # existing fields, with:
    status: Literal["pending", "approved", "rejected", "consumed", "expired"] = "pending"
    expires_at_unix_ms: int = 0
class ConfirmationCapacityError(VertebraError, RuntimeError): ...
class ConfirmationGate:
    def __init__(self, *, ttl_seconds: float = 600.0, max_tickets: int = 10_000, clock: Callable[[], float] = time.time) -> None: ...
    def consume_call_approval(self, token: str | None, *, tool_name: str, arguments: Mapping[str, Any], owner_id: str | None) -> bool: ...
    # is_call_approved is removed; approve / reject / is_approved keep their signatures with corrected semantics.

# src/py/vertebra/agent/mcp/service.py
CONFIRMATION_REQUIRED: int = -32003
PERMISSION_DENIED: int = -32004
RATE_LIMITED: int = -32005
class MCPCore:
    sessions: MCPSessionStore
    def register_extension(self, name: str, payload: Callable[[], dict[str, Any]]) -> None: ...
    async def log(self, level: McpLogLevel, data: Any, *, logger: str | None = None) -> int: ...
    async def notify_resource_updated(self, uri: str) -> int: ...  # becomes async
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `AsgiMcpApp.__call__`, `handle_starlette_request` | `AgentContext.from_scope` -> `MCPCore.handle` | `bind_agent_context`, `JSONRPCRouter.dispatch_payload` |
| `WsgiMcpApp.handle_vertebra_request`, `handle_flask_view`, `__call__` | `AgentContext.from_headers` / `from_wsgi_environ` -> `PortalProvider.default().call(MCPCore.handle)` | Portal event loop |
| `AgentPlugin.mount_native` endpoints, Litestar fallback | `AgentContext.from_headers(request.headers.as_dict())` / `from_scope(request.scope)` -> `MCPCore.handle` | Native DI `request` binding, `litestar.Response` |
| `run_stdio_server` | stdio `AgentContext` + `MCPSessionStore` session -> `MCPCore.handle_payload` | `SseStreamBroker.open_stream` notification writer |
| `MCPCore._handle_tools_call` | `Tool.authorize` -> `ConfirmationGate.consume_call_approval` -> `Tool.call` / `MCPTaskStore` | `GuardChain.execute`, forked `AgentContext` |
| `MCPCore.handle` (`GET`) | `SseStreamBroker.open_stream` -> `McpHttpResponse.stream` | Adapter stream writers |
| `ToolRegistry` listeners, `SkillCatalog` listeners | `MCPCore._publish_notification` | `SseStreamBroker.publish` for session streams |
| `MCPCore.notify_resource_updated` | `MCPSessionStore.subscribed_session_ids` + listen registrations | `SseStreamBroker.publish` |
| `vtb ai mcp serve` | `resolve_app_mcp_surface(app)` -> `run_stdio_server` or `litestar_mcp.mcp.stdio.run_stdio` | `VertebraEnv` |
| `vtb ai mcp bridge` | `run_stdio_http_bridge` + `OidcTokenSource` | HTTP transport, id_token minting from `gcp-embeddings-and-identity` |

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Spoofed identity header | `x-goog-authenticated-user-email: a@x` with default config | Header ignored; `auth_scheme == "anonymous"` | Identity only from trusted sources |
| Starlette without auth middleware | `request.scope` lacks `user` | Context builds from scope; no `AssertionError` | Adapters never crash on identity |
| Guard denial | `RequireRoles("admin")` vs caller without role | HTTP `200`, JSON-RPC `-32004`, `data.tool` set | Guards run once |
| Token bucket | capacity `1`, one call | One token consumed per call | No double consumption |
| Ticket replay | Same approved token twice | Second call returns `-32003` with a fresh token | Single-use approvals |
| Rejected ticket approve | `approve` after `reject` | `False`, status stays `rejected` | Decisions only from `pending` |
| Unsupported version | `MCP-Protocol-Version: 1900-01-01` | HTTP `400`, `-32022`, `data.supported` lists four versions | Clients can renegotiate |
| Legacy method in modern mode | `initialize` with `2026-07-28` header | HTTP `200`, `-32601` | Stateless profile stays stateless |
| Unknown method | `tasks/result`, any version | HTTP `200`, `{"jsonrpc":"2.0","id":<id>,"error":{"code":-32601,"message":"Method not found"}}` | Legacy clients never read a method error as session expiry |
| Unknown session | Legacy POST with expired `Mcp-Session-Id` | HTTP `404`, `-32602` `Session not found or expired` | `404` is reserved for session expiry |
| Proxy headers by default | IAP email, mTLS subject, `x-tenant-id`; default `MCPConfig` | Ignored; anonymous | No implicit header trust |
| IAP assertion | Valid / bad signature / wrong audience / expired `x-goog-iap-jwt-assertion`; plain email header alone | Identity only for the valid token | Signed identity only |
| mTLS subject | Peer inside vs outside `trusted_proxies` | Subject only from trusted proxies | Header origin checked |
| Identity misconfiguration | `trust_proxy_identity=True` without source; IAP without audience; mTLS without CIDRs | `ValueError` at construction | Fail loud |
| Session capacity | `max_sessions` reached | HTTP `503`, `-32000` | Bounded memory |
| GET in modern mode | `GET` with `MCP-Protocol-Version: 2026-07-28` | HTTP `405`, `Allow: POST, DELETE, OPTIONS` | No server-push in stateless mode |
| GET without SSE accept | `Accept: application/json` | HTTP `406` | Content negotiation |
| Stream limit | 9th stream for one owner (limit `8`) | HTTP `503` | Bounded streams |
| Subscription authorizer error | Authorizer raises | `-32004`, subscription not stored | Fail closed |
| Broken UI link | Tool `_meta.ui.resourceUri = "ui://missing"` | `ImproperConfigurationError` at mount | Fail loud |
| Serve without MCP | App with no MCP surface | `click.ClickException`, exit code `1` | Fail loud |
| Bridge `401` | Expired id_token | Refresh once and retry; second `401` is written as JSON-RPC error | Bounded retries |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a Starlette app mounted with `mount_mcp_routes` and a `2025-06-18` client, when it sends `initialize`, then it receives `protocolVersion: "2025-06-18"`, an `Mcp-Session-Id`, and can open `GET /mcp` to receive `notifications/tools/list_changed` after a tool registers.
- Given a tool guarded by `RequireRoles("admin")`, when an ASGI caller whose `scope["user"]` has role `admin` calls it, then the guard sees that identity and the call succeeds; an anonymous caller receives `-32004`.
- Given a confirmation-gated tool, when the approved token is reused for a second identical call, then the second call is blocked with `-32003`.
- Given a legacy session subscribed to `config://app`, when application code awaits `core.notify_resource_updated("config://app")`, then the session stream receives one `notifications/resources/updated` frame.
- Given `vtb ai mcp serve` in a project whose app mounts `AgentPlugin`, when a host writes `tools/list`, then the response lists the app's tools before stdin closes.

### 2.6 Alternatives Considered

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Legacy support location | Dual-mode inside `MCPCore` | A separate legacy service class duplicates routing, guards, and confirmation. |
| Session storage | In-memory `MCPSessionStore` per process | A store abstraction over `vertebra.framework.stores` adds a seam with one adapter; recorded as a limitation. |
| Guard de-duplication | `Tool.authorize` + `Tool.call` split with `invoke` composing both | A `guards_checked` flag on `invoke` leaks protocol state into the tool API. |
| Approval replay | Consume on successful check | Counting uses per ticket adds policy surface without a use case. |
| Streaming envelope | `McpHttpResponse.stream` field | A second response type forces every adapter to branch on type. |
| Litestar | Delegate to `litestar-mcp>=0.20.0` | Duplicating sessions in the Vertebra Litestar fallback diverges from the upstream implementation. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/12 files documented]`
- `[ ]` `docs/usage/agent-protocols.md` — Dual-mode transport, sessions, `GET` SSE, subscriptions, skills, MCP Apps, context and guards (`ch11:t01`).
- `[ ]` `docs/usage/a2a-and-workflows.md` — Confirmation lifecycle and error codes (`ch11:t01`).
- `[ ]` `docs/reference/agent.md` — New public symbols (`ch11:t01`).
- `[ ]` `docs/getting-started/cli.md` — `vtb ai mcp serve` and `bridge --audience` (`ch11:t02`).
- `[ ]` `docs/reference/cli.md` — Regenerated CLI reference (`ch11:t02`).
- `[ ]` `docs/changelog/index.md` — Unreleased entries (`ch11:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Present-tense harness chapter (`ch11:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` — New limitations chapter (`ch11:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — Catalog entry (`ch11:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/framework/cli.md` — CLI command table (`ch11:t04`).
- `[ ]` `.agents/bundles/knowledge/domains/extensions/litestar.md` — `litestar-mcp>=0.20.0` delegation (`ch11:t04`).
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — MCP conformance evidence (`ch11:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 | `ch01 / t01`, `ch01 / t03` (`MCPConfig` validation), `ch12 / t01` (IAP preset) | `regression_tdd`, `behavior_tdd` | `src/py/tests/unit/agent/test_core_and_observability.py`, `test_mcp_conformance.py` |
| R2 | `ch01 / t02` | `regression_tdd` | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R3 | `ch01 / t03`, `ch01 / t04` | `regression_tdd` | `test_mcp_conformance.py`, `src/py/tests/unit/cli/test_mcp_cli.py` |
| R4 | `ch02 / t01` | `regression_tdd` | `test_mcp_conformance.py`, `test_core_and_observability.py` |
| R5 | `ch02 / t02` | `regression_tdd` | `test_mcp_conformance.py`, `src/py/tests/unit/agent/test_runtime_and_workflows.py` |
| R6 | `ch03 / t01` | `behavior_tdd` | `test_mcp_conformance.py` |
| R7 | `ch03 / t02`, `ch03 / t03` | `behavior_tdd` | `test_mcp_conformance.py` |
| R8 | `ch03 / t04` | `behavior_tdd` | `test_mcp_conformance.py` |
| R9 | `ch04 / t01`, `ch04 / t02` | `behavior_tdd` | `test_mcp_conformance.py`, `test_core_and_observability.py` |
| R10 | `ch04 / t03`, `ch04 / t04` | `behavior_tdd` | `test_mcp_conformance.py` |
| R11 | `ch05 / t01`–`t03` | `behavior_tdd` | `test_mcp_conformance.py` |
| R12 | `ch06 / t01`–`t04` | `behavior_tdd` | `test_mcp_conformance.py` |
| R13 | `ch07 / t01`–`t04` | `behavior_tdd` | `test_mcp_conformance.py`, `test_core_and_observability.py` |
| R14 | `ch08 / t01` | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py` |
| R15 | `ch09 / t01`, `ch09 / t02` | `behavior_tdd` | `test_mcp_cli.py` |
| R16 | `ch10 / t01`, `ch10 / t02` | `behavior_tdd` | `test_mcp_cli.py` |
| R17 / docs | `ch11 / t01`, `t02` | `documentation_validation` | `make docs` |
| R17 / OKF | `ch11 / t03`, `t04` | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [~] ch01-agent-context-propagation — Trusted context construction, core binding, task cancellation, and adapter contexts (chapters/ch01-agent-context-propagation/spec.md)
- [ ] ch02-guard-and-confirmation-correctness — Single-pass guards with typed errors and single-use, expiring confirmation tickets (chapters/ch02-guard-and-confirmation-correctness/spec.md)
- [ ] ch03-version-negotiation-and-sessions — Version registry, legacy initialize and sessions, termination, and shared capabilities (chapters/ch03-version-negotiation-and-sessions/spec.md)
- [ ] ch04-get-sse-streams — Streaming envelope, `GET` SSE, adapter writers, and session notifications (chapters/ch04-get-sse-streams/spec.md) [empirical_dependent: server-runtime-correctness lands incremental ASGI response streaming]
- [ ] ch05-resource-subscriptions — Subscribe/unsubscribe, update fan-out, and stateless `subscriptions/listen` (chapters/ch05-resource-subscriptions/spec.md)
- [ ] ch06-skills-extension — Catalog disclosure, core skill surface, and plugin/mount wiring (chapters/ch06-skills-extension/spec.md)
- [ ] ch07-mcp-apps-ui-metadata — SEP-1865 metadata models, projection, validation, and visibility (chapters/ch07-mcp-apps-ui-metadata/spec.md)
- [ ] ch08-litestar-delegation-parity — `litestar-mcp>=0.20.0` dual-mode delegation (chapters/ch08-litestar-delegation-parity/spec.md) [empirical_dependent: litestar-mcp 0.20.0 published to PyPI]
- [ ] ch09-stdio-and-serve — Incremental stdio sessions and `vtb ai mcp serve` app discovery (chapters/ch09-stdio-and-serve/spec.md)
- [ ] ch10-bridge-dual-mode-and-oidc — Dual-mode bridge with SSE relay and OIDC id_token authentication (chapters/ch10-bridge-dual-mode-and-oidc/spec.md) [empirical_dependent: gcp-embeddings-and-identity lands the id_token minting API]
- [ ] ch12-iap-identity-source — Verified IAP signed-header JWT identity source (chapters/ch12-iap-identity-source/spec.md)
- [ ] ch11-docs-and-okf — User docs, CLI reference, changelog, OKF chapters, and limitations (chapters/ch11-docs-and-okf/spec.md)
- **Flow Recovery Checkpoint**: `uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q && make lint-all && make docs && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `c8722c76` — Plan authored; `0/12` chapters and `0/35` tasks completed; `plan_commit` is stamped by the coordinator after approval.
- **Active Chapter**: `ch01-agent-context-propagation` (`0/4` tasks completed).
- **Remaining Plan**: `ch01`–`ch10`, then `ch12`, then `ch11` (listed order; `ch12` follows `ch01` and precedes the docs chapter).
- **Documentation & OKF Sync**: `[0/12 files documented]`.
- **Active Triggers**: `ch04` replans when `server-runtime-correctness` lands incremental ASGI streaming; `ch08` replans when `litestar-mcp` `0.20.0` is published; `ch10:t02` replans when `gcp-embeddings-and-identity` lands id_token minting.

[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — Transport-agnostic MCP JSON-RPC and HTTP core.
[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp` — Shared ASGI and WSGI MCP adapters.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::MCPConfig` — MCP configuration, envelopes, and resource models.
[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext` — Caller identity, cancellation, and context variable binding.
[^guards]: `src/py/vertebra/agent/core/guards.py::authorize` — Guard chain execution and authorization entry point.
[^confirmation-gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Human confirmation ticket store.
[^tool-invoke]: `src/py/vertebra/agent/core/schema.py::Tool` — Tool descriptor invocation, guard execution, and argument binding.
[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — SSE replay buffers and per-session subscriber channels.
[^skills]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` — Skill catalog and filesystem skill source.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin` — Native App and Litestar agent harness mounting.
[^cli-ai]: `src/py/vertebra/cli/ai.py::serve` — `vtb ai mcp serve` and `bridge` commands.
[^mcp-bridge]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — Stdio-to-HTTP MCP bridge.
[^litestar-mcp-plugin]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — LitestarMCP factory and well-known document merge.
[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agent harness OKF chapter.
