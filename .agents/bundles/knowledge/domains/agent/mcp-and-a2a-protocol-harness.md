---
type: Concept
title: "Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols"
description: "Dual-mode MCP 2026-07-28 and legacy Streamable HTTP/SSE/Stdio harness, skills and SEP-1865 MCP Apps UI extensions, A2A 1.0 AgentCards, DAG/PlanTree workflows, and IAP/OIDC identity."
status: stable
tags:
  - domains
  - agent
  - mcp
  - a2a
  - webmcp
  - workflow
  - telemetry
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-19T04:32:00Z"
    commit: 6f76f8a7
  - by: agent:flow-executor
    at: "2026-11-17T05:50:00Z"
    commit: 5a833832
  - by: agent:flow-executor
    at: "2026-11-13T20:35:00Z"
    commit: 54a5103d
  - by: agent:flow-setup
    at: "2026-11-12T23:30:00Z"
    commit: 18baadcd
sources:
  - id: agent-context
    resource: src/py/vertebra/agent/core/context.py::AgentContext
    title: Trusted context construction, independent forks, and binding restoration
  - id: agent-identity
    resource: src/py/vertebra/agent/core/identity.py::IapIdentitySource
    title: IAP ES256, mTLS CIDR, and callable identity sources
  - id: mcp-sse
    resource: src/py/vertebra/agent/core/sse.py::CacheSseStreamBroker
    title: SyncCacheStore-backed SSE replay broker alongside SseStreamBroker
  - id: mcp-tasks
    resource: src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore
    title: Exact-owner task access, TTL pruning, QueueService dispatch, and CacheMCPTaskStore
  - id: mcp-sessions
    resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore
    title: Owner-scoped legacy MCP sessions, subscriptions, and AsyncCacheStore CacheMCPSessionStore
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin
    title: Native and framework MCP/A2A mounting with SkillCatalog and UI validation
  - id: src-typing-public-py
    resource: src/py/vertebra/typing.py
    title: Public neutral ToolProvider and ToolPolicyError exports
  - id: src-db-tools-py
    resource: src/py/vertebra/db/adapters/bigquery/tools.py
    title: Adapter-owned BigQuery and Spanner ToolProvider policies
  - id: src-agent-core-schema-py
    resource: src/py/vertebra/agent/core/schema.py
    title: Single-pass Tool.authorize/Tool.call and SchemaBuilder compilation
  - id: src-agent-tools-init-py
    resource: src/py/vertebra/agent/tools/__init__.py
    title: Generic provider adaptation and caller-bound confirmation
  - id: src-agent-mcp-models-py
    resource: src/py/vertebra/agent/mcp/models.py
    title: SUPPORTED_PROTOCOL_VERSIONS, UI/skills constants, and MCPHttpResponse
  - id: src-agent-mcp-registry-py
    resource: src/py/vertebra/agent/mcp/registry.py
    title: ToolRegistry capability catalog and UI link validation
  - id: src-agent-mcp-webmcp-py
    resource: src/py/vertebra/agent/mcp/webmcp.py
    title: WebMCP manifest builder and app-only tool filtering
  - id: src-agent-mcp-service-py
    resource: src/py/vertebra/agent/mcp/service.py
    title: Dual-mode MCPCore engine, SSE streams, and subscriptions
  - id: src-agent-mcp-routes-py
    resource: src/py/vertebra/agent/mcp/routes.py
    title: AsgiMCPApp, WsgiMCPApp, and .well-known/mcp.json handlers
  - id: src-agent-mcp-stdio-py
    resource: src/py/vertebra/agent/mcp/stdio.py
    title: Incremental cancellable stdio server with legacy session relay
  - id: src-agent-mcp-bridge-py
    resource: src/py/vertebra/agent/mcp/bridge.py
    title: Dual-mode stdio-to-HTTP bridge, GET SSE relay, and OidcTokenSource
  - id: src-agent-mcp-skills-py
    resource: src/py/vertebra/agent/mcp/skills.py
    title: SkillCatalog progressive disclosure and skill:// resource resolution
  - id: src-agent-a2a-service-py
    resource: src/py/vertebra/agent/a2a/service.py
    title: A2A 1.0 AgentCard JWS signing, dynamic URLs, and task lifecycle
  - id: src-agent-workflow-py
    resource: src/py/vertebra/agent/workflow.py
    title: Workflow, PlanTree, and AgentRuntime confirmation gates
  - id: src-vertebra-py-src-agent-rs
    resource: src/rs/vertebra-py/src/agent.rs
    title: Rust JSON-RPC classification and SchemaDigestCache fastpaths
  - id: src-vertebra-http-src-discovery-rs
    resource: src/rs/vertebra-http/src/discovery.rs
    title: Rust WellKnownDiscoveryCache ArcSwap discovery fastpath
  - id: src-src-agent-index-ts
    resource: src/ts/vertebra-client/src/agent/index.ts
    title: TypeScript McpClient, A2aClient, and registerWebMCPTools
  - id: src-agent-okf-py
    resource: src/py/vertebra/agent/okf.py
    title: Pure-Python OKF v0.2 bundle parser, validator, digester, and ZIP serializer
  - id: src-agent-knowledge-catalog-py
    resource: src/py/vertebra/agent/knowledge_catalog.py
    title: Dataplex v1 SyncKnowledgeCatalogClient and AsyncKnowledgeCatalogClient with OKF sync
---

# Agentic Harness, MCP 2026-07-28 & A2A 1.0 Protocols

<!-- truth: start -->
- `ToolSpec` and `ToolAnnotations` compile JSON Schemas once via `typing.get_type_hints` -> `msgspec.defstruct`; `Tool.authorize` runs guards once (`-32004`/`-32005`) before `Tool.call`, and `ConfirmationGate.consume_call_approval` enforces pending-only expiring single-use owner/argument-bound tickets (`-32003`).
- `MCPCore` negotiates `SUPPORTED_PROTOCOL_VERSIONS = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26")` (`-32022` on mismatch), advertises `completions` (`completion/complete` via `ToolRegistry.get_completion`), validates `Mcp-Method`/`Mcp-Name`/`Mcp-Param-*` (`-32020`), and gates `LEGACY_ONLY_METHODS` (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe`, `resources/unsubscribe`; `tasks/result` removed) to legacy sessions [^src-agent-mcp-service-py] [^mcp-sessions].
- Distributed multi-worker state uses `MCPConfig(cache_url=...)` (`CacheMCPSessionStore` over `AsyncCacheStore`; `CacheMCPTaskStore` and `CacheSseStreamBroker` over `SyncCacheStore`) and `A2AConfig(cache_url=...)` (`CacheA2ATaskStore`, cache-backed `PushNotificationDispatcher`, and `CacheSseStreamBroker`) [^mcp-sessions] [^mcp-tasks] [^mcp-sse] [^src-agent-a2a-service-py].
- `SKILLS_EXTENSION` (`io.modelcontextprotocol/skills`), SEP-1865 `UI_EXTENSION` (`io.modelcontextprotocol/ui`, `MCPIcon`, app-only filtering, `MCPCore.validate()`), and canonical `/.well-known/mcp.json` wire identically across `AgentPlugin`, Starlette, FastAPI, Flask, Django, and `litestar-mcp>=0.20.0` [^agent-plugin] [^src-agent-mcp-skills-py] [^src-agent-mcp-routes-py].
- `run_stdio_server` dispatches incrementally with `notifications/cancelled` cancellation and `vtb ai mcp serve --app` discovery; `run_stdio_http_bridge` supports dual-mode SSE + `OidcTokenSource` (`AsyncGcpIdTokenCache`, `401` retry); `IapIdentitySource` verifies ES256 `x-goog-iap-jwt-assertion` JWTs and `BearerJWTGuard` delegates to `JWKSVerifier` [^src-agent-mcp-stdio-py] [^src-agent-mcp-bridge-py] [^agent-identity].
- `A2AService.stream_message` cleans up broker subscriptions in `finally`, `PushNotificationDispatcher.validate_webhook_url` pins DNS-resolved public IPs against SSRF, and `MCPTaskStore` dispatches via `QueueService` and prunes TTL-expired records [^src-agent-a2a-service-py] [^mcp-tasks].
- `vertebra.agent.okf` provides `OkfSource`, `OkfDocument`, `OkfBundle`, and `OkfValidationError` without `PyYAML`, enforcing OKF v0.2 reserved files, fence-aware truth/footnote scanning, `120,832`-byte ceilings, SHA-256 digests, and reproducible ZIP archives [^src-agent-okf-py].
- `vertebra.agent.knowledge_catalog` provides `KnowledgeCatalogConfig`, `CatalogEntryRecord`, `CatalogSyncSummary`, `SyncKnowledgeCatalogClient`, and `AsyncKnowledgeCatalogClient` over `dataplex.googleapis.com/v1` with LRO `entryGroups` polling, `view="ALL"` reads, `<= 250`-entry digest sync, and `locations/global:searchEntries` [^src-agent-knowledge-catalog-py].
- `sync_bundle(delete_stale=True)` deletes only entries marked `entrySource.system == "okf"` with `okf-format` and `okf-sha256` labels, and a failed sync raises `CatalogSyncError` with the completed `CatalogSyncSummary`. The marker check and the `parent=` search predicate are verified against in-memory transports, not a live Dataplex project. The client does not create the `okf-document` aspect type [^src-agent-knowledge-catalog-py].
<!-- truth: end -->

## 1. Dual-Mode MCP (`MCPCore`, Sessions, SSE, Skills & UI)

`MCPCore` (`vertebra.agent.mcp`) replaces legacy `MCPService` aliases across Python, CLI, and ASGI/WSGI mounts (`AsgiMCPApp`, `WsgiMCPApp`) with unified `MCPHttpResponse.stream` SSE delivery [^src-agent-mcp-service-py] [^src-agent-mcp-models-py] [^src-agent-mcp-routes-py] (residual caveats live in [MCP Implementation Limitations](mcp-limitations.md)).

| Component | Responsibility |
| :--- | :--- |
| [`ToolSpec`, `Tool`, `SchemaBuilder`](../../../../../src/py/vertebra/agent/core/schema.py)[^src-agent-core-schema-py] | Derives input/output schemas without `eval()`; splits `Tool.authorize` (`-32004` permission, `-32005` rate/budget limit) from `Tool.call` (`Tool.invoke` composes both). Default `exposure` excludes `"webmcp"` unless opted in. |
| [`ToolRegistry` & `SkillCatalog`](../../../../../src/py/vertebra/agent/mcp/registry.py)[^src-agent-mcp-registry-py] | Canonical protocol-neutral registry across MCP, A2A, WebMCP, and host extensions unifying `ToolProvider`, `@tool`, controllers, completion callbacks (`get_completion`), and route handlers. Validates `ui://` resource links (`validate_ui_links`) and exposes filtered `SKILL.md` resources/prompts under `SKILLS_EXTENSION` [^src-agent-mcp-skills-py]. |
| [`MCPCore` & `MCPSessionStore` / `CacheMCPSessionStore`](../../../../../src/py/vertebra/agent/mcp/service.py) | Validates `Origin`, `Mcp-Method`/`Mcp-Name`/`Mcp-Param-*` (`-32020`), `ETag` `304`, and `SUPPORTED_PROTOCOL_VERSIONS` (`-32022`). Handles `completion/complete`, gates `LEGACY_ONLY_METHODS` (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe`, `resources/unsubscribe`), and selects per-process (`MCPSessionStore`, `SseStreamBroker`) or `vertebra.cache`-backed (`CacheMCPSessionStore` over `AsyncCacheStore`, `CacheSseStreamBroker` over `SyncCacheStore`) stores via `MCPConfig(cache_url=...)` [^mcp-sessions] [^mcp-sse]. |
| [`AgentPlugin` & WebMCP](../../../../../src/py/vertebra/agent/plugin.py) | Coerces `SkillCatalog` and runs `MCPCore.validate()` at mount across native `App`, Litestar (`litestar-mcp>=0.20.0` with awaitable `ObjectStoreSkillSource` reads), Starlette, FastAPI, Flask, and Django; `build_webmcp_manifest` filters `"webmcp"` tools and omits app-only tools for non-UI clients [^src-agent-mcp-webmcp-py]. |
| [`run_stdio_server` & `run_stdio_http_bridge`](../../../../../src/py/vertebra/agent/mcp/stdio.py) | Incremental task-group stdio dispatch (`notifications/cancelled`, `notifications/progress` relay, EOF cleanup) wired into `vtb ai mcp serve --app`; dual-mode HTTP/GET SSE bridge (`Last-Event-ID`, `5` reconnects, EOF `DELETE`) + `OidcTokenSource` (`AsyncGcpIdTokenCache`, `401` single retry) for `vtb ai mcp bridge --audience` [^src-agent-mcp-bridge-py]. |

## 2. Caller Identity, Provider Tools & Task Ownership

`AgentContext.from_scope` / `from_wsgi_environ` read framework-authenticated callers, while proxy headers require `MCPConfig(trust_proxy_identity=True)` paired with `IdentitySource`: `IapIdentitySource` (`vertebra-core[iap]`) cryptographically verifies ES256 `x-goog-iap-jwt-assertion` JWTs (`aud`, `iss`, `exp`), refusing plain `x-goog-authenticated-user-email`; `MtlsIdentitySource` validates immediate proxy peers against CIDRs; and `CallableIdentitySource` delegates to app policy. Inbound HTTP/gRPC Bearer JWT verification uses `JWKSVerifier` (`vertebra.framework.security`) [^agent-context] [^agent-identity].

Promoted tasks fork independent `CancellationToken` state in `MCPTaskStore` / `CacheMCPTaskStore` (with `enqueue_execution` dispatching through `QueueService` and `prune_expired` enforcing `ttl_ms`) so owner `tasks/cancel` stops execution while preserving terminal states and owner isolation [^mcp-tasks]. [`build_tools`](../../../../../src/py/vertebra/agent/tools/__init__.py)[^src-agent-tools-init-py] adapts [`ToolProvider`](../../../../../src/py/vertebra/typing.py)[^src-typing-public-py] implementations (`BigQueryToolProvider`, `SpannerToolProvider`[^src-db-tools-py]) lazily with canonical single-use expiring `ConfirmationGate` argument/owner checks.

## 3. A2A 1.0, Workflows & Native FFI Acceleration

- **A2A 1.0 & TS Client ([`a2a/service.py`](../../../../../src/py/vertebra/agent/a2a/service.py)[^src-agent-a2a-service-py], [`agent/index.ts`](../../../../../src/ts/vertebra-client/src/agent/index.ts)[^src-src-agent-index-ts])**: Dynamic request-origin `/.well-known/agent-card.json` discovery, RFC 7515 JWS (`HS256`) card signing, and owner-isolated A2A tasks (`A2ATaskStore` or `CacheA2ATaskStore` + `CacheSseStreamBroker` via `A2AConfig.cache_url`). Includes `stream_message` `finally` disconnect cleanup, DNS-pinned SSRF validation (`PushNotificationDispatcher.validate_webhook_url`), and stateless TS `McpClient`, `A2aClient`, and `registerWebMCPTools`.
- **Workflows & Native Caches ([`workflow.py`](../../../../../src/py/vertebra/agent/workflow.py)[^src-agent-workflow-py], [`agent.rs`](../../../../../src/rs/vertebra-py/src/agent.rs)[^src-vertebra-py-src-agent-rs], [`discovery.rs`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs])**: `Workflow`, `WorkflowLiveState`, `PlanTree`, and `AgentRuntime` pair with PyO3 `classify_jsonrpc_frame`, `SchemaDigestCache`, and lock-free `WellKnownDiscoveryCache` `304` handling.

## 4. OKF v0.2 Bundles & Knowledge Catalog

- **OKF v0.2 Serializer ([`okf.py`](../../../../../src/py/vertebra/agent/okf.py)[^src-agent-okf-py])**: `OkfDocument` and `OkfBundle` parse canonical YAML frontmatter without `PyYAML` and scan fenced blocks safely. They enforce reserved-file rules (`index.md`, `log.md`), `120,832`-byte ceilings, symlink/traversal guards, canonical SHA-256 digests, and sorted POSIX ZIP export (`1980-01-01`).
- **Dataplex v1 Clients ([`knowledge_catalog.py`](../../../../../src/py/vertebra/agent/knowledge_catalog.py)[^src-agent-knowledge-catalog-py])**: `SyncKnowledgeCatalogClient` and `AsyncKnowledgeCatalogClient` wrap native `GcpTokenCache` and HTTP/2 transport (`gcp_http2_request_*`) with single `401` retry and `x-goog-user-project` forwarding. They support LRO `entryGroups` creation, `resolve_context_set` label matching, `view="ALL"` entry reads, `aspectKeys` updates, `<= 250`-entry `sync_bundle`, and `global:searchEntries`.

[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext` — Trusted constructors, independent forks, and binding restoration.
[^agent-identity]: `src/py/vertebra/agent/core/identity.py::IapIdentitySource` — ES256 IAP JWT, mTLS CIDR, and callable identity policies.
[^mcp-sse]: `src/py/vertebra/agent/core/sse.py::CacheSseStreamBroker` — `SyncCacheStore`-backed SSE replay broker alongside `SseStreamBroker`.
[^mcp-tasks]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — Owner-isolated task records, TTL pruning, `QueueService` dispatch, and `CacheMCPTaskStore`.
[^mcp-sessions]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore` — Owner-scoped legacy sessions, subscriptions, and `CacheMCPSessionStore`.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin` — Native and host-framework MCP/A2A mounting.
[^src-typing-public-py]: `src/py/vertebra/typing.py` — Neutral `ToolProvider` and `ToolPolicyError` exports.
[^src-db-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — Adapter-owned BigQuery/Spanner `ToolProvider` policies.
[^src-agent-core-schema-py]: `src/py/vertebra/agent/core/schema.py` — `ToolSpec`, `Tool.authorize`/`call`, and `SchemaBuilder`.
[^src-agent-tools-init-py]: `src/py/vertebra/agent/tools/__init__.py` — Generic `build_tools` provider adaptation.
[^src-agent-mcp-models-py]: `src/py/vertebra/agent/mcp/models.py` — Protocol constants, extensions, and `MCPHttpResponse`.
[^src-agent-mcp-registry-py]: `src/py/vertebra/agent/mcp/registry.py` — Canonical `ToolRegistry` capability catalog and `validate_ui_links`.
[^src-agent-mcp-webmcp-py]: `src/py/vertebra/agent/mcp/webmcp.py` — WebMCP manifest generation and app-only filtering.
[^src-agent-mcp-service-py]: `src/py/vertebra/agent/mcp/service.py` — Dual-mode `MCPCore` HTTP/JSON-RPC engine.
[^src-agent-mcp-routes-py]: `src/py/vertebra/agent/mcp/routes.py` — `AsgiMCPApp`, `WsgiMCPApp`, and `.well-known` handlers.
[^src-agent-mcp-stdio-py]: `src/py/vertebra/agent/mcp/stdio.py` — Incremental cancellable stdio transport runner.
[^src-agent-mcp-bridge-py]: `src/py/vertebra/agent/mcp/bridge.py` — Dual-mode HTTP/SSE bridge and `OidcTokenSource`.
[^src-agent-mcp-skills-py]: `src/py/vertebra/agent/mcp/skills.py` — `SkillCatalog` disclosure and `skill://` resolution.
[^src-agent-a2a-service-py]: `src/py/vertebra/agent/a2a/service.py` — A2A 1.0 `AgentCard` signing and task execution.
[^src-agent-workflow-py]: `src/py/vertebra/agent/workflow.py` — `Workflow`, `PlanTree`, and `AgentRuntime`.
[^src-vertebra-py-src-agent-rs]: `src/rs/vertebra-py/src/agent.rs` — Rust JSON-RPC classifier and `SchemaDigestCache`.
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust `WellKnownDiscoveryCache` `ArcSwap` handler.
[^src-src-agent-index-ts]: `src/ts/vertebra-client/src/agent/index.ts` — TypeScript `McpClient`, `A2aClient`, and `registerWebMCPTools`.
[^src-agent-okf-py]: `src/py/vertebra/agent/okf.py` — Pure-Python OKF v0.2 parser, validator, digester, and ZIP archive serializer.
[^src-agent-knowledge-catalog-py]: `src/py/vertebra/agent/knowledge_catalog.py` — `SyncKnowledgeCatalogClient` and `AsyncKnowledgeCatalogClient` over Dataplex v1.
