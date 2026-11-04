---
type: Concept
title: "Framework Subdomains"
description: "Built-in vertebra.framework capability modules: DI, security, autowire, HTMX, SSR, OpenAPI, AsyncAPI, queues, channels, stores, email, procedures, and first-party litestar-mcp."
status: stable
tags: [domains, framework, subdomains, security, mcp]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-framework-init-py
    resource: src/py/vertebra/framework/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/__init__.py)"
  - id: src-framework-di-py
    resource: src/py/vertebra/framework/di.py
    title: "Python implementation module (src/py/vertebra/framework/di.py)"
  - id: src-framework-security-py
    resource: src/py/vertebra/framework/security.py
    title: "Python implementation module (src/py/vertebra/framework/security.py)"
  - id: src-framework-autowire-py
    resource: src/py/vertebra/framework/autowire.py
    title: "Python implementation module (src/py/vertebra/framework/autowire.py)"
  - id: src-framework-htmx-py
    resource: src/py/vertebra/framework/htmx.py
    title: "Python implementation module (src/py/vertebra/framework/htmx.py)"
  - id: src-framework-ssr-init-py
    resource: src/py/vertebra/framework/ssr/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/ssr/__init__.py)"
  - id: src-framework-openapi-py
    resource: src/py/vertebra/framework/openapi.py
    title: "Python implementation module (src/py/vertebra/framework/openapi.py)"
  - id: src-framework-asyncapi-py
    resource: src/py/vertebra/framework/asyncapi.py
    title: "Python implementation module (src/py/vertebra/framework/asyncapi.py)"
  - id: src-framework-queues-init-py
    resource: src/py/vertebra/framework/queues/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/queues/__init__.py)"
  - id: src-framework-channels-py
    resource: src/py/vertebra/framework/channels.py
    title: "Python implementation module (src/py/vertebra/framework/channels.py)"
  - id: src-framework-stores-py
    resource: src/py/vertebra/framework/stores.py
    title: "Python implementation module (src/py/vertebra/framework/stores.py)"
  - id: src-framework-email-init-py
    resource: src/py/vertebra/framework/email/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/email/__init__.py)"
  - id: src-framework-procedures-py
    resource: src/py/vertebra/framework/procedures.py
    title: "Python implementation module (src/py/vertebra/framework/procedures.py)"
  - id: src-extensions-litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)"
  - id: src-vertebra-http-src-discovery-rs
    resource: src/rs/vertebra-http/src/discovery.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/discovery.rs)"
---

# Framework Subdomains

<!-- truth: start -->
- `vertebra.framework` exposes lazy-loaded built-in capability modules (`di`, `security`, `autowire`, `procedures`, `htmx`, `inertia`, `ssr`, `openapi`, `asyncapi`, `queues`, `channels`, `stores`, `email`, `live`, `mcp`, `a2a`) without mandatory third-party dependencies.
- `KeyValueStore` backends enforce validated `key_prefix` namespaces with cursor-iterated `SCAN` + `UNLINK` on `delete_all()` and millisecond `px` expiries.
<!-- truth: end -->

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py] exports 14 cohesive capability modules and subpackages (12 covered below; Inertia v3 is in [Python Framework, Routing & Serialization Domain](routing-and-app.md) and Live State is in [Live State Synchronization](live-state.md)).

## 1. DI, Security, Autowiring & Typed Procedures

| Subdomain | Key Symbols | Responsibility |
|---|---|---|
| [`di.py`](../../../../../src/py/vertebra/framework/di.py)[^src-framework-di-py] | `Provide`, `Inject`, `ParamBinding`, `RequestScope`, `DependencyGraph`, `Container`, `Scope` | Hierarchical, type- and key-indexed dependency injection (`singleton`, `request`, `transient` scopes) with generator cleanup (`ExitStack` / `AsyncExitStack`), topological resolution, and cycle detection. |
| [`security.py`](../../../../../src/py/vertebra/framework/security.py)[^src-framework-security-py] | `SecurityConfig`, `SecurityContext`, `Principal`, `Guard`, `CsrfConfig`, `RateLimitConfig` | Declarative guards (`requires_role`, `requires_scope`, `requires_tenant`, `requires_capability`), CSRF signing, and rate limiting. |
| [`autowire.py`](../../../../../src/py/vertebra/framework/autowire.py)[^src-framework-autowire-py] | `AutowireConfig`, `AutowireLoader` | Convention-based package scanning (`discover_controllers`, `discover_services`, `register_all`) that discovers and registers controllers, routers, and services. |
| [`procedures.py`](../../../../../src/py/vertebra/framework/procedures.py)[^src-framework-procedures-py] | `ProcedureRouter`, `Procedure`, `ProcedureKind`, `query`, `mutation`, `subscription` | RPC-style typed procedures with DI and guards bridging directly into TypeScript client codegen (`build/codegen/procedures.py`). |

## 2. Frontend Rendering & API Specifications (`htmx`, `ssr`, `openapi`, `asyncapi`)

- **[`htmx.py`](../../../../../src/py/vertebra/framework/htmx.py)[^src-framework-htmx-py]**: `HTMXRequest` header parser (`HX-Request`, `HX-Target`, `HX-Trigger`) and `HTMXResponse` / `HxSwap` response header builders.
- **[`ssr`](../../../../../src/py/vertebra/framework/ssr/__init__.py)[^src-framework-ssr-init-py]**: `SSRConfig`, `SSRFrame`, `SSRResult`, `SSRRenderTimeoutError`, `create_sync_ssr_client`, and `create_async_ssr_client` selecting between `SyncEmbeddedSSRClient` / `AsyncEmbeddedSSRClient` (`vertebra._bundler.SSRPool`) and `SyncSidecarSSRClient` / `AsyncSidecarSSRClient`, plus `render_page_shell`.
- **[`openapi.py`](../../../../../src/py/vertebra/framework/openapi.py)[^src-framework-openapi-py] & [`asyncapi.py`](../../../../../src/py/vertebra/framework/asyncapi.py)[^src-framework-asyncapi-py]**: `OpenAPIConfig`, `OpenAPISpecBuilder`, and `ScalarDocsConfig` for OpenAPI 3.1.0 + Scalar UI; `AsyncAPIConfig`, `AsyncAPISpecBuilder`, and `AsyncChannelDef` for AsyncAPI 3.0.0 WebSocket/SSE/channel schemas.

## 3. Agent Protocols (`litestar-mcp`) & Background Infrastructure (`queues`, `channels`, `stores`, `email`)

- **First-Party `litestar-mcp` (`LitestarMCP`, `LitestarA2A` via `vertebra[mcp]`)**: Model Context Protocol (MCP) and Agent-to-Agent (A2A) endpoints are provided by the official first-party `litestar-mcp` package (`LitestarMCP`, `MCPConfig`, `LitestarA2A`, `A2AConfig`) discovered by [`VertebraPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] (`mcp_endpoint`, `a2a_endpoint`) and paired with Rust's [`WellKnownDiscoveryCache`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs] (`/.well-known/agent.json`, `/.well-known/mcp.json`).
- **[`queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`channels.py`](../../../../../src/py/vertebra/framework/channels.py)[^src-framework-channels-py], [`stores.py`](../../../../../src/py/vertebra/framework/stores.py)[^src-framework-stores-py], [`email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py]**: `QueueService`, `@task`, `TaskLane` (`CPU`/`IO`/`AGENT`), `TaskRetryPolicy`, and `CronSchedule`; `ChannelsBackend`; `KeyValueStore` and `NamespacedStore` (`key_prefix`-scoped `delete_all`, `PX` TTLs); `EmailService`, `EmailMessage`, and `EmailConfig` (`memory`, `smtp`, `resend`, `sendgrid`, `mailgun`, `ses`).

[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-framework-di-py]: `src/py/vertebra/framework/di.py` — Python implementation module (src/py/vertebra/framework/di.py).
[^src-framework-security-py]: `src/py/vertebra/framework/security.py` — Python implementation module (src/py/vertebra/framework/security.py).
[^src-framework-autowire-py]: `src/py/vertebra/framework/autowire.py` — Python implementation module (src/py/vertebra/framework/autowire.py).
[^src-framework-htmx-py]: `src/py/vertebra/framework/htmx.py` — Python implementation module (src/py/vertebra/framework/htmx.py).
[^src-framework-ssr-init-py]: `src/py/vertebra/framework/ssr/__init__.py` — Python implementation module (src/py/vertebra/framework/ssr/__init__.py).
[^src-framework-openapi-py]: `src/py/vertebra/framework/openapi.py` — Python implementation module (src/py/vertebra/framework/openapi.py).
[^src-framework-asyncapi-py]: `src/py/vertebra/framework/asyncapi.py` — Python implementation module (src/py/vertebra/framework/asyncapi.py).
[^src-framework-queues-init-py]: `src/py/vertebra/framework/queues/__init__.py` — Python implementation module (src/py/vertebra/framework/queues/__init__.py).
[^src-framework-channels-py]: `src/py/vertebra/framework/channels.py` — Python implementation module (src/py/vertebra/framework/channels.py).
[^src-framework-stores-py]: `src/py/vertebra/framework/stores.py` — Python implementation module (src/py/vertebra/framework/stores.py).
[^src-framework-email-init-py]: `src/py/vertebra/framework/email/__init__.py` — Python implementation module (src/py/vertebra/framework/email/__init__.py).
[^src-framework-procedures-py]: `src/py/vertebra/framework/procedures.py` — Python implementation module (src/py/vertebra/framework/procedures.py).
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Python implementation module (src/py/vertebra/extensions/litestar/plugin.py).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
