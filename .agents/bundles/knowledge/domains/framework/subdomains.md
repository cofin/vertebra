---
type: Concept
title: "Framework Subdomains"
description: "Built-in vertebra.framework capability modules: DI, security, autowire, HTMX, SSR, OpenAPI, AsyncAPI, queues, channels, stores, email, procedures, and first-party litestar-mcp."
status: stable
tags: [domains, framework, subdomains, security, mcp]
sources:
  - resource: src/py/vertebra/framework/__init__.py
  - resource: src/py/vertebra/framework/di.py
  - resource: src/py/vertebra/framework/security.py
  - resource: src/py/vertebra/framework/autowire.py
  - resource: src/py/vertebra/framework/htmx.py
  - resource: src/py/vertebra/framework/ssr/__init__.py
  - resource: src/py/vertebra/framework/openapi.py
  - resource: src/py/vertebra/framework/asyncapi.py
  - resource: src/py/vertebra/framework/queues/__init__.py
  - resource: src/py/vertebra/framework/channels.py
  - resource: src/py/vertebra/framework/stores.py
  - resource: src/py/vertebra/framework/email/__init__.py
  - resource: src/py/vertebra/framework/procedures.py
  - resource: src/py/vertebra/extensions/litestar/plugin.py
  - resource: src/rs/vertebra-http/src/discovery.rs
---

# Framework Subdomains

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py) exports 14 cohesive capability modules and subpackages (12 covered below; Inertia v3 is in [Python Framework, Routing & Serialization Domain](routing-and-app.md) and Live State is in [Live State Synchronization](live-state.md)).

## 1. DI, Security, Autowiring & Typed Procedures

| Subdomain | Key Symbols | Responsibility |
|---|---|---|
| [`di.py`](../../../../../src/py/vertebra/framework/di.py) | `Provide`, `Inject`, `ParamBinding`, `RequestScope`, `DependencyGraph`, `Container`, `Scope` | Hierarchical, type- and key-indexed dependency injection (`singleton`, `request`, `transient` scopes) with generator cleanup (`ExitStack` / `AsyncExitStack`), topological resolution, and cycle detection. |
| [`security.py`](../../../../../src/py/vertebra/framework/security.py) | `SecurityConfig`, `SecurityContext`, `Principal`, `Guard`, `CsrfConfig`, `RateLimitConfig` | Declarative guards (`requires_role`, `requires_scope`, `requires_tenant`, `requires_capability`), CSRF signing, and rate limiting. |
| [`autowire.py`](../../../../../src/py/vertebra/framework/autowire.py) | `AutowireConfig`, `AutowireLoader` | Convention-based package scanning (`discover_controllers`, `discover_services`, `register_all`) that discovers and registers controllers, routers, and services. |
| [`procedures.py`](../../../../../src/py/vertebra/framework/procedures.py) | `ProcedureRouter`, `Procedure`, `ProcedureKind`, `query`, `mutation`, `subscription` | RPC-style typed procedures with DI and guards bridging directly into TypeScript client codegen (`build/codegen/procedures.py`). |

## 2. Frontend Rendering & API Specifications (`htmx`, `ssr`, `openapi`, `asyncapi`)

- **[`htmx.py`](../../../../../src/py/vertebra/framework/htmx.py)**: `HtmxRequest` header parser (`HX-Request`, `HX-Target`, `HX-Trigger`) and `HtmxResponse` / `HxSwap` response header builders.
- **[`ssr`](../../../../../src/py/vertebra/framework/ssr/__init__.py)**: `SsrConfig`, `SsrFrame`, and `create_ssr_client` selecting between `EmbeddedSsrClient` (`vertebra._bundler.SsrPool`) and `SidecarSsrClient`, plus static route prerendering (`ssr/_prerender.py`).
- **[`openapi.py`](../../../../../src/py/vertebra/framework/openapi.py) & [`asyncapi.py`](../../../../../src/py/vertebra/framework/asyncapi.py)**: `OpenAPIConfig`, `OpenAPISpecBuilder`, and `ScalarDocsConfig` for OpenAPI 3.1.0 + Scalar UI; `AsyncAPIConfig`, `AsyncAPISpecBuilder`, and `AsyncChannelDef` for AsyncAPI 3.0.0 WebSocket/SSE/channel schemas.

## 3. Agent Protocols (`litestar-mcp`) & Background Infrastructure (`queues`, `channels`, `stores`, `email`)

- **First-Party `litestar-mcp` (`LitestarMCP`, `LitestarA2A` via `vertebra[mcp]`)**: Model Context Protocol (MCP) and Agent-to-Agent (A2A) endpoints are provided by the official first-party `litestar-mcp` package (`LitestarMCP`, `MCPConfig`, `LitestarA2A`, `A2AConfig`) discovered by [`LitestarPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) (`mcp_endpoint`, `a2a_endpoint`) and paired with Rust's [`WellKnownDiscoveryCache`](../../../../../src/rs/vertebra-http/src/discovery.rs) (`/.well-known/agent.json`, `/.well-known/mcp.json`).
- **[`queues`](../../../../../src/py/vertebra/framework/queues/__init__.py), [`channels.py`](../../../../../src/py/vertebra/framework/channels.py), [`stores.py`](../../../../../src/py/vertebra/framework/stores.py), [`email`](../../../../../src/py/vertebra/framework/email/__init__.py)**: `QueueService`, `@task`, `TaskLane` (`CPU`/`IO`/`AGENT`), `TaskRetryPolicy`, and `CronSchedule`; `ChannelsBackend`; `MemoryStore` and `NamespacedStore`; `EmailService`, `EmailMessage`, and `EmailConfig` (`memory`, `smtp`, `resend`, `sendgrid`, `mailgun`, `ses`).
