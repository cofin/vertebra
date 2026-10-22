---
type: Concept
title: "Framework Subdomains"
description: "Built-in vertebra.framework capability modules: DI, security, autowire, HTMX, SSR, OpenAPI, AsyncAPI, MCP, A2A, queues, channels, stores, email, and procedures."
status: stable
tags: [domains, framework, subdomains, security, mcp]
sources:
  - resource: src/py/vertebra/framework/__init__.py
  - resource: src/py/vertebra/framework/di/__init__.py
  - resource: src/py/vertebra/framework/security/__init__.py
  - resource: src/py/vertebra/framework/autowire/__init__.py
  - resource: src/py/vertebra/framework/htmx/__init__.py
  - resource: src/py/vertebra/framework/ssr/__init__.py
  - resource: src/py/vertebra/framework/openapi/__init__.py
  - resource: src/py/vertebra/framework/asyncapi/__init__.py
  - resource: src/py/vertebra/framework/mcp/__init__.py
  - resource: src/py/vertebra/framework/a2a/__init__.py
  - resource: src/py/vertebra/framework/queues/__init__.py
  - resource: src/py/vertebra/framework/channels/__init__.py
  - resource: src/py/vertebra/framework/stores/__init__.py
  - resource: src/py/vertebra/framework/email/__init__.py
  - resource: src/py/vertebra/framework/procedures/__init__.py
  - resource: src/rs/vertebra-http/src/discovery.rs
  - resource: src/rs/vertebra-http/src/mcp_bridge.rs
---

# Framework Subdomains

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py) exports 16 cohesive capability subpackages (14 covered below; Inertia v3 is in [Python Framework, Routing & Serialization Domain](routing-and-app.md) and Live State is in [Live State Synchronization](live-state.md)).

## 1. DI, Security, Autowiring & Typed Procedures

| Subdomain | Key Symbols | Responsibility |
|---|---|---|
| [`di`](../../../../../src/py/vertebra/framework/di/__init__.py) | `Provide`, `DependencyGraph`, `Container`, `Scope` | DAG-validated dependency injection (`SINGLETON`, `REQUEST`, `TASK` scopes) with topological resolution and cycle detection. |
| [`security`](../../../../../src/py/vertebra/framework/security/__init__.py) | `SecurityConfig`, `SecurityContext`, `Principal`, `Guard`, `CsrfConfig`, `RateLimitConfig` | Declarative guards (`requires_role`, `requires_scope`, `requires_tenant`, `requires_capability`), CSRF signing, and rate limiting. |
| [`autowire`](../../../../../src/py/vertebra/framework/autowire/__init__.py) | `AutowireConfig`, `AutowireLoader` | Convention-based package scanning that discovers and registers routers, procedures, tasks, and plugins. |
| [`procedures`](../../../../../src/py/vertebra/framework/procedures/__init__.py) | `ProcedureRouter`, `ProcedureDef`, `ProcedureKind`, `query`, `mutation`, `subscription` | RPC-style typed procedures bridging directly into TypeScript client codegen (`build/codegen/procedures.py`). |

## 2. Frontend Rendering & API Specifications (`htmx`, `ssr`, `openapi`, `asyncapi`)

- **[`htmx`](../../../../../src/py/vertebra/framework/htmx/__init__.py)**: `HtmxRequest` header parser (`HX-Request`, `HX-Target`, `HX-Trigger`) and `HtmxResponse` / `HxSwap` response header builders.
- **[`ssr`](../../../../../src/py/vertebra/framework/ssr/__init__.py)**: `SsrConfig`, `SsrFrame`, and `create_ssr_client` selecting between `NativeSsrClient` (`vertebra._bundler.SsrPool`) and `SidecarSsrClient`, plus static route prerendering (`_ssr/_prerender.py`).
- **[`openapi`](../../../../../src/py/vertebra/framework/openapi/__init__.py) & [`asyncapi`](../../../../../src/py/vertebra/framework/asyncapi/__init__.py)**: `OpenAPIConfig`, `OpenAPISpecBuilder`, and `ScalarDocsConfig` for OpenAPI 3.1.0 + Scalar UI; `AsyncAPIConfig`, `AsyncAPISpecBuilder`, and `AsyncChannelDef` for AsyncAPI 3.0.0 WebSocket/SSE/channel schemas.

## 3. Agent Protocols & Background Infrastructure (`mcp`, `a2a`, `queues`, `channels`, `stores`, `email`)

- **[`mcp`](../../../../../src/py/vertebra/framework/mcp/__init__.py) & [`a2a`](../../../../../src/py/vertebra/framework/a2a/__init__.py)**: `McpServer`, `@mcp_tool`, `@mcp_resource`, and `@mcp_prompt` paired with Rust's [`McpStdioBridge`](../../../../../src/rs/vertebra-http/src/mcp_bridge.rs) (`vtb mcp bridge`); `A2AServer`, `AgentCard`, `AgentSkill`, and `A2ATaskState` paired with Rust's [`WellKnownDiscoveryCache`](../../../../../src/rs/vertebra-http/src/discovery.rs) (`/.well-known/agent.json`, `/.well-known/mcp.json`).
- **[`queues`](../../../../../src/py/vertebra/framework/queues/__init__.py), [`channels`](../../../../../src/py/vertebra/framework/channels/__init__.py), [`stores`](../../../../../src/py/vertebra/framework/stores/__init__.py), [`email`](../../../../../src/py/vertebra/framework/email/__init__.py)**: `QueueService`, `@task`, `TaskLane` (`CPU`/`IO`/`AGENT`), `TaskRetryPolicy`, and `CronSchedule`; `ChannelsBackend`; `MemoryStore` and `NamespacedStore`; `EmailService`, `EmailMessage`, and `EmailConfig` (`memory`, `smtp`, `resend`, `sendgrid`, `mailgun`, `ses`).
