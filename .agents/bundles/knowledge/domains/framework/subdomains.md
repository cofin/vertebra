---
type: Concept
title: "Framework Subdomains"
description: "Built-in vertebra.framework capability subdomains (DI, security, autowire, procedures, HTMX, Inertia, SSR, OpenAPI, AsyncAPI, queues, channels, stores, email, and live state) alongside native vertebra.agent.{mcp,a2a} and Litestar MCP integration."
status: stable
tags:
  - domains
  - framework
  - subdomains
  - security
  - mcp
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
last_reviewed_commit: 5a833832
verified:
  - by: agent:flow-executor
    at: "2026-11-17T02:30:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
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
  - id: src-framework-vite-py
    resource: src/py/vertebra/framework/vite.py
    title: "Framework-neutral Vite asset manifest and HMR tag helpers (src/py/vertebra/framework/vite.py)"
  - id: src-framework-storage-py
    resource: src/py/vertebra/framework/storage.py
    title: "Framework-neutral SyncStorageRegistry and AsyncStorageRegistry (src/py/vertebra/framework/storage.py)"
  - id: src-framework-queues-asgi-py
    resource: src/py/vertebra/framework/queues/_asgi.py
    title: "Framework-neutral ASGI queue lifespan and service helpers (src/py/vertebra/framework/queues/_asgi.py)"
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
  - id: src-cache-init-py
    resource: src/py/vertebra/cache/__init__.py
    title: "Framework-neutral cache store package (src/py/vertebra/cache/__init__.py)"
  - id: src-framework-email-init-py
    resource: src/py/vertebra/framework/email/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/email/__init__.py)"
  - id: src-framework-procedures-py
    resource: src/py/vertebra/framework/procedures.py
    title: "Python implementation module (src/py/vertebra/framework/procedures.py)"
  - id: src-agent-init-py
    resource: src/py/vertebra/agent/__init__.py
    title: "Python implementation module (src/py/vertebra/agent/__init__.py)"
  - id: src-extensions-litestar-mcp-init-py
    resource: src/py/vertebra/extensions/litestar/mcp/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/mcp/__init__.py)"
  - id: src-extensions-litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)"
  - id: src-vertebra-http-src-discovery-rs
    resource: src/rs/vertebra-http/src/discovery.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/discovery.rs)"
---

# Framework Subdomains

<!-- truth: start -->
- `vertebra.framework` exposes exactly 14 lazy-loaded built-in capability subdomains (`asyncapi`, `autowire`, `channels`, `di`, `email`, `htmx`, `inertia`, `live`, `openapi`, `procedures`, `queues`, `security`, `ssr`, `stores`) without mandatory third-party dependencies.
- Native MCP (`2026-07-28`) and A2A (`1.0`) implementations live in `vertebra.agent.mcp` and `vertebra.agent.a2a`, with optional Litestar integration in `vertebra.extensions.litestar.mcp` (`litestar-mcp`).
- `KeyValueStore` backends enforce validated `key_prefix` namespaces with cursor-iterated `SCAN` + `UNLINK` on `delete_all()` and millisecond `px` expiries.
- `vertebra.cache` (`SyncCacheStore`, `AsyncCacheStore`, `CacheStats`) provides framework-neutral `bytes` caching across `memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, and object-store URLs with zero ORM or extension imports.
<!-- truth: end -->

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py] exports 14 cohesive capability modules and subpackages (12 covered below; Inertia v3 is in [Python Framework, Routing & Serialization Domain](routing-and-app.md) and Live State is in [Live State Synchronization](live-state.md)), plus neutral host-extension helpers ([`vite.py`](../../../../../src/py/vertebra/framework/vite.py)[^src-framework-vite-py], [`storage.py`](../../../../../src/py/vertebra/framework/storage.py)[^src-framework-storage-py], [`queues/_asgi.py`](../../../../../src/py/vertebra/framework/queues/_asgi.py)[^src-framework-queues-asgi-py]). See [Framework Extension Limitations](framework-extension-limitations.md) for optional host and SHM caveats.

## 1. DI, Security, Autowiring & Typed Procedures

| Subdomain | Key Symbols | Responsibility |
|---|---|---|
| [`di.py`](../../../../../src/py/vertebra/framework/di.py)[^src-framework-di-py] | `Provide`, `Inject`, `ParamBinding`, `RequestScope`, `DependencyGraph`, `Container`, `Scope` | Hierarchical, type- and key-indexed dependency injection (`singleton`, `request`, `transient` scopes) with generator cleanup (`ExitStack` / `AsyncExitStack`), topological resolution, and cycle detection. |
| [`security.py`](../../../../../src/py/vertebra/framework/security.py)[^src-framework-security-py] | `SecurityConfig`, `SecurityContext`, `Principal`, `AuthenticatedPrincipal`, `Guard`, `JWTAuthGuard`, `JWKSConfig`, `JWKSVerifier`, `CsrfConfig`, `RateLimitConfig`, `RateLimitGuard` | Declarative guards (`requires_role`, `requires_scope`, `requires_tenant`, `requires_capability`), CSRF signing, token-bucket rate limiting, HS256 JWT verification, and cached `JWKSVerifier` inbound Bearer verification with single-flight `kid` key rotation (`alg != "none"`, `iss`/`aud`/`exp`). |
| [`autowire.py`](../../../../../src/py/vertebra/framework/autowire.py)[^src-framework-autowire-py] | `AutowireConfig`, `AutowireLoader` | Convention-based package scanning (`discover_controllers`, `discover_services`, `register_all`) that discovers and registers controllers, routers, and services. |
| [`procedures.py`](../../../../../src/py/vertebra/framework/procedures.py)[^src-framework-procedures-py] | `ProcedureRouter`, `Procedure`, `ProcedureKind`, `query`, `mutation`, `subscription` | RPC-style typed procedures with DI and guards bridging directly into TypeScript client codegen (`build/codegen/procedures.py`). |

## 2. Frontend Rendering & API Specifications (`htmx`, `ssr`, `openapi`, `asyncapi`, `vite`)

- **[`htmx.py`](../../../../../src/py/vertebra/framework/htmx.py)[^src-framework-htmx-py]**: `HTMXRequest` header parser (`HX-Request`, `HX-Target`, `HX-Trigger`) and `HTMXResponse` / `HxSwap` response header builders.
- **[`ssr`](../../../../../src/py/vertebra/framework/ssr/__init__.py)[^src-framework-ssr-init-py]**: `SSRConfig`, `SSRFrame`, `SSRResult`, `SSRRenderTimeoutError`, `create_sync_ssr_client`, and `create_async_ssr_client` selecting between `SyncEmbeddedSSRClient` / `AsyncEmbeddedSSRClient` (`vertebra._bundler.SSRPool`) and `SyncSidecarSSRClient` / `AsyncSidecarSSRClient`, plus `render_page_shell`.
- **[`vite.py`](../../../../../src/py/vertebra/framework/vite.py)[^src-framework-vite-py] & [`storage.py`](../../../../../src/py/vertebra/framework/storage.py)[^src-framework-storage-py]**: `ViteConfig`, `ViteBridge`, `vite_asset`, `vite_hmr_client`, and `get_vertebra_client_vite_aliases` resolve Vite manifests and HMR tags across host frameworks; `SyncStorageRegistry` and `AsyncStorageRegistry` manage named object stores.
- **[`openapi.py`](../../../../../src/py/vertebra/framework/openapi.py)[^src-framework-openapi-py] & [`asyncapi.py`](../../../../../src/py/vertebra/framework/asyncapi.py)[^src-framework-asyncapi-py]**: `OpenAPIConfig`, `OpenAPISpecBuilder`, `ScalarDocsConfig`, `SwaggerDocsConfig`, `RedocDocsConfig`, `ElementsDocsConfig`, `RapiDocDocsConfig`, and `AsyncAPIDocsConfig` for OpenAPI 3.1.0 + multi-UI docs portals; `AsyncAPIConfig`, `AsyncAPISpecBuilder`, and `AsyncChannelDef` for AsyncAPI 3.0.0 WebSocket/SSE/channel schemas.

## 3. Agent Protocols (`vertebra.agent`, `litestar-mcp`) & Background Infrastructure (`queues`, `channels`, `stores`, `email`)

- **Native `vertebra.agent.{mcp,a2a}` & Optional Litestar Integration (`litestar-mcp`)**: `vertebra.framework` does not export `mcp` or `a2a` subdomains; native MCP (`2026-07-28`) and A2A (`1.0`) protocol harnesses live in [`vertebra.agent`](../../../../../src/py/vertebra/agent/__init__.py)[^src-agent-init-py] (`MCPCore`, `ToolRegistry`, `A2AService`, `AgentPlugin`; see [MCP & A2A Protocol Harness](../agent/mcp-and-a2a-protocol-harness.md)). Litestar applications integrate optionally through [`vertebra.extensions.litestar.mcp`](../../../../../src/py/vertebra/extensions/litestar/mcp/__init__.py)[^src-extensions-litestar-mcp-init-py] (`create_mcp_plugin`, `JSONRPCFrameAccelerator`, `ObjectStoreSkillSource`, `WellKnownAgentCardOffload` over `litestar-mcp` `LitestarMCP` / `LitestarA2A`), discovered by [`VertebraPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] (`mcp_endpoint`, `a2a_endpoint`) and paired with Rust's [`WellKnownDiscoveryCache`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs] (`/.well-known/agent-card.json`, `/.well-known/mcp.json`, `/.well-known/oauth-protected-resource`).
- **[`queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`channels.py`](../../../../../src/py/vertebra/framework/channels.py)[^src-framework-channels-py], [`stores.py`](../../../../../src/py/vertebra/framework/stores.py)[^src-framework-stores-py], [`email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py]**: `QueueService`, `@task`, `TaskLane` (`CPU`/`IO`/`AGENT`), `TaskRetryPolicy`, and `CronSchedule`; `ChannelsBackend`; `KeyValueStore` and `NamespacedStore` (`key_prefix`-scoped `delete_all`, `PX` TTLs); `EmailService`, `EmailMessage`, and `EmailConfig` (`memory`, `smtp`, `resend`, `sendgrid`, `mailgun`, `ses`).
- **[`vertebra.cache`](../../../../../src/py/vertebra/cache/__init__.py)[^src-cache-init-py]**: Framework-neutral store package exporting `SyncCacheStore`, `AsyncCacheStore`, and `CacheStats` lazily with zero ORM or extension imports. Stores accept `bytes` values, return `None` on miss, isolate `clear()` and `delete_prefix()` by `key_prefix`, and support `memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, `s3://`, `gs://`, `az://`, `r2://`, `oci://`, and `file://` URLs.

[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-framework-di-py]: `src/py/vertebra/framework/di.py` — Python implementation module (src/py/vertebra/framework/di.py).
[^src-framework-security-py]: `src/py/vertebra/framework/security.py` — Python implementation module (src/py/vertebra/framework/security.py).
[^src-framework-autowire-py]: `src/py/vertebra/framework/autowire.py` — Python implementation module (src/py/vertebra/framework/autowire.py).
[^src-framework-htmx-py]: `src/py/vertebra/framework/htmx.py` — Python implementation module (src/py/vertebra/framework/htmx.py).
[^src-framework-ssr-init-py]: `src/py/vertebra/framework/ssr/__init__.py` — Python implementation module (src/py/vertebra/framework/ssr/__init__.py).
[^src-framework-openapi-py]: `src/py/vertebra/framework/openapi.py` — Python implementation module (src/py/vertebra/framework/openapi.py).
[^src-framework-vite-py]: `src/py/vertebra/framework/vite.py` — Framework-neutral Vite asset manifest and HMR tag helpers.
[^src-framework-storage-py]: `src/py/vertebra/framework/storage.py` — Framework-neutral `SyncStorageRegistry` and `AsyncStorageRegistry`.
[^src-framework-queues-asgi-py]: `src/py/vertebra/framework/queues/_asgi.py` — Framework-neutral ASGI queue lifespan and service helpers.
[^src-framework-asyncapi-py]: `src/py/vertebra/framework/asyncapi.py` — Python implementation module (src/py/vertebra/framework/asyncapi.py).
[^src-framework-queues-init-py]: `src/py/vertebra/framework/queues/__init__.py` — Python implementation module (src/py/vertebra/framework/queues/__init__.py).
[^src-framework-channels-py]: `src/py/vertebra/framework/channels.py` — Python implementation module (src/py/vertebra/framework/channels.py).
[^src-framework-stores-py]: `src/py/vertebra/framework/stores.py` — Python implementation module (src/py/vertebra/framework/stores.py).
[^src-cache-init-py]: `src/py/vertebra/cache/__init__.py` — Framework-neutral cache store package (`SyncCacheStore`, `AsyncCacheStore`, `CacheStats`).
[^src-framework-email-init-py]: `src/py/vertebra/framework/email/__init__.py` — Python implementation module (src/py/vertebra/framework/email/__init__.py).
[^src-framework-procedures-py]: `src/py/vertebra/framework/procedures.py` — Python implementation module (src/py/vertebra/framework/procedures.py).
[^src-agent-init-py]: `src/py/vertebra/agent/__init__.py` — Python implementation module (src/py/vertebra/agent/__init__.py).
[^src-extensions-litestar-mcp-init-py]: `src/py/vertebra/extensions/litestar/mcp/__init__.py` — Python implementation module (src/py/vertebra/extensions/litestar/mcp/__init__.py).
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Python implementation module (src/py/vertebra/extensions/litestar/plugin.py).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
