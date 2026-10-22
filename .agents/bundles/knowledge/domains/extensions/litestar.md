---
type: Concept
title: "Litestar Integration: Route Compiler, Middleware Offload, and Fast Path"
description: "LitestarPlugin, matchit route table extraction, Rust middleware offload specs, fused direct-dispatch fast path, ChannelsBackend, and MemoryStore."
status: stable
tags: [domains, extensions, litestar, asgi, fastpath]
sources:
  - resource: src/py/vertebra/extensions/litestar/__init__.py
  - resource: src/py/vertebra/extensions/litestar/config.py
  - resource: src/py/vertebra/extensions/litestar/plugin.py
  - resource: src/py/vertebra/extensions/litestar/router.py
  - resource: src/py/vertebra/extensions/litestar/offload.py
  - resource: src/py/vertebra/extensions/litestar/fastpath.py
  - resource: src/py/vertebra/extensions/litestar/channels.py
  - resource: src/py/vertebra/extensions/litestar/store.py
---

# Litestar Integration: Route Compiler, Middleware Offload, and Fast Path

[`vertebra.extensions.litestar`](../../../../../src/py/vertebra/extensions/litestar/__init__.py) bridges Litestar applications into Vertebra's Rust HTTP router, middleware offload layer, direct-dispatch worker fast path, pub/sub channels, response-cache store, and first-party `litestar-mcp` (`LitestarMCP`, `LitestarA2A`) plugins.

## 1. Plugin, Config & Ecosystem Integration (`plugin.py`, `config.py`)

- **[`LitestarPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`LitestarConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, compiles the Litestar route table, extracts middleware offload specs, compiles fast-path routes, swaps in `ChannelsBackend` and `MemoryStore` when enabled, and discovers ecosystem plugins (`LitestarMCP` / `MCPConfig` -> `mcp_endpoint`, `LitestarA2A` / `A2AConfig` -> `a2a_endpoint`, Vite, OpenAPI/Scalar, SQLSpec, Advanced Alchemy, and SAQ/worker lanes) directly into `app_config.state["vertebra_ecosystem_bridges"]`.
- **[`LitestarPlugin.enter_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Context manager entering Litestar's `CLIPlugin` server lifespan hooks around worker execution.

## 2. Route Compiler & Middleware Offload (`router.py`, `offload.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`LitestarRouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py) / [`LitestarRouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py) | `vertebra.extensions.litestar.router` | Introspects Litestar routes into [`ExtractedLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/router.py) entries (`LitestarRouteCompiler.extract_routes`, `LitestarRouteCompiler.extract_route_entries`) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`LitestarRouteCompiler.normalize_path_for_matchit`, `LitestarRouteCompiler.convert_path_params`). |
| [`MiddlewareOffloadCompiler`](../../../../../src/py/vertebra/extensions/litestar/offload.py) / [`LitestarOffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py) | `vertebra.extensions.litestar.offload` | Extracts Litestar `CORSConfig`, `CompressionConfig`, static files, and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`MiddlewareOffloadCompiler.extract_offload_plan`, `evaluate_cors_preflight`, `evaluate_cors_response_headers`, `evaluate_openapi_cache_request`). |

## 3. Fused Fast Path, Channels & Store (`fastpath.py`, `channels.py`, `store.py`)

- **[`CompiledLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, array_like=True)` produced by `CompiledLitestarRoute.compile_routes` for routes without custom ASGI middleware stacks. [`CompiledLitestarRoute.dispatch_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) and `CompiledLitestarRoute.preseed_scope_body` bypass full ASGI middleware traversal when eligible.
- **[`ChannelsBackend`](../../../../../src/py/vertebra/extensions/litestar/channels.py)** (`ChannelsBackend.install_into_app_config`): Native in-memory/SHM-compatible backend for Litestar `ChannelsPlugin` supporting `publish`, `subscribe`, `unsubscribe`, `stream_events`, and bounded history.
- **[`MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)** (`MemoryStore.install_into_app_config`): Monotonic-clock TTL key-value store (`get`, `set`, `delete`, `delete_all`, `exists`, `expires_in`) replacing Litestar's default response-cache store.
