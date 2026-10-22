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
  - resource: src/py/vertebra/extensions/litestar/ecosystem.py
---

# Litestar Integration: Route Compiler, Middleware Offload, and Fast Path

[`vertebra.extensions.litestar`](../../../../../src/py/vertebra/extensions/litestar/__init__.py) bridges Litestar applications into Vertebra's Rust HTTP router, middleware offload layer, direct-dispatch worker fast path, pub/sub channels, and response-cache store.

## 1. Plugin, Config & Ecosystem Bridge (`plugin.py`, `config.py`, `ecosystem.py`)

- **[`LitestarPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`LitestarConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, compiles the Litestar route table, extracts middleware offload specs, compiles fast-path routes, swaps in `ChannelsBackend` and `MemoryStore` when enabled, and attaches [`LitestarEcosystemBridge`](../../../../../src/py/vertebra/extensions/litestar/ecosystem.py) (`attach_litestar_ecosystem_bridges` for Vite, OpenAPI/Scalar, SQLSpec, Advanced Alchemy, and SAQ/worker lanes).
- **[`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Context manager entering Litestar's `CLIPlugin` server lifespan hooks around worker execution.

## 2. Route Compiler & Middleware Offload (`router.py`, `offload.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`LitestarRouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py) / [`LitestarRouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py) | `vertebra.extensions.litestar.router` | Introspects Litestar routes into [`ExtractedLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/router.py) entries (`extract_litestar_routes`, `extract_litestar_route_entries`) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`normalize_litestar_path_for_matchit`, `convert_litestar_path_params`). |
| [`MiddlewareOffloadCompiler`](../../../../../src/py/vertebra/extensions/litestar/offload.py) / [`LitestarOffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py) | `vertebra.extensions.litestar.offload` | Extracts Litestar `CORSConfig`, `CompressionConfig`, static files, and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`offload_litestar_middlewares`, `evaluate_cors_preflight`, `evaluate_cors_response_headers`, `evaluate_openapi_cache_request`). |

## 3. Fused Fast Path, Channels & Store (`fastpath.py`, `channels.py`, `store.py`)

- **[`CompiledLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, array_like=True)` produced by `compile_litestar_fastpath_routes` for routes without custom ASGI middleware stacks. [`dispatch_litestar_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) and `preseed_litestar_scope_body` bypass full ASGI middleware traversal when eligible.
- **[`ChannelsBackend`](../../../../../src/py/vertebra/extensions/litestar/channels.py)** (`swap_litestar_channels_backend`): Native in-memory/SHM-compatible backend for Litestar `ChannelsPlugin` supporting `publish`, `subscribe`, `unsubscribe`, `stream_events`, and bounded history.
- **[`MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)** (`swap_litestar_response_cache_store`): Monotonic-clock TTL key-value store (`get`, `set`, `delete`, `delete_all`, `exists`, `expires_in`) replacing Litestar's default response-cache store.
