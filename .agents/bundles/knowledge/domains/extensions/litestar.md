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

[`vertebra.extensions.litestar`](../../../../../src/py/vertebra/extensions/litestar/__init__.py) bridges Litestar v2 and v3 applications into Vertebra's Rust HTTP router, ingress middleware offload layer, direct-dispatch worker fast path, pub/sub channels, response-cache store, and first-party `litestar-mcp` (`LitestarMCP`, `LitestarA2A`) plugins.

## 1. Plugin, Config & Finalization Lifecycle (`plugin.py`, `config.py`)

- **[`LitestarPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`LitestarConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, registers `app_config.state["vertebra_plugin"] = self`, clears `app_config.compression_config = None` strictly when running under an active Vertebra Rust ingress (`VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1`) and `offload_litestar_middlewares` confirms a supported Rust compression codec (`zstd`, `gzip`, `brotli`), and discovers ecosystem plugins (`LitestarMCP` / `MCPConfig` -> `mcp_endpoint`, `LitestarA2A` / `A2AConfig` -> `a2a_endpoint`, Vite, OpenAPI/Scalar, SQLSpec, Advanced Alchemy, and SAQ/worker lanes) directly into `app_config.state["vertebra_ecosystem_bridges"]`.
- **[`LitestarPlugin.ensure_finalized`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Idempotent post-`Litestar.__init__` hook invoked by `enter_litestar_server_lifespan`, `AsgiRunner`, `LitestarFastPathBypass.dispatch`, and `vertebra-http`'s `extract_offload_json_from_app` to swap in `ChannelsBackend` (`swap_litestar_channels_backend`) and `MemoryStore` (`swap_litestar_response_cache_store`), compile the finalized Litestar route table, extract `LitestarOffloadPlan` into `VERTEBRA_OFFLOAD_CONFIG`, compile `LitestarFastPathBypass`, and bind `app.state` (preserving non-`dict` `State` mapping wrappers across Litestar v2 and v3).

## 2. Route Compiler & Middleware Offload (`router.py`, `offload.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`LitestarRouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py) / [`LitestarRouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py) | `vertebra.extensions.litestar.router` | Introspects Litestar routes into [`ExtractedLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/router.py) entries (`LitestarRouteCompiler.compile`, `LitestarRouteCompiler.extract_entries`, `extract_litestar_routes`, `extract_litestar_route_entries`, supporting both Litestar v2 in-place `resolve_opts() -> None` mutation and v3 attribute resolution) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`normalize_litestar_path_for_matchit`, `convert_litestar_path_params`). |
| [`offload_litestar_middlewares`](../../../../../src/py/vertebra/extensions/litestar/offload.py) / [`LitestarOffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py) | `vertebra.extensions.litestar.offload` | Extracts Litestar `CORSConfig`, `CompressionConfig` (reading Litestar v3 `zstd_compress_level` and v2 `zstd_level`, rejecting unsupported backends so Python compression stays intact), static mounts, and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`to_server_config`, `to_server_config_json` consumed by `ServerConfig::apply_offload_json`). |

## 3. Fused Fast Path, Channels & Store (`fastpath.py`, `channels.py`, `store.py`)

- **[`LitestarFastPathBypass`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) & [`CompiledLitestarRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, kw_only=True)` route table compiled by `compile_litestar_fastpath_routes`. `LitestarFastPathBypass.dispatch(scope, receive, send)` executes zero-`Request` `_DirectSignaturePlan` handlers directly and routes DI/DTO/`File`/`ServerSentEvent` handlers through [`dispatch_litestar_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) (`preseed_litestar_scope_body` guarding `Empty`, `http.response.start` emitted before `http.response.pathsend` and `http.response.body` SSE chunks, JSON-serialized `HTTPException`, and guaranteed `UploadFile.close()` in `finally`). Routes with guards, custom ASGI middleware, `before_request`, `after_request` (including Litestar v2 `ownership_layers`), `before_send`, `after_exception`, custom `exception_handlers`, caching, `csrf_config`, `allowed_hosts`, active un-offloaded Python `compression_config` or `cors_config`, layered `type_decoders` or `response_class`, `response_headers`/`cookies`/`etag`/`background`, or bodies exceeding `request_max_body_size` fall back cleanly to Litestar's full ASGI stack (replaying any consumed `receive()` chunks).
- **[`ChannelsBackend`](../../../../../src/py/vertebra/extensions/litestar/channels.py)** (`swap_litestar_channels_backend`): Native in-memory/SHM-compatible `ChannelBus` backend for Litestar `ChannelsPlugin` supporting `publish`, `subscribe`, `unsubscribe`, `stream_events`, and bounded history.
- **[`MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)** (`swap_litestar_response_cache_store`): Monotonic-clock TTL `ResponseCacheStore` (`get`, `set`, `delete`, `delete_all`, `exists`, `expires_in`) replacing Litestar's default response-cache store.
