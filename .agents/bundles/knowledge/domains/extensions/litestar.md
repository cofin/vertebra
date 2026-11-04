---
type: Concept
title: "Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django"
description: "First-class framework plugins, matchit 0.9 RouteCompiler, Rust middleware offload specs, zero-copy C-API FastPath dispatch, and ecosystem bridges across Litestar, Starlette, FastAPI, Flask, and Django."
status: stable
tags: [domains, extensions, litestar, starlette, fastapi, flask, django, asgi, wsgi, fastpath]
sources:
  - resource: src/py/vertebra/extensions/litestar/__init__.py
  - resource: src/py/vertebra/extensions/litestar/config.py
  - resource: src/py/vertebra/extensions/litestar/plugin.py
  - resource: src/py/vertebra/extensions/litestar/router.py
  - resource: src/py/vertebra/extensions/litestar/offload.py
  - resource: src/py/vertebra/extensions/litestar/fastpath.py
  - resource: src/py/vertebra/extensions/litestar/channels.py
  - resource: src/py/vertebra/extensions/litestar/store.py
  - resource: src/py/vertebra/extensions/starlette/__init__.py
  - resource: src/py/vertebra/extensions/fastapi/__init__.py
  - resource: src/py/vertebra/extensions/flask/__init__.py
  - resource: src/py/vertebra/extensions/django/__init__.py
  - resource: src/py/vertebra/server/_routing.py
  - resource: src/py/vertebra/server/_fastpath.py
---

# Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django

Vertebra provides symmetric, zero-coupling extensions for **Litestar** (`vertebra.extensions.litestar`), **Starlette** (`vertebra.extensions.starlette`), **FastAPI** (`vertebra.extensions.fastapi`), **Flask** (`vertebra.extensions.flask`), and **Django** (`vertebra.extensions.django`). Each extension shares the same canonical domain vocabulary (`RouteCompiler`, `RouterTable`, `RouteEntry`, `CompiledRoute`, `FastPath`, `OffloadPlan`, `offload_middlewares`), routes path converter normalization through [`vertebra.server._routing`](../../../../../src/py/vertebra/server/_routing.py) without importing `vertebra.framework.*`, and uses [`vertebra.server._fastpath`](../../../../../src/py/vertebra/server/_fastpath.py) (`wrap_asgi_fastpath`, `resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, `attach_framework_fastpath`) for zero-closure C-API `send_raw_response` dispatch and automatic `App.mount("/prefix", sub_app)` fastpath stacking across all 6 frameworks.

## 1. Litestar Integration (`vertebra.extensions.litestar`)

- **[`LitestarPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`LitestarConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, registers `app_config.state["vertebra_plugin"] = self`, clears `app_config.compression_config = None` strictly when running under an active Vertebra Rust ingress (`VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1`) and `offload_middlewares` confirms a supported Rust compression codec (`zstd`, `gzip`, `brotli`), and discovers ecosystem plugins (`LitestarMCP` / `MCPConfig` -> `mcp_endpoint`, `LitestarA2A` / `A2AConfig` -> `a2a_endpoint`, Vite, OpenAPI/Scalar, SQLSpec, Advanced Alchemy, and SAQ/worker lanes) directly into `app_config.state["vertebra_ecosystem_bridges"]`.
- **[`LitestarPlugin.ensure_finalized`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Idempotent post-`Litestar.__init__` hook invoked by `enter_litestar_server_lifespan`, `AsgiRunner`, `FastPath.dispatch`, and `vertebra-http`'s `extract_offload_json_from_app` to swap in `ChannelsBackend` (`swap_channels_backend`) and `MemoryStore` (`swap_response_cache_store`) when `LitestarConfig.swap_memory_channels` / `swap_memory_response_cache` are enabled (both default `False`), compile the finalized Litestar route table via `RouteCompiler`, extract `OffloadPlan` into `VERTEBRA_OFFLOAD_CONFIG`, compile `FastPath`, and bind `app.state`.
- **[`RouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py) / [`RouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py)**: Introspects Litestar routes into [`RouteEntry`](../../../../../src/py/vertebra/extensions/litestar/router.py) items (`RouteCompiler.compile`, `RouteCompiler.extract_entries`, `extract_routes`, `extract_route_entries`) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`normalize_path_for_matchit`, `convert_path_params`).
- **[`offload_middlewares`](../../../../../src/py/vertebra/extensions/litestar/offload.py) / [`OffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py)**: Extracts Litestar `CORSConfig`, `CompressionConfig`, static mounts (reading `_static_files_config` without triggering Litestar deprecation warnings), and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`to_server_config`, `to_server_config_json`).
- **[`FastPath`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py) & [`CompiledRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, kw_only=True)` route table compiled by `compile_fastpath_routes`. `_compile_direct_signature_plan` unwraps Litestar 2.x/3.0 `FromPath[T]`, `FromQuery[T]`, and `JSONBody[T]` (`Annotated` markers) into zero-`Request` `_DirectSignaturePlan` plans while falling back to `KwargsModel` for constrained `Parameter(gt=...)` / `FromHeader` / `FromCookie` markers. `FastPath.dispatch` and `FastPath.wrap_asgi` read bodies zero-copy via `AsgiScope.body_bytes()`, emit unary responses in 1 C-API call via `send.send_raw_response`, close unawaited `ASGIFileResponse.file_info` coroutines when intercepting `http.response.pathsend`, and route DI/DTO/`File`/`ServerSentEvent` handlers through [`dispatch_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py).
- **[`ChannelsBackend`](../../../../../src/py/vertebra/extensions/litestar/channels.py)** & **[`MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)**: Native `vertebra.db.channels.ChannelBus` (`Memory`, `SHM`, `Redis`, `Valkey`, `PostgreSQL`, `Oracle`) and monotonic-clock TTL `ResponseCacheStore` implementations with zero dependency on `vertebra.framework.*`. `MemoryStore` prefixes keys with `key_prefix` (default `vertebra`), deletes with `SCAN` + `UNLINK`, sends `PX` TTLs, and returns `None` from `expires_in` for keys without expiry.

## 2. Starlette, FastAPI, Flask & Django Extensions

| Extension Package | Plugin & Config | Route Compiler & FastPath | Ecosystem & Data Bridges |
|---|---|---|---|
| [`vertebra.extensions.starlette`](../../../../../src/py/vertebra/extensions/starlette/__init__.py) | `StarlettePlugin`, `StarletteConfig` | `RouteCompiler` (`Mount`/`Route`/`WebSocketRoute` -> `matchit 0.9`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, inline `sync_to_thread=False` default, `msgspec.Struct`, `pydantic.BaseModel`, `@dataclass`, `@attrs.define`), `offload_middlewares` (`GZipMiddleware`, `CORSMiddleware`, `StaticFiles`) | `OpenAPIConfig` (Scalar, Swagger UI, AsyncAPI 3.0), `MCPConfig` (`@mcp_tool`, `@mcp_resource`, `@mcp_prompt`, `.well-known/mcp.json`), `ViteAssetBridge` |
| [`vertebra.extensions.fastapi`](../../../../../src/py/vertebra/extensions/fastapi/__init__.py) | `FastAPIPlugin`, `FastAPIConfig` | `RouteCompiler` (`APIRoute`), `DependencyPlan` (`compile_dependant_plan` topological DAG with `needs_request` tracking and inline `sync_to_thread=False`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, lazy `starlette.requests.Request` skipped when `needs_request=False`, `decode_pydantic_json` + `__pydantic_serializer__.to_json` / `decode_msgspec` C-API bypass) | `OpenAPIConfig` (Scalar multi-spec, Swagger UI, AsyncAPI 3.0), `MCPConfig`, `ViteAssetBridge` |
| [`vertebra.extensions.flask`](../../../../../src/py/vertebra/extensions/flask/__init__.py) | `FlaskPlugin`, `FlaskConfig` | `RouteCompiler` (Werkzeug `app.url_map.iter_rules()` -> `matchit 0.9`), `FastPath` (supports both sync `def` and `async def` views via `_step_coro_sync`, `dispatch_async`, `wrap_asgi`, `wrap_wsgi`, and `_WsgiStartResponse.send_raw_response` over `WsgiInput.decode_msgspec` / `decode_pydantic_json`), `offload_middlewares` (`Flask-Compress`, `Flask-CORS`, `static_folder`) | `CacheBackend`, `StorageBridge`, `MailBridge`, `ChannelsBridge`, `QueuesBridge`, `SecurityBridge`, `ViteAssetBridge`, `OpenAPIConfig`, `MCPConfig` |
| [`vertebra.extensions.django`](../../../../../src/py/vertebra/extensions/django/__init__.py) | `DjangoPlugin`, `DjangoConfig`, `DjangoAppConfig` | `RouteCompiler` (`URLPattern` / `URLResolver`), `FastPath` (`@vertebra_view`, pre-allocated stateless `MsgspecView` instances, sync `def` and `async def` views across `dispatch`, `dispatch_async`, `dispatch_wsgi` with `send_raw_response`, and `wrap_asgi`), `offload_middlewares` (`GZipMiddleware`, `CorsMiddleware`, `WhiteNoiseMiddleware`, `SecurityMiddleware`), `ORMBridge` (`orm_to_struct_vectorcall`) | Native `vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,mssql,duckdb}` (`DatabaseWrapper` sharing pools and `LeaseBroker` across frameworks via `DatabaseRegistry.default()`, `CloudSQLDialer`, `AlloyDBDialer`, PgBouncer `schema_editor()` session switch), `ArrowQuerySet` (`to_arrow`, `to_schema`, `from_arrow`), `CacheBackend`, `DjangoStorage`, `EmailBackend`, `ChannelLayer`, `QueuesBackend`, `SecurityBridge`, `TemplateBridge`, `ViteBridge` (`{% load vertebra_vite %}`) |

## 3. First-Party Litestar Ecosystem Subpackages (`extensions/litestar/*`)

All `litestar-*` ecosystem integrations are nested strictly under `vertebra.extensions.litestar.*`:

| Subpackage | Canonical Helpers & Adapters | Upstream Integration |
|---|---|---|
| `queues` | `create_shm_queue_config()`, `create_sqlspec_queue_config(config)` | Wraps `litestar_queues` (`QueuePlugin`, `QueueConfig`) with Vertebra SHM ring buffers or SQLSpec database queue backends. |
| `security` | `create_shm_security_config()`, `create_session_security_config()` | Wraps `litestar_security` (`SecurityPlugin`, `SecurityConfig`) with zero-copy session stores and security contexts. |
| `autowire` | `create_autowire_config()`, `AutowirePlugin` | Wraps `litestar_autowire` with domain package scanning and dependency auto-wiring. |
| `email` | `create_vertebra_email_config()`, `EmailPlugin` | Bridges `litestar_email` to Vertebra's high-performance transactional email providers. |
| `mcp` | `create_mcp_config()`, `LitestarMCP`, `LitestarA2A` | Bridges `litestar_mcp` with Model Context Protocol routes and Agent-to-Agent communication. |
| `vite` | `create_vite_config()`, `VitePlugin`, `get_vertebra_client_path()`, `get_vertebra_client_vite_aliases()` | Bridges `litestar_vite` (`RuntimeConfig(executor="bun")`) with `vertebra-bundler` (Rolldown/Oxc) asset compilation, bundled `@vertebra/client` alias injection, and Inertia SSR. |
| `openapi` | `ScalarDocsConfig`, `SwaggerDocsConfig`, `AsyncAPIDocsConfig`, `ScalarSpecSource` | Configurable multi-spec API documentation portal (Scalar default, Swagger UI fallback, and AsyncAPI 3.0). |
| `htmx` | `HTMXPlugin`, `HTMXConfig` | Re-exports `litestar_htmx` plugins and decorators for server-rendered HTML applications. |
| `saq` | `SAQPlugin`, `SAQConfig` | Re-exports `litestar_saq` background job scheduling. |
| `asyncapi` | `AsyncAPIPlugin`, `AsyncAPIConfig` | Re-exports `litestar_asyncapi` event documentation. |
